# -*- coding: utf-8 -*-
"""
news_pipeline.py — RSS 主采集器 + 关键词驱动筛选 + 源健康度自愈
- 主力：RSS 源（中文为主，含交易所/监管/巨潮公告、行业垂媒）
- 备用：可选 NewsAPI / 聚合数据（如配置了 key）
- 失败源记录：errors.log + sources_health.json
- 连续失败 ≥2 次：自动拉黑，下一轮跳过；日志里说明原因
- 输出：
    briefing.txt        # 给 LLM：仅 时间 + 来源 + 标题（不带 URL/摘要）
    news_all.csv        # 全量明细，UTF-8-SIG
    keywords_used.txt   # 最终关键词（中文）
    qwen_keywords.txt   # Qwen 生成的原始关键词（便于核查）
    sources_used.txt    # 实际尝试的 RSS/接口列表（含成功条数）
    errors.log          # 详细错误
    sources_health.json # 源健康度（连续失败计数/最近错误）
"""
from __future__ import annotations
import os, re, csv, json, asyncio, time
from pathlib import Path
from datetime import datetime, timedelta, timezone
import httpx
import feedparser

# ── 输出路径 ────────────────────────────────────────────────────────────────
OUT_BRI = Path("briefing.txt")
OUT_CSV = Path("news_all.csv")
OUT_KW  = Path("keywords_used.txt")
OUT_QW  = Path("qwen_keywords.txt")
OUT_SRC = Path("sources_used.txt")
ERR_LOG = Path("errors.log")
HEALTH  = Path("sources_health.json")

# ── 配置 ────────────────────────────────────────────────────────────────────
TZ = timezone(timedelta(hours=8))  # 北京时间
SPAN_DAYS = int(os.getenv("SPAN_DAYS", "3"))
REQ_TIMEOUT = 15.0
RSS_PER_SOURCE_LIMIT = 80
MAX_CONCURRENCY = 8

QWEN_API_KEY    = os.getenv("QWEN_API_KEY")
NEWSAPI_KEY     = os.getenv("NEWSAPI_KEY")     # 可选
JUHE_KEY        = os.getenv("JUHE_KEY")        # 可选

# ── 工具 ────────────────────────────────────────────────────────────────────
def now() -> str:
    return datetime.now(TZ).isoformat(timespec="seconds")

def _append_text(p: Path, line: str):
    p.write_text((p.read_text("utf-8") if p.exists() else "") + line, encoding="utf-8")

def log_err(msg: str):
    _append_text(ERR_LOG, f"{now()} - {msg}\n")

def load_json(p: Path, default):
    if not p.is_file(): return default
    try:
        return json.loads(p.read_text("utf-8"))
    except Exception as e:
        log_err(f"读取 {p.name} 失败: {type(e).__name__}: {e}")
        return default

def save_json(p: Path, obj):
    p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")

def is_chinese_word(s: str) -> bool:
    return bool(re.search(r"[\u4e00-\u9fff]", s))

def uniq_keep_order(items):
    seen=set(); out=[]
    for x in items:
        if x not in seen:
            seen.add(x); out.append(x)
    return out

# ── 源清单（主力 RSS） ──────────────────────────────────────────────────────
# 说明：尽量用官方 RSS；没有就走 RSSHub 镜像（可能偶发失效，已做健康度自愈）
RSS_SOURCES_PRIMARY = [
    # —— 综合财经/主流媒体
    ("FT中文",            "ft_cn",    "https://www.ftchinese.com/rss/news"),
    ("界面新闻",          "jiemian",  "https://a.jiemian.com/index.php?m=article&a=rss"),
    ("新浪财经(热榜)",    "sina",     "https://rss.sina.com.cn/roll/finance/hot_roll.xml"),
    ("财新(镜像)",        "caixin",   "https://rsshub.app/caixin/latest"),
    ("华尔街见闻(镜像)",  "wallst",   "https://rsshub.app/wallstreetcn/news"),
    ("证券时报(镜像)",    "stcn",     "https://rsshub.app/stcn/news"),
    ("财联社(镜像)",      "cls",      "https://rsshub.app/cls/telegraph"),
    ("第一财经(镜像)",    "yicai",    "https://rsshub.app/yicai/brief"),

    # —— 监管/交易所（公告/新闻）
    ("中国证监会(镜像)",   "csrc",     "https://rsshub.app/csrc/news"),             # 监管动态
    ("上交所公告(镜像)",   "sse",      "https://rsshub.app/sse/renewal"),           # 若失效会记录并拉黑
    ("深交所公告(镜像)",   "szse",     "https://rsshub.app/szse/notice"),
    ("巨潮公告-最新(镜像)","cninfo",   "https://rsshub.app/cninfo/announcement"),

    # —— 行业垂媒（半导体/医药/期货等）
    ("半导体行业观察(镜像)","ic",       "https://rsshub.app/icpcw/semiconductor"),  # 半导体行业观察镜像（若失效会拉黑）
    ("药智网(镜像)",       "yaozhi",   "https://rsshub.app/yaozh/news"),
    ("期货日报(镜像)",     "qhrb",     "https://rsshub.app/qhrb/zhongyao"),
    ("中国基金报(镜像)",   "cfund",    "https://rsshub.app/fund/163"),
    ("经济观察报(镜像)",   "eeo",      "https://rsshub.app/eeo/yaowen"),
    ("36氪快讯(镜像)",     "36kr",     "https://rsshub.app/36kr/newsflashes"),
    ("钛媒体(镜像)",       "tmt",      "https://rsshub.app/tmtpost")
]

# 备选源（用于顶替被拉黑的主源）
RSS_SOURCES_BACKUP = [
    ("上证报(镜像)",       "ssepaper", "https://rsshub.app/zzxw/article"),
    ("中证网(镜像)",       "cs",       "https://rsshub.app/cs/news"),
    ("证券日报(镜像)",     "zqrb",     "https://rsshub.app/zqrb/stock")
]

# ── 关键词构建 ───────────────────────────────────────────────────────────────
def load_holdings() -> list[dict]:
    p = Path("holdings.json")
    if p.is_file():
        return json.loads(p.read_text("utf-8"))
    return []

def base_keywords_from_holdings(holds: list[dict]) -> tuple[list[str], list[str]]:
    sectors = set()
    words   = []
    for h in holds:
        name = (h.get("name") or "") + (h.get("symbol") or "")
        if "半导体" in name:
            sectors.add("半导体")
            words += ["半导体","芯片","晶圆","封测","光刻机","EDA","存储","GPU","HBM"]
        if "医药" in name:
            sectors.add("医药")
            words += ["医药","创新药","仿制药","集采","疫苗","器械","临床","MAH","减肥药"]
        if "酒" in name or "白酒" in name:
            sectors.add("白酒")
            words += ["白酒","消费","出厂价","动销","渠道"]
        if "债" in name:
            sectors.add("债券")
            words += ["国债","地方债","收益率","流动性","利率互换","期限利差"]
        if "红利" in name:
            sectors.add("红利")
            words += ["红利","分红","蓝筹","银行","煤炭","石油"]
        if "300" in name:
            sectors.add("宏观")
            words += ["宏观","PMI","通胀","出口","地产","就业"]
        if "豆粕" in name:
            sectors.add("农业")
            words += ["豆粕","饲料","生猪","油脂油料"]
    words = [w for w in words if is_chinese_word(w) and 2 <= len(w) <= 6]
    return sorted(sectors), uniq_keep_order(words)

async def qwen_expand_keywords(holds: list[dict]) -> list[str]:
    if not QWEN_API_KEY:
        return []
    prompt = (
        "请根据以下 ETF 持仓名称和行业，生成 50-120 个**中文**关键词，"
        "每个 2~4 个字为主，用中文逗号分隔，聚焦行业/主题/政策/产品名等：\n"
        + "\n".join(f"- {h.get('name','')} {h.get('symbol','')}" for h in holds)
        + "\n只输出关键词，不要解释。"
    )
    API = "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation"
    hdr = {"Content-Type":"application/json","Authorization":f"Bearer {QWEN_API_KEY}"}
    pl  = {"model":"qwen-plus","input":{"prompt":prompt},"parameters":{"max_tokens":600,"temperature":0.7}}
    try:
        async with httpx.AsyncClient(timeout=REQ_TIMEOUT) as c:
            r = await c.post(API, headers=hdr, json=pl)
            r.raise_for_status()
            text = r.json()["output"]["text"].strip()
    except Exception as e:
        log_err(f"Qwen 调用失败: {type(e).__name__}: {e}")
        return []
    raw = re.split(r"[，,\s]+", text)
    kws = [w.strip() for w in raw if is_chinese_word(w.strip()) and 2 <= len(w.strip()) <= 6]
    OUT_QW.write_text("\n".join(uniq_keep_order(kws)), encoding="utf-8")
    return uniq_keep_order(kws)

# ── RSS 抓取/健康度 ─────────────────────────────────────────────────────────
async def fetch_rss(name: str, sid: str, url: str, client: httpx.AsyncClient) -> list[dict]:
    try:
        r = await client.get(url, timeout=REQ_TIMEOUT)
        r.raise_for_status()
        feed = feedparser.parse(r.content)
        entries = feed.entries or []
        out=[]
        for e in entries[:RSS_PER_SOURCE_LIMIT]:
            title = (e.get("title") or "").strip()
            if not title: 
                continue
            # 时间
            pub = None
            for k in ("published_parsed","updated_parsed","created_parsed"):
                if e.get(k):
                    pub = time.strftime("%Y-%m-%d %H:%M:%S", e[k]); break
            pub = pub or datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
            summary = (e.get("summary") or e.get("description") or "")
            out.append({
                "title": title,
                "source": name,
                "published": pub,
                "url": e.get("link") or "",
                "summary": re.sub(r"<[^>]+>", "", summary).strip()
            })
        return out
    except Exception as e:
        log_err(f"RSS {name} 失败: {type(e).__name__}: {e}")
        return []

def update_health(h: dict, sid: str, ok: bool, err: str | None):
    st = h.get(sid, {"fail":0,"last_error":None,"last_time":None})
    if ok:
        st["fail"] = 0
        st["last_error"] = None
    else:
        st["fail"] = st.get("fail",0) + 1
        st["last_error"] = err or "no_items"
    st["last_time"] = now()
    h[sid] = st

def select_sources_with_health() -> list[tuple[str,str,str]]:
    health = load_json(HEALTH, {})
    selected = []
    skipped  = []
    for name,sid,url in RSS_SOURCES_PRIMARY:
        if health.get(sid,{}).get("fail",0) >= 2:
            skipped.append((name,sid,url))
            continue
        selected.append((name,sid,url))
    for name,sid,url in RSS_SOURCES_BACKUP:
        if len(selected) >= len(RSS_SOURCES_PRIMARY):
            break
        if health.get(sid,{}).get("fail",0) >= 2:
            continue
        selected.append((name,sid,url))
    for n,s,u in skipped:
        log_err(f"跳过源（连续失败≥2）: {n} [{s}] {u}")
    return selected

# ── 备用 API（可选） ────────────────────────────────────────────────────────
async def fetch_newsapi(keys: list[str], client: httpx.AsyncClient) -> list[dict]:
    if not NEWSAPI_KEY: return []
    q = " OR ".join(keys[:10]) or "宏观 OR 市场"
    url = f"https://newsapi.org/v2/everything?q={httpx.QueryParams({'q':q})['q']}&language=zh&pageSize=100&sortBy=publishedAt"
    try:
        r = await client.get(url, headers={"X-Api-Key": NEWSAPI_KEY})
        js = r.json()
        arts = js.get("articles") or []
        out=[]
        for a in arts:
            out.append({
                "title": a.get("title") or "",
                "source": a.get("source",{}).get("name") or "NewsAPI",
                "published": (a.get("publishedAt","")[:19]).replace("T"," "),
                "url": a.get("url") or "",
                "summary": a.get("description") or ""
            })
        return out
    except Exception as e:
        log_err(f"NewsAPI 失败: {type(e).__name__}: {e}")
        return []

async def fetch_juhe(client: httpx.AsyncClient) -> list[dict]:
    if not JUHE_KEY: return []
    try:
        r = await client.get("http://apis.juhe.cn/fapigx/caijing/query", params={"key": JUHE_KEY, "top": 50})
        js = r.json()
        if js.get("error_code") != 0:
            log_err(f"聚合数据返回 error_code={js.get('error_code')} msg={js.get('reason')}")
            return []
        out=[]
        for d in js.get("result",[]) or []:
            out.append({
                "title": d.get("title") or "",
                "source": d.get("src") or "聚合数据",
                "published": (d.get("time") or "")[:19].replace("T"," "),
                "url": d.get("url") or "",
                "summary": d.get("content") or ""
            })
        return out
    except Exception as e:
        log_err(f"聚合数据失败: {type(e).__name__}: {e}")
        return []

# ── 关键词匹配（标题+摘要） ─────────────────────────────────────────────────
def hit_by_keywords(item: dict, kws: list[str]) -> bool:
    txt = f"{item.get('title','')} {item.get('summary','')}"
    for w in kws:
        if w and w in txt:
            return True
    return False

# ── 主流程 ─────────────────────────────────────────────────────────────────
async def main():
    ERR_LOG.write_text("", encoding="utf-8")  # 每次跑清空旧错误

    print(f"{now()} - 开始收集（近 {SPAN_DAYS} 天），调用 Qwen 生成补充关键词")
    holds = load_holdings()
    print(f"{now()} - 读取持仓：holdings.json 共 {len(holds)} 条")

    sectors, base_kw = base_keywords_from_holdings(holds)
    print(f"{now()} - 基础关键词 {len(base_kw)} 个；行业：" + (", ".join(sorted(sectors)) if sectors else "无"))

    qk = await qwen_expand_keywords(holds) if holds else []
    final_kw = uniq_keep_order([w for w in (base_kw + qk) if is_chinese_word(w)])
    OUT_KW.write_text("\n".join(final_kw), encoding="utf-8")
    OUT_SRC.write_text("", encoding="utf-8")
    if qk: print(f"{now()} - Qwen 生成关键词 {len(qk)} 个")
    print(f"{now()} - 最终关键词 {len(final_kw)} 个，已写 keywords_used.txt / sources_used.txt")

    since = datetime.now(TZ) - timedelta(days=SPAN_DAYS)
    health = load_json(HEALTH, {})
    selected = select_sources_with_health()

    news_all = []
    async with httpx.AsyncClient(timeout=REQ_TIMEOUT, headers={"User-Agent":"Mozilla/5.0"}) as client:
        # RSS 并发
        sem = asyncio.Semaphore(MAX_CONCURRENCY)
        async def task(name,sid,url):
            async with sem:
                items = await fetch_rss(name,sid,url,client)
                ok = len(items) > 0
                update_health(health, sid, ok, None if ok else "no_items")
                _append_text(OUT_SRC, f"RSS {name} {url} — {len(items)} 条\n")
                return items
        rss_batches = await asyncio.gather(*[task(n,s,u) for n,s,u in selected])
        rss_items = [it for b in rss_batches for it in b]
        print(f"{now()} - RSS 抓到 {len(rss_items)} 条（未筛）")

        # 备用 API
        api_total = 0
        if JUHE_KEY:
            juhe = await fetch_juhe(client); api_total += len(juhe); news_all += juhe
            _append_text(OUT_SRC, f"API 聚合数据 — {len(juhe)} 条\n")
        if NEWSAPI_KEY:
            nap = await fetch_newsapi(final_kw, client); api_total += len(nap); news_all += nap
            _append_text(OUT_SRC, f"API NewsAPI — {len(nap)} 条\n")
        if api_total:
            print(f"{now()} - 备用 API 抓到 {api_total} 条（未筛）")

    # 合并
    news_all = rss_items + news_all
    print(f"{now()} - 收集完成：全量 {len(news_all)} 条（未去重）")

    # 筛选（标题+摘要；不抓正文，保证稳/快）
    filtered = [x for x in news_all if (not final_kw) or hit_by_keywords(x, final_kw)]
    print(f"{now()} - 标题/摘要命中后保留 {len(filtered)} 条（命中≥1 关键词）")

    # CSV（UTF-8-SIG，防止 Excel 乱码）
    with OUT_CSV.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f); w.writerow(["published","source","title","url"])
        for it in filtered:
            w.writerow([it.get("published",""), it.get("source",""), it.get("title",""), it.get("url","")])

    # briefing：仅 时间 + 来源 + 标题（不带 URL）
    lines = [f"# 新闻清单（近 {SPAN_DAYS} 天，共 {len(filtered)} 条）\n"]
    def kpub(x): return x.get("published","")
    for it in sorted(filtered, key=kpub, reverse=True):
        ts = it.get("published","")[5:16].replace("T"," ")
        lines.append(f"- {ts} [{it.get('source','')}] {it.get('title','').strip()}")
    OUT_BRI.write_text("\n".join(lines), encoding="utf-8")

    # 健康度入库
    save_json(HEALTH, health)

    # 兜底提示
    if len(news_all) == 0:
        log_err("全量 0 条：可能=源空/限流/网络受限；详见 sources_used.txt")
    if len(filtered) == 0 and final_kw:
        log_err("命中 0 条：可能=中文关键词过窄；可临时放宽或增补词表")

    print(f"{now()} - 已写 briefing.txt、news_all.csv、keywords_used.txt、sources_used.txt")
    print(f"{now()} - errors.log 大小 {ERR_LOG.stat().st_size if ERR_LOG.exists() else 0} bytes；健康度见 sources_health.json")
    print(f"{now()} - collector 任务完成")

if __name__ == "__main__":
    asyncio.run(main())
