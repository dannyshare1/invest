# -*- coding: utf-8 -*-
"""
news_pipeline.py — RSS 外置源文件 + 关键词筛选 + 源健康度自愈/自动剔除
- RSS 源不再写死在代码，改为从 sources.yml（优先）或 sources.txt 读取
- 手动在源文件中添加/删除/修改条目，脚本下次运行自动生效
- 连续失败 ≥3（抓取异常或0条）→ 自动从源文件剔除，并记录到 errors.log
- 仍输出：
    briefing.txt        # 给 LLM：仅 时间 + 来源 + 标题（不带 URL/摘要）
    news_all.csv        # 全量明细（UTF-8-SIG，防止 Excel 乱码）
    keywords_used.txt   # 最终关键词（中文）
    qwen_keywords.txt   # Qwen 生成的原始关键词
    sources_used.txt    # 实际尝试的 RSS/接口列表（含成功条数）
    errors.log          # 详细错误
    sources_health.json # 源健康度（连续失败/最近错误）
"""
from __future__ import annotations
import os, re, csv, json, asyncio, time
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Tuple
import httpx
import feedparser
import yaml  # PyYAML

# ── 常量 & 输出路径 ─────────────────────────────────────────────────────────
TZ = timezone(timedelta(hours=8))  # 北京时间
SPAN_DAYS = int(os.getenv("SPAN_DAYS", "3"))
REQ_TIMEOUT = 15.0
RSS_PER_SOURCE_LIMIT = 80
MAX_CONCURRENCY = 8
FAIL_REMOVE_THRESHOLD = 3  # 连续失败≥3次，自动从源文件剔除

OUT_BRI = Path("briefing.txt")
OUT_CSV = Path("news_all.csv")
OUT_KW  = Path("keywords_used.txt")
OUT_QW  = Path("qwen_keywords.txt")
OUT_SRC = Path("sources_used.txt")
ERR_LOG = Path("errors.log")
HEALTH  = Path("sources_health.json")

SFILE_YML = Path("sources.yml")   # 优先读取
SFILE_TXT = Path("sources.txt")   # 备选：每行 name|sid|url（竖线分隔）

# 可选的备用 API（代码保留，不动）
QWEN_API_KEY    = os.getenv("QWEN_API_KEY")
NEWSAPI_KEY     = os.getenv("NEWSAPI_KEY")     # 可选
JUHE_KEY        = os.getenv("JUHE_KEY")        # 可选

# ── 默认内置源（仅用于首次自动初始化 sources.yml，用完就靠外部文件） ─────────────
DEFAULT_RSS_SOURCES: List[Dict[str, str]] = [
    # —— 综合财经/主流媒体
    {"name":"FT中文", "sid":"ft_cn", "url":"https://www.ftchinese.com/rss/news"},
    {"name":"界面新闻", "sid":"jiemian", "url":"https://a.jiemian.com/index.php?m=article&a=rss"},
    {"name":"新浪财经(热榜)", "sid":"sina", "url":"https://rss.sina.com.cn/roll/finance/hot_roll.xml"},
    {"name":"财新(镜像)", "sid":"caixin", "url":"https://rsshub.app/caixin/latest"},
    {"name":"华尔街见闻(镜像)", "sid":"wallst", "url":"https://rsshub.app/wallstreetcn/news"},
    {"name":"证券时报(镜像)", "sid":"stcn", "url":"https://rsshub.app/stcn/news"},
    {"name":"财联社(镜像)", "sid":"cls", "url":"https://rsshub.app/cls/telegraph"},
    {"name":"第一财经(镜像)", "sid":"yicai", "url":"https://rsshub.app/yicai/brief"},
    # —— 监管/交易所/巨潮
    {"name":"中国证监会(镜像)", "sid":"csrc", "url":"https://rsshub.app/csrc/news"},
    {"name":"上交所公告(镜像)", "sid":"sse", "url":"https://rsshub.app/sse/renewal"},
    {"name":"深交所公告(镜像)", "sid":"szse", "url":"https://rsshub.app/szse/notice"},
    {"name":"巨潮公告-最新(镜像)", "sid":"cninfo", "url":"https://rsshub.app/cninfo/announcement"},
    # —— 行业垂媒
    {"name":"半导体行业观察(镜像)", "sid":"ic", "url":"https://rsshub.app/icpcw/semiconductor"},
    {"name":"药智网(镜像)", "sid":"yaozhi", "url":"https://rsshub.app/yaozh/news"},
    {"name":"期货日报(镜像)", "sid":"qhrb", "url":"https://rsshub.app/qhrb/zhongyao"},
    {"name":"中国基金报(镜像)", "sid":"cfund", "url":"https://rsshub.app/fund/163"},
    {"name":"经济观察报(镜像)", "sid":"eeo", "url":"https://rsshub.app/eeo/yaowen"},
    {"name":"36氪快讯(镜像)", "sid":"36kr", "url":"https://rsshub.app/36kr/newsflashes"},
    {"name":"钛媒体(镜像)", "sid":"tmt", "url":"https://rsshub.app/tmtpost"},
]

# ── 小工具 ─────────────────────────────────────────────────────────────────
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

# ── 源文件：加载 / 保存 / 初始化 / 自动剔除 ─────────────────────────────────────
def init_sources_file_if_needed() -> List[Dict[str,str]]:
    """若 sources.yml / sources.txt 都不存在，则写入默认 YAML。"""
    if SFILE_YML.exists() or SFILE_TXT.exists():
        return []
    yaml.safe_dump(DEFAULT_RSS_SOURCES, SFILE_YML.open("w", encoding="utf-8"), allow_unicode=True, sort_keys=False)
    return DEFAULT_RSS_SOURCES

def load_sources_from_txt() -> List[Dict[str,str]]:
    rows=[]
    for line in SFILE_TXT.read_text("utf-8").splitlines():
        line=line.strip()
        if not line or line.startswith("#"): continue
        # 支持  name|sid|url  或者  url（自动生成 sid）
        if "|" in line:
            parts=[p.strip() for p in line.split("|")]
            if len(parts) >= 3:
                rows.append({"name":parts[0], "sid":parts[1], "url":"|".join(parts[2:]).strip()})
        else:
            url=line
            sid=re.sub(r"[^a-z0-9]+","_", url.lower())
            rows.append({"name":url, "sid":sid[:32], "url":url})
    return rows

def load_sources() -> Tuple[List[Dict[str,str]], str]:
    """优先 YAML；无则 TXT；都没有则初始化 YAML（用内置默认）"""
    init_sources_file_if_needed()
    if SFILE_YML.exists():
        try:
            data = yaml.safe_load(SFILE_YML.read_text("utf-8")) or []
            rows=[]
            for d in data:
                if not isinstance(d, dict): continue
                name=d.get("name") or d.get("title") or "未命名源"
                sid =(d.get("sid") or re.sub(r"[^a-z0-9]+","_", (d.get('url') or '').lower())[:32]).strip()
                url =(d.get("url") or "").strip()
                if url:
                    rows.append({"name":name.strip(), "sid":sid, "url":url})
            return rows, "yml"
        except Exception as e:
            log_err(f"读取 sources.yml 失败: {type(e).__name__}: {e}")
            # 回退：若存在 TXT 尝试 TXT，否则返回默认内置
    if SFILE_TXT.exists():
        try:
            rows = load_sources_from_txt()
            return rows, "txt"
        except Exception as e:
            log_err(f"读取 sources.txt 失败: {type(e).__name__}: {e}")
    # 都失败：直接用默认（不落盘，避免覆盖用户文件）
    return DEFAULT_RSS_SOURCES[:], "default"

def save_sources(sources: List[Dict[str,str]], mode: str):
    """按原始介质保存（优先 yml；如果本轮是 default，写回 yml）。"""
    if mode == "txt" and SFILE_TXT.exists() and not SFILE_YML.exists():
        # 写回 TXT（name|sid|url）
        lines=[]
        for s in sources:
            lines.append(f"{s['name']}|{s['sid']}|{s['url']}")
        SFILE_TXT.write_text("\n".join(lines) + "\n", encoding="utf-8")
    else:
        # 其他情况统一写回 YAML
        yaml.safe_dump(sources, SFILE_YML.open("w", encoding="utf-8"), allow_unicode=True, sort_keys=False)

def remove_failed_sources_if_needed(health: dict, sources: List[Dict[str,str]], mode: str) -> List[Dict[str,str]]:
    """连续失败≥阈值则从源列表中剔除并写回文件。"""
    keep=[]; removed=[]
    for s in sources:
        sid=s["sid"]
        fails = health.get(sid,{}).get("fail",0)
        if fails >= FAIL_REMOVE_THRESHOLD:
            removed.append(s)
        else:
            keep.append(s)
    if removed:
        save_sources(keep, mode)
        for s in removed:
            log_err(f"源已剔除（连续失败≥{FAIL_REMOVE_THRESHOLD}）：{s['name']} [{s['sid']}] {s['url']}")
    return keep

# ── 关键词构建（跟原先一致） ────────────────────────────────────────────────
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
        "请根据以下 ETF 持仓名称和行业，生成 50-120 个中文关键词和50-120个英文关键词，"
        "用中文逗号分隔，聚焦行业/主题/政策/产品名等：\n"
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

# ── 抓取 & 健康度 ───────────────────────────────────────────────────────────
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

# ── 备用 API（保留，未改动） ────────────────────────────────────────────────
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
    if not kws:  # 无关键词时不过滤
        return True
    txt = f"{item.get('title','')} {item.get('summary','')}"
    for w in kws:
        if w and w in txt:
            return True
    return False

# ── 主流程 ─────────────────────────────────────────────────────────────────
async def main():
    ERR_LOG.write_text("", encoding="utf-8")  # 每次跑清空旧错误

    print(f"{now()} - 开始收集（近 {SPAN_DAYS} 天），从源文件读取 RSS 列表")
    sources, mode = load_sources()
    print(f"{now()} - 源文件模式: {mode}；共 {len(sources)} 个 RSS 源")

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

    health = load_json(HEALTH, {})
    news_all = []

    async with httpx.AsyncClient(timeout=REQ_TIMEOUT, headers={"User-Agent":"Mozilla/5.0"}) as client:
        # RSS 并发抓取
        sem = asyncio.Semaphore(MAX_CONCURRENCY)
        async def task(src):
            name, sid, url = src["name"], src["sid"], src["url"]
            async with sem:
                items = await fetch_rss(name, sid, url, client)
                ok = len(items) > 0
                update_health(health, sid, ok, None if ok else "no_items")
                _append_text(OUT_SRC, f"RSS {name} {url} — {len(items)} 条\n")
                return items, src, ok

        batches = await asyncio.gather(*[task(s) for s in sources])
        rss_items = []
        for items, src, ok in batches:
            rss_items.extend(items)

        print(f"{now()} - RSS 抓到 {len(rss_items)} 条（未筛）")

        # 备用 API（可选）
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

    # 筛选（标题+摘要；不抓正文）
    filtered = [x for x in news_all if hit_by_keywords(x, final_kw)]
    print(f"{now()} - 标题/摘要命中后保留 {len(filtered)} 条（命中≥1 关键词）")

    # CSV（UTF-8-SIG）
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

    # 保存健康度
    save_json(HEALTH, health)

    # 自动剔除：连续失败≥阈值
    sources_after = remove_failed_sources_if_needed(health, sources, mode)
    if len(sources_after) != len(sources):
        print(f"{now()} - 已从源文件移除 {len(sources) - len(sources_after)} 个连续失败的源")

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
