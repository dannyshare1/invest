# -*- coding: utf-8 -*-
"""
news_pipeline.py — 资讯收集器 v3
变更要点
- 新增 聚合数据「综合财经新闻」API：http://apis.juhe.cn/fapigx/caijing/query
- 抓取策略：先多抓（RSS + 聚合数据），再按关键词筛选生成 briefing
- CSV 全量写入（不去重），并标注是否命中关键词 hit=0/1，便于回溯
- 详细错误日志 errors.log（网络/解析/JSON/字段缺失）
- 关键词：从 holdings.json 推断行业 +（可选）Qwen 扩展
依赖：httpx, charset-normalizer(可选)
环境变量：QWEN_API_KEY(可选), JUHE_KEY(聚合数据key)
"""

from __future__ import annotations
import asyncio, os, json, csv, re, textwrap, math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Tuple
import httpx
from html import unescape

# 可选：更可靠的编码探测
try:
    from charset_normalizer import from_bytes as detect_bytes
except Exception:
    detect_bytes = None

# ───── 路径 / 常量 ─────
TZ = timezone(timedelta(hours=8))
ROOT = Path(".")
OUT_CSV  = ROOT / "news_all.csv"         # 全量，不去重，含 hit 标记
OUT_BRIF = ROOT / "briefing.txt"         # 仅命中关键词的精简版
OUT_KW   = ROOT / "keywords_used.txt"
OUT_SRC  = ROOT / "sources_used.txt"
ERR_LOG  = ROOT / "errors.log"

RSS_SOURCES: List[Tuple[str, str]] = [
    ("FT中文",   "https://www.ftchinese.com/rss/news"),
    ("界面新闻", "https://a.jiemian.com/index.php?m=article&a=rss"),
    ("新浪财经", "https://rss.sina.com.cn/roll/finance/hot_roll.xml"),
]

JUHE_API = "http://apis.juhe.cn/fapigx/caijing/query"  # 聚合数据·综合财经新闻

QWEN_API   = "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation"
QWEN_MODEL = "qwen-plus"

# ───── 工具函数 ─────
def now_str():
    return datetime.now(TZ).isoformat()

def logerr(msg: str):
    ERR_LOG.parent.mkdir(parents=True, exist_ok=True)
    with ERR_LOG.open("a", encoding="utf-8") as f:
        f.write(f"{now_str()}  {msg}\n")

def clean_text(s: str) -> str:
    if not s:
        return ""
    s = unescape(s)
    s = re.sub(r"<[^>]+>", " ", s)
    s = re.sub(r"&[a-zA-Z#0-9]+;", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def safe_decode(content: bytes, fallback_enc: str | None = None) -> str:
    if detect_bytes:
        try:
            m = detect_bytes(content).best()
            if m and m.encoding:
                return content.decode(m.encoding, errors="ignore")
        except Exception as e:
            logerr(f"charset-normalizer fail: {e!r}")
    for enc in ([fallback_enc] if fallback_enc else []) + ["utf-8", "gb18030"]:
        try:
            return content.decode(enc, errors="ignore")
        except Exception:
            pass
    return content.decode("utf-8", errors="ignore")

HEADERS = {"User-Agent": "Mozilla/5.0 (collector/3.0; +bot)"}

async def fetch_text(client: httpx.AsyncClient, url: str, timeout: int = 20) -> str:
    r = await client.get(url, timeout=timeout, headers=HEADERS)
    r.raise_for_status()
    # 个别源（新浪）按 gb 编码，统一走探测+回退
    return safe_decode(r.content, r.encoding)

# ───── 持仓 / 行业 / 关键词 ─────
def load_holdings() -> List[Dict]:
    p = Path("holdings.json")
    if not p.is_file():
        logerr("holdings.json 不存在")
        return []
    try:
        return json.loads(p.read_text("utf-8"))
    except Exception as e:
        logerr(f"holdings.json 解析失败: {e!r}")
        return []

def infer_industries(holds: List[Dict]) -> List[str]:
    m = {
        "半导体": "半导体",
        "医药": "医药",
        "酒":   "白酒",
        "国债": "债券",
        "沪深300": "宏观",
        "豆粕": "农业",
    }
    out = set()
    for h in holds:
        name = (h.get("name") or "") + (h.get("symbol") or "")
        for k, v in m.items():
            if k in name:
                out.add(v)
    if not out:
        out.add("宏观")
    return sorted(out)

def base_keywords_from_industries(inds: List[str]) -> List[str]:
    base = []
    if "债券" in inds:
        base += ["国债","地方债","信用债","利率","利率互换","收益率曲线","流动性","中长期利率","逆回购","央行"]
    if "农业" in inds:
        base += ["豆粕","大豆","玉米","饲料","油脂油料","生猪","供需","库存","农产品价格"]
    if "医药" in inds:
        base += ["医药","创新药","仿制药","集采","PD-1","GLP-1","疫苗","医疗器械","高值耗材","CXO","医保谈判","MAH","生物类似药","基因治疗","细胞治疗"]
    if "半导体" in inds:
        base += ["半导体","芯片","晶圆代工","封测","光刻机","刻蚀机","存储芯片","HBM","GPU","EDA","硅片","车规芯片","功率半导体","碳化硅","氮化镓","集成电路","AI芯片"]
    if "白酒" in inds:
        base += ["白酒","白酒消费升级","渠道","价格","供需","动销"]
    if "宏观" in inds:
        base += ["通胀","通缩","降息","加息","汇率","PMI","社融","M2","财政政策","货币政策","经济复苏"]
    return sorted(set(base))

async def qwen_expand_keywords(client: httpx.AsyncClient, industries: List[str], seeds: List[str]) -> List[str]:
    if not os.getenv("QWEN_API_KEY"):
        return []
    prompt = textwrap.dedent(f"""
        你是资讯检索助手。基于这些行业：{", ".join(industries)}，
        和初始关键词：{", ".join(seeds[:60])}
        扩展出 60~100 个**中文关键词**（仅名词或短语，每行一个，不要编号、不要解释）。
    """).strip()
    hdr = {
        "Authorization": f"Bearer {os.getenv('QWEN_API_KEY')}",
        "Content-Type": "application/json",
    }
    payload = {"model": QWEN_MODEL, "input": {"prompt": prompt},
               "parameters": {"max_tokens": 800, "temperature": 0.2}}
    r = await client.post(QWEN_API, headers=hdr, json=payload, timeout=60)
    r.raise_for_status()
    txt = (r.json().get("output") or {}).get("text", "")
    kws = [clean_text(x) for x in txt.splitlines() if clean_text(x)]
    kws = [k for k in kws if re.search(r"[\u4e00-\u9fffA-Z]", k)]
    return kws[:100]

def match_keywords(title: str, summary: str, kws: List[str]) -> bool:
    txt = f"{title} {summary}"
    return any(k in txt for k in kws)

# ───── RSS 抓取 ─────
def rss_extract(xml_text: str, source: str) -> List[Dict]:
    items = []
    blocks = re.split(r"</item>|</entry>", xml_text, flags=re.I)
    for b in blocks:
        t = clean_text(re.search(r"<title[^>]*>(.*?)</title>", b, re.I|re.S).group(1)) if re.search(r"<title", b, re.I) else ""
        d = clean_text(re.search(r"<description[^>]*>(.*?)</description>|<summary[^>]*>(.*?)</summary>", b, re.I|re.S).group(1) if re.search(r"<description|<summary", b, re.I) else "")
        u = clean_text(re.search(r"<link[^>]*>(.*?)</link>|<link[^>]*href=[\"']([^\"']+)[\"']", b, re.I|re.S).group(1) if re.search(r"<link", b, re.I) else "")
        p = clean_text(re.search(r"<pubDate[^>]*>(.*?)</pubDate>|<updated[^>]*>(.*?)</updated>", b, re.I|re.S).group(1) if re.search(r"<pubDate|<updated", b, re.I) else "")
        if t:
            items.append({"source": source, "title": t, "summary": d, "url": u, "published_at": p})
    return items

async def collect_rss(client: httpx.AsyncClient, kws: List[str], max_items_per_src: int = 200) -> Tuple[List[Dict], List[Dict]]:
    all_items, hit_items = [], []
    for name, url in RSS_SOURCES:
        try:
            txt = await fetch_text(client, url)
            items = rss_extract(txt, name)
            all_items.extend(items[:max_items_per_src])
            hits = [it for it in items if match_keywords(it["title"], it["summary"], kws)]
            hit_items.extend(hits)
            print(f"{now_str()} - INFO - RSS {name} 抓到 {len(items)} 条，命中 {len(hits)} 条")
        except Exception as e:
            print(f"{now_str()} - ERROR - RSS {name} 失败: {e.__class__.__name__}: {e}")
            logerr(f"RSS {name} 失败: {e.__class__.__name__}: {e!r}")
    return all_items, hit_items

# ───── 聚合数据 抓取 ─────
async def juhe_fetch(client: httpx.AsyncClient, pages: int = 8, page_size: int = 50) -> List[Dict]:
    key = os.getenv("JUHE_KEY")
    if not key:
        return []
    out = []
    for p in range(1, pages + 1):
        try:
            r = await client.get(JUHE_API, params={"key": key, "page": p, "pagesize": page_size}, headers=HEADERS, timeout=20)
            js = r.json()
        except Exception as e:
            logerr(f"JUHE page {p} fail: {e.__class__.__name__}: {e!r}")
            continue

        # 兼容不同返回结构
        data = js.get("result") or js.get("resultData") or js.get("data") or {}
        if isinstance(data, dict):
            arr = data.get("data") or data.get("list") or data.get("news") or data.get("articles") or []
        elif isinstance(data, list):
            arr = data
        else:
            arr = []

        for x in arr:
            title   = clean_text(x.get("title") or x.get("contentTitle") or "")
            summary = clean_text(x.get("digest") or x.get("content") or x.get("intro") or "")
            url     = x.get("url") or x.get("src") or x.get("link") or ""
            pub     = x.get("pubDate") or x.get("ctime") or x.get("time") or x.get("date") or ""
            source  = x.get("source") or x.get("media") or "聚合数据"
            if title:
                out.append({"source": f"聚合数据/{source}", "title": title, "summary": summary, "url": url, "published_at": pub})
    return out

# ───── 主流程 ─────
async def main(days: int = 3, juhe_pages: int = 8, juhe_pagesize: int = 50, rss_cap: int = 200):
    print(f"{now_str()} - INFO - 开始收集（近 {days} 天），调用 Qwen 生成补充关键词")

    holds = load_holdings()
    print(f"{now_str()} - INFO - 读取持仓：holdings.json 共 {len(holds)} 条")
    industries = infer_industries(holds)
    seeds = base_keywords_from_industries(industries)
    print(f"{now_str()} - INFO - 基础关键词 {len(seeds)} 个；行业：{', '.join(industries)}")

    async with httpx.AsyncClient(follow_redirects=True, timeout=20, headers=HEADERS) as client:
        # LLM 扩展关键词（可选）
        extra = await qwen_expand_keywords(client, industries, seeds)
        if extra:
            print(f"{now_str()} - INFO - Qwen 生成关键词 {len(extra)} 个")
        all_kw = sorted(set(seeds + extra))
        OUT_KW.write_text(
            "【基础关键词】\n" + "\n".join(seeds) + "\n\n【LLM补充关键词】\n" + "\n".join(extra) + "\n\n【最终用于检索】\n" + "\n".join(all_kw),
            encoding="utf-8"
        )
        OUT_SRC.write_text("\n".join([s for s,_ in RSS_SOURCES] + ["聚合数据/综合财经"]), encoding="utf-8")
        print(f"{now_str()} - INFO - 最终关键词 {len(all_kw)} 个，已写 keywords_used.txt / sources_used.txt")

        # 并行抓取
        rss_all, rss_hit = await collect_rss(client, all_kw, max_items_per_src=rss_cap)
        juhe_all = await juhe_fetch(client, pages=juhe_pages, page_size=juhe_pagesize)
        juhe_hit = [it for it in juhe_all if match_keywords(it["title"], it["summary"], all_kw)]

    # 汇总：全量 / 命中
    all_items = rss_all + juhe_all
    hit_items = rss_hit + juhe_hit

    # 写 CSV（全量，不去重；添加 hit 标记）
    cols = ["published_at", "source", "title", "summary", "url", "hit"]
    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for it in all_items:
            w.writerow({
                "published_at": it.get("published_at",""),
                "source": it.get("source",""),
                "title": it.get("title",""),
                "summary": it.get("summary",""),
                "url": it.get("url",""),
                "hit": 1 if match_keywords(it.get("title",""), it.get("summary",""), all_kw) else 0
            })

    # briefing（只放命中，标题/摘要分行）
    lines = [f"# 今日新闻（命中关键词：{len(hit_items)} / 全量：{len(all_items)}）", ""]
    for i, it in enumerate(hit_items, 1):
        lines.append(f"{i}. **{it['title']}**  [{it['source']}]({it['url']})")
        if it.get("summary"):
            lines.append(it["summary"])
        lines.append("")
    OUT_BRI F.write_text("\n".join(lines), encoding="utf-8")

    print(f"{now_str()} - INFO - 收集完成：全量 {len(all_items)} 条，命中 {len(hit_items)} 条（未去重）")
    print(f"{now_str()} - INFO - 已写 briefing.txt、news_all.csv、keywords_used.txt、sources_used.txt")
    print(f"{now_str()} - INFO - errors.log 大小 {ERR_LOG.stat().st_size if ERR_LOG.exists() else 0} bytes")
    print(f"{now_str()} - INFO - collector 任务完成")

if __name__ == "__main__":
    asyncio.run(main())
