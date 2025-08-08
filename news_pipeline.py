# -*- coding: utf-8 -*-
"""
news_pipeline.py — RSS-first 资讯采集器（中文优先，无 Bing/百度）
---------------------------------------------------------------
- 主力源：Google News RSS（中文地区）+ 国内媒体官方 RSS
- 备用源：NewsAPI / Mediastack / 聚合数据（有 key 则启用，否则自动跳过）
- 关键词：从 holdings.json 推断板块中文关键词 + Qwen 补充（仅中文 2~4 字）
- 过滤：对“标题+摘要+正文”做中文关键词匹配（≥1 命中保留）
- 输出：briefing.txt, news_all.csv, keywords_used.txt, qwen_keywords.txt,
        sources_used.txt, errors.log（UTF-8，无乱码）

环境变量（可选）：
- QWEN_API_KEY           # 用于中文关键词补充（推荐）
- NEWSAPI_KEY            # 备用新闻 API（可选）
- MEDIASTACK_KEY         # 备用新闻 API（可选）
- JUHE_KEY               # 备用新闻 API（可选）
"""

from __future__ import annotations
import os, re, csv, json, math, asyncio, itertools, time
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Tuple
import httpx
import feedparser
from bs4 import BeautifulSoup

TZ = timezone(timedelta(hours=8))
def now() -> str: return datetime.now(TZ).strftime("%Y-%m-%dT%H:%M:%S%z")

# ── 文件路径 ───────────────────────────────────────────────────────────────
OUT_NEWS = "news_all.csv"
OUT_BRI  = "briefing.txt"
OUT_KW   = "keywords_used.txt"
OUT_QKW  = "qwen_keywords.txt"
OUT_SRC  = "sources_used.txt"
OUT_ERR  = "errors.log"

# ── 打印 & 记录 ───────────────────────────────────────────────────────────
def log(msg: str) -> None:
    print(f"{now()} - {msg}")

def log_err(msg: str) -> None:
    with open(OUT_ERR, "a", encoding="utf-8") as f:
        f.write(f"{now()} - {msg}\n")

# ── 读取持仓 → 推断行业/基础中文关键词 ─────────────────────────────────────
def load_holdings(path="holdings.json") -> List[Dict[str, Any]]:
    if not os.path.isfile(path):
        log_err("holdings.json not found")
        return []
    return json.loads(open(path, "r", encoding="utf-8").read())

# 非穷举，仅针对你的持仓习惯做足够好的映射（可继续扩展）
ETF_TO_TOPICS = {
    "红利": ["分红", "股息", "高股息", "价值", "蓝筹"],
    "半导体": ["半导体", "芯片", "晶圆", "封测", "光刻机", "存储", "GPU", "HBM", "车规芯片"],
    "医药": ["医药", "创新药", "仿制药", "集采", "器械", "疫苗", "GLP-1", "CXO"],
    "白酒": ["白酒", "消费升级", "渠道", "动销", "高端酒", "次高端"],
    "农业": ["农业", "饲料", "豆粕", "油脂", "粮食", "玉米", "小麦", "生猪"],
    "债券": ["国债", "地方债", "信用债", "流动性", "利率互换", "收益率曲线"],
    "宽基": ["沪深300", "A股", "北向资金", "成交额", "指数权重"],
    "宏观": ["降息", "加息", "通胀", "汇率", "PMI", "社融", "财政政策", "货币政策"],
}

def infer_topics_from_holdings(holds: List[dict]) -> Tuple[List[str], List[str]]:
    names = [h.get("name", "") for h in holds]
    topics = set()
    for n in names:
        if "红利" in n: topics.add("红利")
        if "半导体" in n: topics.add("半导体")
        if "医药" in n: topics.add("医药")
        if "酒" in n: topics.add("白酒")
        if "豆粕" in n: topics.add("农业")
        if "债" in n or "国债" in n: topics.add("债券")
        if "沪深300" in n: topics.add("宽基")
    # 通用宏观
    topics.add("宏观")

    base_kw = set()
    for t in topics:
        base_kw.update(ETF_TO_TOPICS.get(t, []))
    # 只要中文，最长 4 字
    base_kw = {w for w in base_kw if re.search(r"[\u4e00-\u9fff]", w) and 1 <= len(w) <= 4}
    return sorted(list(topics)), sorted(list(base_kw))

# ── Qwen：生成 2~4 字中文关键词（可选） ─────────────────────────────────────
QWEN_API = "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation"

async def qwen_expand_keywords(base_topics: List[str], base_kw: List[str]) -> List[str]:
    if not os.getenv("QWEN_API_KEY"):
        return []
    prompt = (
        "请根据以下行业主题，列出20~120个 **中文** 关键词，每个 2~4 个汉字，"
        "覆盖宏观、板块、产业链、产品名、政策名等；不要英文、不要标点、不要编号，每行一个：\n\n"
        f"行业主题：{','.join(base_topics)}\n"
        f"已知词：{','.join(base_kw)}\n"
    )
    headers = {
        "Authorization": f"Bearer {os.getenv('QWEN_API_KEY')}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": "qwen-plus",
        "input": {"prompt": prompt},
        "parameters": {"max_tokens": 600, "temperature": 0.5},
    }
    try:
        async with httpx.AsyncClient(timeout=60) as c:
            r = await c.post(QWEN_API, headers=headers, json=payload)
            r.raise_for_status()
            text = r.json()["output"]["text"]
    except Exception as e:
        log_err(f"Qwen 调用失败: {e}")
        return []
    # 提取纯中文 2~4 字
    lines = [s.strip() for s in text.splitlines() if s.strip()]
    zh = set()
    for s in lines:
        s = re.sub(r"[^\u4e00-\u9fff]", "", s)  # 去非中文
        if 1 < len(s) <= 4:
            zh.add(s)
    with open(OUT_QKW, "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(zh)))
    return sorted(zh)

# ── RSS 源（主力） ─────────────────────────────────────────────────────────
# Google News RSS（中文地区）
def google_news_rss_urls(keywords: List[str], batch_size: int = 6) -> List[Tuple[str, str]]:
    urls = []
    for i in range(0, len(keywords), batch_size):
        ks = keywords[i:i+batch_size]
        # (词1 OR 词2 OR ...) 只中文，用中文地区参数，减少英文返回
        q = " OR ".join([f'"{k}"' for k in ks])
        url = (
            "https://news.google.com/rss/search"
            f"?q=({httpx.QueryParams({'q': q})['q']})"
            "&hl=zh-CN&gl=CN&ceid=CN:zh"
        )
        urls.append(("GoogleNews", url))
    return urls

# 国内官方 RSS（选择稳定的）
DOMESTIC_RSS: List[Tuple[str, str]] = [
    ("FT中文", "https://www.ftchinese.com/rss/news"),
    ("界面新闻", "https://a.jiemian.com/index.php?m=article&a=rss"),
    ("新浪财经", "https://rss.sina.com.cn/roll/finance/hot_roll.xml"),
]

# ── 备用 API（有 key 则用） ────────────────────────────────────────────────
async def fetch_newsapi(keywords: List[str], days: int = 3) -> List[dict]:
    key = os.getenv("NEWSAPI_KEY")
    if not key: return []
    since = (datetime.utcnow() - timedelta(days=days)).date().isoformat()
    q = " OR ".join([f'"{k}"' for k in keywords])
    url = "https://newsapi.org/v2/everything"
    params = {"q": q, "language": "zh", "from": since, "pageSize": 100, "sortBy": "publishedAt", "apiKey": key}
    try:
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.get(url, params=params)
            js = r.json()
            arts = js.get("articles", []) or []
            out = []
            for a in arts:
                out.append({
                    "source": "NewsAPI",
                    "title": a.get("title",""),
                    "link": a.get("url",""),
                    "published": a.get("publishedAt",""),
                    "summary": a.get("description",""),
                    "content": a.get("content",""),
                })
            return out
    except Exception as e:
        log_err(f"NewsAPI 异常：{e}")
        return []

async def fetch_mediastack(keywords: List[str], days: int = 3) -> List[dict]:
    key = os.getenv("MEDIASTACK_KEY")
    if not key: return []
    date_from = (datetime.utcnow() - timedelta(days=days)).date().isoformat()
    q = ",".join(keywords[:20])  # mediastack 不支持太复杂的布尔
    url = "http://api.mediastack.com/v1/news"
    params = {"access_key": key, "languages": "zh", "date": f"{date_from},", "keywords": q, "limit": 100, "sort": "published_desc"}
    try:
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.get(url, params=params)
            js = r.json()
            data = js.get("data", []) or []
            out = []
            for a in data:
                out.append({
                    "source": "Mediastack",
                    "title": a.get("title",""),
                    "link": a.get("url",""),
                    "published": a.get("published_at",""),
                    "summary": a.get("description",""),
                    "content": "",
                })
            return out
    except Exception as e:
        log_err(f"Mediastack 异常：{e}")
        return []

async def fetch_juhe_caijing(keywords: List[str]) -> List[dict]:
    key = os.getenv("JUHE_KEY")
    if not key: return []
    # 聚合这个“综合财经新闻”接口字段比较固定，无关键词过滤，只当补量
    url = "http://apis.juhe.cn/fapigx/caijing/query"
    try:
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.get(url, params={"key": key, "size": 50})
            js = r.json()
            if js.get("error_code") != 0:
                log_err(f"聚合数据错误: {js}")
                return []
            data = js.get("result", []) or []
            out = []
            for a in data:
                out.append({
                    "source": "Juhe",
                    "title": a.get("title",""),
                    "link": a.get("url",""),
                    "published": a.get("pubDate",""),
                    "summary": a.get("digest",""),
                    "content": a.get("content","") or "",
                })
            return out
    except Exception as e:
        log_err(f"聚合数据异常：{e}")
        return []

# ── 抓取与解析 ───────────────────────────────────────────────────────────
async def fetch_text(url: str) -> str:
    try:
        headers = {"User-Agent":"Mozilla/5.0"}
        async with httpx.AsyncClient(timeout=20, headers=headers) as c:
            r = await c.get(url, follow_redirects=True)
            r.raise_for_status()
            r.encoding = r.encoding or "utf-8"
            return r.text
    except Exception as e:
        log_err(f"抓取正文失败 {url} :: {e}")
        return ""

def extract_text(html: str) -> str:
    if not html: return ""
    soup = BeautifulSoup(html, "lxml")
    texts = []
    for tag in soup.find_all(["p", "article", "section", "div"]):
        t = tag.get_text(" ", strip=True)
        if t and len(t) > 20:
            texts.append(t)
    out = " ".join(texts)
    # 只保留中文/常见标点和少量空格，避免乱码
    out = re.sub(r"[^\u4e00-\u9fff，。！？；：、“”‘’《》·—\-0-9a-zA-Z\s]", " ", out)
    return re.sub(r"\s+", " ", out).strip()

def parse_feed(name: str, url: str) -> List[dict]:
    try:
        fp = feedparser.parse(url)
        items = []
        for e in fp.entries:
            items.append({
                "source": name,
                "title": e.get("title",""),
                "link": e.get("link",""),
                "published": e.get("published","") or e.get("updated",""),
                "summary": e.get("summary",""),
                "content": "",
            })
        return items
    except Exception as e:
        log_err(f"RSS 解析失败 {name} :: {e}")
        return []

# ── 匹配（标题+摘要+正文） ────────────────────────────────────────────────
def hit_keywords(text: str, kws: List[str]) -> bool:
    if not text: return False
    for k in kws:
        if k in text:
            return True
    return False

# ── 主流程 ────────────────────────────────────────────────────────────────
async def main():
    log("开始收集（近 3 天），调用 Qwen 生成补充关键词")
    holds = load_holdings()
    log(f"读取持仓：holdings.json 共 {len(holds)} 条")

    topics, base_kw = infer_topics_from_holdings(holds)
    log(f"基础关键词 {len(base_kw)} 个；行业：{', '.join(topics)}")

    qkw = await qwen_expand_keywords(topics, base_kw)
    # 合并关键词（只中文、≤4 字）
    all_kw = []
    seen = set()
    for w in itertools.chain(base_kw, qkw):
        if re.search(r"[\u4e00-\u9fff]", w) and 1 <= len(w) <= 4 and w not in seen:
            all_kw.append(w); seen.add(w)

    # 输出关键词文件
    with open(OUT_KW, "w", encoding="utf-8") as f:
        f.write("\n".join(all_kw))
    with open(OUT_SRC, "w", encoding="utf-8") as f:
        f.write("")  # 先清空，后续附加

    log(f"最终关键词 {len(all_kw)} 个，已写 {OUT_KW} / {OUT_QKW}")

    # 1) Google News RSS（中文）
    rss_urls = google_news_rss_urls(all_kw, batch_size=6) + DOMESTIC_RSS
    rss_items: List[dict] = []
    for name, url in rss_urls:
        items = parse_feed(name, url)
        rss_items.extend(items)
        with open(OUT_SRC, "a", encoding="utf-8") as f:
            f.write(f"[RSS] {name}\t{url}\t{len(items)}\n")
    log(f"RSS 抓到 {len(rss_items)} 条（未筛）")

    # 2) 备用 API（有 key 就上）
    api_items: List[dict] = []
    api_items += await fetch_newsapi(all_kw)
    api_items += await fetch_mediastack(all_kw)
    api_items += await fetch_juhe_caijing(all_kw)
    if api_items:
        with open(OUT_SRC, "a", encoding="utf-8") as f:
            f.write(f"[API] total\t{len(api_items)}\n")

    # 全量
    all_items = rss_items + api_items
    log(f"收集完成：全量 {len(all_items)} 条（未去重）")

    # 拉正文并过滤（中文关键词 ≥1 命中）
    kept: List[dict] = []
    sem = asyncio.Semaphore(8)

    async def enrich_and_filter(it: dict):
        async with sem:
            html = await fetch_text(it["link"])
            body = extract_text(html)
            it["content"] = body
            text = " ".join([it["title"], it["summary"], body])
            if hit_keywords(text, all_kw):
                kept.append(it)

    await asyncio.gather(*[enrich_and_filter(x) for x in all_items])
    log(f"正文/标题命中后保留 {len(kept)} 条（命中≥1 关键词）")

    # 写 CSV
    with open(OUT_NEWS, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["source","published","title","link","summary","content"])
        for it in kept:
            w.writerow([it["source"], it["published"], it["title"], it["link"], it["summary"], it["content"]])

    # 写 briefing.txt
    lines = []
    for i, it in enumerate(kept, 1):
        ts = it["published"] or ""
        lines.append(f"{i}. [{it['source']}] {it['title']}\n{it['link']}\n")
    open(OUT_BRI, "w", encoding="utf-8").write("\n".join(lines))
    log(f"已写 {OUT_BRI}、{OUT_NEWS}、{OUT_KW}、{OUT_SRC}")

    # 打包（给 workflow artifacts）
    try:
        import zipfile, glob
        zipname = f"logs-and-history-{datetime.now(TZ).strftime('%Y%m%d-%H%M%S')}.zip"
        with zipfile.ZipFile(zipname, "w", zipfile.ZIP_DEFLATED) as z:
            for p in [OUT_BRI, OUT_NEWS, OUT_KW, OUT_QKW, OUT_SRC, OUT_ERR]:
                if os.path.isfile(p):
                    z.write(p)
        log(f"打包 → {zipname}")
    except Exception as e:
        log_err(f"打包失败：{e}")

    log("collector 任务完成")

if __name__ == "__main__":
    asyncio.run(main())
