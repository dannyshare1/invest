# -*- coding: utf-8 -*-
"""
news_pipeline.py — 采集器（Google News + 多中文RSS + 聚合数据）→ 正文/标题命中过滤 → 日志与打包
用法：
    python news_pipeline.py

说明：
- 关键词来源：持仓名称 → 中文短词（兜底） + Qwen 生成短词（可选） + 英文行业同义词扩展（自动）
- 命中逻辑：只要「正文 或 标题」包含任一关键词即计为命中
- 来源：Google News（按词搜）+ 一揽子中文 RSS + 聚合数据（可选）
- 输出：
    keywords_used.txt / sources_used.txt / news_all.csv / briefing.txt / errors.log
    logs-and-history-YYYYMMDD-HHMMSS.zip（便于下载/工作流上传）
环境变量：
    QWEN_API_KEY （可选：用于生成短词）
    JUHE_KEY     （可选：聚合数据财经新闻）
依赖：见 requirements.txt
"""
from __future__ import annotations
import os, re, csv, json, asyncio, logging, zipfile
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import List, Dict, Any, Tuple
import httpx, feedparser
from urllib.parse import quote_plus
from dateutil import parser as dtparse
from bs4 import BeautifulSoup
from readability import Document

# ─────────── 常量 ───────────
TZ = ZoneInfo("Asia/Shanghai")
now = lambda: datetime.now(TZ).isoformat(timespec="seconds")
OUT_KW  = Path("keywords_used.txt")
OUT_SRC = Path("sources_used.txt")
OUT_CSV = Path("news_all.csv")
OUT_BRI = Path("briefing.txt")
OUT_ERR = Path("errors.log")
HOLDINGS = Path("holdings.json")
QWEN_API = "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation"
JUHE_API = "http://apis.juhe.cn/fapigx/caijing/query"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def logerr(msg: str, exc: Exception | None = None):
    try:
        with OUT_ERR.open("a", encoding="utf-8") as f:
            f.write(f"{now()} - {msg}\n")
            if exc:
                f.write(f"{type(exc).__name__}: {exc}\n")
    except Exception:
        pass

# ─────────── 基础工具 ───────────
def read_holdings() -> List[Dict[str, Any]]:
    if HOLDINGS.is_file():
        return json.loads(HOLDINGS.read_text("utf-8"))
    logging.warning("未找到 holdings.json，默认空列表")
    return []

def _cn_chunks(name: str) -> List[str]:
    # 从中文名称中取 2~4 字短片段（去常见后缀）
    name = re.sub(r"(ETF|指数|基金|沪深|中证|交易型|开放式|联接|A|C)", "", name, flags=re.I)
    words = set()
    for m in re.finditer(r"[\u4e00-\u9fa5]{2,4}", name):
        words.add(m.group(0))
    return list(words)

def base_keywords_from_holdings(holds: List[Dict[str, Any]]) -> Tuple[List[str], List[str]]:
    kw, tags = set(), set()
    for h in holds:
        nm = (h.get("name") or h.get("Name") or "").strip()
        for w in _cn_chunks(nm): kw.add(w)
        if any(x in nm for x in ("半导体","芯片","电子","科创")): tags.add("半导体")
        if any(x in nm for x in ("医药","医疗","生物")):         tags.add("医药")
        if any(x in nm for x in ("白酒","酒")):                  tags.add("白酒")
        if any(x in nm for x in ("债","国债","固收","利率")):     tags.add("债券")
        if any(x in nm for x in ("豆粕","农业","饲料")):          tags.add("农业")
        if "300" in nm or "沪深300" in nm:                      tags.add("宏观")
        if "红利" in nm:                                        tags.add("红利")
    if not tags: tags = {"宏观"}
    # 兜底短词，保证永不为空
    fallback = {"半导体","医药","白酒","农业","债券","利率","宏观","通胀","出口","消费","基建","新能源"}
    return sorted(kw | fallback), sorted(tags)

async def call_qwen_short_keywords(holds: List[Dict[str, Any]], base_kw: List[str]) -> List[str]:
    key = os.getenv("QWEN_API_KEY")
    if not key: return []
    names = "、".join([(h.get("name") or h.get("Name") or "").strip() for h in holds if (h.get("name") or h.get("Name"))])
    prompt = f"""根据这些ETF中文名称，生成与其行业/主题高度相关的**中文短关键词**（每个2~4字），用顿号或换行分隔。
避免过泛（如“经济”），更偏产业链环节/材料/工艺/政策要点等；最多100个。
ETF：{names}
参考短词（可部分吸收）：{'、'.join(base_kw[:30])}
只输出词表，不要任何解释。"""
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    payload = {"model":"qwen-plus","input":{"prompt":prompt},"parameters":{"max_tokens":500,"temperature":0.3}}
    async with httpx.AsyncClient(timeout=30) as c:
        r = await c.post(QWEN_API, headers=headers, json=payload)
        r.raise_for_status()
        text = r.json().get("output",{}).get("text","")
    parts = re.split(r"[、，,；;。\s\n]+", text)
    return [p.strip() for p in parts if 1 < len(p.strip()) <= 4 and re.search(r"[\u4e00-\u9fa5]", p)]

def expand_en_keywords(tags: List[str]) -> List[str]:
    # 根据行业标签添加英文同义词，解决“正文英文 → 中文词命中为0”的问题
    lib = {
        "半导体": ["semiconductor","chip","foundry","wafer","lithography","packaging","EDA","HBM","GPU","AI chip"],
        "医药":   ["pharma","biotech","drug","vaccine","clinical trial","GLP-1","MAH","gene therapy","CDMO","CRO"],
        "白酒":   ["baijiu","liquor","spirits","distillery","premium liquor","alcohol sales"],
        "农业":   ["agriculture","soybean","corn","grain","fertilizer","feed","hog","poultry"],
        "债券":   ["bond","yield","treasury","duration","credit spread","curve","issuance"],
        "宏观":   ["CPI","inflation","PMI","GDP","export","consumption","fiscal policy","monetary policy","central bank"],
        "红利":   ["dividend","payout","buyback","yield factor"]
    }
    out = []
    for t in tags:
        out += lib.get(t, [])
    # 常见英文兜底
    out += ["policy","subsidy","regulation","price","supply chain","merger","acquisition","guidance"]
    # 去重保序
    seen, res = set(), []
    for w in out:
        if w not in seen:
            res.append(w); seen.add(w)
    return res

def write_keywords_file(final_kw: List[Tuple[str,str]]):
    with OUT_KW.open("w", encoding="utf-8") as f:
        for w, tag in final_kw:
            f.write(f"{w}\t{tag}\n")

# ─────────── 抓取：Google News ───────────
def google_news_search(keyword: str, per_kw_limit: int = 30) -> List[Dict[str, Any]]:
    url = "https://news.google.com/rss/search?" + f"q={quote_plus(keyword)}&hl=zh-CN&gl=CN&ceid=CN:zh-Hans"
    feed = feedparser.parse(url)
    out = []
    for e in feed.entries[:per_kw_limit]:
        title = e.get("title","").strip()
        link = (e.get("link") or e.get("id") or "").strip()
        published = e.get("published") or e.get("updated") or ""
        try:
            ts = dtparse.parse(published).astimezone(TZ).isoformat(timespec="seconds") if published else ""
        except Exception:
            ts = ""
        out.append({"source":"GoogleNews","title":title,"url":link,"published":ts,"summary":e.get("summary","").strip(),"keyword":keyword})
    return out

# ─────────── 抓取：聚合数据 ───────────
async def juhe_finance_fetch(max_pages=5, page_size=50) -> List[Dict[str, Any]]:
    key = os.getenv("JUHE_KEY")
    if not key: return []
    out = []
    async with httpx.AsyncClient(timeout=20) as c:
        for page in range(1, max_pages+1):
            try:
                r = await c.get(JUHE_API, params={"key":key,"page":page,"pagesize":page_size})
                js = r.json()
                if js.get("error_code") == 0:
                    data = (js.get("result") or {}).get("data") or []
                else:
                    data = js.get("result") or js.get("data") or []
                for d in data:
                    title = str(d.get("title") or "").strip()
                    url = str(d.get("url") or d.get("source_url") or "").strip()
                    summary = str(d.get("content") or d.get("desc") or "").strip()
                    ptime = d.get("ptime") or d.get("pubdate") or d.get("ctime") or ""
                    try:
                        ts = dtparse.parse(str(ptime)).astimezone(TZ).isoformat(timespec="seconds") if ptime else ""
                    except Exception:
                        ts = ""
                    if title and url:
                        out.append({"source":"JuheFinance","title":title,"url":url,"published":ts,"summary":summary,"keyword":""})
            except Exception as e:
                logerr(f"聚合数据 第{page}页失败", e)
    return out

# ─────────── 抓取：多中文 RSS ───────────
RSS_SOURCES = [
    ("财联社-电报",     "https://rsshub.app/cls/telegraph"),
    ("财新-最新",       "https://rsshub.app/caixin/latest"),
    ("证券时报",        "https://rsshub.app/stcn/news"),
    ("华尔街见闻-快讯", "https://rsshub.app/wallstreetcn/live"),
    ("第一财经-最新",   "https://rsshub.app/yicai/latest"),
    ("36氪",            "https://36kr.com/feed"),
    ("澎湃新闻-首页",   "https://www.thepaper.cn/rss.jsp"),
    ("FT中文-新闻",     "https://www.ftchinese.com/rss/news"),
    ("界面新闻",        "https://a.jiemian.com/index.php?m=article&a=rss"),
    ("新浪财经-热门",   "https://rss.sina.com.cn/roll/finance/hot_roll.xml"),
    # 21财经不太稳，但保留
    ("21财经-股票",     "https://rsshub.app/21jingji/channel/stock"),
]

def fetch_rss_feed(name: str, url: str, limit: int = 80) -> List[Dict[str, Any]]:
    try:
        feed = feedparser.parse(url)
        out = []
        for e in feed.entries[:limit]:
            title = e.get("title","").strip()
            link = (e.get("link") or e.get("id") or "").strip()
            published = e.get("published") or e.get("updated") or ""
            try:
                ts = dtparse.parse(published).astimezone(TZ).isoformat(timespec="seconds") if published else ""
            except Exception:
                ts = ""
            out.append({"source":name,"title":title,"url":link,"published":ts,"summary":e.get("summary","").strip(),"keyword":""})
        return out
    except Exception as e:
        logerr(f"RSS {name} 失败: {url}", e)
        return []

# ─────────── 正文抓取与命中 ───────────
async def fetch_article_text(url: str, client: httpx.AsyncClient) -> str:
    try:
        r = await client.get(url, follow_redirects=True)
        html = r.text
        doc = Document(html)
        soup = BeautifulSoup(doc.summary(), "lxml")
        txt = soup.get_text(separator="\n").strip()
        if not txt:
            soup2 = BeautifulSoup(html, "lxml")
            txt = soup2.get_text(separator="\n")
        return re.sub(r"\s+\n", "\n", txt).strip()
    except Exception as e:
        logerr(f"正文抓取失败 {url}", e)
        return ""

def hits_in_text(text: str, kwset: set[str]) -> List[str]:
    if not text: return []
    t = text.lower()
    out=[]
    for k in kwset:
        if k.lower() in t:
            out.append(k)
    return out

# ─────────── 主流程 ───────────
async def main():
    print(f"{now()} - 开始收集（近 3 天），调用 Qwen 生成补充关键词")
    holds = read_holdings()
    print(f"{now()} - 读取持仓：holdings.json 共 {len(holds)} 条")
    base_kw, tags = base_keywords_from_holdings(holds)
    print(f"{now()} - 基础关键词 {len(base_kw)} 个；行业：{', '.join(tags)}")

    # LLM 中文短词（可失败，不阻断）
    llm_kw = []
    try:
        llm_kw = await call_qwen_short_keywords(holds, base_kw)
        logging.info(f"Qwen 生成关键词 {len(llm_kw)} 个")
    except Exception:
        logging.error("Qwen 调用失败")

    # 英文扩展
    en_kw = expand_en_keywords(tags)

    # 最终关键词（限 150 个，且永不为空）
    seen=set()
    final_kw: List[Tuple[str,str]]=[]
    for w in base_kw:
        if w and w not in seen:
            final_kw.append((w,"zh_base")); seen.add(w)
    for w in llm_kw:
        if w and w not in seen:
            final_kw.append((w,"zh_llm")); seen.add(w)
    for w in en_kw:
        if w and w not in seen:
            final_kw.append((w,"en_auto")); seen.add(w)
    if not final_kw:
        final_kw=[("宏观","fallback"),("利率","fallback"),("semiconductor","en_auto")]
    final_kw = final_kw[:150]
    write_keywords_file(final_kw)
    # 统计源
    src_stat = {}

    # Google News（取前 30 关键词，每词 ≤30 篇 → 最高 900）
    kw_for_gn = [w for w, _ in final_kw[:30]]
    gn_items=[]
    for kw in kw_for_gn:
        try:
            items = await asyncio.to_thread(google_news_search, kw, 30)
            gn_items += items
        except Exception as e:
            logerr(f"GoogleNews 关键词 {kw} 失败", e)
    src_stat["GoogleNews"] = len(gn_items)
    print(f"{now()} - GoogleNews 抓到 {len(gn_items)} 条（未筛）")

    # 多 RSS
    rss_items=[]
    for name,url in RSS_SOURCES:
        items = await asyncio.to_thread(fetch_rss_feed, name, url, 80)
        rss_items += items
        src_stat[name]=src_stat.get(name,0)+len(items)
    print(f"{now()} - RSS 抓到 {sum(len(await asyncio.to_thread(fetch_rss_feed, n,u,0)) for n,u in []) or len(rss_items)} 条（未筛）")

    # 聚合数据（可选）
    juhe_items = await juhe_finance_fetch(max_pages=5, page_size=50)
    src_stat["JuheFinance"]=len(juhe_items)
    print(f"{now()} - 聚合数据 抓到 {len(juhe_items)} 条（未筛）")

    all_items = gn_items + rss_items + juhe_items
    print(f"{now()} - 收集完成：全量 {len(all_items)} 条（未去重）")

    # 写 sources_used.txt
    with OUT_SRC.open("w", encoding="utf-8") as f:
        for k,v in src_stat.items():
            f.write(f"{k}\t{v}\n")

    # 正文/标题命中过滤
    kwset=set([w for w,_ in final_kw])
    kept=[]
    sem = asyncio.Semaphore(10)
    async with httpx.AsyncClient(timeout=15) as client:
        async def work(item):
            async with sem:
                body = await fetch_article_text(item["url"], client)
                title = item.get("title","")
                # 命中：正文 或 标题
                hits = set(hits_in_text(body, kwset) + hits_in_text(title, kwset))
                item["kw_hits"]="、".join(hits)
                item["content_len"]=len(body)
                return item
        tasks=[work(it) for it in all_items]
        for fut in asyncio.as_completed(tasks):
            try:
                kept.append(await fut)
            except Exception as e:
                logerr("正文过滤任务失败", e)

    # 写 CSV（全量）
    cols=["source","title","url","published","summary","keyword","kw_hits","content_len"]
    with OUT_CSV.open("w", encoding="utf-8", newline="") as f:
        w=csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for it in kept:
            w.writerow({c: it.get(c,"") for c in cols})

    hit_count = sum(1 for it in kept if it.get("kw_hits"))
    print(f"{now()} - 正文/标题命中后保留 {hit_count} 条")

    # briefing
    lines=[
        f"时间：{now()}",
        f"全量：{len(all_items)}；命中：{hit_count}；关键词：{len(final_kw)}",
        "—— Top 30 ——"
    ]
    for it in kept[:30]:
        mark="✅" if it.get("kw_hits") else "—"
        title=(it.get("title") or "").replace("\n"," ").strip()
        src=it.get("source") or ""
        ts=it.get("published") or ""
        lines.append(f"{mark} {title} [{src}] {ts}")
    OUT_BRI.write_text("\n".join(lines), encoding="utf-8")
    print(f"{now()} - 已写 briefing.txt、news_all.csv、keywords_used.txt、sources_used.txt")

    # 打包 logs-and-history
    zipname=f"logs-and-history-{datetime.now(TZ).strftime('%Y%m%d-%H%M%S')}.zip"
    with zipfile.ZipFile(zipname, "w", zipfile.ZIP_DEFLATED) as z:
        for p in [OUT_KW, OUT_SRC, OUT_CSV, OUT_BRI, OUT_ERR]:
            if p.exists():
                z.write(p, arcname=p.name)
    sz = OUT_ERR.stat().st_size if OUT_ERR.exists() else 0
    print(f"{now()} - errors.log 大小 {sz} bytes；打包 → {zipname}")
    print(f"{now()} - collector 任务完成")

if __name__ == "__main__":
    asyncio.run(main())
