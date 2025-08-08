# -*- coding: utf-8 -*-
"""
news_pipeline.py — 采集器（Baidu 优先 + Bing/Google 回退 + 多中文 RSS + 聚合数据）
流程：
  1) 从 holdings.json 读持仓 → 生成基础中文短词 & 行业标签
  2) 调 Qwen 追加 2~4 字中文短词（失败不阻断）
  3) 英文同义词扩展（补贴 Google/Bing 英文正文）
  4) 采集：
       - Baidu News（RSSHub /baidu/search/{kw}，失败回退到 Bing News RSS）
       - 多中文 RSS（财新/一财/证券时报/华尔街见闻/36氪/澎湃/界面/新浪等）
       - 聚合数据（可选 JUHE_KEY）
       - （可选）Google News（设 USE_GOOGLE=1 才会启用）
  5) 命中规则：「标题 或 摘要」先筛；未命中再抓正文（readability）补筛
  6) 输出：news_all.csv / briefing.txt / keywords_used.txt / sources_used.txt / errors.log
           以及 logs-and-history-YYYYMMDD-HHMMSS.zip
环境变量：
  QWEN_API_KEY（可选）、JUHE_KEY（可选）、USE_GOOGLE=0/1（默认 0）
"""

from __future__ import annotations
import os, re, csv, json, asyncio, zipfile, logging
from typing import List, Dict, Any, Tuple
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from urllib.parse import quote_plus

import httpx, feedparser
from dateutil import parser as dtparse
from bs4 import BeautifulSoup
from readability import Document

# ─────────── 全局与输出 ───────────
TZ = ZoneInfo("Asia/Shanghai")
def now(): return datetime.now(TZ).isoformat(timespec="seconds")

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
            if exc: f.write(f"{type(exc).__name__}: {exc}\n")
    except Exception:
        pass

# ─────────── 基础：持仓→关键词 ───────────
def read_holdings() -> List[Dict[str, Any]]:
    if HOLDINGS.is_file():
        return json.loads(HOLDINGS.read_text("utf-8"))
    logging.warning("未找到 holdings.json，默认空列表")
    return []

def _cn_chunks(name: str) -> List[str]:
    name = (name or "").strip()
    name = re.sub(r"(ETF|指数|基金|沪深|中证|国债|交易型|开放式|联接|A|C)", "", name, flags=re.I)
    words=set()
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
    # 最小兜底，避免空
    fallback = {"半导体","医药","白酒","农业","债券","利率","宏观","通胀","消费","出口","投资","财政","货币"}
    return sorted(kw | fallback), sorted(tags)

async def call_qwen_short_keywords(holds: List[Dict[str, Any]], base_kw: List[str]) -> List[str]:
    key = os.getenv("QWEN_API_KEY")
    if not key: return []
    names = "、".join([(h.get("name") or h.get("Name") or "").strip() for h in holds if (h.get("name") or h.get("Name"))])
    prompt = f"""根据这些ETF中文名称，生成与其行业/主题高度相关的**中文短关键词**（每个2~4字），用顿号或换行分隔；避免“经济/市场”这类过泛词，聚焦产业链/材料/工艺/政策。最多100个。
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
    lib = {
        "半导体": ["semiconductor","chip","foundry","wafer","lithography","packaging","EDA","HBM","GPU","AI chip"],
        "医药":   ["pharma","biotech","drug","vaccine","clinical trial","GLP-1","MAH","gene therapy","CDMO","CRO"],
        "白酒":   ["baijiu","liquor","spirits","distillery","premium liquor"],
        "农业":   ["agriculture","soybean","corn","grain","fertilizer","feed","hog","poultry"],
        "债券":   ["bond","yield","treasury","duration","credit spread","curve","issuance"],
        "宏观":   ["CPI","inflation","PMI","GDP","export","consumption","fiscal policy","monetary policy","central bank"],
        "红利":   ["dividend","payout","buyback","yield factor"],
    }
    out=[]
    for t in tags: out+=lib.get(t,[])
    out+=["policy","subsidy","regulation","price","supply chain","merger","acquisition","guidance"]
    seen=set(); res=[]
    for w in out:
        if w not in seen:
            res.append(w); seen.add(w)
    return res

def write_keywords_file(final_kw: List[Tuple[str,str]]):
    with OUT_KW.open("w", encoding="utf-8") as f:
        for w, tag in final_kw:
            f.write(f"{w}\t{tag}\n")

# ─────────── 新闻抓取器 ───────────

def parse_feed(url: str, source_name: str, limit: int = 80) -> List[Dict[str, Any]]:
    feed = feedparser.parse(url)
    out=[]
    for e in feed.entries[:limit]:
        title = (e.get("title") or "").strip()
        link  = (e.get("link") or e.get("id") or "").strip()
        published = e.get("published") or e.get("updated") or ""
        try:
            ts = dtparse.parse(published).astimezone(TZ).isoformat(timespec="seconds") if published else ""
        except Exception:
            ts = ""
        out.append({"source":source_name,"title":title,"url":link,"published":ts,"summary":(e.get("summary") or "").strip(),"keyword":""})
    return out

async def baidu_or_bing_news(keyword: str, per_kw_limit: int = 30) -> List[Dict[str, Any]]:
    # 优先：RSSHub 的 Baidu 搜索；回退：Bing News RSS（带 zh-cn）
    try:
        url_baidu = f"https://rsshub.app/baidu/search/{quote_plus(keyword)}"
        items = parse_feed(url_baidu, "BaiduNews", per_kw_limit)
        if items: return items
    except Exception as e:
        logerr(f"BaiduNews 关键词 {keyword} 失败", e)
    # 回退 Bing
    try:
        url_bing = f"https://www.bing.com/news/search?q={quote_plus(keyword)}&format=rss&setlang=zh-cn"
        return parse_feed(url_bing, "BingNews", per_kw_limit)
    except Exception as e:
        logerr(f"BingNews 关键词 {keyword} 失败", e)
        return []

def google_news_search(keyword: str, per_kw_limit: int = 30) -> List[Dict[str, Any]]:
    url = "https://news.google.com/rss/search?" + f"q={quote_plus(keyword)}&hl=zh-CN&gl=CN&ceid=CN:zh-Hans"
    return parse_feed(url, "GoogleNews", per_kw_limit)

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
]

def fetch_rss_feed(name: str, url: str, limit: int = 80) -> List[Dict[str, Any]]:
    try:
        return parse_feed(url, name, limit)
    except Exception as e:
        logerr(f"RSS {name} 失败: {url}", e)
        return []

async def juhe_finance_fetch(max_pages=5, page_size=50) -> List[Dict[str, Any]]:
    key = os.getenv("JUHE_KEY")
    if not key: return []
    out=[]
    async with httpx.AsyncClient(timeout=20) as c:
        for page in range(1, max_pages+1):
            try:
                r = await c.get(JUHE_API, params={"key":key,"page":page,"pagesize":page_size})
                js = r.json()
                data = []
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

# ─────────── 正文抓取与命中 ───────────
async def fetch_article_text(url: str, client: httpx.AsyncClient) -> str:
    try:
        r = await client.get(url, follow_redirects=True, timeout=15)
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
    low = text.lower()
    return [k for k in kwset if k.lower() in low]

# ─────────── 主流程 ───────────
async def main():
    print(f"{now()} - 开始收集（近 3 天），调用 Qwen 生成补充关键词")
    holds = read_holdings()
    print(f"{now()} - 读取持仓：holdings.json 共 {len(holds)} 条")
    base_kw, tags = base_keywords_from_holdings(holds)
    print(f"{now()} - 基础关键词 {len(base_kw)} 个；行业：{', '.join(tags)}")

    # Qwen 追加中文短词（不阻断）
    llm_kw=[]
    try:
        llm_kw = await call_qwen_short_keywords(holds, base_kw)
        logging.info(f"Qwen 生成关键词 {len(llm_kw)} 个")
    except Exception:
        logging.error("Qwen 调用失败")

    en_kw = expand_en_keywords(tags)

    # 汇总关键词（≤150）
    seen=set(); final_kw: List[Tuple[str,str]]=[]
    for w in base_kw:
        if w and w not in seen: final_kw.append((w,"zh_base")); seen.add(w)
    for w in llm_kw:
        if w and w not in seen: final_kw.append((w,"zh_llm"));  seen.add(w)
    for w in en_kw:
        if w and w not in seen: final_kw.append((w,"en_auto")); seen.add(w)
    if not final_kw:
        final_kw=[("宏观","fallback"),("利率","fallback"),("semiconductor","en_auto")]
    final_kw=final_kw[:150]
    write_keywords_file(final_kw)

    src_stat={}
    # —— Baidu（回退 Bing）按词抓
    kw_for_search = [w for w,_ in final_kw[:30]]
    baidu_items=[]
    for kw in kw_for_search:
        try:
            items = await baidu_or_bing_news(kw, per_kw_limit=30)
            for it in items: it["keyword"]=kw
            baidu_items += items
        except Exception as e:
            logerr(f"Baidu/Bing 关键词 {kw} 失败", e)
    got_baidu = len(baidu_items)
    src_stat["Baidu/Bing"]=got_baidu
    print(f"{now()} - Baidu/Bing 抓到 {got_baidu} 条（未筛）")

    # （可选）Google
    gn_items=[]
    if os.getenv("USE_GOOGLE","0") == "1":
        for kw in kw_for_search:
            try:
                items = await asyncio.to_thread(google_news_search, kw, 30)
                for it in items: it["keyword"]=kw
                gn_items += items
            except Exception as e:
                logerr(f"GoogleNews 关键词 {kw} 失败", e)
        src_stat["GoogleNews"]=len(gn_items)
        print(f"{now()} - GoogleNews 抓到 {len(gn_items)} 条（未筛）")

    # 多 RSS
    rss_items=[]
    for name,url in RSS_SOURCES:
        items = await asyncio.to_thread(fetch_rss_feed, name, url, 80)
        rss_items += items
        src_stat[name]=src_stat.get(name,0)+len(items)
    print(f"{now()} - RSS 抓到 {len(rss_items)} 条（未筛）")   # ← 修复：不再错误 await 生成器

    # 聚合数据（可选）
    juhe_items = await juhe_finance_fetch(max_pages=5, page_size=50)
    src_stat["JuheFinance"]=len(juhe_items)
    print(f"{now()} - 聚合数据 抓到 {len(juhe_items)} 条（未筛）")

    # 汇总
    all_items = baidu_items + gn_items + rss_items + juhe_items
    print(f"{now()} - 收集完成：全量 {len(all_items)} 条（未去重）")

    # 写 sources_used.txt
    with OUT_SRC.open("w", encoding="utf-8") as f:
        for k,v in src_stat.items():
            f.write(f"{k}\t{v}\n")

    # —— 命中过滤：先看 标题/摘要；不命中再抓 正文
    kwset=set([w for w,_ in final_kw])
    kept=[]
    prelim=[]
    for it in all_items:
        title = it.get("title") or ""
        summ  = it.get("summary") or ""
        hits = set(hits_in_text(title, kwset) + hits_in_text(summ, kwset))
        if hits:
            it["kw_hits"]="、".join(sorted(hits))
            it["content_len"]=0
            prelim.append(it)
        else:
            kept.append(it)  # 暂存，等正文再筛
    # 正文筛
    sem = asyncio.Semaphore(10)
    body_kept=[]
    async with httpx.AsyncClient() as client:
        async def work(item):
            async with sem:
                body = await fetch_article_text(item["url"], client)
                hits = set(hits_in_text(body, kwset))
                item["kw_hits"]="、".join(sorted(hits))
                item["content_len"]=len(body)
                return item
        tasks=[work(it) for it in kept]
        results=[]
        for fut in asyncio.as_completed(tasks):
            try:
                results.append(await fut)
            except Exception as e:
                logerr("正文过滤任务失败", e)
        body_kept=results

    final_items = prelim + [it for it in body_kept if it.get("kw_hits")]
    # 写 CSV（全量，包含未命中的正文长度与命中词便于复盘）
    cols=["source","title","url","published","summary","keyword","kw_hits","content_len"]
    with OUT_CSV.open("w", encoding="utf-8", newline="") as f:
        w=csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for it in (prelim + body_kept):   # 全量都写入，命中词/正文长度做参考
            w.writerow({c: it.get(c,"") for c in cols})

    hit_count = sum(1 for it in final_items if it.get("kw_hits"))
    print(f"{now()} - 正文/标题命中后保留 {hit_count} 条（命中≥1 关键词）")

    # briefing（前 30）
    lines=[
        f"时间：{now()}",
        f"全量：{len(all_items)}；命中：{hit_count}；关键词：{len(final_kw)}",
        "—— Top 30 ——"
    ]
    for it in final_items[:30]:
        title=(it.get("title") or "").replace("\n"," ").strip()
        src=it.get("source") or ""
        ts=it.get("published") or ""
        lines.append(f"✅ {title} [{src}] {ts}")
    OUT_BRI.write_text("\n".join(lines), encoding="utf-8")
    print(f"{now()} - 已写 briefing.txt、news_all.csv、keywords_used.txt、sources_used.txt")

    # 打包
    zipname=f"logs-and-history-{datetime.now(TZ).strftime('%Y%m%d-%H%M%S')}.zip"
    with zipfile.ZipFile(zipname, "w", zipfile.ZIP_DEFLATED) as z:
        for p in [OUT_KW, OUT_SRC, OUT_CSV, OUT_BRI, OUT_ERR]:
            if p.exists(): z.write(p, arcname=p.name)
    sz = OUT_ERR.stat().st_size if OUT_ERR.exists() else 0
    print(f"{now()} - errors.log 大小 {sz} bytes；打包 → {zipname}")
    print(f"{now()} - collector 任务完成")

if __name__ == "__main__":
    asyncio.run(main())
