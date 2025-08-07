# -*- coding: utf-8 -*-
"""
news_pipeline.py r12
• 带 Cookie 抓 tushare.pro 资讯 7 页 (+国际/其他)
• ETF → 前 10 大成份股名 做关键词
• RSS 仅保留关键词 & 7 日内
• 所有结果写 news_all.csv，keywords_used.txt
"""
from __future__ import annotations
import asyncio, httpx, os, json, csv, re, logging, html, sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from bs4 import BeautifulSoup
from dateutil import parser as dtparse

TZ = timezone(timedelta(hours=8))
TODAY = datetime.now(TZ).strftime("%Y%m%d")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s",
                    handlers=[logging.FileHandler("pipeline.log", "a", "utf-8"),
                              logging.StreamHandler(sys.stdout)])

# ── 抓 tushare 网页（需要登录 Cookie） ──
TS_COOKIE = os.getenv("TUSHARE_COOKIE")  # 在 GitHub Secrets 里添加你的浏览器 ts_token=xxx
HEADERS = {"Cookie": TS_COOKIE} if TS_COOKIE else {}

async def ts_page(tag: str) -> list[dict]:
    url = f"https://tushare.pro/news/{tag}"
    r = await httpx.AsyncClient().get(url, headers=HEADERS, timeout=20, follow_redirects=False)
    if r.status_code == 302:
        logging.error(f"{tag} 需要登录，未抓取")
        return []
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    cat = soup.select_one(".news-title h1").get_text(strip=True) if soup.select_one(".news-title h1") else tag
    out = []
    for li in soup.select("ul.news-list li"):
        a = li.select_one("a")
        tm = li.select_one(".time")
        if not (a and tm): continue
        ts = dtparse.parse(tm.get_text(strip=True)).astimezone(TZ).isoformat()
        out.append(dict(title=a.get_text(strip=True),
                        summary="",
                        url=a["href"],
                        time=ts,
                        src=f"TS-{tag}/{cat}"))
    return out

# ── RSS 抓取 ──
def strip(x): return re.sub(r"<[^>]+>", "", html.unescape(x)).strip()

async def rss(url:str, src:str) -> list[dict]:
    r = await httpx.AsyncClient().get(url, timeout=20)
    if r.status_code != 200:
        logging.warning(f"{src} HTTP {r.status_code}")
        return []
    import xml.etree.ElementTree as ET
    root = ET.fromstring(r.text)
    out=[]
    for it in root.findall(".//item"):
        ttl=strip(it.findtext("title") or "")
        link=it.findtext("link") or ""
        desc=strip(it.findtext("description") or "")[:180]
        pub_raw=it.findtext("pubDate") or ""
        try: pub=dtparse.parse(pub_raw).astimezone(TZ).isoformat()
        except: pub=datetime.now(TZ).isoformat()
        out.append(dict(title=ttl, summary=desc, url=link, time=pub, src=src))
    return out

# ── 提取关键词：ETF 前 10 重仓股 ──
def load_keywords() -> list[str]:
    fp=Path("holdings.json")
    if not fp.is_file(): return []
    data=json.loads(fp.read_text("utf-8"))
    kws=set()
    for h in data:
        comp = h.get("components", [])[:10]   # 预留字段 components，如无则跳过
        kws.update(comp)
    kw_list=sorted(kws)
    Path("keywords_used.txt").write_text("\n".join(kw_list),"utf-8")
    return kw_list

def kw_filter(news, kw_set):
    txt=(news["title"]+" "+news["summary"]).lower()
    return any(k.lower() in txt for k in kw_set)

def in_7d(iso):
    try: dt=dtparse.isoparse(iso).astimezone(TZ)
    except: return False
    return dt >= datetime.now(TZ)-timedelta(days=7)

# ── 主流程 ──
async def main():
    kw_set=set(load_keywords())
    tags=['fenghuang','jinrongjie','10jqka','sina','yuncaijing','eastmoney','wallstreetcn']
    tasks=[ts_page(t) for t in tags] + [
        rss("https://rsshub.app/cls/telegraph","财联社"),
        rss("https://rss.sina.com.cn/roll/finance/hot_roll.xml","新浪财经")
    ]
    res=await asyncio.gather(*tasks, return_exceptions=True)
    all_news=[]
    for r in res:
        if isinstance(r, Exception):
            logging.exception(r); continue
        all_news.extend(r)

    # 过滤 RSS 根据关键词 & 7d
    filtered=[]
    for n in all_news:
        if n["src"].startswith("TS-"):
            filtered.append(n)
        elif kw_filter(n, kw_set) and in_7d(n["time"]):
            filtered.append(n)

    # 按时间倒序
    filtered.sort(key=lambda x: x["time"], reverse=True)

    with Path("news_all.csv").open("w",newline="",encoding="utf-8-sig") as f:
        csv.writer(f).writerows([["time","source","title","summary","url"],
                                 *[[n["time"],n["src"],n["title"],n["summary"],n["url"]] for n in filtered]])
    logging.info(f"共抓 {len(filtered)} 条，已写 news_all.csv")

if __name__=="__main__":
    asyncio.run(main())
