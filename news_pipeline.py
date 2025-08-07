# -*- coding: utf-8 -*-
"""
news_pipeline.py  r11 — 按新需求抓取
1. Tushare 官网 7 页 + 子标签（国际/其他）全部抓，按时间排序
2. 其他 RSS / API 仅筛选【持仓关键词】，日期 ≤7 天
3. 不去重、不推送；全部新闻写 news_all.csv，日志关键字写 keywords_used.txt
"""
from __future__ import annotations
import asyncio, httpx, json, csv, os, re, logging, html, sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from dataclasses import dataclass, asdict
from bs4 import BeautifulSoup
from dateutil import parser as dtparse

# ─── 基本配置 ───
TZ = timezone(timedelta(hours=8))
TODAY = datetime.now(TZ).strftime("%Y%m%d")
OUT_CSV = Path("news_all.csv")
KW_FILE = Path("keywords_used.txt")
LOG = Path("pipeline.log")

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s",
                    handlers=[logging.FileHandler(LOG, "a", "utf-8"),
                              logging.StreamHandler(sys.stdout)])

@dataclass
class News:
    t: str       # title
    s: str       # summary
    u: str       # url
    ts: str      # ISO time
    src: str     # source
    def row(self): return [self.ts, self.src, self.t, self.s, self.u]

# ─── 辅助函数 ───
def strip_html(x:str): return re.sub(r"<[^>]+>", "", html.unescape(x)).strip()

async def fetch_tushare_page(tag: str) -> list[News]:
    """抓取 tushare.pro/news/{tag} 页面正文列表，含 #国际 #其他 子页"""
    url = f"https://tushare.pro/news/{tag}"
    r = await httpx.AsyncClient().get(url, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")
    out: list[News] = []
    for li in soup.select("ul.news-list li"):
        a = li.select_one("a")
        if not a:
            continue
        ttl = a.get_text(strip=True)
        link = a["href"]
        tm_txt = li.select_one(".time")
        if not tm_txt:
            continue
        ts = dtparse.parse(tm_txt.get_text(strip=True)).astimezone(TZ).isoformat()
        cat = soup.select_one(".news-title h1").get_text(strip=True) if soup.select_one(".news-title h1") else tag
        out.append(News(ttl, "", link, ts, f"TS-{tag}/{cat}"))
    return out

async def fetch_rss(url: str, src: str) -> list[News]:
    r = await httpx.AsyncClient().get(url, timeout=20)
    if r.status_code != 200:
        logging.warning(f"{src} HTTP {r.status_code}")
        return []
    from xml.etree import ElementTree as ET
    root = ET.fromstring(r.text)
    out = []
    for item in root.findall(".//item"):
        ttl = strip_html(item.findtext("title") or "")
        link = item.findtext("link") or ""
        desc = strip_html(item.findtext("description") or "")
        pub_raw = item.findtext("pubDate") or ""
        try:
            pub = dtparse.parse(pub_raw).astimezone(TZ)
        except Exception:
            pub = datetime.now(TZ)
        out.append(News(ttl, desc[:180], link, pub.isoformat(), src))
    return out

def load_holdings_kw() -> list[str]:
    fp = Path("holdings.json")
    if not fp.is_file():
        return []
    data = json.loads(fp.read_text("utf-8"))
    kws = []
    for h in data:
        kws.extend([h["symbol"].split(".")[0], h["name"]])
    return list(set(kws))

def within_7d(iso_ts: str) -> bool:
    try:
        dt = dtparse.isoparse(iso_ts)
    except Exception:
        return False
    return dt >= datetime.now(TZ) - timedelta(days=7)

def kw_match(news: News, kw_set: set[str]) -> bool:
    txt = f"{news.t} {news.s}".lower()
    return any(k.lower() in txt for k in kw_set)

# ─── 主流程 ───
async def main():
    kws = load_holdings_kw()
    kw_set = set(kws)
    KW_FILE.write_text("\n".join(kws), "utf-8")

    pages = ['fenghuang','jinrongjie','10jqka','sina',
             'yuncaijing','eastmoney','wallstreetcn']

    tasks = [fetch_tushare_page(p) for p in pages] + [
        fetch_rss("https://rsshub.app/cls/telegraph", "财联社"),
        fetch_rss("https://rss.sina.com.cn/roll/finance/hot_roll.xml", "新浪财经")
    ]

    all_news: list[News] = []
    res = await asyncio.gather(*tasks, return_exceptions=True)
    for r in res:
        if isinstance(r, Exception):
            logging.exception(r)
            continue
        all_news.extend(r)

    # 只对 RSS 源做关键词 & 7d 过滤；Tushare 页面全部保留
    filtered = []
    for n in all_news:
        if n.src.startswith("TS-"):
            filtered.append(n)
        elif kw_match(n, kw_set) and within_7d(n.ts):
            filtered.append(n)

    # 时间倒序
    filtered.sort(key=lambda x: x.ts, reverse=True)

    # 写 CSV
    with OUT_CSV.open("w", newline="", encoding="utf-8-sig") as f:
        csv.writer(f).writerows([["time", "source", "title", "summary", "url"],
                                 *[n.row() for n in filtered]])

    logging.info(f"抓取完成，共 {len(filtered)} 条；已写入 {OUT_CSV}")

if __name__ == "__main__":
    asyncio.run(main())
