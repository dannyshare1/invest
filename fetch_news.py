# -*- coding: utf-8 -*-
"""
Reuters · Yahoo Finance · FT  +  中国证券报 · 新华社财经 · 21财经 · 上证报
→ 汇总为 news.json（含 200-300 字摘要）
依赖：requests beautifulsoup4 lxml chardet
"""
import os, json, datetime, re, hashlib, html as ihtml, requests, chardet
from bs4 import BeautifulSoup

# -------- 全局参数 -------- #
MAX_PER_SRC = 10          # 每源最多保留
TIMEOUT     = 12
ua_hdr      = {"User-Agent": "Mozilla/5.0"}

today = datetime.date.today()

# —— 实用函数 —— #
def tidy(resp: requests.Response) -> str:
    if resp.encoding == "ISO-8859-1":
        resp.encoding = chardet.detect(resp.content)["encoding"] or "utf-8"
    return resp.text

def first_paras(html: str, css: str, n: int = 3, cap: int = 300) -> str:
    soup = BeautifulSoup(html, "lxml")
    paras = [p.get_text(" ", strip=True) for p in soup.select(css)[:n]]
    txt = " ".join(paras) or soup.get_text(" ", strip=True)
    return ihtml.unescape(txt)[:cap]

def norm(txt: str) -> str:
    return re.sub(r"[^\w\u4e00-\u9fa5]", "", txt).lower()

# 关键词（长度 >1）
INV_KWS = [w.lower() for w in """
股票 股市 指数 证券 分红 回购 半导体 芯片 AI 宏观 业绩 盈利
bull bear dividend buyback earnings volatility yield inflation
""".split() if len(w) > 1]

def related(t: str) -> bool:
    txt = norm(t)
    return any(k in txt for k in INV_KWS)

# —— 汇总容器 —— #
items, seen = [], set()
def add(title, snippet, src, date, origin):
    if not title or len(snippet) < 30 or not related(title + snippet):
        return
    key = hashlib.md5(f"{title}_{date}".encode()).hexdigest()
    if key in seen: return
    seen.add(key)
    items.append({
        "title":   title.strip(),
        "snippet": snippet.strip(),
        "source":  src,
        "published": date[:10],
        "origin":  origin
    })

# -------- A. 国际财经 RSS -------- #
RSS_INTL = [
    ("Reuters", "https://feeds.reuters.com/reuters/businessNews"),
    ("YahooFinance", "https://feeds.finance.yahoo.com/rss/2.0/headline?s=%5EGSPC&region=US&lang=en-US"),
    ("FT", "https://www.ft.com/?format=rss")
]

for src, url in RSS_INTL:
    try:
        xml = requests.get(url, headers=ua_hdr, timeout=TIMEOUT).text
        for itm in BeautifulSoup(xml, "xml").find_all("item")[:MAX_PER_SRC]:
            title = itm.title.get_text(strip=True)
            link  = itm.link.get_text(strip=True)
            desc  = BeautifulSoup(itm.description.get_text(), "lxml").get_text(" ", strip=True)
            add(title, desc, src, today.isoformat(), "INT_RSS")
    except Exception as e:
        print(src, "RSS error:", e)

# -------- B. 国内财经 JSON / RSS -------- #
RSS_CN = [
    # 中国证券报快讯
    ("中国证券报", "https://rsshub.app/cs/news"),
    # 新华社财经
    ("新华社财经", "https://rsshub.app/xinhua/finance"),
    # 21财经滚动
    ("21世纪经济报道", "https://rsshub.app/21jingji/rolling"),
    # 上海证券报要闻
    ("上海证券报", "https://rsshub.app/stcn/kuaixun")
]

for src, url in RSS_CN:
    try:
        xml = requests.get(url, headers=ua_hdr, timeout=TIMEOUT).text
        for itm in BeautifulSoup(xml, "xml").find_all("item")[:MAX_PER_SRC]:
            title = itm.title.get_text(strip=True)
            link  = itm.link.get_text(strip=True)
            # 抓正文首段
            art = tidy(requests.get(link, headers=ua_hdr, timeout=TIMEOUT))
            snp = first_paras(art, "article p, div.article p, p")
            add(title, snp, src, today.isoformat(), "CN_RSS")
    except Exception as e:
        print(src, "RSS error:", e)

# -------- C. 保存 -------- #
with open("news.json", "w", encoding="utf-8") as f:
    json.dump(items, f, ensure_ascii=False, indent=2)

print(f"✅ 输出 {len(items)} 条 → news.json")
