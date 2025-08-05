# -*- coding: utf-8 -*-
"""
Reuters + Yahoo Finance + FT  和  中证报 · 新华社财经 · 21财经 · 上证报
→ 生成 news.json（含 300 字摘要）
"""
import json, requests, datetime, hashlib, re, html as ihtml, chardet
from bs4 import BeautifulSoup

# ---------- 参数 ----------
MAX_PER = 10
TIMEOUT = 12
UA      = {"User-Agent": "Mozilla/5.0"}
today   = datetime.date.today()

# ---------- 关键词 ----------
KWS = [w.lower() for w in """
股票 股市 指数 证券 分红 回购 半导体 芯片 AI 宏观 业绩 盈利
bull bear dividend buyback earnings volatility inflation
""".split() if len(w) > 1]

def related(txt: str) -> bool:
    txt = re.sub(r"[^\w\u4e00-\u9fa5]", "", txt.lower())
    return any(k in txt for k in KWS)

# ---------- 工具 ----------
def tidy(resp):
    if resp.encoding == "ISO-8859-1":
        resp.encoding = chardet.detect(resp.content)["encoding"] or "utf-8"
    return resp.text

def snippet(html, css="article p, div.article p, p", n=4, cap=300):
    soup = BeautifulSoup(html, "lxml")
    seg  = [p.get_text(" ", strip=True) for p in soup.select(css)[:n]]
    txt  = " ".join(seg) or soup.get_text(" ", strip=True)
    return ihtml.unescape(txt)[:cap]

def hid(title, date):
    return hashlib.md5(f"{title}_{date}".encode()).hexdigest()

news, seen = [], set()
def add(title, snip, src):
    if not title or len(snip) < 40 or not related(title + snip):
        return
    key = hid(title, today.isoformat())
    if key in seen: return
    seen.add(key)
    news.append({
        "title": title.strip(),
        "snippet": snip.strip(),
        "source": src,
        "published": today.isoformat(),
    })

# ---------- A. 国际 RSS ----------
INTL_RSS = [
    ("Reuters", "https://www.reuters.com/rssFeed/businessNews"),
    ("YahooFinance", "https://feeds.finance.yahoo.com/rss/2.0/headline?s=%5EGSPC&region=US&lang=en-US"),
    ("FT", "https://www.ft.com/?format=rss")
]
for src, url in INTL_RSS:
    try:
        xml = requests.get(url, headers=UA, timeout=TIMEOUT).text
        for itm in BeautifulSoup(xml, "xml").find_all("item")[:MAX_PER]:
            title = itm.title.get_text(strip=True)
            desc  = BeautifulSoup(itm.description.get_text(), "lxml").get_text(" ", strip=True)
            add(title, desc, src)
    except Exception as e:
        print(src, "RSS error:", e)

# ---------- B. 国内 HTML ----------
DOMESTIC = [
    ("中国证券报",  "https://www.cs.com.cn/xwzx/hg/"),
    ("新华社财经",  "https://www.news.cn/fortune/"),
    ("21财经",     "https://www.21jingji.com/list/stock"),
    ("上海证券报",  "https://news.cnstock.com/news/")
]
for src, list_url in DOMESTIC:
    try:
        lst = tidy(requests.get(list_url, headers=UA, timeout=TIMEOUT))
        links = [a["href"] for a in BeautifulSoup(lst, "lxml").select("a[href]") if a["href"].startswith("http")][:MAX_PER]
        for lk in links:
            page = tidy(requests.get(lk, headers=UA, timeout=TIMEOUT))
            soup = BeautifulSoup(page, "lxml")
            title = soup.title.get_text(strip=True)
            snip  = snippet(page)
            add(title, snip, src)
    except Exception as e:
        print(src, "列表解析失败:", e)

# ---------- 保存 ----------
with open("news.json", "w", encoding="utf-8") as f:
    json.dump(news, f, ensure_ascii=False, indent=2)

print(f"✅ 国际 {len([n for n in news if n['source'] in dict(INTL_RSS)])} 条，"
      f"国内 {len(news) - len([n for n in news if n['source'] in dict(INTL_RSS)])} 条 "
      f"→ 共 {len(news)} 条写入 news.json")
