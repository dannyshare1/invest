# -*- coding: utf-8 -*-
"""
国际：Reuters / FT / Yahoo Finance (+ NewsAPI & Mediastack 可选)
国内：中证报 / 新华社财经 / 21财经 / 上证报
→ 生成 news.json（含 300 字摘要）
"""
import os, json, datetime, re, hashlib, html as ihtml, traceback
import requests, chardet
from bs4 import BeautifulSoup

# ====== 基础设置 ======
MAX_PER = 10
TIMEOUT = 12
UA = {"User-Agent": "Mozilla/5.0"}
today = datetime.date.today()
NEWS_KEY = os.getenv("NEWSAPI_KEY")
MS_KEY   = os.getenv("MEDIASTACK_KEY")

# ====== 关键词 >1 字母/汉字 ======
KWS = [w.lower() for w in """
股票 股市 指数 证券 分红 回购 半导体 芯片 AI 宏观 业绩 盈利 融资
bull bear dividend buyback earnings volatility inflation semiconductor
""".split() if len(w) > 1]

def relevant(t: str) -> bool:
    txt = re.sub(r"[^\w\u4e00-\u9fa5]", "", t.lower())
    return any(k in txt for k in KWS)

# ====== 工具函数 ======
def tidy(resp: requests.Response) -> str:
    if resp.encoding == "ISO-8859-1":
        resp.encoding = chardet.detect(resp.content)['encoding'] or "utf-8"
    return resp.text

def make_snippet(html: str, css="article p, div.article p, p", n=3, cap=300) -> str:
    soup = BeautifulSoup(html, "lxml")
    meta = soup.select_one("meta[name=description]")
    base = meta["content"] if meta and meta.get("content") else \
           " ".join(p.get_text(" ", strip=True) for p in soup.select(css)[:n])
    base = base or soup.get_text(" ", strip=True)
    return ihtml.unescape(base)[:cap]

def add(title, snip, src, out):
    if not title or len(snip) < 40 or not relevant(title+snip): return
    uid = hashlib.md5(f"{title}_{src}".encode()).hexdigest()
    if uid in seen: return
    seen.add(uid)
    out.append({
        "title": title.strip(),
        "snippet": snip.strip(),
        "source": src,
        "published": today.isoformat()
    })

items, seen = [], set()

# ====== A. 国际：Reuters JSON → RSS 兜底 ======
try:
    rj = requests.get("https://www.reuters.com/pf/api/v3/content/fetch/articles-by-section-alias-or-id-v1"
                      "?queryParams=section=%2Fbusiness&offset=0&size=15",
                      headers=UA, timeout=TIMEOUT)
    if rj.status_code == 200:
        for art in rj.json()["result"]["articles"][:MAX_PER]:
            add(art["title"], art.get("description","") or art.get("dek",""),
                "Reuters", items)
    else:
        raise ValueError("Reuters JSON bad status")
except Exception:
    # 回退到 RSS
    try:
        xml = requests.get("https://www.reuters.com/rssFeed/businessNews", headers=UA, timeout=TIMEOUT).text
        for it in BeautifulSoup(xml,"xml").find_all("item")[:MAX_PER]:
            title = it.title.get_text(strip=True)
            desc  = BeautifulSoup(it.description.get_text(),"lxml").get_text(" ",strip=True)
            add(title, desc, "Reuters", items)
    except Exception as e:
        print("Reuters fallback fail:", e)

# ====== B. 国际 RSS (FT & Yahoo) ======
RSS_INT = [
    ("FT", "https://www.ft.com/?format=rss"),
    ("Yahoo", "https://feeds.finance.yahoo.com/rss/2.0/headline?s=%5EGSPC&region=US&lang=en-US")
]
for src, url in RSS_INT:
    try:
        xml = requests.get(url, headers=UA, timeout=TIMEOUT).text
        for it in BeautifulSoup(xml,"xml").find_all("item")[:MAX_PER]:
            title = it.title.get_text(strip=True)
            desc  = BeautifulSoup(it.description.get_text(),"lxml").get_text(" ",strip=True)
            add(title, desc, src, items)
    except Exception as e:
        print(src, "RSS error:", e)

# ====== C. NewsAPI / Mediastack (若密钥可用) ======
DOM_WHITELIST = ",".join([
    "bloomberg.com","reuters.com","ft.com","wsj.com",
    "cnbc.com","marketwatch.com","finance.yahoo.com","seekingalpha.com"
])
if NEWS_KEY:
    try:
        r = requests.get("https://newsapi.org/v2/everything",
                         params={"q":"stock market","language":"en",
                                 "from":today- datetime.timedelta(days=1),
                                 "pageSize":MAX_PER,
                                 "domains":DOM_WHITELIST,
                                 "apiKey":NEWS_KEY},
                         timeout=TIMEOUT)
        if r.status_code == 200:
            for a in r.json().get("articles",[]):
                add(a["title"], a.get("description") or a["title"],
                    "NewsAPI", items)
    except Exception as e:
        print("NewsAPI err:", e)

if MS_KEY:
    try:
        r = requests.get("http://api.mediastack.com/v1/news",
                         params={"access_key":MS_KEY,"keywords":"earnings","limit":MAX_PER,
                                 "domains":DOM_WHITELIST,"languages":"en"},
                         timeout=TIMEOUT)
        if r.status_code == 200:
            for a in r.json().get("data",[]):
                add(a["title"], a.get("description") or a["title"],
                    "Mediastack", items)
    except Exception as e:
        print("MStack err:", e)

# 1⃣  国内列表页 URL
DOMESTIC = [
    ("中国证券报",  "https://www.cs.com.cn/ssgs/"),
    ("新华社财经",  "https://www.news.cn/fortune/"),
    ("21财经",      "https://m.21jingji.com/channel/stock"),
    ("上海证券报",  "https://news.cnstock.com/news/sns_htfb/")
]

# 2⃣  make_snippet() 里的默认 CSS
def make_snippet(html: str,
                 css="article p, div.article p, div.content p, p",
                 n=3, cap=300):
   
def real_links(html):
    soup = BeautifulSoup(html,"lxml")
    return [a["href"] for a in soup.select("a[href]") 
            if a["href"].startswith("http") and
               (a["href"].endswith(".shtml") or "/content/" in a["href"])]

for src, lst in DOMESTIC:
    try:
        lst_html = tidy(requests.get(lst, headers=UA, timeout=TIMEOUT))
        for link in real_links(lst_html)[:MAX_PER]:
            art = tidy(requests.get(link, headers=UA, timeout=TIMEOUT))
            title = BeautifulSoup(art,"lxml").title.get_text(strip=True)
            snip  = make_snippet(art)
            add(title, snip, src, items)
    except Exception as e:
        print(src, "列表解析失败:", e)

# ====== 保存 ======
with open("news.json","w",encoding="utf-8") as f:
    json.dump(items, f, ensure_ascii=False, indent=2)

print(f"✅ 国际 {len([i for i in items if i['source'] in ['Reuters','FT','Yahoo','NewsAPI','Mediastack']])} 条，"
      f"国内 {len([i for i in items if '财' in i['source'] or '报' in i['source']])} 条 → 共 {len(items)} 条写入 news.json")
