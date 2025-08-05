# -*- coding: utf-8 -*-
"""
抓取 NewsAPI、Mediastack、财新网、新浪财经  → 去重 → news.json（含摘要）
"""
import os, json, datetime, re, requests, time
from bs4 import BeautifulSoup

NEWSAPI_KEY    = os.getenv("NEWSAPI_KEY")
MEDIASTACK_KEY = os.getenv("MEDIASTACK_KEY")
MAX_PER_SRC    = 5

# 关键词
keywords_en = [
    "stock market", "equity", "capital market",
    "bull market", "bear market", "volatility",
    "dividend", "buyback", "earnings report",
    "sector analysis", "analyst rating",
    "interest rate impact stock"
]
keywords_cn = [
    "股市", "股票", "证券市场", "资本市场",
    "牛市", "熊市", "波动率",
    "分红", "回购", "财报",
    "行业分析", "机构评级",
    "上证综指", "深证成指", "创业板"
]
combo_kw = ["公司 财报", "宏观经济 股市", "利率 影响 股票", "行业 前景"]

today     = datetime.date.today()
yesterday = today - datetime.timedelta(days=1)
news_items = []

def norm(text): return re.sub(r"[^\w\u4e00-\u9fa5]", "", text).lower()

def add(title, snippet, src, pub, origin):
    if title and snippet:
        news_items.append({
            "title": title.strip(),
            "snippet": snippet.strip()[:200],
            "source": src,
            "published": pub[:10],
            "origin": origin
        })

# --------------------------------------------------------
# 1. NewsAPI (英文，白名单域)
# --------------------------------------------------------
domains_white = ",".join([
    "bloomberg.com","reuters.com","ft.com","wsj.com",
    "cnbc.com","marketwatch.com","finance.yahoo.com","seekingalpha.com"
])
if NEWSAPI_KEY:
    for kw in keywords_en + combo_kw:
        r = requests.get("https://newsapi.org/v2/everything",
            params={
                "q": kw,
                "language": "en",
                "from": yesterday.isoformat(),
                "to":   today.isoformat(),
                "pageSize": MAX_PER_SRC,
                "sortBy": "publishedAt",
                "domains": domains_white,
                "apiKey": NEWSAPI_KEY
            }, timeout=10)
        for art in r.json().get("articles", []):
            add(art["title"],
                art.get("description") or art["title"],
                art["source"]["name"],
                art["publishedAt"],
                "NEWSAPI")

# --------------------------------------------------------
# 2. Mediastack (英文/中文)  同样白名单
# --------------------------------------------------------
if MEDIASTACK_KEY:
    doms = domains_white
    for kw in keywords_en + keywords_cn + combo_kw:
        r = requests.get("http://api.mediastack.com/v1/news",
            params={
                "access_key": MEDIASTACK_KEY,
                "keywords": kw,
                "languages": "en,zh",
                "domains": doms,
                "date": yesterday.isoformat(),
                "limit": MAX_PER_SRC
            }, timeout=10)
        for art in r.json().get("data", []):
            add(art["title"],
                art.get("description") or art["title"],
                art["source"],
                art["published_at"],
                "MEDIASTACK")

# --------------------------------------------------------
# 3. 财新网首页要闻 → 正文首段
# --------------------------------------------------------
try:
    ua = {"User-Agent": "Mozilla/5.0"}
    index = requests.get("https://www.caixin.com/", headers=ua, timeout=10).text
    soup  = BeautifulSoup(index, "lxml")
    links = [a["href"] for a in soup.select(".news_list a") if a["href"].startswith("https://")]
    for url in links[:MAX_PER_SRC]:
        art = requests.get(url, headers=ua, timeout=10).text
        s   = BeautifulSoup(art, "lxml")
        title = s.title.get_text(strip=True)
        para  = (s.select_one("meta[name=description]") or s.find("p")).get_text(strip=True)
        add(title, para, "财新网", today.isoformat(), "CN_JSON")
    print("财新网 抓到", len(links[:MAX_PER_SRC]), "条")
except Exception as e:
    print("财新网抓取失败:", e)

# --------------------------------------------------------
# 4. 新浪财经焦点新闻 → 正文首段
# --------------------------------------------------------
try:
    sina_list = requests.get(
        "https://feed.sina.com.cn/api/roll/get",
        params={"pageid":155, "lid":1686, "num":20},
        timeout=10).json()
    ua = {"User-Agent": "Mozilla/5.0"}
    for it in sina_list["result"]["data"][:MAX_PER_SRC]:
        art = requests.get(it["url"], headers=ua, timeout=10).text
        s   = BeautifulSoup(art, "lxml")
        para = (s.select_one("meta[name=description]") or s.find("p")).get_text(strip=True)
        add(it["title"], para, "新浪财经", it["ctime"], "CN_JSON")
    print("新浪财经 抓到", len(sina_list['result']['data'][:MAX_PER_SRC]), "条")
except Exception as e:
    print("新浪财经抓取失败:", e)

# --------------------------------------------------------
# 统计 + 去重
# --------------------------------------------------------
print("NewsAPI",  sum(n['origin']=="NEWSAPI"   for n in news_items),
      "MStack",  sum(n['origin']=="MEDIASTACK" for n in news_items),
      "CN_JSON", sum(n['origin']=="CN_JSON"    for n in news_items))

seen, unique = set(), []
for n in news_items:
    key = norm(n["title"])
    if key not in seen:
        unique.append(n); seen.add(key)

with open("news.json", "w", encoding="utf-8") as f:
    json.dump(unique, f, ensure_ascii=False, indent=2)

print(f"✅ 去重后保留 {len(unique)} 条，已写入 news.json")
