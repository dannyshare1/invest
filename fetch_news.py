# -*- coding: utf-8 -*-
"""
抓取 NewsAPI、Mediastack、财新网、新浪财经 → 生成 news.json（含摘要）
"""
import os, json, datetime, re, requests, time, html as ihtml
from bs4 import BeautifulSoup
from sumy.parsers.plaintext import PlaintextParser
from sumy.summarizers.text_rank import TextRankSummarizer
from sumy.nlp.tokenizers import Tokenizer

# ---------- 通用工具 ---------- #
def clean_html(raw_html: str) -> str:
    soup = BeautifulSoup(raw_html, "lxml")
    for tag in soup(["script", "style", "aside", "footer"]):
        tag.decompose()
    return soup.get_text("\n")

def make_snippet(raw_html: str, css: str, max_len: int = 250) -> str:
    soup = BeautifulSoup(raw_html, "lxml")
    paras = [p.get_text(" ", strip=True) for p in soup.select(css)[:3]]
    text  = " ".join(paras) or clean_html(raw_html)[:max_len]
    return ihtml.unescape(text).strip()[:max_len]

def norm(txt: str) -> str:
    return re.sub(r"[^\w\u4e00-\u9fa5]", "", txt).lower()

# ---------- 环境 & 关键词 ---------- #
NEWSAPI_KEY    = os.getenv("NEWSAPI_KEY")
MEDIASTACK_KEY = os.getenv("MEDIASTACK_KEY")
MAX_PER_SRC    = 5

keywords_en = [
    "stock market", "equity", "capital market", "bull market", "bear market",
    "volatility", "dividend", "buyback", "earnings report", "sector analysis",
    "analyst rating", "interest rate impact stock"
]
keywords_cn = [
    "股市", "股票", "证券市场", "资本市场", "牛市", "熊市", "波动率",
    "分红", "回购", "财报", "行业分析", "机构评级", "上证综指", "深证成指", "创业板"
]
combo_kw = ["公司 财报", "宏观经济 股市", "利率 影响 股票", "行业 前景"]

INV_KWS = set(k.lower().replace(" ", "") for k in (keywords_cn + keywords_en + combo_kw))

today, yesterday = datetime.date.today(), datetime.date.today() - datetime.timedelta(days=1)
news_items = []

def add(title, snippet, src, pub, origin):
    if title and snippet:
        news_items.append({
            "title": title.strip(),
            "snippet": snippet.strip()[:350],
            "source": src,
            "published": pub[:10],
            "origin": origin
        })

# ---------- 1. NewsAPI ---------- #
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
            add(art["title"], art.get("description") or art["title"],
                art["source"]["name"], art["publishedAt"], "NEWSAPI")

# ---------- 2. Mediastack ---------- #
if MEDIASTACK_KEY:
    for kw in keywords_en + keywords_cn + combo_kw:
        r = requests.get("http://api.mediastack.com/v1/news",
                         params={
                             "access_key": MEDIASTACK_KEY,
                             "keywords": kw,
                             "languages": "en,zh",
                             "domains": domains_white,
                             "date": yesterday.isoformat(),
                             "limit": MAX_PER_SRC
                         }, timeout=10)
        for art in r.json().get("data", []):
            add(art["title"], art.get("description") or art["title"],
                art["source"], art["published_at"], "MEDIASTACK")

# ---------- 3. 财新网 ---------- #
try:
    ua = {"User-Agent": "Mozilla/5.0"}
    idx_html = requests.get("https://www.caixin.com/", headers=ua, timeout=10).text
    links = [a["href"] for a in BeautifulSoup(idx_html, "lxml")
             .select(".title_list a, .news_list a")][:MAX_PER_SRC]
    count = 0
    for url in links:
        r = requests.get(url, headers=ua, timeout=10)
        r.encoding = r.apparent_encoding
        soup_art = BeautifulSoup(r.text, "lxml")
        title   = soup_art.title.get_text(strip=True)          # ❶ 获取标题
        snippet = make_snippet(r.text, "article p, div.article p")
        txt     = re.sub(r"[^\w\u4e00-\u9fa5]", "", (title + snippet).lower())
        if not any(k in txt for k in INV_KWS):
            continue
        add(title, snippet, "财新网", today.isoformat(), "CN_JSON")
        count += 1
    print("财新网 抓到", count, "条")
except Exception as e:
    print("财新网抓取失败:", e)

# ---------- 4. 新浪财经 ---------- #
try:
    feed = requests.get("https://feed.sina.com.cn/api/roll/get",
                        params={"pageid": 155, "lid": 1686, "num": 30},
                        timeout=10).json()
    ua = {"User-Agent": "Mozilla/5.0"}
    count = 0
    for it in feed["result"]["data"]:
        if not it["url"].startswith("https://finance.sina.com.cn/"):
            continue
        r = requests.get(it["url"], headers=ua, timeout=10)
        r.encoding = r.apparent_encoding
        soup_art = BeautifulSoup(r.text, "lxml")
        snippet  = make_snippet(r.text, "article p, div.article p, p")
        txt      = re.sub(r"[^\w\u4e00-\u9fa5]", "", (it["title"] + snippet).lower())
        if not any(k in txt for k in INV_KWS):
            continue
        add(it["title"], snippet, "新浪财经", it["ctime"], "CN_JSON")
        count += 1
        if count >= MAX_PER_SRC:
            break
    print("新浪财经 抓到", count, "条")
except Exception as e:
    print("新浪财经抓取失败:", e)

# ---------- 去重 & 输出 ---------- #
print("NewsAPI",  sum(n['origin']=="NEWSAPI"   for n in news_items),
      "MStack",  sum(n['origin']=="MEDIASTACK" for n in news_items),
      "CN_JSON", sum(n['origin']=="CN_JSON"    for n in news_items))

seen, unique = set(), []
for n in news_items:
    key = f"{norm(n['title'])}_{n['source']}"
    if key not in seen:
        unique.append(n)
        seen.add(key)

with open("news.json", "w", encoding="utf-8") as f:
    json.dump(unique, f, ensure_ascii=False, indent=2)

print(f"✅ 去重后保留 {len(unique)} 条，已写入 news.json")
