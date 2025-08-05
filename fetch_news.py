# -*- coding: utf-8 -*-
"""
抓取 NewsAPI + Mediastack + 东方财富 / 新浪RSS ——> news.json
"""
import os, json, datetime, re, requests, feedparser

NEWSAPI_KEY      = os.getenv("NEWSAPI_KEY")      # 必填
MEDIASTACK_KEY   = os.getenv("MEDIASTACK_KEY")   # 可选
MAX_PER_SOURCE   = 5

keywords_en = ["dividend", "semiconductor", "CSI 300", "A-share"]
keywords_cn = ["红利", "半导体", "沪深300"]

today      = datetime.date.today()
yesterday  = today - datetime.timedelta(days=1)

news_items = []

def norm(txt):     # 标题规范化去重
    return re.sub(r"[^\w\u4e00-\u9fa5]", "", txt).lower()

def add(title, src, url, dt):
    if title and url:
        news_items.append({
            "title": title.strip(),
            "source": src,
            "url":   url,
            "published": (dt or today.isoformat())[:10]
        })

# —— 1. NewsAPI (英文) ——
if NEWSAPI_KEY:
    for kw in keywords_en:
        r = requests.get(
            "https://newsapi.org/v2/everything",
            params={
                "q": kw,
                "language": "en",
                "from":   yesterday.isoformat(),
                "to":     today.isoformat(),
                "pageSize": MAX_PER_SOURCE,
                "sortBy": "publishedAt",
                "apiKey": NEWSAPI_KEY
            },
            timeout=10)
        for art in r.json().get("articles", []):
            add(art["title"], art["source"]["name"], art["url"], art["publishedAt"])

# —— 2. Mediastack (可选) ——
if MEDIASTACK_KEY:
    for kw in keywords_en + keywords_cn:
        r = requests.get(
            "http://api.mediastack.com/v1/news",
            params={
                "access_key": MEDIASTACK_KEY,
                "keywords": kw,
                "languages": "en,zh",
                "date": yesterday.isoformat(),
                "limit": MAX_PER_SOURCE
            },
            timeout=10)
        for art in r.json().get("data", []):
            add(art["title"], art["source"], art["url"], art["published_at"])

# —— 3. 中文 RSS ——
rss_list = [
    ("东方财富", "https://rsshub.app/eastmoney/stock"),
    ("新浪财经", "https://rsshub.app/sina/finance")
]
for src, link in rss_list:
    feed = feedparser.parse(link)
    for ent in feed.entries[:MAX_PER_SOURCE]:
        add(ent.title, src, ent.link, getattr(ent, "published", today.isoformat()))

# —— 去重：URL ➜ 标题规范化 ——
seen, unique = set(), []
for n in news_items:
    key = n["url"] or norm(n["title"])
    if key not in seen:
        unique.append(n); seen.add(key)

with open("news.json", "w", encoding="utf-8") as f:
    json.dump(unique, f, ensure_ascii=False, indent=2)

print(f"✅ 已保存 {len(unique)} 条新闻至 news.json")
