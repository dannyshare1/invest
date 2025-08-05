# -*- coding: utf-8 -*-
"""
抓取 NewsAPI、Mediastack、东方财富/Sina RSS → 去重 → 保存 news.json
"""
import os, json, datetime, re
import requests, feedparser

NEWSAPI_KEY      = os.getenv("NEWSAPI_KEY")      # 必填
MEDIASTACK_KEY   = os.getenv("MEDIASTACK_KEY")   # 可选
MAX_PER_SOURCE   = 5                             # 每源最多抓几条
keywords_en = ["dividend", "semiconductor", "CSI 300", "A-share"]
keywords_cn = ["红利", "半导体", "沪深300"]

today = datetime.date.today().isoformat()
news_items = []

def normalize(text):               # 标题简单规范化做去重
    return re.sub(r"[^\w\u4e00-\u9fa5]", "", text).lower()

def add_item(title, src, url, dt):
    if title and url:
        news_items.append({
            "title": title.strip(),
            "source": src,
            "url": url,
            "published": dt[:10] if dt else today
        })

# ——— 1. NewsAPI (英文) ———
if NEWSAPI_KEY:
    for kw in keywords_en:
        r = requests.get(
            "https://newsapi.org/v2/everything",
            params={"q": kw,
                    "language": "en",
                    "from": today,
                    "pageSize": MAX_PER_SOURCE,
                    "apiKey": NEWSAPI_KEY},
            timeout=10)
        for art in r.json().get("articles", []):
            add_item(art["title"], art["source"]["name"], art["url"], art["publishedAt"])

# ——— 2. Mediastack (兜底) ———
if MEDIASTACK_KEY:
    for kw in keywords_en + keywords_cn:
        url = "http://api.mediastack.com/v1/news"
        r = requests.get(url,
                         params={"access_key": MEDIASTACK_KEY,
                                 "languages": "en,zh",
                                 "keywords": kw,
                                 "limit": MAX_PER_SOURCE},
                         timeout=10)
        for art in r.json().get("data", []):
            add_item(art["title"], art["source"], art["url"], art["published_at"])

# ——— 3. 中文 RSS (东方财富 / 新浪) ———
rss_list = [
    ("东方财富", "https://rsshub.app/eastmoney/stock"),
    ("新浪财经", "https://rsshub.app/sina/finance")
]
for src, link in rss_list:
    feed = feedparser.parse(link)
    for ent in feed.entries[:MAX_PER_SOURCE]:
        add_item(ent.title, src, ent.link, ent.published if hasattr(ent, "published") else today)

# —— 去重：URL 一级，标题规范化二级 ——
unique, seen = [], set()
for n in news_items:
    key = n["url"] or normalize(n["title"])
    if key not in seen:
        unique.append(n)
        seen.add(key)

# 保存
with open("news.json", "w", encoding="utf-8") as f:
    json.dump(unique, f, ensure_ascii=False, indent=2)

print(f"抓取完成，保存 {len(unique)} 条到 news.json")
