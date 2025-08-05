# -*- coding: utf-8 -*-
"""
拉取 NewsAPI、Mediastack、东方财富 / 新浪 RSS → 去重 → 保存 news.json
"""
import os, json, datetime, re, requests, feedparser

NEWSAPI_KEY    = os.getenv("NEWSAPI_KEY")      # 必填
MEDIASTACK_KEY = os.getenv("MEDIASTACK_KEY")   # 可选
MAX_PER_SRC    = 5

# 关键词更丰富：宏观、行业、资产类别
keywords_en = [
    "dividend", "high yield", "semiconductor",
    "CSI 300", "China A-shares", "AI chip",
    "interest rate", "inflation", "crude oil", "gold ETF"
]
keywords_cn = [
    "红利", "高股息", "半导体", "沪深300",
    "国企分红", "人工智能", "油价", "黄金", "汇率"
]

today      = datetime.date.today()
yesterday  = today - datetime.timedelta(days=1)
news_items = []

def norm(text: str):
    """规范化标题用于去重"""
    return re.sub(r"[^\w\u4e00-\u9fa5]", "", text).lower()

def add(title, src, url, dt, origin):
    """写入新闻列表"""
    if title and url:
        news_items.append({
            "title": title.strip(),
            "source": src,
            "url": url,
            "published": (dt or today.isoformat())[:10],
            "origin": origin
        })

# —— 1. NewsAPI (英文) ——
if NEWSAPI_KEY:
    for kw in keywords_en:
        resp = requests.get(
            "https://newsapi.org/v2/everything",
            params={
                "q": kw,
                "language": "en",
                "from":   yesterday.isoformat(),
                "to":     today.isoformat(),
                "pageSize": MAX_PER_SRC,
                "sortBy": "publishedAt",
                "apiKey": NEWSAPI_KEY
            },
            timeout=10)
        for art in resp.json().get("articles", []):
            add(art["title"], art["source"]["name"], art["url"],
                art["publishedAt"], "NEWSAPI")

# —— 2. Mediastack (可选兜底) ——
if MEDIASTACK_KEY:
    for kw in keywords_en + keywords_cn:
        resp = requests.get(
            "http://api.mediastack.com/v1/news",
            params={
                "access_key": MEDIASTACK_KEY,
                "keywords": kw,
                "languages": "en,zh",
                "date": yesterday.isoformat(),
                "limit": MAX_PER_SRC
            },
            timeout=10)
        for art in resp.json().get("data", []):
            add(art["title"], art["source"], art["url"],
                art["published_at"], "MEDIASTACK")

# —— 3. 中文 RSS ——
rss_list = [
    ("东方财富", "https://rsshub.app/eastmoney/stock"),
    ("新浪财经", "https://rsshub.app/sina/finance")
]
ua_hdr = {"User-Agent": "Mozilla/5.0"}   # 防 403
for src, link in rss_list:
    feed = feedparser.parse(link, request_headers=ua_hdr)
    for ent in feed.entries[:MAX_PER_SRC]:
        add(ent.title, src, ent.link,
            getattr(ent, "published", today.isoformat()), "RSS")

print("抓取完毕：",
      f"NewsAPI {sum(n['origin']=='NEWSAPI' for n in news_items)} 条，",
      f"MStack {sum(n['origin']=='MEDIASTACK' for n in news_items)} 条，",
      f"RSS {sum(n['origin']=='RSS' for n in news_items)} 条")

# —— 去重：URL > 标题 —— 
seen, unique = set(), []
for n in news_items:
    key = n["url"] or norm(n["title"])
    if key not in seen:
        unique.append(n)
        seen.add(key)

with open("news.json", "w", encoding="utf-8") as f:
    json.dump(unique, f, ensure_ascii=False, indent=2)

print(f"✅ 去重后保留 {len(unique)} 条，已写入 news.json")
