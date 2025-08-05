# -*- coding: utf-8 -*-
"""
抓取 NewsAPI、Mediastack、东方财富 & 新浪财经官方接口 → 去重 → 保存 news.json
"""
import os, json, datetime, re, requests, time   # ← 在这里补上 time

NEWSAPI_KEY    = os.getenv("NEWSAPI_KEY")      # 必填
MEDIASTACK_KEY = os.getenv("MEDIASTACK_KEY")   # 可选
MAX_PER_SRC    = 5                             # 每源最多保留多少条

# 关键词（英/中）
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

def norm(txt: str):
    """规范化标题用于去重"""
    return re.sub(r"[^\w\u4e00-\u9fa5]", "", txt).lower()

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

# ------------------------------------------------------------------
# 1. NewsAPI (英文)
# ------------------------------------------------------------------
if NEWSAPI_KEY:
    for kw in keywords_en:
        r = requests.get(
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
        for art in r.json().get("articles", []):
            add(art["title"], art["source"]["name"], art["url"],
                art["publishedAt"], "NEWSAPI")

# ------------------------------------------------------------------
# 2. Mediastack (可选)
# ------------------------------------------------------------------
if MEDIASTACK_KEY:
    for kw in keywords_en + keywords_cn:
        r = requests.get(
            "http://api.mediastack.com/v1/news",
            params={
                "access_key": MEDIASTACK_KEY,
                "keywords": kw,
                "languages": "en,zh",
                "date": yesterday.isoformat(),
                "limit": MAX_PER_SRC
            },
            timeout=10)
        for art in r.json().get("data", []):
            add(art["title"], art["source"], art["url"],
                art["published_at"], "MEDIASTACK")

# ------------------------------------------------------------------
# 3. 东方财富官方接口（要闻）
# ------------------------------------------------------------------
try:
    em_url = "https://push2.eastmoney.com/api/qt/top_news"
    em_params = {"type": "finance", "limit": 20, "_": int(time.time()*1000)}
    em = requests.get(em_url, params=em_params, timeout=10).json()
    for it in em["data"]["list"][:MAX_PER_SRC]:
        add(it["title"], "东方财富", it["url"], it["date"], "RSS")
except Exception as e:
    print("东方财富抓取失败：", e)

# ------------------------------------------------------------------
# 4. 新浪财经官方接口（焦点新闻）
# ------------------------------------------------------------------
try:
    sina_url = "https://feed.sina.com.cn/api/roll/get"
    sina_params = {"pageid": 155, "lid": 1686, "num": 20}
    s = requests.get(sina_url, params=sina_params, timeout=10).json()
    for it in s["result"]["data"][:MAX_PER_SRC]:
        add(it["title"], "新浪财经", it["url"], it["ctime"], "RSS")
except Exception as e:
    print("新浪财经抓取失败：", e)

# ------------------------------------------------------------------
# 5. 打印抓取统计
# ------------------------------------------------------------------
print("NewsAPI",  sum(n['origin']=="NEWSAPI"   for n in news_items),
      "MStack",  sum(n['origin']=="MEDIASTACK" for n in news_items),
      "RSS",     sum(n['origin']=="RSS"        for n in news_items))

# ------------------------------------------------------------------
# 6. 去重：先 URL，再规范化标题
# ------------------------------------------------------------------
seen, unique = set(), []
for n in news_items:
    key = n["url"] or norm(n["title"])
    if key not in seen:
        unique.append(n); seen.add(key)

with open("news.json", "w", encoding="utf-8") as f:
    json.dump(unique, f, ensure_ascii=False, indent=2)

print(f"✅ 去重后保留 {len(unique)} 条，已写入 news.json")
