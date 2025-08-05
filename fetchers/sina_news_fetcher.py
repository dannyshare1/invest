
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fetchers/sina_news_fetcher.py
---------------------------------
抓取新浪新闻热门条目，并将首段作为摘要
"""
import datetime as dt
import time
from typing import List, Dict
import requests
import feedparser
from bs4 import BeautifulSoup

FEED_URL = "https://rsshub.app/sina/news/hot"
REQUEST_TIMEOUT = 10
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/138.0.0.0 Safari/537.36"
    )
}
SLEEP_BETWEEN_REQUESTS = 1.2

def _fetch_rss_items(limit: int) -> List[Dict]:
    feed = feedparser.parse(FEED_URL)
    items = []
    for entry in feed.entries[:limit]:
        items.append(
            {
                "title": entry.title,
                "url": entry.link,
                "published_at": (
                    entry.published
                    if "published" in entry
                    else dt.datetime.utcnow().isoformat()
                ),
            }
        )
    return items

def _first_paragraph(url: str) -> str:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        resp.encoding = resp.apparent_encoding or "utf-8"
    except requests.RequestException:
        return ""
    soup = BeautifulSoup(resp.text, "lxml")
    container = soup
    p = container.find("p")
    if not p:
        return ""
    return " ".join(p.get_text(strip=True).split())

def build_news(limit: int = 10) -> List[Dict]:
    items = _fetch_rss_items(limit)
    news = []
    for itm in items:
        summary = _first_paragraph(itm["url"])
        news.append(
            {
                "title": itm["title"],
                "url": itm["url"],
                "summary": summary,
                "source": "新浪新闻",
                "published_at": itm["published_at"],
                "fetched_at": dt.datetime.utcnow().isoformat(),
            }
        )
        time.sleep(SLEEP_BETWEEN_REQUESTS)
    return news
