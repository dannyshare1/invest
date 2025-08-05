
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
compile_news.py
----------------
聚合所有 fetchers，生成 news.json
"""
import json
import datetime as dt
from pathlib import Path
from fetchers.caixin_news_fetcher import build_news as caixin_news
from fetchers.sina_news_fetcher import build_news as sina_news

OUTPUT_FILE = Path("news.json")

def main():
    news_items = []
    news_items.extend(caixin_news(10))
    news_items.extend(sina_news(10))

    # 排序：发布时间新 -> 旧
    news_items.sort(key=lambda x: x.get("published_at", ""), reverse=True)
    with OUTPUT_FILE.open("w", encoding="utf-8") as fp:
        json.dump(news_items, fp, ensure_ascii=False, indent=2)
    print(f"✅ 生成 {len(news_items)} 条新闻 → {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
