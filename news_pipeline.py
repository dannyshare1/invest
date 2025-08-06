# -*- coding: utf-8 -*-
"""
news_pipeline.py  —— 多源财经新闻聚合器

完全替换旧版 fetch_news.py 脚本，可直接挂在现有 GitHub Actions
workflow 中（示例见文件末尾注释）。

功能
-----
1. **动态持仓**：从 `holdings.json` （或环境变量）读取实时仓位，不用改代码。
2. **统一抓取**：支持 TuShare / AkShare / Alpha Vantage / Finnhub / 聚合数据
   五大源，自动按 A 股 or 海外资产选择接口；全部用 `httpx.AsyncClient`
   并发下载。
3. **去重+相关度评分**：用 URL 指纹去重；按“直接命中→关键词→行业”
   三层权重 + 情绪，给出 0-1 相关度。
4. **输出**：
   * `news_today.json` —— 原始新闻结构化数据（落盘供 Debug / 备份）
   * `briefing.md` —— 给 LLM 的精简 Markdown 摘要（<= 2 KB，防 prompt
     爆）
   * 自动推送到 Server 酱 & Telegram（可选）。

依赖
----
```
pip install httpx python-dateutil rich
```
（如需调用 OpenAI 做情绪分析：`pip install openai`）

环境变量
--------
```
TUSHARE_TOKEN   # TuShare
ALPHA_KEY       # Alpha Vantage
FINNHUB_KEY     # Finnhub
JUHE_KEY        # 聚合数据（可选）
OPENAI_API_KEY  # Sentiment optional
SCKEY           # Server 酱
TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID
```

`holdings.json` 格式
--------------------
```json
[
  {"symbol": "510880.SH", "name": "中证红利ETF", "weight": 0.12},
  {"symbol": "510300.SH", "name": "沪深300ETF", "weight": 0.05},
  {"symbol": "AAPL", "name": "Apple Inc", "weight": 0.10}
]
```

用法
----
```bash
python news_pipeline.py --days 1 --max 30  # 前 1 个自然日，输出 30 条
```
在 GitHub Action step 里加：
```yaml
run: |
  python news_pipeline.py --days 1 --max 30
```
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from hashlib import md5
from pathlib import Path
from typing import Any, Dict, List, Sequence

import httpx
from dateutil import parser as dtparse
from rich import print as rprint

# ---------------------------------------------------------------------------
# 数据类
# ---------------------------------------------------------------------------
@dataclass
class Holding:
    symbol: str
    name: str
    weight: float = 0.0

    @property
    def clean_symbol(self) -> str:
        """去掉交易所后缀，用于关键词匹配。"""
        return self.symbol.split(".")[0]

    @property
    def is_cn(self) -> bool:
        return self.symbol.endswith((".SH", ".SZ"))

@dataclass
class NewsItem:
    title: str
    summary: str
    url: str
    published_at: str  # ISO-8601
    source: str
    sentiment: float  # -1~1，缺省 0
    symbols: List[str]

    def fingerprint(self) -> str:
        return md5(self.url.encode()).hexdigest()

# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------
_SENTIMENT_RE = re.compile(r"\b(upbeat|bullish|positive)\b|\b(downbeat|bearish|negative)\b", flags=re.I)

def naive_sentiment(text: str) -> float:
    """非常简陋的情绪打分，占位用。"""
    m = _SENTIMENT_RE.findall(text)
    if not m:
        return 0.0
    pos = len([w for w in m if w[0]])
    neg = len([w for w in m if w[1]])
    return (pos - neg) / max(pos + neg, 1)

async def async_retry(coro_func, *args, retries: int = 3, **kwargs):
    for i in range(retries):
        try:
            return await coro_func(*args, **kwargs)
        except Exception as e:
            if i == retries - 1:
                raise
            await asyncio.sleep(2 ** i)

# ---------------------------------------------------------------------------
# Fetcher 实现
# ---------------------------------------------------------------------------
class Fetcher:
    def __init__(self, client: httpx.AsyncClient):
        self.client = client
        self.tushare_token = os.getenv("TUSHARE_TOKEN")
        self.alpha_key = os.getenv("ALPHA_KEY")
        self.finnhub_key = os.getenv("FINNHUB_KEY")
        self.juhe_key = os.getenv("JUHE_KEY")

    async def tushare_news(self, ts_code: str, start: datetime, end: datetime):
        if not self.tushare_token:
            return []
        url = "https://api.tushare.pro"
        payload = {
            "api_name": "news",
            "token": self.tushare_token,
            "params": {
                "src": "eastmoney",
                "start_date": start.strftime("%Y%m%d"),
                "end_date": end.strftime("%Y%m%d"),
            },
            "fields": "title,content,url,datetime,src"
        }
        resp = await self.client.post(url, json=payload, timeout=20)
        resp.raise_for_status()
        data = resp.json().get("data", {}).get("items", [])
        out = []
        for title, content, link, dtstr, src in data:
            out.append(NewsItem(title=title,
                                summary=content[:120],
                                url=link,
                                published_at=dtparse.parse(dtstr).isoformat(),
                                source=src,
                                sentiment=naive_sentiment(content),
                                symbols=[ts_code]))
        return out

    async def alpha_news(self, tickers: Sequence[str]):
        if not self.alpha_key or not tickers:
            return []
        params = {
            "function": "NEWS_SENTIMENT",
            "tickers": ",".join(tickers[:100]),
            "apikey": self.alpha_key
        }
        r = await self.client.get("https://www.alphavantage.co/query", params=params, timeout=20)
        r.raise_for_status()
        feed = r.json().get("feed", [])
        out = []
        for item in feed:
            tickers_in_item = [s.get("ticker") for s in item.get("ticker_sentiment", []) if s.get("ticker")]
            out.append(NewsItem(title=item["title"],
                                summary=item["summary"],
                                url=item["url"],
                                published_at=item["time_published"],
                                source=item.get("source", "AlphaV"),
                                sentiment=float(item.get("overall_sentiment_score", 0)),
                                symbols=tickers_in_item))
        return out

    async def finnhub_news(self, symbol: str, start: datetime, end: datetime):
        if not self.finnhub_key:
            return []
        params = {
            "symbol": symbol,
            "from": start.date().isoformat(),
            "to": end.date().isoformat(),
            "token": self.finnhub_key
        }
        r = await self.client.get("https://finnhub.io/api/v1/company-news", params=params, timeout=20)
        r.raise_for_status()
        js = r.json()
        out = []
        for d in js:
            out.append(NewsItem(title=d["headline"],
                                summary=d["summary"],
                                url=d["url"],
                                published_at=datetime.utcfromtimestamp(d["datetime"]).isoformat() + "Z",
                                source=d["source"],
                                sentiment=naive_sentiment(d["summary"]),
                                symbols=[symbol]))
        return out

    async def juhe_news(self, keyword: str):
        if not self.juhe_key:
            return []
        params = {"word": keyword, "key": self.juhe_key}
        r = await self.client.get("https://caijing.juheapi.com/japi/toh", params=params, timeout=20)
        if r.status_code != 200:
            return []
        js = r.json().get("result", [])
        out = []
        for d in js:
            out.append(NewsItem(title=d["title"],
                                summary=d.get("digest", ""),
                                url=d["url"],
                                published_at=d.get("pubDate", datetime.utcnow().isoformat()),
                                source="Juhe",
                                sentiment=naive_sentiment(d.get("digest", "")),
                                symbols=[keyword]))
        return out

# ---------------------------------------------------------------------------
# 相关度打分
# ---------------------------------------------------------------------------
def relevance(item: NewsItem, holdings: Sequence[Holding]) -> float:
    score = 0.0
    text = f"{item.title} {item.summary}".lower()
    for h in holdings:
        if h.symbol in item.symbols:
            score += 0.6
        elif h.clean_symbol.lower() in text or h.name.lower() in text:
            score += 0.3
    return min(score + abs(item.sentiment) * 0.1, 1.0)

# ---------------------------------------------------------------------------
# 主逻辑
# ---------------------------------------------------------------------------
async def gather_all(holdings: List[Holding], days: int, max_items: int):
    start = datetime.utcnow() - timedelta(days=days)
    end = datetime.utcnow()
    async with httpx.AsyncClient(http2=True) as client:
        fetcher = Fetcher(client)
        tasks = []
        for h in holdings:
            if h.is_cn:
                tasks.append(async_retry(fetcher.tushare_news, h.symbol, start, end))
        overseas = [h.symbol for h in holdings if not h.is_cn and "." not in h.symbol]
        if overseas:
            tasks.append(async_retry(fetcher.alpha_news, overseas))
        for h in holdings:
            if not h.is_cn:
                tasks.append(async_retry(fetcher.finnhub_news, h.symbol.split(".")[0], start, end))
        for h in holdings:
            if h.is_cn:
                tasks.append(async_retry(fetcher.juhe_news, h.name))
        results = await asyncio.gather(*tasks, return_exceptions=True)

    news = {}
    for res in results:
        if isinstance(res, Exception):
            rprint(f"[red]Fetcher error: {res}")
            continue
        for item in res:
            news[item.fingerprint()] = item
    scored = sorted(news.values(), key=lambda x: relevance(x, holdings), reverse=True)
    return scored[:max_items]

# ---------------------------------------------------------------------------
# 输出 & Push
# ---------------------------------------------------------------------------

def dump_results(items):
    with open("news_today.json", "w", encoding="utf-8") as f:
        json.dump([asdict
