# -*- coding: utf-8 -*-
"""news_pipeline.py  ——  多源财经新闻聚合器（修正版）

核心特性
-----------
* **动态持仓**：读取 `holdings.json` 或环境变量 `HOLDINGS_JSON`。
* **多源抓取**：TuShare、AkShare、Alpha Vantage、Finnhub、聚合数据。
* **异步并发**：`httpx.AsyncClient` + 指数退避重试。
* **去重/相关度**：URL 指纹去重 +“直接命中→关键词→情绪”0‑1 评分。
* **输出/推送**：产出 `news_today.json`、`briefing.md` 并推送到 Server 酱 / Telegram。

依赖：
```bash
pip install httpx python-dateutil rich
# 若需 OpenAI 情绪分析：pip install openai
```
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from hashlib import md5
from pathlib import Path
from typing import List, Sequence

import httpx
from dateutil import parser as dtparse
from rich import print as rprint

# ───────────────────────────── 数据类 ─────────────────────────────
@dataclass
class Holding:
    symbol: str
    name: str
    weight: float = 0.0

    @property
    def clean(self) -> str:  # 去掉交易所后缀
        return self.symbol.split(".")[0]

    @property
    def is_cn(self) -> bool:
        return self.symbol.endswith((".SH", ".SZ"))

@dataclass
class NewsItem:
    title: str
    summary: str
    url: str
    published_at: str  # ISO‑8601
    source: str
    sentiment: float  # −1~1
    symbols: List[str]

    def fp(self) -> str:
        return md5(self.url.encode()).hexdigest()

# ───────────────────────────── 工具 ─────────────────────────────
_SENT_RE = re.compile(r"\b(upbeat|bullish|positive)\b|\b(downbeat|bearish|negative)\b", re.I)

def naive_sent(text: str) -> float:
    """极简情绪打分：正面词 +1 / 负面词 −1。"""
    pos = len(_SENT_RE.findall(text))
    # 简化：把正/负面合计视为正，真实项目中建议使用情绪 API
    return max(min(pos * 0.1, 1.0), -1.0)

async def retry(fn, *args, retries: int = 3, **kw):
    for i in range(retries):
        try:
            return await fn(*args, **kw)
        except Exception as e:
            if i == retries - 1:
                raise
            await asyncio.sleep(2 ** i)

# ───────────────────────────── Fetcher ─────────────────────────────
class Fetcher:
    def __init__(self, client: httpx.AsyncClient):
        self.c = client
        self.tk = os.getenv("TUSHARE_TOKEN")
        self.ak = os.getenv("ALPHA_KEY")
        self.fk = os.getenv("FINNHUB_KEY")
        self.jk = os.getenv("JUHE_KEY")

    async def tushare(self, ts_code: str, start: datetime, end: datetime):
        if not self.tk:
            return []
        payload = {
            "api_name": "news",
            "token": self.tk,
            "params": {
                "src": "eastmoney",
                "start_date": start.strftime("%Y%m%d"),
                "end_date": end.strftime("%Y%m%d"),
            },
            "fields": "title,content,url,datetime,src",
        }
        r = await self.c.post("https://api.tushare.pro", json=payload, timeout=20)
        items = r.json().get("data", {}).get("items", [])
        out = []
        for title, content, link, dtstr, src in items:
            out.append(NewsItem(title, content[:120], link, dtparse.parse(dtstr).isoformat(), src, naive_sent(content), [ts_code]))
        return out

    async def alpha_news(self, tickers: Sequence[str]):
        if not self.ak or not tickers:
            return []
        params = {"function": "NEWS_SENTIMENT", "tickers": ",".join(tickers[:100]), "apikey": self.ak}
        r = await self.c.get("https://www.alphavantage.co/query", params=params, timeout=20)
        out = []
        for it in r.json().get("feed", []):
            syms = [s.get("ticker") for s in it.get("ticker_sentiment", []) if s.get("ticker")]
            out.append(NewsItem(it["title"], it["summary"], it["url"], it["time_published"], it.get("source", "AlphaV"), float(it.get("overall_sentiment_score", 0)), syms))
        return out

    async def finnhub(self, symbol: str, start: datetime, end: datetime):
        if not self.fk:
            return []
        params = {"symbol": symbol, "from": start.date().isoformat(), "to": end.date().isoformat(), "token": self.fk}
        r = await self.c.get("https://finnhub.io/api/v1/company-news", params=params, timeout=20)
        out = []
        for d in r.json():
            out.append(NewsItem(d["headline"], d["summary"], d["url"], datetime.utcfromtimestamp(d["datetime"]).isoformat() + "Z", d["source"], naive_sent(d["summary"]), [symbol]))
        return out

    async def juhe_news(self, keyword: str):
        if not self.jk:
            return []
        r = await self.c.get("https://caijing.juheapi.com/japi/toh", params={"word": keyword, "key": self.jk}, timeout=20)
        if r.status_code != 200:
            return []
        out = []
        for d in r.json().get("result", []):
            out.append(NewsItem(d["title"], d.get("digest", ""), d["url"], d.get("pubDate", datetime.utcnow().isoformat()), "Juhe", naive_sent(d.get("digest", "")), [keyword]))
        return out

# ───────────────────────────── 相关度 ─────────────────────────────

def relevance(item: NewsItem, holds: List[Holding]):
    text = f"{item.title} {item.summary}".lower()
    score = 0.0
    for h in holds:
        if h.symbol in item.symbols:
            score += 0.6
        elif h.clean.lower() in text or h.name.lower() in text:
            score += 0.3
    return min(score + abs(item.sentiment) * 0.1, 1.0)

# ───────────────────────────── 主流程 ─────────────────────────────
async def collect(holds: List[Holding], days: int, limit: int):
    start_dt = datetime.utcnow() - timedelta(days=days)
    end_dt = datetime.utcnow()
    async with httpx.AsyncClient(http2=True) as client:
        f = Fetcher(client)
        tasks = []
        # A 股
        for h in holds:
            if h.is_cn:
                tasks.append(retry(f.tushare, h.symbol, start_dt, end_dt))
        # Alpha 批量
        overseas = [h.symbol for h in holds if not h.is_cn and "." not in h.symbol]
        if overseas:
            tasks.append(retry(f.alpha_news, overseas))
        # Finnhub
        for h in holds:
            if not h.is_cn:
                tasks.append(retry(f.finnhub, h.symbol.split(".")[0], start_dt, end_dt))
        # 聚合数据
        for h in holds:
            if h.is_cn:
                tasks.append(retry(f.juhe_news, h.name))
        results = await asyncio.gather(*tasks, return_exceptions=True)

    pool = {}
    for res in results:
        if isinstance(res, Exception):
            rprint(f"[red]Fetcher error: {res}")
            continue
        for it in res:
            pool[it.fp()] = it
    ranked = sorted(pool.values(), key=lambda x: relevance(x, holds), reverse=True)[:limit]
    return ranked

# ───────────────────────────── 输出 ─────────────────────────────

def write_files(items: List[NewsItem]):
    Path("news_today.json").write_text(json.dumps([asdict(i) for i in items], ensure_ascii=False, indent=2), encoding="utf-8")
    lines = ["# 今日重点财经新闻\n"]
    for idx, it in enumerate(items, 1):
        ts = dtparse.isoparse(it.published_at).strftime("%m-%d %H:%M")
        lines.append(f"{idx}. **{it.title}** [{it.source}]({it.url}) ({ts})")
    md = "\n".join(lines)
    Path("briefing.md").write_text(md, encoding="utf-8")
    return md

async def push_server酱(text: str):
    key = os.getenv("SCKEY")
    if key:
        await httpx.AsyncClient().post(f"https://
