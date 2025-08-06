# -*- coding: utf-8 -*-
"""news_pipeline.py  ── 多源财经新闻聚合器

功能概览
---------
* **动态持仓**：读取 `holdings.json` 或环境变量 `HOLDINGS_JSON`。
* **多源抓取**：TuShare、AkShare、Alpha Vantage、Finnhub、聚合数据。
* **并发请求**：`httpx.AsyncClient` + 指数退避重试。
* **去重/打分**：URL 指纹去重，按“直接命中→关键词→情绪”0‑1 评分。
* **输出/推送**：生成 `news_today.json`、`briefing.md` 并推送到 Server 酱 + Telegram。

依赖：`pip install httpx python-dateutil rich`（情绪用 OpenAI 需另装 openai）。
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

# ────────────────────────── 数据结构 ──────────────────────────
@dataclass
class Holding:
    symbol: str
    name: str
    weight: float = 0.0

    @property
    def clean(self) -> str:  # 去掉 .SH / .SZ
        return self.symbol.split(".")[0]

    @property
    def is_cn(self) -> bool:
        return self.symbol.endswith((".SH", ".SZ"))

@dataclass
class NewsItem:
    title: str
    summary: str
    url: str
    published_at: str  # ISO 8601
    source: str
    sentiment: float
    symbols: List[str]

    def fp(self) -> str:
        return md5(self.url.encode()).hexdigest()

# ────────────────────────── 工具 ──────────────────────────
_SENT_RE = re.compile(r"\b(upbeat|bullish|positive)\b|\b(downbeat|bearish|negative)\b", re.I)

def naive_sent(text: str) -> float:
    matches = _SENT_RE.findall(text)
    if not matches:
        return 0.0
    pos = sum(bool(a) for a, _ in matches)
    neg = sum(bool(b) for _, b in matches)
    return (pos - neg) / max(pos + neg, 1)

async def retry(fn, *args, tries: int = 3, **kw):
    for i in range(tries):
        try:
            return await fn(*args, **kw)
        except Exception as e:
            if i == tries - 1:
                raise
            await asyncio.sleep(2 ** i)

# ────────────────────────── Fetcher ──────────────────────────
class Fetcher:
    def __init__(self, client: httpx.AsyncClient):
        self.c = client
        self.tu = os.getenv("TUSHARE_TOKEN")
        self.alpha = os.getenv("ALPHA_KEY")
        self.finn = os.getenv("FINNHUB_KEY")
        self.juhe = os.getenv("JUHE_KEY")

    # TuShare
    async def tushare(self, ts_code: str, start: str, end: str):
        if not self.tu:
            return []
        payload = {
            "api_name": "news",
            "token": self.tu,
            "params": {"src": "eastmoney", "start_date": start, "end_date": end},
            "fields": "title,content,url,datetime,src",
        }
        r = await self.c.post("https://api.tushare.pro", json=payload, timeout=20)
        items = r.json().get("data", {}).get("items", [])
        out = []
        for title, content, link, dtstr, src in items:
            out.append(NewsItem(title, content[:120], link, dtparse.parse(dtstr).isoformat(), src, naive_sentiment(content), [ts_code]))
        return out

    # Alpha Vantage
    async def alpha_v(self, tickers: Sequence[str]):
        if not self.alpha or not tickers:
            return []
        params = {"function": "NEWS_SENTIMENT", "tickers": ",".join(tickers[:100]), "apikey": self.alpha}
        r = await self.c.get("https://www.alphavantage.co/query", params=params, timeout=20)
        out = []
        for it in r.json().get("feed", []):
            syms = [s.get("ticker") for s in it.get("ticker_sentiment", []) if s.get("ticker")]
            out.append(NewsItem(it["title"], it["summary"], it["url"], it["time_published"], it.get("source", "AlphaV"), float(it.get("overall_sentiment_score", 0)), syms))
        return out

    # Finnhub
    async def finnhub(self, symbol: str, frm: str, to: str):
        if not self.finn:
            return []
        p = {"symbol": symbol, "from": frm, "to": to, "token": self.finn}
        r = await self.c.get("https://finnhub.io/api/v1/company-news", params=p, timeout=20)
        out = []
        for d in r.json():
            out.append(NewsItem(d["headline"], d["summary"], d["url"], datetime.utcfromtimestamp(d["datetime"]).isoformat() + "Z", d["source"], naive_sent(d["summary"]), [symbol]))
        return out

    # 聚合数据
    async def juhe_news(self, keyword: str):
        if not self.juhe:
            return []
        r = await self.c.get("https://caijing.juheapi.com/japi/toh", params={"word": keyword, "key": self.juhe}, timeout=20)
        if r.status_code != 200:
            return []
        out = []
        for d in r.json().get("result", []):
            out.append(NewsItem(d["title"], d.get("digest", ""), d["url"], d.get("pubDate", datetime.utcnow().isoformat()), "Juhe", naive_sent(d.get("digest", "")), [keyword]))
        return out

# ────────────────────────── 相关度 ──────────────────────────

def rel(item: NewsItem, holds: List[Holding]):
    t = f"{item.title} {item.summary}".lower()
    sc = 0.0
    for h in holds:
        if h.symbol in item.symbols:
            sc += 0.6
        elif h.clean.lower() in t or h.name.lower() in t:
            sc += 0.3
    return min(sc + abs(item.sentiment) * 0.1, 1.0)

# ────────────────────────── 主流程 ──────────────────────────
async def collect(holds: List[Holding], days: int, limit: int):
    start_dt = datetime.utcnow() - timedelta(days=days)
    start_str = start_dt.strftime("%Y-%m-%d")
    end_str = datetime.utcnow().strftime("%Y-%m-%d")
    async with httpx.AsyncClient(http2=True) as client:
        f = Fetcher(client)
        tasks = []
        for h in holds:
            if h.is_cn:
                tasks.append(retry(f.tushare, h.symbol, start_dt.strftime("%Y%m%d"), end_dt.strftime("%Y%m%d")))
        overseas = [h.symbol for h in holds if not h.is_cn and "." not in h.symbol]
        if overseas:
            tasks.append(retry(f.alpha_v, overseas))
        for h in holds:
            if not h.is_cn:
                tasks.append(retry(f.finnhub, h.symbol.split(".")[0], start_str, end_str))
        for h in holds if h.is_cn:
            tasks.append(retry(f.juhe_news, h.name))
        res = await asyncio.gather(*tasks, return_exceptions=True)
    pool = {}
    for r in res:
        if isinstance(r, Exception):
            rprint(f"[red]Fetcher error: {r}")
            continue
        for it in r:
            pool[it.fp()] = it
    ranked = sorted(pool.values(), key=lambda x: rel(x, holds), reverse=True)[:limit]
    return ranked

# ────────────────────────── 输出 ──────────────────────────

def write_out(items: List[NewsItem]):
    Path("news_today.json").write_text(json.dumps([asdict(i) for i in items], ensure_ascii=False, indent=2), encoding="utf-8")
    lines = ["# 今日重点财经新闻\n"]
    for i, it in enumerate(items, 1):
        ts = dtparse.isoparse(it.published_at).strftime("%m-%d %H:%M")
        lines.append(f"{i}. **{it.title}** [{it.source}]({it.url}) ({ts})")
    md = "\n".join(lines)
    Path("briefing.md").write_text(md, encoding="utf-8")
    return md

async def push_sc(text: str):
    key = os.getenv("SCKEY")
    if not key:
        return
    await httpx.AsyncClient().post(f"https://sctapi.ftqq.com/{key}.send", data={"text": "每日投资资讯", "desp": text}, timeout=20)

async def push_tg(text: str):
    tok, cid = os.getenv("TELEGRAM_BOT_TOKEN"), os.getenv("TELEGRAM_CHAT_ID")
    if not tok or not cid:
        return
    await httpx.AsyncClient().post(f"https://api.telegram.org/bot{tok}/sendMessage", data={"chat_id": cid, "text": text, "parse_mode": "Markdown"}, timeout=20)

# ────────────────────────── CLI ──────────────────────────

def load_holdings(path: str | None):
    if path and Path(path).is_file():
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    else:
        data = json.loads(os.getenv("HOLDINGS_JSON", "[]"))
    return [Holding(**d) for d in data]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--holdings", default="holdings.json")
    ap.add_argument("--days", type=int, default=1)
    ap.add_argument("--max", type=int, default=40)
    args = ap.parse_args()

    holds = load_holdings(args.holdings)
    if not holds:
        rprint("[yellow]⚠️  持仓为空，脚本终止。")
        sys.exit(0)

    items = asyncio.run(collect(holds, args.days, args.max))
    md = write_out(items)
    asyncio.run(push_sc(md))
    asyncio.run(push_tg(md))
    rprint(f"[green]✅ 完成，输出 {len(items)} 条。")

if __name__ == "__main__":
    main()
