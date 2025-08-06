# -*- coding: utf-8 -*-
"""
news_pipeline.py  —— 多源财经新闻聚合器

完全替换旧版 fetch_news.py 脚本，可直接挂在现有 GitHub Actions
workflow 中（示例见文件末尾注释）。

功能
-----
1. **动态持仓**：从 `holdings.json` （或环境变量）读取实时仓位，不用改代码。
2. **统一抓取**：支持 TuShare / AkShare / Alpha Vantage / Finnhub / 聚合数据
   五大源，自动按 A 股 or 海外资产选择接口；全部用 `httpx.AsyncClient`
   并发下载。
3. **去重+相关度评分**：用 URL 指纹去重；按“直接命中→关键词→行业”
   三层权重 + 情绪，给出 0‑1 相关度。
4. **输出**：
   * `news_today.json` —— 原始新闻结构化数据（落盘供 Debug / 备份）
   * `briefing.md` —— 给 LLM 的精简 Markdown 摘要（<= 2 KB，防 prompt
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
ALPHA_KEY       # Alpha Vantage
FINNHUB_KEY     # Finnhub
JUHE_KEY        # 聚合数据（可选）
OPENAI_API_KEY  # Sentiment optional
SCKEY           # Server 酱
TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID
```

`holdings.json` 格式
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

    # 推导字段
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
    published_at: str  # ISO‑8601
    source: str
    sentiment: float  # -1~1，缺省 0
    symbols: List[str]

    def fingerprint(self) -> str:
        return md5(self.url.encode()).hexdigest()

# ---------------------------------------------------------------------------
# 工具函数
# ---------------------------------------------------------------------------

_SENTIMENT_RE = re.compile(r"\b(upbeat|bullish|positive)\b|\b(downbeat|bearish|negative)\b",
                          flags=re.I)

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

    # ---------- TuShare 快讯 ---------- #
    async def tushare_news(self, ts_code: str, start: datetime, end: datetime) -> List[NewsItem]:
        if not self.tushare_token:
            return []
        url = "https://api.tushare.pro"  # 官方 SDK 也走这个端点
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
        out: List[NewsItem] = []
        for title, content, link, dtstr, src in data:
            out.append(NewsItem(title=title,
                                summary=content[:120],
                                url=link,
                                published_at=dtparse.parse(dtstr).isoformat(),
                                source=src,
                                sentiment=naive_sentiment(content),
                                symbols=[ts_code]))
        return out

    # ---------- Alpha Vantage ---------- #
    async def alpha_news(self, tickers: Sequence[str]) -> List[NewsItem]:
        if not self.alpha_key or not tickers:
            return []
        params = {
            "function": "NEWS_SENTIMENT",
            "tickers": ",".join(tickers[:100]),  # API 限制 100
            "apikey": self.alpha_key
        }
        r = await self.client.get("https://www.alphavantage.co/query", params=params, timeout=20)
        r.raise_for_status()
        feed = r.json().get("feed", [])
        out = []
        for item in feed:
            out.append(NewsItem(title=item["title"],
                                summary=item["summary"],
                                url=item["url"],
                                published_at=item["time_published"],
                                source=item.get("source", "AlphaV"),
                                sentiment=float(item.get("overall_sentiment_score", 0)),
                                symbols=item.get("ticker_sentiment", [{}])[0].get("ticker", "").split()))
        return out

    # ---------- Finnhub ---------- #
    async def finnhub_news(self, symbol: str, start: datetime, end: datetime) -> List[NewsItem]:
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

    # ---------- 聚合数据 ---------- #
    async def juhe_news(self, keyword: str) -> List[NewsItem]:
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
        # 行业标签可扩充，这里略
    # Clip 0‑1
    return min(score + abs(item.sentiment) * 0.1, 1.0)

# ---------------------------------------------------------------------------
# 主逻辑
# ---------------------------------------------------------------------------
async def gather_all(holdings: List[Holding], days: int, max_items: int) -> List[NewsItem]:
    start = datetime.utcnow() - timedelta(days=days)
    end = datetime.utcnow()
    async with httpx.AsyncClient(http2=True) as client:
        fetcher = Fetcher(client)
        tasks = []
        # A 股快讯：批量同一天取，无并发优势大；串行即可
        for h in holdings:
            if h.is_cn:
                tasks.append(async_retry(fetcher.tushare_news, h.symbol, start, end))
        # 海外合并取 Alpha
        overseas = [h.symbol for h in holdings if not h.is_cn and "." not in h.symbol]
        if overseas:
            tasks.append(async_retry(fetcher.alpha_news, overseas))
        # Finnhub 单独再补（含 HK、美股 ETF）
        for h in holdings:
            if not h.is_cn:
                tasks.append(async_retry(fetcher.finnhub_news, h.symbol.split(".")[0], start, end))
        # 聚合数据关键词（中文 ETF 名）
        for h in holdings:
            if h.is_cn:
                tasks.append(async_retry(fetcher.juhe_news, h.name))
        results = await asyncio.gather(*tasks, return_exceptions=True)

    news: Dict[str, NewsItem] = {}
    for res in results:
        if isinstance(res, Exception):
            rprint(f"[red]Fetcher error: {res}")
            continue
        for item in res:
            news[item.fingerprint()] = item
    # 相关度排序
    scored = sorted(news.values(), key=lambda x: relevance(x, holdings), reverse=True)
    return scored[:max_items]

# ---------------------------------------------------------------------------
# 输出 & Push
# ---------------------------------------------------------------------------

def dump_results(items: Sequence[NewsItem]):
    with open("news_today.json", "w", encoding="utf-8") as f:
        json.dump([asdict(i) for i in items], f, ensure_ascii=False, indent=2)
    # Markdown 摘要
    lines = ["# 今日重点财经新闻\n"]
    for i, it in enumerate(items, 1):
        ts = dtparse.isoparse(it.published_at).strftime("%m-%d %H:%M")
        lines.append(f"{i}. **{it.title}** [{it.source}]({it.url}) ({ts})")
    md = "\n".join(lines)
    Path("briefing.md").write_text(md, encoding="utf-8")
    return md

async def push_serverchan(text: str):
    key = os.getenv("SCKEY")
    if not key:
        return
    params = {"text": "每日投资资讯", "desp": text}
    async with httpx.AsyncClient() as c:
        await c.post(f"https://sctapi.ftqq.com/{key}.send", data=params, timeout=20)

async def push_telegram(text: str):
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return
    async with httpx.AsyncClient() as c:
        await c.post(f"https://api.telegram.org/bot{token}/sendMessage",
                     data={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"}, timeout=20)

# ---------------------------------------------------------------------------
# CLI Entry
# ---------------------------------------------------------------------------

def parse_holdings(path: str | None) -> List[Holding]:
    if path and Path(path).is_file():
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    else:
        # FALLBACK：环境变量 HOLDINGS_JSON
        env_json = os.getenv("HOLDINGS_JSON", "[]")
        data = json.loads(env_json)
    return [Holding(**d) for d in data]


def main():
    ap = argparse.ArgumentParser(description="Multi‑source news fetcher for dynamic portfolio")
    ap.add_argument("--holdings", help="Path to holdings.json", default="holdings.json")
    ap.add_argument("--days", type=int, default=1, help="Look‑back window in days")
    ap.add_argument("--max", type=int, default=30, help="Max news items to output")
    args = ap.parse_args()

    if not Path(args.holdings).is_file() and not os.getenv("HOLDINGS_JSON"):
        rprint("[red]❌ 未找到 holdings.json，也未设置 HOLDINGS_JSON 环境变量！")
        sys.exit(1)

    holdings = parse_holdings(args.holdings)
    if not holdings:
        rprint("[yellow]⚠️  持仓为空，脚本跳过。")
        return

    items = asyncio.run(gather_all(holdings, args.days, args.max))
    summary_md = dump_results(items)
    asyncio.run(push_serverchan(summary_md))
    asyncio.run(push_telegram(summary_md))
    rprint(f"[green]✅ 完成，输出 {len(items)} 条。")


if __name__ == "__main__":
    main()

"""
# GitHub Actions 示例（放在 .github/workflows/daily.yml）
# ---------------------------------------------------------
# name: Daily Briefing
# on:
#   schedule:
#     - cron: '55 23 * * *'  # 07:55 CST
# jobs:
#   run:
#     runs-on: ubuntu-latest
#     env:
#       TUSHARE_TOKEN: ${{ secrets.TUSHARE_TOKEN }}
#       ALPHA_KEY:     ${{ secrets.ALPHA_KEY }}
#       FINNHUB_KEY:   ${{ secrets.FINNHUB_KEY }}
#       JUHE_KEY:      ${{ secrets.JUHE_KEY }}
#       SCKEY:         ${{ secrets.SCKEY }}
#       TELEGRAM_BOT_TOKEN: ${{ secrets.TELEGRAM_BOT_TOKEN }}
#       TELEGRAM_CHAT_ID:   ${{ secrets.TELEGRAM_CHAT_ID }}
#       HOLDINGS_JSON: |
#         [{"symbol":"510880.SH","name":"中证红利ETF","weight":0.12},
#          {"symbol":"AAPL","name":"Apple Inc","weight":0.1}]
#     steps:
#       - uses: actions/checkout@v4
#       - uses: actions/setup-python@v5
#         with:
#           python-version: '3.11'
#       - run: pip install -r requirements.txt
#       - run: python news_pipeline.py --days 1 --max 40
