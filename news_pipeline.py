# -*- coding: utf-8 -*-
"""news_pipeline.py — 多源财经新闻聚合器（stable-r4）

变更概要（2025-08-06）
────────────────────
* FIX  Tushare 在 token 失效或 rate-limit 时返回 {"code":xx,"msg":...}，
       之前直接 .get('items') 会触发 NoneType 异常；现已捕获并记录。
* FIX  聚合数据接口对关键词含 “ETF”/“Fund” 经常 404，
       现在自动去掉 “ETF”“指数”“基金”等尾缀后再请求。
* CHG  Juhe 接口改用 “get” 端点 /fin – 更贴近实时财经，
       并增加 rate-limit / 404 处理。
* MISC 细化日志级别，防抖 429：RSSHub 4xx 时等待 2 s 再重试一次。
"""

from __future__ import annotations
import argparse, asyncio, json, os, re, sys
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from hashlib import md5
from pathlib import Path
from typing import List, Sequence
import httpx
from dateutil import parser as dtparse
import logging
import random
import xml.etree.ElementTree as ET

# ─── 日志 ───
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log', mode='a', encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ],
)

# ─── 数据类 ───
@dataclass
class Holding:
    symbol: str
    name: str
    weight: float = 0.0

    @property
    def clean(self):  # 510300.SH → 510300
        return self.symbol.split('.')[0]

    @property
    def is_cn(self):
        return self.symbol.endswith(('.SH', '.SZ'))

@dataclass
class NewsItem:
    title: str
    summary: str
    url: str
    published_at: str
    source: str
    sentiment: float
    symbols: List[str]

    def fp(self):
        return md5(self.url.encode()).hexdigest()

# ─── 简易情感分析 ───
_SENT_RE = re.compile(
    r"\b(upbeat|bullish|positive|beat|surge|rally|涨)\b|"
    r"\b(downbeat|bearish|negative|miss|slump|跌)\b",
    re.I,
)

def naive_sent(text: str) -> float:
    pos = len(_SENT_RE.findall(text))
    return max(min(pos * 0.1, 1.0), -1.0)

async def retry(fn, *a, retries=3, **k):
    for i in range(retries):
        try:
            return await fn(*a, **k)
        except Exception as e:
            if i == retries - 1:
                raise
            backoff = 2 ** i + random.random()
            logging.warning(f"{fn.__name__} retry {i+1}/{retries} after error: {e}")
            await asyncio.sleep(backoff)

# ─── Fetcher ───
class Fetcher:
    def __init__(s, c: httpx.AsyncClient):
        s.c = c
        s.tk = os.getenv('TUSHARE_TOKEN')
        s.ak = os.getenv('ALPHA_KEY')
        s.fk = os.getenv('FINNHUB_KEY')
        s.jk = os.getenv('JUHE_KEY')

    # —— A 股 → Tushare —— #
    async def tushare(s, code: str, start: datetime, end: datetime):
        if not s.tk:
            return []
        payload = {
            "api_name": "news",
            "token": s.tk,
            "params": {
                "src": "eastmoney",
                "start_date": start.strftime('%Y%m%d'),
                "end_date": end.strftime('%Y%m%d'),
                "limit": 80,
            },
            "fields": "title,content,url,datetime,src",
        }
        r = await s.c.post('https://api.tushare.pro', json=payload, timeout=20)
        js = r.json()
        if js.get('code'):  # 非 0 代表错误 / 频率限制
            logging.error(f"Tushare {code} error {js.get('code')}: {js.get('msg')}")
            return []
        items = js.get('data', {}).get('items') or []
        out = []
        for t, con, url, dtstr, src in items:
            out.append(
                NewsItem(
                    t,
                    con[:120],
                    url,
                    dtparse.parse(dtstr).isoformat(),
                    src,
                    naive_sent(con),
                    [code],
                )
            )
        return out

    # —— 美股/全球 → Alpha Vantage —— #
    async def alpha_news(s, tickers: Sequence[str]):
        if not s.ak or not tickers:
            return []
        r = await s.c.get(
            'https://www.alphavantage.co/query',
            params={
                'function': 'NEWS_SENTIMENT',
                'tickers': ','.join(tickers[:100]),
                'apikey': s.ak,
            },
            timeout=20,
        )
        feed = r.json().get('feed')
        if not isinstance(feed, list):
            return []
        return [
            NewsItem(
                it['title'],
                it['summary'],
                it['url'],
                it['time_published'],
                it.get('source', 'AV'),
                float(it.get('overall_sentiment_score', 0)),
                [x.get('ticker') for x in it.get('ticker_sentiment', []) if x.get('ticker')],
            )
            for it in feed
        ]

    # —— 美股 → Finnhub —— #
    async def finnhub(s, sym: str, start: datetime, end: datetime):
        if not s.fk:
            return []
        r = await s.c.get(
            'https://finnhub.io/api/v1/company-news',
            params={'symbol': sym, 'from': start.date(), 'to': end.date(), 'token': s.fk},
            timeout=20,
        )
        if r.status_code != 200:
            logging.error(f"Finnhub {sym} HTTP {r.status_code}")
            return []
        return [
            NewsItem(
                d['headline'],
                d['summary'],
                d['url'],
                datetime.utcfromtimestamp(d['datetime']).isoformat() + 'Z',
                d['source'],
                naive_sent(d['summary']),
                [sym],
            )
            for d in r.json()
        ]

    # —— 中文聚合数据 —— #
    async def juhe_finance(s, kw: str):
        if not s.jk:
            return []
        # 去掉 ETF/基金/指数 等噪声
        kw2 = re.sub(r'(ETF|基金|指数)$', '', kw, flags=re.I)
        r = await s.c.get(
            'https://v.juhe.cn/finance_news_headline',
            params={'num': 40, 'word': kw2, 'key': s.jk},
            timeout=20,
        )
        if r.status_code != 200:
            logging.warning(f"Juhe {kw2} HTTP {r.status_code}")
            return []
        js = r.json()
        if js.get('error_code'):
            logging.warning(f"Juhe {kw2} error {js.get('error_code')}: {js.get('reason')}")
            return []
        res = js.get('result') or []
        return [
            NewsItem(
                d['title'],
                d.get('summary', '')[:120],
                d['url'],
                d.get('ctime', datetime.utcnow().isoformat()),
                '聚合财经',
                naive_sent(d.get('summary', '')),
                [kw],
            )
            for d in res
        ]

    # —— 通用 RSS —— #
    async def rss_feed(s, url: str, source: str):
        try:
            r = await s.c.get(url, timeout=20)
            if r.status_code == 429:
                await asyncio.sleep(2)  # 简易退避
                r = await s.c.get(url, timeout=20)
            if r.status_code != 200:
                logging.warning(f"{source} fetch HTTP {r.status_code}")
                return []
            root = ET.fromstring(r.text)
            items = []
            for item in root.findall('.//item'):
                title = item.findtext('title') or ''
                link = item.findtext('link') or ''
                summary = item.findtext('description') or item.findtext('summary') or ''
                pubDate = item.findtext('pubDate') or ''
                try:
                    dt = dtparse.parse(pubDate) if pubDate else None
                    ts = dt.isoformat() if dt else datetime.utcnow().isoformat()
                except Exception:
                    ts = datetime.utcnow().isoformat()
                items.append(
                    NewsItem(title, summary[:120], link, ts, source, naive_sent(title + summary), [])
                )
            logging.info(f"Fetched {len(items)} items from {source}")
            return items
        except Exception as e:
            logging.error(f"RSS {source} error: {e}")
            return []

# ─── 相关度评分 ───
def relevance(it: NewsItem, holds: list[Holding]) -> float:
    txt = f"{it.title} {it.summary}".lower()
    sc = 0.0
    for h in holds:
        if h.symbol in it.symbols:
            sc += 0.6
        elif h.clean.lower() in txt or h.name.lower() in txt:
            sc += 0.3
        if h.is_cn and h.name.endswith('ETF'):
            alt = h.name.lower().removesuffix('etf')
            if alt and alt in txt:
                sc += 0.3
    return min(sc + abs(it.sentiment) * 0.1, 1.0)

# ─── 主流程 ───
async def collect(holds: list[Holding], days: int, limit: int):
    st = datetime.utcnow() - timedelta(days=days)
    ed = datetime.utcnow()
    async with httpx.AsyncClient(http2=False) as cli:
        f = Fetcher(cli)
        tasks = []

        for h in holds:
            if h.is_cn:
                tasks.append(retry(f.tushare, h.symbol, st, ed))
            else:
                tasks.append(retry(f.finnhub, h.clean, st, ed))

        sym_oversea = [h.symbol for h in holds if not h.is_cn and '.' not in h.symbol]
        if sym_oversea:
            tasks.append(retry(f.alpha_news, sym_oversea))

        for h in holds:
            tasks.append(retry(f.juhe_finance, h.name))

        # 额外 RSS 源
        rss_list = [
            ("https://rsshub.app/cls/telegraph", "财联社"),
            ("https://rsshub.app/10jqka/realtimenews", "同花顺"),
            ("https://rsshub.app/wallstreetcn/news", "华尔街见闻"),
            ("https://rss.sina.com.cn/roll/finance/hot_roll.xml", "新浪财经"),
            ("https://a.jiemian.com/index.php?m=article&a=rss", "界面新闻"),
            ("https://rsshub.app/yicai/brief", "第一财经"),
        ]
        for u, sname in rss_list:
            tasks.append(retry(f.rss_feed, u, sname))

        res = await asyncio.gather(*tasks, return_exceptions=True)

    pool: dict[str, NewsItem] = {}
    for r in res:
        if isinstance(r, Exception):
            logging.error(f"Fetcher error: {r}")
            continue
        for it in r:
            pool[it.fp()] = it

    items = sorted(pool.values(), key=lambda x: relevance(x, holds), reverse=True)[:limit]
    logging.info(f"Collected {len(items)} news items (pre-filter)")
    return items

# ─── 输出 & 推送 ───
def write_files(items: list[NewsItem]) -> str:
    Path('news_today.json').write_text(
        json.dumps([asdict(i) for i in items], ensure_ascii=False, indent=2),
        encoding='utf-8',
    )
    lines = ['今日重点财经新闻']
    for idx, it in enumerate(items, 1):
        ts = dtparse.isoparse(it.published_at).strftime('%m-%d %H:%M')
        lines.append(f"{idx}. {it.title} ({it.source}, {ts}) {it.url}")
    txt = '\n'.join(lines)
    Path('briefing.md').write_text(txt, encoding='utf-8')
    return txt

async def push_serverchan(text: str):
    key = os.getenv('SCKEY')
    if not key:
        return
    r = await httpx.AsyncClient().post(
        f'https://sctapi.ftqq.com/{key}.send',
        data={'text': '每日投资资讯', 'desp': text},
        timeout=20,
    )
    if r.status_code != 200:
        logging.error(f"ServerChan HTTP {r.status_code}: {r.text[:120]}")

async def push_telegram(text: str):
    tok = os.getenv("TELEGRAM_BOT_TOKEN")
    cid = os.getenv("TELEGRAM_CHAT_ID")
    if not tok or not cid:
        return
    chunks = [text[i : i + 3500] for i in range(0, len(text), 3500)]
    async with httpx.AsyncClient() as c:
        for ch in chunks:
            rr = await c.post(
                f"https://api.telegram.org/bot{tok}/sendMessage",
                data={'chat_id': cid, 'text': ch},
                timeout=20,
            )
            if rr.status_code != 200:
                logging.error(f"Telegram HTTP {rr.status_code}: {rr.text[:120]}")
                break

# ─── CLI ───
def parse_holds(pth: str | None):
    if pth and Path(pth).is_file():
        return [Holding(**d) for d in json.loads(Path(pth).read_text('utf-8'))]
    env = os.getenv('HOLDINGS_JSON')
    if env:
        return [Holding(**d) for d in json.loads(env)]
    return []

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--holdings', default='holdings.json')
    ap.add_argument('--days', type=int, default=1)
    ap.add_argument('--max', type=int, default=40)
    args = ap.parse_args()

    holds = parse_holds(args.holdings)
    if not holds:
        logging.warning('⚠️  持仓为空')
        return

    items = asyncio.run(collect(holds, args.days, args.max))
    txt = write_files(items)
    asyncio.run(push_serverchan(txt))
    asyncio.run(push_telegram(txt))
    logging.info(f"✅ 输出 {len(items)} 条")

if __name__ == '__main__':
    main()
