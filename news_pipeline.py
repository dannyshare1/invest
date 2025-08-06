# -*- coding: utf-8 -*-
"""news_pipeline.py — 多源财经新闻聚合器（stable-r3）
修复内容
--------
1. **Alpha Vantage feed==None** → 返回空列表而非抛错。
2. **push_telegram**：正确读取 `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID`，并用 `chat_id` 参数；捕获错误码。
3. 移除上一版 patch 中残留的无用代码段，确保脚本可运行。
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

# 设置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ─── 数据类 ───
@dataclass
class Holding:
    symbol: str; name: str; weight: float = 0.0
    @property
    def clean(self): return self.symbol.split('.')[0]
    @property
    def is_cn(self): return self.symbol.endswith(('.SH', '.SZ'))

@dataclass
class NewsItem:
    title: str; summary: str; url: str; published_at: str; source: str; sentiment: float; symbols: List[str]
    def fp(self): return md5(self.url.encode()).hexdigest()

# ─── 工具 ───
_SENT_RE = re.compile(r"\b(upbeat|bullish|positive)\b|\b(downbeat|bearish|negative)\b", re.I)

def naive_sent(text):
    pos = len(_SENT_RE.findall(text))
    return max(min(pos * 0.1, 1.0), -1.0)

async def retry(fn, *a, retries=3, **k):
    for i in range(retries):
        try:
            return await fn(*a, **k)
        except Exception as e:
            if i == retries - 1:
                logging.error(f"Retry failed after {retries} attempts: {e}")
                raise
            logging.warning(f"Attempt {i+1} failed, retrying...")
            await asyncio.sleep(2 ** i)

# ─── Fetcher ───
class Fetcher:
    def __init__(self, c):
        self.c = c
        self.tk = os.getenv('TUSHARE_TOKEN')
        self.ak = os.getenv('ALPHA_KEY')
        self.fk = os.getenv('FINNHUB_KEY')
        self.jk = os.getenv('JUHE_KEY')

    async def tushare(self, code, start, end):
        if not self.tk:
            logging.info("Tushare token未设置，跳过")
            return []
        p = {
            "api_name": "news",
            "token": self.tk,
            "params": {"src": "eastmoney", "start_date": start.strftime('%Y%m%d'), "end_date": end.strftime('%Y%m%d')},
            "fields": "title,content,url,datetime,src"
        }
        try:
            r = await self.c.post('https://api.tushare.pro', json=p, timeout=20)
            r.raise_for_status()
            response_data = r.json()
            items = response_data.get('data', {}).get('items', [])
            if not isinstance(items, list):
                logging.warning(f"Tushare 返回非预期格式: {response_data}")
                return []
            logging.info(f"Tushare 获取到 {len(items)} 条新闻")
            return [
                NewsItem(t, con[:120], url, dtparse.parse(dtstr).isoformat(), src, naive_sent(con), [code])
                for t, con, url, dtstr, src in items
            ]
        except httpx.HTTPStatusError as e:
            logging.error(f"Tushare HTTP 错误: {e.response.status_code} - {e.response.text}")
            return []
        except Exception as e:
            logging.error(f"Tushare 请求失败: {e}")
            return []

    async def alpha_news(self, tickers: Sequence[str]):
        if not self.ak or not tickers:
            logging.info("Alpha Vantage key未设置或没有ticker，跳过")
            return []
        params = {
            'function': 'NEWS_SENTIMENT',
            'tickers': ','.join(tickers[:100]),
            'apikey': self.ak,
        }
        try:
            r = await self.c.get('https://www.alphavantage.co/query', params=params, timeout=20)
            r.raise_for_status()
            js = r.json()
            feed = js.get('feed')
            if feed is None:
                logging.warning("Alpha Vantage 返回 None 或非预期格式: {js}")
                return []
            if not isinstance(feed, list):
                logging.warning(f"Alpha Vantage 返回非预期格式: {feed}")
                return []

            out = []
            for it in feed:
                syms = [x.get('ticker') for x in it.get('ticker_sentiment', []) if x.get('ticker')]
                out.append(
                    NewsItem(
                        it['title'],
                        it['summary'],
                        it['url'],
                        it.get('time_published'),
                        it.get('source', 'AV'),
                        float(it.get('overall_sentiment_score', 0)),
                        syms,
                    )
                )
            logging.info(f"Alpha Vantage 获取到 {len(out)} 条新闻")
            return out
        except httpx.HTTPStatusError as e:
            logging.error(f"Alpha Vantage HTTP 错误: {e.response.status_code} - {e.response.text}")
            return []
        except Exception as e:
            logging.error(f"Alpha Vantage 请求失败: {e}")
            return []

    async def finnhub(self, sym, start, end):
        if not self.fk:
            logging.info("Finnhub token未设置，跳过")
            return []
        try:
            r = await self.c.get(
                'https://finnhub.io/api/v1/company-news',
                params={'symbol': sym, 'from': start.date(), 'to': end.date(), 'token': self.fk},
                timeout=20
            )
            r.raise_for_status()
            response_data = r.json()
            if not isinstance(response_data, list):
                logging.warning(f"Finnhub 返回非预期格式: {response_data}")
                return []
            logging.info(f"Finnhub 获取到 {len(response_data)} 条新闻")
            return [
                NewsItem(
                    d['headline'],
                    d['summary'],
                    d['url'],
                    datetime.utcfromtimestamp(d['datetime']).isoformat() + 'Z',
                    d['source'],
                    naive_sent(d['summary']),
                    [sym]
                ) for d in response_data
            ]
        except httpx.HTTPStatusError as e:
            logging.error(f"Finnhub HTTP 错误: {e.response.status_code} - {e.response.text}")
            return []
        except Exception as e:
            logging.error(f"Finnhub 请求失败: {e}")
            return []

    async def juhe_news(self, kw):
        if not self.jk:
            logging.info("Juhe key未设置，跳过")
            return []
        try:
            r = await self.c.get(
                'https://caijing.juheapi.com/japi/toh',
                params={'word': kw, 'key': self.jk},
                timeout=20
            )
            r.raise_for_status()
            response_data = r.json()
            result = response_data.get('result', [])
            if not isinstance(result, list):
                logging.warning(f"Juhe 返回非预期格式: {response_data}")
                return []
            logging.info(f"Juhe 获取到 {len(result)} 条新闻")
            return [
                NewsItem(
                    d['title'],
                    d.get('digest', ''),
                    d['url'],
                    d.get('pubDate', datetime.utcnow().isoformat()),
                    'Juhe',
                    naive_sent(d.get('digest', '')),
                    [kw]
                ) for d in result
            ]
        except httpx.HTTPStatusError as e:
            logging.error(f"Juhe HTTP 错误: {e.response.status_code} - {e.response.text}")
            return []
        except Exception as e:
            logging.error(f"Juhe 请求失败: {e}")
            return []

# ─── 相关度 ───
def relevance(it, holds):
    txt = f"{it.title} {it.summary}".lower(); score = 0.0
    for h in holds:
        if h.symbol in it.symbols:
            score += 0.6
        elif h.clean.lower() in txt or h.name.lower() in txt:
            score += 0.3
    return min(score + abs(it.sentiment) * 0.1, 1.0)

# ─── 主流程 ───
async def collect(holds, days, limit):
    st = datetime.utcnow() - timedelta(days=days); ed = datetime.utcnow()
    async with httpx.AsyncClient(http2=False) as cli:
        f = Fetcher(cli); tasks = []
        for h in holds:
            if h.is_cn:
                tasks.append(retry(f.tushare, h.symbol, st, ed))
        ovs = [h.symbol for h in holds if not h.is_cn and '.' not in h.symbol]
        if ovs:
            tasks.append(retry(f.alpha_news, ovs))
        for h in holds:
            if not h.is_cn:
                tasks.append(retry(f.finnhub, h.symbol.split('.')[0], st, ed))
        for h in holds:
            if h.is_cn:
                tasks.append(retry(f.juhe_news, h.name))
        res = await asyncio.gather(*tasks, return_exceptions=True)
    pool = {}
    for r in res:
        if isinstance(r, Exception):
            logging.error(f"Fetcher error: {r}")
            continue
        for it in r:
            pool[it.fp()] = it
    return sorted(pool.values(), key=lambda x: relevance(x, holds), reverse=True)[:limit]

# ─── 文件输出 ───
def write_files(items):
    if not items:
        logging.warning("没有获取到任何新闻，无法生成文件")
        return ""
    news_json_path = Path('news_today.json')
    briefing_md_path = Path('briefing.md')
    news_json_path.write_text(json.dumps([asdict(i) for i in items], ensure_ascii=False, indent=2), encoding='utf-8')
    lines = ['# 今日重点财经新闻\n']
    for idx, it in enumerate(items, 1):
        ts = dtparse.isoparse(it.published_at).strftime('%m-%d %H:%M')
        lines.append(f"{idx}. **{it.title}** [{it.source}]({it.url}) ({ts})")
    md_content = '\n'.join(lines)
    briefing_md_path.write_text(md_content, encoding='utf-8')
    logging.info(f"生成了 {len(items)} 条新闻，保存到 {news_json_path} 和 {briefing_md_path}")
    return md_content

# ─── 推送 ───
async def push_serverchan(text):
    key = os.getenv('SCKEY')
    if not key:
        logging.info("Server酱 Key 未设置，跳过推送")
        return
    try:
        await httpx.AsyncClient().post(
            f'https://sctapi.ftqq.com/{key}.send',
            data={'text': '每日投资资讯', 'desp': text},
            timeout=20
        )
        logging.info("Server酱推送成功")
    except Exception as e:
        logging.error(f"Server酱推送失败: {e}")

async def push_telegram(text: str):
    """Send plain-text message to Telegram. Uses the same logic as daily_push_qwen.py
    (chunk to ≤ 3 500 chars, no Markdown)."""
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        logging.info("Telegram Token 或 Chat ID 未设置，跳过推送")
        return

    chunks = [text[i:i + 3500] for i in range(0, len(text), 3500)]
    async with httpx.AsyncClient() as client:
        for chunk in chunks:
            try:
                resp = await client.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    data={"chat_id": chat_id, "text": chunk},  # plain text
                    timeout=20,
                )
                resp.raise_for_status()
                logging.info("Telegram 推送成功")
            except httpx.HTTPStatusError as e:
                logging.error(f"Telegram HTTP 错误 {resp.status_code}: {resp.text}")
                break
            except Exception as e:
                logging.error(f"Telegram 推送失败: {e}")
                break

# ─── CLI ───
def parse_holds(pth: str | None):
    if pth and Path(pth).is_file():
        data = json.loads(Path(pth).read_text('utf-8'))
    else:
        data = json.loads(os.getenv('HOLDINGS_JSON', '[]'))
    return [Holding(**d) for d in data]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--holdings', default='holdings.json')
    ap.add_argument('--days', type=int, default=1)
    ap.add_argument('--max', type=int, default=40)
    args = ap.parse_args()
    holds = parse_holds(args.holdings)
    if not holds:
        logging.warning('持仓为空')
        return
    items = asyncio.run(collect(holds, args.days, args.max))
    md = write_files(items)
    asyncio.run(push_serverchan(md))
    asyncio.run(push_telegram(md))
    logging.info(f'✅ 输出 {len(items)} 条')

if __name__ == '__main__':
    main()



