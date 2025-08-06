# -*- coding: utf-8 -*-
"""news_pipeline.py — 多源财经新闻聚合器（稳定版）"""
from __future__ import annotations
import argparse, asyncio, json, os, re, sys
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from hashlib import md5
from pathlib import Path
from typing import List, Sequence
import httpx
from dateutil import parser as dtparse
from rich import print as rprint

# ─── 数据类 ───
@dataclass
class Holding:
    symbol: str; name: str; weight: float = 0.0
    @property
    def clean(self): return self.symbol.split(".")[0]
    @property
    def is_cn(self): return self.symbol.endswith((".SH", ".SZ"))

@dataclass
class NewsItem:
    title: str; summary: str; url: str; published_at: str; source: str; sentiment: float; symbols: List[str]
    def fp(self): return md5(self.url.encode()).hexdigest()

# ─── 辅助 ───
_SENT_RE = re.compile(r"\b(upbeat|bullish|positive)\b|\b(downbeat|bearish|negative)\b", re.I)

def naive_sent(text):
    pos = len(_SENT_RE.findall(text)); return max(min(pos * 0.1, 1.0), -1.0)
async def retry(fn,*a,retries=3,**k):
    for i in range(retries):
        try: return await fn(*a,**k)
        except Exception as e:
            if i==retries-1: raise; await asyncio.sleep(2**i)

# ─── Fetcher ───
class Fetcher:
    def __init__(s, c):
        s.c=c; s.tk=os.getenv('TUSHARE_TOKEN'); s.ak=os.getenv('ALPHA_KEY'); s.fk=os.getenv('FINNHUB_KEY'); s.jk=os.getenv('JUHE_KEY')
    async def tushare(s,code,start,end):
        if not s.tk: return []
        p={"api_name":"news","token":s.tk,"params":{"src":"eastmoney","start_date":start.strftime('%Y%m%d'),"end_date":end.strftime('%Y%m%d')},"fields":"title,content,url,datetime,src"}
        r=await s.c.post('https://api.tushare.pro',json=p,timeout=20); items=r.json().get('data',{}).get('items',[]); out=[]
        for t,con,url,dtstr,src in items:
            out.append(NewsItem(t,con[:120],url,dtparse.parse(dtstr).isoformat(),src,naive_sent(con),[code]))
        return out
    async def alpha_news(s,tickers):
        if not s.ak or not tickers: return []
        r=await s.c.get('https://www.alphavantage.co/query',params={'function':'NEWS_SENTIMENT','tickers':','.join(tickers[:100]),'apikey':s.ak},timeout=20)
        out=[]
        for it in r.json().get('feed',[]):
            syms=[x.get('ticker') for x in it.get('ticker_sentiment',[]) if x.get('ticker')]
            out.append(NewsItem(it['title'],it['summary'],it['url'],it['time_published'],it.get('source','AV'),float(it.get('overall_sentiment_score',0)),syms))
        return out
    async def finnhub(s,sym,start,end):
        if not s.fk: return []
        r=await s.c.get('https://finnhub.io/api/v1/company-news',params={'symbol':sym,'from':start.date(),'to':end.date(),'token':s.fk},timeout=20)
        out=[NewsItem(d['headline'],d['summary'],d['url'],datetime.utcfromtimestamp(d['datetime']).isoformat()+'Z',d['source'],naive_sent(d['summary']),[sym]) for d in r.json()]
        return out
    async def juhe_news(s,kw):
        if not s.jk: return []
        r=await s.c.get('https://caijing.juheapi.com/japi/toh',params={'word':kw,'key':s.jk},timeout=20)
        out=[NewsItem(d['title'],d.get('digest',''),d['url'],d.get('pubDate',datetime.utcnow().isoformat()),'Juhe',naive_sent(d.get('digest','')),[kw]) for d in r.json().get('result',[])] if r.status_code==200 else []
        return out

# ─── 相关度 ───
def relevance(it,holds):
    txt=f"{it.title} {it.summary}".lower(); s=0.0
    for h in holds:
        if h.symbol in it.symbols: s+=0.6
        elif h.clean.lower() in txt or h.name.lower() in txt: s+=0.3
    return min(s+abs(it.sentiment)*0.1,1.0)

# ─── 主流程 ───
async def collect(holds,days,limit):
    st=datetime.utcnow()-timedelta(days=days); ed=datetime.utcnow()
    async with httpx.AsyncClient(http2=False) as cli:
        f=Fetcher(cli); tasks=[]
        for h in holds:
            if h.is_cn: tasks.append(retry(f.tushare,h.symbol,st,ed))
        ov=[h.symbol for h in holds if not h.is_cn and '.' not in h.symbol]
        if ov: tasks.append(retry(f.alpha_news,ov))
        for h in holds:
            if not h.is_cn: tasks.append(retry(f.finnhub,h.symbol.split('.')[0],st,ed))
        for h in holds:
            if h.is_cn: tasks.append(retry(f.juhe_news,h.name))
        res=await asyncio.gather(*tasks,return_exceptions=True)
    pool={};
    for r in res:
        if isinstance(r,Exception): rprint(f"[red]Fetcher error: {r}"); continue
        for it in r: pool[it.fp()]=it
    return sorted(pool.values(),key=lambda x:relevance(x,holds),reverse=True)[:limit]

# ─── 输出 ───

def write_files(items):
    Path('news_today.json').write_text(json.dumps([asdict(i) for i in items],ensure_ascii=False,indent=2),encoding='utf-8')
    md=['# 今日重点财经新闻\n']
    for i,it in enumerate(items,1):
        md.append(f"{i}. **{it.title}** [{it.source}]({it.url}) ({dtparse.isoparse(it.published_at).strftime('%m-%d %H:%M')})")
    Path('briefing.md').write_text('\n'.join(md),encoding='utf-8')
    return '\n'.join(md)

async def push_serverchan(text):
    key=os.getenv('SCKEY');
    if not key: return
    async with httpx.AsyncClient() as c:
        await c.post(f'https://sctapi.ftqq.com/{key}.send',data={'text':'每日投资资讯','desp':text},timeout=20)

# Telegram 自动分段 + 错误显示
async with httpx.AsyncClient(http2=False) as c:
    for ch in chunks:
        resp = await c.post(
            f'https://api.telegram.org/bot{tok}/sendMessage',
            data={'chat_id': cid, 'text': ch, 'parse_mode': 'Markdown'},
            timeout=20)
        if resp.status_code != 200:
            rprint(f'[red]Telegram error {resp.status_code}: {resp.text}')
            break

    js = r.json()
    feed = js["feed"] if isinstance(js.get("feed"), list) else []

# ─── CLI ───

def parse_holds(path:str|None):
    if path and Path(path).is_file():
        data=json.loads(Path(path).read_text('utf-8'))
    else:
        data=json.loads(os.getenv('HOLDINGS_JSON','[]'))
    return [Holding(**d) for d in data]

def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--holdings',default='holdings.json'); ap.add_argument('--days',type=int,default=1); ap.add_argument('--max',type=int,default=30); args=ap.parse_args()
    holds=parse_holds(args.holdings)
    if not holds: rprint('[yellow]⚠️ 持仓为空'); return
    items=asyncio.run(collect(holds,args.days,args.max)); md=write_files(items)
    asyncio.run(push_serverchan(md)); asyncio.run(push_telegram(md)); rprint(f'[green]✅ 输出 {len(items)} 条')

if __name__=='__main__':
    main()
