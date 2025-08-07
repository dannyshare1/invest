# -*- coding: utf-8 -*-
"""
news_pipeline.py — 聚合+摘要 r6  (2025-08-07)
"""
from __future__ import annotations
import argparse, asyncio, json, os, random, re, logging, html, sys
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from hashlib import md5
from pathlib import Path
from typing import List, Sequence
import httpx
from dateutil import parser as dtparse
_SENT_RE=re.compile(r"(利好|上涨|飙升|反弹|大涨|收涨|upbeat|bullish|positive|利空|下跌|暴跌|收跌|bearish|negative)",re.I)

def naive_sent(t): return max(min(len(_SENT_RE.findall(t))*0.1,1.0),-1.0)
def strip_html(txt): return re.sub(r"<[^>]+>","",html.unescape(txt))
def digest(txt,l=180):
    txt=strip_html(txt).replace("\u3000"," ").replace("\xa0"," ").strip()
    out=[]
    for s in re.split(r"[。.!！？\n]",txt):
        s=s.strip()
        if s: out.append(s)
        if len("".join(out))>=l or len(out)>=3: break
    return "。".join(out)[:l]

@dataclass
class NewsItem:
    title:str; summary:str; url:str; published_at:str; source:str; sentiment:float; symbols:List[str]
    def fp(self): return md5(self.url.encode()).hexdigest()

@dataclass
class Holding:
    symbol:str; name:str; weight:float=0.0
    @property
    def clean(self): return self.symbol.split('.')[0]
    @property
    def is_cn(self): return self.symbol.endswith(('.SH','.SZ'))

async def retry(fn,*a,retries=3,**k):
    for i in range(retries):
        try:return await fn(*a,**k)
        except Exception as e:
            if i==retries-1: raise
            await asyncio.sleep(2**i+random.uniform(1,2))

class Fetcher:
    sem=asyncio.Semaphore(1)
    def __init__(s,c): s.c=c; s.tk=os.getenv('TUSHARE_TOKEN'); s.ak=os.getenv('ALPHA_KEY')
    async def tushare(s,st,ed,log):
        log.append(f"Tushare {st:%Y%m%d}-{ed:%Y%m%d}")
        cache=Path(f"tushare_{st:%Y%m%d}.json")
        if cache.is_file(): return [NewsItem(**d) for d in json.loads(cache.read_text())]
        if not s.tk: return []
        body={"api_name":"news","token":s.tk,
              "params":{"src":"eastmoney","start_date":st.strftime('%Y%m%d'),"end_date":ed.strftime('%Y%m%d')},
              "fields":"title,content,url,datetime,src"}
        r=await s.c.post('https://api.tushare.pro',json=body,timeout=30)
        js=r.json() if r.content else {}
        items=js.get('data',{}).get('items',[]) if not js.get('code') else []
        out=[NewsItem(t,digest(con),url,dtparse.parse(dt).isoformat(),src,naive_sent(con),[])
             for t,con,url,dt,src in items]
        cache.write_text(json.dumps([asdict(i) for i in out],ensure_ascii=False))
        return out
    async def rss(s,url,src,log):
        log.append(f"RSS {src}")
        async with s.sem:
            await asyncio.sleep(random.uniform(1,2))
            try:
                r=await s.c.get(url,timeout=20)
                if r.status_code!=200: return []
                import xml.etree.ElementTree as ET
                root=ET.fromstring(r.text); out=[]
                for item in root.findall('.//item'):
                    ttl=item.findtext('title') or ''; link=item.findtext('link') or ''
                    desc=item.findtext('description') or ''
                    pub=item.findtext('pubDate') or datetime.utcnow().isoformat()
                    try: pub=dtparse.parse(pub).isoformat()
                    except: pub=datetime.utcnow().isoformat()
                    out.append(NewsItem(ttl,digest(desc or ttl),link,pub,src,naive_sent(ttl+desc),[]))
                await asyncio.sleep(random.uniform(5,6))
                return out
            except: return []

def rel(n,holds):
    txt=(n.title+n.summary).lower(); sc=0
    for h in holds:
        if h.clean.lower() in txt or h.name.lower() in txt: sc+=0.4
    return sc

async def collect(holds):
    st=datetime.utcnow()-timedelta(days=1); ed=datetime.utcnow()
    log=[]
    async with httpx.AsyncClient(timeout=30) as cli:
        f=Fetcher(cli)
        tasks=[retry(f.tushare,st,ed,log)]
        rss=[('https://rsshub.app/cls/telegraph','财联社'),
             ('https://rss.sina.com.cn/roll/finance/hot_roll.xml','新浪财经')]
        for u,s in rss: tasks.append(retry(f.rss,u,s,log))
        res=await asyncio.gather(*tasks,return_exceptions=True)
    for l in log: logging.info(f"KW: {l}")
    pool={}
    for r in res:
        if isinstance(r,Exception): continue
        for it in r: pool[it.fp()]=it
    return sorted(pool.values(),key=lambda x:rel(x,holds),reverse=True)[:40]

def write(items):
    Path('news_today.json').write_text(json.dumps([asdict(i) for i in items],ensure_ascii=False,indent=2))
    brief='今日重点财经新闻'
    for i,it in enumerate(items,1):
        brief+=f"\n{i}. {it.title}\n   {it.summary}"
    Path('briefing.md').write_text(brief); return brief

def parse_holds(fp):
    if Path(fp).is_file(): return [Holding(**d) for d in json.loads(Path(fp).read_text())]
    return []

def main():
    holds=parse_holds('holdings.json')
    items=asyncio.run(collect(holds))
    write(items)

if __name__=='__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler('pipeline.log','a'),logging.StreamHandler(sys.stdout)])
    main()
