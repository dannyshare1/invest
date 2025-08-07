# -*- coding: utf-8 -*-
"""
news_pipeline.py — r10 (fix async-generator issue)
• async generators → wrapped into awaitables
• 逻辑保持 r9：全部新闻写 news_all.csv、详细异常日志
"""
from __future__ import annotations
import asyncio, json, os, random, re, logging, html, sys, csv
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
import httpx
from dateutil import parser as dtparse

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler('pipeline.log','a'),
                              logging.StreamHandler(sys.stdout)])

_SENT=re.compile(r"(利好|上涨|飙升|反弹|大涨|收涨|upbeat|bullish|positive|利空|下跌|暴跌|收跌|bearish|negative)",re.I)
def digest(txt,l=180):
    txt=re.sub(r"<[^>]+>","",html.unescape(txt)).strip()
    parts=[s.strip() for s in re.split(r"[。.!！？\n]",txt) if s.strip()]
    out=[]
    for s in parts:
        out.append(s)
        if len("".join(out))>=l or len(out)>=3: break
    return "。".join(out)[:l]

@dataclass
class NewsItem:
    title:str; summary:str; url:str; published_at:str; source:str
    def to_csv(self): return [self.published_at,self.source,self.title,self.summary,self.url]

class Fetcher:
    sem=asyncio.Semaphore(1)
    def __init__(s,c): s.c=c; s.tk=os.getenv('TUSHARE_TOKEN'); s.jk=os.getenv('JUHE_KEY')
    async def tushare(s,st,ed,kw):
        kw.append(f"Tushare {st:%Y%m%d}-{ed:%Y%m%d}")
        try:
            body={"api_name":"news","token":s.tk,
                  "params":{"src":"eastmoney","start_date":st.strftime('%Y%m%d'),"end_date":ed.strftime('%Y%m%d')},
                  "fields":"title,content,url,datetime,src"}
            r=await s.c.post('https://api.tushare.pro',json=body,timeout=30)
            for t,con,url,dt,src in r.json().get('data',{}).get('items',[]) or []:
                yield NewsItem(t,digest(con),url,dtparse.parse(dt).isoformat(),src)
        except Exception: logging.exception("Tushare error")
    async def juhe(s,kw):
        kw.append("Juhe caijing")
        try:
            r=await s.c.get('https://v.juhe.cn/toutiao/index',params={'type':'caijing','key':s.jk},timeout=20)
            for d in r.json().get('result',{}).get('data',[]) or []:
                yield NewsItem(d['title'],digest(d['title']),d['url'],d['date'],'聚合财经')
        except Exception: logging.exception("Juhe error")
    async def rss(s,url,src,kw):
        kw.append(f"RSS {src}")
        async with s.sem:
            await asyncio.sleep(random.uniform(1,2))
            try:
                r=await s.c.get(url,timeout=20)
                if r.status_code!=200:
                    logging.warning(f"{src} HTTP {r.status_code}"); return
                import xml.etree.ElementTree as ET
                root=ET.fromstring(r.text)
                for it in root.findall('.//item'):
                    ttl=it.findtext('title') or ''; link=it.findtext('link') or ''
                    desc=it.findtext('description') or ''
                    pub_raw=it.findtext('pubDate') or datetime.utcnow().isoformat()
                    try: pub=dtparse.parse(pub_raw).isoformat()
                    except: pub=pub_raw
                    yield NewsItem(ttl,digest(desc or ttl),link,pub,src)
                await asyncio.sleep(random.uniform(5,6))
            except Exception: logging.exception(f"{src} error")

async def wrap(gen):
    """Collect async-generator into list."""
    return [item async for item in gen]

async def collect():
    st=datetime.utcnow()-timedelta(days=1); ed=datetime.utcnow()
    kw_log=[]; items=[]
    async with httpx.AsyncClient(timeout=30) as cli:
        f=Fetcher(cli)
        tasks=[
            wrap(f.tushare(st,ed,kw_log)),
            wrap(f.juhe(kw_log)),
            wrap(f.rss('https://rsshub.app/cls/telegraph','财联社',kw_log)),
            wrap(f.rss('https://rss.sina.com.cn/roll/finance/hot_roll.xml','新浪财经',kw_log))
        ]
        results=await asyncio.gather(*tasks,return_exceptions=True)
        for res in results:
            if isinstance(res,Exception):
                logging.error(res); continue
            items.extend(res)
    Path('keywords_used.txt').write_text("\n".join(kw_log),'utf-8')
    return items

def write(items:list[NewsItem]):
    Path('news_today.json').write_text(json.dumps([asdict(i) for i in items],ensure_ascii=False,indent=2),'utf-8')
    with Path('news_all.csv').open('w',newline='',encoding='utf-8-sig') as f:
        csv.writer(f).writerows([['time','source','title','summary','url'],*map(lambda n:n.to_csv(),items)])
    brief="今日重点财经新闻"
    for i,n in enumerate(items,1):
        brief+=f"\n{i}. {n.title}\n   {n.summary}"
    Path('briefing.md').write_text(brief,'utf-8')

def main():
    items=asyncio.run(collect())
    write(items)
    logging.info(f"news collected {len(items)} items")

if __name__=='__main__':
    main()
