# -*- coding: utf-8 -*-
"""
news_pipeline.py — r7 (实时摘要 + 关键词文件)
• 每次运行都刷 briefing.md
• 把检索关键词写入 keywords_used.txt
"""
from __future__ import annotations
import argparse, asyncio, json, os, random, re, logging, html, sys
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from hashlib import md5
from pathlib import Path
from typing import List
import httpx
from dateutil import parser as dtparse

# ── 工具函数 ──
_SENT_RE=re.compile(r"(利好|上涨|飙升|反弹|大涨|收涨|upbeat|bullish|positive|利空|下跌|暴跌|收跌|bearish|negative)",re.I)
def naive_sent(t): return max(min(len(_SENT_RE.findall(t))*0.1,1.0),-1.0)
def strip_html(x:str): return re.sub(r"<[^>]+>","",html.unescape(x))
def digest(txt:str,l:int=180):
    txt=strip_html(txt).strip()
    out=[]
    for s in re.split(r"[。.!！？\n]",txt):
        s=s.strip()
        if s: out.append(s)
        if len("".join(out))>=l or len(out)>=3: break
    return "。".join(out)[:l]

# ── 数据类 ──
@dataclass
class NewsItem:
    title:str; summary:str; url:str; published_at:str; source:str; sentiment:float
    def fp(self): return md5(self.url.encode()).hexdigest()

# ── Fetchers ──
class Fetcher:
    sem=asyncio.Semaphore(1)
    def __init__(s,c):
        s.c=c; s.tk=os.getenv('TUSHARE_TOKEN'); s.jk=os.getenv('JUHE_KEY')
    async def tushare(s,st,ed,kw):
        kw.append(f"Tushare {st:%Y%m%d}-{ed:%Y%m%d}")
        cache=Path(f"tushare_{st:%Y%m%d}.json")
        if cache.is_file():
            return [NewsItem(**d) for d in json.loads(cache.read_text())]
        if not s.tk: return []
        body={"api_name":"news","token":s.tk,
              "params":{"src":"eastmoney","start_date":st.strftime('%Y%m%d'),"end_date":ed.strftime('%Y%m%d')},
              "fields":"title,content,url,datetime,src"}
        r=await s.c.post('https://api.tushare.pro',json=body,timeout=30)
        js=r.json() if r.content else {}
        items=js.get('data',{}).get('items',[]) if not js.get('code') else []
        out=[NewsItem(t,digest(con),url,dtparse.parse(dt).isoformat(),src,naive_sent(con))
             for t,con,url,dt,src in items]
        cache.write_text(json.dumps([asdict(i) for i in out],ensure_ascii=False))
        return out
    async def juhe(s,kw):
        kw.append("Juhe caijing")
        if not s.jk: return []
        r=await s.c.get('https://v.juhe.cn/toutiao/index',params={'type':'caijing','key':s.jk},timeout=20)
        data=r.json().get('result',{}).get('data',[]) if r.status_code==200 else []
        return [NewsItem(d['title'],digest(d['title']),d['url'],d['date'],'聚合财经',naive_sent(d['title'])) for d in data]
    async def rss(s,url,src,kw):
        kw.append(f"RSS {src}")
        async with s.sem:
            await asyncio.sleep(random.uniform(1,2))
            try:
                r=await s.c.get(url,timeout=20)
                if r.status_code!=200: return []
                import xml.etree.ElementTree as ET
                root=ET.fromstring(r.text); out=[]
                for it in root.findall('.//item'):
                    ttl=it.findtext('title') or ''; link=it.findtext('link') or ''
                    desc=it.findtext('description') or ''
                    pub=it.findtext('pubDate') or datetime.utcnow().isoformat()
                    try: pub=dtparse.parse(pub).isoformat()
                    except: pub=datetime.utcnow().isoformat()
                    out.append(NewsItem(ttl,digest(desc or ttl),link,pub,src,naive_sent(ttl+desc)))
                await asyncio.sleep(random.uniform(5,6))
                return out
            except: return []

async def run_collect():
    st=datetime.utcnow()-timedelta(days=1); ed=datetime.utcnow()
    kw_log=[]; items=[]
    async with httpx.AsyncClient(timeout=30) as cli:
        f=Fetcher(cli)
        tasks=[f.tushare(st,ed,kw_log), f.juhe(kw_log),
               f.rss('https://rsshub.app/cls/telegraph','财联社',kw_log),
               f.rss('https://rss.sina.com.cn/roll/finance/hot_roll.xml','新浪财经',kw_log)]
        res=await asyncio.gather(*tasks,return_exceptions=True)
    for r in res:
        if isinstance(r,Exception): continue
        items.extend(r)
    # 写关键词文件
    Path('keywords_used.txt').write_text("\n".join(kw_log),encoding='utf-8')
    # 去重
    pool={n.fp():n for n in items}
    return list(pool.values())

def write_brief(items):
    Path('news_today.json').write_text(json.dumps([asdict(i) for i in items],ensure_ascii=False,indent=2))
    brief="今日重点财经新闻"
    for i,it in enumerate(items,1):
        brief+=f"\n{i}. {it.title}\n   {it.summary}"
    Path('briefing.md').write_text(brief); return brief

def main():
    items=asyncio.run(run_collect())
    write_brief(items)
    logging.info(f"news collected {len(items)} items")

if __name__=='__main__':
    logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(message)s',
                        handlers=[logging.FileHandler('pipeline.log','a'),logging.StreamHandler(sys.stdout)])
    main()
