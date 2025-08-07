# -*- coding: utf-8 -*-
"""
news_pipeline.py — 聚合+摘要 r6  (2025-08-07)
改动要点
1.  `import sys` → 解决 NameError
2.  检索关键词写入日志：Tushare 日期段、AlphaVantage tickers、每条 RSS 源名
3.  briefing.md 里：标题与摘要分行显示
4.  额外空值保护，避免  'NoneType' object has no attribute ...'
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

# ── 数据类 ──
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

# ── 工具 ──
_SENT_RE=re.compile(r"(利好|上涨|飙升|反弹|大涨|收涨|upbeat|bullish|positive|利空|下跌|暴跌|收跌|bearish|negative)",re.I)
def naive_sent(t): return max(min(len(_SENT_RE.findall(t))*0.1,1.0),-1.0)

def strip_html(txt:str)->str:
    return re.sub(r"<[^>]+>","",html.unescape(txt))

def make_digest(txt:str,max_len:int=180)->str:
    txt=strip_html(txt).replace("\u3000"," ").replace("\xa0"," ").strip()
    sents=re.split(r"[。.!！？\n]",txt)
    out=[]
    for s in sents:
        s=s.strip()
        if s: out.append(s)
        if len("".join(out))>=max_len or len(out)>=3: break
    return "。".join(out)[:max_len]

async def retry(fn,*a,retries=3,**k):
    for i in range(retries):
        try: return await fn(*a,**k)
        except Exception as e:
            if i==retries-1: raise
            await asyncio.sleep(2**i+random.uniform(1,2))

# ── Fetcher ──
class Fetcher:
    sem=asyncio.Semaphore(1)
    def __init__(s,c): s.c=c; s.tk=os.getenv('TUSHARE_TOKEN'); s.ak=os.getenv('ALPHA_KEY'); s.fk=os.getenv('FINNHUB_KEY')

    async def tushare(s,st,ed,kw_log:list):
        kw_log.append(f"Tushare src=eastmoney {st:%Y%m%d}-{ed:%Y%m%d}")
        cache=Path(f"tushare_{st:%Y%m%d}.json")
        if cache.is_file():
            logging.info("Tushare 缓存"); return [NewsItem(**d) for d in json.loads(cache.read_text('utf-8'))]
        if not s.tk: return []
        p={"api_name":"news","token":s.tk,
           "params":{"src":"eastmoney","start_date":st.strftime('%Y%m%d'),"end_date":ed.strftime('%Y%m%d')},
           "fields":"title,content,url,datetime,src"}
        r=await s.c.post('https://api.tushare.pro',json=p,timeout=30)
        js=r.json() if r.content else {}
        if js.get('code'):
            logging.warning(f"Tushare code {js.get('code')}: {js.get('msg')}"); return []
        items=js.get('data',{}).get('items',[]) or []
        out=[NewsItem(t,make_digest(con),url,dtparse.parse(dt).isoformat(),src,naive_sent(con),[])
             for t,con,url,dt,src in items]
        cache.write_text(json.dumps([asdict(i) for i in out],ensure_ascii=False))
        logging.info(f"Tushare 获取 {len(out)}")
        return out

    async def alpha(s,tks,kw_log:list):
        if not s.ak or not tks: return []
        kw_log.append(f"AlphaVantage tickers={','.join(tks)}")
        r=await s.c.get('https://www.alphavantage.co/query',
                        params={'function':'NEWS_SENTIMENT','tickers':','.join(tks[:100]),'apikey':s.ak},timeout=20)
        try: feed=r.json().get('feed') or []
        except Exception:
            feed=[]
        return [NewsItem(it.get('title',''),make_digest(it.get('summary','') or it.get('title','')),it.get('url',''),
                         it.get('time_published',datetime.utcnow().isoformat()),it.get('source','AV'),
                         float(it.get('overall_sentiment_score',0)),[]) for it in feed]

    async def juhe(s,kw_log:list):
        jk=os.getenv('JUHE_KEY'); 
        if not jk: return []
        kw_log.append("Juhe Toutiao type=caijing")
        r=await s.c.get('https://v.juhe.cn/toutiao/index',params={'type':'caijing','key':jk},timeout=20)
        data=r.json().get('result',{}).get('data',[]) if r.status_code==200 else []
        return [NewsItem(d['title'],make_digest(d['title']),d['url'],d['date'],
                         '聚合财经',naive_sent(d['title']),[]) for d in data]

    async def rss(s,url,src,kw_log:list):
        kw_log.append(f"RSS {src}")
        async with s.sem:
            await asyncio.sleep(random.uniform(1,2))
            try:
                r=await s.c.get(url,timeout=20)
                if r.status_code!=200:
                    logging.warning(f"{src} HTTP {r.status_code}"); return []
                import xml.etree.ElementTree as ET
                root=ET.fromstring(r.text); out=[]
                for it in root.findall('.//item'):
                    ttl=it.findtext('title') or ''; link=it.findtext('link') or ''
                    desc=it.findtext('description') or ''
                    pub=it.findtext('pubDate') or ''
                    try: pub_iso=dtparse.parse(pub).isoformat()
                    except: pub_iso=datetime.utcnow().isoformat()
                    out.append(NewsItem(ttl,make_digest(desc or ttl),link,pub_iso,src,naive_sent(ttl+desc),[]))
                await asyncio.sleep(random.uniform(5,6))
                return out
            except Exception as e:
                logging.error(f"{src} err {e}"); return []

# ── 相关度 ──
def rel(n,holds):
    txt=(n.title+n.summary).lower(); sc=0
    for h in holds:
        if h.clean.lower() in txt or h.name.lower() in txt: sc+=0.4
    return min(sc+abs(n.sentiment)*0.1,1.0)

async def collect(holds,days,limit):
    st=datetime.utcnow()-timedelta(days=days); ed=datetime.utcnow()
    keywords_log=[]
    async with httpx.AsyncClient(timeout=30) as cli:
        f=Fetcher(cli)
        tasks=[retry(f.tushare,st,ed,keywords_log), retry(f.juhe,keywords_log)]
        ovs=[h.symbol for h in holds if not h.is_cn and '.' not in h.symbol]
        if ovs: tasks.append(retry(f.alpha,ovs,keywords_log))
        rss=[('https://rsshub.app/21jingji/channel/stock','21财经'),
             ('https://rsshub.app/caixin/latest','财新'),
             ('https://rsshub.app/stcn/news','证券时报'),
             ('https://rss.sina.com.cn/roll/finance/hot_roll.xml','新浪财经'),
             ('https://a.jiemian.com/index.php?m=article&a=rss','界面新闻'),
             ('https://rsshub.app/cls/telegraph','财联社')]
        for u,s in rss: tasks.append(retry(f.rss,u,s,keywords_log))
        res=await asyncio.gather(*tasks,return_exceptions=True)
    for kw in keywords_log:
        logging.info(f"KW: {kw}")
    pool={}
    for r in res:
        if isinstance(r,Exception): logging.error(r); continue
        for it in r: pool[it.fp()]=it
    return sorted(pool.values(),key=lambda x:rel(x,holds),reverse=True)[:limit]

# ── 输出 & 推送 ──
def write(items):
    Path('news_today.json').write_text(json.dumps([asdict(i) for i in items],ensure_ascii=False,indent=2),'utf-8')
    brief="今日重点财经新闻"
    for i,it in enumerate(items,1):
        brief+=f"\n{i}. {it.title}\n   {it.summary}"
    Path('briefing.md').write_text(brief,'utf-8'); return brief

def esc(x): return x.replace('&','&amp;').replace('<','&lt;').replace('>','&gt;')
async def sc(text):
    k=os.getenv('SCKEY')
    if not k: return
    r=await httpx.AsyncClient().post(f'https://sctapi.ftqq.com/{k}.send',
                                     data={'text':'投资资讯','desp':esc(text[:8000])},timeout=20)
    if r.json().get('code')==40001: logging.warning("Server酱限额")

async def tg(text):
    t=os.getenv('TELEGRAM_BOT_TOKEN'); c=os.getenv('TELEGRAM_CHAT_ID')
    if not t or not c: return
    for seg in [text[i:i+3500] for i in range(0,len(text),3500)]:
        await httpx.AsyncClient().post(f'https://api.telegram.org/bot{t}/sendMessage',
                                       data={'chat_id':c,'text':seg},timeout=20)

def parse_holdings(path):
    if path and Path(path).is_file(): return [Holding(**d) for d in json.loads(Path(path).read_text('utf-8'))]
    env=os.getenv('HOLDINGS_JSON'); return [Holding(**d) for d in json.loads(env)] if env else []

def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--holdings',default='holdings.json')
    ap.add_argument('--days',type=int,default=1); ap.add_argument('--max',type=int,default=40)
    ar=ap.parse_args(); holds=parse_holdings(ar.holdings)
    if not holds: logging.warning("持仓为空"); return
    items=asyncio.run(collect(holds,ar.days,ar.max))
    txt=write(items)
    asyncio.run(sc(txt)); asyncio.run(tg(txt))
    logging.info(f"✅ 输出 {len(items)} 条")

if __name__=='__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler('pipeline.log','a','utf-8'),
                                  logging.StreamHandler(sys.stdout)])
    main()
