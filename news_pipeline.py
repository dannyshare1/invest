# -*- coding: utf-8 -*-
"""
news_pipeline.py — 多源财经新闻聚合器（rate-limit friendly）
• RSSHub 请求串行 + 3-4 s 间隔 ⇒ 消除 429
• Tushare 批量结果按天缓存 ⇒ 避免“每天 2 次”限额
• Server 酱仍自动跳过当日 5 次上限(code 40001)
"""
from __future__ import annotations
import argparse, asyncio, json, os, random, re, sys, logging
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from hashlib import md5
from pathlib import Path
from typing import List, Sequence
import httpx
from dateutil import parser as dtparse

# ─── 基本数据类 ───
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

# ─── 简易情感 ───
_SENT_RE = re.compile(r"(利好|上涨|飙升|反弹|大涨|收涨|upbeat|bullish|positive|利空|下跌|暴跌|收跌|bearish|negative)", re.I)
def naive_sent(t): return max(min(len(_SENT_RE.findall(t))*0.1,1.0),-1.0)

# ─── 工具 ───
async def retry(fn,*a,retries=3,**k):
    for i in range(retries):
        try: return await fn(*a,**k)
        except Exception as e:
            if i==retries-1: raise
            await asyncio.sleep(2**i+random.uniform(0.5,1.5))

# ─── Fetcher ───
class Fetcher:
    def __init__(s,c): s.c=c; s.tk=os.getenv('TUSHARE_TOKEN'); s.ak=os.getenv('ALPHA_KEY'); s.fk=os.getenv('FINNHUB_KEY')

    # Tushare — 每日一次并缓存
    async def tushare_bulk(s,start,end):
        cache=Path(f"tushare_cache_{start.strftime('%Y%m%d')}.json")
        if cache.is_file():
            logging.info("Tushare use local cache"); return [NewsItem(**d) for d in json.loads(cache.read_text('utf-8'))]
        if not s.tk: return []
        p={"api_name":"news","token":s.tk,
           "params":{"src":"eastmoney","start_date":start.strftime('%Y%m%d'),"end_date":end.strftime('%Y%m%d')},
           "fields":"title,content,url,datetime,src"}
        r=await s.c.post('https://api.tushare.pro',json=p,timeout=30)
        js=r.json()
        if js.get('code'):
            logging.warning(f"Tushare code {js.get('code')} msg={js.get('msg')}"); return []
        items=js.get('data',{}).get('items',[]) or []
        out=[NewsItem(t,con[:120],url,dtparse.parse(dt).isoformat(),src,naive_sent(con),[]) for t,con,url,dt,src in items]
        cache.write_text(json.dumps([asdict(i) for i in out],ensure_ascii=False))
        logging.info(f"Tushare fetched {len(out)}")
        return out

    async def alpha_news(s,tks:Sequence[str]):
        if not s.ak or not tks: return []
        r=await s.c.get('https://www.alphavantage.co/query',
                        params={'function':'NEWS_SENTIMENT','tickers':','.join(tks[:100]),'apikey':s.ak},timeout=20)
        feed=r.json().get('feed') or []
        return [NewsItem(it['title'],it['summary'],it['url'],it['time_published'],
                         it.get('source','AV'),float(it.get('overall_sentiment_score',0)),
                         [x.get('ticker') for x in it.get('ticker_sentiment',[]) if x.get('ticker')]) for it in feed]

    async def finnhub(s,sym,start,end):
        if not s.fk: return []
        r=await s.c.get('https://finnhub.io/api/v1/company-news',
                        params={'symbol':sym,'from':start.date(),'to':end.date(),'token':s.fk},timeout=20)
        return [NewsItem(d['headline'],d['summary'],d['url'],
                         datetime.utcfromtimestamp(d['datetime']).isoformat()+'Z',
                         d['source'],naive_sent(d['summary']),[sym]) for d in r.json()]

    async def juhe_stream(s):
        jk=os.getenv('JUHE_KEY')
        if not jk: return []
        r=await s.c.get('https://v.juhe.cn/toutiao/index',params={'type':'caijing','key':jk},timeout=20)
        if r.status_code!=200 or r.json().get('error_code'): return []
        data=r.json().get('result',{}).get('data',[]) or []
        return [NewsItem(d['title'],d.get('author_name',''),d['url'],d['date'],'聚合财经',naive_sent(d['title']),[]) for d in data]

    # RSS — 全局 semaphore 串行 + 3-4 秒间隔
    rss_sem=asyncio.Semaphore(1)
    async def rss(s,url:str,src:str):
        async with s.rss_sem:
            try:
                r=await s.c.get(url,timeout=20)
                if r.status_code!=200:
                    logging.warning(f"{src} HTTP {r.status_code}"); return []
                import xml.etree.ElementTree as ET
                root=ET.fromstring(r.text); out=[]
                for item in root.findall('.//item'):
                    ttl=item.findtext('title') or ''; link=item.findtext('link') or ''
                    desc=item.findtext('description') or ''; pub=item.findtext('pubDate') or ''
                    try: published=dtparse.parse(pub).isoformat()
                    except: published=datetime.utcnow().isoformat()
                    out.append(NewsItem(ttl,desc[:120],link,published,src,naive_sent(ttl+desc),[]))
                await asyncio.sleep(random.uniform(3,4))
                return out
            except Exception as e:
                logging.error(f"{src} err: {e}"); return []

# 相关度
def rel(it,holds):
    txt=(it.title+' '+it.summary).lower(); sc=0.0
    for h in holds:
        if h.clean.lower() in txt or h.name.lower() in txt: sc+=0.4
    return min(sc+abs(it.sentiment)*0.1,1.0)

# 主流程
async def collect(holds,days,limit):
    st=datetime.utcnow()-timedelta(days=days); ed=datetime.utcnow()
    async with httpx.AsyncClient(timeout=30) as cli:
        f=Fetcher(cli); tasks=[retry(f.tushare_bulk,st,ed), retry(f.juhe_stream)]
        ovs=[h.symbol for h in holds if not h.is_cn and '.' not in h.symbol]
        if ovs: tasks.append(retry(f.alpha_news,ovs))
        for h in holds:
            if not h.is_cn: tasks.append(retry(f.finnhub,h.clean,st,ed))
        rss=[('https://rsshub.app/cls/telegraph','财联社'),
             ('https://rsshub.app/10jqka/realtimenews','同花顺7x24'),
             ('https://rsshub.app/yicai/brief','第一财经'),
             ('https://rss.sina.com.cn/roll/finance/hot_roll.xml','新浪财经'),
             ('https://a.jiemian.com/index.php?m=article&a=rss','界面新闻'),
             ('https://rsshub.app/wallstreetcn/news','华尔街见闻')]
        for u,sr in rss: tasks.append(retry(f.rss,u,sr))
        res=await asyncio.gather(*tasks,return_exceptions=True)
    pool={}
    for r in res:
        if isinstance(r,Exception): logging.error(r); continue
        for it in r: pool[it.fp()]=it
    return sorted(pool.values(),key=lambda x:rel(x,holds),reverse=True)[:limit]

# 文件 & 推送
def write(items):
    Path('news_today.json').write_text(json.dumps([asdict(i) for i in items],ensure_ascii=False,indent=2),'utf-8')
    txt='今日重点财经新闻\n'+'\n'.join(f"{i+1}. {it.title} ({it.source}) {it.url}" for i,it in enumerate(items))
    Path('briefing.md').write_text(txt,'utf-8'); return txt

def esc(x): return x.replace('&','&amp;').replace('<','&lt;').replace('>','&gt;')
async def sc(text):
    key=os.getenv('SCKEY')
    if not key: return
    r=await httpx.AsyncClient().post(f'https://sctapi.ftqq.com/{key}.send',
                                     data={'text':'投资资讯','desp':esc(text[:8000])},timeout=20)
    if r.json().get('code')==40001: logging.warning('Server酱日限额，跳过')
async def tg(text):
    t,c=os.getenv('TELEGRAM_BOT_TOKEN'),os.getenv('TELEGRAM_CHAT_ID')
    if not t or not c: return
    seg=[text[i:i+3500] for i in range(0,len(text),3500)]
    async with httpx.AsyncClient() as cli:
        for s in seg:
            await cli.post(f'https://api.telegram.org/bot{t}/sendMessage',data={'chat_id':c,'text':s},timeout=20)

def parse_holds(p):
    if p and Path(p).is_file(): return [Holding(**d) for d in json.loads(Path(p).read_text('utf-8'))]
    env=os.getenv('HOLDINGS_JSON'); return [Holding(**d) for d in json.loads(env)] if env else []

def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--holdings',default='holdings.json')
    ap.add_argument('--days',type=int,default=1); ap.add_argument('--max',type=int,default=40)
    ar=ap.parse_args(); holds=parse_holds(ar.holdings)
    if not holds: logging.warning('持仓为空'); return
    items=asyncio.run(collect(holds,ar.days,ar.max))
    txt=write(items)
    asyncio.run(sc(txt)); asyncio.run(tg(txt))
    logging.info(f'✅ 输出 {len(items)} 条')

if __name__=='__main__':
    logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler('pipeline.log','a','utf-8'),logging.StreamHandler(sys.stdout)])
    main()
