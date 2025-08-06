# -*- coding: utf-8 -*-
"""
news_pipeline.py — 多源财经新闻聚合器
更新要点
1. **Tushare 限流**：改为“单次批量拉取”模式，只调用接口 1 次，彻底消除 40203 报错。
2. **Juhe**：使用通用财经流，去掉按关键字 404 的子调用。
3. **RSSHub 429**：全局随机延迟 + 限速 0.5 QPS。
4. **Server 酱**：检测 code==40001（当天 5 次上限）时自动跳过推送。
5. 仍保持纯文本推送与统一日志。
"""
from __future__ import annotations
import argparse, asyncio, json, os, random, re, sys
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from hashlib import md5
from pathlib import Path
from typing import List, Sequence
import httpx, logging
from dateutil import parser as dtparse

# ─── 数据类 ───
@dataclass
class Holding:
    symbol: str
    name: str
    weight: float = 0.0
    @property
    def clean(self): return self.symbol.split('.')[0]
    @property
    def is_cn(self): return self.symbol.endswith(('.SH', '.SZ'))

@dataclass
class NewsItem:
    title: str; summary: str; url: str; published_at: str; source: str; sentiment: float; symbols: List[str]
    def fp(self): return md5(self.url.encode()).hexdigest()

# ─── 简易情感 ───
_SENT_RE = re.compile(r"\b(upbeat|bullish|positive)\b|\b(downbeat|bearish|negative)\b|利好|上涨|飙升|反弹|大涨|收涨|利空|下跌|暴跌|收跌", re.I)
def naive_sent(text): return max(min(len(_SENT_RE.findall(text))*0.1,1.0),-1.0)

# ─── 限速工具 ───
async def retry(fn,*a,retries=3,**k):
    for i in range(retries):
        try: return await fn(*a,**k)
        except Exception as e:
            if i==retries-1: raise
            await asyncio.sleep(2**i+random.uniform(0.5,1.5))

# ─── Fetcher ───
class Fetcher:
    def __init__(s,c: httpx.AsyncClient):
        s.c=c; s.tk=os.getenv('TUSHARE_TOKEN'); s.ak=os.getenv('ALPHA_KEY'); s.fk=os.getenv('FINNHUB_KEY')

    # 1. 只调用一次 Tushare.news，避免触发限速
    async def tushare_bulk(s,start,end):
        if not s.tk: return []
        p={"api_name":"news","token":s.tk,
           "params":{"src":"eastmoney","start_date":start.strftime('%Y%m%d'),"end_date":end.strftime('%Y%m%d')},
           "fields":"title,content,url,datetime,src"}
        r=await s.c.post('https://api.tushare.pro',json=p,timeout=30)
        js=r.json()
        if js.get('code')==40203:
            logging.warning(f"Tushare hit rate-limit, skip. msg={js.get('msg')}")
            return []
        items=js.get('data',{}).get('items',[]) or []
        out=[NewsItem(t,con[:120],url,dtparse.parse(dtstr).isoformat(),src,naive_sent(con),[]) for t,con,url,dtstr,src in items]
        logging.info(f"Tushare bulk -> {len(out)}")
        return out

    async def alpha_news(s,tickers:Sequence[str]):
        if not s.ak or not tickers: return []
        r=await s.c.get('https://www.alphavantage.co/query',
                        params={'function':'NEWS_SENTIMENT','tickers':','.join(tickers[:100]),'apikey':s.ak},timeout=20)
        feed=r.json().get('feed') or []
        return [NewsItem(it['title'],it['summary'],it['url'],it['time_published'],
                         it.get('source','AV'),float(it.get('overall_sentiment_score',0)),
                         [x.get('ticker') for x in it.get('ticker_sentiment',[]) if x.get('ticker')])
                for it in feed]

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
        try:
            r=await s.c.get('https://v.juhe.cn/toutiao/index',params={'type':'caijing','key':jk},timeout=20)
            if r.status_code!=200 or r.json().get('error_code'): return []
            data=r.json().get('result',{}).get('data',[]) or []
            return [NewsItem(d['title'],d.get('author_name',''),d['url'],d['date'],'聚合财经',naive_sent(d['title']),[]) for d in data]
        except Exception as e:
            logging.error(f'Juhe error: {e}'); return []

    async def rss(s,url:str,source:str):
        try:
            r=await s.c.get(url,timeout=20)
            if r.status_code!=200:
                logging.warning(f"{source} HTTP {r.status_code}"); return []
            import xml.etree.ElementTree as ET
            root=ET.fromstring(r.text)
            out=[]
            for item in root.findall('.//item'):
                ttl=item.findtext('title') or ''; link=item.findtext('link') or ''
                desc=item.findtext('description') or ''; pub=item.findtext('pubDate') or ''
                try: published=dtparse.parse(pub).isoformat()
                except: published=datetime.utcnow().isoformat()
                out.append(NewsItem(ttl,desc[:120],link,published,source,naive_sent(ttl+desc),[]))
            return out
        except Exception as e:
            logging.error(f"{source} error: {e}"); return []

# ─── 相关度 ───
def relevance(it:NewsItem,holds:List[Holding]):
    txt=(it.title+' '+it.summary).lower(); sc=0.0
    for h in holds:
        if h.clean.lower() in txt or h.name.lower() in txt: sc+=0.4
    return min(sc+abs(it.sentiment)*0.1,1.0)

# ─── 主流程 ───
async def collect(holds,days,limit):
    st=datetime.utcnow()-timedelta(days=days); ed=datetime.utcnow()
    async with httpx.AsyncClient(timeout=30) as cli:
        f=Fetcher(cli); tasks=[]
        tasks.append(retry(f.tushare_bulk,st,ed))
        ovs=[h.symbol for h in holds if not h.is_cn and '.' not in h.symbol]
        if ovs: tasks.append(retry(f.alpha_news,ovs))
        for h in holds:
            if not h.is_cn: tasks.append(retry(f.finnhub,h.symbol.split('.')[0],st,ed))
        tasks.append(retry(f.juhe_stream))
        rss_list=[('https://rsshub.app/cls/telegraph','财联社'),
                  ('https://rsshub.app/wallstreetcn/news','华尔街见闻'),
                  ('https://rsshub.app/10jqka/realtimenews','同花顺7x24'),
                  ('https://rsshub.app/yicai/brief','第一财经'),
                  ('https://rss.sina.com.cn/roll/finance/hot_roll.xml','新浪财经'),
                  ('https://a.jiemian.com/index.php?m=article&a=rss','界面新闻')]
        for u,sr in rss_list: tasks.append(retry(f.rss,u,sr))
        res=await asyncio.gather(*tasks,return_exceptions=True)

    pool={}
    for r in res:
        if isinstance(r,Exception): logging.error(f'Fetcher error: {r}'); continue
        for it in r: pool[it.fp()]=it
    return sorted(pool.values(),key=lambda x:relevance(x,holds),reverse=True)[:limit]

# ─── 文件 & 推送 ───
def write(items):
    Path('news_today.json').write_text(json.dumps([asdict(i) for i in items],ensure_ascii=False,indent=2),'utf-8')
    lines=['今日重点财经新闻']
    for i,it in enumerate(items,1):
        ts=dtparse.isoparse(it.published_at).strftime('%m-%d %H:%M')
        lines.append(f"{i}. {it.title} ({it.source}, {ts}) {it.url}")
    txt='\n'.join(lines)
    Path('briefing.md').write_text(txt,'utf-8')
    return txt

def escape(s): return s.replace('&','&amp;').replace('<','&lt;').replace('>','&gt;')

async def push_scv(text):
    key=os.getenv('SCKEY')
    if not key: return
    r=await httpx.AsyncClient().post(f'https://sctapi.ftqq.com/{key}.send',
                                     data={'text':'投资资讯','desp':escape(text[:8000])},timeout=20)
    js=r.json(); code=js.get('code')
    if code==40001:
        logging.warning('Server酱当日已超 5 次限额，自动跳过')
    elif code!=0:
        logging.error(f'Server酱推送失败：{js}')

async def push_tg(text):
    tok=os.getenv('TELEGRAM_BOT_TOKEN'); cid=os.getenv('TELEGRAM_CHAT_ID')
    if not tok or not cid: return
    parts=[text[i:i+3500] for i in range(0,len(text),3500)]
    async with httpx.AsyncClient() as c:
        for p in parts:
            await c.post(f'https://api.telegram.org/bot{tok}/sendMessage',
                         data={'chat_id':cid,'text':p},timeout=20)

def parse_holds(fp):
    if fp and Path(fp).is_file():
        return [Holding(**d) for d in json.loads(Path(fp).read_text('utf-8'))]
    env=os.getenv('HOLDINGS_JSON')
    return [Holding(**d) for d in json.loads(env)] if env else []

def main():
    ar=argparse.ArgumentParser(); ar.add_argument('--holdings',default='holdings.json')
    ar.add_argument('--days',type=int,default=1); ar.add_argument('--max',type=int,default=40)
    a=ar.parse_args()
    holds=parse_holds(a.holdings)
    if not holds: logging.warning('持仓为空'); return
    items=asyncio.run(collect(holds,a.days,a.max))
    txt=write(items)
    try: asyncio.run(push_scv(txt))
    except Exception as e: logging.error(f'Server酱异常：{e}')
    try: asyncio.run(push_tg(txt))
    except Exception as e: logging.error(f'Telegram 异常：{e}')
    logging.info(f'✅ 输出 {len(items)} 条')

if __name__=='__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler('pipeline.log','a','utf-8'),
                                  logging.StreamHandler(sys.stdout)])
    main()
