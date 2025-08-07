# -*- coding: utf-8 -*-
"""
news_pipeline.py r17  —— 不再空关键词 & 全面日志
"""
from __future__ import annotations
import asyncio, httpx, os, json, csv, re, logging, html, time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from dateutil import parser as dtparse
from bs4 import BeautifulSoup

TZ    = timezone(timedelta(hours=8))
TODAY = datetime.now(TZ).strftime("%Y%m%d")
TOKEN = os.getenv("TUSHARE_TOKEN", "")
TAGS  = ['fenghuang','jinrongjie','10jqka','sina','yuncaijing','eastmoney','wallstreetcn']

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("pipeline.log","a","utf-8"),
              logging.StreamHandler()]
)
ERR_LOG = Path("errors.log")

def strip(x:str)->str: return re.sub(r"<[^>]+>","",html.unescape(x)).strip()
def row(n:dict):       return [n['time'],n['src'],n['title'],n['summary'],n['url']]

# ─── 1. 关键词生成 ───
async def ts_weight_names(code:str)->list[str]:
    """尝试过去 5 个交易日直到拿到前10"""
    if not TOKEN: return []
    cli=httpx.AsyncClient(timeout=20)
    today=datetime.now(TZ)
    for d in range(5):
        day=(today-timedelta(days=d)).strftime("%Y%m%d")
        body={"api_name":"index_weight","token":TOKEN,
              "params":{"index_code":f"{code}.CI","trade_date":day},
              "fields":"con_code,weight"}
        js=(await cli.post("https://api.tushare.pro",json=body)).json()
        if js.get("code"): continue
        items=js.get("data",{}).get("items",[])
        if items:
            codes=[c for c,_ in items[:10]]
            body={"api_name":"stock_basic","token":TOKEN,
                  "params":{"ts_code":",".join(codes)},"fields":"ts_code,name"}
            js=(await cli.post("https://api.tushare.pro",json=body)).json()
            return [n for _,n in js.get("data",{}).get("items",[])]
    return []

async def eastmoney_web(code:str)->list[str]:
    """抓 fund.eastmoney.com ETF 网页持仓"""
    url=f"https://fund.eastmoney.com/{code}.html"
    r=await httpx.AsyncClient().get(url,timeout=20)
    if r.status_code!=200: return []
    soup=BeautifulSoup(r.text,"lxml")
    table=soup.select_one("#etfinfoTable")
    if not table: return []
    names=[]
    for td in table.select("td:nth-child(1)")[:10]:
        n=td.get_text(strip=True)
        if n: names.append(n)
    return names

async def one_etf_names(ts_code:str)->list[str]:
    code=ts_code.split(".")[0]
    try:
        names=await ts_weight_names(code)
        if names: return names
    with ERR_LOG.open("a", encoding="utf-8") as f:
        f.write(f"{datetime.now(TZ)}  ETF {h['symbol']} 无法获取重仓\n")
    # fallback 东财网页
    try:
        names=await eastmoney_web(code)
        if names: return names
    with ERR_LOG.open("a", encoding="utf-8") as f:
        f.write(f"{datetime.now(TZ)}  RSS {src}  {e}\n")
    return []

async def build_keywords()->set[str]:
    fp=Path("holdings.json")
    if not fp.is_file():
        logging.warning("holdings.json missing")
        Path("keywords_used.txt").write_text("")
        return set()
    holds=json.loads(fp.read_text("utf-8"))
    kws=set()
    for h in holds:
        names=await one_etf_names(h['symbol'])
        if not names:
            ERR_LOG.write_text(f"{datetime.now(TZ)}  ETF {h['symbol']} 无法获取重仓\n",append=True)
        kws.update(names)
    kw_list=sorted(kws)
    Path("keywords_used.txt").write_text("\n".join(kw_list),"utf-8")
    return set(kw_list)

# ─── 2. Tushare news API (带缓存降级) ───
async def ts_news(tag:str)->list[dict]:
    cache=Path(f"news_{tag}_{TODAY}.json")
    if cache.is_file(): return json.loads(cache.read_text("utf-8"))
    if not TOKEN: return []
    body={"api_name":"news","token":TOKEN,
          "params":{"src":tag,"start_date":TODAY,"end_date":TODAY},
          "fields":"title,content,url,datetime,src"}
    r=await httpx.AsyncClient().post("https://api.tushare.pro",json=body,timeout=30)
    js=r.json()
    if js.get("code"):
        ERR_LOG.write_text(f"{datetime.now(TZ)}  news API {tag}  code={js['code']}\n",append=True)
        prev=Path(f"news_{tag}_{(datetime.now(TZ)-timedelta(days=1)).strftime('%Y%m%d')}.json")
        return json.loads(prev.read_text("utf-8")) if prev.is_file() else []
    items=[dict(title=t,
                summary=strip(con)[:180],
                url=u,
                time=dtparse.parse(dt).astimezone(TZ).isoformat(),
                src=f"API-{tag}")
           for t,con,u,dt,_ in js.get("data",{}).get("items",[])[:]]
    cache.write_text(json.dumps(items,ensure_ascii=False))
    return items

# ─── 3. RSS ───
async def rss(url,src):
    try:
        r=await httpx.AsyncClient().get(url,timeout=20)
        if r.status_code!=200: return []
        import xml.etree.ElementTree as ET
        root=ET.fromstring(r.text);out=[]
        for it in root.findall(".//item"):
            ttl=strip(it.findtext("title") or "")
            link=it.findtext("link") or ""
            desc=strip(it.findtext("description") or "")[:180]
            pub_raw=it.findtext("pubDate") or ""
            try: pub=dtparse.parse(pub_raw).astimezone(TZ).isoformat()
            except: pub=datetime.now(TZ).isoformat()
            out.append(dict(title=ttl,summary=desc,url=link,time=pub,src=src))
        return out
    except Exception as e:
        ERR_LOG.write_text(f"{datetime.now(TZ)}  RSS {src}  {e}\n",append=True)
        return []

def hit_kw(n,kw): return any(k.lower() in (n['title']+n['summary']).lower() for k in kw)
def within7d(s):  return dtparse.isoparse(s)>=datetime.now(TZ)-timedelta(days=7)

# ─── main ───
async def main():
    kw=set(await build_keywords())
    tasks=[ts_news(t) for t in TAGS]+[
        rss("https://rsshub.app/cls/telegraph","财联社"),
        rss("https://rss.sina.com.cn/roll/finance/hot_roll.xml","新浪财经")
    ]
    res=await asyncio.gather(*tasks)
    news=[n for lst in res for n in lst]
    news=[n for n in news if n['src'].startswith("API-") or (hit_kw(n,kw) and within7d(n['time']))]
    news.sort(key=lambda x:x['time'],reverse=True)
    with Path("news_all.csv").open("w",newline="",encoding="utf-8-sig") as f:
        csv.writer(f).writerows([["time","source","title","summary","url"],*map(row,news)])
    logging.info(f"已写 news_all.csv {len(news)} 条；关键词 {len(kw)} 个")

if __name__=="__main__":
    asyncio.run(main())
