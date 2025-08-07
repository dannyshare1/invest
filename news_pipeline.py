# -*- coding: utf-8 -*-
"""
news_pipeline.py  r16  (2025-08-07)
------------------------------------
关键词 = 每只 ETF 前10 重仓股票中文名
东财 push2 接口 → 成功即返回
失败 → Tushare index_weight (限速, 本日×一次)
新闻 = Tushare news API + RSS 财联社/新浪 (含关键词&7d)
输出 = news_all.csv, keywords_used.txt
依赖 = httpx, python-dateutil
必需 Secret = TUSHARE_TOKEN
"""

from __future__ import annotations
import asyncio, httpx, os, json, csv, re, logging, html, time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from dateutil import parser as dtparse

# ─── 基础 ───
TZ     = timezone(timedelta(hours=8))
TODAY  = datetime.now(TZ).strftime("%Y%m%d")
TOKEN  = os.getenv("TUSHARE_TOKEN", "")
TAGS   = ['fenghuang','jinrongjie','10jqka','sina','yuncaijing','eastmoney','wallstreetcn']

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("pipeline.log","a","utf-8"),
              logging.StreamHandler()])

def strip(x:str)->str: return re.sub(r"<[^>]+>","",html.unescape(x)).strip()
def row(n:dict): return [n['time'],n['src'],n['title'],n['summary'],n['url']]

# ─── 1. 关键词生成 ───
def em_secid(ts_code:str)->str:
    code,ex=ts_code.split(".")
    mkt = "1" if ex in ("SH","XSHG") else "0"
    return f"{mkt}.{code}"

async def top10_eastmoney(ts_code:str)->list[str]:
    secid=em_secid(ts_code)
    url=f"https://push2.eastmoney.com/api/qt/etf/top10?secid={secid}"
    r=await httpx.AsyncClient().get(url,timeout=20)
    js=r.json()
    return [it['name'] for it in js.get('data',{}).get('stockinfo',[])[:10]]

# --- Tushare 兜底, 且每分钟≤2次 ---
_last_call=0.0
async def top10_tushare(ts_code:str)->list[str]:
    global _last_call
    if not TOKEN: return []
    # 简单限速：距离上次调用 <35s 就 sleep
    gap=time.time()-_last_call
    if gap<35: await asyncio.sleep(35-gap)
    _last_call=time.time()

    code=ts_code.split(".")[0]
    cli=httpx.AsyncClient(timeout=20)
    payload={"api_name":"index_weight","token":TOKEN,
             "params":{"index_code":f"{code}.CI","trade_date":TODAY},
             "fields":"con_code,weight"}
    js=(await cli.post("https://api.tushare.pro",json=payload)).json()
    codes=[c for c,_ in js.get('data',{}).get('items',[])[:10]]
    if not codes: return []
    payload={"api_name":"stock_basic","token":TOKEN,
             "params":{"ts_code":",".join(codes)},"fields":"ts_code,name"}
    js=(await cli.post("https://api.tushare.pro",json=payload)).json()
    return [n for _,n in js.get('data',{}).get('items',[])]

async def build_keywords()->set[str]:
    fp=Path("holdings.json")
    if not fp.is_file():
        logging.warning("holdings.json not found; keywords empty")
        Path("keywords_used.txt").write_text("")
        return set()
    holds=json.loads(fp.read_text("utf-8"))
    kw=set()
    for h in holds:
        ts=h['symbol']
        names=[]
        try:
            names=await top10_eastmoney(ts)
        except Exception as e:
            logging.warning(f"EM {ts} 404/err {e}")
        if not names:
            try: names=await top10_tushare(ts)
            except Exception as e: logging.warning(f"TS {ts} err {e}")
        kw.update(names)
    kw_list=sorted(kw)
    Path("keywords_used.txt").write_text("\n".join(kw_list),"utf-8")
    return set(kw_list)

# ─── 2. Tushare news API (本地缓存) ───
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
        logging.warning(f"news API {tag} code {js['code']}")
        # 若当天超额, 读昨日缓存
        prev=Path(f"news_{tag}_{(datetime.now(TZ)-timedelta(days=1)).strftime('%Y%m%d')}.json")
        return json.loads(prev.read_text("utf-8")) if prev.is_file() else []
    items=[]
    for t,con,u,dt,_ in js.get("data",{}).get("items",[]) or []:
        items.append(dict(title=t,summary=strip(con)[:180],url=u,
                          time=dtparse.parse(dt).astimezone(TZ).isoformat(),
                          src=f"API-{tag}"))
    cache.write_text(json.dumps(items,ensure_ascii=False))
    return items

# ─── 3. RSS ───
async def rss(url:str,src:str)->list[dict]:
    try:
        r=await httpx.AsyncClient().get(url,timeout=20)
        if r.status_code!=200: return []
        import xml.etree.ElementTree as ET
        root=ET.fromstring(r.text); out=[]
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
        logging.warning(f"RSS {src} err {e}"); return []

def hit_kw(n:dict,kw:set)->bool:
    txt=(n["title"]+" "+n["summary"]).lower()
    return any(k.lower() in txt for k in kw)

def within7d(iso:str)->bool:
    return dtparse.isoparse(iso)>=datetime.now(TZ)-timedelta(days=7)

# ─── main ───
async def main():
    kws=set(await build_keywords())
    tasks=[ts_news(t) for t in TAGS]+[
        rss("https://rsshub.app/cls/telegraph","财联社"),
        rss("https://rss.sina.com.cn/roll/finance/hot_roll.xml","新浪财经")
    ]
    res=await asyncio.gather(*tasks)
    news=[n for lst in res for n in lst]

    # 保留 API 新闻；RSS 需关键词&7d
    news=[n for n in news if n["src"].startswith("API-") or (hit_kw(n,kws) and within7d(n["time"]))]
    news.sort(key=lambda x:x["time"],reverse=True)

    with Path("news_all.csv").open("w",newline="",encoding="utf-8-sig") as f:
        csv.writer(f).writerows([["time","source","title","summary","url"],*map(row,news)])
    logging.info(f"news_all.csv 写入 {len(news)} 条；关键词 {len(kws)} 个")

if __name__=="__main__":
    asyncio.run(main())
