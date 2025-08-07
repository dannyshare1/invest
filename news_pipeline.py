# -*- coding: utf-8 -*-
"""
news_pipeline.py r14
关键词 = 每只 ETF 当日 index_weight 前 10 大成份股中文名
抓取逻辑:
    1) tushare.pro 网页 (需 ts_token Cookie)
    2) 若 302 → tushare 'news' API + 本地缓存
    3) RSS 财联社 / 新浪 仅保留含关键词 且 7 日内
输出:
    news_all.csv, keywords_used.txt （写在仓库根目录）
"""
from __future__ import annotations
import asyncio, httpx, os, csv, json, re, logging, html
from datetime import datetime, timedelta, timezone
from pathlib import Path
from bs4 import BeautifulSoup
from dateutil import parser as dtparse

TZ   = timezone(timedelta(hours=8))
TODAY= datetime.now(TZ).strftime("%Y%m%d")
COOKIE= os.getenv("TUSHARE_COOKIE","")
TOKEN = os.getenv("TUSHARE_TOKEN","")

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(levelname)s - %(message)s",
                    handlers=[logging.FileHandler("pipeline.log","a","utf-8"),
                              logging.StreamHandler()])

HEADERS = {"Cookie": COOKIE} if COOKIE else {}
TAG_LIST=['fenghuang','jinrongjie','10jqka','sina','yuncaijing','eastmoney','wallstreetcn']

# ---------- 辅助 ----------
def strip(x:str)->str:
    return re.sub(r"<[^>]+>","",html.unescape(x)).strip()

def row(n:dict): return [n["time"],n["src"],n["title"],n["summary"],n["url"]]

# ---------- 获取 ETF 前10 成份股中文名 ----------
async def top10_names(etf_code:str)->list[str]:
    if not TOKEN: return []
    cli=httpx.AsyncClient(timeout=20)
    # 1) index_weight
    payload={"api_name":"index_weight","token":TOKEN,
             "params":{"index_code": etf_code.split(".")[0]+".CI","trade_date":TODAY},
             "fields":"con_code,weight"}
    js=(await cli.post("https://api.tushare.pro",json=payload)).json()
    items=js.get("data",{}).get("items",[]) if not js.get("code") else []
    codes=[c for c,_ in items[:10]]
    if not codes: return []
    # 2) stock_basic to name
    payload={"api_name":"stock_basic","token":TOKEN,
             "params":{"ts_code":",".join(codes)},"fields":"ts_code,name"}
    js=(await cli.post("https://api.tushare.pro",json=payload)).json()
    names=[n for _,n in js.get("data",{}).get("items",[])]
    return names

async def build_keywords()->set[str]:
    fp=Path("holdings.json")
    if not fp.is_file(): return set()
    holds=json.loads(fp.read_text("utf-8"))
    kws=set()
    for h in holds:
        try:
            kws.update(await top10_names(h["symbol"]))
        except Exception as e:
            logging.warning(f"top10 {h['symbol']} err {e}")
    Path("keywords_used.txt").write_text("\n".join(sorted(kws)),"utf-8")
    return kws

# ---------- tushare 网页 & API ----------
async def ts_web(tag)->list[dict]:
    url=f"https://tushare.pro/news/{tag}"
    r=await httpx.AsyncClient().get(url,headers=HEADERS,timeout=20,follow_redirects=False)
    if r.status_code==302: raise RuntimeError("NEED_LOGIN")
    soup=BeautifulSoup(r.text,"html.parser")
    cat=soup.select_one(".news-title h1").get_text(strip=True) if soup.select_one(".news-title h1") else tag
    out=[]
    for li in soup.select("ul.news-list li"):
        a=li.select_one("a"); tm=li.select_one(".time")
        if not (a and tm): continue
        ts=dtparse.parse(tm.get_text(strip=True)).astimezone(TZ).isoformat()
        out.append(dict(title=a.get_text(strip=True),summary="",url=a["href"],
                        time=ts,src=f"TS-{tag}/{cat}"))
    return out

async def ts_api(tag)->list[dict]:
    if not TOKEN: return []
    cache=Path(f"ts_api_{tag}_{TODAY}.json")
    if cache.is_file(): return json.loads(cache.read_text("utf-8"))
    payload={"api_name":"news","token":TOKEN,
             "params":{"src":tag,"start_date":TODAY,"end_date":TODAY},
             "fields":"title,content,url,datetime,src"}
    r=await httpx.AsyncClient().post("https://api.tushare.pro",json=payload,timeout=30)
    js=r.json()
    if js.get("code"):
        logging.warning(f"API {tag} code {js['code']}")
        return []
    items=[]
    for t,con,u,dt,_ in js.get("data",{}).get("items",[]) or []:
        items.append(dict(title=t,summary=strip(con)[:180],url=u,
                          time=dtparse.parse(dt).astimezone(TZ).isoformat(),
                          src=f"API-{tag}"))
    cache.write_text(json.dumps(items,ensure_ascii=False))
    return items

async def grab_tag(tag):
    try:
        data=await ts_web(tag)
        if data: return data
    except RuntimeError:
        logging.info(f"{tag}→需要登录，用 API")
    except Exception as e:
        logging.warning(e)
    data=await ts_api(tag)
    if data: return data
    # 最后读昨日缓存
    prev=Path(f"ts_api_{tag}_{(datetime.now(TZ)-timedelta(days=1)).strftime('%Y%m%d')}.json")
    return json.loads(prev.read_text("utf-8")) if prev.is_file() else []

# ---------- RSS ----------
async def rss(url,src):
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
        logging.warning(e); return []

def in7d(iso): return dtparse.isoparse(iso)>=datetime.now(TZ)-timedelta(days=7)

# ---------- 主 ----------
async def main():
    kw=set(await build_keywords())
    tasks=[grab_tag(t) for t in TAG_LIST]+[
        rss("https://rsshub.app/cls/telegraph","财联社"),
        rss("https://rss.sina.com.cn/roll/finance/hot_roll.xml","新浪财经")
    ]
    res=await asyncio.gather(*tasks)
    news=[]
    for lst in res: news.extend(lst)
    # 保留 7d 内且含关键词 的 RSS；网页/API 全保留
    news=[n for n in news if n["src"].startswith(("TS-","API-")) or (in7d(n["time"]) and any(k in n["title"] for k in kw))]
    news.sort(key=lambda x:x["time"],reverse=True)
    with Path("news_all.csv").open("w",newline="",encoding="utf-8-sig") as f:
        csv.writer(f).writerows([["time","source","title","summary","url"],*map(row,news)])
    logging.info(f"写入 {len(news)} 条 news_all.csv")

if __name__=="__main__":
    asyncio.run(main())
