# -*- coding: utf-8 -*-
"""
news_pipeline.py  r15  (2025-08-07)
-------------------------------------------------
1. 关键词逻辑
   · 读取 holdings.json → ETF 列表
   · 对每只 ETF 先调东财 push2 接口拿前 10 大成份股「中文简称」
   · 如东财失败，再用 Tushare index_weight + stock_basic 兜底
   · 汇总去重 → keywords_used.txt

2. 新闻抓取
   · tushare `news` API：凤凰 / 金融界 / 同花顺 / 新浪 / 云财经 / 东方财富 / 华尔街见闻
     + 本地 (每日) 缓存，接口超限时用前日缓存
   · RSS 财联社、RSS 新浪 仅保留「含关键词 & 7 日内」
   · 不去重，全部写入 news_all.csv

3. 依赖
   httpx · python-dateutil
   必需 Secrets:  TUSHARE_TOKEN
"""
from __future__ import annotations
import asyncio, httpx, os, json, csv, re, logging, html
from datetime import datetime, timedelta, timezone
from pathlib import Path
from dateutil import parser as dtparse

# ─── 基本配置 ───
TZ     = timezone(timedelta(hours=8))
TODAY  = datetime.now(TZ).strftime("%Y%m%d")
TOKEN  = os.getenv("TUSHARE_TOKEN", "")     # 必填
TAG_LST= ['fenghuang','jinrongjie','10jqka','sina',
          'yuncaijing','eastmoney','wallstreetcn']

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("pipeline.log", "a", "utf-8"),
              logging.StreamHandler()]
)

# ─── 工具函数 ───
def strip(x:str) -> str:
    return re.sub(r"<[^>]+>", "", html.unescape(x)).strip()

def csv_row(n:dict):
    return [n["time"], n["src"], n["title"], n["summary"], n["url"]]

async def call_push2(sym:str)->list[str]:
    """东财接口获取 ETF 前10 持仓中文名"""
    url=f"https://push2.eastmoney.com/api/qt/etf/top10?&symbol={sym}.SH"
    r=await httpx.AsyncClient().get(url, timeout=20)
    js=r.json()
    return [it["name"] for it in js.get("data",{}).get("stockinfo",[])[:10]]

async def call_ts_weight(sym:str)->list[str]:
    """Tushare 兜底：index_weight+stock_basic"""
    if not TOKEN: return []
    cli=httpx.AsyncClient(timeout=20)
    payload={"api_name":"index_weight","token":TOKEN,
             "params":{"index_code": sym+".CI","trade_date":TODAY},
             "fields":"con_code,weight"}
    js=(await cli.post("https://api.tushare.pro",json=payload)).json()
    codes=[c for c,_ in js.get("data",{}).get("items",[])[:10]]
    if not codes: return []
    payload={"api_name":"stock_basic","token":TOKEN,
             "params":{"ts_code":",".join(codes)},"fields":"ts_code,name"}
    js=(await cli.post("https://api.tushare.pro",json=payload)).json()
    return [n for _,n in js.get("data",{}).get("items",[])]

async def build_keywords()->set[str]:
    fp=Path("holdings.json")
    if not fp.is_file():
        logging.warning("holdings.json 缺失，关键词为空")
        Path("keywords_used.txt").write_text("")
        return set()
    holds=json.loads(fp.read_text("utf-8"))
    kw=set()
    for h in holds:
        sym=h["symbol"].split(".")[0]
        names=[]
        try:
            names=await call_push2(sym)          # 东财优先
            if not names:
                names=await call_ts_weight(sym)  # Tushare 兜底
        except Exception as e:
            logging.warning(f"{sym} 获取持仓失败: {e}")
        kw.update(names)
    kw_list=sorted(kw)
    Path("keywords_used.txt").write_text("\n".join(kw_list),"utf-8")
    return set(kw_list)

# ─── Tushare news API (每日缓存) ───
async def ts_news(tag:str)->list[dict]:
    cache=Path(f"ts_news_{tag}_{TODAY}.json")
    if cache.is_file():
        return json.loads(cache.read_text("utf-8"))
    if not TOKEN: return []
    body={"api_name":"news","token":TOKEN,
          "params":{"src":tag,"start_date":TODAY,"end_date":TODAY},
          "fields":"title,content,url,datetime,src"}
    try:
        r=await httpx.AsyncClient().post("https://api.tushare.pro",json=body,timeout=30)
        js=r.json()
        if js.get("code"):
            logging.warning(f"news API {tag} code {js['code']}")
            return []
        items=[]
        for t,con,u,dt,_ in js.get("data",{}).get("items",[]) or []:
            items.append(dict(
                title=t,
                summary=strip(con)[:180],
                url=u,
                time=dtparse.parse(dt).astimezone(TZ).isoformat(),
                src=f"API-{tag}"
            ))
        cache.write_text(json.dumps(items,ensure_ascii=False))
        return items
    except Exception as e:
        logging.warning(f"news API {tag} err {e}")
        # 读昨日缓存兜底
        prev=Path(f"ts_news_{tag}_{(datetime.now(TZ)-timedelta(days=1)).strftime('%Y%m%d')}.json")
        return json.loads(prev.read_text("utf-8")) if prev.is_file() else []

# ─── RSS 抓取 ───
async def rss(url:str, src:str)->list[dict]:
    try:
        r=await httpx.AsyncClient().get(url, timeout=20)
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

def within_7d(iso:str)->bool:
    return dtparse.isoparse(iso)>=datetime.now(TZ)-timedelta(days=7)

# ─── 主流程 ───
async def main():
    kw=set(await build_keywords())
    tasks=[ts_news(t) for t in TAG_LST]+[
        rss("https://rsshub.app/cls/telegraph","财联社"),
        rss("https://rss.sina.com.cn/roll/finance/hot_roll.xml","新浪财经")
    ]
    res=await asyncio.gather(*tasks)
    news=[]
    for lst in res: news.extend(lst)

    # 过滤 RSS：关键词 + 7 日
    news=[n for n in news if n["src"].startswith("API-") or (hit_kw(n,kw) and within_7d(n["time"]))]
    news.sort(key=lambda x:x["time"], reverse=True)

    with Path("news_all.csv").open("w",newline="",encoding="utf-8-sig") as f:
        csv.writer(f).writerows([["time","source","title","summary","url"],*map(csv_row,news)])
    logging.info(f"写入 {len(news)} 条 news_all.csv")

if __name__=="__main__":
    asyncio.run(main())
