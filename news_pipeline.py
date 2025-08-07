# -*- coding: utf-8 -*-
"""
news_pipeline.py  r20  (2025-08-07)
  • ETF 成分股来源：
      1) push2 API  → /qt/etf/top10?secid=1.<code>&pn=10&type=weight
      2) F10 API    → FundArchivesDatas.aspx?type=jjcc&code=<code>&topline=10
      3)（兜底）Tushare index_weight
  • 错误全部写 errors.log
  • User-Agent 统一
依赖：httpx·python-dateutil·beautifulsoup4·lxml
"""
from __future__ import annotations
import asyncio, httpx, os, json, csv, re, logging, html, json as jmod
from datetime import datetime, timedelta, timezone
from pathlib import Path
from dateutil import parser as dtparse
from bs4 import BeautifulSoup

TZ    = timezone(timedelta(hours=8))
NOW   = datetime.now(TZ)
TODAY = NOW.strftime("%Y%m%d")
TOKEN = os.getenv("TUSHARE_TOKEN","").strip()
TAGS  = ['fenghuang','jinrongjie','10jqka','sina',
         'yuncaijing','eastmoney','wallstreetcn']
UA = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("pipeline.log","a","utf-8"),
              logging.StreamHandler()])
ERR = Path("errors.log"); ERR.touch(exist_ok=True)
# 原来的 logerr() 中 Path.write_text(..., append=False) 会报错
def logerr(msg: str):
    logging.error(msg)
    with ERR.open("a", encoding="utf-8") as f:     # 统一用 open("a")
        f.write(f"{datetime.now(TZ)}  {msg}\n")


def strip(x:str)->str: return re.sub(r"<[^>]+>","",html.unescape(x or "")).strip()
def row(n): return [n['time'],n['src'],n['title'],n['summary'],n['url']]

# ── 1. ETF 成分股 ──────────────────────────────────────
async def push2_top10(ts_code: str) -> list[str]:
    code, ex = ts_code.split(".")
    secid = ("1" if ex.startswith("SH") else "0") + f".{code}"
    url = (f"https://push2.eastmoney.com/api/qt/etf/top10?"
           f"secid={secid}&pn=10&type=weight")
    r = await httpx.AsyncClient().get(url, headers=UA, timeout=15)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}")
    # push2 有时返回 “var({})” 或空白，先做简单校验
    try:
        js = r.json()
    except Exception:
        raise RuntimeError("JSON decode fail")
    data = js.get("data") or {}
    return [d["name"] for d in data.get("weightList", [])]


async def f10_top10(ts_code:str)->list[str]:
    code=ts_code.split(".")[0]
    url=(f"https://fundf10.eastmoney.com/"
         f"FundArchivesDatas.aspx?type=jjcc&code={code}&topline=10")
    r=await httpx.AsyncClient().get(url,headers=UA,timeout=15)
    m=re.search(r'var apidata=\{ content:"(.+?)"\}',r.text)
    if not m: return []
    table=BeautifulSoup(html.unescape(m.group(1)),"lxml")
    return [td.get_text(strip=True) for td in table.select("td")[:10]]

async def ts_top10(ts_code:str)->list[str]:
    if not TOKEN: return []
    code=ts_code.split(".")[0]; cli=httpx.AsyncClient(timeout=20)
    for off in range(5):
        day=(NOW-timedelta(days=off)).strftime("%Y%m%d")
        js=(await cli.post("https://api.tushare.pro",
             json={"api_name":"index_weight","token":TOKEN,
                   "params":{"index_code":f"{code}.CI","trade_date":day},
                   "fields":"con_code,weight"})).json()
        if js.get("code"): continue
        items=js.get("data",{}).get("items",[])
        if items:
            codes=[c for c,_ in items[:10]]
            js=(await cli.post("https://api.tushare.pro",
                 json={"api_name":"stock_basic","token":TOKEN,
                       "params":{"ts_code":",".join(codes)},
                       "fields":"ts_code,name"})).json()
            return [n for _,n in js.get("data",{}).get("items",[])]
    return []

async def etf_names(ts_code: str) -> list[str]:
    for fn in (push2_top10, f10_top10, ts_top10):
        try:
            names = await fn(ts_code)
            if names:
                return names
        except Exception as e:
            # 把异常写入错误日志，继续尝试下一个源
            logerr(f"{ts_code} {fn.__name__} {e}")
    logerr(f"{ts_code} 所有源均失败")
    return []


async def build_kw()->set[str]:
    fp=Path("holdings.json")
    if not fp.is_file():
        logerr("holdings.json missing"); Path("keywords_used.txt").write_text("")
        return set()
    holds=json.loads(fp.read_text("utf-8"))
    kw=set()
    for h in holds: kw.update(await etf_names(h['symbol']))
    if not kw: logerr("Empty keyword list")
    Path("keywords_used.txt").write_text("\n".join(sorted(kw)),"utf-8")
    return kw

# ── 2. Tushare news（含缓存） ───────────────────────────
async def ts_news(tag):
    cache=Path(f"news_{tag}_{TODAY}.json")
    if cache.is_file(): return json.loads(cache.read_text())
    if not TOKEN: return []
    js=(await httpx.AsyncClient().post("https://api.tushare.pro",
         json={"api_name":"news","token":TOKEN,
               "params":{"src":tag,"start_date":TODAY,"end_date":TODAY},
               "fields":"title,content,url,datetime,src"})).json()
    if js.get("code"):
        logerr(f"news API {tag} {js['code']}")
        prev=Path(f"news_{tag}_{(NOW-timedelta(days=1)).strftime('%Y%m%d')}.json")
        return json.loads(prev.read_text()) if prev.is_file() else []
    items=[dict(title=t,summary=strip(c)[:180],url=u,
                time=dtparse.parse(dt).astimezone(TZ).isoformat(),
                src=f"API-{tag}")
           for t,c,u,dt,_ in js.get("data",{}).get("items",[])]
    cache.write_text(jmod.dumps(items,ensure_ascii=False))
    return items

# ── 3. RSS ─────────────────────────────────────────────
async def rss(url,src):
    try:
        r=await httpx.AsyncClient().get(url,headers=UA,timeout=15)
        import xml.etree.ElementTree as ET
        out=[]; root=ET.fromstring(r.text)
        for it in root.findall(".//item"):
            ttl=strip(it.findtext("title")); link=it.findtext("link") or ""
            desc=strip(it.findtext("description"))[:180]
            pub=dtparse.parse(it.findtext("pubDate") or NOW.isoformat()).astimezone(TZ)
            out.append(dict(title=ttl,summary=desc,url=link,
                            time=pub.isoformat(),src=src))
        return out
    except Exception as e:
        logerr(f"RSS {src} {e}"); return []

def hit(n,kw): return any(k.lower() in (n['title']+n['summary']).lower() for k in kw)
def within7(s): return dtparse.isoparse(s)>=NOW-timedelta(days=7)

# ── 4. main ────────────────────────────────────────────
async def main():
    kw=set(await build_kw())
    tasks=[ts_news(t) for t in TAGS]+[
        rss("https://rsshub.app/cls/telegraph","财联社"),
        rss("https://rss.sina.com.cn/roll/finance/hot_roll.xml","新浪财经")]
    news=[n for lst in await asyncio.gather(*tasks) for n in lst]
    news=[n for n in news if n['src'].startswith("API-") or (hit(n,kw) and within7(n['time']))]
    news.sort(key=lambda x:x['time'],reverse=True)
    Path("news_all.csv").write_text("")  # 清空旧文件
    with Path("news_all.csv").open("w",newline="",encoding="utf-8-sig") as f:
        csv.writer(f).writerows([["time","source","title","summary","url"],*map(row,news)])
    logging.info(f"✅ 写入 {len(news)} 条；关键词 {len(kw)} 个；errors.log {ERR.stat().st_size}B")

if __name__=="__main__":
    asyncio.run(main())
