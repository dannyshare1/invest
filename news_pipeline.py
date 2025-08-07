# -*- coding: utf-8 -*-
"""
news_pipeline.py  r19  (2025-08-07)
改动：错误日志必生成 + ETF 网页解析更鲁棒 + 控制台 ERROR 提示
"""
from __future__ import annotations
import asyncio, httpx, os, json, csv, re, logging, html
from datetime import datetime, timedelta, timezone
from pathlib import Path
from dateutil import parser as dtparse
from bs4 import BeautifulSoup

# ─── 基本配置 ─────────────────────────────────────────────
TZ     = timezone(timedelta(hours=8))
TODAY  = datetime.now(TZ).strftime("%Y%m%d")
TOKEN  = os.getenv("TUSHARE_TOKEN", "").strip()
TAGS   = ['fenghuang','jinrongjie','10jqka','sina',
          'yuncaijing','eastmoney','wallstreetcn']

UA_HDR = {"User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
          "(KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("pipeline.log", "a", "utf-8"),
              logging.StreamHandler()]
)
ERR_LOG = Path("errors.log"); ERR_LOG.touch(exist_ok=True)

def log_err(msg:str):
    logging.error(msg)
    with ERR_LOG.open("a", encoding="utf-8") as f:
        f.write(f"{datetime.now(TZ)}  {msg}\n")

def strip_html(x:str)->str:
    return re.sub(r"<[^>]+>", "", html.unescape(x or "")).strip()

def csv_row(n:dict): return [n["time"], n["src"], n["title"], n["summary"], n["url"]]

# ─── 1. ETF 前 10 持仓 ───────────────────────────────────
async def eastmoney_top10(ts_code:str)->list[str]:
    code = ts_code.split(".")[0]
    url  = f"https://fund.eastmoney.com/{code}.html"
    r    = await httpx.AsyncClient().get(url, headers=UA_HDR, timeout=20)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}")
    soup = BeautifulSoup(r.text, "lxml")

    table = soup.select_one("#etfinfoTable")                       # 场内 ETF
    if not table:                                                  # 场外基金页面备选
        table = soup.find("table", text=re.compile("十大重仓股票"))
    if not table:
        # 把前三行 HTML 片段写入 errors.log 方便事后排查
        frag = "\n".join(str(x)[:200] for x in soup.select("table")[:3])
        log_err(f"{ts_code} eastmoney 无表格\n{frag}")
        return []

    names = [td.get_text(strip=True) for td in table.select("td:nth-child(1)")[:10]]
    return [n for n in names if n]

async def tushare_top10(ts_code:str)->list[str]:
    if not TOKEN:
        return []
    cli  = httpx.AsyncClient(timeout=20)
    code = ts_code.split(".")[0]
    for offset in range(5):                                 # 回溯 5 天
        day = (datetime.now(TZ) - timedelta(days=offset)).strftime("%Y%m%d")
        body = {"api_name":"index_weight","token":TOKEN,
                "params":{"index_code":f"{code}.CI","trade_date":day},
                "fields":"con_code,weight"}
        js = (await cli.post("https://api.tushare.pro", json=body)).json()
        if js.get("code"):            # 权限/超额
            continue
        items = js.get("data",{}).get("items",[])
        if not items:
            continue
        codes = [c for c,_ in items[:10]]
        body  = {"api_name":"stock_basic","token":TOKEN,
                 "params":{"ts_code":",".join(codes)}, "fields":"ts_code,name"}
        js    = (await cli.post("https://api.tushare.pro", json=body)).json()
        return [n for _, n in js.get("data",{}).get("items",[])]
    return []

async def get_etf_names(ts_code:str)->list[str]:
    try:
        names = await eastmoney_top10(ts_code)
        if names:
            return names
    except Exception as e:
        log_err(f"ETF {ts_code} eastmoney {e}")

    try:
        names = await tushare_top10(ts_code)
        if names:
            return names
    except Exception as e:
        log_err(f"ETF {ts_code} index_weight {e}")

    log_err(f"ETF {ts_code} 获取失败")
    return []

async def build_keywords()->set[str]:
    fp = Path("holdings.json")
    if not fp.is_file():
        logging.warning("⚠️ holdings.json 缺失，关键词为空")
        Path("keywords_used.txt").write_text("")
        log_err("holdings.json missing")
        return set()

    holds = json.loads(fp.read_text("utf-8"))
    kw    = set()
    for h in holds:
        kw.update(await get_etf_names(h["symbol"]))

    if not kw:
        logging.warning("⚠️ keywords_used.txt 为空！请检查 ETF 持仓抓取")
        log_err("Empty keywords list generated")

    Path("keywords_used.txt").write_text("\n".join(sorted(kw)), "utf-8")
    return kw

# ─── 2. Tushare news API（缓存 / 降级） ───────────────────
async def ts_news(tag:str)->list[dict]:
    cache = Path(f"news_{tag}_{TODAY}.json")
    if cache.is_file():
        return json.loads(cache.read_text("utf-8"))

    if not TOKEN:
        return []

    body = {"api_name":"news","token":TOKEN,
            "params":{"src":tag,"start_date":TODAY,"end_date":TODAY},
            "fields":"title,content,url,datetime,src"}
    r  = await httpx.AsyncClient().post("https://api.tushare.pro", json=body, timeout=30)
    js = r.json()
    if js.get("code"):
        log_err(f"news API {tag} code={js['code']}")
        prev = Path(f"news_{tag}_{(datetime.now(TZ)-timedelta(days=1)).strftime('%Y%m%d')}.json")
        return json.loads(prev.read_text("utf-8")) if prev.is_file() else []

    items = [dict(title=t,
                  summary=strip_html(con)[:180],
                  url=u,
                  time=dtparse.parse(dt).astimezone(TZ).isoformat(),
                  src=f"API-{tag}")
             for t, con, u, dt, _ in js.get("data",{}).get("items",[])]
    cache.write_text(json.dumps(items, ensure_ascii=False))
    return items

# ─── 3. RSS ───────────────────────────────────────────────
async def rss(url:str, src:str)->list[dict]:
    try:
        r = await httpx.AsyncClient().get(url, headers=UA_HDR, timeout=20)
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code}")
        import xml.etree.ElementTree as ET
        root = ET.fromstring(r.text)
        out  = []
        for it in root.findall(".//item"):
            ttl = strip_html(it.findtext("title") or "")
            link = it.findtext("link") or ""
            desc = strip_html(it.findtext("description") or "")[:180]
            pub_raw = it.findtext("pubDate") or ""
            try:
                pub = dtparse.parse(pub_raw).astimezone(TZ).isoformat()
            except Exception:
                pub = datetime.now(TZ).isoformat()
            out.append(dict(title=ttl, summary=desc, url=link, time=pub, src=src))
        return out
    except Exception as e:
        log_err(f"RSS {src} {e}")
        return []

def hit_kw(n:dict, kw:set)->bool:
    txt = (n["title"] + n["summary"]).lower()
    return any(k.lower() in txt for k in kw)

def within_7d(iso:str)->bool:
    return dtparse.isoparse(iso) >= datetime.now(TZ) - timedelta(days=7)

# ─── 4. 主入口 ────────────────────────────────────────────
async def main():
    kw = set(await build_keywords())

    tasks = [ts_news(t) for t in TAGS] + [
        rss("https://rsshub.app/cls/telegraph", "财联社"),
        rss("https://rss.sina.com.cn/roll/finance/hot_roll.xml", "新浪财经")
    ]
    res  = await asyncio.gather(*tasks)
    news = [n for lst in res for n in lst]

    news = [n for n in news if n["src"].startswith("API-") or (hit_kw(n, kw) and within_7d(n["time"]))]
    news.sort(key=lambda x: x["time"], reverse=True)

    with Path("news_all.csv").open("w", newline="", encoding="utf-8-sig") as f:
        csv.writer(f).writerows([["time","source","title","summary","url"], *map(csv_row, news)])

    logging.info(f"✅ news_all.csv 写入 {len(news)} 条；关键词 {len(kw)} 个；errors.log {ERR_LOG.stat().st_size} Bytes")

if __name__ == "__main__":
    asyncio.run(main())
