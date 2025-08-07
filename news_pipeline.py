# -*- coding: utf-8 -*-
"""
news_pipeline.py  r18  (2025-08-07)
────────────────────────────────────
关键词 = 每只 ETF 前 10 大重仓股中文名
步骤：
1. 先调东财 fund.eastmoney.com 网页抓持仓
2. 失败则调 Tushare index_weight + stock_basic （跨最近 5 个交易日，自动限速）
3. 写 keywords_used.txt（保证非空，如实在拿不到会在 errors.log 记错）
4. 抓新闻：
   • Tushare news API（7 个源，日缓存；超限读昨日）
   • RSS 财联社 / RSS 新浪（仅保留含关键词且 7 日内）
5. 输出 news_all.csv + 详细的 pipeline.log / errors.log
依赖： httpx · python-dateutil · beautifulsoup4 · lxml
必需 Secrets：TUSHARE_TOKEN
"""

from __future__ import annotations
import asyncio, httpx, os, json, csv, re, logging, html, time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from dateutil import parser as dtparse
from bs4 import BeautifulSoup

# ── 运行环境 ──────────────────────────────────────────────
TZ     = timezone(timedelta(hours=8))
TODAY  = datetime.now(TZ).strftime("%Y%m%d")
TOKEN  = os.getenv("TUSHARE_TOKEN", "")
TAGS   = ['fenghuang','jinrongjie','10jqka','sina',
          'yuncaijing','eastmoney','wallstreetcn']

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("pipeline.log", "a", "utf-8"),
              logging.StreamHandler()]
)

ERR_LOG = Path("errors.log")

def log_err(msg:str):
    with ERR_LOG.open("a", encoding="utf-8") as f:
        f.write(f"{datetime.now(TZ)}  {msg}\n")

def strip(x:str) -> str:
    return re.sub(r"<[^>]+>", "", html.unescape(x)).strip()

def csv_row(n:dict):
    return [n["time"], n["src"], n["title"], n["summary"], n["url"]]

# ── 1. 关键词生成 ────────────────────────────────────────
def em_secid(ts_code:str) -> str:
    code, ex = ts_code.split(".")
    mkt = "1" if ex in ("SH", "XSHG") else "0"
    return f"{mkt}.{code}"

async def top10_eastmoney(ts_code:str) -> list[str]:
    """东财网页解析前 10 大重仓股中文名"""
    code = ts_code.split(".")[0]
    url  = f"https://fund.eastmoney.com/{code}.html"
    r = await httpx.AsyncClient().get(url, timeout=20)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}")
    soup = BeautifulSoup(r.text, "lxml")
    table = soup.select_one("#etfinfoTable")
    if not table:
        raise RuntimeError("table not found")
    out = [td.get_text(strip=True) for td in table.select("td:nth-child(1)")[:10]]
    return [n for n in out if n]

async def top10_tushare(ts_code:str) -> list[str]:
    """最近 5 个交易日，多日回溯，取前 10 名"""
    if not TOKEN:
        return []
    cli = httpx.AsyncClient(timeout=20)
    code = ts_code.split(".")[0]
    today = datetime.now(TZ)
    for d in range(5):
        day = (today - timedelta(days=d)).strftime("%Y%m%d")
        body = {"api_name": "index_weight", "token": TOKEN,
                "params": {"index_code": f"{code}.CI", "trade_date": day},
                "fields": "con_code,weight"}
        js = (await cli.post("https://api.tushare.pro", json=body)).json()
        if js.get("code"):
            continue                       # 可能是 400xx 超额
        items = js.get("data", {}).get("items", [])
        if not items:
            continue
        codes = [c for c, _ in items[:10]]
        body = {"api_name": "stock_basic", "token": TOKEN,
                "params": {"ts_code": ",".join(codes)}, "fields": "ts_code,name"}
        js   = (await cli.post("https://api.tushare.pro", json=body)).json()
        return [n for _, n in js.get("data", {}).get("items", [])]
    return []

async def build_keywords() -> set[str]:
    fp = Path("holdings.json")
    if not fp.is_file():
        logging.warning("holdings.json 缺失，关键词为空")
        Path("keywords_used.txt").write_text("")
        return set()

    holds = json.loads(fp.read_text("utf-8"))
    kw    = set()

    for h in holds:
        ts = h["symbol"]
        names = []
        try:
            names = await top10_eastmoney(ts)
        except Exception as e:
            log_err(f"ETF {ts}  eastmoney  {e}")
        if not names:
            try:
                names = await top10_tushare(ts)
            except Exception as e:
                log_err(f"ETF {ts}  index_weight  {e}")

        if not names:
            log_err(f"ETF {ts}  无法获取重仓")
        kw.update(names)

    Path("keywords_used.txt").write_text("\n".join(sorted(kw)), "utf-8")
    return kw

# ── 2. Tushare news API（缓存 + 降级） ──────────────────
async def ts_news(tag:str) -> list[dict]:
    cache = Path(f"news_{tag}_{TODAY}.json")
    if cache.is_file():
        return json.loads(cache.read_text("utf-8"))

    if not TOKEN:
        return []

    body = {"api_name": "news", "token": TOKEN,
            "params": {"src": tag, "start_date": TODAY, "end_date": TODAY},
            "fields": "title,content,url,datetime,src"}
    r  = await httpx.AsyncClient().post("https://api.tushare.pro", json=body, timeout=30)
    js = r.json()

    if js.get("code"):                       # 超额
        log_err(f"news API {tag}  code={js['code']}")
        prev = Path(f"news_{tag}_{(datetime.now(TZ) - timedelta(days=1)).strftime('%Y%m%d')}.json")
        return json.loads(prev.read_text("utf-8")) if prev.is_file() else []

    items = [dict(title=t,
                  summary=strip(con)[:180],
                  url=u,
                  time=dtparse.parse(dt).astimezone(TZ).isoformat(),
                  src=f"API-{tag}")
             for t, con, u, dt, _ in js.get("data", {}).get("items", [])]
    cache.write_text(json.dumps(items, ensure_ascii=False))
    return items

# ── 3. RSS ─────────────────────────────────────────────
async def rss(url:str, src:str) -> list[dict]:
    try:
        r = await httpx.AsyncClient().get(url, timeout=20)
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code}")
        import xml.etree.ElementTree as ET
        root = ET.fromstring(r.text)
        out  = []
        for it in root.findall(".//item"):
            ttl = strip(it.findtext("title") or "")
            link = it.findtext("link") or ""
            desc = strip(it.findtext("description") or "")[:180]
            pub_raw = it.findtext("pubDate") or ""
            try:
                pub = dtparse.parse(pub_raw).astimezone(TZ).isoformat()
            except Exception:
                pub = datetime.now(TZ).isoformat()
            out.append(dict(title=ttl, summary=desc, url=link, time=pub, src=src))
        return out
    except Exception as e:
        log_err(f"RSS {src}  {e}")
        return []

def hit_kw(n:dict, kw:set) -> bool:
    txt = (n["title"] + " " + n["summary"]).lower()
    return any(k.lower() in txt for k in kw)

def within_7d(iso:str) -> bool:
    return dtparse.isoparse(iso) >= datetime.now(TZ) - timedelta(days=7)

# ── 4. 主流程 ───────────────────────────────────────────
async def main():
    kw = set(await build_keywords())

    tasks = [ts_news(t) for t in TAGS] + [
        rss("https://rsshub.app/cls/telegraph", "财联社"),
        rss("https://rss.sina.com.cn/roll/finance/hot_roll.xml", "新浪财经")
    ]
    res   = await asyncio.gather(*tasks)
    news  = [n for lst in res for n in lst]

    news  = [n for n in news if n["src"].startswith("API-") or (hit_kw(n, kw) and within_7d(n["time"]))]
    news.sort(key=lambda x: x["time"], reverse=True)

    with Path("news_all.csv").open("w", newline="", encoding="utf-8-sig") as f:
        csv.writer(f).writerows([["time", "source", "title", "summary", "url"], *map(csv_row, news)])

    logging.info(f"news_all.csv 写入 {len(news)} 条；关键词 {len(kw)} 个；错误记录 {ERR_LOG.stat().st_size if ERR_LOG.exists() else 0} bytes")

if __name__ == "__main__":
    asyncio.run(main())
