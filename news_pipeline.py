# -*- coding: utf-8 -*-
"""
news_pipeline.py — 基于 Google News + 聚合数据 的资讯收集器
要点
----
1) 不再调用 TuShare 的资讯/成分股接口。
2) 关键词来源：仅用你的 holdings.json 的“名称” + LLM(Qwen)补充短关键词（≤4字，中文优先）。
3) 新闻来源：
   - Google News RSS: https://news.google.com/rss/search?q=<query>
   - 聚合数据(可选): http://apis.juhe.cn/fapigx/caijing/query?k=<关键词>|<关键词>...
4) 先抓链接，再抓 **正文**（BeautifulSoup 提取 <p> 文本），用 OR 逻辑匹配关键词；命中的才写入。
5) 详细错误写入 errors.log；本轮使用过的关键词写入 keywords_used.txt；来源写入 sources_used.txt。
6) 输出：
   - news_all.csv（所有抓到且命中的新闻）
   - briefing.txt（简单汇总）
运行
----
pip install -r requirements.txt
python news_pipeline.py --days 3 --limit 400
需要环境变量：
    QWEN_API_KEY=...
    JUHE_KEY=...  # 聚合数据可留空；留空则跳过
"""
from __future__ import annotations
import os, re, sys, csv, json, asyncio, traceback, time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Any, Tuple
import httpx, feedparser
from bs4 import BeautifulSoup
from dateutil import parser as dtparse
from charset_normalizer import from_bytes
from rich import print as rprint

# ---------- 路径/常量 ----------
BASE = Path(".")
OUT_CSV = BASE/"news_all.csv"
OUT_BRI = BASE/"briefing.txt"
OUT_KW  = BASE/"keywords_used.txt"
OUT_SRC = BASE/"sources_used.txt"
ERR_LOG = BASE/"errors.log"
HOLDINGS = BASE/"holdings.json"
TZ = timezone(timedelta(hours=8))
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")

GOOGLE_NEWS_RSS = "https://news.google.com/rss/search"
JUHE_API = "http://apis.juhe.cn/fapigx/caijing/query"

# ---------- 小工具 ----------
def nowstr():
    return datetime.now(TZ).isoformat(timespec="seconds")

def logerr(msg: str, exc: Exception|None=None):
    line = f"{nowstr()} - {msg}"
    if exc:
        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        line += f"\n{tb}"
    with ERR_LOG.open("a", encoding="utf-8") as f:
        f.write(line + "\n")
    rprint(f"[red]ERROR[/red] - {msg}")

def info(msg: str):
    rprint(f"{nowstr()} - [cyan]{msg}[/cyan]")

def read_json(p: Path) -> Any:
    return json.loads(p.read_text("utf-8"))

def decode_bytes(b: bytes) -> str:
    # 解决“乱码”：用 charset-normalizer 自动识别
    res = from_bytes(b).best()
    return (res or "").output_text if res else b.decode("utf-8", errors="ignore")

def strip_text(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    # 主体取 <p> 文本，若过少，再退化取全页文本
    ps = [p.get_text(" ", strip=True) for p in soup.find_all("p")]
    body = " ".join(ps).strip()
    if len(body) < 120:
        body = soup.get_text(" ", strip=True)
    return re.sub(r"\s+", " ", body)

def norm_kw(kw: str) -> str:
    kw = kw.strip()
    # 只保留 ≤4 字的中文短词 或 简短英文 token
    if re.fullmatch(r"[\u4e00-\u9fff]{1,4}", kw):  # 中文1-4字
        return kw
    if re.fullmatch(r"[A-Za-z0-9\-]{1,12}", kw):
        return kw.lower()
    return ""

def hit_any(text: str, keywords: List[str]) -> bool:
    if not keywords:
        return True
    lt = text.lower()
    for k in keywords:
        k2 = k.lower()
        if k2 and k2 in lt:
            return True
    return False

# ---------- 关键词 ----------
async def call_qwen_short_keywords(names: List[str]) -> List[str]:
    api = "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation"
    key = os.getenv("QWEN_API_KEY")
    if not key:
        info("Qwen 未配置，跳过 LLM 关键词生成")
        return []
    prompt = (
        "请基于以下ETF/行业名称，列出**仅中文优先的短关键词**（每个≤4字；"
        "允许少量英文缩写≤12字符；侧重行业、赛道、产品、政策、上游材料、下游应用等），"
        "用顿号或空格分隔，不要解释：\n" + "、".join(names)
    )
    hdr = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    pl = {"model": "qwen-plus", "input": {"prompt": prompt},
          "parameters": {"max_tokens": 300, "temperature": 0.3}}
    async with httpx.AsyncClient(timeout=30) as c:
        try:
            r = await c.post(api, headers=hdr, json=pl)
            r.raise_for_status()
            text = r.json()["output"]["text"]
        except Exception as e:
            logerr("Qwen 调用失败", e); return []
    # 粗解析：按非字母/数字/中文切分
    raw = re.split(r"[，、,\s/|]+", text)
    out = []
    for w in raw:
        w2 = norm_kw(w)
        if w2:
            out.append(w2)
    # 去重
    seen, final = set(), []
    for x in out:
        if x not in seen:
            seen.add(x); final.append(x)
    return final[:120]

def base_keywords_from_holdings(holds: List[dict]) -> Tuple[List[str], List[str]]:
    """返回(ETF名称列表, 初始短关键词)"""
    etf_names = []
    base = []
    for h in holds:
        n = (h.get("name") or "").strip()
        s = (h.get("symbol") or "").strip()
        if n:
            etf_names.append(n)
            # 从 ETF 名称抽取 1-2 个关键词（去掉 “ETF/指数/基金”）
            n2 = re.sub(r"(ETF|指数|基金)", "", n, flags=re.I)
            # 取中文连续词/数字词
            toks = re.findall(r"[\u4e00-\u9fff]{2,4}|[A-Za-z0-9]{2,8}", n2)
            for t in toks[:2]:
                t2 = norm_kw(t)
                if t2:
                    base.append(t2)
        elif s:
            etf_names.append(s)
    # 去重
    seen, base2 = set(), []
    for x in base:
        if x not in seen:
            seen.add(x); base2.append(x)
    return etf_names, base2

# ---------- 抓 Google News ----------
async def fetch_google_news(keywords: List[str], days: int) -> List[dict]:
    # 组合 OR 查询（避免太长，分批）
    out: List[dict] = []
    since = datetime.now(TZ) - timedelta(days=days)
    queries = []
    batch, ln = [], 0
    for k in keywords:
        token = f"\"{k}\""
        if ln + len(token) > 120:  # 控 query 长度
            if batch: queries.append(" OR ".join(batch))
            batch, ln = [token], len(token)
        else:
            batch.append(token); ln += len(token) + 4
    if batch: queries.append(" OR ".join(batch))

    async with httpx.AsyncClient(headers={"User-Agent": UA}, timeout=20) as c:
        for q in queries:
            params = {"q": q, "hl": "zh-CN"}
            try:
                # 直接用 feedparser 读 RSS
                url = httpx.URL(GOOGLE_NEWS_RSS, params=params)
                # 不能直接让 feedparser 发网，我们先取文本自己喂它，便于日志与容错
                r = await c.get(str(url))
                if r.status_code != 200:
                    logerr(f"GoogleNews RSS 非200: {r.status_code}")
                    continue
                feed = feedparser.parse(r.content)
                for e in feed.entries:
                    # 时间
                    ts = None
                    if "published" in e:
                        try:
                            ts = dtparse.parse(e.published)
                        except Exception:
                            ts = None
                    if not ts and "updated" in e:
                        try:
                            ts = dtparse.parse(e.updated)
                        except Exception:
                            ts = None
                    if ts and ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                    if ts and ts < since:
                        continue
                    out.append({
                        "title": e.get("title", "").strip(),
                        "summary": BeautifulSoup(e.get("summary", ""), "lxml").get_text(" ", strip=True),
                        "url": e.get("link", ""),
                        "source": "GoogleNews",
                        "published_at": (ts.astimezone(TZ).isoformat() if ts else ""),
                    })
            except Exception as e:
                logerr("GoogleNews 抓取失败", e)
    return out

# ---------- 抓 聚合数据（可选） ----------
async def fetch_juhe_caijing(keywords: List[str], days: int) -> List[dict]:
    key = os.getenv("JUHE_KEY")
    if not key:
        info("JUHE_KEY 未配置，跳过聚合数据"); return []
    # 聚合接口文档：fapigx/caijing/query?k=关键词（支持多关键词 OR）
    # 我们把 10~20 个短词拼成一个管道分隔串，分批请求
    out: List[dict] = []
    since = datetime.now(TZ) - timedelta(days=days)
    async with httpx.AsyncClient(timeout=15, headers={"User-Agent": UA}) as c:
        batch = []
        for i, k in enumerate(keywords, 1):
            batch.append(k)
            if len(batch) >= 15 or i == len(keywords):
                q = "|".join(batch); batch = []
                try:
                    r = await c.get(JUHE_API, params={"key": key, "k": q, "ps": 50, "page": 1})
                    r.raise_for_status()
                    js = r.json()
                    if js.get("error_code") != 0:
                        logerr(f"聚合数据返回错误: {js.get('reason')}")
                        continue
                    data = js.get("result", []) or js.get("result", {}).get("data", [])
                    for d in (data or []):
                        title = (d.get("title") or "").strip()
                        url = (d.get("url") or "").strip()
                        src = (d.get("src") or d.get("author") or "聚合数据").strip()
                        tstr = d.get("time") or d.get("pubDate") or ""
                        try:
                            ts = dtparse.parse(tstr) if tstr else None
                        except Exception:
                            ts = None
                        if ts and ts < since:
                            continue
                        out.append({
                            "title": title,
                            "summary": (d.get("digest") or d.get("content") or "").strip(),
                            "url": url, "source": src,
                            "published_at": ts.astimezone(TZ).isoformat() if ts else "",
                        })
                except Exception as e:
                    logerr("聚合数据请求失败", e)
    return out

# ---------- 抓正文并筛选 ----------
async def fetch_body_and_filter(items: List[dict], keywords: List[str], limit: int) -> List[dict]:
    out: List[dict] = []
    sem = asyncio.Semaphore(12)

    async def worker(it: dict):
        if not it.get("url"): 
            return
        try:
            async with sem:
                async with httpx.AsyncClient(headers={"User-Agent": UA}, timeout=15, follow_redirects=True) as c:
                    r = await c.get(it["url"])
            if r.status_code != 200:
                logerr(f"正文请求非200: {r.status_code} {it['url']}")
                return
            html = decode_bytes(r.content)
            body = strip_text(html)
            if not body or len(body) < 60:
                return
            if hit_any((it.get("title", "") + " " + body), keywords):
                it2 = dict(it)
                it2["body_snippet"] = body[:800]
                out.append(it2)
        except Exception as e:
            logerr(f"抓正文失败: {it.get('url','')}", e)

    tasks = [asyncio.create_task(worker(it)) for it in items]
    if tasks:
        await asyncio.gather(*tasks)

    # 不去重，按时间降序；保留 limit
    def ts_of(d):
        try:
            return dtparse.parse(d.get("published_at") or "1970-01-01")
        except Exception:
            return datetime(1970,1,1,tzinfo=timezone.utc)
    out.sort(key=ts_of, reverse=True)
    return out[:limit]

# ---------- 主逻辑 ----------
async def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=3, help="抓取近 N 天")
    ap.add_argument("--limit", type=int, default=500, help="最多入表条数（筛选后）")
    args = ap.parse_args()

    # 清空本轮错误
    ERR_LOG.write_text("", encoding="utf-8")

    info("开始收集（近 %d 天），调用 Qwen 生成补充关键词" % args.days)

    if not HOLDINGS.exists():
        raise SystemExit("holdings.json 不存在")
    holds = read_json(HOLDINGS)
    info(f"读取持仓：holdings.json 共 {len(holds)} 条")

    etf_names, base_kw = base_keywords_from_holdings(holds)
    # LLM 生成短关键词（≤4字）：
    llm_kw = await call_qwen_short_keywords(etf_names)
    # 最终关键词：以 LLM 结果为主，去重；（不再强制加“初始关键词”）
    seen, final_kw = set(), []
    for w in llm_kw:
        if w not in seen:
            seen.add(w); final_kw.append(w)
    info(f"最终关键词 {len(final_kw)} 个，已写 keywords_used.txt / sources_used.txt")
    OUT_KW.write_text("\n".join(final_kw), encoding="utf-8")

    # 记录来源（固定两个模块）
    OUT_SRC.write_text("GoogleNews RSS\n聚合数据 fapigx/caijing\n", encoding="utf-8")

    # 1) Google News
    gn_items = await fetch_google_news(final_kw, args.days)
    info(f"GoogleNews 抓到 {len(gn_items)} 条（未筛）")

    # 2) 聚合数据（可选）
    juhe_items = await fetch_juhe_caijing(final_kw, args.days)
    info(f"聚合数据 抓到 {len(juhe_items)} 条（未筛）")

    # 合并
    raw_items = gn_items + juhe_items
    info(f"收集完成：全量 {len(raw_items)} 条（未去重）")

    # 抓正文并筛选
    kept = await fetch_body_and_filter(raw_items, final_kw, args.limit)
    info(f"正文筛选后保留 {len(kept)} 条")

    # 写 CSV
    if kept:
        with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["published_at","source","title","url","summary","body_snippet"])
            for it in kept:
                w.writerow([
                    it.get("published_at",""),
                    it.get("source",""),
                    it.get("title",""),
                    it.get("url",""),
                    (it.get("summary","") or "")[:300],
                    (it.get("body_snippet","") or "").replace("\n"," ")[:1500],
                ])
    # 写 briefing
    lines = [f"共 {len(kept)} 条（{nowstr()}）"]
    for i, it in enumerate(kept[:40], 1):
        lines.append(f"{i}. [{it.get('title','')}]({it.get('url','')}) — {it.get('source','')}  {it.get('published_at','')}")
        if it.get("summary"):
            lines.append("   " + it["summary"][:180])
    OUT_BRI.write_text("\n".join(lines), encoding="utf-8")

    # 错误大小
    err_sz = ERR_LOG.stat().st_size if ERR_LOG.exists() else 0
    info(f"已写 briefing.txt、news_all.csv、keywords_used.txt、sources_used.txt")
    info(f"errors.log 大小 {err_sz} bytes")
    info("collector 任务完成")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    except Exception as e:
        logerr("程序致命错误", e)
        raise
