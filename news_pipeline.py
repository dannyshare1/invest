# -*- coding: utf-8 -*-
"""
news_pipeline.py — RSS 收集 + 关键词过滤（中文）+ 源健康自动维护 + API 备源

输出文件
--------
- briefing.txt           # 仅“命中关键词”的简报（每行：日期  来源 | 标题）
- news_all.csv           # 抓到的全部新闻（未去重），UTF-8 with BOM，便于 Excel
- keywords_used.txt      # 最终关键词（基础中文词 + Qwen 扩展去重）
- qwen_keywords.txt      # Qwen 仅扩展的中文关键词（若失败则为空）
- sources_used.txt       # 每个源抓取条数（all/hit/status），RSS 源会参与健康状态回写
- errors.log             # 详细错误日志（HTTP/解析/0条等）

RSS 源配置
---------
- sources.yml  顶层即为 list，每项：
  - key: 唯一键
    name: 名称
    url: RSS 地址
    keep: false     # 是否保护；即使连续 3 次“失败/0条”也不删
    consec_fail: 0  # 连续失败计数（失败含 0 条）
    last_ok:
    last_error:

行为
----
- 并发抓取 RSS；失败（含 0 条）则 consec_fail +=1 并写 last_error；成功则 consec_fail=0 并写 last_ok
- 若 consec_fail >= 3 且 keep != true → 从 sources.yml 中移除该源
- 若设置了以下可选 API key，会在 RSS 之后追加抓取：
    NEWSAPI_KEY     → NewsAPI everything
    MEDIASTACK_KEY  → mediastack news
    JUHE_KEY        → 聚合数据财经资讯（fapigx/caijing/query）
- 关键词：从 holdings.json 生成基础中文词；如有 QWEN_API_KEY，则用通义千问扩展 2~4 字中文词

环境变量（可选）
--------------
QWEN_API_KEY
NEWSAPI_KEY
MEDIASTACK_KEY
JUHE_KEY
CHINESE_ONLY=1        # 1=尽量只抓中文（用于 API）；0=中英都要
API_MAX_PAGES=2       # 每个 API 源最多翻页数（防止额度骤增）
API_BATCH_KW=6        # 每批关键词个数（分批请求，降低 q 过长）
"""

from __future__ import annotations
import asyncio
import csv
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import feedparser
import httpx
import yaml

# ── 常量 ──────────────────────────────────────────────────────────────────────
TZ = timezone(timedelta(hours=8))
DAYS_LOOKBACK = 3
REQ_TIMEOUT   = httpx.Timeout(20.0, read=30.0)
HEADERS       = {
    "User-Agent": "Mozilla/5.0 (RSSCollector; +https://github.com/)",
    "Accept": "*/*",
    "Accept-Language": "zh-CN,zh;q=0.9",
}

# 输出文件
SRC_FILE     = Path("sources.yml")
OUT_BRI      = Path("briefing.txt")
OUT_ALL      = Path("news_all.csv")
OUT_KW       = Path("keywords_used.txt")
OUT_QW       = Path("qwen_keywords.txt")
OUT_SRC_USED = Path("sources_used.txt")
OUT_ERR      = Path("errors.log")

# 可选 API key
QWEN_API_KEY     = os.getenv("QWEN_API_KEY", "").strip()
NEWSAPI_KEY      = os.getenv("NEWSAPI_KEY", "").strip()
MEDIASTACK_KEY   = os.getenv("MEDIASTACK_KEY", "").strip()
JUHE_KEY         = os.getenv("JUHE_KEY", "").strip()
CHINESE_ONLY     = os.getenv("CHINESE_ONLY", "1").strip() == "1"
API_MAX_PAGES    = max(1, int(os.getenv("API_MAX_PAGES", "2")))
API_BATCH_KW     = max(3, int(os.getenv("API_BATCH_KW", "6")))

# ── 日志 ──────────────────────────────────────────────────────────────────────
logger = logging.getLogger("collector")
logger.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
sh = logging.StreamHandler()
sh.setFormatter(_fmt)
logger.handlers.clear()
logger.addHandler(sh)

# 记录 WARNING+ 到 errors.log（每次覆盖）
fh = logging.FileHandler(OUT_ERR, mode="w", encoding="utf-8")
fh.setLevel(logging.WARNING)
fh.setFormatter(_fmt)
logger.addHandler(fh)

def now_iso() -> str:
    return datetime.now(TZ).strftime("%Y-%m-%dT%H:%M:%S%z")

# ── 小工具 ────────────────────────────────────────────────────────────────────
def is_chinese_word(s: str) -> bool:
    s = s.strip()
    return bool(s) and all('\u4e00' <= ch <= '\u9fff' for ch in s)

def uniq_keep_order(seq):
    seen, out = set(), []
    for x in seq:
        x = x.strip()
        if not x or x in seen:
            continue
        seen.add(x); out.append(x)
    return out

def parse_dt(entry) -> datetime | None:
    tm = getattr(entry, "published_parsed", None) or getattr(entry, "updated_parsed", None)
    if tm:
        try:
            return datetime(*tm[:6], tzinfo=timezone.utc).astimezone(TZ)
        except Exception:
            return None
    return None

def entry_text(entry) -> Tuple[str, str, str]:
    title = getattr(entry, "title", "") or ""
    summary = getattr(entry, "summary", "") or getattr(entry, "description", "") or ""
    content = ""
    try:
        if getattr(entry, "content", None):
            content = (
                " ".join((c.get("value") or "") for c in entry.content if isinstance(entry.content, list))
                if isinstance(entry.content, list) else str(entry.content)
            )
    except Exception:
        content = ""
    return title, summary, content

def hit_by_keywords(title: str, summary: str, content: str, kws: List[str]) -> bool:
    blob = f"{title} {summary} {content or ''}"
    return any(k in blob for k in kws)

# ── sources.yml 读写（仅 RSS 源）─────────────────────────────────────────────
def load_sources() -> List[Dict]:
    if not SRC_FILE.is_file():
        logger.warning("sources.yml 不存在，使用空列表")
        return []
    data = yaml.safe_load(SRC_FILE.read_text("utf-8")) or []
    items = data if isinstance(data, list) else data.get("sources", [])
    normed = []
    for it in items or []:
        d = {
            "key": it.get("key"),
            "name": it.get("name", it.get("key", "")),
            "url": it.get("url", ""),
            "keep": bool(it.get("keep", False)),
            "consec_fail": int(it.get("consec_fail", 0)),
            "last_ok": it.get("last_ok"),
            "last_error": it.get("last_error"),
        }
        if d["key"] and d["url"]:
            normed.append(d)
    return normed

def save_sources(items: List[Dict]) -> None:
    items = sorted(items, key=lambda x: (not x.get("keep", False), x.get("key", "")))
    SRC_FILE.write_text(yaml.safe_dump(items, allow_unicode=True, sort_keys=False), encoding="utf-8")

# ── 关键词构建（保持与你之前一致）────────────────────────────────────────────
def load_holdings() -> List[dict]:
    p = Path("holdings.json")
    if p.is_file():
        try:
            return json.loads(p.read_text("utf-8"))
        except Exception as e:
            logger.warning(f"holdings.json 读取失败：{e}")
    return []

def base_keywords_from_holdings(holds: List[dict]) -> Tuple[List[str], List[str]]:
    sectors = set()
    words: List[str] = []
    for h in holds:
        name = (h.get("name") or "") + (h.get("symbol") or "")
        if "半导体" in name:
            sectors.add("半导体")
            words += ["半导体","芯片","晶圆","封测","光刻机","EDA","存储","GPU","HBM"]
        if "医药" in name:
            sectors.add("医药")
            words += ["医药","创新药","仿制药","集采","疫苗","器械","临床","MAH","减肥药","GLP-1"]
        if "酒" in name or "白酒" in name:
            sectors.add("白酒")
            words += ["白酒","消费","出厂价","动销","渠道"]
        if "债" in name:
            sectors.add("债券")
            words += ["国债","地方债","收益率","流动性","利率互换","期限利差"]
        if "红利" in name:
            sectors.add("红利")
            words += ["红利","分红","蓝筹","银行","煤炭","石油"]
        if "300" in name:
            sectors.add("宏观")
            words += ["宏观","PMI","通胀","出口","地产","就业","政策"]
        if "豆粕" in name:
            sectors.add("农业")
            words += ["豆粕","饲料","生猪","油脂油料","农产品"]
    words = [w for w in words if is_chinese_word(w) and 2 <= len(w) <= 6]
    return sorted(sectors), uniq_keep_order(words)

async def qwen_expand_keywords(holds: List[dict]) -> List[str]:
    if not QWEN_API_KEY:
        return []
    prompt = (
        "请根据以下 ETF 持仓名称或行业，生成 50-120 个**中文**关键词，每个以 2~4 个字为主；"
        "聚焦行业/主题/政策/产品名/热点名词，用中文逗号分隔：\n"
        + "\n".join(f"- {h.get('name','')} {h.get('symbol','')}" for h in holds)
        + "\n只输出关键词，用中文逗号分隔，不要任何解释。"
    )
    API = "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation"
    hdr = {"Content-Type":"application/json","Authorization":f"Bearer {QWEN_API_KEY}"}
    pl  = {"model":"qwen-plus","input":{"prompt":prompt},"parameters":{"max_tokens":650,"temperature":0.7}}
    try:
        async with httpx.AsyncClient(timeout=REQ_TIMEOUT) as c:
            r = await c.post(API, headers=hdr, json=pl)
            r.raise_for_status()
            text = r.json()["output"]["text"].strip()
    except Exception as e:
        logger.error(f"Qwen 调用失败: {type(e).__name__}: {e}")
        return []
    raw = re.split(r"[，,\s]+", text)
    kws = [w.strip() for w in raw if is_chinese_word(w.strip()) and 2 <= len(w.strip()) <= 6]
    OUT_QW.write_text("\n".join(uniq_keep_order(kws)) or "", encoding="utf-8")
    return uniq_keep_order(kws)

# ── RSS 抓取 ──────────────────────────────────────────────────────────────────
async def fetch_rss_source(client: httpx.AsyncClient, src: Dict) -> Tuple[str, List[Dict], str | None]:
    key, name, url = src["key"], src["name"], src["url"]
    try:
        resp = await client.get(url, headers=HEADERS, timeout=REQ_TIMEOUT)
        if resp.status_code != 200:
            msg = f"HTTP {resp.status_code}"
            logger.warning(f"{key} {msg}")
            return key, [], msg
        parsed = feedparser.parse(resp.content)
        if getattr(parsed, "bozo", False):
            be = getattr(parsed, "bozo_exception", None)
            logger.warning(f"{key} bozo: {be}")
        items: List[Dict] = []
        cutoff = datetime.now(TZ) - timedelta(days=DAYS_LOOKBACK)
        for e in parsed.entries:
            dt = parse_dt(e) or datetime.now(TZ)
            if dt < cutoff:
                continue
            title, summary, content = entry_text(e)
            link = getattr(e, "link", "") or ""
            items.append({
                "date": dt.strftime("%Y-%m-%d %H:%M"),
                "source_key": key,
                "source_name": name,
                "title": title.strip(),
                "summary": summary.strip(),
                "content": content.strip(),
                "url": link.strip(),
            })
        return key, items, None
    except Exception as e:
        msg = f"{type(e).__name__}: {e}"
        logger.error(f"{key} 抓取失败: {msg}")
        return key, [], msg

# ── API 备源（可选）───────────────────────────────────────────────────────────
def _mk_item(date_dt: datetime, source_key: str, source_name: str,
             title: str, desc: str, url: str) -> Dict:
    return {
        "date": date_dt.astimezone(TZ).strftime("%Y-%m-%d %H:%M"),
        "source_key": source_key,
        "source_name": source_name,
        "title": (title or "").strip(),
        "summary": (desc or "").strip(),
        "content": "",
        "url": (url or "").strip(),
    }

async def fetch_newsapi(kws: List[str]) -> Tuple[str, List[Dict], str | None]:
    if not NEWSAPI_KEY:
        return "newsapi", [], "no_key"
    base = "https://newsapi.org/v2/everything"
    headers = {"X-Api-Key": NEWSAPI_KEY}
    lang_list = (["zh"] if CHINESE_ONLY else ["zh","en"])
    all_items: List[Dict] = []
    try:
        async with httpx.AsyncClient(timeout=REQ_TIMEOUT) as c:
            batches = [kws[i:i+API_BATCH_KW] for i in range(0, len(kws), API_BATCH_KW)] or [[]]
            for lang in lang_list:
                for b in batches:
                    if not b: 
                        continue
                    q = " OR ".join(b)
                    for page in range(1, API_MAX_PAGES+1):
                        params = {
                            "q": q, "language": lang,
                            "pageSize": 100, "page": page,
                            "sortBy": "publishedAt",
                        }
                        r = await c.get(base, params=params, headers=headers)
                        if r.status_code != 200:
                            logger.warning(f"newsapi HTTP {r.status_code} q={q[:20]}...")
                            break
                        js = r.json()
                        arts = js.get("articles") or []
                        if not arts:
                            break
                        for a in arts:
                            dt_str = a.get("publishedAt") or ""
                            try:
                                dt = datetime.fromisoformat(dt_str.replace("Z","+00:00"))
                            except Exception:
                                dt = datetime.utcnow().replace(tzinfo=timezone.utc)
                            all_items.append(_mk_item(
                                dt, "newsapi", "NewsAPI",
                                a.get("title",""), a.get("description",""), a.get("url","")
                            ))
    except Exception as e:
        return "newsapi", [], f"{type(e).__name__}: {e}"
    return ("newsapi", all_items, None if all_items else "0 items")

async def fetch_mediastack(kws: List[str]) -> Tuple[str, List[Dict], str | None]:
    if not MEDIASTACK_KEY:
        return "mediastack", [], "no_key"
    base = "http://api.mediastack.com/v1/news"
    all_items: List[Dict] = []
    try:
        async with httpx.AsyncClient(timeout=REQ_TIMEOUT) as c:
            batches = [kws[i:i+API_BATCH_KW] for i in range(0, len(kws), API_BATCH_KW)] or [[]]
            for b in batches:
                if not b:
                    continue
                # mediastack 用 keywords，逗号分隔
                params = {
                    "access_key": MEDIASTACK_KEY,
                    "languages": "zh" if CHINESE_ONLY else "zh,en",
                    "limit": 100,
                    "sort": "published_desc",
                    "keywords": ",".join(b),
                }
                r = await c.get(base, params=params)
                if r.status_code != 200:
                    logger.warning(f"mediastack HTTP {r.status_code}")
                    continue
                js = r.json()
                data = js.get("data") or []
                for a in data:
                    dt_str = a.get("published_at") or ""
                    try:
                        dt = datetime.fromisoformat(dt_str.replace("Z","+00:00"))
                    except Exception:
                        dt = datetime.utcnow().replace(tzinfo=timezone.utc)
                    all_items.append(_mk_item(
                        dt, "mediastack", "mediastack",
                        a.get("title",""), a.get("description",""), a.get("url","")
                    ))
    except Exception as e:
        return "mediastack", [], f"{type(e).__name__}: {e}"
    return ("mediastack", all_items, None if all_items else "0 items")

async def fetch_juhe_caijing(kws: List[str]) -> Tuple[str, List[Dict], str | None]:
    if not JUHE_KEY:
        return "juhe_caijing", [], "no_key"
    base = "http://apis.juhe.cn/fapigx/caijing/query"
    all_items: List[Dict] = []
    try:
        async with httpx.AsyncClient(timeout=REQ_TIMEOUT) as c:
            batches = [kws[i:i+API_BATCH_KW] for i in range(0, len(kws), API_BATCH_KW)] or [[]]
            for b in batches:
                if not b:
                    continue
                params = {"key": JUHE_KEY, "word": " ".join(b)}
                r = await c.get(base, params=params)
                if r.status_code != 200:
                    logger.warning(f"juhe HTTP {r.status_code}")
                    continue
                js = r.json()
                result = js.get("result") or []
                for a in result:
                    dt_str = a.get("pubDate") or ""
                    try:
                        dt = datetime.fromisoformat(dt_str.replace("Z","+00:00"))
                    except Exception:
                        dt = datetime.utcnow().replace(tzinfo=timezone.utc)
                    all_items.append(_mk_item(
                        dt, "juhe_caijing", "聚合数据",
                        a.get("title",""), a.get("digest",""), a.get("url","")
                    ))
    except Exception as e:
        return "juhe_caijing", [], f"{type(e).__name__}: {e}"
    return ("juhe_caijing", all_items, None if all_items else "0 items")

# ── 主流程 ────────────────────────────────────────────────────────────────────
async def main():
    logger.info("开始收集（RSS + 可选 API 备源）")

    # 1) RSS 源
    sources_rss = load_sources()

    # 2) 关键词
    holds = load_holdings()
    sectors, base_kws = base_keywords_from_holdings(holds)
    logger.info(f"基础关键词 {len(base_kws)} 个；行业：{', '.join(sectors) if sectors else '-'}")

    extra_kws = await qwen_expand_keywords(holds) if holds else []
    final_kws = uniq_keep_order([*base_kws, *extra_kws])
    OUT_KW.write_text("\n".join(final_kws) or "", encoding="utf-8")
    if extra_kws and not OUT_QW.is_file():
        OUT_QW.write_text("\n".join(extra_kws), encoding="utf-8")

    # 3) 并发抓取 RSS
    all_items: List[Dict] = []
    per_source_all: Dict[str, int] = {}
    per_source_hit: Dict[str, int] = {}
    last_status: Dict[str, str] = {}

    async with httpx.AsyncClient(timeout=REQ_TIMEOUT) as client:
        tasks = [fetch_rss_source(client, s) for s in sources_rss]
        for coro in asyncio.as_completed(tasks):
            key, items, err = await coro
            all_items.extend(items)
            per_source_all[key] = len(items)
            last_status[key] = ("OK" if (err is None and len(items) > 0) else (err or "0 items"))
            if err is not None or len(items) == 0:
                logger.warning(f"{key} 抓到 {len(items)} 条（失败记 1 次）：{last_status[key]}")
            else:
                logger.info(f"{key} 抓到 {len(items)} 条")

    # 4) 可选 API 备源
    api_results = await asyncio.gather(
        fetch_newsapi(final_kws),
        fetch_mediastack(final_kws),
        fetch_juhe_caijing(final_kws),
    )
    for key, items, err in api_results:
        if key == "newsapi" and not NEWSAPI_KEY:  # 没 key 就别吓人写错误
            continue
        if key == "mediastack" and not MEDIASTACK_KEY:
            continue
        if key == "juhe_caijing" and not JUHE_KEY:
            continue
        all_items.extend(items)
        per_source_all[key] = per_source_all.get(key, 0) + len(items)
        last_status[key] = ("OK" if (err is None and len(items) > 0) else (err or "0 items"))
        if err is not None or len(items) == 0:
            logger.warning(f"{key} 抓到 {len(items)} 条：{last_status[key]}")
        else:
            logger.info(f"{key} 抓到 {len(items)} 条")

    logger.info(f"收集完成：全量 {len(all_items)} 条（未去重）")

    # 5) 关键词命中（标题 + 摘要 + content）
    if final_kws:
        hit_items = [it for it in all_items if hit_by_keywords(it["title"], it["summary"], it.get("content",""), final_kws)]
    else:
        hit_items = all_items[:]
    for it in hit_items:
        k = it["source_key"]
        per_source_hit[k] = per_source_hit.get(k, 0) + 1
    logger.info(f"正文/标题命中后保留 {len(hit_items)} 条（命中≥1 关键词）")

    # 6) 输出文件
    # news_all.csv：UTF-8 with BOM（防乱码）
    with OUT_ALL.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["date","source_key","source_name","title","summary","url"])
        for it in all_items:
            w.writerow([it["date"], it["source_key"], it["source_name"], it["title"], it["summary"], it["url"]])

    # briefing.txt：仅命中；每行：YYYY-MM-DD HH:MM  来源 | 标题
    OUT_BRI.write_text("\n".join(
        f"{it['date']}  {it['source_name']} | {it['title']}" for it in hit_items
    ), encoding="utf-8")

    # sources_used.txt：所有源统计（RSS + API）
    with OUT_SRC_USED.open("w", encoding="utf-8") as f:
        keys = list({**{s['key']:1 for s in sources_rss}, **{k:1 for k in per_source_all}}.keys())
        for k in keys:
            f.write(f"{k}\tall={per_source_all.get(k,0)}\thit={per_source_hit.get(k,0)}\tstatus={last_status.get(k,'-')}\n")

    # 7) 回写 sources.yml（仅 RSS 源）
    updated: List[Dict] = []
    removed: List[str] = []
    for s in sources_rss:
        k = s["key"]
        all_cnt = per_source_all.get(k, 0)
        status = last_status.get(k, "-")
        if all_cnt > 0 and status == "OK":
            s["consec_fail"] = 0
            s["last_ok"] = now_iso()
            s["last_error"] = None
        else:
            s["consec_fail"] = int(s.get("consec_fail", 0)) + 1
            s["last_error"] = status
        if s["consec_fail"] >= 3 and not s.get("keep", False):
            removed.append(k)
            continue
        updated.append(s)
    if removed:
        logger.warning(f"连续 3 次失败移除 RSS 源：{', '.join(removed)}")
    save_sources(updated)

    logger.info("已写 briefing.txt、news_all.csv、keywords_used.txt、qwen_keywords.txt、sources_used.txt")
    logger.info(f"errors.log 大小 {OUT_ERR.stat().st_size if OUT_ERR.exists() else 0} bytes")
    logger.info("collector 任务完成")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
