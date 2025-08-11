# -*- coding: utf-8 -*-
"""
news_pipeline.py — RSS 收集 + 关键词过滤（中文）+ 源健康自动维护

输出文件
--------
- briefing.txt           # 仅“命中关键词”的简报（每行：日期  来源 | 标题）
- news_all.csv           # 抓到的全部 RSS 新闻（未去重、未筛），UTF-8 with BOM，便于 Excel
- keywords_used.txt      # 最终关键词（基础中文词 + Qwen 扩展去重）
- qwen_keywords.txt      # Qwen 仅扩展的中文关键词（若失败则为空）
- sources_used.txt       # 每个源抓取条数（all/hit），以及最后状态
- errors.log             # 详细错误日志（HTTP/解析/0条等）

源配置
------
- sources.yml  顶层即为 list，每项：
  - key: 唯一键
    name: 名称
    url: RSS 地址
    keep: false     # 是否“保护”，即使连续 3 次失败也不删
    consec_fail: 0  # 连续失败计数（失败含 0 条）
    last_ok:        # 上次成功时间（ISO）
    last_error:     # 上次失败原因（字符串）

行为
----
- 读取 sources.yml 并并发抓取；对每源统计“全部条数”和“命中关键词条数”
- 失败（含 0 条）则 consec_fail += 1 且写 last_error；成功则 consec_fail=0 且写 last_ok
- 若 consec_fail >= 3 且 keep != true，则从 sources.yml 中移除该源
- 关键词：从 holdings.json 生成基础中文词；若有 QWEN_API_KEY，用通义千问扩展 2~4 字中文词

依赖
----
pip install feedparser httpx pyyaml

环境变量
--------
QWEN_API_KEY  # 可选；有则扩展中文关键词
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

# ── 常量 & 路径 ────────────────────────────────────────────────────────────────
TZ = timezone(timedelta(hours=8))

SRC_FILE     = Path("sources.yml")
OUT_BRI      = Path("briefing.txt")
OUT_ALL      = Path("news_all.csv")
OUT_KW       = Path("keywords_used.txt")
OUT_QW       = Path("qwen_keywords.txt")
OUT_SRC_USED = Path("sources_used.txt")
OUT_ERR      = Path("errors.log")

QWEN_API_KEY = os.getenv("QWEN_API_KEY", "").strip()

DAYS_LOOKBACK = 3  # 抓近 3 天
REQ_TIMEOUT   = httpx.Timeout(20.0, read=30.0)
HEADERS       = {
    "User-Agent": "Mozilla/5.0 (RSSCollector; +https://github.com/)",
    "Accept": "*/*",
    "Accept-Language": "zh-CN,zh;q=0.9",
}

# ── 日志 ──────────────────────────────────────────────────────────────────────
logger = logging.getLogger("collector")
logger.setLevel(logging.INFO)
_fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
sh = logging.StreamHandler()
sh.setFormatter(_fmt)
logger.handlers.clear()
logger.addHandler(sh)

# 记录 WARNING 及以上到 errors.log（每次运行覆盖）
fh = logging.FileHandler(OUT_ERR, mode="w", encoding="utf-8")
fh.setLevel(logging.WARNING)
fh.setFormatter(_fmt)
logger.addHandler(fh)

def now_str() -> str:
    return datetime.now(TZ).strftime("%Y-%m-%dT%H:%M:%S%z")

# ── 小工具 ────────────────────────────────────────────────────────────────────
def is_chinese_word(s: str) -> bool:
    s = s.strip()
    return bool(s) and all('\u4e00' <= ch <= '\u9fff' for ch in s)

def uniq_keep_order(seq):
    seen, out = set(), []
    for x in seq:
        x = x.strip()
        if not x:
            continue
        if x not in seen:
            seen.add(x); out.append(x)
    return out

def parse_dt(entry) -> datetime | None:
    # 尽量从 feed entry 解析时间
    tm = getattr(entry, "published_parsed", None) or getattr(entry, "updated_parsed", None)
    if tm:
        try:
            return datetime(*tm[:6], tzinfo=timezone.utc).astimezone(TZ)
        except Exception:
            return None
    # 实在没有就 None
    return None

def entry_text(entry) -> Tuple[str, str, str]:
    title = getattr(entry, "title", "") or ""
    summary = getattr(entry, "summary", "") or getattr(entry, "description", "") or ""
    content = ""
    try:
        if getattr(entry, "content", None):
            # content 是列表
            content = " ".join((c.get("value") or "") for c in entry.content if isinstance(entry.content, list)) \
                      if isinstance(entry.content, list) else str(entry.content)
    except Exception:
        content = ""
    return title, summary, content

def hit_by_keywords(title: str, summary: str, content: str, kws: List[str]) -> bool:
    blob = f"{title} {summary} {content or ''}"
    return any(k in blob for k in kws)

# ── 读取/保存 sources.yml ─────────────────────────────────────────────────────
def load_sources() -> List[Dict]:
    if not SRC_FILE.is_file():
        logger.warning("sources.yml 不存在，使用空列表")
        return []
    data = yaml.safe_load(SRC_FILE.read_text("utf-8")) or []
    if isinstance(data, dict) and "sources" in data:
        items = data["sources"]
    elif isinstance(data, list):
        items = data
    else:
        items = []
    normed = []
    for it in items:
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
    # 排序：先 keep，再按 key
    items = sorted(items, key=lambda x: (not x.get("keep", False), x.get("key", "")))
    SRC_FILE.write_text(yaml.safe_dump(items, allow_unicode=True, sort_keys=False), encoding="utf-8")

# ── 关键词构建（跟之前一致，并接入 Qwen 扩展） ───────────────────────────────
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
    # 仅中文词，长度 2~6
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
async def fetch_rss_source(client: httpx.AsyncClient, src: Dict]) -> Tuple[str, List[Dict], str | None]:
    """
    返回: (key, items, error)
    error 为 None 代表请求和解析成功；但若 items==0，也视为“失败一次”（由主流程登记 0 条原因）
    """
    key, name, url = src["key"], src["name"], src["url"]
    try:
        resp = await client.get(url, headers=HEADERS, timeout=REQ_TIMEOUT)
        if resp.status_code != 200:
            msg = f"HTTP {resp.status_code}"
            logger.warning(f"{key} {msg}")
            return key, [], msg
        parsed = feedparser.parse(resp.content)
        if getattr(parsed, "bozo", False):
            # bozo_exception 可能依然有 entries；这里保守：记录警告，但仍读取 entries
            be = getattr(parsed, "bozo_exception", None)
            logger.warning(f"{key} bozo: {be}")
        items: List[Dict] = []
        cutoff = datetime.now(TZ) - timedelta(days=DAYS_LOOKBACK)
        for e in parsed.entries:
            dt = parse_dt(e)
            if dt and dt < cutoff:
                continue
            title, summary, content = entry_text(e)
            link = getattr(e, "link", "") or ""
            items.append({
                "date": (dt or datetime.now(TZ)).strftime("%Y-%m-%d %H:%M"),
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

# ── 主流程 ────────────────────────────────────────────────────────────────────
async def main():
    logger.info("开始收集（仅 RSS）")

    # 1) 读取 sources
    sources = load_sources()
    if not sources:
        logger.warning("sources.yml 为空或不存在")
    # 2) 读取持仓，构建关键词
    holds = load_holdings()
    sectors, base_kws = base_keywords_from_holdings(holds)
    logger.info(f"基础关键词 {len(base_kws)} 个；行业：{', '.join(sectors) if sectors else '-'}")

    # Qwen 扩展（失败不影响主流程）
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
        tasks = [fetch_rss_source(client, s) for s in sources]
        for coro in asyncio.as_completed(tasks):
            key, items, err = await coro
            all_items.extend(items)
            per_source_all[key] = len(items)
            last_status[key] = ("OK" if (err is None and len(items) > 0) else (err or "0 items"))
            if err is not None or len(items) == 0:
                logger.warning(f"{key} 抓到 {len(items)} 条（失败记 1 次）：{last_status[key]}")
            else:
                logger.info(f"{key} 抓到 {len(items)} 条")

    logger.info(f"收集完成：全量 {len(all_items)} 条（未去重）")

    # 4) 关键词命中（标题 + 摘要 + content）
    if final_kws:
        hit_items = [it for it in all_items if hit_by_keywords(it["title"], it["summary"], it.get("content",""), final_kws)]
    else:
        hit_items = all_items[:]  # 没关键词就全保留
    for it in hit_items:
        k = it["source_key"]
        per_source_hit[k] = per_source_hit.get(k, 0) + 1

    logger.info(f"正文/标题命中后保留 {len(hit_items)} 条（命中≥1 关键词）")

    # 5) 输出 news_all.csv（全部）、briefing.txt（命中）
    # news_all.csv：UTF-8 with BOM，防止 Excel 乱码
    with OUT_ALL.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["date","source_key","source_name","title","summary","url"])
        for it in all_items:
            w.writerow([it["date"], it["source_key"], it["source_name"], it["title"], it["summary"], it["url"]])

    # briefing.txt：仅命中；每行：YYYY-MM-DD HH:MM  来源 | 标题
    lines = []
    for it in hit_items:
        lines.append(f"{it['date']}  {it['source_name']} | {it['title']}")
    OUT_BRI.write_text("\n".join(lines), encoding="utf-8")

    # 6) sources_used.txt：每源统计
    with OUT_SRC_USED.open("w", encoding="utf-8") as f:
        for s in sources:
            k = s["key"]
            f.write(f"{k}\tall={per_source_all.get(k,0)}\thit={per_source_hit.get(k,0)}\tstatus={last_status.get(k,'-')}\n")

    # 7) 回写 sources.yml：成功则 consec_fail=0+last_ok，失败或 0 条则 consec_fail+1+last_error
    updated: List[Dict] = []
    removed_keys: List[str] = []
    for s in sources:
        k = s["key"]
        all_cnt = per_source_all.get(k, 0)
        status = last_status.get(k, "-")
        if all_cnt > 0 and status == "OK":
            s["consec_fail"] = 0
            s["last_ok"] = now_str()
            s["last_error"] = None
        else:
            s["consec_fail"] = int(s.get("consec_fail", 0)) + 1
            s["last_error"] = status
        if s["consec_fail"] >= 3 and not s.get("keep", False):
            removed_keys.append(k)
            continue
        updated.append(s)

    if removed_keys:
        logger.warning(f"连续 3 次失败移除源：{', '.join(removed_keys)}")

    save_sources(updated)

    logger.info("已写 briefing.txt、news_all.csv、keywords_used.txt、qwen_keywords.txt、sources_used.txt")
    logger.info(f"errors.log 大小 {OUT_ERR.stat().st_size if OUT_ERR.exists() else 0} bytes")
    logger.info("collector 任务完成")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
