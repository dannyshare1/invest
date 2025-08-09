# -*- coding: utf-8 -*-
"""
news_pipeline.py — 仅 RSS 采集版（sources.yml 驱动）
功能
----
1) 从仓库根目录的 sources.yml 读取 RSS 源列表（列表结构，不需要 enabled/trusted/lang）。
2) 逐源抓取，成功/失败都会更新 sources.yml：
   - 成功(有条目)：consec_fail=0, last_ok=now, last_error=None
   - 失败/0条：consec_fail+=1, last_error=简述
   - 连续失败≥3 且 keep!=true -> 直接从 sources.yml 删除该源
3) 输出：
   - briefing.txt：每行 “YYYY-MM-DD HH:MM | 来源 | 标题”（不含 URL，不含摘要）
   - news_all.csv：UTF-8-SIG，含 date, source, title, link（CSV里保留链接，方便人工溯源）
   - errors.log：记录解析异常/HTTP问题/0条等原因
4) 纯 RSS，不依赖任何外部 News API/LLM。

依赖:
    pip install feedparser PyYAML
"""
from __future__ import annotations
import sys, csv
from pathlib import Path
from typing import List, Dict, Tuple, Optional
from datetime import datetime, timezone
import feedparser
import yaml
import time
import html
from email.utils import parsedate_to_datetime

# ── 常量 ──────────────────────────────────────────────────────────────
SRC_YML = Path("sources.yml")
OUT_CSV = Path("news_all.csv")
OUT_BRI = Path("briefing.txt")
ERR_LOG = Path("errors.log")
FAIL_CUTOFF = 3  # 连续失败/0条 达到阈值，且 keep!=true → 删除

# ── 工具函数 ──────────────────────────────────────────────────────────
def now() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")

def log_err(msg: str):
    line = f"{now()} - {msg}\n"
    ERR_LOG.parent.mkdir(parents=True, exist_ok=True)
    with ERR_LOG.open("a", encoding="utf-8") as f:
        f.write(line)
    print(line, end="")

def safe_unescape(s: str) -> str:
    if not s:
        return ""
    return html.unescape(s).strip()

def parse_rss_datetime(entry) -> Tuple[Optional[str], Optional[float]]:
    """
    返回 (iso_str, epoch)；都可能 None。
    优先 published_parsed，其次 updated_parsed，否则 None。
    """
    dt = None
    if getattr(entry, "published_parsed", None):
        dt = datetime.fromtimestamp(time.mktime(entry.published_parsed), tz=timezone.utc).astimezone()
    elif getattr(entry, "updated_parsed", None):
        dt = datetime.fromtimestamp(time.mktime(entry.updated_parsed), tz=timezone.utc).astimezone()
    else:
        # 部分源只给 RFC822 字符串（极少）
        txt = getattr(entry, "published", "") or getattr(entry, "updated", "")
        try:
            if txt:
                dt = parsedate_to_datetime(txt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                dt = dt.astimezone()
        except Exception:
            dt = None
    if not dt:
        return None, None
    return dt.isoformat(timespec="seconds"), dt.timestamp()

# ── sources.yml 读写（兼容 root 为 list 或 dict{'sources':[...] }） ───────────
def load_sources() -> Tuple[List[Dict], str]:
    """
    返回 (sources_list, root_kind)
    root_kind ∈ {'list','dict'}，保存时保持原结构。
    """
    if not SRC_YML.is_file():
        # 初次创建一个空列表
        SRC_YML.write_text("[]\n", encoding="utf-8")
        return [], "list"

    data = yaml.safe_load(SRC_YML.read_text(encoding="utf-8"))
    if isinstance(data, dict) and "sources" in data:
        raw = data.get("sources") or []
        root = "dict"
    elif isinstance(data, list):
        raw = data
        root = "list"
    else:
        raw = []
        root = "list"

    # 归一化 & 去除遗留无用字段
    out: List[Dict] = []
    for s in raw:
        if not isinstance(s, dict):
            continue
        out.append({
            "key": s.get("key"),
            "name": s.get("name"),
            "url": s.get("url"),
            "keep": bool(s.get("keep", False)),
            "consec_fail": int(s.get("consec_fail", 0)),
            "last_ok": s.get("last_ok"),
            "last_error": s.get("last_error"),
        })
    return out, root

def save_sources(sources: List[Dict], root_kind: str):
    """保持源文件的根结构(list 或 dict)写回，并保持字段顺序。"""
    payload = {"sources": sources} if root_kind == "dict" else sources
    SRC_YML.write_text(
        yaml.safe_dump(payload, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )

def mark_result(sources: List[Dict], key: str, ok: bool, err_msg: Optional[str] = None):
    for s in sources:
        if s.get("key") == key:
            if ok:
                s["consec_fail"] = 0
                s["last_ok"] = now()
                s["last_error"] = None
            else:
                s["consec_fail"] = int(s.get("consec_fail", 0)) + 1
                s["last_error"] = (err_msg or "unknown error")[:300]
            return

def prune_sources(sources: List[Dict]) -> List[Dict]:
    """不保留 keep==True 的失败源，删除 consec_fail>=FAIL_CUTOFF 的其他源。返回被删除的列表。"""
    removed: List[Dict] = []
    kept: List[Dict] = []
    for s in sources:
        if s.get("keep", False):
            kept.append(s)
            continue
        if int(s.get("consec_fail", 0)) >= FAIL_CUTOFF:
            removed.append(s)
        else:
            kept.append(s)
    if removed:
        print(f"{now()} - ⚠️ 将从 sources.yml 移除 {len(removed)} 个不健康源（连续失败≥{FAIL_CUTOFF}）")
    sources[:] = kept
    return removed

# ── RSS 抓取 ───────────────────────────────────────────────────────────
def fetch_one_rss(name: str, url: str) -> Tuple[List[Dict], Optional[str]]:
    """
    抓取单个 RSS。返回 (items, error)
    items: [{source_key, source_name, title, link, published, published_ts}]
    error: None 表示成功；否则返回错误简述（包括 0 条的场景）
    """
    try:
        fp = feedparser.parse(url, request_headers={
            "User-Agent": "Mozilla/5.0 (compatible; NewsCollector/1.0; +https://example.org)",
            "Accept": "application/rss+xml, application/atom+xml, application/xml;q=0.9, */*;q=0.8",
            "Accept-Language": "zh-CN,zh;q=0.9",
        })
        if getattr(fp, "bozo", False):
            be = getattr(fp, "bozo_exception", None)
            return [], f"bozo:{type(be).__name__ if be else ''}"
        entries = fp.entries or []
        if not entries:
            return [], "zero entries"
        items: List[Dict] = []
        for e in entries:
            iso, ts = parse_rss_datetime(e)
            items.append({
                "source_key": name,      # 展示用：直接放 name
                "source_name": name,
                "title": safe_unescape(getattr(e, "title", "")),
                "link": getattr(e, "link", "") or "",
                "published": iso or "",
                "published_ts": ts or 0.0,
            })
        return items, None
    except Exception as ex:
        return [], f"{type(ex).__name__}: {ex}"

# ── 输出写入 ───────────────────────────────────────────────────────────
def write_csv(items: List[Dict]):
    """news_all.csv 用 UTF-8-SIG 防 Excel 乱码。"""
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with OUT_CSV.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f)
        w.writerow(["date", "source", "title", "link"])
        for it in items:
            w.writerow([
                it.get("published", ""),
                it.get("source_name", ""),
                it.get("title", ""),
                it.get("link", ""),
            ])

def write_briefing(items: List[Dict]):
    """
    briefing.txt：每行 “YYYY-MM-DD HH:MM | 来源 | 标题”
    不含 URL，不含摘要，避免 LLM 误点链接。
    """
    OUT_BRI.parent.mkdir(parents=True, exist_ok=True)
    lines = []
    for it in items:
        dt = it.get("published", "")
        short = dt.replace("T", " ").split("+")[0][:16] if dt else ""
        lines.append(f"{short} | {it.get('source_name','')} | {it.get('title','')}")
    OUT_BRI.write_text("\n".join(lines), encoding="utf-8")

# ── 主流程 ─────────────────────────────────────────────────────────────
async def main():
    print(f"{now()} - 开始收集（仅 RSS）")

    sources, root_kind = load_sources()
    if not sources:
        print(f"{now()} - sources.yml 为空；请在仓库根目录添加 RSS 源。")
        # 仍写空文件，保持流程稳定
        write_csv([])
        write_briefing([])
        ERR_LOG.write_text("", encoding="utf-8")
        return

    all_items: List[Dict] = []
    errors_happened = False

    for s in sources:
        key = s.get("key") or s.get("name") or s.get("url")
        name = s.get("name") or key
        url = s.get("url")
        if not url:
            mark_result(sources, key, ok=False, err_msg="missing url")
            log_err(f"RSS 失败：{name} 缺少 URL")
            errors_happened = True
            continue

        items, err = fetch_one_rss(name=name, url=url)
        if err is None:
            mark_result(sources, key, ok=True)
            all_items.extend(items)
            print(f"{now()} - RSS 成功：{name} 抓到 {len(items)} 条")
        else:
            mark_result(sources, key, ok=False, err_msg=err)
            log_err(f"RSS 失败：{name} {url} | {err}")
            errors_happened = True

    # 裁剪不健康源 & 保存 sources.yml（保持原根结构）
    removed = prune_sources(sources)
    save_sources(sources, root_kind)
    if removed:
        for r in removed:
            print(f"{now()} - 已从 sources.yml 移除：{r.get('name') or r.get('key')} ({r.get('url')})")

    # 排序：按发布时间倒序（无时间置后）
    all_items.sort(key=lambda x: (x.get("published_ts") or 0.0), reverse=True)

    # 写输出
    write_csv(all_items)
    write_briefing(all_items)

    # 如果这轮没有任何失败/0条，清空上一轮的 errors.log；否则保留累加
    if not errors_happened:
        ERR_LOG.write_text("", encoding="utf-8")

    print(f"{now()} - 收集完成：共 {len(all_items)} 条；briefing/news_all 已写入")
    print(f"{now()} - errors.log 大小 {ERR_LOG.stat().st_size if ERR_LOG.exists() else 0} bytes")

if __name__ == "__main__":
    try:
        import asyncio
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
