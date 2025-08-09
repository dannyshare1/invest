# -*- coding: utf-8 -*-
"""
news_pipeline.py — RSS 主力采集器（自更新 sources.yml 版）
- 仅使用 sources.yml 维护/自愈源清单：成功→ trusted:true, consec_fail=0；失败或0条→ consec_fail+1；
  consec_fail>=3 且非 keep:true → 直接从 sources.yml 移除
- briefing.txt：仅写 “YYYY-MM-DD [来源] 标题”
- news_all.csv：UTF-8-SIG（Excel 友好，不乱码）
"""

from __future__ import annotations
import asyncio, csv, os, re, sys
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Tuple
import httpx
import yaml
import feedparser
from dateutil import parser as dtparse

# ── sources.yml 管理（精简版） ─────────────────────────────────────────────
from pathlib import Path
import datetime as _dt
import feedparser, yaml

SRC_YML = Path("sources.yml")
FAIL_CUTOFF = 3  # 连续失败/0条 达到阈值且 keep!=true → 删除

def _now_iso():
    return _dt.datetime.now(_dt.timezone.utc).astimezone().isoformat(timespec="seconds")

def load_sources():
    """读取 sources.yml；若不存在则写一个空模板。返回 list[dict]."""
    if not SRC_YML.is_file():
        SRC_YML.write_text("[]\n", encoding="utf-8")
        return []
    data = yaml.safe_load(SRC_YML.read_text("utf-8")) or []
    # 兜底默认值 & 清理废字段
    out = []
    for s in data:
        out.append({
            "key": s.get("key"),
            "name": s.get("name"),
            "url": s.get("url"),
            "keep": bool(s.get("keep", False)),
            "consec_fail": int(s.get("consec_fail", 0)),
            "last_ok": s.get("last_ok"),
            "last_error": s.get("last_error"),
        })
    return out

def save_sources(sources: list[dict]):
    """写回 sources.yml（保留字段次序；UTF-8）。"""
    SRC_YML.write_text(
        yaml.safe_dump(sources, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )

def mark_result(sources: list[dict], key: str, ok: bool, err_msg: str | None = None):
    """根据结果更新 consec_fail / last_ok / last_error。"""
    for s in sources:
        if s["key"] == key:
            if ok:
                s["consec_fail"] = 0
                s["last_ok"] = _now_iso()
                s["last_error"] = None
            else:
                s["consec_fail"] = int(s.get("consec_fail", 0)) + 1
                s["last_error"] = (err_msg or "unknown error")[:300]
            break

def prune_sources(sources: list[dict], logger_print=print):
    """删除连续失败达到阈值的源（keep=true 除外）。返回是否有改动。"""
    before = len(sources)
    kept = []
    removed = []
    for s in sources:
        if s.get("keep", False):
            kept.append(s)
            continue
        if int(s.get("consec_fail", 0)) >= FAIL_CUTOFF:
            removed.append(s)
        else:
            kept.append(s)
    if removed:
        for r in removed:
            logger_print(f"⚠️  移除源（连续失败 ≥{FAIL_CUTOFF}）：{r['key']} {r['name']} {r.get('url')}")
    sources[:] = kept
    return len(sources) != before

async def fetch_rss_via_sources(logger_print=print) -> list[dict]:
    """
    从 sources.yml 读取 RSS 列表，逐个抓取。
    - 成功（解析正常且条目数>0）→ consec_fail=0
    - 失败 或 0条 → consec_fail+1
    - 跑完执行 prune + save，返回 items（不去重）
    item 结构：{source_key, source_name, title, link, published, summary}
    """
    sources = load_sources()
    items: list[dict] = []
    for s in sources:
        key, name, url = s["key"], s["name"], s["url"]
        try:
            fp = feedparser.parse(url)
            # feedparser 认为解析异常会设置 bozo=True
            if getattr(fp, "bozo", False):
                err = getattr(fp, "bozo_exception", None)
                mark_result(sources, key, ok=False, err_msg=f"bozo:{type(err).__name__ if err else ''}")
                logger_print(f"RSS 失败（解析）：{name} {url}")
                continue
            entries = fp.entries or []
            if not entries:
                mark_result(sources, key, ok=False, err_msg="zero entries")
                logger_print(f"RSS 0条：{name} {url}")
                continue
            mark_result(sources, key, ok=True)
            # 统一抽取字段
            for e in entries:
                items.append({
                    "source_key": key,
                    "source_name": name,
                    "title": getattr(e, "title", "").strip(),
                    "link": getattr(e, "link", ""),
                    "published": getattr(e, "published", "") or getattr(e, "updated", ""),
                    "summary": getattr(e, "summary", "") or getattr(e, "description", ""),
                })
            logger_print(f"RSS 成功：{name} 抓到 {len(entries)} 条")
        except Exception as ex:
            mark_result(sources, key, ok=False, err_msg=f"{type(ex).__name__}: {ex}")
            logger_print(f"RSS 失败（异常）：{name} {url}")
    # 清理与保存
    changed = prune_sources(sources, logger_print=logger_print)
    save_sources(sources)
    if changed:
        logger_print("已更新 sources.yml（移除不健康源或刷新状态）")
    return items


# ── 路径 / 常量 ────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent
SOURCE_FILE = Path(os.getenv("SOURCE_FILE", ROOT / "sources.yml"))
OUT_BRIEF = ROOT / "briefing.txt"
OUT_CSV = ROOT / "news_all.csv"
OUT_ERR = ROOT / "errors.log"
OUT_USED = ROOT / "sources_used.txt"

UA = "Mozilla/5.0 (compatible; invest-newsbot/1.0; +https://example.invalid)"
TZ = timezone.utc

def now() -> str:
    return datetime.now(TZ).astimezone().isoformat(timespec="seconds")

# ── I/O 辅助 ──────────────────────────────────────────────────────────────────
def load_sources() -> List[Dict]:
    if not SOURCE_FILE.is_file():
        raise FileNotFoundError(f"sources.yml 不存在：{SOURCE_FILE}")
    data = yaml.safe_load(SOURCE_FILE.read_text("utf-8")) or {}
    items: List[Dict] = data.get("sources", [])
    # 兜底字段
    for s in items:
        s.setdefault("key", re.sub(r"\W+", "_", s.get("name", "src")).lower())
        s.setdefault("enabled", True)
        s.setdefault("keep", False)
        s.setdefault("trusted", False)
        s.setdefault("consec_fail", 0)
        s.setdefault("lang", "zh")
    return items

def save_sources(sources: List[Dict]):
    # 排序：enabled 优先、keep/trusted 优先，字母序
    sources_sorted = sorted(
        sources,
        key=lambda s: (
            not s.get("enabled", True),
            not (s.get("keep") or s.get("trusted")),
            s.get("name", ""),
        ),
    )
    yaml.safe_dump(
        {"sources": sources_sorted},
        (ROOT / "sources.yml").open("w", encoding="utf-8"),
        allow_unicode=True,
        sort_keys=False,
        width=1000,
        default_flow_style=False,
    )

def log_err(msg: str):
    OUT_ERR.parent.mkdir(parents=True, exist_ok=True)
    with OUT_ERR.open("a", encoding="utf-8") as f:
        f.write(f"{now()} - {msg}\n")

# ── 解析 & 规范化 ─────────────────────────────────────────────────────────────
def as_date_str(entry: dict) -> str:
    for k in ("published", "updated", "dc_date"):
        v = entry.get(k)
        if v:
            try:
                return dtparse.parse(v).date().isoformat()
            except Exception:
                pass
    return datetime.now().date().isoformat()

def clean_text(s: str) -> str:
    if not s:
        return ""
    # 去除多余空白 & 控制字符
    s = re.sub(r"[\u0000-\u001F\u007F]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

# ── 抓取 ──────────────────────────────────────────────────────────────────────
async def fetch_rss(src: Dict, client: httpx.AsyncClient) -> Tuple[str, List[Dict], str]:
    """
    返回: (key, items, err)
      - items: [{'date','source','title'}...]
      - err: 空串表示成功；非空为错误描述
    """
    key, name, url = src["key"], src["name"], src["url"]
    try:
        r = await client.get(url, headers={"User-Agent": UA}, timeout=20, follow_redirects=True)
        r.raise_for_status()
        # feedparser 自动探测编码；用 text 以保留原始字符
        feed = feedparser.parse(r.text)
        items: List[Dict] = []
        for e in feed.entries:
            title = clean_text(e.get("title", ""))
            if not title:
                continue
            date_str = as_date_str(e)
            items.append({"date": date_str, "source": name, "title": title})
        return key, items, ""
    except Exception as e:
        return key, [], f"{name}({key}) ERR: {type(e).__name__}: {e}"

# ── 主流程 ────────────────────────────────────────────────────────────────────
async def main():
    print(f"{now()} - 开始收集（仅 RSS）")
    sources = [s for s in load_sources() if s.get("enabled", True)]
    print(f"{now()} - sources.yml 读取 {len(sources)} 条（enabled）")

    # 并发抓取
    used_count: Dict[str, int] = {}
    errors: List[str] = []
    all_items: List[Dict] = []

    async with httpx.AsyncClient() as client:
        tasks = [fetch_rss(s, client) for s in sources]
        for coro in asyncio.as_completed(tasks):
            key, items, err = await coro
            src = next((x for x in sources if x["key"] == key), None)
            if not src:
                continue
            if err:
                errors.append(err)
                src["consec_fail"] = int(src.get("consec_fail", 0)) + 1
                used_count[key] = 0
                print(f"{now()} - {src['name']} 抓取失败/0条，consec_fail={src['consec_fail']}")
            else:
                cnt = len(items)
                all_items.extend(items)
                used_count[key] = cnt
                if cnt > 0:
                    src["trusted"] = True
                    src["consec_fail"] = 0
                    print(f"{now()} - {src['name']} 抓到 {cnt} 条")
                else:
                    src["consec_fail"] = int(src.get("consec_fail", 0)) + 1
                    print(f"{now()} - {src['name']} 0 条，consec_fail={src['consec_fail']}")

    # 写 outputs
    all_items.sort(key=lambda x: (x["date"], x["source"], x["title"]), reverse=True)
    # briefing.txt：YYYY-MM-DD [来源] 标题
    with OUT_BRIEF.open("w", encoding="utf-8") as f:
        for it in all_items:
            f.write(f"{it['date']} [{it['source']}] {it['title']}\n")

    # news_all.csv：UTF-8-SIG（避免中文在 Excel 乱码）
    with OUT_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date", "source", "title"])
        for it in all_items:
            w.writerow([it["date"], it["source"], it["title"]])

    # errors.log / sources_used.txt
    OUT_ERR.write_text("", encoding="utf-8")  # 先清空
    if errors:
        for e in errors:
            log_err(e)
    with OUT_USED.open("w", encoding="utf-8") as f:
        for s in sources:
            f.write(f"{s['key']}\t{s['name']}\t{used_count.get(s['key'], 0)}\n")

    print(f"{now()} - 收集完成：共 {len(all_items)} 条；errors.log {OUT_ERR.stat().st_size if OUT_ERR.exists() else 0} bytes")

    # 自愈：连续3次失败/0条的（且非 keep/trusted）直接从 sources.yml 删除；成功过的打 trusted:true
    before = len(sources)
    pruned: List[str] = []
    survivors: List[Dict] = []
    for s in sources:
        cf = int(s.get("consec_fail", 0))
        keep = bool(s.get("keep", False))
        trusted = bool(s.get("trusted", False))
        if cf >= 3 and not keep and not trusted:
            pruned.append(f"{s['name']}({s['key']}) consec_fail={cf} → 移除")
            continue
        survivors.append(s)

    if pruned:
        for p in pruned:
            print(f"{now()} - 剔除源：{p}")

    save_sources(survivors)
    print(f"{now()} - sources.yml 已更新：{before} → {len(survivors)}（成功的已置 trusted:true；乏力源已剔除）")
    print(f"{now()} - collector 任务完成")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        log_err(f"FATAL: {type(e).__name__}: {e}")
        raise
