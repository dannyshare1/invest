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
