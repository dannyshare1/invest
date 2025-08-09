# -*- coding: utf-8 -*-
"""
migrate_sources_yml.py — 一次性迁移 sources.yml 到精简结构
保留字段：key, name, url, keep, consec_fail, last_ok, last_error
- keep = old.keep or old.trusted
- enabled == false → 丢弃此源
- 其它废字段（lang/trusted/enabled）删除
- 写入前自动创建备份：sources.yml.bak-YYYYMMDD-HHMMSS
"""
from __future__ import annotations
import sys, shutil, datetime as dt
from pathlib import Path
import yaml

SRC = Path("sources.yml")

PINNED_DEFAULT = {  # 可按需添加需要长期保留的关键源
    "sse", "szse", "csrc", "cninfo",
}

def now_tag():
    return dt.datetime.now().strftime("%Y%m%d-%H%M%S")

def main():
    if not SRC.is_file():
        print("❌ sources.yml 不存在（请把文件放到仓库根目录后再运行）")
        sys.exit(1)

    raw = yaml.safe_load(SRC.read_text("utf-8")) or []
    if not isinstance(raw, list):
        print("❌ sources.yml 内容不是列表")
        sys.exit(1)

    # 备份
    bak = SRC.with_name(f"sources.yml.bak-{now_tag()}")
    shutil.copyfile(SRC, bak)
    print(f"🗂️ 已备份为 {bak.name}")

    out = []
    dropped = []
    seen_keys = set()

    for s in raw:
        if not isinstance(s, dict):
            continue
        key = s.get("key")
        name = s.get("name")
        url  = s.get("url")
        if not key or not url:
            # 必要字段缺失，丢弃
            dropped.append((key, name, "missing key/url"))
            continue

        if key in seen_keys:
            dropped.append((key, name, "duplicate key"))
            continue
        seen_keys.add(key)

        enabled = s.get("enabled", True)
        if enabled is False:
            dropped.append((key, name, "enabled=false"))
            continue

        keep = bool(s.get("keep", False) or s.get("trusted", False) or (key in PINNED_DEFAULT))
        consec_fail = int(s.get("consec_fail", 0) or 0)

        out.append({
            "key": key,
            "name": name,
            "url": url,
            "keep": keep,
            "consec_fail": consec_fail,
            "last_ok": s.get("last_ok"),
            "last_error": s.get("last_error"),
        })

    # 写回
    SRC.write_text(yaml.safe_dump(out, allow_unicode=True, sort_keys=False), encoding="utf-8")

    print(f"✅ 迁移完成：保留 {len(out)} 个源；丢弃 {len(dropped)} 个。")
    if dropped:
        for k, n, r in dropped[:20]:
            print(f"  - 丢弃 {k or '?'} / {n or ''} ：{r}")
        if len(dropped) > 20:
            print(f"  ... 其余 {len(dropped)-20} 条省略")

if __name__ == "__main__":
    main()
