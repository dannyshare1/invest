# -*- coding: utf-8 -*-
"""holdings_tracker.py — 持仓变动监控器

用途
----
1. **作为流水线第一步**：在 news_pipeline / daily_push 前先运行本脚本。
2. **功能**：
   - 读取 `holdings.json`（当前持仓）。
   - 与上一次快照 `holdings_snapshot.json` 对比。
   - 如有变动（新增 / 删除 / 比例变化），把差异记录到 `holdings_log.csv`，并更新快照。
   - 若无变动，则什么也不写。

依赖：仅标准库。

`holdings.json` 格式：
[
  {"symbol": "510880.SH", "name": "中证红利ETF", "weight": 0.12},
  ...
]

`holdings_log.csv` 样例：
```
2025-08-06,ADD,512480.SH,半导体ETF,0.00
2025-08-07,UPDATE,510880.SH,中证红利ETF,0.10->0.08
2025-08-09,REMOVE,159985.SH,豆粕ETF,0.00
```
"""
from __future__ import annotations
import json, csv, datetime, pathlib, sys, typing as t

SNAPSHOT = pathlib.Path('holdings_snapshot.json')
LOG_FILE = pathlib.Path('holdings_log.csv')
CUR_FILE = pathlib.Path('holdings.json')

def load_json(path: pathlib.Path) -> t.List[dict]:
    if not path.is_file():
        return []
    return json.loads(path.read_text('utf-8'))

def dict_by_symbol(items: t.List[dict]):
    return {i['symbol']: i for i in items}

def compare(old: t.List[dict], new: t.List[dict]):
    old_map, new_map = dict_by_symbol(old), dict_by_symbol(new)
    changes = []
    today = datetime.date.today().isoformat()
    # Removed
    for sym in old_map:
        if sym not in new_map:
            changes.append([today, 'REMOVE', sym, old_map[sym]['name'], f"{old_map[sym]['weight']}"])
    # Added & Updated
    for sym, item in new_map.items():
        if sym not in old_map:
            changes.append([today, 'ADD', sym, item['name'], f"{item['weight']}"])
        else:
            old_w = old_map[sym]['weight']
            if abs(old_w - item['weight']) > 1e-6:
                changes.append([today, 'UPDATE', sym, item['name'], f"{old_w}->{item['weight']}"])
    return changes

def append_log(rows: t.List[list]):
    if not rows:
        return
    new_file = not LOG_FILE.is_file()
    with LOG_FILE.open('a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if new_file:
            writer.writerow(['date', 'type', 'symbol', 'name', 'detail'])
        writer.writerows(rows)


def main():
    if not CUR_FILE.is_file():
        print('❌ holdings.json not found', file=sys.stderr)
        sys.exit(1)
    curr = load_json(CUR_FILE)
    prev = load_json(SNAPSHOT)
    diff = compare(prev, curr)
    append_log(diff)
    # update snapshot
    SNAPSHOT.write_text(json.dumps(curr, ensure_ascii=False, indent=2), encoding='utf-8')
    if diff:
        print(f'📈 Holdings changed, {len(diff)} records logged.')
    else:
        print('✅ Holdings unchanged.')

if __name__ == '__main__':
    main()
