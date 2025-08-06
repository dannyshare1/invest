# -*- coding: utf-8 -*-
"""
holdings_tracker.py — 持仓变动监控器
用途：
1. 先运行本脚本，记录 holdings.json 与上次快照的差异。
2. 写入 holdings_log.csv，更新 holdings_snapshot.json。

示例 holdings_log.csv：
2025-08-09,UPDATE,510880.SH,中证红利ETF,0.10->0.08
"""
from __future__ import annotations
import json, csv, datetime, pathlib, sys, typing as t, logging, sys

SNAPSHOT = pathlib.Path('holdings_snapshot.json')
LOG_FILE = pathlib.Path('holdings_log.csv')
CUR_FILE = pathlib.Path('holdings.json')


def load_json(path: pathlib.Path) -> t.List[dict]:
    if not path.is_file():
        return []
    try:
        return json.loads(path.read_text('utf-8'))
    except Exception as e:
        logging.error(f'解析 {path} 失败：{e}')
        return []


def dict_by_symbol(items: t.List[dict]):
    return {i['symbol']: i for i in items}


def compare(old: t.List[dict], new: t.List[dict]):
    old_map, new_map = dict_by_symbol(old), dict_by_symbol(new)
    changes = []
    today = datetime.date.today().isoformat()
    for sym in old_map:
        if sym not in new_map:
            changes.append([today, 'REMOVE', sym, old_map[sym]['name'], f"{old_map[sym]['weight']}"])
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
    try:
        new_file = not LOG_FILE.is_file()
        with LOG_FILE.open('a', newline='', encoding='utf-8') as f:
            w = csv.writer(f)
            if new_file:
                w.writerow(['date', 'type', 'symbol', 'name', 'detail'])
            w.writerows(rows)
    except Exception as e:
        logging.error(f'写 holdings_log.csv 失败：{e}')


def main():
    if not CUR_FILE.is_file():
        logging.error('❌ holdings.json 缺失，流水线终止')
        sys.exit(1)
    curr = load_json(CUR_FILE)
    prev = load_json(SNAPSHOT)
    diff = compare(prev, curr)
    append_log(diff)
    try:
        SNAPSHOT.write_text(json.dumps(curr, ensure_ascii=False, indent=2), encoding='utf-8')
    except Exception as e:
        logging.error(f'写 holdings_snapshot.json 失败：{e}')
        sys.exit(1)
    if diff:
        logging.info(f'Holdings changed, {len(diff)} records logged.')
    else:
        logging.info('Holdings unchanged.')


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler('pipeline.log', mode='a', encoding='utf-8'),
                                  logging.StreamHandler(sys.stdout)])
    main()
