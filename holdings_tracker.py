# -*- coding: utf-8 -*-
from __future__ import annotations
import json, csv, datetime, pathlib, sys, typing as t
import logging

SNAPSHOT = pathlib.Path('holdings_snapshot.json')
LOG_FILE = pathlib.Path('holdings_log.csv')
CUR_FILE = pathlib.Path('holdings.json')

def load_json(path: pathlib.Path) -> t.List[dict]:
    if not path.is_file():
        return []
    try:
        data_text = path.read_text('utf-8')
        return json.loads(data_text)
    except Exception as e:
        logging.error(f'Failed to parse JSON in {path}: {e}')
        return []

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
    try:
        new_file = not LOG_FILE.is_file()
        with LOG_FILE.open('a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if new_file:
                writer.writerow(['date', 'type', 'symbol', 'name', 'detail'])
            writer.writerows(rows)
    except Exception as e:
        logging.error(f'Failed to append to log: {e}')
        # 继续执行，不抛出异常，以免中断流水线

def main():
    if not CUR_FILE.is_file():
        logging.error('holdings.json not found')
        sys.exit(1)
    curr = load_json(CUR_FILE)
    prev = load_json(SNAPSHOT)
    diff = compare(prev, curr)
    append_log(diff)
    # update snapshot
    try:
        SNAPSHOT.write_text(json.dumps(curr, ensure_ascii=False, indent=2), encoding='utf-8')
    except Exception as e:
        logging.error(f'Failed to write snapshot: {e}')
        sys.exit(1)
    if diff:
        logging.info(f'Holdings changed, {len(diff)} records logged.')
    else:
        logging.info('Holdings unchanged.')

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler('pipeline.log', mode='w', encoding='utf-8'),
                                  logging.StreamHandler(sys.stdout)])
    main()
