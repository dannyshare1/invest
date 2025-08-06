from __future__ import annotations
import json
import csv
import datetime
import pathlib
import sys
import logging
from typing import List, Dict

SNAPSHOT = pathlib.Path('holdings_snapshot.json')
LOG_FILE = pathlib.Path('holdings_log.csv')
CUR_FILE = pathlib.Path('holdings.json')

# 设置日志记录
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_json(path: pathlib.Path) -> List[Dict]:
    try:
        if not path.is_file():
            logging.warning(f"File {path} not found")
            return []
        return json.loads(path.read_text('utf-8'))
    except Exception as e:
        logging.error(f"Failed to load JSON from {path}: {e}")
        return []

def dict_by_symbol(items: List[Dict]):
    return {i['symbol']: i for i in items}

def compare(old: List[Dict], new: List[Dict]):
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

def append_log(rows: List[List[str]]):
    if not rows:
        return
    new_file = not LOG_FILE.is_file()
    try:
        with LOG_FILE.open('a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if new_file:
                writer.writerow(['date', 'type', 'symbol', 'name', 'detail'])
            writer.writerows(rows)
    except Exception as e:
        logging.error(f"Failed to append log to {LOG_FILE}: {e}")

def main():
    if not CUR_FILE.is_file():
        logging.error("holdings.json not found")
        sys.exit(1)
    curr = load_json(CUR_FILE)
    prev = load_json(SNAPSHOT)
    diff = compare(prev, curr)
    append_log(diff)
    # update snapshot
    try:
        SNAPSHOT.write_text(json.dumps(curr, ensure_ascii=False, indent=2), encoding='utf-8')
    except Exception as e:
        logging.error(f"Failed to update snapshot file {SNAPSHOT}: {e}")
        return
    if diff:
        logging.info(f"Holdings changed, {len(diff)} records logged.")
    else:
        logging.info("Holdings unchanged.")

if __name__ == '__main__':
    main()
