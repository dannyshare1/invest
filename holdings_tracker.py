# -*- coding: utf-8 -*-
"""
持仓监控：产生
1) holdings_log.csv   —— 只记录 ADD/UPDATE/REMOVE
2) holdings_history.csv —— 按日期累积时间序列
"""
from __future__ import annotations
import json, csv, datetime, pathlib, sys, typing as t, logging

SNAPSHOT = pathlib.Path('holdings_snapshot.json')
LOG_FILE = pathlib.Path('holdings_log.csv')
HIST_FILE = pathlib.Path('holdings_history.csv')
CUR_FILE = pathlib.Path('holdings.json')

def load_json(p: pathlib.Path)->t.List[dict]:
    if not p.is_file(): return []
    try: return json.loads(p.read_text('utf-8'))
    except Exception as e:
        logging.error(f'解析 {p} 失败: {e}'); return []

def dict_by_sym(lst): return {i['symbol']: i for i in lst}

def compare(old,new):
    o,n=dict_by_sym(old),dict_by_sym(new)
    today=datetime.date.today().isoformat()
    changes,history=[],[]
    for sym in o:
        if sym not in n:
            changes.append([today,'REMOVE',sym,o[sym]['name'],f"{o[sym]['weight']}"])
    for sym,item in n.items():
        history.append([today,sym,item['name'],item['weight']])
        if sym not in o:
            changes.append([today,'ADD',sym,item['name'],f"{item['weight']}"])
        elif abs(o[sym]['weight']-item['weight'])>1e-6:
            changes.append([today,'UPDATE',sym,item['name'],f"{o[sym]['weight']}->{item['weight']}"])
    return changes,history

def append(path,rows,header):
    if not rows: return
    new=not path.is_file()
    with path.open('a',newline='',encoding='utf-8-sig') as f:
        w=csv.writer(f)
        if new: w.writerow(header)
        w.writerows(rows)

def main():
    if not CUR_FILE.is_file():
        logging.error('❌ holdings.json 缺失'); sys.exit(1)
    curr=load_json(CUR_FILE)
    prev=load_json(SNAPSHOT)
    diff,hist=compare(prev,curr)
    append(LOG_FILE,diff,['date','type','symbol','name','detail'])
    append(HIST_FILE,hist,['date','symbol','name','weight'])
    SNAPSHOT.write_text(json.dumps(curr,ensure_ascii=False,indent=2),'utf-8')
    logging.info(f'记录变动 {len(diff)} 行；history 追加 {len(hist)} 行')

if __name__=='__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler('pipeline.log','a','utf-8'),
                                  logging.StreamHandler(sys.stdout)])
    main()
