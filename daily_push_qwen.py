# -*- coding: utf-8 -*-
"""
daily_push_qwen.py — 自动根据北京时间段生成 prompt
09:30-10:30 → morning
11:00-11:30 → noon
15:00-16:00 → close
"""
from __future__ import annotations
import asyncio, os, json, pathlib, textwrap, logging, sys
from datetime import datetime, timedelta, timezone
import httpx

TZ=timezone(timedelta(hours=8))     # Beijing
API="https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation"
MODEL="qwen-plus"

def period() -> str:
    now=datetime.now(TZ).time()
    if now>=datetime.strptime("05:30","%H:%M").time() and now<=datetime.strptime("10:59","%H:%M").time():
        return 'morning'
    if now>=datetime.strptime("11:00","%H:%M").time() and now<=datetime.strptime("14:59","%H:%M").time():
        return 'noon'
    return 'close'

def load(file:str): return pathlib.Path(file).read_text() if pathlib.Path(file).is_file() else ""
def load_holdings():
    fp=pathlib.Path("holdings.json")
    return json.loads(fp.read_text()) if fp.is_file() else []

def build_prompt(pt:str,holds:list,brief:str):
    holds_txt="\n".join([f"- {h['name']}({h['symbol']}): {h['weight']*100:.1f}%" for h in holds]) or "(空)"
    now=datetime.now(TZ).strftime("%Y-%m-%d %H:%M")
    focus={'morning':"现在是 A 股开盘 20 分钟（09:50）",
           'noon':"现在是上午收盘前 10 分钟（11:20）",
           'close':"现在是收盘后 5 分钟（15:05）"}[pt]
    return textwrap.dedent(f"""
        你是一名专业中国量化策略师。北京时间 {now}，{focus}。
        ### 当前持仓
        {holds_txt}

        ### 市场快讯
        {brief}

        请在 200 字内输出：
        1. 今日关键行情 / 板块异动
        2. 对每个持仓的操作提示（维持/加/减/止盈）
        3. 若有新的机会或风险，可另外列出
    """).strip()

async def llm(prompt:str):
    headers={"Content-Type":"application/json","Authorization":f"Bearer {os.getenv('QWEN_API_KEY')}"}
    payload={"model":MODEL,"input":{"prompt":prompt},"parameters":{"max_tokens":800,"temperature":0.7}}
    r=await httpx.AsyncClient().post(API,headers=headers,json=payload,timeout=60)
    r.raise_for_status(); return r.json()["output"]["text"].strip()

async def push(text:str):
    if tok:=os.getenv('TELEGRAM_BOT_TOKEN'):
        cid=os.getenv('TELEGRAM_CHAT_ID')
        await httpx.AsyncClient().post(f"https://api.telegram.org/bot{tok}/sendMessage",
                                       data={'chat_id':cid,'text':text})
    if key:=os.getenv('SCKEY'):
        await httpx.AsyncClient().post(f"https://sctapi.ftqq.com/{key}.send",
                                       data={'text':'盘中提示','desp':text})

async def main():
    pt=period()
    holds=load_holdings()
    brief=load('briefing.md')
    prompt=build_prompt(pt,holds,brief)
    ans=await llm(prompt)
    await push(ans)
    logging.info(f"✅ {pt} 推送完成")

if __name__=='__main__':
    logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(message)s',
                        handlers=[logging.FileHandler('pipeline.log','a'),logging.StreamHandler(sys.stdout)])
    asyncio.run(main())
