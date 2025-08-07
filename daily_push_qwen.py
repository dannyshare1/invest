# -*- coding: utf-8 -*-
"""
盘中推送 v4 — 三段 Prompt
"""
from __future__ import annotations
import asyncio, os, json, pathlib, textwrap, logging, sys
from datetime import datetime, time as dtime, timezone, timedelta
import httpx

TZ=timezone(timedelta(hours=8))
API="https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation"
MODEL="qwen-plus"

PROMPTS={
    "open":textwrap.dedent("""\
        你是一名专业中国量化策略师。现在是 A 股开盘 20 分钟（09:50）。
        【当前持仓】
        {holds}
        【盘面实时数据】
        {live}
        请在 200 字内输出：
        1. 开盘异动板块 & 影响
        2. 需要立刻调整或挂单的仓位
        3. 早盘潜在机会（量价配合理想标的）
    """),
    "noon":textwrap.dedent("""\
        你是一名专业中国量化策略师。现在是上午收盘前 10 分钟（11:20）。
        【当前持仓】
        {holds}
        【盘面上午摘要】
        {live}
        请在 200 字内输出：
        1. 持仓维持/加减策略
        2. 午盘可能高换手板块
        3. 风险提示（大资金流出板块）
    """),
    "close":textwrap.dedent("""\
        你是一名专业中国量化策略师。现在是收盘后（15:05）。
        【当前持仓】
        {holds}
        【日内行情摘要】
        {live}
        请用 3-5 条总结今日行情，并输出：
        1. 每只持仓的维持/加减/止盈建议（≤50 字/项）
        2. 夜盘/隔夜需关注的宏观或外盘因素
        3. 明日开盘优先观察标的
    """)
}

def slot():
    now=datetime.now(TZ).time()
    if dtime(9,40)<=now<=dtime(10,0): return "open"
    if dtime(11,10)<=now<=dtime(11,30): return "noon"
    if dtime(15,0)<=now<=dtime(15,20): return "close"
    return "close"

def load_holdings()->str:
    fp=pathlib.Path("holdings.json")
    if not fp.is_file(): return "(无)"
    data=json.loads(fp.read_text())
    return "\n".join(f"- {h['name']}({h['symbol']})" for h in data)

def load_live()->str:
    f=pathlib.Path("briefing.md")
    return (f.read_text() if f.is_file() else "(实时行情文件缺失)")[:800]

async def llm(prompt):
    hdr={"Content-Type":"application/json","Authorization":f"Bearer {os.getenv('QWEN_API_KEY')}"}
    pl={"model":MODEL,"input":{"prompt":prompt},"parameters":{"max_tokens":800,"temperature":0.7}}
    for i in range(3):
        try:
            r=await httpx.AsyncClient().post(API,headers=hdr,json=pl,timeout=60)
            r.raise_for_status()
            return r.json()["output"]["text"].strip()
        except Exception as e:
            logging.warning(f"Qwen fail {i+1}/3 {e}")
            await asyncio.sleep(2**i)
    return "⚠️ LLM 连续失败"

async def tg_send(txt):
    tok,cid=os.getenv("TELEGRAM_BOT_TOKEN"),os.getenv("TELEGRAM_CHAT_ID")
    if not tok or not cid: return
    await httpx.AsyncClient().post(f"https://api.telegram.org/bot{tok}/sendMessage",
                                   data={'chat_id':cid,'text':txt},timeout=20)

async def sc_send(txt):
    key=os.getenv("SCKEY")
    if not key: return
    await httpx.AsyncClient().post(f"https://sctapi.ftqq.com/{key}.send",
                                   data={'text':'盘中提示','desp':txt},timeout=20)

async def main():
    tp=slot()
    prompt=PROMPTS[tp].format(holds=load_holdings(),live=load_live())
    ans=await llm(prompt)
    await tg_send(ans); await sc_send(ans)
    logging.info(f"{tp} prompt sent")

if __name__=="__main__":
    logging.basicConfig(level=logging.INFO,format="%(asctime)s - %(message)s",
                        handlers=[logging.FileHandler("pipeline.log","a"),logging.StreamHandler(sys.stdout)])
    asyncio.run(main())
