# -*- coding: utf-8 -*-
"""
盘中推送 v2
• 自动识别时段
• LLM 结论 + briefing.md 各推一次
• POST 失败回退 GET
"""
from __future__ import annotations
import asyncio, os, json, pathlib, textwrap, logging, sys
from datetime import datetime, timedelta, timezone
import httpx, html

TZ=timezone(timedelta(hours=8))
API="https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation"
MODEL="qwen-plus"

def time_slot():
    now=datetime.now(TZ).time()
    if now<=datetime.strptime("10:30","%H:%M").time(): return 'morning'
    if now<=datetime.strptime("11:30","%H:%M").time(): return 'noon'
    return 'close'

def load_holdings():
    fp=pathlib.Path("holdings.json")
    return json.loads(fp.read_text()) if fp.is_file() else []

def load_brief(): return pathlib.Path("briefing.md").read_text() if pathlib.Path("briefing.md").is_file() else ""

def make_prompt(slot:str,holds,brief:str):
    ht="\n".join([f"- {h['name']}({h['symbol']}): {h['weight']*100:.1f}%" for h in holds]) or "(空)"
    focus={'morning':'开盘 20 分钟','noon':'上午收盘前 10 分钟','close':'收盘后 5 分钟'}[slot]
    now=datetime.now(TZ).strftime("%Y-%m-%d %H:%M")
    return textwrap.dedent(f"""
        你是一名专业中国量化策略师。北京时间 {now}，{focus}。
        ### 当前持仓
        {ht}

        ### 市场快讯
        {brief}

        请在 200 字内输出：
        1. 今日关键行情 / 板块异动
        2. 对每个持仓的操作提示（维持/加/减/止盈）
        3. 若有新的机会或风险，可另外列出
    """).strip()

# 只展示改动块，其余保持 v2 代码
async def call_llm(prompt:str):
    hdr={"Content-Type":"application/json","Authorization":f"Bearer {os.getenv('QWEN_API_KEY')}"}
    pl = {"model":MODEL,"input":{"prompt":prompt},"parameters":{"max_tokens":800,"temperature":0.7}}
    for i in range(3):      # 最多 3 次退避
        try:
            r=await httpx.AsyncClient().post(API,headers=hdr,json=pl,timeout=60)
            r.raise_for_status()
            return r.json()["output"]["text"].strip()
        except Exception as e:
            logging.warning(f"LLM 调用失败({i+1}/3): {e}")
            await asyncio.sleep(2**i)
    raise RuntimeError("LLM 调用失败已重试 3 次")

async def tg_send(text):
    tok,cid=os.getenv('TELEGRAM_BOT_TOKEN'),os.getenv('TELEGRAM_CHAT_ID')
    if not tok or not cid: return
    try:
        await httpx.AsyncClient().post(f"https://api.telegram.org/bot{tok}/sendMessage",
                                       data={'chat_id':cid,'text':text},timeout=20)
    except Exception:
        await httpx.AsyncClient().get(f"https://api.telegram.org/bot{tok}/sendMessage",
                                      params={'chat_id':cid,'text':text})

async def sc_send(text):
    key=os.getenv('SCKEY')
    if not key: return
    data={'text':'盘中提示','desp':html.escape(text)}
    try:
        await httpx.AsyncClient().post(f"https://sctapi.ftqq.com/{key}.send",data=data,timeout=20)
    except Exception:
        await httpx.AsyncClient().get(f"https://sctapi.ftqq.com/{key}.send",params=data)

async def main():
    slot=time_slot()
    holds=load_holdings(); brief=load_brief()
    prompt=make_prompt(slot,holds,brief)
    ans=await call_llm(prompt)
    # 先推 LLM 结论
    await tg_send(ans); await sc_send(ans)
    # 再推 briefing
    if brief:
        await tg_send(brief); await sc_send(brief)
    logging.info(f"✅ {slot} 推送完成")

if __name__=='__main__':
    logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(message)s',
                        handlers=[logging.FileHandler('pipeline.log','a'),logging.StreamHandler(sys.stdout)])
    asyncio.run(main())
