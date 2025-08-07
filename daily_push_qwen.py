# -*- coding: utf-8 -*-
"""
盘中多时段推送
通过环境变量 PROMPT_VARIANT = morning | noon | close
"""
from __future__ import annotations
import asyncio, os, json, pathlib, textwrap, logging, sys
from datetime import datetime
import httpx

VARIANT=os.getenv('PROMPT_VARIANT','morning')   # 默认早盘
QWEN_API="https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation"
MODEL="qwen-plus"

def load_holdings():
    fp=pathlib.Path("holdings.json")
    return json.loads(fp.read_text()) if fp.is_file() else []

def load_brief():
    fp=pathlib.Path("briefing.md")
    return fp.read_text() if fp.is_file() else ""

def build_prompt(var,holds,brief):
    holds_txt="\n".join([f"- {h['name']}({h['symbol']}): {h['weight']*100:.1f}%" for h in holds]) or "(空)"
    base=f"你是一名专业中国量化策略师。当前 UTC 时间 {datetime.utcnow():%Y-%m-%d %H:%M}。\n"
    if var=='morning':
        focus="现在是 A 股开盘 20 分钟（09:50）。"
    elif var=='noon':
        focus="现在是上午收盘前 10 分钟（11:20）。"
    else:
        focus="现在是收盘后 5 分钟（15:05）。"
    template=textwrap.dedent(f"""
        {base}{focus}
        ### 当前持仓
        {holds_txt}

        ### 市场快讯
        {brief}

        请在 200 字内给出：
        1. 关键行情 / 板块异动
        2. 对现有持仓的操作提示（维持/加/减/止盈）
        3. （可选）新的机会或风险
    """)
    return template.strip()

async def llm(prompt):
    hdr={"Content-Type":"application/json","Authorization":f"Bearer {os.getenv('QWEN_API_KEY')}"}
    pl={"model":MODEL,"input":{"prompt":prompt},"parameters":{"max_tokens":800,"temperature":0.7}}
    r=await httpx.AsyncClient().post(QWEN_API,headers=hdr,json=pl,timeout=60)
    r.raise_for_status()
    return r.json()["output"]["text"].strip()

async def push(text):
    tok, cid=os.getenv('TELEGRAM_BOT_TOKEN'),os.getenv('TELEGRAM_CHAT_ID')
    if tok and cid:
        await httpx.AsyncClient().post(f"https://api.telegram.org/bot{tok}/sendMessage",
                                       data={'chat_id':cid,'text':text})
    key=os.getenv('SCKEY')
    if key:
        await httpx.AsyncClient().post(f"https://sctapi.ftqq.com/{key}.send",
                                       data={'text':'盘中提示','desp':text})

async def main():
    holds=load_holdings()
    brief=load_brief()
    prompt=build_prompt(VARIANT,holds,brief)
    ans=await llm(prompt)
    await push(ans)
    logging.info("✅ 提示已推送")

if __name__=='__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler('pipeline.log','a'),logging.StreamHandler(sys.stdout)])
    asyncio.run(main())
