# -*- coding: utf-8 -*-
"""
daily_push_qwen.py — 动态行业 + 三段式提示（关键行情/板块异动，逐持仓操作提示，新机会/风险）
输入：holdings.json（必需），briefing.txt（可选上下文）
推送：Telegram / Server酱（都配置就都发）
"""
from __future__ import annotations
import os, json, asyncio, textwrap, pathlib, logging
from datetime import datetime
import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

QWEN_API = "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation"
MODEL = "qwen-plus"

def read_holdings() -> list[dict]:
    p = pathlib.Path("holdings.json")
    if p.is_file():
        return json.loads(p.read_text("utf-8"))
    return []

def infer_industries(holds: list[dict]) -> list[str]:
    nm = " ".join([(h.get("name") or h.get("Name") or "") for h in holds])
    tags=set()
    if any(x in nm for x in ("半导体","芯片","电子","科创")): tags.add("半导体")
    if any(x in nm for x in ("医药","医疗","生物")):         tags.add("医药")
    if any(x in nm for x in ("白酒","酒")):                  tags.add("白酒")
    if any(x in nm for x in ("债","国债","固收","利率")):     tags.add("债券")
    if any(x in nm for x in ("豆粕","农业","饲料")):          tags.add("农业")
    if "300" in nm or "沪深300" in nm:                      tags.add("宏观")
    if "红利" in nm:                                        tags.add("红利")
    return sorted(tags) or ["宏观"]

async def call_qwen(prompt: str) -> str:
    headers={"Authorization": f"Bearer {os.getenv('QWEN_API_KEY','')}",
             "Content-Type":"application/json"}
    payload={"model":MODEL,"input":{"prompt":prompt},
             "parameters":{"max_tokens":700,"temperature":0.6}}
    async with httpx.AsyncClient(timeout=60) as c:
        r = await c.post(QWEN_API, headers=headers, json=payload)
        r.raise_for_status()
        return r.json()["output"]["text"].strip()

async def push_telegram(text: str):
    tok=os.getenv('TELEGRAM_BOT_TOKEN'); cid=os.getenv('TELEGRAM_CHAT_ID')
    if not tok or not cid: return
    chunks=[text[i:i+3800] for i in range(0,len(text),3800)]
    async with httpx.AsyncClient(timeout=20) as c:
        for ch in chunks:
            await c.post(f'https://api.telegram.org/bot{tok}/sendMessage',
                         data={'chat_id':cid,'text':ch})

async def push_serverchan(text: str):
    key=os.getenv('SCKEY')
    if not key: return
    async with httpx.AsyncClient(timeout=20) as c:
        await c.post(f'https://sctapi.ftqq.com/{key}.send',
                     data={'text':'盘中提示','desp':text})

async def main():
    holds=read_holdings()
    inds=infer_industries(holds)
    holdings_lines="\n".join([f"- {h.get('name') or h.get('Name')} ({h.get('symbol') or h.get('Symbol')}): {h.get('weight',0)*100:.1f}%" for h in holds]) or "(空)"
    briefing=""
    if pathlib.Path("briefing.txt").is_file():
        briefing = pathlib.Path("briefing.txt").read_text("utf-8")

    today=datetime.now().strftime("%Y-%m-%d %H:%M")
    prompt=textwrap.dedent(f"""
    你是一名专业中国投资策略分析师，请根据以下持仓和市场新闻为 C5 进取型投资者生成投资建议。

    【当前持仓】
    {holdings_lines}

    【涉及行业】
    {", ".join(inds)}

    【近况摘录（如有）】
    {briefing}

    请按此结构输出：
    1) 先给出 3-5 条市场要点。
    2) 对每个持仓标的给出“维持/加仓/减仓/调仓”及 ≤50 字理由。
    3) 如有新的定投机会或风险提示，请列出。
    """)

    try:
        ans = await call_qwen(prompt)
    except Exception as e:
        logging.error(f"Qwen 调用失败：{e}")
        ans = f"【系统提示】Qwen 调用失败：{e}"

    # 推送两个渠道（能发哪个发哪个）
    await push_telegram(ans)
    await push_serverchan(ans)
    logging.info("generic 推送完成")

if __name__ == "__main__":
    asyncio.run(main())
