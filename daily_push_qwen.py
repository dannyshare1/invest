# -*- coding: utf-8 -*-
"""
daily_push_qwen.py — 精简高质量 Prompt（行业动态自动从 holdings.json 推断）
读取：
- holdings.json
- briefing.txt（由 news_pipeline.py 生成）

推送：
- Telegram（可选）
- Server 酱（可选）

环境变量：
- QWEN_API_KEY
- TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
- SCKEY
"""

from __future__ import annotations
import os, json, textwrap, asyncio
from datetime import datetime
from pathlib import Path
import httpx

def load_holdings() -> list[dict]:
    p = Path("holdings.json")
    if p.is_file():
        return json.loads(p.read_text("utf-8"))
    env = os.getenv("HOLDINGS_JSON")
    return json.loads(env) if env else []

def infer_industries(holds: list[dict]) -> list[str]:
    # 极简启发式：从中文名称中提取板块词
    m = {
        "半导体": "半导体", "芯片": "半导体", "医药": "医药", "医疗": "医药", "酒": "白酒/消费", "红利": "红利/高股息",
        "沪深300": "宽基", "中证": "宽基/主题", "国债": "债券/利率", "农业": "农业/大宗", "豆粕": "农业/大宗",
    }
    out = []
    for h in holds:
        name = h.get("name","") + h.get("symbol","")
        hit = [v for k, v in m.items() if k in name]
        out.extend(hit or ["其他"])
    # 去重保序
    seen, res = set(), []
    for x in out:
        if x not in seen:
            seen.add(x); res.append(x)
    return res

async def call_qwen(prompt: str) -> str:
    api = "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation"
    key = os.getenv("QWEN_API_KEY")
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {key}"}
    payload = {"model": "qwen-plus", "input": {"prompt": prompt},
               "parameters": {"max_tokens": 800, "temperature": 0.5}}
    async with httpx.AsyncClient(timeout=60) as c:
        r = await c.post(api, headers=headers, json=payload)
        r.raise_for_status()
        return r.json()["output"]["text"].strip()

async def push_serverchan(text: str):
    key = os.getenv("SCKEY")
    if not key: return
    async with httpx.AsyncClient(timeout=20) as c:
        await c.post(f"https://sctapi.ftqq.com/{key}.send", data={"text":"每日策略", "desp":text})

async def push_telegram(text: str):
    tok=os.getenv("TELEGRAM_BOT_TOKEN"); cid=os.getenv("TELEGRAM_CHAT_ID")
    if not tok or not cid: return
    chunks=[text[i:i+3900] for i in range(0,len(text),3900)]
    async with httpx.AsyncClient(timeout=20) as c:
        for ch in chunks:
            await c.post(f"https://api.telegram.org/bot{tok}/sendMessage",
                         data={"chat_id":cid,"text":ch,"parse_mode":"Markdown"})

async def main():
    holds = load_holdings()
    print(f"{datetime.now().isoformat()} - INFO - Loaded holdings from holdings.json ({len(holds)})")

    brief = ""
    if Path("briefing.txt").is_file():
        brief = Path("briefing.txt").read_text("utf-8")
        print(f"{datetime.now().isoformat()} - INFO - Loaded briefing.txt for context")

    # 行业自动推断
    industries = infer_industries(holds)
    hold_lines = "\n".join([f"- {h.get('name','?')} ({h.get('symbol','?')}): {h.get('weight',0):.2f}" for h in holds]) or "(空)"

    prompt = textwrap.dedent(f"""
    你是一名专业中国量化策略师。基于我的当前持仓（见下）与“新闻简报”，给出**简洁、操作性强**的盘后建议。
    - 持仓行业（自动推断）：{", ".join(industries) or "(未知)"}

    【当前持仓】
    {hold_lines}

    【新闻简报/材料（可选）】
    {brief}

    请输出 Markdown，结构严格如下（不要赘述）：
    ### 今日关键行情 / 板块异动
    - 3–5 条要点，覆盖与持仓相关行业

    ### 持仓操作提示
    - 逐项给出 *维持/加/减/止盈*（≤40字/项，说明逻辑）

    ### 新的机会 / 风险
    - 2–4 条，若无则写“暂无”
    """)

    print(f"{datetime.now().isoformat()} - INFO - Prompt prepared, calling Qwen...")
    try:
        ans = await call_qwen(prompt)
    except Exception as e:
        print(f"{datetime.now().isoformat()} - ERROR - Qwen 调用失败：{e}")
        return

    await push_telegram(ans)
    await push_serverchan(ans)
    print(f"{datetime.now().isoformat()} - INFO - generic 推送完成")

if __name__ == "__main__":
    asyncio.run(main())
