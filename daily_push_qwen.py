# -*- coding: utf-8 -*-
"""
daily_push_qwen.py — 动态行业 + 固定三段输出
输出固定包含：
1) 今日关键行情 / 板块异动
2) 每个持仓的操作提示（维持/加/减/止盈）
3) 新机会 / 新风险（可选）
"""
from __future__ import annotations
import asyncio, os, json, pathlib, textwrap
from datetime import datetime, timezone, timedelta
import httpx

TZ = timezone(timedelta(hours=8))
QWEN_API = "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation"
MODEL = "qwen-plus"

def now_str(): return datetime.now(TZ).strftime("%Y-%m-%d %H:%M")

def load_holdings():
    p = pathlib.Path("holdings.json")
    if p.is_file():
        return json.loads(p.read_text("utf-8"))
    return []

def infer_industries(holds):
    m = {
        "半导体": "半导体",
        "医药": "医药",
        "酒": "白酒",
        "国债": "债券",
        "沪深300": "宏观",
        "豆粕": "农业",
    }
    out = set()
    for h in holds:
        name = (h.get("name") or "") + (h.get("symbol") or "")
        for k, v in m.items():
            if k in name:
                out.add(v)
    if not out:
        out.add("宏观")
    return sorted(out)

async def call_llm(prompt: str) -> str:
    hdr = {
        "Authorization": f"Bearer {os.getenv('QWEN_API_KEY')}",
        "Content-Type": "application/json",
    }
    pl = {"model": MODEL, "input": {"prompt": prompt}, "parameters": {"max_tokens": 900, "temperature": 0.3}}
    async with httpx.AsyncClient() as c:
        # 简单重试
        for i in range(3):
            try:
                r = await c.post(QWEN_API, headers=hdr, json=pl, timeout=60)
                r.raise_for_status()
                return r.json()["output"]["text"].strip()
            except Exception:
                if i == 2: raise
                await asyncio.sleep(2*(i+1))

def build_prompt(holds, industries, briefing_text: str) -> str:
    holdings_lines = "\n".join([f"- {h.get('name','?')} ({h.get('symbol','?')}): {h.get('weight',0)*100:.1f}%" for h in holds]) or "(空)"
    industry_str = ", ".join(industries)
    ctx = f"\n\n【新闻上下文（命中关键词节选）】\n{briefing_text}\n" if briefing_text else ""
    return textwrap.dedent(f"""
        你是一名专业中国市场策略分析师。现在时间 {now_str()}。

        【持仓】
        {holdings_lines}

        【关注行业（动态生成）】{industry_str}
        {ctx}

        请用中文输出，且严格包含以下三部分：
        1) **今日关键行情 / 板块异动**（3-6 条，尽量与上述行业相关）
        2) **每个持仓的操作提示**（逐项给出：维持/加/减/止盈，≤40 字/项，直给理由）
        3) **新的机会或风险（可选）**（1-3 条，可含标的或主题，给出触发条件）

        约束：
        - 结论要具体；避免空话。
        - 若新闻上下文不足，请基于常识给出“需要观察的指标/数据”。
        - 不构成投资建议。
    """).strip()

async def push_serverchan(text: str):
    key = os.getenv("SCKEY")
    if not key: return
    async with httpx.AsyncClient() as c:
        await c.post(f"https://sctapi.ftqq.com/{key}.send", data={"text":"策略提示","desp":text}, timeout=20)

async def push_telegram(text: str):
    tok=os.getenv('TELEGRAM_BOT_TOKEN'); cid=os.getenv('TELEGRAM_CHAT_ID')
    if not tok or not cid: return
    chunks=[text[i:i+3900] for i in range(0,len(text),3900)]
    async with httpx.AsyncClient() as c:
        for ch in chunks:
            await c.post(f"https://api.telegram.org/bot{tok}/sendMessage",
                         data={"chat_id":cid,"text":ch,"parse_mode":"Markdown"},
                         timeout=20)

async def main():
    holds = load_holdings()
    print(f"{now_str()} - INFO - Loaded holdings ({len(holds)})")
    industries = infer_industries(holds)

    brief = ""
    if pathlib.Path("briefing.txt").is_file():
        brief = pathlib.Path("briefing.txt").read_text("utf-8")[:4000]  # 控制上下文长度
        print(f"{now_str()} - INFO - Loaded briefing.txt for context")

    prompt = build_prompt(holds, industries, brief)
    print(f"{now_str()} - INFO - Prompt prepared, calling Qwen...")
    try:
        ans = await call_llm(prompt)
    except Exception as e:
        print(f"{now_str()} - ERROR - Qwen 调用失败: {e!r}")
        return

    await push_serverchan(ans)
    await push_telegram(ans)
    print(f"{now_str()} - INFO - 推送完成")

if __name__ == "__main__":
    asyncio.run(main())
