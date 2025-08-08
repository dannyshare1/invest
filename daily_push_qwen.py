# -*- coding: utf-8 -*-
"""
daily_push_qwen.py — 交易时段多次推送（动态行业 + 精简高质量 Prompt）
说明
----
1) 动态行业：从 holdings.json 的名称里“猜行业”交给 LLM 归纳（不写死）。
2) 推送文本结构固定为：
   - 今日关键行情 / 板块异动（3-5条）
   - 每个持仓的操作提示（维持/加/减/止盈，≤50字/项）
   - 新机会/新风险（有则列出）
3) briefing.txt 若存在则作为上下文补充；不存在也可运行。
4) 同一个脚本，可在不同时间段用不同 mode（open/close/noon 等）控制语气。
运行
----
python daily_push_qwen.py --mode open|noon|close|generic
需要环境变量：
    QWEN_API_KEY=...
    TELEGRAM_BOT_TOKEN=...
    TELEGRAM_CHAT_ID=...
    SCKEY=...  # 可选
"""
from __future__ import annotations
import os, json, pathlib, asyncio, textwrap
from datetime import datetime, timezone, timedelta
import httpx
from rich import print as rprint

TZ = timezone(timedelta(hours=8))
HOLD = pathlib.Path("holdings.json")
BRIEF = pathlib.Path("briefing.txt")

API = "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation"
MODEL = "qwen-plus"

def nowstr():
    return datetime.now(TZ).strftime("%Y-%m-%d %H:%M")

def load_holdings():
    if HOLD.exists():
        return json.loads(HOLD.read_text("utf-8"))
    return []

def fmt_holdings(holds):
    if not holds: return "(空)"
    rows=[]
    for h in holds:
        w = h.get("weight",0)
        rows.append(f"- {h.get('name','?')} ({h.get('symbol','?')}): {w*100:.1f}%")
    return "\n".join(rows)

async def call_llm(prompt: str) -> str:
    hdr = {"Authorization": f"Bearer {os.getenv('QWEN_API_KEY')}",
           "Content-Type": "application/json"}
    pl = {"model": MODEL,
          "input": {"prompt": prompt},
          "parameters": {"max_tokens": 900, "temperature": 0.5}}
    async with httpx.AsyncClient(timeout=60) as c:
        r = await c.post(API, headers=hdr, json=pl)
        r.raise_for_status()
        return r.json()["output"]["text"].strip()

async def push_serverchan(text: str):
    key = os.getenv("SCKEY")
    if not key: return
    async with httpx.AsyncClient(timeout=15) as c:
        await c.post(f"https://sctapi.ftqq.com/{key}.send",
                     data={"text":"每日盘中提示","desp":text})

async def push_telegram(text: str):
    tok=os.getenv("TELEGRAM_BOT_TOKEN"); cid=os.getenv("TELEGRAM_CHAT_ID")
    if not tok or not cid: return
    chunks=[text[i:i+3900] for i in range(0,len(text),3900)]
    async with httpx.AsyncClient(timeout=20) as c:
        for ch in chunks:
            await c.post(f"https://api.telegram.org/bot{tok}/sendMessage",
                         data={"chat_id":cid,"text":ch,"parse_mode":"Markdown"})

def prompt_for(mode: str, holds_md: str, briefing: str) -> str:
    # 让 LLM 先“自动识别行业/主题”，再输出固定结构；语气随时段轻微调整
    timing = {
        "open":  "现在是开盘后约 20 分钟。",
        "noon":  "现在是上午收盘前 10 分钟。",
        "close": "现在是收盘后。"
    }.get(mode, "现在是交易时段。")

    return textwrap.dedent(f"""
    你是一名专业中国量化策略师。{timing}
    ### 当前持仓
    {holds_md}

    ### 可用资讯（如有为空可忽略）
    {briefing or "(无)"}

    ### 任务
    1. **今日关键行情 / 板块异动**：3–5 条（简洁、有信息量）。
    2. **持仓操作提示**：逐项给出“维持/加/减/止盈”，每项 ≤50 字。
    3. **新的机会或风险**：若有，列出 1–3 条。

    **注意**：
    - 自动根据持仓名称归纳行业/主题（不要写死）。
    - 输出用 Markdown；不要长篇大论；避免套话和虚词。
    - 所有结论要有“为什么”。
    """)

async def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", default="generic", choices=["open","noon","close","generic"])
    args = ap.parse_args()

    holds = load_holdings()
    holds_md = fmt_holdings(holds)
    brief = BRIEF.read_text("utf-8") if BRIEF.exists() else ""

    rprint(f"[cyan]{nowstr()} - Loaded holdings from holdings.json ({len(holds)})[/cyan]")
    if brief:
        rprint("[cyan]Loaded briefing.txt for context[/cyan]")
    prompt = prompt_for(args.mode, holds_md, brief)
    rprint("[cyan]Prompt prepared, calling Qwen...[/cyan]")

    txt = await call_llm(prompt)
    await push_serverchan(txt)
    await push_telegram(txt)
    rprint(f"[green]{args.mode} 推送完成[/green]")

if __name__ == "__main__":
    asyncio.run(main())
