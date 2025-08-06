# -*- coding: utf-8 -*-
"""daily_push_qwen.py — 生成每日投资建议并推送

替换旧版脚本，解决 NameError / Telegram 长度超限等问题。
关键点
———
1. **动态持仓**：与 news_pipeline.py 共用 `HOLDINGS_JSON` / holdings.json。
2. **调用通义千问 (Qwen)**：示例使用官方 ChatCompletion REST。
3. **推送**：Server 酱 & Telegram；Telegram 自动分段 ≤ 4096 字。
4. **文件依赖**：可引用 news_pipeline.py 产出的 `briefing.md` 作为市场新闻上下文。
   如果文件不存在，自动跳过。

依赖：
    pip install httpx rich
"""
from __future__ import annotations
import asyncio, os, json, pathlib, textwrap, typing as t
from datetime import datetime
import httpx
from rich import print as rprint

# ─── 配置 ───
QWEN_API = "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation"
MODEL_NAME = "qwen-plus"  # 亦可修改成你的付费模型

# ─── 数据加载 ───

def load_holdings() -> list[dict]:
    env = os.getenv("HOLDINGS_JSON")
    if env:
        return json.loads(env)
    if pathlib.Path("holdings.json").is_file():
        return json.loads(pathlib.Path("holdings.json").read_text("utf-8"))
    rprint("[yellow]⚠️  未找到持仓信息，默认为空 list")
    return []

# ─── LLM 调用 ───
async def call_qwen(prompt: str) -> str:
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {os.getenv('QWEN_API_KEY')}",
    }
    payload = {
        "model": MODEL_NAME,
        "input": {"prompt": prompt},
        "parameters": {"max_tokens": 800,
                        "temperature": 0.7},
    }
    async with httpx.AsyncClient() as c:
        r = await c.post(QWEN_API, json=payload, headers=headers, timeout=60)
        r.raise_for_status()
        return r.json()["output"]["text"].strip()

# ─── 推送 ───
async def push_serverchan(text: str):
    key=os.getenv('SCKEY');
    if not key: return
    async with httpx.AsyncClient() as c:
        await c.post(f'https://sctapi.ftqq.com/{key}.send',data={'text':'每日投资建议','desp':text},timeout=20)

async def push_telegram(text: str):
    tok=os.getenv('TELEGRAM_BOT_TOKEN'); cid=os.getenv('TELEGRAM_CHAT_ID');
    if not tok or not cid: return
    chunks=[text[i:i+4000] for i in range(0,len(text),4000)]  # 4096-安全余量
    async with httpx.AsyncClient() as c:
        for ch in chunks:
            await c.post(f'https://api.telegram.org/bot{tok}/sendMessage',
                         data={'chat_id':cid,'text':ch,'parse_mode':'Markdown'},timeout=20)

# ─── 主逻辑 ───
async def main():
    holds=load_holdings()
    holdings_lines = "\n".join([f"- {h['name']} ({h['symbol']}): {h.get('weight',0)*100:.1f}%" for h in holds]) or "(空)"

    # 新闻上下文
    news_ctx = ""
    if pathlib.Path("briefing.md").is_file():
        news_ctx = pathlib.Path("briefing.md").read_text("utf-8")
        news_ctx = "\n\n## 市场新闻摘要 (近 1 日)\n" + news_ctx

    today = datetime.utcnow().strftime("%Y-%m-%d")
    prompt=textwrap.dedent(f"""
        你是一名专业中国投资策略分析师，需要根据投资者(C5进取型)的当前持仓和市场新闻，总结今日(UTC {today})的投资建议。

        ### 当前持仓
        {holdings_lines}

        {news_ctx}

        ### 输出格式(用 Markdown)：
        1. **重点市场动态摘要**：3-5 条要点，覆盖相关行业。可引用上面新闻，但请用 own words 概括。
        2. **操作建议**：针对持仓逐项给出"维持/加仓/减仓/调仓"并说明理由(≤50字/项)。
        3. **可选**：适合定投的标的 & 近期风险提示。
    """)

    try:
        answer = await call_qwen(prompt)
    except Exception as e:
        rprint(f"[red]LLM 调用失败：{e}")
        return

    # 推送
    await push_serverchan(answer)
    await push_telegram(answer)
    rprint("[green]✅ 投资建议已推送")

if __name__ == "__main__":
    asyncio.run(main())
