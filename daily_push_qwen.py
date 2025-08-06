# -*- coding: utf-8 -*-
from __future__ import annotations
import asyncio, os, json, pathlib, textwrap, typing as t
from datetime import datetime
import httpx
import sys
# from rich import print as rprint  # Removed in favor of logging
import logging

# ─── 配置 ───
QWEN_API = "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation"
MODEL_NAME = "qwen-plus"  # 亦可修改成你的付费模型

# ─── 数据加载 ───
def load_holdings() -> list[dict]:
    env = os.getenv("HOLDINGS_JSON")
    if env:
        try:
            return json.loads(env)
        except Exception as e:
            logging.error(f'Failed to parse holdings from environment: {e}')
            return []
    if pathlib.Path("holdings.json").is_file():
        try:
            data_text = pathlib.Path("holdings.json").read_text("utf-8")
            return json.loads(data_text)
        except Exception as e:
            logging.error(f'Failed to load holdings.json: {e}')
            return []
    logging.warning("⚠️ 未找到持仓信息，默认为空 list")
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
        "parameters": {"max_tokens": 800, "temperature": 0.7},
    }
    async with httpx.AsyncClient() as c:
        r = await c.post(QWEN_API, json=payload, headers=headers, timeout=60)
        r.raise_for_status()
        return r.json()["output"]["text"].strip()

# ─── 推送 ───
async def push_serverchan(text: str):
    key = os.getenv('SCKEY')
    if not key:
        return
    async with httpx.AsyncClient() as c:
        await c.post(
            f'https://sctapi.ftqq.com/{key}.send',
            data={'text': '每日投资建议', 'desp': text},
            timeout=20
        )

async def push_telegram(text: str):
    tok = os.getenv('TELEGRAM_BOT_TOKEN')
    cid = os.getenv('TELEGRAM_CHAT_ID')
    if not tok or not cid:
        return
    chunks = [text[i:i + 4000] for i in range(0, len(text), 4000)]  # 4096-安全余量
    async with httpx.AsyncClient() as c:
        for ch in chunks:
            await c.post(
                f'https://api.telegram.org/bot{tok}/sendMessage',
                data={'chat_id': cid, 'text': ch},
                timeout=20
            )

# ─── 主逻辑 ───
async def main():
    holds = load_holdings()
    logging.info(f'Loaded {len(holds)} holdings.')
    holdings_lines = "\n".join([f"- {h['name']} ({h['symbol']}): {h.get('weight', 0) * 100:.1f}%" for h in holds]) or "(空)"

    # 新闻上下文
    if pathlib.Path("briefing.md").is_file():
        news_ctx = pathlib.Path("briefing.md").read_text("utf-8")
        news_ctx = "\n\n## 市场新闻摘要 (近 1 日)\n" + news_ctx
        logging.info('Included news context from briefing.md')
    else:
        news_ctx = ""
        logging.info('No briefing.md found, proceeding without news context')

    today = datetime.utcnow().strftime("%Y-%m-%d")
    prompt = textwrap.dedent(f"""
        你是一名专业中国投资策略分析师，需要根据投资者(C5进取型)的当前持仓和市场新闻，总结今日(UTC {today})的投资建议。

        ### 当前持仓
        {holdings_lines}

        {news_ctx}

        ### 输出格式：
        1. 重点市场动态摘要：3-5 条要点，覆盖相关行业。可引用上面新闻，但请用自己的话概括。
        2. 操作建议：针对持仓逐项给出“维持/加仓/减仓/调仓”，并说明理由（不超过50字/项）。
        3. 可选：适合定投的标的 & 近期风险提示。
    """)

    logging.info('Prompt prepared, calling Qwen API...')
    try:
        answer = await call_qwen(prompt)
    except Exception as e:
        logging.error(f"LLM 调用失败: {e}")
        return

    # 推送
    try:
        await push_serverchan(answer)
    except Exception as e:
        logging.error(f'TerverChan push failed: {e}')
    try:
        await push_telegram(answer)
    except Exception as e:
        logging.error(f'Telegram push failed: {e}')
    logging.info("✅ 投资建议已推送")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler('pipeline.log', mode='a', encoding='utf-8'),
                                  logging.StreamHandler(sys.stdout)])
    asyncio.run(main())
