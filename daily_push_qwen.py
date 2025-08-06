# -*- coding: utf-8 -*-
"""
daily_push_qwen.py — 生成每日投资建议并推送
"""
from __future__ import annotations
import asyncio, os, json, pathlib, textwrap, logging, sys, random
from datetime import datetime
import httpx

QWEN_API = "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation"
MODEL_NAME = "qwen-plus"


def load_holdings() -> list[dict]:
    env = os.getenv("HOLDINGS_JSON")
    if env:
        try:
            return json.loads(env)
        except Exception as e:
            logging.error(f'HOLDINGS_JSON 解析失败：{e}')
            return []
    fp = pathlib.Path("holdings.json")
    if fp.is_file():
        try:
            return json.loads(fp.read_text("utf-8"))
        except Exception as e:
            logging.error(f'加载 holdings.json 失败：{e}')
            return []
    return []


async def call_qwen(prompt: str) -> str:
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {os.getenv('QWEN_API_KEY')}"}
    payload = {"model": MODEL_NAME, "input": {"prompt": prompt}, "parameters": {"max_tokens": 800, "temperature": 0.7}}
    async with httpx.AsyncClient() as c:
        for i in range(3):  # 重试最多 3 次
            try:
                r = await c.post(QWEN_API, json=payload, headers=headers, timeout=60)
                r.raise_for_status()
                return r.json()["output"]["text"].strip()
            except Exception as e:
                logging.error(f'Qwen API 调用失败（第 {i+1} 次）：{e}')
                if i == 2:
                    raise
                await asyncio.sleep(2 ** i + random.uniform(0.5, 1.5))


async def push_serverchan(text: str):
    key = os.getenv('SCKEY')
    if not key:
        return
    import html
    r = await httpx.AsyncClient().post(
        f'https://sctapi.ftqq.com/{key}.send',
        data={'text': '投资建议', 'desp': html.escape(text[:8000])},
        timeout=20,
    )
    if r.status_code != 200 or r.json().get('code') != 0:
        logging.error(f'ServerChan 推送失败：{r.text}')


async def push_telegram(text: str):
    tok = os.getenv('TELEGRAM_BOT_TOKEN')
    cid = os.getenv('TELEGRAM_CHAT_ID')
    if not tok or not cid:
        return
    chunks = [text[i : i + 4000] for i in range(0, len(text), 4000)]
    async with httpx.AsyncClient() as c:
        for ch in chunks:
            await c.post(f'https://api.telegram.org/bot{tok}/sendMessage', data={'chat_id': cid, 'text': ch}, timeout=20)


async def main():
    holds = load_holdings()
    logging.info(f'Loaded {len(holds)} holdings.')
    hold_lines = "\n".join([f"- {h['name']}({h['symbol']}): {h.get('weight', 0)*100:.1f}%" for h in holds]) or "(空)"

    news_ctx = ""
    if pathlib.Path("briefing.md").is_file():
        news_ctx = "\n\n【市场新闻摘要】\n" + pathlib.Path("briefing.md").read_text("utf-8")
        logging.info('Loaded briefing.md for context')

    today = datetime.utcnow().strftime("%Y-%m-%d")
    prompt = textwrap.dedent(
        f"""
        你是一名专业中国投资策略分析师，请根据以下持仓和市场新闻为 C5 进取型投资者生成 {today} 的投资建议。
        ### 当前持仓
        {hold_lines}

        {news_ctx}

        ### 输出要求
        1. 先给出 3-5 条市场要点。
        2. 对每个持仓标的给出“维持/加仓/减仓/调仓”及 ≤50 字理由。
        3. 如有新的定投机会或风险提示，请列出。
        """
    )

    try:
        advice = await call_qwen(prompt)
    except Exception as e:
        logging.error(f"LLM 调用最终失败：{e}")
        return

    await push_serverchan(advice)
    await push_telegram(advice)
    logging.info("✅ 投资建议已推送")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.FileHandler('pipeline.log', mode='a', encoding='utf-8'), logging.StreamHandler(sys.stdout)],
    )
    asyncio.run(main())
