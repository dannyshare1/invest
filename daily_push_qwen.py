# -*- coding: utf-8 -*-
"""
daily_push_qwen.py — 基于 briefing.txt + 持仓，生成当日中文投资提示，并推送至 Server酱 / Telegram

输入：
- holdings.json      当前持仓
- briefing.txt       news_pipeline.py 产出（每行：YYYY-MM-DD HH:MM  来源 | 标题）

环境变量：
- QWEN_API_KEY
- SCKEY
- TELEGRAM_BOT_TOKEN
- TELEGRAM_CHAT_ID
"""

from __future__ import annotations
import asyncio, os, json, textwrap, re
from pathlib import Path
from typing import List, Dict
import httpx
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup


def markdown_to_html(md: str) -> str:
    """Convert Markdown text to HTML, keeping tables."""
    try:
        import markdown
    except Exception:
        return md
    return markdown.markdown(md, extensions=["tables"])


def markdown_to_text(md: str) -> str:
    """Convert Markdown text to plain text and expand tables as bullet lists."""
    html = markdown_to_html(md)
    soup = BeautifulSoup(html, "html.parser")
    for tbl in soup.find_all("table"):
        headers = [th.get_text(strip=True) for th in tbl.find_all("th")]
        lines: List[str] = []
        for row in tbl.find_all("tr"):
            cells = [td.get_text(strip=True) for td in row.find_all("td")]
            if not cells:
                continue
            if headers and len(headers) == len(cells):
                parts = [f"{h}: {c}" for h, c in zip(headers, cells)]
            else:
                parts = cells
            lines.append("- " + "; ".join(parts))
        tbl.replace_with("\n".join(lines))
    return soup.get_text("\n", strip=True)

TZ = timezone(timedelta(hours=8))
timeout = float(os.getenv("QWEN_TIMEOUT", "30"))
read_timeout = float(os.getenv("QWEN_READ_TIMEOUT", "90"))
REQ_TIMEOUT = httpx.Timeout(timeout, read=read_timeout)

QWEN_API = "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation"
QWEN_MODEL = "qwen-plus-latest"

def now_date():
    return datetime.now(TZ).strftime("%Y-%m-%d")

def load_holdings() -> List[Dict]:
    p = Path("holdings.json")
    if p.is_file():
        try:
            return json.loads(p.read_text("utf-8"))
        except Exception:
            return []
    return []

def load_briefing(max_lines: int = 120) -> str:
    p = Path("briefing.txt")
    if not p.is_file():
        return ""
    lines = [ln.strip() for ln in p.read_text("utf-8").splitlines() if ln.strip()]
    return "\n".join(lines[:max_lines])

def holdings_lines(holds: List[Dict]) -> str:
    if not holds:
        return "(空)"
    return "\n".join([f"- {h.get('name','')} ({h.get('symbol','')}): {h.get('weight',0)*100:.1f}%" for h in holds])

def infer_sectors(holds: List[Dict]) -> List[str]:
    name = " ".join((h.get("name","")+h.get("symbol","")) for h in holds)
    sec = []
    if re.search("半导体|芯片", name): sec.append("半导体")
    if re.search("医药|医疗", name): sec.append("医药")
    if re.search("酒", name): sec.append("白酒")
    if re.search("债|国债|固收", name): sec.append("债券")
    if re.search("红利|价值|蓝筹", name): sec.append("红利")
    if re.search("300|沪深|宽基", name): sec.append("宏观")
    if re.search("豆粕|农业", name): sec.append("农业")
    return sorted(set(sec))

async def call_qwen(prompt: str) -> str:
    headers = {"Content-Type":"application/json","Authorization":f"Bearer {os.getenv('QWEN_API_KEY','')}"}
    payload = {"model": QWEN_MODEL, "input":{"prompt": prompt}, "parameters":{"max_tokens":900,"temperature":0.7}}
    async with httpx.AsyncClient(timeout=REQ_TIMEOUT) as c:
        for attempt in range(3):
            try:
                r = await c.post(QWEN_API, json=payload, headers=headers)
                r.raise_for_status()
                return r.json()["output"]["text"].strip()
            except (httpx.ReadTimeout, httpx.RequestError) as e:
                if attempt == 2:
                    print(f"Qwen request failed after {attempt+1} attempts: {e}")
                    raise
                delay = 2 ** attempt
                print(f"Qwen request error (attempt {attempt+1}/3): {e}; retrying in {delay}s")
                await asyncio.sleep(delay)

async def push_serverchan(md_text: str):
    key = os.getenv("SCKEY","").strip()
    if not key:
        return
    html = markdown_to_html(md_text)
    async with httpx.AsyncClient(timeout=REQ_TIMEOUT) as c:
        try:
            r = await c.post(
                f"https://sctapi.ftqq.com/{key}.send",
                data={"text": "每日提示", "desp": html},
            )
            r.raise_for_status()
        except httpx.HTTPError as e:
            print(f"ServerChan push failed: {e}")

async def push_telegram(md_text: str):
    tok = os.getenv("TELEGRAM_BOT_TOKEN","").strip()
    cid = os.getenv("TELEGRAM_CHAT_ID","").strip()
    if not tok or not cid:
        return
    text = markdown_to_text(md_text)
    chunks = [text[i:i + 3500] for i in range(0, len(text), 3500)]
    async with httpx.AsyncClient(timeout=REQ_TIMEOUT) as c:
        for ch in chunks:
            try:
                r = await c.post(
                    f"https://api.telegram.org/bot{tok}/sendMessage",
                    data={"chat_id": cid, "text": ch},
                )
                r.raise_for_status()
            except httpx.HTTPError as e:
                print(f"Telegram push failed: {e}")

def build_prompt(holds: List[Dict], briefing: str) -> str:
    secs = ", ".join(infer_sectors(holds)) or "-"
    today = now_date()
    return textwrap.dedent(f"""
    你是一名专业中国投资策略分析师，请根据以下持仓和市场新闻为 C5 进取型投资者生成投资建议。
    日期：{today}
    行业聚焦：{secs}

    【当前持仓】
    {holdings_lines(holds)}

    【今日命中资讯（节选，无链接）】
    {briefing}

    请输出三部分：
    1) 先给出 3-5 条市场要点。
    2) 对每个持仓标的给出“维持/加仓/减仓/调仓”及 ≤50 字理由。
    3) 如有新的定投机会或风险提示，请列出。
    """).strip()

async def main():
    holds = load_holdings()
    briefing = load_briefing()
    if not holds and not briefing:
        print("No holdings and no briefing; skip push."); return
    prompt = build_prompt(holds, briefing)
    try:
        answer = await call_qwen(prompt)
    except Exception as e:
        print(f"Qwen 调用失败：{type(e).__name__}: {e}")
        return
    await push_serverchan(answer)
    await push_telegram(answer)
    print("generic 推送完成")

if __name__ == "__main__":
    asyncio.run(main())
