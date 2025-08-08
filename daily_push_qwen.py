# -*- coding: utf-8 -*-
"""
daily_push_qwen.py — 盘中/收盘三段式推送（动态行业 + 简洁可执行建议）
用法：
    python daily_push_qwen.py --slot open|noon|close

说明：
- 自动从 holdings.json 推断行业标签（如 半导体/医药/白酒/农业/债券/宏观/红利）。
- briefing.txt 若存在，会作为“今日新闻要点”上下文参与提示词。
- 输出格式：
  1) 今日关键行情 / 板块异动
  2) 每个持仓的操作提示（维持/加/减/止盈）
  3) 新的机会或风险（如有）

环境变量：
    QWEN_API_KEY
    TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID
    SCKEY  （可选，Server酱）
"""
from __future__ import annotations
import os, json, argparse, asyncio, httpx
from pathlib import Path
from zoneinfo import ZoneInfo
from datetime import datetime
from dateutil import tz

TZ = ZoneInfo("Asia/Shanghai")
HOLDINGS = Path("holdings.json")
BRIEFING = Path("briefing.txt")
QWEN_API = "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation"

def now_s():
    return datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")

def read_holdings():
    if HOLDINGS.is_file():
        return json.loads(HOLDINGS.read_text("utf-8"))
    return []

def infer_sectors(holds):
    tags = set()
    for h in holds:
        nm = (h.get("name") or h.get("Name") or "").strip()
        if any(x in nm for x in ("半导体", "芯片", "电子")): tags.add("半导体")
        if any(x in nm for x in ("医药", "医疗", "生物")):   tags.add("医药")
        if any(x in nm for x in ("白酒", "酒")):            tags.add("白酒")
        if any(x in nm for x in ("债", "国债", "固收")):    tags.add("债券")
        if any(x in nm for x in ("豆粕", "农业", "饲料")):   tags.add("农业")
        if "300" in nm or "沪深300" in nm:                 tags.add("宏观")
        if "红利" in nm:                                   tags.add("红利")
    return sorted(tags) or ["宏观"]

def slot_prompt(slot: str):
    if slot == "open":
        return "现在是 A 股开盘 20 分钟（09:50）"
    if slot == "noon":
        return "现在是上午收盘前 10 分钟（11:20）"
    return "现在是收盘后（15:05）"

def make_prompt(slot: str, holds, sectors, briefing_text: str):
    holds_lines = "\n".join([f"- {h.get('name','')}({h.get('symbol','')}): {float(h.get('weight',0))*100:.1f}%" for h in holds]) or "(空)"

    head = slot_prompt(slot)
    core = f"""你是一名专业中国量化策略师。{head}。

【当前持仓】
{holds_lines}

【今日新闻要点（来自采集器）】
{briefing_text or "(无)"}"""

    tail_common = """
请输出：简洁、可执行（不要长篇大论）。
"""

    if slot == "open":
        task = """生成 ≤200 字的操作提示，务必包含：
1) 今日关键行情 / 板块异动（结合所涉行业：%s）
2) 针对每个持仓的操作（维持/加/减/止盈），每项 ≤20 字
3) 若有新的机会或风险，简要列出 1-2 点""" % ("、".join(sectors))
    elif slot == "noon":
        task = """请总结上午行情，并给出：
1) 对每个持仓的维持/加/减/止盈（每项 ≤20 字）
2) 午后可能活跃的板块（2-3 个）
3) 风险提示（1-2 条）"""
    else:
        task = """请用 3-5 条总结今日行情，并输出：
1) 每只持仓的维持/加/减/止盈（每项 ≤20 字）
2) 夜盘/隔夜需关注的宏观/期指/外盘
3) 明日开盘的优先观察标的（2-3 个）"""

    return core + "\n\n" + task + tail_common

async def call_qwen(prompt: str) -> str:
    key = os.getenv("QWEN_API_KEY")
    if not key:
        return "（未配置 QWEN_API_KEY）"
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    payload = {
        "model": "qwen-plus",
        "input": {"prompt": prompt},
        "parameters": {"max_tokens": 800, "temperature": 0.4},
    }
    async with httpx.AsyncClient(timeout=60) as c:
        # 简单重试
        for i in range(3):
            try:
                r = await c.post(QWEN_API, headers=headers, json=payload)
                r.raise_for_status()
                return r.json().get("output", {}).get("text", "").strip() or "(LLM空响应)"
            except Exception:
                if i == 2:
                    raise
                await asyncio.sleep(2 * (i + 1))

async def push_serverchan(text: str):
    key = os.getenv("SCKEY")
    if not key: return
    async with httpx.AsyncClient(timeout=20) as c:
        try:
            await c.post(f"https://sctapi.ftqq.com/{key}.send",
                         data={"text": "盘中策略", "desp": text})
        except Exception:
            pass

async def push_telegram(text: str):
    tok = os.getenv("TELEGRAM_BOT_TOKEN")
    cid = os.getenv("TELEGRAM_CHAT_ID")
    if not tok or not cid: return
    chunks = [text[i:i+3500] for i in range(0, len(text), 3500)]
    async with httpx.AsyncClient(timeout=20) as c:
        for ch in chunks:
            try:
                await c.post(f"https://api.telegram.org/bot{tok}/sendMessage",
                             data={"chat_id": cid, "text": ch})
            except Exception:
                break

async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--slot", choices=["open","noon","close"], default="close")
    args = ap.parse_args()

    holds = read_holdings()
    sectors = infer_sectors(holds)
    briefing = BRIEFING.read_text("utf-8") if BRIEFING.is_file() else ""

    prompt = make_prompt(args.slot, holds, sectors, briefing)
    print(f"{now_s()} - Prompt prepared, calling Qwen...")

    try:
        ans = await call_qwen(prompt)
    except Exception as e:
        ans = f"(Qwen 失败) {e}"

    await push_telegram(ans)
    await push_serverchan(ans)
    print(f"{now_s()} - {args.slot} 推送完成")

if __name__ == "__main__":
    asyncio.run(main())
