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
- QWEN_TIMEOUT (optional, seconds)
"""

from __future__ import annotations
import asyncio, os, json, textwrap, re
from pathlib import Path
from typing import List, Dict
import httpx
from datetime import datetime, timezone, timedelta

TZ = timezone(timedelta(hours=8))
REQ_TIMEOUT = float(os.getenv("QWEN_TIMEOUT", "60"))

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
    timeout = httpx.Timeout(REQ_TIMEOUT)
    async with httpx.AsyncClient(timeout=timeout) as c:
        for attempt in range(3):
            try:
                r = await c.post(QWEN_API, json=payload, headers=headers)
                r.raise_for_status()
                return r.json()["output"]["text"].strip()
            except (httpx.ReadTimeout, httpx.RequestError) as e:
                name = type(e).__name__
                if attempt == 2:
                    print(f"Qwen request failed after {attempt+1} attempts: {name}: {e}")
                    raise
                delay = 2 ** attempt
                print(f"Qwen request error (attempt {attempt+1}/3): {name}: {e}; retrying in {delay}s")
                await asyncio.sleep(delay)

async def push_serverchan(md_text: str):
    key = os.getenv("SCKEY","").strip()
    if not key:
        return
    async with httpx.AsyncClient(timeout=REQ_TIMEOUT) as c:
        try:
            r = await c.post(
                f"https://sctapi.ftqq.com/{key}.send",
                data={
                    "title": "每日提示",   # 标题
                    "desp": md_text       # 直接传 Markdown
                },
                headers={"Content-Type": "application/x-www-form-urlencoded; charset=utf-8"},
            )
            r.raise_for_status()
        except httpx.HTTPError as e:
            print(f"ServerChan push failed: {e}")

def _html_escape(s: str) -> str:
    return (s.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;"))


def _md_tables_to_bullets(lines: list[str]) -> list[str]:
    """把 Markdown 表格块转为要点列表：每行一只标的，短理由（操作用 ** 占位避免被转义）"""
    out = []
    i = 0
    while i < len(lines):
        if lines[i].lstrip().startswith("|"):
            tbl = []
            while i < len(lines) and lines[i].lstrip().startswith("|"):
                tbl.append(lines[i]); i += 1
            if len(tbl) >= 2:
                header = [h.strip() for h in tbl[0].strip("|").split("|")]
                # 跳过对齐行
                data_rows = [r for r in tbl[1:] if not set(r.strip("|").strip()).issubset(set("-:| "))]
                for r in data_rows:
                    cols = [c.strip() for c in r.strip("|").split("|")]
                    rec = dict(zip(header, cols))
                    name = rec.get("持仓标的") or rec.get("标的") or cols[0]
                    adv  = rec.get("建议") or rec.get("操作") or ""
                    why  = rec.get("理由（≤50字）") or rec.get("理由") or ""
                    out.append(f"• {name} — **{adv}**｜理由：{why}")
            else:
                out.extend(tbl)  # 非标准表格就原样
        else:
            out.append(lines[i]); i += 1
    return out


def md_to_telegram_html(md_text: str) -> str:
    """轻量 Markdown -> Telegram HTML（标题加粗、表格转要点、项目符号、美化引用）"""
    lines = md_text.splitlines()

    # 1) 表格 -> 要点
    lines = _md_tables_to_bullets(lines)

    text = "\n".join(lines)
    text = _html_escape(text)

    # 2) 标题、粗体、列表符号
    # ### / ## / # -> <b>…</b>
    text = re.sub(r'(?m)^(#{1,6})\s*([^\n]+)$',
                  lambda m: f"<b>{m.group(2).strip()}</b>", text)
    # **bold** -> <b>bold</b>
    text = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', text)
    # 引用 > -> 竖线
    text = re.sub(r'(?m)^&gt;\s*', '│ ', text)
    # 列表 - -> •
    text = re.sub(r'(?m)^\s*-\s+', '• ', text)

    # 3) 连续空行压缩
    text = re.sub(r'\n{3,}', '\n\n', text).strip()
    return text


def split_by_sections(html: str) -> list[str]:
    """按 1)/2)/3) 这类小标题切块，天然更短更清晰"""
    # 保留分隔符：把分隔符放回到每段开头
    parts = re.split(r'(?=<b>\s*\d\)\s)', html)
    parts = [p.strip() for p in parts if p.strip()]
    return parts if parts else [html]


def split_for_telegram(html: str, limit: int = 3900) -> list[str]:
    """稳健切片：章节 > 段落 > 行 > 字符；并做完整性校验"""

    def chunk_block(block: str) -> list[str]:
        if len(block) <= limit:
            return [block]
        out = []
        # 1) 段落
        cur = ""
        for para in re.split(r'\n{2,}', block):
            if len(cur) + (2 if cur else 0) + len(para) <= limit:
                cur = (cur + ("\n\n" if cur else "") + para)
                continue
            if cur:
                out.append(cur)
                cur = ""
            # 2) 行
            buf = ""
            for ln in para.splitlines():
                if len(buf) + (1 if buf else 0) + len(ln) <= limit:
                    buf = (buf + ("\n" if buf else "") + ln)
                else:
                    if buf:
                        out.append(buf)
                        buf = ""
                    # 3) 字符兜底
                    for i in range(0, len(ln), limit):
                        seg = ln[i:i+limit]
                        if len(seg) == limit:
                            out.append(seg)
                        else:
                            buf = seg
            if buf:
                out.append(buf)
        if cur:
            out.append(cur)
        return out

    chunks = []
    for sec in split_by_sections(html):
        chunks.extend(chunk_block(sec))

    # 发送前做完整性校验（确保无丢字）
    assert "".join(chunks) == html, "split_for_telegram: 内容在切分时丢失"
    return chunks


def _strip_html(s: str) -> str:
    """移除所有 HTML 标签，作为 Telegram 发送失败时的兜底"""
    return re.sub(r"</?[^>]+>", "", s)


def safe_split_for_telegram(html: str, limit: int = 3900) -> list[str]:
    """对 split_for_telegram 增强：断言失败时退回等长切片，永不抛异常"""
    try:
        return split_for_telegram(html, limit)
    except AssertionError as e:
        print("split_for_telegram assertion failed, fallback:", e)
        return [html[i:i + limit] for i in range(0, len(html), limit)]


async def push_telegram(md_text: str):
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        print("TG env missing")
        return

    html = md_to_telegram_html(md_text)
    chunks = safe_split_for_telegram(html)  # ≤3900/条，永不抛异常
    print("TG length:", len(html), "chunks:", [len(c) for c in chunks])

    async with httpx.AsyncClient(timeout=REQ_TIMEOUT) as c:
        # 预检 token（可选，但能快速发现 token/网络问题）
        try:
            gm = await c.get(f"https://api.telegram.org/bot{token}/getMe")
            if gm.status_code != 200:
                print("getMe failed:", gm.status_code, gm.text)
        except httpx.RequestError as e:
            print("TG getMe network error:", e)
            return

        for i, body in enumerate(chunks, 1):
            try:
                r = await c.post(
                    f"https://api.telegram.org/bot{token}/sendMessage",
                    data={
                        "chat_id": chat_id,
                        "text": body,
                        "parse_mode": "HTML",
                        "disable_web_page_preview": True,
                    },
                )

                if r.status_code != 200 or not r.json().get("ok", False):
                    desc = ""
                    try:
                        desc = r.json().get("description", "")
                    except Exception:
                        desc = r.text
                    print(f"TG send {i}/{len(chunks)} failed:", desc)

                    # 典型错误自动兜底一次
                    if "entities" in desc:
                        # HTML 解析失败 -> 发送纯文本
                        plain = _strip_html(body)
                        r2 = await c.post(
                            f"https://api.telegram.org/bot{token}/sendMessage",
                            data={
                                "chat_id": chat_id,
                                "text": plain,
                                "disable_web_page_preview": True,
                            },
                        )
                        print("fallback plain:", r2.status_code, r2.text[:120])
                    elif "message is too long" in desc.lower():
                        # 极少数边界再切小点重试
                        for seg in safe_split_for_telegram(body, limit=3000):
                            await c.post(
                                f"https://api.telegram.org/bot{token}/sendMessage",
                                data={
                                    "chat_id": chat_id,
                                    "text": seg,
                                    "parse_mode": "HTML",
                                    "disable_web_page_preview": True,
                                },
                            )
            except httpx.RequestError as e:
                print(f"TG send network error on part {i}: {e}")

def build_prompt(holds: List[Dict], briefing: str) -> str:
    secs = ", ".join(infer_sectors(holds)) or "-"
    today = now_date()
    return textwrap.dedent(f"""
    Search for the prior trading day's key market news covering my current holdings相关行业(红利/高股息、半导体、蓝筹(沪深300)、债券、大消费、大宗商品等)，
    请根据以下持仓和市场新闻为 C5 进取型投资者生成专业、简洁的投资建议(维持、加仓、减仓、调仓等)。
    如当前市场适合定投，请明确标的、频率与理由;如某类资产存在阶段性高位或风险，请提示止盈或风控策略。
    日期：{today}
    行业聚焦：{secs}

    【当前持仓】
    {holdings_lines(holds)}

    【今日命中资讯（节选，无链接）】
    {briefing}

    请输出三部分(语言简洁，条理清晰)：
    1) 前一交易日重点新闻摘要。
    2) 仓位操作建议:。
    3) 可选:定投与止盈策略与触发条件
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
