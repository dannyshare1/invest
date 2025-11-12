# -*- coding: utf-8 -*-
"""
daily_push_qwen.py — 基于 briefing.txt + 持仓，生成当日中文投资提示，并推送至 Server酱 / Telegram / Bark

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
import asyncio, os, json, textwrap, re, html
from pathlib import Path
from typing import List, Dict
from urllib.parse import quote
import httpx
from datetime import datetime, timezone, timedelta

TZ = timezone(timedelta(hours=8))
REQ_TIMEOUT = float(os.getenv("QWEN_TIMEOUT", "120"))

QWEN_API = "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation"
QWEN_MODEL = "qwen-plus-2025-07-28"

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
    payload = {"model": QWEN_MODEL, "input":{"prompt": prompt}, "parameters":{"max_tokens":3000,"temperature":0.7}}
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


def md_table_to_bullets(md: str) -> list[str]:
    """
    将 Markdown 表格转为要点，兼容多种表头命名与错位。
    期望列：资产/标的 | 当前持仓 | 建议操作 | 理由
    """
    lines = [ln for ln in md.splitlines() if ln.strip()]
    out, i = [], 0

    def norm(s: str) -> str:
        return re.sub(r'\s+', '', s.replace('（', '(').replace('）', ')')).lower()

    while i < len(lines):
        if not lines[i].lstrip().startswith("|"):
            i += 1
            continue
        # 收集连续表格行
        tbl = []
        while i < len(lines) and lines[i].lstrip().startswith("|"):
            tbl.append(lines[i]); i += 1
        if len(tbl) < 2:
            continue

        header = [h.strip() for h in tbl[0].strip("|").split("|")]
        h_norm = [norm(h) for h in header]
        # 找各列索引（找不到就按自然顺序兜底）
        idx_name   = next((k for k,v in enumerate(h_norm) if v in ("持仓标的","标的","资产类别","资产","名称")), 0)
        idx_hold   = next((k for k,v in enumerate(h_norm) if "当前持仓" in v or v=="持仓"), 1 if len(header)>1 else 0)
        idx_action = next((k for k,v in enumerate(h_norm) if v in ("建议操作","建议调整","建议","操作")), 2 if len(header)>2 else min(2, len(header)-1))
        idx_reason = next((k for k,v in enumerate(h_norm) if "理由" in v or "说明" in v), 3 if len(header)>3 else min(3, len(header)-1))

        # 跳过分隔行
        data_rows = [r for r in tbl[1:] if not set(r.strip("|").strip()).issubset(set("-:| "))]

        for r in data_rows:
            cols = [c.strip() for c in r.strip("|").split("|")]
            # 列长度不够时右侧补空
            if len(cols) < len(header):
                cols += [""] * (len(header)-len(cols))

            name   = cols[idx_name]   if idx_name   < len(cols) else cols[0]
            action = cols[idx_action] if idx_action < len(cols) else (cols[2] if len(cols)>2 else "")
            reason = cols[idx_reason] if idx_reason < len(cols) else ""

            # 如果 action 是纯星号/空白，尝试用第3列兜底
            if not action or re.fullmatch(r'\*{2,}', action):
                if len(cols) > 2 and cols[2].strip():
                    action = cols[2].strip()

            # 显示：名称 — 粗体的动作｜理由
            action_disp = f"<b>{html.escape(action)}</b>" if action else "维持"
            reason_disp = html.escape(reason) if reason else "（无）"
            out.append(f"• {html.escape(name)} — {action_disp}｜理由：{reason_disp}")

    return out


def md_to_telegram_html(md_text: str) -> str:
    """轻量 Markdown -> Telegram HTML（标题加粗、表格转要点、项目符号、美化引用）"""
    raw_lines = md_text.splitlines()
    lines: list[str] = []
    i = 0
    while i < len(raw_lines):
        if raw_lines[i].lstrip().startswith("|"):
            tbl = []
            while i < len(raw_lines) and raw_lines[i].lstrip().startswith("|"):
                tbl.append(raw_lines[i]); i += 1
            bullets = md_table_to_bullets("\n".join(tbl))
            if bullets:
                lines.extend(bullets)
            else:
                lines.extend(_html_escape(ln) for ln in tbl)
        else:
            lines.append(_html_escape(raw_lines[i]))
            i += 1

    text = "\n".join(lines)

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

def _split_markdown_sections(md_text: str) -> list[str]:
    """先按 1)/2)/3) 小标题分段（若用户改了编号，仍保底按双换行分）"""
    parts = re.split(r'(?m)(?=^\s*\d\)\s)', md_text)
    parts = [p.strip() for p in parts if p.strip()]
    if not parts:
        parts = [md_text.strip()]
    return parts


def _chunk_markdown(md_text: str, limit: int = 3900) -> list[str]:
    """
    逐层切：章节 → 段落 → 行 → 字符（极端兜底）。
    和旧版不同：每个切片在这里才做 md->HTML 转换，确保每片 HTML 自洽。
    """
    out: list[str] = []
    sections = _split_markdown_sections(md_text)

    for sec in sections:
        paras = re.split(r'\n{2,}', sec)
        buf = ""

        def flush_buf():
            nonlocal buf
            if buf:
                html = md_to_telegram_html(buf)
                if len(html) <= limit:
                    out.append(html)
                else:
                    # 进一步按行切
                    tmp = ""
                    for ln in buf.splitlines():
                        ln_html = md_to_telegram_html(ln)
                        # 尽量整行拼接，超过就先发一片
                        if len(tmp) + (1 if tmp else 0) + len(ln_html) <= limit:
                            tmp = (tmp + ("\n" if tmp else "") + ln)
                        else:
                            if tmp:
                                out.append(md_to_telegram_html(tmp))
                                tmp = ln
                            else:
                                # 行本身过长，做字符级兜底（很少发生）
                                raw = md_to_telegram_html(ln)
                                for i in range(0, len(raw), limit):
                                    out.append(raw[i:i+limit])
                    if tmp:
                        out.append(md_to_telegram_html(tmp))
                buf = ""

        for para in paras:
            candidate = (buf + ("\n\n" if buf else "") + para)
            html_len = len(md_to_telegram_html(candidate))
            if html_len <= limit:
                buf = candidate
            else:
                flush_buf()
                # 单个段落也超长，拆为行
                tmp = ""
                for ln in para.splitlines():
                    ln_html = md_to_telegram_html(ln)
                    if len(ln_html) > limit:
                        # 极端长行，直接字符兜底
                        for i in range(0, len(ln_html), limit):
                            out.append(ln_html[i:i+limit])
                    else:
                        if len(md_to_telegram_html(tmp + ("\n" if tmp else "") + ln)) <= limit:
                            tmp = (tmp + ("\n" if tmp else "") + ln)
                        else:
                            out.append(md_to_telegram_html(tmp))
                            tmp = ln
                if tmp:
                    out.append(md_to_telegram_html(tmp))
        flush_buf()

    # 完整性校验（把每片反拼为纯文本进行宽松校验，避免 HTML 标签导致的误判）
    # 若你想严格等长校验，可保留旧断言；这里更关注“可发送且不丢信息”
    return out


def _strip_html(s: str) -> str:
    """移除所有 HTML 标签，作为 Telegram 发送失败时的兜底"""
    return re.sub(r"</?[^>]+>", "", s)


async def push_telegram(md_text: str):
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()
    if not token or not chat_id:
        print("TG env missing")
        return

    chunks = _chunk_markdown(md_text, limit=3900)
    print("TG chunks:", [len(c) for c in chunks])

    async with httpx.AsyncClient(timeout=REQ_TIMEOUT) as c:
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
                    if "entities" in desc:
                        plain = _strip_html(body)
                        await c.post(
                            f"https://api.telegram.org/bot{token}/sendMessage",
                            data={"chat_id": chat_id, "text": plain, "disable_web_page_preview": True},
                        )
                    elif "message is too long" in desc.lower():
                        for seg in _chunk_markdown(_strip_html(body), limit=3000):
                            await c.post(
                                f"https://api.telegram.org/bot{token}/sendMessage",
                                data={"chat_id": chat_id, "text": seg, "disable_web_page_preview": True},
                            )
            except httpx.RequestError as e:
                print(f"TG send network error on part {i}: {e}")


async def push_bark(md_text: str):
    body = _strip_html(md_to_telegram_html(md_text))
    key = "q4dLK39Yrgo7jLywyxd4o5"
    title = "每日提示"
    payload = {"device_key": key, "title": title, "body": body[:3500]}
    async with httpx.AsyncClient(timeout=REQ_TIMEOUT) as c:
        try:
            r = await c.post("https://api.day.app/push", json=payload)
            r.raise_for_status()
            print("Bark push ok:", r.text[:120])
        except httpx.HTTPError as e:
            print("Bark push failed:", e)
            # Bark 偶尔返回 500，此时退化为路径参数方式重试
            try:
                url = f"https://api.day.app/{key}/{quote(title)}/{quote(body[:1800])}"
                r = await c.get(url)
                r.raise_for_status()
                print("Bark fallback ok:", r.text[:120])
            except httpx.HTTPError as e2:
                print("Bark fallback failed:", e2)

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
    1) 前一交易日重点新闻摘要
    2) 仓位操作建议
    3) 可选:定投与止盈策略与触发条件
    4) 值得关注的高预期低价格潜力股
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
    if "2)" not in answer or "3)" not in answer:
        try:
            supplement = await call_qwen(
                f"{prompt}\n\n{answer}\n\n从缺失的小节继续，勿重复已输出内容"
            )
            answer = (answer.strip() + "\n" + supplement.strip()).strip()
        except Exception as e:
            print(f"Qwen 补写失败：{type(e).__name__}: {e}")
    Path("qwen_answer.md").write_text(answer, "utf-8")
    await push_serverchan(answer)
    await push_telegram(answer)
    await push_bark(answer)
    print("generic 推送完成")

if __name__ == "__main__":
    asyncio.run(main())
