# -*- coding: utf-8 -*-
"""
daily_push_qwen.py — 精简高质量 Prompt + 纯文本推送

功能
  1) 读取 holdings.json + briefing.txt；若 briefing.txt 不存在则提示“材料不足”；
  2) 以结构化“精简高质量 Prompt”调用 Qwen，输出纯文本点评；
  3) 推送 Server 酱与 Telegram（纯文本，不设置 Markdown parse_mode）；
  4) 详细日志 pipeline.log，错误 errors.log。

依赖： httpx
"""
from __future__ import annotations

import asyncio, os, json, pathlib, logging, sys, textwrap
from datetime import datetime, timezone, timedelta
import httpx

TZ = timezone(timedelta(hours=8))
QWEN_API = "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation"
MODEL = os.getenv("QWEN_MODEL", "qwen-plus")

# 日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("pipeline.log", mode="a", encoding="utf-8"),
              logging.StreamHandler(sys.stdout)],
)
ERR = pathlib.Path("errors.log"); ERR.touch(exist_ok=True)
def logerr(msg:str):
    logging.error(msg)
    ERR.write_text(ERR.read_text("utf-8")+f"{datetime.now(TZ).isoformat()}  {msg}\n", encoding="utf-8")

# 加载持仓
def load_holdings() -> list[dict]:
    for p in ("holdings.json","holding.json","holding.jason"):
        fp=pathlib.Path(p)
        if fp.is_file():
            try:
                data=json.loads(fp.read_text("utf-8"))
                logging.info(f"Loaded holdings from {p} ({len(data)})")
                return data
            except Exception as e:
                logerr(f"读持仓失败 {p}: {type(e).__name__}: {e}")
    logging.warning("未找到 holdings.json")
    return []

def holdings_text(holds:list[dict])->str:
    if not holds: return "(空)"
    lines=[]
    for h in holds:
        nm=h.get("name",""); sym=h.get("symbol",""); wt=h.get("weight",0)
        lines.append(f"{nm}（{sym}）权重 {wt}")
    return "\n".join(lines)

def read_brief()->str:
    p=pathlib.Path("briefing.txt")
    if p.is_file():
        try:
            txt=p.read_text("utf-8")
            logging.info("Loaded briefing.txt for context")
            return txt
        except Exception as e:
            logerr(f"读 briefing.txt 失败: {type(e).__name__}: {e}")
    logging.warning("briefing.txt 不存在，材料不足")
    return "（提示：briefing.txt 不存在或为空，资讯材料不足。）"

def time_slot_desc(slot:str)->str:
    if slot=="open": return "开盘 09:50（开盘 20 分钟）"
    if slot=="mid": return "上午收盘前 11:20（临近午盘）"
    if slot=="close": return "收盘后 15:05（日内收盘）"
    return "常规时段"

def build_prompt(slot:str, holds:list[dict], briefing:str)->str:
    tspan=f"{(datetime.now(TZ)-timedelta(days=3)).strftime('%Y-%m-%d')} 至 {datetime.now(TZ).strftime('%Y-%m-%d')}"
    return textwrap.dedent(f"""
    角色设定：
    你是一位面向 A 股与全球市场的行业分析助理。根据给定“持仓行业列表”和“最近 1–3 天资讯摘要（文末）”，给出最新、相关性强且可操作的信息整合与专业点评。材料不足请直说“暂无关键增量信息”。

    写作要求：
    - 只输出纯文本，不要 Markdown、不用表格。
    - 按“行业板块”分段，每段 200–300 字；最后给出 3–5 条“整体要点”。
    - 每条点评要包含：事件本质、原因/背景、潜在影响（行业/龙头/ETF）、市场反应或可能的预期差、风险与不确定性。
    - 优先使用过去 1–3 天信息；若材料不足请说明。

    当前时段：{time_slot_desc(slot)}
    我的持仓（ETF/行业）：
    {holdings_text(holds)}

    资讯摘要（过去 1–3 天）：
    {briefing}

    输出结构（纯文本）：
    1) 半导体 —— 关键资讯与点评（{tspan}）
    2) 医药 —— 关键资讯与点评（{tspan}）
    3) 白酒 —— 关键资讯与点评（{tspan}）
    4) 宏观/债券/其他 —— 关键资讯与点评（{tspan}）
    最后：整体要点（3–5 条）。
    """).strip()

# 调 Qwen
async def call_qwen(prompt:str)->str:
    api_key=os.getenv("QWEN_API_KEY")
    if not api_key:
        raise RuntimeError("QWEN_API_KEY 未设置")
    headers={"Content-Type":"application/json","Authorization":f"Bearer {api_key}"}
    payload={"model":MODEL,"input":{"prompt":prompt},"parameters":{"max_tokens":1000,"temperature":0.4}}
    async with httpx.AsyncClient(timeout=60) as c:
        for i in range(3):
            try:
                r=await c.post(QWEN_API, json=payload, headers=headers)
                r.raise_for_status()
                txt=r.json().get("output",{}).get("text","").strip()
                if not txt: raise RuntimeError("空响应")
                return txt
            except Exception as e:
                logerr(f"Qwen 调用失败 {i+1}/3: {type(e).__name__}: {e}")
                await asyncio.sleep(2*(i+1))
    raise RuntimeError("Qwen 连续失败")

# 推送（纯文本）
async def push_serverchan(text:str):
    key=os.getenv("SCKEY")
    if not key: return
    try:
        async with httpx.AsyncClient(timeout=20) as c:
            r=await c.post(f"https://sctapi.ftqq.com/{key}.send", data={"text":"每日投资点评","desp":text})
            if r.status_code!=200:
                logerr(f"ServerChan HTTP {r.status_code}: {r.text}")
    except Exception as e:
        logerr(f"ServerChan 推送异常: {type(e).__name__}: {e}")

async def push_telegram(text:str):
    tok=os.getenv("TELEGRAM_BOT_TOKEN"); cid=os.getenv("TELEGRAM_CHAT_ID")
    if not tok or not cid: return
    chunks=[text[i:i+3500] for i in range(0,len(text),3500)]
    async with httpx.AsyncClient(timeout=20) as c:
        for ch in chunks:
            try:
                r=await c.post(f"https://api.telegram.org/bot{tok}/sendMessage",
                               data={"chat_id":cid,"text":ch})  # 不设 parse_mode
                if r.status_code!=200:
                    logerr(f"Telegram HTTP {r.status_code}: {r.text}"); break
            except Exception as e:
                logerr(f"Telegram 推送异常: {type(e).__name__}: {e}"); break

async def main():
    slot=os.getenv("PUSH_SLOT","generic")  # open/mid/close/generic
    holds=load_holdings()
    briefing=read_brief()
    prompt=build_prompt(slot, holds, briefing)
    logging.info("Prompt prepared, calling Qwen...")
    try:
        out=await call_qwen(prompt)
    except Exception as e:
        logerr(f"LLM 生成失败: {type(e).__name__}: {e}")
        return
    await push_serverchan(out)
    await push_telegram(out)
    logging.info(f"{slot} 推送完成")

if __name__=="__main__":
    asyncio.run(main())
