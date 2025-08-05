import os
import yaml
import requests
import time
from datetime import datetime
from zoneinfo import ZoneInfo   # Python 3.9+ 环境
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ——— 环境变量 ———
SCKEY = os.getenv("SCKEY")
QWEN_API_KEY = os.getenv("QWEN_API_KEY")
if not SCKEY:
    raise ValueError("❌ 缺少 Server 酱密钥 SCKEY")
if not QWEN_API_KEY:
    raise ValueError("❌ 缺少通义千问 API 密钥 QWEN_API_KEY")

# ——— 带状态码重试的 Session ———
session = requests.Session()
retries = Retry(
    total=0  # 我们采用手动重试，所以这里设 0
)
adapter = HTTPAdapter(max_retries=retries)
session.mount("https://", adapter)
session.mount("http://", adapter)

# ——— 获取北京时间 ———
bj_now = datetime.now(ZoneInfo("Asia/Shanghai"))
today = bj_now.date()
date_str = today.strftime("%Y年%m月%d日")
weekday = "一二三四五六日"[bj_now.weekday()]
title = f"📈 每日投资建议 · {date_str}"

# ——— 读取并校验 holdings.yaml ———
with open("holdings.yaml", "r", encoding="utf-8") as f:
    holdings = yaml.safe_load(f)

total_pct = sum(float(v.strip("%")) for v in holdings.values())
if round(total_pct) != 100:
    raise ValueError(f"❌ 仓位总和为 {total_pct}%，应为 100%。请检查 holdings.yaml。")

# ——— 仓位变动检测 & 记录 ———
last_path = "last_holdings.yaml"
should_record = True
if os.path.exists(last_path):
    with open(last_path, "r", encoding="utf-8") as f:
        last_holdings = yaml.safe_load(f)
    should_record = (holdings != last_holdings)

if should_record:
    with open(last_path, "w", encoding="utf-8") as f:
        yaml.dump(holdings, f, allow_unicode=True)

holdings_lines = "\n".join(f"- {k}：{v}" for k, v in holdings.items())

import json, textwrap

# 读取新闻片段
news_snippets = ""
if os.path.exists("news.json"):
    with open("news.json", "r", encoding="utf-8") as nf:
        for n in json.load(nf)[:8]:        # 取前 8 条
            line = f"- {n['title']} ({n['source']} {n['published']})"
            news_snippets += textwrap.shorten(line, 120, placeholder="…") + "\n"

# ---------------- Prompt ----------------
prompt = f"""
请先看以下参考资料，然后在全网搜索相关的报道和分析评论
{news_snippets or '—今日抓取为空—'}

再结合投资者（C5进取型）当前仓位：
{holdings_lines}

要求有独特的观点，而且剖析很深刻：
1. 选出最重要的 6 条信息（中英文至少各 2 条），合并同义条目，每条括号注明媒体与日期；
2. 逐项说明对当前持仓的影响，并给出操作建议（维持/加仓/减仓/定投/止盈）；
3. 如定投或止盈时机明确，请用「✅ 建议」或「⚠️ 风险」高亮。
"""


# ——— 手动重试调用通义千问 API ———
api_url = "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation"
payload = {
    "model": "qwen-max",  # 或 "qwen-plus"
    "input": {"prompt": prompt},
    "parameters": {"result_format": "message"},
    "workspace": "ilm-c9d12em00wxjtstn"  # 替换为你的 workspace ID
}

for attempt in range(3):
    try:
        resp = session.post(
            api_url,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {QWEN_API_KEY}"
            },
            json=payload,
            timeout=(10, 60)  # 连接超时10s，读超时60s
        )
        resp.raise_for_status()
        break
    except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
        print(f"⚠️ 第 {attempt+1} 次调用通义千问失败：{e}")
        if attempt < 2:
            time.sleep(2 ** attempt)  # 2s、4s 后重试
            continue
        else:
            raise

qwen_reply = resp.json()["output"]["choices"][0]["message"]["content"].strip()

# ——— 推送到 Server 酱 ———
sck_url = f"https://sctapi.ftqq.com/{SCKEY}.send"
sck_res = session.post(sck_url, data={"title": title, "desp": qwen_reply}, timeout=10)
print("✅ Server 酱 推送：", sck_res.status_code, sck_res.text)

# ——— 写入日志 ———
logs_dir = "logs"
os.makedirs(logs_dir, exist_ok=True)
log_file = os.path.join(logs_dir, today.isoformat() + ".md")
with open(log_file, "w", encoding="utf-8") as f:
    f.write(f"# {title}\n\n{qwen_reply}\n\n")
    f.write("📌 仓位已变动，已记录持仓。\n" if should_record else "📌 仓位未变动，无需记录。\n")

# ——— 推送到 Telegram ———
tg_token = os.getenv("TELEGRAM_BOT_TOKEN")
tg_chat  = os.getenv("TELEGRAM_CHAT_ID")
if tg_token and tg_chat:
    tg_url = f"https://api.telegram.org/bot{tg_token}/sendMessage"
    tg_payload = {
        "chat_id": tg_chat,
        "text": f"{title}\n\n{qwen_reply}",
        "parse_mode": "Markdown"
    }
    tg_res = session.post(tg_url, json=tg_payload, timeout=10)
    print("✅ Telegram 推送：", tg_res.status_code, tg_res.text)
