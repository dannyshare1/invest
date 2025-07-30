import os
import yaml
import requests
import time
from datetime import datetime
from zoneinfo import ZoneInfo  # Python 3.9+
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ——— 环境变量读取 ———
SCKEY = os.getenv("SCKEY")
QWEN_API_KEY = os.getenv("QWEN_API_KEY")
if not SCKEY:
    raise ValueError("❌ 缺少 Server 酱密钥 SCKEY")
if not QWEN_API_KEY:
    raise ValueError("❌ 缺少通义千问 API 密钥 QWEN_API_KEY")

# ——— 准备带重试的 HTTP Session ———
session = requests.Session()
retries = Retry(
    total=3,                # 最多重试 3 次
    backoff_factor=2,       # 重试间隔：2s, 4s, 8s
    status_forcelist=[502, 503, 504]
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

# ——— 读取并校验持仓 ———
with open("holdings.yaml", "r", encoding="utf-8") as f:
    holdings = yaml.safe_load(f)

total_pct = sum(float(v.strip('%')) for v in holdings.values())
if round(total_pct) != 100:
    raise ValueError(f"❌ 仓位总和为 {total_pct}%，应为 100%。请检查 holdings.yaml。")

# ——— 仓位变动检测与记录 ———
last_path = "last_holdings.yaml"
should_record = True
if os.path.exists(last_path):
    with open(last_path, "r", encoding="utf-8") as f:
        last_holdings = yaml.safe_load(f)
    should_record = holdings != last_holdings

if should_record:
    with open(last_path, "w", encoding="utf-8") as f:
        yaml.dump(holdings, f, allow_unicode=True)

holdings_lines = "\n".join([f"- {k}：{v}" for k, v in holdings.items()])

# ——— 构建 Prompt ———
prompt = f"""
你是一个专业的中国投资策略分析师，请根据当前投资者（C5进取型）的仓位结构和市场动态，生成一份每日投资建议：

📅 日期：{date_str}（周{weekday}）

📊 当前持仓比例：
{holdings_lines}

📌 请你提供以下内容（总字数控制在500字以内）：
1. 前一交易日的重点市场新闻摘要，覆盖当前持仓相关行业（如红利、高股息、半导体、蓝筹、债券、大消费、大宗商品等）。
2. 针对上述仓位，提供专业、简洁的操作建议（如维持、加仓、减仓、调仓等）。
3. 可选项：如当前市场适合进行定投，请明确指出，并说明建议的定投标的和理由；如某类资产存在阶段性高位或风险，也请提示止盈策略。
"""

# ——— 调用通义千问 API（带重试+超时） ———
try:
    resp = session.post(
        "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {QWEN_API_KEY}"
        },
        json={
            "model": "qwen-max",  # 或 "qwen-plus"
            "input": {"prompt": prompt},
            "parameters": {"result_format": "message"},
            "workspace": "ilm-c9d12em00wxjtstn"  # 替换为你的 workspace ID
        },
        timeout=30
    )
    resp.raise_for_status()
except Exception as e:
    print("❌ 通义千问接口调用失败：", e)
    raise

qwen_reply = resp.json()["output"]["choices"][0]["message"]["content"].strip()

# ——— 推送到 Server 酱 ———
push_url = f"https://sctapi.ftqq.com/{SCKEY}.send"
sck_res = session.post(push_url, data={"title": title, "desp": qwen_reply}, timeout=10)
print("✅ Server 酱 推送：", sck_res.status_code, sck_res.text)

# ——— 写入日志文件 ———
logs_dir = "logs"
os.makedirs(logs_dir, exist_ok=True)
log_path = os.path.join(logs_dir, today.isoformat() + ".md")
with open(log_path, "w", encoding="utf-8") as f:
    f.write(f"# {title}\n\n")
    f.write(qwen_reply + "\n\n")
    if should_record:
        f.write("📌 本次仓位已变动，已记录最新持仓。\n")
    else:
        f.write("📌 持仓未发生变化，未更新持仓记录。\n")

# ——— 推送到 Telegram ———
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
    tg_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    tg_payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": f"{title}\n\n{qwen_reply}",
        "parse_mode": "Markdown"
    }
    tg_res = session.post(tg_url, json=tg_payload, timeout=10)
    print("✅ Telegram 推送：", tg_res.status_code, tg_res.text)
