import os
import requests
import datetime
import yaml

# 获取环境变量
SCKEY = os.getenv("SCKEY")
QWEN_API_KEY = os.getenv("QWEN_API_KEY")

if not SCKEY:
    raise ValueError("❌ 缺少 Server 酱密钥 SCKEY")
if not QWEN_API_KEY:
    raise ValueError("❌ 缺少通义千问 API 密钥 QWEN_API_KEY")

# 获取日期
today = datetime.date.today()
date_str = today.strftime("%Y年%m月%d日")
weekday = "一二三四五六日"[today.weekday()]
title = f"📈 每日投资建议 · {date_str}"

# 读取 holdings.yaml
with open("holdings.yaml", "r", encoding="utf-8") as f:
    holdings = yaml.safe_load(f)

# 校验总和是否为 100%
total_pct = sum(float(v.strip('%')) for v in holdings.values())
if round(total_pct) != 100:
    raise ValueError(f"❌ 仓位总和为 {total_pct}%，应为 100%。请检查 holdings.yaml 文件。")

# 自动检测仓位是否有变动
last_path = "last_holdings.yaml"
should_record_holdings = True

if os.path.exists(last_path):
    with open(last_path, "r", encoding="utf-8") as f:
        last_holdings = yaml.safe_load(f)
    should_record_holdings = holdings != last_holdings

if should_record_holdings:
    with open(last_path, "w", encoding="utf-8") as f:
        yaml.dump(holdings, f, allow_unicode=True)

# 构造持仓文本
holdings_lines = "\n".join([f"- {k}：{v}" for k, v in holdings.items()])

# 构建 prompt
prompt = f"""
你是一个专业的中国投资策略分析师，请根据当前投资者（C5进取型）的仓位结构和市场动态，生成一份每日投资建议：

📅 日期：{date_str}（周{weekday}）

📊 当前持仓比例：
{holdings_lines}

📌 请你提供以下内容（建议控制在500字以内，最多不超过800字）：
1. 前一交易日的重点市场新闻摘要，覆盖当前持仓相关行业（包括但不局限红利、高股息、半导体、蓝筹、债券、大消费、大宗商品等）。
2. 针对上述仓位，提供专业、简洁的操作建议（如维持、加仓、减仓、调仓等）。
3. 可选项：如当前市场适合进行定投，请明确指出，并说明建议的定投标的和理由；如某类资产存在阶段性高位或风险，也请提示止盈策略。
"""

# 调用通义千问
response = requests.post(
    url="https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation",
    headers={
        "Content-Type": "application/json",
        "Authorization": f"Bearer {QWEN_API_KEY}"
    },
    json={
        "model": "qwen-plus",
        "input": {
            "prompt": prompt
        },
        "parameters": {
            "result_format": "message"
        },
        "workspace": "ilm-c9d12em00wxjtstn"  # ← 改成你自己的 workspace ID
    }
)

if response.status_code != 200:
    raise Exception("❌ 通义千问 API 调用失败: " + response.text)

qwen_reply = response.json()["output"]["choices"][0]["message"]["content"]

# 推送到 Server 酱
push_url = f"https://sctapi.ftqq.com/{SCKEY}.send"
res = requests.post(push_url, data={"title": title, "desp": qwen_reply.strip()})

# 写入 markdown 日志
logs_dir = "logs"
os.makedirs(logs_dir, exist_ok=True)
log_path = os.path.join(logs_dir, today.strftime("%Y-%m-%d") + ".md")

with open(log_path, "w", encoding="utf-8") as f:
    f.write(f"# {title}\n\n")
    f.write(qwen_reply.strip())
    if should_record_holdings:
        f.write("\n\n📌 本次仓位已变动，记录当前持仓结构。")
    else:
        f.write("\n\n📌 仓位未发生变化，未记录当前结构。")

print("✅ 推送完成！", res.status_code, res.text)

# 推送到 Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
    telegram_url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    telegram_payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": f"{title}\n\n{qwen_reply.strip()}",
        "parse_mode": "Markdown"
    }
    telegram_response = requests.post(telegram_url, json=telegram_payload)
    print("📤 Telegram 推送：", telegram_response.status_code, telegram_response.text)

