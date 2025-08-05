import os, time, json, yaml, requests, textwrap
from datetime import datetime
from zoneinfo import ZoneInfo
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ——— 环境变量 ———
SCKEY  = os.getenv("SCKEY")
APIKEY = os.getenv("QWEN_API_KEY")
if not SCKEY or not APIKEY:
    raise ValueError("缺少 SCKEY 或 QWEN_API_KEY")

# ——— Session ———
session = requests.Session()
session.mount("https://", HTTPAdapter(max_retries=Retry(total=0)))

# ——— 日期 / 标题 ———
bj_now = datetime.now(ZoneInfo("Asia/Shanghai"))
today  = bj_now.date()
date_str = bj_now.strftime("%Y年%m月%d日")
weekday = "一二三四五六日"[bj_now.weekday()]
title = f"📈 每日投资建议 · {date_str}"

# ——— 读取持仓 ———
with open("holdings.yaml", "r", encoding="utf-8") as f:
    holdings = yaml.safe_load(f)
if round(sum(float(v.strip('%')) for v in holdings.values())) != 100:
    raise ValueError("持仓总和≠100%")
holdings_lines = "\n".join(f"- {k}：{v}" for k, v in holdings.items())

# ——— 仓位变动记录 ———
last_path = "last_holdings.yaml"
should_record = not os.path.exists(last_path) or holdings != yaml.safe_load(open(last_path,encoding="utf-8"))
if should_record:
    yaml.dump(holdings, open(last_path,"w",encoding="utf-8"), allow_unicode=True)

# ——— 读取新闻片段 ———
news_snippets = ""
if os.path.exists("news.json"):
    with open("news.json", "r", encoding="utf-8") as nf:
        for n in json.load(nf)[:8]:
            line = f"- {n['title']}：{n['snippet']} ({n['source']} {n['published']})"
            news_snippets += textwrap.shorten(line, 300, placeholder="…") + "\n"

# ——— Prompt ———
prompt = f"""
你是专业投资策略分析师。请参考以下资料，同时全网搜索包括但不限于持仓相关行业的媒体观点：
{news_snippets or '—今日抓取为空—'}

投资者（C5进取型）当前持仓：
{holdings_lines}

然后请输出：
1. 关于最近一些市场热点的见解；
2. 分析这些信息对持仓各资产的影响，并给出操作建议（维持/加仓/减仓/定投/止盈）；
3. 如时机明确，请用「✅ 建议」或「⚠️ 风险」高亮；
要求观点独到、剖析深入，避免流水账。
"""

# ——— 调用通义千问：MAX → PLUS 降级 ———
api_url = "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation"
headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {APIKEY}"
}
payload_base = {
    "input": {"prompt": prompt},
    "parameters": {"result_format": "message"},
    "workspace": "ilm-c9d12em00wxjtstn"   # ← 换成你的 workspace
}
models = ["qwen-max", "qwen-plus"]
used_model, resp = None, None

for model in models:
    payload = payload_base | {"model": model}
    for i in range(3):
        try:
            print(f"→ 调用 {model} 第 {i+1}/3 次")
            resp = requests.post(api_url, headers=headers, json=payload,
                                 timeout=(10, 90))
            resp.raise_for_status()
            used_model = model
            break
        except (requests.exceptions.Timeout,
                requests.exceptions.ConnectionError) as e:
            print(f"⚠️ {model} 第 {i+1} 次失败：{e}")
            if i < 2:
                time.sleep(2 ** i)
    if used_model:
        break

if not used_model:
    raise RuntimeError("❌ qwen-max 与 qwen-plus 均超时，放弃本次推送")

content = resp.json()["output"]["choices"][0]["message"]["content"].strip()
content += f"\n\n—— 本报告由 **{used_model}** 模型生成"

# ——— Server 酱推送 ———
session.post(f"https://sctapi.ftqq.com/{SCKEY}.send",
             data={"title": title, "desp": content}, timeout=10)

# ——— Telegram 推送（可选） ———
tg_token, tg_chat = os.getenv("TELEGRAM_BOT_TOKEN"), os.getenv("TELEGRAM_CHAT_ID")
if tg_token and tg_chat:
    session.post(f"https://api.telegram.org/bot{tg_token}/sendMessage",
                 json={"chat_id": tg_chat,
                       "text": f"{title}\n\n{content}",
                       "parse_mode": "Markdown"},
                 timeout=10)

# ——— 日志记录 ———
os.makedirs("logs", exist_ok=True)
with open(f"logs/{today.isoformat()}.md", "w", encoding="utf-8") as f:
    f.write(f"# {title}\n\n{content}\n\n")
    f.write("📌 持仓变动已记录。\n" if should_record else "📌 持仓未变动。\n")

print("✅ 推送完成")
