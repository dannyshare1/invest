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

# ——— 带重试 Session ———
session = requests.Session()
session.mount("https://", HTTPAdapter(max_retries=Retry(total=0)))

# ——— 北京时间 ———
bj_now = datetime.now(ZoneInfo("Asia/Shanghai"))
today   = bj_now.date()
date_str, weekday = bj_now.strftime("%Y年%m月%d日"), "一二三四五六日"[bj_now.weekday()]
title = f"📈 每日投资建议 · {date_str}"

# ——— 读取持仓 & 校验 ———
with open("holdings.yaml", "r", encoding="utf-8") as f:
    holdings = yaml.safe_load(f)
if round(sum(float(v.strip('%')) for v in holdings.values())) != 100:
    raise ValueError("持仓总和≠100%")

# —— 仓位变动记录 ——（同之前，省略详情）
last_path, should_record = "last_holdings.yaml", True
if os.path.exists(last_path):
    should_record = holdings != yaml.safe_load(open(last_path, encoding="utf-8"))
if should_record:
    yaml.dump(holdings, open(last_path,"w",encoding="utf-8"), allow_unicode=True)

holdings_lines = "\n".join(f"- {k}：{v}" for k,v in holdings.items())

# —— 读取 news.json ——
news_snippets = ""
if os.path.exists("news.json"):
    with open("news.json","r",encoding="utf-8") as nf:
        for n in json.load(nf)[:8]:
            line = f"- {n['title']} ({n['source']} {n['published']})"
            news_snippets += textwrap.shorten(line, 120, placeholder="…") + "\n"

# —— Prompt ——
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

# —— 调用通义千问（4 次重试，读超时 90s） ——
payload = {
    "model": "qwen-max",
    "input": {"prompt": prompt},
    "parameters": {"result_format": "message"},
    "workspace": "ilm-c9d12em00wxjtstn"
}
for i in range(4):
    try:
        r = session.post(
            "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation",
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {APIKEY}"
            },
            json=payload,
            timeout=(10, 90)
        )
        r.raise_for_status()
        break
    except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
        print(f"⚠️ 第 {i+1} 次调用失败：{e}")
        if i == 3: raise
        time.sleep(2**i)

content = r.json()["output"]["choices"][0]["message"]["content"].strip()

# —— Server 酱推送 ——
session.post(f"https://sctapi.ftqq.com/{SCKEY}.send",
             data={"title": title, "desp": content}, timeout=10)

# —— Telegram 推送（可选） ——
TG_TOKEN, TG_CHAT = os.getenv("TELEGRAM_BOT_TOKEN"), os.getenv("TELEGRAM_CHAT_ID")
if TG_TOKEN and TG_CHAT:
    session.post(f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                 json={"chat_id": TG_CHAT, "text": f"{title}\n\n{content}", "parse_mode": "Markdown"},
                 timeout=10)

# —— 日志记录 ——
os.makedirs("logs", exist_ok=True)
with open(f"logs/{today.isoformat()}.md","w",encoding="utf-8") as f:
    f.write(f"# {title}\n\n{content}\n\n")
    f.write("📌 持仓变动已记录。\n" if should_record else "📌 持仓未变动。\n")
print("✅ 推送完成")
