import requests
import datetime

# Server酱 Turbo 的推送 URL（使用 GitHub Secrets 注入 SCKEY）
import os
SCKEY = os.getenv("SCKEY")
if not SCKEY:
    raise ValueError("请在 GitHub Secrets 中配置 SCKEY")

push_url = f"https://sctapi.ftqq.com/{SCKEY}.send"

# 模拟生成投资建议（你可以替换为 ChatGPT 接口）
today = datetime.date.today().strftime("%Y年%m月%d日")
title = f"📈 每日投资建议 · {today}"
content = f"""
📅 {today}（周五）

🔹 市场概况：沪指+0.3%，北向资金小幅流入
🔹 热门板块：半导体、AI、机器人
🔹 弱势板块：银行、煤炭、地产

✅ 操作建议：
- 红利ETF暂不加仓，回调可再吸
- 半导体ETF（512480）可继续定投
- 白酒ETF等回调至1.22附近再考虑加仓
"""

res = requests.post(push_url, data={"title": title, "desp": content.strip()})
print("推送状态：", res.status_code, res.text)