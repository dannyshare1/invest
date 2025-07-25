import os
import datetime
import requests
import yaml

# 日期
today = datetime.date.today()
date_str = today.strftime("%Y年%m月%d日")
weekday = "一二三四五六日"[today.weekday()]

# 读取持仓
with open("holdings.yaml", "r", encoding="utf-8") as f:
    holdings = yaml.safe_load(f)
holdings_lines = "\n".join([f"- {k}：{v}" for k, v in holdings.items()])

# 构造 prompt
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
