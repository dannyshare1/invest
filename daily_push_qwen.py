import requests
import datetime
import os

# Server酱推送密钥（GitHub Secrets 中设置）
SCKEY = os.getenv("SCKEY")
if not SCKEY:
    raise ValueError("请设置 GitHub Secrets 中的 SCKEY")

# 通义千问 API 密钥
QWEN_API_KEY = os.getenv("QWEN_API_KEY")
if not QWEN_API_KEY:
    raise ValueError("请设置 GitHub Secrets 中的 QWEN_API_KEY")

# 生成日期信息
today = datetime.date.today()
date_str = today.strftime("%Y年%m月%d日")
weekday = "一二三四五六日"[today.weekday()]
title = f"📈 每日投资建议 · {date_str}"

# 仓位（可以替换为读取 holdings.yaml）
holdings = {
    "红利ETF": "10%",
    "半导体ETF": "7%",
    "沪深300": "5%",
    "短债": "78%"
}

# 构建 prompt 给通义千问
prompt = f"""
你是一个擅长ETF分析的投资顾问，请根据以下仓位结构和最近市场热点，生成一份简洁的每日投资建议：

📅 日期：{date_str}（周{weekday}）

📊 当前仓位：
- 红利ETF：{holdings['红利ETF']}
- 半导体ETF：{holdings['半导体ETF']}
- 沪深300：{holdings['沪深300']}
- 短债：{holdings['短债']}

🧠 要求：
- 请列出今日的市场重点新闻摘要（围绕红利、高股息、半导体、沪深300相关行业）
- 给出针对当前持仓的操作建议
- 建议语言风格专业、简明，不要机械模板
"""

# 调用通义千问（v1 接口）
response = requests.post(
    url="https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation",
    headers={
        "Content-Type": "application/json",
        "Authorization": f"Bearer {QWEN_API_KEY}"
    },
    json={
        "model": "qwen-plus",  # ← 或你控制台可用的模型名
        "input": {
            "prompt": prompt
        },
        "parameters": {
            "result_format": "message"
        },
        "workspace": "ilm-c9d12em00wxjtstn"  # ← 改成你截图中的“默认业务空间 ID”
    }
)


if response.status_code != 200:
    raise Exception("通义千问 API 调用失败: " + response.text)

qwen_reply = response.json()["output"]["choices"][0]["message"]["content"]

# 发送到 Server 酱
push_url = f"https://sctapi.ftqq.com/{SCKEY}.send"
res = requests.post(push_url, data={"title": title, "desp": qwen_reply.strip()})

print("✅ 推送完成：", res.status_code, res.text)
