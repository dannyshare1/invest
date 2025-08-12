# Daily News & Investment Pipeline

一组 Python 脚本，用于抓取市场资讯、记录持仓变动，并生成每日投资提示。

## 脚本介绍

- `news_pipeline.py`
  - 根据 `sources.yml` 中的 RSS 源和可选 API，结合 `holdings.json` 自动生成关键词
  - 抓取新闻并输出 `briefing.txt`、`news_all.csv` 等文件，同时维护源的健康状态
- `holdings_tracker.py`
  - 比较 `holdings.json` 与历史快照，记录持仓增删改
  - 更新 `holdings_log.csv`、`holdings_history.csv`
- `daily_push_qwen.py`
  - 读取 `briefing.txt` 与当前持仓
  - 调用通义千问生成中文投资提示，并通过 Server 酱或 Telegram 推送

## 使用示例

```bash
pip install -r requirements.txt

# 更新持仓或源配置后依次运行：
python news_pipeline.py      # 抓取并过滤新闻
python holdings_tracker.py   # 记录持仓变化
python daily_push_qwen.py    # 生成并推送提示
```

`news_pipeline.py` 会输出：
- `briefing.txt`
- `news_all.csv`
- `keywords_used.txt` / `qwen_keywords.txt`
- `sources_used.txt`
- `errors.log`

若需推送或调用额外 API，请设置相关环境变量（如 `QWEN_API_KEY`、`NEWSAPI_KEY`、`MEDIASTACK_KEY`、`JUHE_KEY`、`SCKEY`、`TELEGRAM_BOT_TOKEN`、`TELEGRAM_CHAT_ID`）。

## 依赖管理

`requirements.txt` 中的依赖已固定版本以确保可重复运行，建议定期（例如每月或每季度）检查并更新这些版本，以获得安全补丁和兼容性修复。
