# -*- coding: utf-8 -*-
"""
news_pipeline.py — Google 新闻聚合 + 持仓驱动关键词（Qwen 生成），带完整错误日志

你将得到：
- news_all.csv         # 全量新闻（不去重）
- briefing.txt         # 简要摘要：标题 + 摘要分行
- keywords_used.txt    # 本轮实际用到的关键词（含 LLM 生成）
- sources_used.txt     # 实际访问过的源与 URL
- errors.log           # 详细错误日志（永不空写，会在 0 条时也写明原因）

环境变量（可选）：
- QWEN_API_KEY         # 生成关键词（2-4 个汉字），如果没有也行，会只用 ETF 名称的切词备选
- TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID / SCKEY  # 与本脚本无关，这里不推送
"""

from __future__ import annotations
import asyncio, os, json, csv, html, re, traceback
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dateutil import tz
import httpx
from bs4 import BeautifulSoup
import feedparser
from urllib.parse import quote, urlparse
from rich import print as rprint

# ───────── 路径 & 常量 ─────────
TZ = tz.gettz("Asia/Shanghai")
NOW_ISO = datetime.now(TZ).isoformat()
OUT_DIR = Path(".")
OUT_CSV = OUT_DIR / "news_all.csv"
OUT_BRI = OUT_DIR / "briefing.txt"
OUT_KW = OUT_DIR / "keywords_used.txt"
OUT_SRC = OUT_DIR / "sources_used.txt"
OUT_ERR = OUT_DIR / "errors.log"

DAYS_LOOKBACK = int(os.getenv("NEWS_DAYS", "3"))   # 近几天
CONCURRENCY = 8                                    # 并发抓取上限/关键词
MAX_PER_KEYWORD = 80                               # 每个关键词抓取条数上限（Google News RSS 能给到的就拿）
USER_AGENT = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
              "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36")

# ───────── 工具 ─────────
def logerr(msg: str):
    OUT_ERR.parent.mkdir(parents=True, exist_ok=True)
    with OUT_ERR.open("a", encoding="utf-8") as f:
        f.write(f"{datetime.now(TZ).isoformat()} - {msg}\n")

def load_holdings() -> list[dict]:
    # 优先读文件，其次读 env HOLDINGS_JSON
    p = Path("holdings.json")
    if p.is_file():
        return json.loads(p.read_text("utf-8"))
    env = os.getenv("HOLDINGS_JSON")
    return json.loads(env) if env else []

def uniq_keep_order(seq):
    seen = set()
    out = []
    for x in seq:
        if x and x not in seen:
            seen.add(x)
            out.append(x)
    return out

def strip_html(s: str) -> str:
    if not s:
        return ""
    return BeautifulSoup(s, "html.parser").get_text(" ", strip=True)

def host_of(url: str) -> str:
    try:
        return urlparse(url).netloc or "unknown"
    except Exception:
        return "unknown"

# ───────── LLM 生成关键词（2–4 汉字）─────────
async def qwen_keywords(etf_names: list[str]) -> list[str]:
    api = "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation"
    key = os.getenv("QWEN_API_KEY")
    if not key:
        rprint("[yellow]⚠️ 未配置 QWEN_API_KEY，将退化为规则切词[/yellow]")
        return []

    # 让模型只吐 JSON：{"kw": ["XXXX","YYYY",...]}，每个词 2-4 个汉字，金融/行业相关
    prompt = (
        "请根据这些ETF中文名称，产生与其行业/主题强相关的搜索关键词（仅中文，2-4个汉字，避免过泛词如“市场/公司/经济”）。"
        "不要解释，不要换行，严格输出 JSON 格式：{\"kw\": [\"词1\",\"词2\",...]}。ETF：\n"
        + "、".join(etf_names)
    )
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {key}"}
    payload = {"model": "qwen-plus", "input": {"prompt": prompt},
               "parameters": {"max_tokens": 500, "temperature": 0.3}}

    try:
        async with httpx.AsyncClient(timeout=60, headers=headers) as c:
            resp = await c.post(api, json=payload)
            text = resp.json()["output"]["text"]
    except Exception as e:
        logerr(f"Qwen 调用失败：{e}")
        return []

    # 尝试解析 JSON；如果失败，回退用正则提取 2-4 汉字词
    try:
        js = json.loads(text)
        arr = js.get("kw", [])
        out = [x.strip() for x in arr if re.fullmatch(r"[\u4e00-\u9fa5]{2,4}", x or "")]
        return uniq_keep_order(out)
    except Exception:
        # 正则兜底
        out = re.findall(r"[\u4e00-\u9fa5]{2,4}", text)
        return uniq_keep_order(out)

# ───────── 规则备份关键词（无 LLM 时）─────────
def backup_keywords_from_etf_names(etf_names: list[str]) -> list[str]:
    # 从 ETF 名里抽取 2-4 字词（去掉“ETF/指数/基金/交易型”等常见词）
    junk = {"ETF", "指数", "基金", "交易", "交易型", "场内", "份额", "联接", "增强", "沪深", "中证"}
    cand = []
    for name in etf_names:
        name = re.sub(r"[A-Za-z0-9\-\s\.]+", "", name or "")
        words = re.findall(r"[\u4e00-\u9fa5]{2,4}", name)
        for w in words:
            if w not in junk:
                cand.append(w)
    return uniq_keep_order(cand)

# ───────── Google News RSS 搜索 ─────────
async def google_news_for_keyword(client: httpx.AsyncClient, kw: str, days: int, max_items: int) -> list[dict]:
    # q=关键词 when:Xd；中文界面
    url = (f"https://news.google.com/rss/search?q={quote(kw)}+when:{days}d"
           f"&hl=zh-CN&gl=CN&ceid=CN:zh-Hans")
    try:
        r = await client.get(url)
        txt = r.text
    except Exception as e:
        logerr(f"GoogleNews 请求失败 kw={kw} err={e}")
        return []

    try:
        feed = feedparser.parse(txt)
        items = []
        for e in feed.entries[:max_items]:
            title = html.unescape(e.get("title", "").strip())
            summ = strip_html(e.get("summary", ""))
            link = e.get("link", "")
            # 发布时间
            if e.get("published_parsed"):
                dt = datetime.fromtimestamp(
                    __import__("time").mktime(e.published_parsed), tz=timezone.utc
                ).astimezone(TZ)
            else:
                dt = datetime.now(TZ)
            src = (e.get("source", {}) or {}).get("title") or host_of(link)
            items.append({
                "published_at": dt.isoformat(),
                "title": title,
                "summary": summ,
                "url": link,
                "source": f"GoogleNews:{src}",
                "keyword": kw
            })
        return items
    except Exception as e:
        logerr(f"GoogleNews 解析失败 kw={kw} err={e}")
        return []

# （可选）聚合数据 - 综合财经新闻（按关键词逐次查）
async def juhe_finance(client: httpx.AsyncClient, kw: str, key: str) -> list[dict]:
    # 文档不详，尽量兼容常见参数名；失败只记日志不中断
    api = "http://apis.juhe.cn/fapigx/caijing/query"
    params_try = [
        {"key": key, "q": kw, "size": 50},
        {"key": key, "keyword": kw, "size": 50},
        {"key": key, "word": kw, "size": 50},
    ]
    for params in params_try:
        try:
            r = await client.get(api, params=params)
            js = r.json()
            if js.get("result"):
                out = []
                for d in js["result"]:
                    out.append({
                        "published_at": (d.get("pubDate") or d.get("ctime") or NOW_ISO),
                        "title": d.get("title", "").strip(),
                        "summary": strip_html(d.get("digest") or d.get("content") or ""),
                        "url": d.get("url") or d.get("link") or "",
                        "source": f"Juhe:{d.get('source','')}",
                        "keyword": kw
                    })
                return out
        except Exception as e:
            logerr(f"Juhe 调用失败 kw={kw} err={e}")
    return []

# ───────── 主收集流程 ─────────
async def collect():
    print(f"{datetime.now(TZ).isoformat()} - INFO - 开始收集（近 {DAYS_LOOKBACK} 天），调用 Qwen 生成补充关键词")

    holds = load_holdings()
    print(f"{datetime.now(TZ).isoformat()} - INFO - 读取持仓：holdings.json 共 {len(holds)} 条")
    etf_names = [h.get("name") or h.get("symbol") for h in holds]
    etf_names = [n for n in etf_names if n]

    # 1) LLM 关键词
    kw_llm = await qwen_keywords(etf_names)
    # 2) 备份规则关键词（LLM 为空时使用；否则也一并加入）
    kw_backup = backup_keywords_from_etf_names(etf_names)
    base_kw = uniq_keep_order(kw_llm + kw_backup)

    # 限制词长 2-4 汉字 & 去重
    base_kw = [k for k in base_kw if re.fullmatch(r"[\u4e00-\u9fa5]{2,4}", k or "")]
    print(f"{datetime.now(TZ).isoformat()} - INFO - 最终关键词 {len(base_kw)} 个，已写 keywords_used.txt / sources_used.txt")

    if not base_kw:
        OUT_KW.write_text("(空)\n", encoding="utf-8")
        OUT_SRC.write_text("未调用任何源（关键词为空）\n", encoding="utf-8")
        logerr("关键词集合为空：可能 Qwen 无返回且备份规则未命中")
        return []

    # 写 keywords_used.txt
    OUT_KW.write_text("\n".join(base_kw), encoding="utf-8")

    # 并发抓取
    juhe_key = os.getenv("JUHE_KEY")
    sem = asyncio.Semaphore(CONCURRENCY)
    src_lines = []

    async with httpx.AsyncClient(timeout=20, headers={"User-Agent": USER_AGENT}) as client:
        async def one_kw(kw: str) -> list[dict]:
            async with sem:
                g_items = await google_news_for_keyword(client, kw, DAYS_LOOKBACK, MAX_PER_KEYWORD)
                src_lines.append(f"GoogleNews kw={kw} items={len(g_items)}")
                j_items = []
                if juhe_key:
                    j_items = await juhe_finance(client, kw, juhe_key)
                    src_lines.append(f"Juhe kw={kw} items={len(j_items)}")
                return g_items + j_items

        tasks = [one_kw(k) for k in base_kw]
        all_lists = await asyncio.gather(*tasks, return_exceptions=True)

    # 汇总 & 错误处理
    rows = []
    for idx, res in enumerate(all_lists):
        if isinstance(res, Exception):
            logerr(f"关键词[{base_kw[idx]}] 任务异常: {traceback.format_exception_only(type(res), res)[0].strip()}")
            continue
        rows.extend(res)

    # 写 sources_used.txt
    OUT_SRC.write_text("\n".join(src_lines) + "\n", encoding="utf-8")

    # 写 CSV（不去重）
    if rows:
        rows.sort(key=lambda x: x["published_at"], reverse=True)
        with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["published_at", "source", "title", "summary", "url", "keyword"])
            for it in rows:
                w.writerow([it["published_at"], it["source"], it["title"], it["summary"], it["url"], it["keyword"]])

        # 写 briefing.txt（标题和摘要分行，方便阅读）
        lines = ["# 今日新闻简报（自动汇总）", f"时间：{NOW_ISO}", ""]
        for i, it in enumerate(rows[:60], 1):  # 简报最多 60 条
            lines.append(f"{i}. {it['title']}  [{it['source']}]")
            if it["summary"]:
                lines.append(f"    {it['summary']}")
            lines.append(f"    {it['url']}")
            lines.append("")
        OUT_BRI.write_text("\n".join(lines), encoding="utf-8")

        print(f"{datetime.now(TZ).isoformat()} - INFO - 收集完成：全量 {len(rows)} 条（未去重）")
        print(f"{datetime.now(TZ).isoformat()} - INFO - 已写 briefing.txt、news_all.csv、keywords_used.txt、sources_used.txt")
    else:
        # 即便 0 条，也要写空文件，避免你找不到
        OUT_CSV.write_text("", encoding="utf-8")
        OUT_BRI.write_text("# 今日新闻简报（空）\n", encoding="utf-8")
        print(f"{datetime.now(TZ).isoformat()} - INFO - 收集完成：全量 0 条（未去重）")
        print(f"{datetime.now(TZ).isoformat()} - INFO - 已写 briefing.txt、news_all.csv、keywords_used.txt、sources_used.txt")

    # errors.log 如果没内容，也写一行说明
    if not OUT_ERR.exists() or OUT_ERR.stat().st_size == 0:
        OUT_ERR.write_text(f"{NOW_ISO} - 没有捕获到错误\n", encoding="utf-8")
    print(f"{datetime.now(TZ).isoformat()} - INFO - errors.log 大小 {OUT_ERR.stat().st_size} bytes")
    print(f"{datetime.now(TZ).isoformat()} - INFO - collector 任务完成")

def main():
    asyncio.run(collect())

if __name__ == "__main__":
    main()
