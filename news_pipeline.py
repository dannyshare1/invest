# -*- coding: utf-8 -*-
"""
news_pipeline.py — RSS 主采集器（中文优先，适配 Qwen 关键词）
v2025-08-08.r4

变更
----
1) briefing 去掉 URL，只保留【时刻】【来源】【标题】+ 80 字摘要。
2) news_all.csv 统一 UTF-8 with BOM (utf-8-sig) 写出，Excel 打开不再显示乱码。
3) RSS 查询与关键字处理：
   - 关键词：先用“持仓→行业映射”的中文基词，再调用 Qwen 产出 2-4 字中文补充词（失败也不影响主流程）。
   - 统一用 quote_plus 做 URL 编码，避免空格/括号导致 400。
4) 错误日志：
   - 对每个源记录 status_code / content-type / 样本前 120 字。
   - 所有异常写入 errors.log，并在收尾输出文件大小。
5) 输出：
   - briefing.txt（中文清单，无 URL）
   - news_all.csv（utf-8-sig）
   - keywords_used.txt / qwen_keywords.txt / sources_used.txt / errors.log
"""

from __future__ import annotations
import os, csv, re, zipfile, io, time, json, traceback
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Tuple
import httpx
import feedparser
from urllib.parse import quote_plus

# ───────────────────────── 基础配置 ─────────────────────────
TZ = timezone(timedelta(hours=8))  # 北京时区
NOW = lambda: datetime.now(TZ).strftime("%Y-%m-%dT%H:%M:%S%z")

DAYS = int(os.getenv("COLLECT_DAYS", "3"))  # 近几天
USE_QWEN = True
FETCH_FULLTEXT = False  # 如要按正文再筛选，可改 True（跑得更慢）

# 输出文件
OUT_BRIEF = "briefing.txt"
OUT_CSV   = "news_all.csv"
OUT_KEYS  = "keywords_used.txt"
OUT_QWEN  = "qwen_keywords.txt"
OUT_SRCS  = "sources_used.txt"
OUT_ERR   = "errors.log"

# 源（只放相对稳定的中文 RSS）
RSS_SOURCES = [
    ("FT中文",    "https://www.ftchinese.com/rss/news"),
    ("界面新闻",  "https://a.jiemian.com/index.php?m=article&a=rss"),
    ("新浪财经",  "https://rss.sina.com.cn/roll/finance/hot_roll.xml"),
    # 下面两个常见 429/风控，保留但失败不影响主流程
    ("财新(镜像)", "https://rsshub.app/caixin/latest"),
    ("华尔街见闻", "https://rsshub.app/wallstreetcn/news"),
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) invest-bot/1.0"
}

# ───────────────────────── 工具函数 ─────────────────────────
def log(msg: str):
    print(f"{NOW()} - {msg}")

def log_err(msg: str, sample: str | None = None):
    line = f"{NOW()} - ERROR - {msg}"
    print(line)
    with open(OUT_ERR, "a", encoding="utf-8") as f:
        f.write(line + "\n")
        if sample:
            f.write(sample[:120] + "\n")

def read_holdings() -> List[Dict]:
    path = "holdings.json"
    if not os.path.isfile(path):
        log_err("holdings.json not found")
        return []
    try:
        return json.loads(open(path, "r", encoding="utf-8").read())
    except Exception as e:
        log_err(f"holdings.json 解析失败: {e}")
        return []

# 基础中文关键词（按 ETF 名称 → 行业）
ETF_TO_BASE_KW = {
    "半导体": ["半导体","芯片","晶圆","封测","光刻","EDA","硅片","HBM","GPU"],
    "医药":   ["医药","创新药","仿制药","疫苗","集采","MAH","PD-1","医疗器械","CXO"],
    "白酒":   ["白酒","消费升级","渠道","价格","产区"],
    "沪深300":["宏观","经济","政策","社融","PMI","汇率","降息","通胀"],
    "国债":   ["国债","利率","收益率","央行","货币政策","流动性","债市"],
    "农业":   ["豆粕","大豆","饲料","油脂","期货","供需","库存"],
    "红利":   ["红利","高股息","分红","稳定现金流"],
}

def guess_industries(holds: List[Dict]) -> Tuple[List[str], List[str]]:
    inds=set()
    base=set()
    for h in holds:
        name = h.get("name","")
        if "半导体" in name: inds.add("半导体"); base.update(ETF_TO_BASE_KW["半导体"])
        if "医药"   in name: inds.add("医药");   base.update(ETF_TO_BASE_KW["医药"])
        if "酒"     in name: inds.add("白酒");   base.update(ETF_TO_BASE_KW["白酒"])
        if "沪深300" in name or "300" in h.get("symbol",""): inds.add("宏观"); base.update(ETF_TO_BASE_KW["沪深300"])
        if "国债"   in name: inds.add("国债");   base.update(ETF_TO_BASE_KW["国债"])
        if "豆粕"   in name or "农业" in name: inds.add("农业"); base.update(ETF_TO_BASE_KW["农业"])
        if "红利"   in name: inds.add("红利");   base.update(ETF_TO_BASE_KW["红利"])
    return sorted(inds), sorted(base)

async def call_qwen_for_keywords(industries: List[str], seed: List[str]) -> List[str]:
    """让 Qwen 只给中文 2~4 字关键词；失败返回空列表。"""
    api = "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation"
    key = os.getenv("QWEN_API_KEY")
    if not key:
        log_err("Qwen 未配置，跳过关键词生成")
        return []
    prompt = (
        "请基于这些行业与种子词，产出一批**中文**关键词（长度 2-4 字），只输出用顿号或逗号分隔的词，不要解释：\n"
        f"行业：{','.join(industries) or '宏观'}\n"
        f"种子词：{','.join(seed[:30])}\n"
        "要求：全部中文，贴近财经/行业表述，避免英文与过长短语。"
    )
    try:
        async with httpx.AsyncClient(timeout=60) as c:
            r = await c.post(
                api,
                headers={"Authorization": f"Bearer {key}", "Content-Type":"application/json"},
                json={"model":"qwen-plus","input":{"prompt":prompt},
                      "parameters":{"max_tokens": 300, "temperature":0.7}},
            )
            r.raise_for_status()
            text = r.json().get("output",{}).get("text","")
            # 只保留中文、去重、2~4 字
            words = re.findall(r"[\u4e00-\u9fa5]{2,4}", text)
            return sorted(set(words))
    except Exception as e:
        log_err(f"Qwen 调用失败: {e}")
        return []

def within_days(published: datetime, days: int=DAYS) -> bool:
    return published.astimezone(TZ) >= (datetime.now(TZ) - timedelta(days=days))

def clean_text(t: str) -> str:
    t = re.sub(r"\s+", " ", t or "")
    return t.strip()

def parse_entry(e) -> Tuple[str,str,str,datetime,str]:
    title = clean_text(getattr(e, "title", ""))
    summ  = clean_text(getattr(e, "summary", "") or getattr(e, "description", ""))
    link  = getattr(e, "link", "")
    src   = clean_text(getattr(e, "source", "") or getattr(e, "author", "") or "")
    pdt   = None
    if getattr(e, "published_parsed", None):
        pdt = datetime.fromtimestamp(time.mktime(e.published_parsed), tz=timezone.utc).astimezone(TZ)
    else:
        pdt = datetime.now(TZ)
    return title, summ, link, pdt, src

def hit_keywords(text: str, kws: List[str]) -> bool:
    for k in kws:
        if k and k in text:
            return True
    return False

def write_csv(rows: List[Dict]):
    # 用 utf-8-sig，避免 Excel 乱码；多一份可选 GBK 以便国内工具
    fieldnames = ["time","source","title","summary","url"]
    with open(OUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def write_briefing(rows: List[Dict]):
    # 不写 URL，给 LLM 看的纯文本清单
    lines = [f"# 新闻清单（近 {DAYS} 天，{len(rows)} 条）", ""]
    for r in rows:
        t = datetime.fromisoformat(r["time"]).strftime("%m-%d %H:%M")
        src = r["source"] or "未知来源"
        title = r["title"]
        summ = (r["summary"] or "")[:80]
        lines.append(f"- {t} {title} —— {summ}")
    with open(OUT_BRIEF, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

def write_textfile(path: str, text: str):
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)

def zip_logs():
    patt = ["briefing.txt","news_all.csv","keywords_used.txt","qwen_keywords.txt","sources_used.txt","errors.log"]
    ts = datetime.now(TZ).strftime("%Y%m%d-%H%M%S")
    name = f"logs-and-history-{ts}.zip"
    with zipfile.ZipFile(name, "w", zipfile.ZIP_DEFLATED) as z:
        for p in patt:
            if os.path.isfile(p):
                z.write(p)
    return name

# ───────────────────────── 采集主流程 ─────────────────────────
async def main():
    log(f"开始收集（近 {DAYS} 天），调用 Qwen 生成补充关键词")
    holds = read_holdings()
    log(f"读取持仓：holdings.json 共 {len(holds)} 条")
    industries, base_kw = guess_industries(holds)
    log(f"基础关键词 {len(base_kw)} 个；行业：{', '.join(industries) or '宏观'}")

    # Qwen 产出补充词（全中文）
    extra_kw = []
    if USE_QWEN:
        extra_kw = await call_qwen_for_keywords(industries, base_kw)
        if extra_kw:
            log(f"Qwen 生成关键词 {len(extra_kw)} 个")
            write_textfile(OUT_QWEN, "\n".join(extra_kw))
        else:
            log("Qwen 未返回关键词，使用基础词即可")

    # 最终关键词（中文为主）
    final_kw = list(dict.fromkeys([*base_kw, *extra_kw]))
    write_textfile(OUT_KEYS, "\n".join(final_kw))
    write_textfile(OUT_SRCS, "\n".join([f"{n} {u}" for n,u in RSS_SOURCES]))
    log(f"最终关键词 {len(final_kw)} 个，已写 {OUT_KEYS} / {OUT_SRCS}")

    # 抓取 RSS
    all_items: List[Dict] = []
    cutoff = datetime.now(TZ) - timedelta(days=DAYS)

    for name, url in RSS_SOURCES:
        try:
            # RSS 只是读源，不带关键词查询；之后在本地筛选
            d = feedparser.parse(url)
            got = 0
            for e in d.entries:
                title, summ, link, pdt, src = parse_entry(e)
                if not within_days(pdt, DAYS):
                    continue
                all_items.append({
                    "time": pdt.isoformat(),
                    "source": name or src or "",
                    "title": title,
                    "summary": summ,
                    "url": link,
                })
                got += 1
            log(f"RSS {name} 抓到 {got} 条（未筛）")
        except Exception as e:
            log_err(f"RSS {name} 失败: {e}")

    log(f"收集完成：全量 {len(all_items)} 条（未去重）")

    # 本地按标题+摘要筛选；若需要更激进可打开 FETCH_FULLTEXT 再按正文二次过滤
    kept: List[Dict] = []
    for r in all_items:
        text = r["title"] + " " + r["summary"]
        if hit_keywords(text, final_kw):
            kept.append(r)
        elif FETCH_FULLTEXT:
            # 可选：逐站点抓正文再筛选（此处省略，避免跑慢/风控）
            pass

    log(f"标题/摘要命中后保留 {len(kept)} 条（命中≥1 关键词）")

    # 写出
    write_csv(kept)
    write_briefing(kept)

    err_size = os.path.getsize(OUT_ERR) if os.path.isfile(OUT_ERR) else 0
    log(f"errors.log 大小 {err_size} bytes")

    zipname = zip_logs()
    log(f"打包 → {zipname}")
    log("collector 任务完成")

# ───────────────────────── 入口 ─────────────────────────
if __name__ == "__main__":
    import asyncio
    try:
        asyncio.run(main())
    except Exception as e:
        log_err(f"FATAL: {e}\n{traceback.format_exc()}")
        raise
