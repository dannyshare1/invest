# -*- coding: utf-8 -*-
"""
news_pipeline.py — 市场资讯采集器（Google News + 聚合数据）+ 正文命中过滤 + 详细日志
用法：
    python news_pipeline.py

环境变量：
    QWEN_API_KEY   （用于补充短关键词；失败会兜底，不会阻塞）
    JUHE_KEY       （聚合数据接口密钥 http://apis.juhe.cn/fapigx/caijing/query）
    TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID （若你想这里也推送，可自行加；当前不推送）
输出文件：
    - keywords_used.txt   （最终关键词列表；含 LLM 与兜底来源标记）
    - sources_used.txt    （来源统计）
    - news_all.csv        （全量采集，不去重）
    - briefing.txt        （简要统计 + Top 若干标题）
    - errors.log          （详细错误堆栈：网络/解析/正文提取等）
依赖：见 requirements.txt
"""
from __future__ import annotations
import os, re, csv, math, json, time, asyncio, logging
from pathlib import Path
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import List, Dict, Any, Tuple
import httpx
import feedparser
from urllib.parse import quote_plus
from dateutil import parser as dtparse
from bs4 import BeautifulSoup
from readability import Document

# ────────────── 常量 & 全局 ──────────────
TZ = ZoneInfo("Asia/Shanghai")
NOW = lambda: datetime.now(TZ).isoformat(timespec="seconds")
OUT_KW   = Path("keywords_used.txt")
OUT_SRC  = Path("sources_used.txt")
OUT_CSV  = Path("news_all.csv")
OUT_BRI  = Path("briefing.txt")
OUT_ERR  = Path("errors.log")
HOLDINGS = Path("holdings.json")

QWEN_API = "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation"
JUHE_API = "http://apis.juhe.cn/fapigx/caijing/query"  # 你给的接口

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def logerr(msg: str, exc: Exception | None = None):
    try:
        with OUT_ERR.open("a", encoding="utf-8") as f:
            f.write(f"{NOW()} - {msg}\n")
            if exc:
                f.write(f"{type(exc).__name__}: {exc}\n")
    except Exception:
        # 避免日志写入再抛错
        pass

# ────────────── 工具函数 ──────────────
async def aretry(coro_fn, *args, retries=2, backoff=3, **kwargs):
    last_exc = None
    for i in range(retries + 1):
        try:
            return await coro_fn(*args, **kwargs)
        except Exception as e:
            last_exc = e
            if i < retries:
                await asyncio.sleep(backoff * (2 ** i))
    raise last_exc

def read_holdings() -> List[Dict[str, Any]]:
    if not HOLDINGS.is_file():
        logging.warning("未找到 holdings.json，默认空列表")
        return []
    return json.loads(HOLDINGS.read_text("utf-8"))

def _cn_chunks(name: str) -> List[str]:
    # 从中文名称中取 2~4 字短片段（去常见后缀）
    name = re.sub(r"(ETF|指数|基金|沪深|中证|交易型|开放式|联接|A|C)", "", name, flags=re.I)
    words = set()
    for m in re.finditer(r"[\u4e00-\u9fa5]{2,4}", name):
        words.add(m.group(0))
    return list(words)

def base_keywords_from_holdings(holds: List[Dict[str, Any]]) -> Tuple[List[str], List[str]]:
    """返回（短关键词, 行业标签）"""
    kw, tags = set(), set()
    for h in holds:
        nm = (h.get("name") or h.get("Name") or "").strip()
        sym = (h.get("symbol") or h.get("Symbol") or "").strip()
        for w in _cn_chunks(nm):
            kw.add(w)
        # 行业推断（非常保守）
        if any(x in nm for x in ("半导体", "芯片", "科创", "电子")): tags.add("半导体")
        if any(x in nm for x in ("医药", "医疗", "生物")):        tags.add("医药")
        if any(x in nm for x in ("白酒", "酒")):                 tags.add("白酒")
        if any(x in nm for x in ("债", "国债", "固收", "利率")):   tags.add("债券")
        if any(x in nm for x in ("豆粕", "农业", "农", "饲料")):   tags.add("农业")
        if "300" in nm or "沪深300" in nm or "HS300" in sym:    tags.add("宏观")
        if "红利" in nm:                                        tags.add("红利")
    # 兜底行业
    if not tags:
        tags = {"宏观"}
    # 兜底短词（一定要有）
    fallback = {"半导体","医药","白酒","农业","债券","利率","宏观","通胀","出口","消费","基建","新能源"}
    kw = kw | fallback
    return sorted(kw), sorted(tags)

async def call_qwen_short_keywords(holds: List[Dict[str, Any]], base_kw: List[str]) -> List[str]:
    key = os.getenv("QWEN_API_KEY")
    if not key:
        logging.warning("QWEN_API_KEY 未配置，跳过 LLM 关键词生成")
        return []
    names = ", ".join([(h.get("name") or h.get("Name") or "").strip() for h in holds if (h.get("name") or h.get("Name"))])
    prompt = f"""你将根据以下ETF中文名称，生成与其行业/主题高度相关的**中文短关键词**（每个2~4字），用顿号或换行分隔。
要求：避免过泛（如“经济”），更偏行业/材料/工艺/产业链环节/政策要点等；输出不超过100个。
ETF名称：{names}
已知相关短词（可参考但别重复太多）：{'、'.join(base_kw[:30])}
只输出词表，不要任何解释。"""

    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    payload = {
        "model": "qwen-plus",
        "input": {"prompt": prompt},
        "parameters": {"max_tokens": 500, "temperature": 0.3},
    }
    async with httpx.AsyncClient(timeout=30) as c:
        try:
            r = await c.post(QWEN_API, headers=headers, json=payload)
            r.raise_for_status()
            text = r.json().get("output", {}).get("text", "")
        except Exception as e:
            logerr("Qwen 调用失败", e)
            raise
    # 解析短词
    parts = re.split(r"[、，,；;。\s\n]+", text)
    out = [p.strip() for p in parts if 1 < len(p.strip()) <= 4 and re.search(r"[\u4e00-\u9fa5]", p)]
    return list(dict.fromkeys(out))  # 去重保序

def write_keywords_file(final_kw: List[Tuple[str, str]]):
    """final_kw: [(word, source_tag), ...]"""
    with OUT_KW.open("w", encoding="utf-8") as f:
        for w, tag in final_kw:
            f.write(f"{w}\t{tag}\n")

# ────────────── 抓取源：Google News RSS ──────────────
def google_news_search(keyword: str, per_kw_limit: int = 30) -> List[Dict[str, Any]]:
    # 注意：feedparser 是同步库
    url = (
        "https://news.google.com/rss/search?"
        f"q={quote_plus(keyword)}&hl=zh-CN&gl=CN&ceid=CN:zh-Hans"
    )
    feed = feedparser.parse(url)
    out = []
    for e in feed.entries[:per_kw_limit]:
        title = e.get("title", "").strip()
        link = (e.get("link") or e.get("id") or "").strip()
        published = e.get("published") or e.get("updated") or ""
        try:
            ts = dtparse.parse(published).astimezone(TZ).isoformat(timespec="seconds") if published else ""
        except Exception:
            ts = ""
        out.append({
            "source": "GoogleNews",
            "title": title,
            "url": link,
            "published": ts,
            "summary": "",
            "keyword": keyword,
        })
    return out

# ────────────── 抓取源：聚合数据财经 ──────────────
async def juhe_finance_fetch(max_pages: int = 5, page_size: int = 50) -> List[Dict[str, Any]]:
    key = os.getenv("JUHE_KEY")
    if not key:
        logging.info("聚合数据：JUHE_KEY 未配置，跳过")
        return []
    out = []
    async with httpx.AsyncClient(timeout=20) as c:
        for page in range(1, max_pages + 1):
            try:
                r = await c.get(JUHE_API, params={"key": key, "page": page, "pagesize": page_size})
                js = r.json()
                # 这个接口字段不稳定，尽量宽松解析
                # 常见结构：{"error_code":0,"result":{"data":[{title,content,url,ptime}]}}
                if js.get("error_code") == 0:
                    data = (js.get("result") or {}).get("data") or []
                else:
                    data = js.get("result") or js.get("data") or []
                for d in data:
                    title = str(d.get("title") or "").strip()
                    url = str(d.get("url") or d.get("source_url") or "").strip()
                    summary = str(d.get("content") or d.get("desc") or "").strip()
                    ptime = d.get("ptime") or d.get("pubdate") or d.get("ctime") or ""
                    try:
                        ts = dtparse.parse(str(ptime)).astimezone(TZ).isoformat(timespec="seconds") if ptime else ""
                    except Exception:
                        ts = ""
                    if title and url:
                        out.append({
                            "source": "JuheFinance",
                            "title": title,
                            "url": url,
                            "published": ts,
                            "summary": summary,
                            "keyword": "",
                        })
            except Exception as e:
                logerr(f"聚合数据 第{page}页失败", e)
                continue
    return out

# ────────────── 正文抓取 & 过滤 ──────────────
async def fetch_article_text(url: str, client: httpx.AsyncClient) -> str:
    try:
        r = await client.get(url, follow_redirects=True)
        html = r.text
        doc = Document(html)
        summary_html = doc.summary()
        soup = BeautifulSoup(summary_html, "lxml")
        txt = soup.get_text(separator="\n").strip()
        if not txt:
            # 兜底直接提取
            soup2 = BeautifulSoup(html, "lxml")
            txt = soup2.get_text(separator="\n")
        return re.sub(r"\s+\n", "\n", txt).strip()
    except Exception as e:
        logerr(f"正文抓取失败 {url}", e)
        return ""

def keyword_hit(text: str, kwset: set[str]) -> List[str]:
    hits = []
    for k in kwset:
        if k in text:
            hits.append(k)
    return hits

# ────────────── 主流程 ──────────────
async def main():
    print(f"{NOW()} - 开始收集（近 3 天），调用 Qwen 生成补充关键词")
    holds = read_holdings()
    print(f"{NOW()} - 读取持仓：holdings.json 共 {len(holds)} 条")

    base_kw, tags = base_keywords_from_holdings(holds)
    print(f"{NOW()} - 基础关键词 {len(base_kw)} 个；行业：{', '.join(tags)}")

    # LLM 关键词（失败不阻断）
    llm_kw: List[str] = []
    try:
        llm_kw = await aretry(call_qwen_short_keywords, holds, base_kw, retries=2, backoff=3)
        logging.info(f"Qwen 生成关键词 {len(llm_kw)} 个")
    except Exception:
        logging.error("Qwen 调用失败")
        pass

    # 最终关键词：去重保序。限制到 ~120 避免请求量过大
    final_kw_list: List[Tuple[str, str]] = []
    seen = set()
    for w in base_kw:
        if w and w not in seen:
            final_kw_list.append((w, "base"))
            seen.add(w)
    for w in llm_kw:
        if w and w not in seen:
            final_kw_list.append((w, "llm"))
            seen.add(w)
    # 不允许为空
    if not final_kw_list:
        final_kw_list = [(w, "fallback") for w in ["宏观", "利率", "通胀", "半导体", "医药", "白酒", "农业", "债券"]]
    # 限制长度
    final_kw_list = final_kw_list[:120]
    write_keywords_file(final_kw_list)
    print(f"{NOW()} - 最终关键词 {len(final_kw_list)} 个，已写 keywords_used.txt / sources_used.txt")

    # 收集：Google News（每词最多 30 条，关键词取前 30 个避免请求过多）
    kw_for_gn = [w for w, _ in final_kw_list[:30]]
    all_items: List[Dict[str, Any]] = []
    total_gn = 0
    for kw in kw_for_gn:
        try:
            items = await asyncio.to_thread(google_news_search, kw, 30)
            total_gn += len(items)
            all_items.extend(items)
        except Exception as e:
            logerr(f"GoogleNews 关键词 {kw} 失败", e)

    print(f"{NOW()} - GoogleNews 抓到 {total_gn} 条（未筛）")

    # 聚合数据（无关键词，取多页）
    juhe_items = await juhe_finance_fetch(max_pages=5, page_size=50)
    print(f"{NOW()} - 聚合数据 抓到 {len(juhe_items)} 条（未筛）")
    all_items.extend(juhe_items)

    print(f"{NOW()} - 收集完成：全量 {len(all_items)} 条（未去重）")

    # 写 sources_used.txt
    with OUT_SRC.open("w", encoding="utf-8") as f:
        f.write(f"GoogleNews\t{total_gn}\nJuheFinance\t{len(juhe_items)}\n")

    # —— 正文过滤（命中任一关键词）——
    kwset = set([w for w, _ in final_kw_list])
    kept_items: List[Dict[str, Any]] = []
    sem = asyncio.Semaphore(8)
    async with httpx.AsyncClient(timeout=15) as client:
        async def fetch_and_check(item):
            async with sem:
                txt = await fetch_article_text(item["url"], client)
                item["content_len"] = len(txt)
                if not txt:
                    item["kw_hits"] = ""
                    return item, False
                hits = keyword_hit(txt, kwset)
                item["kw_hits"] = "、".join(hits)
                return item, bool(hits)

        tasks = [fetch_and_check(it) for it in all_items]
        for fut in asyncio.as_completed(tasks):
            try:
                item, ok = await fut
                kept_items.append(item if ok else item)
            except Exception as e:
                logerr("正文过滤任务失败", e)

    # 写 news_all.csv（全量，附上命中关键词/正文长度）
    cols = ["source", "title", "url", "published", "summary", "keyword", "kw_hits", "content_len"]
    with OUT_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for it in kept_items:
            w.writerow({c: it.get(c, "") for c in cols})

    hit_count = sum(1 for it in kept_items if it.get("kw_hits"))
    print(f"{NOW()} - 正文筛选后保留 {hit_count} 条（命中≥1 关键词）")

    # 写 briefing.txt
    lines = [
        f"时间：{NOW()}",
        f"全量抓取：{len(all_items)} 条；正文命中：{hit_count} 条；关键词数：{len(final_kw_list)}",
        "—— Top 标题（按原始顺序）——"
    ]
    for it in kept_items[:30]:
        mark = "✅" if it.get("kw_hits") else "—"
        title = (it.get("title") or "").replace("\n", " ").strip()
        src = it.get("source") or ""
        ts = it.get("published") or ""
        lines.append(f"{mark} {title} [{src}] {ts}")
    OUT_BRI.write_text("\n".join(lines), encoding="utf-8")

    # 打印 errors.log 大小
    size = OUT_ERR.stat().st_size if OUT_ERR.exists() else 0
    print(f"{NOW()} - 已写 briefing.txt、news_all.csv、keywords_used.txt、sources_used.txt")
    print(f"{NOW()} - errors.log 大小 {size} bytes")
    print(f"{NOW()} - collector 任务完成")

if __name__ == "__main__":
    asyncio.run(main())
