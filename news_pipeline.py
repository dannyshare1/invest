# -*- coding: utf-8 -*-
"""
news_pipeline.py — 仅抓 Bing News 的资讯收集器（中文关键词，防乱码版）

功能概述
--------
1) 读取 holdings.json（持仓），自动生成【中文】关键词（≤4字）。
   - 基础中文词：由持仓名称提取 + 行业典型词。
   - 可选：调用通义千问(Qwen)补充中文细分词（仅中文，不要英文/数字/符号）。
   - 最终关键词写入：keywords_used.txt；若调用 LLM，也写 qwen_keywords.txt。
2) 只抓 Bing News 搜索结果：
   - 解析 Bing 的 apiclick 跳转，直接落到源站 URL，避免乱码。
   - 抓源站正文：先 utf-8，失败回退 gb18030；对少数域名（如 china.org.cn）再做一遍
     verify=False 的容错尝试；内容中中文字符太少视为“疑似乱码/非中文”，丢弃。
   - 标题 + 正文 同时做关键词命中（≥1 即保留）。
3) 写出：
   - news_all.csv（全量命中，未去重）
   - briefing.txt（简要清单）
   - keywords_used.txt / qwen_keywords.txt / sources_used.txt（执行轨迹）
   - errors.log（详细错误）
   - logs-and-history-YYYYmmdd-HHMMSS.zip（上述文件打包）
4) 只依赖：httpx、bs4(lxml)、python-dateutil、rich

环境变量（可选）
--------------
QWEN_API_KEY        调用通义千问补充中文关键词（可缺省，缺省则不用 LLM）
ONLY_ZH_KEYWORDS    "1"（默认）：只要中文关键词。设为 "0" 则不过滤（不建议）
SEARCH_DAYS         近几天新闻，默认 3
BING_PAGES          每个关键词批次抓多少页，默认 2（每页 ~ 10-15 条）
BATCH_SIZE          每批合并多少关键词一起搜，默认 6（降低请求数）
MAX_ARTICLES        最多抓多少篇正文，默认 900（防炸）
"""

from __future__ import annotations
import asyncio, csv, json, os, re, sys, zipfile
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Tuple
from urllib.parse import urlparse, parse_qs, unquote

import httpx
from bs4 import BeautifulSoup
from dateutil import tz
from rich import print as rprint

# ─── 常量 & 输出文件 ───
ROOT = Path(".")
OUT_CSV = ROOT / "news_all.csv"
OUT_BRI = ROOT / "briefing.txt"
OUT_KW = ROOT / "keywords_used.txt"
OUT_KW_QWEN = ROOT / "qwen_keywords.txt"
OUT_SRC = ROOT / "sources_used.txt"
OUT_ERR = ROOT / "errors.log"

TZ = timezone(timedelta(hours=8))  # 显示北京时间
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "zh-CN,zh;q=0.9",
}

# 对极少数证书偶发报错的域名，再尝试一次 verify=False
SSL_LAX_DOMAINS = {"china.org.cn", "chinese.gov.cn"}

# ─── 辅助 ───
def now() -> str:
    return datetime.now(TZ).strftime("%Y-%m-%dT%H:%M:%S%z")

def log_info(msg: str):
    print(f"{now()} - {msg}")

def log_err(msg: str):
    with OUT_ERR.open("a", encoding="utf-8") as f:
        f.write(f"{now()} - {msg}\n")

def ensure_clean_logs():
    # 每次跑前把 errors.log 清空（其他文件不会清）
    OUT_ERR.write_text("", encoding="utf-8")

def chinese_words_only(words: List[str], maxlen: int = 4) -> List[str]:
    out = []
    for w in words:
        w = re.sub(r"[，,;；\s]+", "", w)
        if not w:
            continue
        # 仅保留含中文的，长度 2~maxlen
        if re.fullmatch(r"[\u4e00-\u9fff]{2,%d}" % maxlen, w):
            out.append(w)
    # 去重（保持顺序）
    seen = set()
    dedup = []
    for w in out:
        if w not in seen:
            seen.add(w)
            dedup.append(w)
    return dedup

def chinese_ratio(text: str) -> float:
    if not text:
        return 0.0
    zh = len(re.findall(r"[\u4e00-\u9fff]", text))
    return zh / max(len(text), 1)

def first_n_cn(text: str, n: int = 160) -> str:
    # 提取前 n 个中文字符作为摘要（不破坏编码）
    buf = []
    for ch in text:
        if re.match(r"[\u4e00-\u9fff]", ch):
            buf.append(ch)
        if len(buf) >= n:
            break
    return "".join(buf)

# ─── 读取持仓 & 生成基础中文关键词 ───
def load_holdings() -> List[Dict]:
    p = ROOT / "holdings.json"
    if not p.is_file():
        log_info("holdings.json 不存在，按空处理")
        return []
    try:
        data = json.loads(p.read_text("utf-8"))
        return data if isinstance(data, list) else []
    except Exception as e:
        log_err(f"读取 holdings.json 失败: {e}")
        return []

BASE_DICT = {
    "红利": ["分红", "股息"],
    "沪深300": ["沪深300", "沪指", "蓝筹"],
    "半导体": ["半导体", "芯片", "集成电路", "封测", "代工"],
    "医药": ["医药", "创新药", "集采", "生物医药", "医疗器械", "中药"],
    "白酒": ["白酒", "消费升级", "渠道"],
    "豆粕": ["豆粕", "饲料", "大豆", "油脂油料"],
    "国债": ["国债", "收益率", "利率", "流动性"],
    "宏观": ["货币政策", "财政政策", "通胀", "逆回购", "PMI"],
}

def derive_base_keywords(holds: List[Dict]) -> Tuple[List[str], List[str]]:
    base = set()
    industries = set()
    for h in holds:
        name = str(h.get("name", "")).strip()
        if not name:
            continue
        # 去掉 ETF 后缀
        name = name.replace("ETF", "").replace("指数", "")
        # 行业判定（粗略）
        for k in BASE_DICT:
            if k in name:
                industries.add(k)
                for t in BASE_DICT[k]:
                    base.add(t)
        # 直接把简化后的 ETF 名也放进去
        # 保证是中文且 2~4 个字
        for w in re.findall(r"[\u4e00-\u9fff]{2,4}", name):
            base.add(w)
    # 宏观类总是加一点
    industries.add("宏观")
    for t in BASE_DICT["宏观"]:
        base.add(t)

    base_kw = chinese_words_only(list(base), maxlen=4)
    return base_kw, sorted(list(industries))

# ─── 调 Qwen 只补充中文 ≤4 字关键词 ───
async def call_qwen_for_keywords(base_kw: List[str], holds: List[Dict]) -> List[str]:
    api = "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation"
    key = os.getenv("QWEN_API_KEY")
    if not key:
        return []
    holds_lines = "\n".join([f"- {h.get('name','')} ({h.get('symbol','')})" for h in holds])
    prompt = (
        "你是中文资讯抽取器。根据以下持仓与基础词，只输出“中文关键词”，每个2-4个汉字，"
        "不要英文/数字/符号/空格；不要重复；尽量具体（如“集采”“HBM”可保留为中文或缩写，但整体输出必须是中文）。\n\n"
        f"【持仓】\n{holds_lines}\n\n"
        f"【基础中文词】\n{', '.join(base_kw)}\n\n"
        "请输出：仅一行，用中文逗号分隔的关键词列表。"
    )
    headers = {"Content-Type": "application/json", "Authorization": f"Bearer {key}"}
    payload = {
        "model": "qwen-plus",
        "input": {"prompt": prompt},
        "parameters": {"max_tokens": 500, "temperature": 0.5},
    }
    try:
        async with httpx.AsyncClient(timeout=60) as c:
            r = await c.post(api, json=payload, headers=headers)
            r.raise_for_status()
            text = r.json().get("output", {}).get("text", "") or ""
            # 提取中文 2~4 字词
            raw = re.findall(r"[\u4e00-\u9fff]{2,4}", text)
            return chinese_words_only(raw, maxlen=4)
    except Exception as e:
        log_err(f"Qwen 关键词失败: {e}")
        return []

# ─── Bing News 搜索 & 解析 ───
def build_bing_query(kw_batch: List[str], days: int) -> str:
    # 仅中文关键词 OR 组合；增加 past days 限制（Bing 不稳定，用“过去N天”中文提示）
    q = " OR ".join(kw_batch)
    # 轻提示中文 + 时间；Bing 不保证遵守，但有用
    return f"({q}) 过去{days}天 中文 新闻"

def extract_real_url(apiclick_url: str) -> str:
    # Bing apiclick 里会带 url= 真实链接
    try:
        qs = parse_qs(urlparse(apiclick_url).query)
        if "url" in qs and qs["url"]:
            return unquote(qs["url"][0])
    except Exception:
        pass
    return apiclick_url

async def fetch_bing_news_html(client: httpx.AsyncClient, query: str, page: int) -> str:
    # page 从 1 开始；first=1, 11, 21...
    first = 1 + (page - 1) * 10
    url = "https://www.bing.com/news/search"
    params = {
        "q": query,
        "qft": 'sortbydate="1"',   # 按时间
        "form": "YFNR",
        "setlang": "ZH-Hans",
        "cc": "CN",
        "first": str(first),
    }
    r = await client.get(url, params=params, headers=HEADERS)
    r.raise_for_status()
    return r.text

def parse_bing_cards(html: str) -> List[Tuple[str, str]]:
    # 返回 [(title, real_url)]
    soup = BeautifulSoup(html, "lxml")
    items = []
    for a in soup.select("a.title, a.t_t"):  # 尝试覆盖不同模板
        href = a.get("href")
        if not href:
            continue
        title = (a.get_text() or "").strip()
        real = extract_real_url(href)
        items.append((title, real))
    # 兜底：搜索 apiclick 链接
    for a in soup.select("a[href*='bing.com/news/apiclick']"):
        href = a.get("href")
        title = (a.get_text() or "").strip()
        real = extract_real_url(href)
        items.append((title, real))
    # 去重（按 url）
    seen = set()
    uniq = []
    for t, u in items:
        if u not in seen:
            seen.add(u)
            uniq.append((t, u))
    return uniq

# ─── 抓正文（防乱码） ───
async def fetch_article(url: str, client: httpx.AsyncClient) -> Tuple[str, str]:
    """
    返回 (clean_text, final_url)
    - 先 utf-8 decode，失败回退 gb18030
    - 如果中文占比过低，判定为无效
    - 对 SSL_LAX_DOMAINS 再尝试一次 verify=False
    """
    try:
        r = await client.get(url, headers=HEADERS, timeout=20)
    except httpx.SSLError:
        host = urlparse(url).hostname or ""
        if any(host.endswith(d) for d in SSL_LAX_DOMAINS):
            try:
                r = await client.get(url, headers=HEADERS, timeout=20, verify=False)
            except Exception as e:
                log_err(f"SSL 容错失败: {url} | {e}")
                return "", url
        else:
            log_err(f"SSL 错误: {url}")
            return "", url
    except Exception as e:
        log_err(f"抓取失败: {url} | {e}")
        return "", url

    # 尝试多种解码
    raw = r.content
    text = ""
    for enc in ("utf-8", "gb18030", None):
        try:
            text = raw.decode(enc or "utf-8", errors="ignore")
            break
        except Exception:
            continue

    soup = BeautifulSoup(text, "lxml")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    clean = soup.get_text("\n", strip=True)
    # 中文校验
    if chinese_ratio(clean) < 0.05:  # 中文比例太低，视为无效/乱码
        return "", str(r.url)
    return clean, str(r.url)

def match_any_kw(text: str, title: str, kws: List[str]) -> List[str]:
    hit = set()
    blob = f"{title}\n{text}"
    for w in kws:
        if w in blob:
            hit.add(w)
    return sorted(hit)

# ─── 主流程 ───
async def collect(days: int, batch_size: int, pages: int, max_articles: int,
                  use_llm: bool, only_zh_kw: bool):
    ensure_clean_logs()

    holds = load_holdings()
    log_info(f"读取持仓：holdings.json 共 {len(holds)} 条")

    base_kw, industries = derive_base_keywords(holds)
    log_info(f"基础关键词 {len(base_kw)} 个；行业：{', '.join(industries) if industries else '无'}")

    qwen_kw: List[str] = []
    if use_llm:
        try:
            qwen_kw = await call_qwen_for_keywords(base_kw, holds)
        except Exception as e:
            log_err(f"Qwen 调用失败: {e}")
            qwen_kw = []

    # 关键词整合
    final_kw = base_kw + [w for w in qwen_kw if w not in base_kw]
    if only_zh_kw:
        final_kw = chinese_words_only(final_kw, maxlen=4)

    # 写关键词文件
    OUT_KW.write_text("\n".join(final_kw), encoding="utf-8")
    if qwen_kw:
        OUT_KW_QWEN.write_text("\n".join(qwen_kw), encoding="utf-8")
    OUT_SRC.write_text("", encoding="utf-8")  # 稍后填充

    log_info(f"最终关键词 {len(final_kw)} 个，已写 keywords_used.txt / {'qwen_keywords.txt' if qwen_kw else ''}".rstrip(" / "))

    if not final_kw:
        log_info("关键词为空，直接结束。")
        return [], []

    # 组装批次查询
    batches = []
    buf = []
    for w in final_kw:
        buf.append(w)
        if len(buf) >= batch_size:
            batches.append(buf); buf = []
    if buf:
        batches.append(buf)

    all_cards: List[Tuple[str, str]] = []
    sources_log = []

    async with httpx.AsyncClient(timeout=20, headers=HEADERS) as c:
        # 搜索页并解析
        for b in batches:
            q = build_bing_query(b, days)
            fetched = 0
            for p in range(1, pages + 1):
                try:
                    html = await fetch_bing_news_html(c, q, p)
                    cards = parse_bing_cards(html)
                    all_cards.extend(cards)
                    fetched += len(cards)
                except Exception as e:
                    log_err(f"Bing 搜索失败: page={p} | {e}")
            sources_log.append(f"[BingNews] batch={','.join(b)} | pages={pages} | got={fetched}")

        # 去重 URL
        seen = set()
        uniq_cards = []
        for t, u in all_cards:
            if u not in seen:
                seen.add(u)
                uniq_cards.append((t, u))

        log_info(f"BingNews 抓到 {len(uniq_cards)} 条（未筛）")

        # 抓正文
        sem = asyncio.Semaphore(12)
        results = []

        async def worker(title: str, url: str):
            async with sem:
                text, final_url = await fetch_article(url, c)
                if not text:
                    return
                hits = match_any_kw(text, title, final_kw)
                if hits:
                    results.append({
                        "time": now(),
                        "source": urlparse(final_url).hostname or "",
                        "title": title,
                        "url": final_url,
                        "hits": ",".join(hits),
                        "excerpt": first_n_cn(text, 160),
                        "length": len(text),
                    })

        tasks = []
        for i, (t, u) in enumerate(uniq_cards):
            if i >= max_articles:
                break
            tasks.append(asyncio.create_task(worker(t, u)))
        await asyncio.gather(*tasks, return_exceptions=True)

    # 写 sources_used
    OUT_SRC.write_text("\n".join(sources_log), encoding="utf-8")
    return results, sources_log

def write_outputs(rows: List[Dict], sources_log: List[str]):
    # CSV
    if rows:
        with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["time", "source", "title", "url", "hits", "excerpt", "length"])
            w.writeheader()
            w.writerows(rows)

    # briefing
    lines = []
    lines.append(f"# 命中资讯（{len(rows)}）\n")
    for i, r in enumerate(rows[:80], 1):
        lines.append(f"{i}. {r['title']}\n   {r['url']}\n   命中：{r['hits']}\n")
    OUT_BRI.write_text("\n".join(lines), encoding="utf-8")

    # 压缩包
    stamp = datetime.now(TZ).strftime("%Y%m%d-%H%M%S")
    zipname = ROOT / f"logs-and-history-{stamp}.zip"
    with zipfile.ZipFile(zipname, "w", compression=zipfile.ZIP_DEFLATED) as z:
        for p in [OUT_CSV, OUT_BRI, OUT_KW, OUT_KW_QWEN, OUT_SRC, OUT_ERR]:
            if p.exists():
                z.write(p, p.name)
    log_info(f"errors.log 大小 {OUT_ERR.stat().st_size if OUT_ERR.exists() else 0} bytes；打包 → {zipname.name}")

# ─── CLI ───
async def main():
    days = int(os.getenv("SEARCH_DAYS", "3"))
    pages = int(os.getenv("BING_PAGES", "2"))
    batch_size = int(os.getenv("BATCH_SIZE", "6"))
    max_articles = int(os.getenv("MAX_ARTICLES", "900"))
    only_zh_kw = os.getenv("ONLY_ZH_KEYWORDS", "1") == "1"
    use_llm = bool(os.getenv("QWEN_API_KEY"))

    log_info(f"开始收集（近 {days} 天），仅 Bing News；"
             f"{'启用' if use_llm else '不启用'} Qwen 关键词，"
             f"{'仅中文关键词' if only_zh_kw else '关键词不过滤'}")

    rows, src = await collect(
        days=days,
        batch_size=batch_size,
        pages=pages,
        max_articles=max_articles,
        use_llm=use_llm,
        only_zh_kw=only_zh_kw,
    )
    log_info(f"收集完成：命中 {len(rows)} 条（未去重）")

    write_outputs(rows, src)
    log_info("collector 任务完成")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
