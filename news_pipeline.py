# -*- coding: utf-8 -*-
"""
news_pipeline.py — RSS 主力采集 + 关键词筛选 + 简版 briefing
- 关键词：从持仓推断的基础词 +（可选）Qwen 自动扩展
- 来源：RSS 为主（FT中文/界面/新浪/财新镜像/华尔街见闻镜像 等）
- 输出：
  - briefing.txt   # 只含「日期 + 标题」，无 URL、无摘要（避免 LLM 误用链接）
  - news_all.csv   # 全部命中新闻（UTF-8-SIG 防止 Excel 乱码）
  - keywords_used.txt / qwen_keywords.txt / sources_used.txt
  - errors.log     # 详细错误
"""

from __future__ import annotations
import os, csv, re, html, json, time
from datetime import datetime, timedelta, timezone
from typing import List, Tuple
import feedparser
import httpx

TZ = timezone(timedelta(hours=8))  # 北京时间
BASE_DIR = os.getcwd()

OUT_BRIEF = os.path.join(BASE_DIR, "briefing.txt")
OUT_CSV = os.path.join(BASE_DIR, "news_all.csv")
OUT_KW = os.path.join(BASE_DIR, "keywords_used.txt")
OUT_QWEN = os.path.join(BASE_DIR, "qwen_keywords.txt")
OUT_SRC = os.path.join(BASE_DIR, "sources_used.txt")
OUT_ERR = os.path.join(BASE_DIR, "errors.log")

DAYS = int(os.getenv("COLLECT_DAYS", "3"))

# —— 主力 RSS 源（可按需增减）——
RSS_SOURCES: List[Tuple[str, str]] = [
    ("FT中文", "https://www.ftchinese.com/rss/news"),
    ("界面新闻", "https://a.jiemian.com/index.php?m=article&a=rss"),
    ("新浪财经", "https://rss.sina.com.cn/roll/finance/hot_roll.xml"),
    ("财新(镜像)", "https://rsshub.app/caixin/latest"),
    ("华尔街见闻(镜像)", "https://rsshub.app/wallstreetcn/news"),
]

def now() -> str:
    return datetime.now(TZ).isoformat(timespec="seconds")

def log(msg: str):
    print(f"{now()} - {msg}")

def log_err(msg: str):
    with open(OUT_ERR, "a", encoding="utf-8") as f:
        f.write(f"{now()} - {msg}\n")

def strip_html(s: str) -> str:
    s = html.unescape(s or "")
    s = re.sub(r"<[^>]+>", "", s, flags=re.S)  # 去标签
    s = re.sub(r"\s+", " ", s).strip()
    return s

# —— 读取持仓，构造基础行业关键词 —— 
def load_holdings() -> list[dict]:
    p = os.path.join(BASE_DIR, "holdings.json")
    if not os.path.isfile(p):
        return []
    return json.loads(open(p, "r", encoding="utf-8").read())

def infer_sectors(holds: list[dict]) -> List[str]:
    # 非严格分类，只按常见 ETF 名称里出现的关键词做个大致映射
    name_blob = " ".join([h.get("name","") + " " + h.get("symbol","") for h in holds])
    sec = set()
    m = name_blob
    if any(k in m for k in ["红利","分红","高股息","红利ETF"]): sec.add("红利")
    if any(k in m for k in ["国债","债","利率","固收","债券ETF"]): sec.add("债券")
    if any(k in m for k in ["半导体","芯片","科创","硬科技"]): sec.add("半导体")
    if any(k in m for k in ["医药","医疗"]): sec.add("医药")
    if any(k in m for k in ["豆粕","农业","农"]): sec.add("农业")
    sec.add("宏观")
    return list(sec)

def base_keywords_from_sectors(sectors: List[str]) -> List[str]:
    # 简洁稳妥的中文短词
    pool = {
        "红利":  ["红利","分红","高股息","稳定现金流"],
        "债券":  ["国债","收益率","利率","社融","央行","流动性","降息","加息"],
        "半导体":["半导体","芯片","封测","光刻","晶圆","硅片","EDA","GPU","HBM"],
        "医药":  ["医药","创新药","仿制药","医保谈判","疫苗","医疗器械","CXO","PD-1","MAH"],
        "农业":  ["豆粕","大豆","饲料","油脂","供需","库存","期货"],
        "宏观":  ["宏观","通胀","汇率","政策","PMI","经济"],
    }
    out = []
    for s in sectors:
        out += pool.get(s, [])
    # 去重
    u = []
    for x in out:
        if x not in u:
            u.append(x)
    return u

# —— 可选：用 Qwen 扩展关键词 —— 
async def qwen_expand(kw: List[str]) -> List[str]:
    key = os.getenv("QWEN_API_KEY")
    if not key:
        return []
    prompt = (
        "请基于以下财经/行业词，扩展一份中文关键词清单。"
        "要求：每个关键词≤4字；全是中文；避免公司名和英文缩写；与原词高度相关；去重；最多120个。\n原词："
        + "、".join(kw)
    )
    payload = {
        "model": "qwen-plus",
        "input": {"prompt": prompt},
        "parameters": {"max_tokens": 600, "temperature": 0.3},
    }
    try:
        async with httpx.AsyncClient(timeout=60) as c:
            r = await c.post(
                "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation",
                headers={"Authorization": f"Bearer {key}", "Content-Type":"application/json"},
                json=payload,
            )
            r.raise_for_status()
            text = r.json()["output"]["text"]
            # 解析成行
            cands = re.split(r"[，、,\s]+", strip_html(text))
            cands = [x for x in cands if 1 <= len(x) <= 4 and re.search(r"[\u4e00-\u9fa5]", x)]
            # 去重
            out = []
            for w in cands:
                if w not in out:
                    out.append(w)
            return out[:120]
    except Exception as e:
        log_err(f"Qwen 扩展失败: {e}")
        return []

# —— 抓取 RSS —— 
def fetch_rss(name: str, url: str) -> List[dict]:
    try:
        d = feedparser.parse(url)
        items = []
        for e in d.entries:
            t = strip_html(getattr(e, "title", "") or "")
            if not t:
                continue
            # 时间
            if hasattr(e, "published_parsed") and e.published_parsed:
                dt = datetime.fromtimestamp(time.mktime(e.published_parsed), tz=TZ)
            else:
                dt = datetime.now(TZ)
            # 简要正文用于关键词判定（很多源给的是摘要）
            summary = strip_html(getattr(e, "summary", "") or "")
            items.append({
                "title": t,
                "summary": summary,
                "source": name,
                "date": dt,   # datetime 对象，写文件时再格式化
            })
        return items
    except Exception as e:
        log_err(f"RSS 失败 {name}: {e}")
        return []

# —— 关键词筛选（标题 + 摘要二选一命中即通过）——
def hit(item: dict, kws: List[str]) -> bool:
    text = f"{item['title']} {item.get('summary','')}"
    return any(k in text for k in kws)

# —— 写文件 —— 
def write_keywords(all_kw: List[str], qwen_kw: List[str]):
    with open(OUT_KW, "w", encoding="utf-8") as f:
        f.write("\n".join(all_kw))
    with open(OUT_QWEN, "w", encoding="utf-8") as f:
        f.write("\n".join(qwen_kw))
    with open(OUT_SRC, "w", encoding="utf-8") as f:
        f.write("\n".join([f"{n} {u}" for n, u in RSS_SOURCES]))

def write_csv(items: List[dict]):
    # 为了 Excel 兼容使用 BOM
    with open(OUT_CSV, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date","source","title"])
        for it in items:
            w.writerow([it["date"].strftime("%Y-%m-%d %H:%M"), it["source"], it["title"]])

def write_briefing(items: List[dict]):
    # 只写「日期 + 标题」，不写 URL，不写摘要
    lines = []
    lines.append(f"# 新闻清单（近 {DAYS} 天，共 {len(items)} 条）")
    lines.append("")  # 空行
    for it in items:
        lines.append(f"- {it['date'].strftime('%m-%d %H:%M')} {it['title']}")
    with open(OUT_BRIEF, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

# —— 主流程 —— 
import asyncio

async def main():
    log(f"开始收集（近 {DAYS} 天），调用 Qwen 生成补充关键词")
    holds = load_holdings()
    log(f"读取持仓：holdings.json 共 {len(holds)} 条")

    sectors = infer_sectors(holds)
    base_kw = base_keywords_from_sectors(sectors)
    log(f"基础关键词 {len(base_kw)} 个；行业：" + ", ".join(sectors) if sectors else "无")

    # Qwen 扩展（可失败不阻断）
    qkw = await qwen_expand(base_kw)
    if qkw:
        log(f"Qwen 生成关键词 {len(qkw)} 个")
    all_kw = base_kw[:]  # 不强制合并，防止奇怪词
    # 合并扩展词，但保持中文短词
    for k in qkw:
        if k not in all_kw and 1 <= len(k) <= 4 and re.search(r"[\u4e00-\u9fa5]", k):
            all_kw.append(k)
    log(f"最终关键词 {len(all_kw)} 个，已写 keywords_used.txt / sources_used.txt")
    write_keywords(all_kw, qkw)

    # 抓取 RSS
    rss_items: List[dict] = []
    for name, url in RSS_SOURCES:
        items = fetch_rss(name, url)
        rss_items.extend(items)
    log(f"RSS 抓到 {len(rss_items)} 条（未筛）")

    # 时间窗口筛选
    since = datetime.now(TZ) - timedelta(days=DAYS)
    rss_items = [x for x in rss_items if x["date"] >= since]

    # 关键词筛（标题 + 摘要）
    hit_items = [x for x in rss_items if hit(x, all_kw)]
    # 统一排序（时间降序）
    hit_items.sort(key=lambda x: x["date"], reverse=True)

    log(f"收集完成：全量 {len(rss_items)} 条，命中 {len(hit_items)} 条（未去重）")

    # 写文件
    write_csv(hit_items)
    write_briefing(hit_items)

    # 错误文件大小
    try:
        sz = os.path.getsize(OUT_ERR) if os.path.isfile(OUT_ERR) else 0
    except Exception:
        sz = 0
    log(f"已写 briefing.txt、news_all.csv、keywords_used.txt、sources_used.txt")
    log(f"errors.log 大小 {sz} bytes")
    log("collector 任务完成")

if __name__ == "__main__":
    asyncio.run(main())
