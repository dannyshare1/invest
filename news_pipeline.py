# -*- coding: utf-8 -*-
"""
news_pipeline.py — 资讯收集器（无去重，修正编码/乱码，详细错误日志）
- 从 holdings.json 读取持仓，提取行业 -> 基础关键词
-（可选）用 Qwen 扩展关键词（开启需设置 QWEN_API_KEY）
- 抓取若干 RSS（中英），仅按关键词筛选（近 N 天，不依赖 Tushare）
- 写出：
  - news_all.csv（全量，不去重）
  - briefing.txt（用于推送，标题与摘要分行）
  - keywords_used.txt / sources_used.txt
  - errors.log（详细错误）
"""
from __future__ import annotations
import asyncio, os, json, csv, re, textwrap
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict
import httpx
from html import unescape

# 可选：更可靠的编码探测（pip install charset-normalizer）
try:
    from charset_normalizer import from_bytes as detect_bytes
except Exception:
    detect_bytes = None

TZ = timezone(timedelta(hours=8))
ROOT = Path(".")
OUT_CSV = ROOT / "news_all.csv"
OUT_BRIEF = ROOT / "briefing.txt"
OUT_KW = ROOT / "keywords_used.txt"
OUT_SRC = ROOT / "sources_used.txt"
ERR_LOG = ROOT / "errors.log"

# ============ 工具 ============
def now_str():
    return datetime.now(TZ).isoformat()

def logerr(msg: str):
    ERR_LOG.parent.mkdir(parents=True, exist_ok=True)
    with ERR_LOG.open("a", encoding="utf-8") as f:
        f.write(f"{now_str()}  {msg}\n")

def clean_text(s: str) -> str:
    if not s:
        return ""
    s = unescape(s)
    s = re.sub(r"<[^>]+>", " ", s)         # 去 HTML 标签
    s = re.sub(r"&[a-zA-Z#0-9]+;", " ", s) # 避免残留实体
    s = re.sub(r"\s+", " ", s).strip()
    return s

def safe_decode(content: bytes, fallback_enc: str | None = None) -> str:
    if detect_bytes:
        try:
            m = detect_bytes(content).best()
            if m and m.encoding:
                return content.decode(m.encoding, errors="ignore")
        except Exception as e:
            logerr(f"charset-normalizer fail: {e!r}")
    # 回退（utf-8 -> gb18030）
    for enc in ([fallback_enc] if fallback_enc else []) + ["utf-8", "gb18030"]:
        try:
            return content.decode(enc, errors="ignore")
        except Exception:
            pass
    return content.decode("utf-8", errors="ignore")

async def fetch_text(client: httpx.AsyncClient, url: str, timeout: int = 20) -> str:
    r = await client.get(url, timeout=timeout)
    r.raise_for_status()
    # 有些源会带 charset header，但保险起见走我们自己的探测
    return safe_decode(r.content, r.encoding)

# ============ 持仓 & 行业 ============

def load_holdings() -> List[Dict]:
    p = Path("holdings.json")
    if not p.is_file():
        logerr("holdings.json 不存在")
        return []
    try:
        return json.loads(p.read_text("utf-8"))
    except Exception as e:
        logerr(f"holdings.json 解析失败: {e!r}")
        return []

# 极简行业映射（从持仓 name 推断）
def infer_industries(holds: List[Dict]) -> List[str]:
    m = {
        "半导体": "半导体",
        "医药": "医药",
        "酒": "白酒",
        "国债": "债券",
        "沪深300": "宏观",
        "豆粕": "农业",
    }
    out = set()
    for h in holds:
        name = (h.get("name") or "") + (h.get("symbol") or "")
        for k, v in m.items():
            if k in name:
                out.add(v)
    if not out:
        out.add("宏观")
    return sorted(out)

def base_keywords_from_industries(inds: List[str]) -> List[str]:
    base = []
    if "债券" in inds:
        base += ["国债","地方债","信用债","利率","利率互换","收益率曲线","流动性","中长期利率","逆回购","央行"]
    if "农业" in inds:
        base += ["豆粕","大豆","玉米","饲料","油脂油料","生猪","供需","库存","农产品价格"]
    if "医药" in inds:
        base += ["医药","创新药","仿制药","集采","PD-1","GLP-1","疫苗","医疗器械","高值耗材","CXO","医保谈判","MAH","生物类似药","基因治疗","细胞治疗"]
    if "半导体" in inds:
        base += ["半导体","芯片","晶圆代工","封测","光刻机","刻蚀机","存储芯片","HBM","GPU","EDA","硅片","车规芯片","功率半导体","碳化硅","氮化镓","集成电路","AI芯片"]
    if "白酒" in inds:
        base += ["白酒","白酒消费升级","渠道","价格","供需","动销"]
    if "宏观" in inds:
        base += ["通胀","通缩","降息","加息","汇率","PMI","社融","M2","财政政策","货币政策","经济复苏"]
    # 去重
    return sorted(set(base))

# ============ LLM（可选）扩展关键词 ============
QWEN_API = "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation"
QWEN_MODEL = "qwen-plus"

async def call_qwen_keywords(client: httpx.AsyncClient, industries: List[str], seeds: List[str]) -> List[str]:
    if not os.getenv("QWEN_API_KEY"):
        return []
    prompt = textwrap.dedent(f"""
        你是资讯检索助手。基于这些行业：{", ".join(industries)}，
        和初始关键词：{", ".join(seeds[:60])}
        扩展出 60~100 个**中文关键词**（仅名词或短语，每行一个，不要编号、不要解释）。
    """).strip()
    hdr = {
        "Authorization": f"Bearer {os.getenv('QWEN_API_KEY')}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": QWEN_MODEL,
        "input": {"prompt": prompt},
        "parameters": {"max_tokens": 800, "temperature": 0.2},
    }
    r = await client.post(QWEN_API, headers=hdr, json=payload, timeout=60)
    r.raise_for_status()
    txt = (r.json().get("output") or {}).get("text", "")
    kws = [clean_text(x) for x in txt.splitlines() if clean_text(x)]
    # 仅保留中文或包含大写缩写的词
    kws = [k for k in kws if re.search(r"[\u4e00-\u9fffA-Z]", k)]
    return kws[:100]

# ============ RSS 抓取（关键词过滤） ============

RSS_SOURCES = [
    ("FT中文", "https://www.ftchinese.com/rss/news"),
    ("界面新闻", "https://a.jiemian.com/index.php?m=article&a=rss"),
    ("新浪财经", "https://rss.sina.com.cn/roll/finance/hot_roll.xml"),  # 可能 gbk/gb2312
]

def rss_extract(xml_text: str, source: str) -> List[Dict]:
    # 粗暴解析：title/description/link/pubDate
    items = []
    # 兼容 <item> 或 <entry>
    blocks = re.split(r"</item>|</entry>", xml_text, flags=re.I)
    for b in blocks:
        t = clean_text(re.search(r"<title[^>]*>(.*?)</title>", b, re.I|re.S).group(1)) if re.search(r"<title", b, re.I) else ""
        d = clean_text(re.search(r"<description[^>]*>(.*?)</description>|<summary[^>]*>(.*?)</summary>", b, re.I|re.S).group(1) if re.search(r"<description|<summary", b, re.I) else "")
        u = clean_text(re.search(r"<link[^>]*>(.*?)</link>|<link[^>]*href=[\"']([^\"']+)[\"']", b, re.I|re.S).group(1) if re.search(r"<link", b, re.I) else "")
        p = clean_text(re.search(r"<pubDate[^>]*>(.*?)</pubDate>|<updated[^>]*>(.*?)</updated>", b, re.I|re.S).group(1) if re.search(r"<pubDate|<updated", b, re.I) else "")
        if t:
            items.append({"source": source, "title": t, "summary": d, "url": u, "published_at": p})
    return items

def match_keywords(title: str, summary: str, kws: List[str]) -> bool:
    text = f"{title} {summary}"
    return any(k in text for k in kws)

# ============ 主流程 ============

async def main(days: int = 3, max_items_per_src: int = 200):
    print(f"{now_str()} - INFO - 开始收集（近 {days} 天），调用 Qwen 生成补充关键词")
    holds = load_holdings()
    print(f"{now_str()} - INFO - 读取持仓：holdings.json 共 {len(holds)} 条")
    industries = infer_industries(holds)
    seeds = base_keywords_from_industries(industries)
    print(f"{now_str()} - INFO - 基础关键词 {len(seeds)} 个；行业：{', '.join(industries)}")

    async with httpx.AsyncClient(follow_redirects=True, timeout=20) as client:
        extra = await call_qwen_keywords(client, industries, seeds)
        if extra:
            print(f"{now_str()} - INFO - Qwen 生成关键词 {len(extra)} 个")
        all_kw = sorted(set(seeds + extra))
        OUT_KW.write_text(
            "【基础关键词】\n" + "\n".join(seeds) + "\n\n【LLM补充关键词】\n" + "\n".join(extra) + "\n\n【最终用于检索】\n" + "\n".join(all_kw),
            encoding="utf-8"
        )
        OUT_SRC.write_text("\n".join([s for s,_ in RSS_SOURCES]), encoding="utf-8")
        print(f"{now_str()} - INFO - 最终关键词 {len(all_kw)} 个，已写 keywords_used.txt / sources_used.txt")

        collected: List[Dict] = []
        for name, url in RSS_SOURCES:
            try:
                txt = await fetch_text(client, url)
                items = rss_extract(txt, name)
                # 仅保留命中关键词的，且尽量靠近近 days 的（RSS 大多是最新）
                for it in items[:max_items_per_src]:
                    if match_keywords(it["title"], it["summary"], all_kw):
                        collected.append(it)
                print(f"{now_str()} - INFO - RSS {name} 抓到 {len(items)} 条，命中 {sum(match_keywords(i['title'], i['summary'], all_kw) for i in items)} 条")
            except Exception as e:
                print(f"{now_str()} - ERROR - RSS {name} 失败: {e.__class__.__name__}: {e}")
                logerr(f"RSS {name} 失败: {e.__class__.__name__}: {e!r}")

    # 写 CSV（不去重）
    cols = ["published_at", "source", "title", "summary", "url"]
    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for it in collected:
            w.writerow({k: it.get(k, "") for k in cols})

    # briefing（标题/摘要分行）
    lines = [f"# 今日新闻（命中关键词，共 {len(collected)} 条）", ""]
    for i, it in enumerate(collected, 1):
        lines.append(f"{i}. **{it['title']}**  [{it['source']}]({it['url']})")
        if it.get("summary"):
            lines.append(it["summary"])
        lines.append("")
    OUT_BRIEF.write_text("\n".join(lines), encoding="utf-8")

    print(f"{now_str()} - INFO - 收集完成，共 {len(collected)} 条（未去重）")
    print(f"{now_str()} - INFO - 已写 briefing.txt、news_all.csv、keywords_used.txt、sources_used.txt")
    print(f"{now_str()} - INFO - errors.log 大小 {ERR_LOG.stat().st_size if ERR_LOG.exists() else 0} bytes")
    print(f"{now_str()} - INFO - collector 任务完成")

if __name__ == "__main__":
    asyncio.run(main())
