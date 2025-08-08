# -*- coding: utf-8 -*-
"""
news_pipeline.py — Collector + LLM 关键词增强（覆盖旧脚本）

功能
  1) 读取 holdings.json，推断行业 → 生成基础中文关键词；
  2) 调用 Qwen 一次，让其基于持仓补充“中文行业/主题关键词”；记录到日志与文件；
  3) 用合并后的关键词，从 GDELT + RSS（路透/FT中文/界面/新浪）抓近 N 天资讯；
  4) 输出：
      - briefing.txt（每条：时间|来源|标题；下一行摘要；再下一行链接；条目之间空行；每次覆盖）
      - news_all.csv（原始汇总明细）
      - keywords_used.txt（基础词 + LLM 词，分别标注）
      - sources_used.txt（本次使用的源与行业）
      - pipeline.log（流程日志） / errors.log（错误日志，追加）

依赖： httpx, python-dateutil
备注：不依赖 bs4 / lxml，RSS 用 xml.etree 解析；不使用 Tushare。
"""
from __future__ import annotations

import asyncio, httpx, json, logging, os, re, sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Any, Tuple
from dateutil import parser as dtparse
import xml.etree.ElementTree as ET

# ─────────── 配置 ───────────
TZ = timezone(timedelta(hours=8))
NOW = datetime.now(TZ)
DAYS = int(os.getenv("COLLECT_DAYS", "3"))          # 近几天
QWEN_API = "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation"
QWEN_MODEL = os.getenv("QWEN_MODEL", "qwen-plus")
UA = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/124 Safari/537.36"
}

# 输出文件
BRIEF = Path("briefing.txt")
ALLCSV = Path("news_all.csv")
KWFILE = Path("keywords_used.txt")
SRCFILE = Path("sources_used.txt")
ERR = Path("errors.log"); ERR.touch(exist_ok=True)

# 日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("pipeline.log", mode="a", encoding="utf-8"),
              logging.StreamHandler(sys.stdout)],
)

def logerr(msg: str):
    logging.error(msg)
    with ERR.open("a", encoding="utf-8") as f:
        f.write(f"{datetime.now(TZ).isoformat()}  {msg}\n")

# ─────────── 数据结构 ───────────
class News:
    __slots__=("time","source","title","summary","url")
    def __init__(self, time: datetime, source: str, title: str, summary: str, url: str):
        self.time=time; self.source=source; self.title=title; self.summary=summary; self.url=url
    def row(self) -> List[str]:
        # 简易 CSV 转义
        def esc(x:str)->str: return '"' + x.replace('"','""').replace("\n"," ").strip() + '"'
        t=self.time.astimezone(TZ).isoformat(timespec="seconds")
        return [esc(t),esc(self.source),esc(self.title),esc(self.summary),esc(self.url)]

# ─────────── 持仓 / 关键词 ───────────
def load_holdings() -> List[Dict[str, Any]]:
    for p in (Path("holdings.json"), Path("holding.json"), Path("holding.jason")):
        if p.is_file():
            try:
                data=json.loads(p.read_text("utf-8"))
                logging.info(f"读取持仓：{p.name} 共 {len(data)} 条")
                return data
            except Exception as e:
                logerr(f"读取 {p} 失败: {type(e).__name__}: {e}")
    logging.warning("未找到 holdings.json，后续仅使用通用宏观关键词")
    return []

# 行业映射与基础关键词库（可自行扩展）
ETF_TO_INDUSTRY = [
    (r"半导体", "半导体"),
    (r"医药|医疗", "医药"),
    (r"酒", "白酒"),
    (r"沪深300|上证50|中证|红利|宽基", "宏观"),
    (r"国债|债", "债券"),
    (r"豆粕|农业|农产品|大豆|玉米", "农业"),
]

BASE_KWS: Dict[str, List[str]] = {
    "半导体": ["半导体","芯片","晶圆代工","封测","光刻机","刻蚀机","存储芯片","HBM","GPU","EDA","硅片","车规芯片","功率半导体","碳化硅","氮化镓","集成电路","AI芯片"],
    "医药": ["医药","创新药","仿制药","集采","PD-1","GLP-1","疫苗","医疗器械","高值耗材","CXO","医保谈判","MAH","生物类似药","基因治疗","细胞治疗"],
    "白酒": ["白酒","动销","批价","渠道","高端白酒","次高端","库存","消费复苏"],
    "宏观": ["通胀","通缩","利率","降息","加息","汇率","PMI","社融","M2","财政政策","货币政策","逆回购","央行","经济复苏"],
    "债券": ["国债","地方债","信用债","利率互换","收益率曲线","流动性","中长期利率"],
    "农业": ["豆粕","大豆","玉米","饲料","油脂油料","生猪","供需","库存"],
}

def infer_industries(holds: List[Dict[str, Any]]) -> List[str]:
    tags=set()
    for h in holds:
        base=f"{h.get('name','')}-{h.get('symbol','')}"
        for pat,tag in ETF_TO_INDUSTRY:
            if re.search(pat, base):
                tags.add(tag)
    if not tags: tags.add("宏观")
    return sorted(tags)

def build_base_keywords(holds: List[Dict[str, Any]]) -> Tuple[List[str], List[str]]:
    inds=infer_industries(holds)
    kws=[]
    for ind in inds:
        kws.extend(BASE_KWS.get(ind, []))
    # 去重
    seen=set(); base=[]
    for k in kws:
        if k and (k not in seen):
            seen.add(k); base.append(k)
    logging.info(f"基础关键词 {len(base)} 个；行业：{', '.join(inds)}")
    return inds, base

# ─────────── 调 Qwen 生成补充关键词（一次） ───────────
async def qwen_extra_keywords(holds: List[Dict[str, Any]]) -> List[str]:
    api_key=os.getenv("QWEN_API_KEY")
    if not api_key:
        logging.warning("QWEN_API_KEY 未设置，跳过 LLM 关键词增强")
        return []
    # 构造持仓文本
    lines=[]
    for h in holds:
        nm=h.get("name",""); sym=h.get("symbol",""); wt=h.get("weight",0)
        lines.append(f"{nm} ({sym}) 权重 {wt}")
    holds_txt="\n".join(lines) if lines else "(空)"
    prompt=(
        "你是资深行业研究助理。请基于以下持仓（可能是宽基/行业ETF），输出一份“中文行业与主题关键词”清单，"
        "用于去资讯源检索近几天的新闻。要求：\n"
        "1) 仅输出关键词，每行一个，禁止代码、英文缩写、上市公司代码；\n"
        "2) 优先与半导体、医药、白酒、宏观、债券、农业等行业相关；\n"
        "3) 覆盖产业链要点（上游材料/设备、中游制造、下游应用）、政策、价格与供需、竞争、并购、监管等；\n"
        "4) 数量控制在 40~120 个。\n\n"
        f"【持仓】\n{holds_txt}\n\n"
        "只输出换行分隔的中文关键词列表："
    )
    payload={"model":QWEN_MODEL, "input":{"prompt":prompt}, "parameters":{"max_tokens":800,"temperature":0.3}}
    headers={"Content-Type":"application/json","Authorization":f"Bearer {api_key}"}

    async with httpx.AsyncClient(timeout=60) as c:
        for i in range(3):
            try:
                r=await c.post(QWEN_API, json=payload, headers=headers)
                r.raise_for_status()
                text=(r.json().get("output",{}) or {}).get("text","")
                raw=[x.strip() for x in re.split(r"[\n,，；;]", text) if x.strip()]
                # 仅保留“主要为中文、长度 2~12”的词
                out=[]
                seen=set()
                for w in raw:
                    if 2 <= len(w) <= 12 and re.search(r"[\u4e00-\u9fff]", w):
                        w=re.sub(r"[^0-9\u4e00-\u9fffA-Za-z\-\+ ]","",w)
                        if w and (w not in seen):
                            seen.add(w); out.append(w)
                logging.info(f"Qwen 生成关键词 {len(out)} 个")
                return out
            except Exception as e:
                logerr(f"Qwen 关键词调用失败 {i+1}/3: {type(e).__name__}: {e}")
                await asyncio.sleep(2*(i+1))
    return []

def write_keyword_files(inds: List[str], base: List[str], extra: List[str]):
    # 合并去重
    seen=set(); merged=[]
    for w in (base + extra):
        if w and (w not in seen):
            seen.add(w); merged.append(w)
    # 明细写入
    KWFILE.write_text(
        "【基础关键词】\n" + "\n".join(base) + "\n\n" +
        "【LLM补充关键词】\n" + ("\n".join(extra) if extra else "(空)") + "\n\n" +
        "【最终用于检索】\n" + "\n".join(merged) + "\n",
        encoding="utf-8"
    )
    SRCFILE.write_text(
        "industries: " + ", ".join(inds) + "\n" +
        "sources: GDELT Doc API, Reuters RSS, FT中文RSS, 界面RSS, 新浪RSS\n",
        encoding="utf-8"
    )
    logging.info(f"最终关键词 {len(merged)} 个，已写 keywords_used.txt / sources_used.txt")
    return merged

# ─────────── 抓取源 ───────────
RSS_LIST = [
    ("Reuters", "https://feeds.reuters.com/reuters/businessNews"),
    ("FT中文", "https://www.ftchinese.com/rss/news"),
    ("界面新闻", "https://a.jiemian.com/index.php?m=article&a=rss"),
    ("新浪财经", "https://rss.sina.com.cn/roll/finance/hot_roll.xml"),
]

def _one_line(x: str) -> str:
    x = re.sub(r"<[^>]+>", " ", x)
    x = re.sub(r"\s+", " ", x)
    return x.strip()

async def gdelt_search(client: httpx.AsyncClient, keywords: List[str], days: int) -> List[News]:
    out: List[News]=[]
    span=f"{max(1,min(7,days))}d"
    groups=[keywords[i:i+5] for i in range(0,len(keywords),5)] or [[]]
    for grp in groups:
        if not grp: continue
        q=" OR ".join([f'"{k}"' for k in grp])
        query=f"({q}) AND (sourcelang:zh OR sourcelang:en)"
        params={"query":query,"timespan":span,"mode":"ArtList","maxrecords":"50","format":"json"}
        try:
            r=await client.get("https://api.gdeltproject.org/api/v2/doc/doc", params=params)
            r.raise_for_status()
            js=r.json(); arts=js.get("articles",[])
            for a in arts:
                tstr=a.get("seendate") or ""
                try: t=dtparse.parse(tstr).astimezone(TZ)
                except Exception: t=NOW
                out.append(News(
                    time=t,
                    source=a.get("sourceCommonName") or a.get("domain") or "GDELT",
                    title=(a.get("title") or "").strip(),
                    summary=(a.get("language") or "").strip(),
                    url=a.get("url") or ""
                ))
        except Exception as e:
            logerr(f"GDELT 批次 {grp[:2]}.. 失败: {type(e).__name__}: {e}")
        await asyncio.sleep(0.2)
    logging.info(f"GDELT 抓到 {len(out)} 条")
    return out

async def fetch_rss(client: httpx.AsyncClient, name: str, url: str, days: int) -> List[News]:
    try:
        r=await client.get(url, headers=UA, timeout=30)
        r.raise_for_status()
        root=ET.fromstring(r.text)
        items=root.findall(".//item")
        out=[]; since=NOW - timedelta(days=days)
        for it in items:
            ttl=(it.findtext("title") or "").strip()
            lk=(it.findtext("link") or "").strip()
            desc=(it.findtext("description") or "").strip()
            pd=it.findtext("pubDate") or ""
            try: t=dtparse.parse(pd).astimezone(TZ) if pd else NOW
            except Exception: t=NOW
            if t >= since:
                out.append(News(time=t, source=name, title=ttl, summary=_one_line(desc), url=lk))
        logging.info(f"RSS {name} 抓到 {len(out)} 条")
        return out
    except Exception as e:
        logerr(f"RSS {name} 失败: {type(e).__name__}: {e}")
        return []

def hit_keywords(n: News, kws: List[str]) -> bool:
    hay=(n.title + " " + n.summary).lower()
    return any(k.lower() in hay for k in kws)

# ─────────── 主流程 ───────────
async def collect(days:int)->List[News]:
    holds=load_holdings()
    inds, base = build_base_keywords(holds)
    extra = await qwen_extra_keywords(holds)
    kws = write_keyword_files(inds, base, extra)

    all_news: List[News]=[]
    async with httpx.AsyncClient(headers=UA, follow_redirects=True, timeout=30) as client:
        # GDELT（按关键词）
        all_news += await gdelt_search(client, kws, days)
        # RSS（抓后再按关键词过滤）
        rsstasks=[fetch_rss(client, n, u, days) for n,u in RSS_LIST]
        results=await asyncio.gather(*rsstasks, return_exceptions=True)
        for res in results:
            if isinstance(res, Exception):
                logerr(f"并发 RSS 任务异常: {type(res).__name__}: {res}")
                continue
            all_news += [n for n in res if hit_keywords(n, kws)]
    all_news.sort(key=lambda x:x.time, reverse=True)
    return all_news

def write_outputs(items: List[News]):
    # CSV
    with ALLCSV.open("w", encoding="utf-8") as f:
        f.write("time,source,title,summary,url\n")
        for n in items:
            f.write(",".join(n.row())+"\n")
    # briefing.txt（标题一行、摘要一行、链接一行、空行分隔）
    lines=[]
    for n in items:
        tstr=n.time.strftime("%Y-%m-%d %H:%M")
        lines.append(f"[{tstr}] {n.source}  {n.title}".strip())
        if n.summary: lines.append(n.summary)
        if n.url: lines.append(n.url)
        lines.append("")
    BRIEF.write_text("\n".join(lines).strip()+"\n", encoding="utf-8")
    logging.info(f"已写 briefing.txt（{len(items)} 条）、news_all.csv、keywords_used.txt、sources_used.txt")
    logging.info(f"errors.log 大小 {ERR.stat().st_size} bytes")

def main():
    try:
        logging.info(f"开始收集（近 {DAYS} 天），调用 Qwen 生成补充关键词")
        items=asyncio.run(collect(DAYS))
        logging.info(f"收集完成，共 {len(items)} 条（未去重）")
        write_outputs(items)
        logging.info("collector 任务完成")
    except Exception as e:
        logerr(f"主流程异常: {type(e).__name__}: {e}")
        raise

if __name__=="__main__":
    main()
