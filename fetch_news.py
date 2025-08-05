# -*- coding: utf-8 -*-
"""
统一抓取国际财经 + 财新 + 新浪 -> news.json
"""
import os, json, datetime, hashlib, re, html as ihtml, requests, chardet
from bs4 import BeautifulSoup
from sumy.parsers.plaintext import PlaintextParser
from sumy.summarizers.text_rank import TextRankSummarizer
from sumy.nlp.tokenizers import Tokenizer

#### 0. 参数 ####
NEWSAPI_KEY    = os.getenv("NEWSAPI_KEY")
MEDIASTACK_KEY = os.getenv("MEDIASTACK_KEY")
MAX_PER_SRC    = 10                # 每源最多保留

# 宽泛+细分关键词（长于1字）
KWS = [
    "股票","股市","指数","融资","分红","回购","半导体","芯片",
    "人工智能","宏观","业绩","盈利","通胀","利率",
    "bull","bear","dividend","volatility","earnings","buyback",
    "semiconductor","inflation"
]
KWS = [k.lower() for k in KWS if len(k) > 1]

today, yest = datetime.date.today(), datetime.date.today() - datetime.timedelta(days=1)
news = []

def good(txt:str)->bool:
    txt = re.sub(r"[^\w\u4e00-\u9fa5]", "", txt.lower())
    return any(k in txt for k in KWS)

#### 1. 摘要工具 ####
def textrank(text:str, sent=1)->str:
    parser = PlaintextParser.from_string(text, Tokenizer("chinese"))
    summ   = TextRankSummarizer()(parser.document, sent)
    return " ".join(map(str, summ))

def snippet_from_html(html:str, css:str, limit=300)->str:
    s = BeautifulSoup(html, "lxml")
    paras = [p.get_text(" ", strip=True) for p in s.select(css)[:3]]
    raw   = " ".join(paras) or s.get_text(" ", strip=True)[:limit]
    raw   = ihtml.unescape(raw)
    return (raw + " " + textrank(raw))[:limit]

def tidy(resp:requests.Response)->str:
    if resp.encoding=="ISO-8859-1":
        resp.encoding = chardet.detect(resp.content)['encoding'] or "utf-8"
    return resp.text

def add(title, snippet, src, pub, origin):
    if len(snippet) < 20 or not good(title+snippet):   # 过滤无关 or 过短
        return
    hid = hashlib.md5((title+pub).encode()).hexdigest()
    news.append({
        "id": hid,
        "title": title.strip(),
        "snippet": snippet.strip(),
        "source": src,
        "published": pub[:10],
        "origin": origin
    })

#### 2. 国际：NewsAPI + Mediastack ####
dom_wh = ",".join(["bloomberg.com","reuters.com","ft.com","wsj.com",
                   "cnbc.com","marketwatch.com","finance.yahoo.com","seekingalpha.com"])

if NEWSAPI_KEY:
    for kw in ["stock market","earnings","dividend","china market"]:
        r = requests.get("https://newsapi.org/v2/everything",
            params={"q":kw,"language":"en","from":yest,"to":today,
                    "pageSize":MAX_PER_SRC,"sortBy":"publishedAt",
                    "apiKey":NEWSAPI_KEY,"domains":dom_wh},timeout=10)
        for a in r.json().get("articles",[]):
            add(a["title"], a.get("description","") or a["title"],
                a["source"]["name"], a["publishedAt"], "NEWSAPI")

if MEDIASTACK_KEY:
    for kw in ["股市","分红","半导体","宏观经济"]:
        r = requests.get("http://api.mediastack.com/v1/news",
            params={"access_key":MEDIASTACK_KEY,"keywords":kw,
                    "languages":"en,zh","domains":dom_wh,
                    "date":yest,"limit":MAX_PER_SRC},timeout=10)
        for a in r.json().get("data",[]):
            add(a["title"], a.get("description","") or a["title"],
                a["source"], a["published_at"], "MEDIASTACK")

#### 3. 国内：财新网 & 新浪财经 (RSSHub) ####
rss_list = [
    ("财新网", "https://rsshub.app/caixin/latest"),
    ("新浪财经", "https://rsshub.app/sina/finance")
]
ua = {"User-Agent":"Mozilla/5.0"}
for src, link in rss_list:
    try:
        xml = requests.get(link, headers=ua, timeout=10).text
        for it in BeautifulSoup(xml,"xml").find_all("item")[:MAX_PER_SRC]:
            title = it.title.get_text(strip=True)
            pub   = it.pubDate.get_text(strip=True) if it.pubDate else today.isoformat()
            link  = it.link.get_text(strip=True)
            # 再抓正文做 snippet
            art = requests.get(link, headers=ua, timeout=10)
            snp = snippet_from_html(tidy(art), "article p, div.article p, p")
            add(title, snp, src, pub, "CN_RSS")
    except Exception as e:
        print(src,"RSS失败:", e)

#### 4. 去重 & 输出 ####
unique = {n["id"]:n for n in news}.values()
with open("news.json","w",encoding="utf-8") as f:
    json.dump(list(unique), f, ensure_ascii=False, indent=2)

print(f"✅ 抓取完成：NewsAPI {sum(n['origin']=='NEWSAPI' for n in news)}, "
      f"MStack {sum(n['origin']=='MEDIASTACK' for n in news)}, "
      f"CN_RSS {sum(n['origin']=='CN_RSS' for n in news)} → "
      f"去重后 {len(unique)} 条写入 news.json")
