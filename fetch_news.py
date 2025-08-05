# -*- coding: utf-8 -*-
"""
NewsAPI + Mediastack + 财新(官方JSON) + 新浪财经(官方JSON) → news.json（含摘要）
"""
import os, json, datetime, hashlib, re, html as ihtml, requests, chardet, traceback
from bs4 import BeautifulSoup
from sumy.parsers.plaintext import PlaintextParser
from sumy.summarizers.text_rank import TextRankSummarizer
from sumy.nlp.tokenizers import Tokenizer

# ========== 基础设置 ==========
NEWS_KEY  = os.getenv("NEWSAPI_KEY")
MS_KEY    = os.getenv("MEDIASTACK_KEY")
MAX_EACH  = 10          # 每源最多条数
TIMEOUT   = 10

today  = datetime.date.today()
yest   = today - datetime.timedelta(days=1)
ua_hdr = {"User-Agent": "Mozilla/5.0"}

# 关键词（≥2 字／字母）
INV_KWS = [w.lower() for w in
  "股市 股票 指数 证券 融资 分红 回购 半导体 芯片 AI 宏观 业绩 盈利 通胀 利率 bull bear dividend buyback earnings volatility semiconductor inflation".split()]

def is_relevant(txt: str) -> bool:
    txt = re.sub(r"[^\w\u4e00-\u9fa5]", "", txt.lower())
    return any(k in txt for k in INV_KWS)

# ========== 通用摘要 ==========
def clean_html(h: str) -> str:
    soup = BeautifulSoup(h, "lxml")
    for t in soup(["script", "style", "aside", "footer"]):
        t.decompose()
    return soup.get_text(" ", strip=True)

def snippet(h: str, css: str, cap: int = 300) -> str:
    s  = BeautifulSoup(h, "lxml")
    seg = [p.get_text(" ", strip=True) for p in s.select(css)[:3]]
    base = " ".join(seg) or clean_html(h)[:cap]
    # TextRank 再压一句
    pt  = PlaintextParser.from_string(base, Tokenizer("chinese"))
    tex = TextRankSummarizer()(pt.document, 1)
    full = (base + " " + " ".join(map(str, tex)))[:cap]
    return ihtml.unescape(full)

def tidy(resp: requests.Response) -> str:
    if resp.encoding == "ISO-8859-1":
        resp.encoding = chardet.detect(resp.content)["encoding"] or "utf-8"
    return resp.text

def sha(title: str, date: str) -> str:
    return hashlib.md5(f"{title}_{date}".encode()).hexdigest()

items = []

def add(title, desc, src, date, origin):
    if not title or not desc or len(desc) < 30:
        return
    if not is_relevant(title + desc):
        return
    items.append({
        "id": sha(title, date),
        "title": title.strip(),
        "snippet": desc.strip(),
        "source": src,
        "published": date[:10],
        "origin": origin
    })

# ========== 1. NewsAPI ==========
dom_whitelist = ",".join([
    "bloomberg.com","reuters.com","ft.com","wsj.com",
    "cnbc.com","marketwatch.com","finance.yahoo.com","seekingalpha.com"
])
if NEWS_KEY:
    for q in ["stock market", "earnings", "dividend", "china chip"]:
        r = requests.get("https://newsapi.org/v2/everything",
                         params={
                             "q": q, "language": "en",
                             "from": yest, "to": today,
                             "pageSize": MAX_EACH, "domains": dom_whitelist,
                             "sortBy": "publishedAt", "apiKey": NEWS_KEY
                         }, timeout=TIMEOUT)
        if r.status_code == 429:
            print("NewsAPI hit daily limit, skip")
            break
        for art in r.json().get("articles", []):
            add(art["title"], art.get("description") or art["title"],
                art["source"]["name"], art["publishedAt"], "NEWSAPI")

# ========== 2. Mediastack ==========
if MS_KEY:
    for q in ["股市", "分红", "半导体", "宏观经济"]:
        r = requests.get("http://api.mediastack.com/v1/news",
                         params={
                             "access_key": MS_KEY, "keywords": q,
                             "languages": "en,zh",
                             "domains": dom_whitelist,
                             "date": yest, "limit": MAX_EACH
                         }, timeout=TIMEOUT)
        if r.status_code != 200:
            print("Mediastack error:", r.text); continue
        for art in r.json().get("data", []):
            add(art["title"], art.get("description") or art["title"],
                art["source"], art["published_at"], "MEDIASTACK")

# ========== 3. 财新官方 JSON ==========
try:
    j = requests.get("https://datanews.caixin.com/m/api/headline?limit=20",
                     headers=ua_hdr, timeout=TIMEOUT).json()
    cnt = 0
    for art in j.get("data", [])[:MAX_EACH]:
        link = art["url"]
        cont = requests.get(link, headers=ua_hdr, timeout=TIMEOUT)
        desc = snippet(tidy(cont), "article p, div.article p")
        add(art["title"], desc, "财新网", art["time"], "CN_JSON")
        cnt += 1
    print("Caixin JSON:", cnt)
except Exception:
    print("Caixin JSON failed:", traceback.format_exc()[:200])

# ========== 4. 新浪财经 JSON ==========
try:
    j = requests.get("https://feed.sina.com.cn/api/roll/get",
                     params={"pageid":155, "lid":1686, "num":40},
                     timeout=TIMEOUT).json()
    cnt = 0
    for art in j["result"]["data"]:
        if cnt >= MAX_EACH: break
        link = art["url"]
        if not link.startswith("https://finance.sina.com.cn/"): continue
        page = requests.get(link, headers=ua_hdr, timeout=TIMEOUT)
        desc = snippet(tidy(page), "article p, div.article p, p")
        add(art["title"], desc, "新浪财经", art["ctime"], "CN_JSON")
        cnt += 1
    print("Sina JSON:", cnt)
except Exception:
    print("Sina JSON failed:", traceback.format_exc()[:200])

# ========== 去重 & 保存 ==========
uniq = {x["id"]: x for x in items}.values()
with open("news.json", "w", encoding="utf-8") as f:
    json.dump(list(uniq), f, ensure_ascii=False, indent=2)

print(f"✅ NewsAPI {sum(i['origin']=='NEWSAPI' for i in items)}, "
      f"MStack {sum(i['origin']=='MEDIASTACK' for i in items)}, "
      f"CN_JSON {sum(i['origin']=='CN_JSON' for i in items)} → "
      f"保留 {len(uniq)} 条")
