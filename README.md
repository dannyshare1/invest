
# Daily News Fetcher

Python scripts to fetch latest news from Caixin and Sina, extract the first paragraph as summary,
and output a **news.json** file for downstream processing / push notifications.

## Directory layout

```
.
├── fetchers/
│   ├── caixin_news_fetcher.py
│   ├── sina_news_fetcher.py
│   └── __init__.py
├── compile_news.py
├── requirements.txt
└── .github/
    └── workflows/
        └── daily_push.yml
```

## Usage

```bash
pip install -r requirements.txt
python compile_news.py
```

The generated `news.json` will contain fields:

* `title`
* `url`
* `summary`
* `source`
* `published_at`
* `fetched_at`
