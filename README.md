# National Weather Big Data Analytics Platform (SIH PS 26069)

A working, runnable implementation of the Ministry of Earth Sciences / IMD
problem statement: collect weather-related reports from multiple sources,
clean and deduplicate them, classify the event type with ML, score how
trustworthy each report is, store everything centrally, and surface it on
a dashboard + admin panel.

Real Twitter/IMD API access, Kafka, and PostgreSQL are **simulated** so the
whole thing runs on your laptop with no accounts, no API keys, and no
external services — see "Going to production" below for how each mock
maps onto the real infrastructure named in your architecture diagram.

## Architecture -> code map

```
DATA SOURCES (Weather APIs, News/Social, Citizen Reports)
        -> data_ingestion.py        (generates realistic mock records)
DATA INGESTION (Python/Kafka)
        -> raw_events table          (the "landing zone")
DATA PROCESSING
    Clean Data          -> data_processing.py: clean_text()
    Remove Duplicates   -> data_processing.py: content_hash() + dedup logic
    NLP/AI Classification -> data_processing.py: EventClassifier (TF-IDF + Naive Bayes)
    Verification Engine -> data_processing.py: verification_score()
STORAGE (PostgreSQL / Big Data Storage)
        -> database.py: processed_events table (SQLite standing in for Postgres)
ANALYTICS PROCESSING
        -> app.py: /api/stats, /api/categories, /api/timeseries
Web Dashboard   -> templates/dashboard.html + static/js/dashboard.js
Admin Panel     -> templates/admin.html + static/js/admin.js
```

## Step-by-step: run it

**1. Install Python 3.10+** if you don't already have it.

**2. Open a terminal in this folder** and create a virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate        # on Windows: venv\Scripts\activate
```

**3. Install dependencies:**
```bash
pip install -r requirements.txt
```

**4. Generate an initial batch of data and run it through the pipeline:**
```bash
python run_pipeline.py
```
You should see: `Pipeline run complete. NN new events processed and stored.`
Run this command again a couple more times if you want more sample data —
each run adds a fresh batch on top of what's already there.

**5. Start the web app:**
```bash
python app.py
```

**6. Open your browser:**
- Dashboard: http://127.0.0.1:5000/
- Admin review queue: http://127.0.0.1:5000/admin

**7. Try it out:**
- Click **"Pull latest reports"** on the dashboard — this simulates a new
  wave of incoming data arriving, runs it through the full pipeline, and
  refreshes the map, charts and table live.
- Filter the event table by category or verification status.
- Go to **Admin** and Approve/Reject the reports sitting in the manual
  review queue (these are the ones the verification engine wasn't
  confident enough about to auto-verify or auto-flag).

That's the whole loop: sources -> ingestion -> cleaning -> dedup ->
classification -> verification -> storage -> dashboard -> admin action.

## Project structure

```
weather-analytics-platform/
├── app.py                  Flask web server + REST API
├── database.py             SQLite schema (raw_events, processed_events, admin_actions)
├── data_ingestion.py       Simulated Weather API / Social Media / Citizen Report sources
├── data_processing.py      Cleaning, dedup, ML classifier, verification engine
├── run_pipeline.py         Orchestrates one full ingestion -> processing cycle
├── requirements.txt
├── data/                   weather_platform.db lives here (created on first run)
├── templates/
│   ├── dashboard.html      Public map + charts + event feed
│   └── admin.html          Manual verification queue
└── static/
    ├── css/style.css
    └── js/
        ├── dashboard.js
        └── admin.js
```

## How verification scoring works

Each report gets a trust score from 0 to 1, built from:
- **Source**: official weather-API alerts start highest, citizen reports
  next, raw social posts lowest.
- **Corroboration**: 2+ independent reports about the same thing in the
  same place add confidence (this is the "multiple reports confirm it"
  signal your PS description asks for).
- **Media attached**: a photo/video bumps the score up slightly.
- **Spam signals**: known spam phrases, an all-caps/exclamation-heavy
  post, or the classifier itself tagging it "Other/Spam" pull the score
  down.

Score >= 0.55 -> **verified** automatically. Score 0.3–0.55 -> **unverified**,
sits in the admin queue. Below 0.3 -> **flagged**, also sits in the queue
(shown first, since it's most likely spam/fake).

## Going to production

Everything here is written so each mock has an obvious real-world
replacement:

| In this project | Swap in for production |
|---|---|
| `data_ingestion.py` mock generators | Real IMD/OpenWeather API polling, a Twitter/X + news RSS listener filtering `#IMD`, a citizen-reporting mobile app upload endpoint |
| Direct function calls into `raw_events` | A Kafka topic (`raw_ingestion_topic`); ingestion services produce to it, a consumer calls the same processing functions |
| SQLite (`database.py`) | PostgreSQL — the schema in `database.py` is deliberately plain SQL that ports over almost unchanged; for very large scale, add a columnar store (e.g. ClickHouse) for the analytics queries |
| `EventClassifier` (TF-IDF + Naive Bayes) | A larger labelled dataset + a transformer-based classifier (e.g. a fine-tuned BERT) for better accuracy on informal social-media text |
| Exact-hash dedup | Embedding-based semantic similarity (cosine similarity on sentence embeddings) to catch near-duplicate reports worded differently |
| Flask dev server | Gunicorn/uWSGI behind Nginx, or a managed platform |

## Notes for your SIH submission

- The "AI/ML" requirement is satisfied by the NLP event classifier
  (`EventClassifier`) and the rule-based verification engine — both are
  real, running code, not placeholders.
- "Real-time" is simulated via the **Pull latest reports** button; wiring
  `run_pipeline.run_once()` to a scheduler (cron, APScheduler, or a Kafka
  consumer loop) makes it genuinely continuous with no other code changes.
- All sample data is synthetic — safe to demo without any real citizen
  data or API keys.
