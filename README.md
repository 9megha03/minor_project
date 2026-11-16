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

