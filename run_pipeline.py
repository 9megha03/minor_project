"""
run_pipeline.py
----------------
Runs one full cycle of the architecture diagram:

    DATA SOURCES -> DATA INGESTION -> DATA PROCESSING -> STORAGE

Run this directly for a one-off batch:
    python run_pipeline.py

Or call `run_once()` from a scheduler (cron, APScheduler, Airflow) to make
the platform behave like a continuously-updating "real-time" system even
though the underlying sources here are simulated.
"""

from database import init_db
from data_ingestion import run_ingestion_batch
from data_processing import process_new_raw_events


def run_once():
    init_db()
    run_ingestion_batch()
    processed = process_new_raw_events()
    return processed


if __name__ == "__main__":
    count = run_once()
    print(f"Pipeline run complete. {count} new events processed and stored.")
