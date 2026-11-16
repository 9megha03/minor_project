"""
database.py
------------
Storage layer for the National Weather Big Data Analytics Platform.

Uses SQLite so the whole project runs with zero external setup.
The schema is written so it maps 1:1 onto PostgreSQL if you want to swap
it in for production — see README.md "Going to production".
"""

import sqlite3
import os
from contextlib import contextmanager

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "weather_platform.db")


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@contextmanager
def db_cursor(commit=False):
    conn = get_connection()
    try:
        cur = conn.cursor()
        yield cur
        if commit:
            conn.commit()
    finally:
        conn.close()


def init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with db_cursor(commit=True) as cur:
        # Raw, untouched data exactly as it arrived from a source.
        # This is the landing zone / "data lake" layer in the architecture diagram.
        cur.execute("""
        CREATE TABLE IF NOT EXISTS raw_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,                 -- weather_api | social_media | citizen_report
            raw_text TEXT NOT NULL,
            city TEXT,
            state TEXT,
            lat REAL,
            lon REAL,
            has_media INTEGER DEFAULT 0,
            event_time TEXT NOT NULL,
            ingested_at TEXT NOT NULL
        )
        """)

        # Cleaned, deduplicated, classified, verified data.
        # This is what the dashboard and admin panel read from.
        cur.execute("""
        CREATE TABLE IF NOT EXISTS processed_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            raw_id INTEGER NOT NULL REFERENCES raw_events(id),
            source TEXT NOT NULL,
            cleaned_text TEXT NOT NULL,
            city TEXT,
            state TEXT,
            lat REAL,
            lon REAL,
            has_media INTEGER DEFAULT 0,
            event_time TEXT NOT NULL,
            category TEXT NOT NULL,
            category_confidence REAL NOT NULL,
            content_hash TEXT NOT NULL,
            is_duplicate INTEGER DEFAULT 0,
            duplicate_of INTEGER,
            corroboration_count INTEGER DEFAULT 1,
            verification_score REAL NOT NULL,
            verification_status TEXT NOT NULL,     -- verified | unverified | flagged
            processed_at TEXT NOT NULL
        )
        """)

        # Every manual decision an admin makes, for audit trail.
        cur.execute("""
        CREATE TABLE IF NOT EXISTS admin_actions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            processed_id INTEGER NOT NULL REFERENCES processed_events(id),
            action TEXT NOT NULL,                  -- approve | reject
            actor TEXT NOT NULL,
            action_time TEXT NOT NULL
        )
        """)

        cur.execute("CREATE INDEX IF NOT EXISTS idx_processed_category ON processed_events(category)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_processed_status ON processed_events(verification_status)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_processed_hash ON processed_events(content_hash)")


def reset_db():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    init_db()


if __name__ == "__main__":
    reset_db()
    print(f"Initialized fresh database at {DB_PATH}")
