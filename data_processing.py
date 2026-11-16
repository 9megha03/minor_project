"""
data_processing.py
-------------------
Implements the DATA PROCESSING block of the architecture diagram:

    Clean Data -> Remove Duplicates -> NLP/AI Event Classification
               -> Verification Engine -> (stored in processed_events)

Classification uses a small TF-IDF + Naive Bayes text classifier
(scikit-learn) trained on a seed set of labelled examples — this is the
"NLP/AI" box. It's intentionally lightweight so it trains in under a
second with no GPU/internet, but the interface (`classify(text)`) is the
same shape you'd use for a bigger transformer model later.
"""

import re
import hashlib
from datetime import datetime

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.naive_bayes import MultinomialNB

from database import db_cursor

# ---------------------------------------------------------------------------
# 1. CLEANING
# ---------------------------------------------------------------------------

URL_RE = re.compile(r"http\S+|www\.\S+")
HASHTAG_RE = re.compile(r"#\w+")
NON_ALNUM_RE = re.compile(r"[^a-z0-9\s]")
MULTI_SPACE_RE = re.compile(r"\s+")


def clean_text(raw_text: str) -> str:
    text = raw_text.lower()
    text = URL_RE.sub(" ", text)
    text = HASHTAG_RE.sub(" ", text)
    text = NON_ALNUM_RE.sub(" ", text)
    text = MULTI_SPACE_RE.sub(" ", text).strip()
    return text


# ---------------------------------------------------------------------------
# 2. NLP / AI EVENT CLASSIFICATION
# ---------------------------------------------------------------------------

CATEGORIES = ["Flood", "Heavy Rain", "Thunderstorm", "Heatwave", "Cyclone", "Cold Wave", "Other/Spam"]

# Small labelled seed corpus used to train the classifier at startup.
# In production this would be replaced by a larger, periodically-retrained
# dataset of historical reports.
SEED_TRAINING_DATA = [
    ("heavy rainfall predicted orange alert issued", "Heavy Rain"),
    ("continuous rainfall for hours drainage overflowing", "Heavy Rain"),
    ("very heavy rain expected next 24 hours", "Heavy Rain"),
    ("water entered ground floor houses flooded", "Flood"),
    ("river water level rising low lying colonies risk", "Flood"),
    ("flood warning issued for low lying areas", "Flood"),
    ("roads submerged after rain traffic stuck", "Flood"),
    ("waterlogging near station streets flooded", "Flood"),
    ("severe thunderstorm with lightning warning", "Thunderstorm"),
    ("thunder and lightning non stop stay safe", "Thunderstorm"),
    ("tree fell on road due to strong winds", "Thunderstorm"),
    ("power lines down after thunderstorm no electricity", "Thunderstorm"),
    ("heavy hailstorm occurred crops damaged fields", "Thunderstorm"),
    ("heatwave conditions likely max temp cross degrees", "Heatwave"),
    ("extremely hot today tap water is warm", "Heatwave"),
    ("cyclonic circulation observed near coast heavy rain", "Cyclone"),
    ("cyclone warning siren evacuation announced coast", "Cyclone"),
    ("cold wave conditions minimum temperature dip", "Cold Wave"),
    ("cold wave hitting hard temperature dropped suddenly", "Cold Wave"),
    ("click here free recharge offer winners announced", "Other/Spam"),
    ("random spam post about crypto giveaway", "Other/Spam"),
]


class EventClassifier:
    def __init__(self):
        texts, labels = zip(*SEED_TRAINING_DATA)
        self.vectorizer = TfidfVectorizer()
        X = self.vectorizer.fit_transform(texts)
        self.model = MultinomialNB()
        self.model.fit(X, labels)

    def classify(self, cleaned_text: str):
        """Returns (category, confidence 0-1)."""
        if not cleaned_text.strip():
            return "Other/Spam", 0.5
        X = self.vectorizer.transform([cleaned_text])
        probs = self.model.predict_proba(X)[0]
        best_idx = probs.argmax()
        category = self.model.classes_[best_idx]
        confidence = float(probs[best_idx])
        return category, round(confidence, 3)


_classifier = EventClassifier()


def classify(cleaned_text: str):
    return _classifier.classify(cleaned_text)


# ---------------------------------------------------------------------------
# 3. DEDUPLICATION
# ---------------------------------------------------------------------------

def content_hash(cleaned_text: str, city: str) -> str:
    """Hash used to spot near-identical reports about the same city.
    A production system would use fuzzy/semantic similarity (e.g. embedding
    cosine similarity) instead of an exact hash — this keeps the demo fast
    and dependency-free while showing the exact same pipeline slot."""
    key = f"{city or ''}|{cleaned_text}".encode("utf-8")
    return hashlib.sha256(key).hexdigest()


# ---------------------------------------------------------------------------
# 4. VERIFICATION ENGINE
# ---------------------------------------------------------------------------

SPAM_MARKERS = ["click here", "free recharge", "giveaway", "winners announced", "crypto"]
SOURCE_TRUST = {"weather_api": 0.45, "citizen_report": 0.20, "social_media": 0.10}


def verification_score(source: str, raw_text: str, has_media: bool, corroboration_count: int, category: str) -> float:
    score = SOURCE_TRUST.get(source, 0.1)

    lowered = raw_text.lower()
    if any(marker in lowered for marker in SPAM_MARKERS):
        score -= 0.5
    if category == "Other/Spam":
        score -= 0.3

    if has_media:
        score += 0.1

    if corroboration_count >= 3:
        score += 0.3
    elif corroboration_count == 2:
        score += 0.15

    # heavy use of caps/exclamation is a light spam signal
    letters = [c for c in raw_text if c.isalpha()]
    if letters:
        caps_ratio = sum(1 for c in letters if c.isupper()) / len(letters)
        if caps_ratio > 0.5:
            score -= 0.15

    return max(0.0, min(1.0, round(score, 3)))


def status_from_score(score: float) -> str:
    if score >= 0.55:
        return "verified"
    if score >= 0.3:
        return "unverified"
    return "flagged"


# ---------------------------------------------------------------------------
# PIPELINE ENTRY POINT
# ---------------------------------------------------------------------------

def process_new_raw_events():
    """Pulls every raw_events row that hasn't been processed yet, runs it
    through clean -> classify -> dedup -> verify, and writes the result to
    processed_events."""
    with db_cursor(commit=True) as cur:
        cur.execute("""
            SELECT r.* FROM raw_events r
            LEFT JOIN processed_events p ON p.raw_id = r.id
            WHERE p.id IS NULL
        """)
        rows = cur.fetchall()

        # Pre-load existing hash -> corroboration counts seen so far this run
        hash_counts = {}
        cur.execute("SELECT content_hash, COUNT(*) c FROM processed_events GROUP BY content_hash")
        for r in cur.fetchall():
            hash_counts[r["content_hash"]] = r["c"]

        processed_count = 0
        for row in rows:
            cleaned = clean_text(row["raw_text"])
            category, confidence = classify(cleaned)
            chash = content_hash(cleaned, row["city"])

            prior_count = hash_counts.get(chash, 0)
            corroboration_count = prior_count + 1
            is_duplicate = 1 if prior_count > 0 else 0

            duplicate_of = None
            if is_duplicate:
                cur.execute(
                    "SELECT id FROM processed_events WHERE content_hash = ? ORDER BY id LIMIT 1", (chash,)
                )
                original = cur.fetchone()
                duplicate_of = original["id"] if original else None

            score = verification_score(
                row["source"], row["raw_text"], bool(row["has_media"]), corroboration_count, category
            )
            status = status_from_score(score)

            cur.execute("""
                INSERT INTO processed_events
                (raw_id, source, cleaned_text, city, state, lat, lon, has_media, event_time,
                 category, category_confidence, content_hash, is_duplicate, duplicate_of,
                 corroboration_count, verification_score, verification_status, processed_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row["id"], row["source"], cleaned, row["city"], row["state"], row["lat"], row["lon"],
                row["has_media"], row["event_time"], category, confidence, chash, is_duplicate,
                duplicate_of, corroboration_count, score, status, datetime.utcnow().isoformat(),
            ))

            hash_counts[chash] = corroboration_count

            # Also bump the corroboration_count on every earlier row sharing this hash,
            # so "verified by N reports" stays accurate for the original report too.
            if is_duplicate:
                cur.execute(
                    "UPDATE processed_events SET corroboration_count = ? WHERE content_hash = ?",
                    (corroboration_count, chash),
                )

            processed_count += 1

        return processed_count


if __name__ == "__main__":
    n = process_new_raw_events()
    print(f"Processed {n} new raw events.")
