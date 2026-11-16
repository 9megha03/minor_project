"""
data_ingestion.py
------------------
Simulates the three data sources from the architecture diagram:
    Weather APIs | News/Websites & Social Media | Citizen Reports

In production each of these would be a real connector (IMD/OpenWeather API
polling, a Twitter/X + news RSS listener filtering on #IMD and weather
keywords, and a mobile-app upload endpoint) pushing onto a Kafka topic.
Here they generate realistic-looking records directly, and
`push_to_raw_queue()` is the drop-in point where a Kafka consumer would
instead read from `raw_ingestion_topic` and call the same function.

Every record produced matches the raw_events table in database.py.
"""

import random
from datetime import datetime, timedelta

from database import db_cursor

CITIES = [
    ("Mumbai", "Maharashtra", 19.0760, 72.8777),
    ("Delhi", "Delhi", 28.7041, 77.1025),
    ("Chennai", "Tamil Nadu", 13.0827, 80.2707),
    ("Kolkata", "West Bengal", 22.5726, 88.3639),
    ("Bengaluru", "Karnataka", 12.9716, 77.5946),
    ("Guwahati", "Assam", 26.1445, 91.7362),
    ("Bhubaneswar", "Odisha", 20.2961, 85.8245),
    ("Jaipur", "Rajasthan", 26.9124, 75.7873),
    ("Patna", "Bihar", 25.5941, 85.1376),
    ("Lucknow", "Uttar Pradesh", 26.8467, 80.9462),
]

WEATHER_API_TEMPLATES = [
    "IMD ALERT: Heavy to very heavy rainfall predicted over {city} in next 24 hours. Orange alert issued.",
    "IMD ALERT: Severe thunderstorm with lightning warning for {city} district.",
    "IMD BULLETIN: Heatwave conditions likely over {city} for next 3 days. Max temp expected to cross 43C.",
    "IMD ALERT: Cyclonic circulation observed near coast, {city} likely to receive heavy rain.",
    "IMD BULLETIN: Cold wave conditions over {city}, minimum temperature to dip below normal.",
    "IMD ALERT: Flood warning issued for low-lying areas of {city} due to continuous rainfall.",
]

SOCIAL_MEDIA_TEMPLATES = [
    "Massive waterlogging near {city} station right now #IMD #{city}rains streets are flooded",
    "Extremely hot today in {city}, tap water is literally warm #heatwave #IMD",
    "Thunder and lightning non stop in {city} since afternoon, stay safe everyone #IMD",
    "Roads submerged in {city} after 3 hours of rain, traffic stuck for kms #IMDweather",
    "Cyclone warning siren going off near {city} coast, evacuation announced #IMD #cyclonealert",
    "Cold wave hitting {city} hard this week, temperature dropped suddenly #IMD",
    "CLICK HERE for FREE recharge!! www.totally-real-offer.com {city} winners announced!!!",
    "asdkj random spam post about crypto giveaway nothing to do with weather {city}",
]

CITIZEN_REPORT_TEMPLATES = [
    "Water has entered ground floor of houses in our locality in {city}, need help.",
    "Tree fell on the main road in {city} due to strong winds, road blocked.",
    "Heavy hailstorm just occurred in {city}, crops damaged in nearby fields.",
    "Power lines down in {city} after thunderstorm, no electricity since morning.",
    "River water level rising fast near {city}, low lying colonies at risk.",
    "It has been raining continuously for 6 hours in {city}, drainage overflowing.",
]


def _random_time_within(hours=48):
    return (datetime.utcnow() - timedelta(minutes=random.randint(0, hours * 60))).isoformat()


def _push(cur, source, text, city, state, lat, lon, has_media, event_time):
    cur.execute(
        """INSERT INTO raw_events (source, raw_text, city, state, lat, lon, has_media, event_time, ingested_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (source, text, city, state, lat, lon, has_media, event_time, datetime.utcnow().isoformat()),
    )


def ingest_weather_api(cur, n=8):
    """Official IMD/weather-API style structured alerts — highest trust source."""
    for _ in range(n):
        city, state, lat, lon = random.choice(CITIES)
        text = random.choice(WEATHER_API_TEMPLATES).format(city=city)
        # small jitter so points don't all stack exactly on the city centre
        jlat, jlon = lat + random.uniform(-0.05, 0.05), lon + random.uniform(-0.05, 0.05)
        _push(cur, "weather_api", text, city, state, jlat, jlon, 0, _random_time_within())


def ingest_social_media(cur, n=25):
    """Social/news posts tagged with weather hashtags — mixed trust, some spam."""
    for _ in range(n):
        city, state, lat, lon = random.choice(CITIES)
        text = random.choice(SOCIAL_MEDIA_TEMPLATES).format(city=city)
        jlat, jlon = lat + random.uniform(-0.1, 0.1), lon + random.uniform(-0.1, 0.1)
        has_media = random.choice([0, 0, 1])
        _push(cur, "social_media", text, city, state, jlat, jlon, has_media, _random_time_within())


def ingest_citizen_reports(cur, n=15):
    """Citizen app reports with GPS + optional photo — medium trust, on-the-ground detail."""
    for _ in range(n):
        city, state, lat, lon = random.choice(CITIES)
        text = random.choice(CITIZEN_REPORT_TEMPLATES).format(city=city)
        jlat, jlon = lat + random.uniform(-0.03, 0.03), lon + random.uniform(-0.03, 0.03)
        has_media = random.choice([0, 1, 1])
        _push(cur, "citizen_report", text, city, state, jlat, jlon, has_media, _random_time_within())


def run_ingestion_batch():
    """One 'tick' of ingestion across all three sources. Call this on a schedule
    (cron / APScheduler) for a continuously-updating platform, or on demand for a demo."""
    with db_cursor(commit=True) as cur:
        ingest_weather_api(cur)
        ingest_social_media(cur)
        ingest_citizen_reports(cur)


if __name__ == "__main__":
    run_ingestion_batch()
    print("Ingested one batch of simulated weather_api + social_media + citizen_report records.")
