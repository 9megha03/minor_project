"""
app.py
------
Flask backend for the National Weather Big Data Analytics Platform.

Routes:
    GET  /                    -> public dashboard (map + charts + feed)
    GET  /admin               -> admin panel (verify/reject queue)

API:
    GET  /api/stats           -> summary counters
    GET  /api/events          -> filterable event list (for map + table)
    GET  /api/timeseries      -> events per hour, for the trend chart
    GET  /api/categories      -> event counts by category, for the bar chart
    GET  /api/admin/queue     -> events needing manual review
    POST /api/admin/action    -> approve/reject an event
    POST /api/pipeline/run    -> trigger one ingestion+processing cycle (demo control)
"""

from flask import Flask, jsonify, render_template, request
from datetime import datetime

from database import init_db, db_cursor
from run_pipeline import run_once

app = Flask(__name__)


@app.route("/")
def dashboard():
    return render_template("dashboard.html")


@app.route("/admin")
def admin():
    return render_template("admin.html")


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------

@app.route("/api/stats")
def api_stats():
    with db_cursor() as cur:
        cur.execute("SELECT COUNT(*) c FROM processed_events")
        total = cur.fetchone()["c"]

        cur.execute("SELECT verification_status, COUNT(*) c FROM processed_events GROUP BY verification_status")
        by_status = {r["verification_status"]: r["c"] for r in cur.fetchall()}

        cur.execute("SELECT COUNT(*) c FROM processed_events WHERE is_duplicate = 1")
        duplicates = cur.fetchone()["c"]

        cur.execute("SELECT COUNT(DISTINCT city) c FROM processed_events")
        cities = cur.fetchone()["c"]

    return jsonify({
        "total_events": total,
        "verified": by_status.get("verified", 0),
        "unverified": by_status.get("unverified", 0),
        "flagged": by_status.get("flagged", 0),
        "duplicates_filtered": duplicates,
        "cities_covered": cities,
    })


@app.route("/api/events")
def api_events():
    category = request.args.get("category")
    status = request.args.get("status")
    limit = int(request.args.get("limit", 200))

    query = """
        SELECT id, source, cleaned_text, city, state, lat, lon, has_media, event_time,
               category, category_confidence, is_duplicate, corroboration_count,
               verification_score, verification_status
        FROM processed_events
        WHERE is_duplicate = 0
    """
    params = []
    if category and category != "All":
        query += " AND category = ?"
        params.append(category)
    if status and status != "All":
        query += " AND verification_status = ?"
        params.append(status)
    query += " ORDER BY event_time DESC LIMIT ?"
    params.append(limit)

    with db_cursor() as cur:
        cur.execute(query, params)
        rows = [dict(r) for r in cur.fetchall()]

    return jsonify(rows)


@app.route("/api/timeseries")
def api_timeseries():
    with db_cursor() as cur:
        cur.execute("""
            SELECT substr(event_time, 1, 13) as hour_bucket, COUNT(*) c
            FROM processed_events
            WHERE is_duplicate = 0
            GROUP BY hour_bucket
            ORDER BY hour_bucket
        """)
        rows = [{"hour": r["hour_bucket"], "count": r["c"]} for r in cur.fetchall()]
    return jsonify(rows)


@app.route("/api/categories")
def api_categories():
    with db_cursor() as cur:
        cur.execute("""
            SELECT category, COUNT(*) c FROM processed_events
            WHERE is_duplicate = 0
            GROUP BY category ORDER BY c DESC
        """)
        rows = [{"category": r["category"], "count": r["c"]} for r in cur.fetchall()]
    return jsonify(rows)


@app.route("/api/admin/queue")
def api_admin_queue():
    with db_cursor() as cur:
        cur.execute("""
            SELECT id, source, cleaned_text, city, state, event_time, category,
                   category_confidence, corroboration_count, verification_score, verification_status
            FROM processed_events
            WHERE is_duplicate = 0 AND verification_status IN ('unverified', 'flagged')
            ORDER BY verification_score ASC
            LIMIT 100
        """)
        rows = [dict(r) for r in cur.fetchall()]
    return jsonify(rows)


@app.route("/api/admin/action", methods=["POST"])
def api_admin_action():
    data = request.get_json(force=True)
    processed_id = data.get("processed_id")
    action = data.get("action")  # approve | reject
    actor = data.get("actor", "admin")

    if action not in ("approve", "reject"):
        return jsonify({"error": "action must be 'approve' or 'reject'"}), 400

    new_status = "verified" if action == "approve" else "flagged"

    with db_cursor(commit=True) as cur:
        cur.execute("UPDATE processed_events SET verification_status = ? WHERE id = ?", (new_status, processed_id))
        cur.execute(
            "INSERT INTO admin_actions (processed_id, action, actor, action_time) VALUES (?, ?, ?, ?)",
            (processed_id, action, actor, datetime.utcnow().isoformat()),
        )

    return jsonify({"ok": True, "new_status": new_status})


@app.route("/api/pipeline/run", methods=["POST"])
def api_pipeline_run():
    """Lets the dashboard's 'Simulate new data' button pull in a fresh batch
    on demand, standing in for a live/real-time feed."""
    count = run_once()
    return jsonify({"ok": True, "new_events_processed": count})


if __name__ == "__main__":
    init_db()
    app.run(debug=True, port=5000)
