import re
import threading

from flask import Flask, jsonify, render_template, request

from load import get_db
from config import MONGO_COLLECTION
from pipeline import run as run_pipeline

app = Flask(__name__)

_pipeline_status = {"running": False, "last_result": None}


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/search")
def search():
    q = request.args.get("q", "").strip().upper()
    if not q or not re.match(r"^[A-Z.]{1,10}$", q):
        return jsonify([])

    col = get_db()[MONGO_COLLECTION]
    pattern = f"^{re.escape(q)}"
    tickers = col.distinct("ticker", {"ticker": {"$regex": pattern}})
    return jsonify(sorted(tickers)[:20])


@app.route("/api/prices/<ticker>")
def prices(ticker):
    ticker = ticker.upper().strip()
    if not re.match(r"^[A-Z.]{1,10}$", ticker):
        return jsonify([])

    days = request.args.get("days", 365, type=int)

    col = get_db()[MONGO_COLLECTION]
    from datetime import datetime, timedelta
    cutoff = datetime.today() - timedelta(days=days)

    docs = list(
        col.find(
            {"ticker": ticker, "date": {"$gte": cutoff}},
            {"_id": 0},
        ).sort("date", 1)
    )

    for doc in docs:
        doc["date"] = doc["date"].strftime("%Y-%m-%d")

    return jsonify(docs)


@app.route("/api/pipeline", methods=["POST"])
def trigger_pipeline():
    if _pipeline_status["running"]:
        return jsonify({"status": "already_running"}), 409

    data = request.json or {}
    days = data.get("days")
    start_date = data.get("start_date")
    end_date = data.get("end_date")
    tickers = data.get("tickers")

    if tickers:
        tickers = [t.strip().upper() for t in tickers if re.match(r"^[A-Z.]{1,10}$", t.strip().upper())]
        if not tickers:
            return jsonify({"status": "error", "message": "No valid tickers"}), 400

    def _run():
        _pipeline_status["running"] = True
        _pipeline_status["stats"] = None
        try:
            if start_date and end_date:
                stats = run_pipeline(start_date=start_date, end_date=end_date, tickers=tickers)
            else:
                stats = run_pipeline(days=days or 1, tickers=tickers)
            _pipeline_status["last_result"] = "success"
            _pipeline_status["stats"] = stats
        except Exception as e:
            _pipeline_status["last_result"] = str(e)
        finally:
            _pipeline_status["running"] = False

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "started", "tickers": tickers or "all"})


@app.route("/api/pipeline/status")
def pipeline_status():
    return jsonify(_pipeline_status)


if __name__ == "__main__":
    app.run(debug=True, port=5000)
