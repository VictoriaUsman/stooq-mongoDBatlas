# Stooq EOD Price Pipeline

## Overview
An end-to-end data engineering pipeline that extracts, transforms, and loads (ETL) historical end-of-day stock price data from Stooq.com into MongoDB, served through a Flask web application deployed on Google Cloud Run.

## Use Case
Analysts and investors need quick access to historical closing prices for S&P 500 stocks to identify trends, compare performance across time ranges, and make informed decisions. This pipeline automates the daily ingestion of price data at market close and provides an interactive web interface to search any ticker and visualize its price history.

## Architecture

### Data Pipeline (ETL)
- **Extract** — Fetches daily OHLCV (Open, High, Low, Close, Volume) CSV data from Stooq.com for up to 494 S&P 500 tickers with rate-limited API calls
- **Transform** — Parses raw CSV into structured records with typed fields (datetime, float, int) and validates data integrity
- **Load** — Bulk upserts records into MongoDB Atlas with deduplication on (ticker, date) composite key

### Web Application
- Flask API with three endpoints: ticker search, price retrieval, and pipeline trigger
- Plain HTML/JS frontend with Chart.js for interactive price charts
- Autocomplete search box backed by MongoDB regex queries
- Configurable date range selection (1D to 5Y or custom dates) for data fetching
- Single-ticker or full S&P 500 fetch with real-time progress and run statistics

### Scheduling & Deployment
- Local cron job runs the pipeline at 4:30 PM ET on weekdays (market close)
- Dockerized and deployed to Google Cloud Run for serverless hosting
- MongoDB Atlas for managed, cloud-hosted storage
- Environment-based configuration — same codebase works locally and in production

## Tech Stack
Python, Flask, Gunicorn, MongoDB Atlas, PyMongo, Chart.js, Docker, Google Cloud Run

## Setup

### Local Development
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python pipeline.py --days 7          # load last 7 days
python app.py                        # run at http://localhost:5000
```

### Load Data into Atlas
```bash
# Set your Atlas URI in .env, then:
source .env && MONGO_URI="$MONGO_URI" python pipeline.py --days 1
```

### Deploy to Cloud Run
```bash
gcloud run deploy stooq \
  --source . \
  --region us-central1 \
  --set-env-vars "MONGO_URI=<your-atlas-uri>" \
  --allow-unauthenticated
```

## Pipeline CLI Usage
```bash
python pipeline.py --days 1                    # fetch last 1 day
python pipeline.py --days 30                   # fetch last 30 days
python pipeline.py --full                      # full 5-year backfill
python pipeline.py --tickers AAPL,MSFT,GOOGL   # specific tickers only
```
