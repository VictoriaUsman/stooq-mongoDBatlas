import argparse
import logging
from datetime import datetime, timedelta

from config import TICKERS
from extract import fetch_all_tickers
from transform import parse_ticker_csv
from load import ensure_indexes, load_prices

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


def run(days=1, tickers=None, start_date=None, end_date=None):
    if tickers is None:
        tickers = TICKERS

    if start_date and end_date:
        d1 = start_date
        d2 = end_date
    else:
        end = datetime.today()
        start = end - timedelta(days=days)
        d1 = start.strftime("%Y%m%d")
        d2 = end.strftime("%Y%m%d")

    logger.info("Pipeline: %s to %s (%d tickers)", d1, d2, len(tickers))

    ensure_indexes()

    csv_data = fetch_all_tickers(tickers, d1, d2)

    all_records = []
    for ticker, csv_text in csv_data.items():
        records = parse_ticker_csv(ticker, csv_text)
        all_records.extend(records)

    logger.info("Parsed %d total records", len(all_records))

    load_result = load_prices(all_records) or {}

    logger.info("Pipeline complete")

    return {
        "date_range": f"{d1} — {d2}",
        "tickers_requested": len(tickers),
        "tickers_fetched": len(csv_data),
        "records_parsed": len(all_records),
        "records_inserted": load_result.get("inserted", 0),
        "records_updated": load_result.get("updated", 0),
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Stooq EOD price pipeline")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--full", action="store_true", help="Full 5-year backfill")
    group.add_argument("--days", type=int, default=7, help="Number of days to fetch (default: 1)")
    parser.add_argument("--tickers", type=str, help="Comma-separated tickers (e.g. AAPL,MSFT,GOOGL)")
    args = parser.parse_args()

    if args.tickers:
        import config
        config.TICKERS = [t.strip().upper() for t in args.tickers.split(",")]

    days = 5 * 365 if args.full else args.days
    run(days)
