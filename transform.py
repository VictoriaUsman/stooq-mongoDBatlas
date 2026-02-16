import csv
import io
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def parse_ticker_csv(ticker, csv_text):
    """Parse Stooq CSV text into a list of price record dicts.

    Args:
        ticker: Stock symbol (e.g. "AAPL")
        csv_text: Raw CSV string with columns Date,Open,High,Low,Close,Volume

    Returns:
        List of dicts with keys: ticker, date, open, high, low, close, volume
    """
    records = []
    reader = csv.DictReader(io.StringIO(csv_text))
    for row in reader:
        try:
            records.append({
                "ticker": ticker,
                "date": datetime.strptime(row["Date"], "%Y-%m-%d"),
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"]),
                "volume": int(float(row["Volume"])),
            })
        except (KeyError, ValueError) as e:
            logger.warning("Skipping bad row for %s: %s — %s", ticker, row, e)
    return records
