import logging
import time

import requests

from config import STOOQ_URL

logger = logging.getLogger(__name__)


def fetch_ticker_csv(ticker, start_date, end_date):
    """Download CSV data for a single ticker from Stooq.

    Args:
        ticker: Stock symbol (e.g. "AAPL")
        start_date: Start date as YYYY-MM-DD string
        end_date: End date as YYYY-MM-DD string

    Returns:
        Raw CSV text, or None if the request failed.
    """
    url = STOOQ_URL.format(ticker=ticker.lower(), d1=start_date, d2=end_date)
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        text = resp.text.strip()
        if not text or "No data" in text:
            logger.warning("No data returned for %s", ticker)
            return None
        return text
    except requests.RequestException as e:
        logger.error("Failed to fetch %s: %s", ticker, e)
        return None


def fetch_all_tickers(tickers, start_date, end_date, delay=0.5):
    """Download CSV data for multiple tickers with rate limiting.

    Args:
        tickers: List of stock symbols
        start_date: Start date as YYYYMMDD string
        end_date: End date as YYYYMMDD string
        delay: Seconds to wait between requests

    Returns:
        Dict mapping ticker to CSV text (skips failed tickers).
    """
    results = {}
    total = len(tickers)
    for i, ticker in enumerate(tickers, 1):
        logger.info("Fetching %s (%d/%d)", ticker, i, total)
        csv_text = fetch_ticker_csv(ticker, start_date, end_date)
        if csv_text:
            results[ticker] = csv_text
        if i < total:
            time.sleep(delay)
    logger.info("Fetched data for %d/%d tickers", len(results), total)
    return results
