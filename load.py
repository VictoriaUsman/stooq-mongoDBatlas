import logging
import ssl

import certifi
from pymongo import MongoClient, UpdateOne

from config import MONGO_URI, MONGO_DB, MONGO_COLLECTION

logger = logging.getLogger(__name__)

_client = None


def get_db():
    """Return a pymongo database handle (reuses connection)."""
    global _client
    if _client is None:
        _client = MongoClient(
            MONGO_URI,
            tls=True,
            tlsCAFile=certifi.where(),
            tlsAllowInvalidCertificates=False,
        )
    return _client[MONGO_DB]


def ensure_indexes():
    """Create indexes on the prices collection if they don't exist."""
    col = get_db()[MONGO_COLLECTION]
    col.create_index([("ticker", 1), ("date", 1)], unique=True)
    col.create_index("ticker")
    logger.info("Indexes ensured on %s.%s", MONGO_DB, MONGO_COLLECTION)


def load_prices(records, batch_size=1000):
    """Bulk upsert price records into MongoDB.

    Args:
        records: List of price dicts (ticker, date, open, high, low, close, volume)
        batch_size: Number of operations per bulk_write call
    """
    if not records:
        logger.info("No records to load")
        return

    col = get_db()[MONGO_COLLECTION]
    ops = [
        UpdateOne(
            {"ticker": r["ticker"], "date": r["date"]},
            {"$set": r},
            upsert=True,
        )
        for r in records
    ]

    total_upserted = 0
    total_modified = 0
    for i in range(0, len(ops), batch_size):
        batch = ops[i : i + batch_size]
        result = col.bulk_write(batch, ordered=False)
        total_upserted += result.upserted_count
        total_modified += result.modified_count

    logger.info(
        "Loaded %d records: %d inserted, %d updated",
        len(records),
        total_upserted,
        total_modified,
    )
    return {"total": len(records), "inserted": total_upserted, "updated": total_modified}
