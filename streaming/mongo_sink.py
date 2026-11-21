"""Serving layer: upsert per-minute stats into MongoDB for low-latency reads."""
from __future__ import annotations

from typing import Iterable

from pymongo import ASCENDING, DESCENDING, UpdateOne

COLLECTION = "edits_per_minute"


def ensure_indexes(collection) -> None:
    # One document per (wiki, minute): makes replays idempotent.
    collection.create_index([("wiki", ASCENDING), ("window_start", ASCENDING)], unique=True)
    # Supports "latest N minutes" dashboard queries.
    collection.create_index([("window_start", DESCENDING)])


def upsert_minute_stats(collection, rows: Iterable[dict]) -> int:
    ops = [
        UpdateOne(
            {"wiki": r["wiki"], "window_start": r["window_start"]},
            {"$set": {k: v for k, v in r.items() if k not in ("wiki", "window_start")}},
            upsert=True,
        )
        for r in rows
    ]
    if not ops:
        return 0
    result = collection.bulk_write(ops, ordered=False)
    return result.upserted_count + result.modified_count
