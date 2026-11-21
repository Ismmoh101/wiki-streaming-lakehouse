from datetime import datetime

import mongomock

from streaming.mongo_sink import ensure_indexes, upsert_minute_stats


class BulkResult:
    def __init__(self, upserted, modified):
        self.upserted_count, self.modified_count = upserted, modified


class Collection:
    """mongomock collection whose bulk_write replays UpdateOne ops one by one.

    (mongomock's own bulk_write lags behind the current pymongo API.)
    """

    def __init__(self):
        self.c = mongomock.MongoClient().db.edits_per_minute

    def __getattr__(self, name):
        return getattr(self.c, name)

    def bulk_write(self, ops, ordered=True):
        up = mod = 0
        for op in ops:
            r = self.c.update_one(op._filter, op._doc, upsert=op._upsert)
            up += r.upserted_id is not None
            mod += r.modified_count
        return BulkResult(up, mod)


def test_upsert_is_idempotent():
    coll = Collection()
    ensure_indexes(coll)
    t = datetime(2026, 11, 20, 12, 0)
    rows = [{"wiki": "enwiki", "window_start": t, "edits": 3}, {"wiki": "dewiki", "window_start": t, "edits": 1}]
    assert upsert_minute_stats(coll, rows) == 2
    assert upsert_minute_stats(coll, [{"wiki": "enwiki", "window_start": t, "edits": 5}]) == 1  # late update
    assert upsert_minute_stats(coll, rows[1:]) == 0  # exact replay changes nothing
    assert coll.count_documents({}) == 2
    assert coll.find_one({"wiki": "enwiki"})["edits"] == 5
    assert upsert_minute_stats(coll, []) == 0
