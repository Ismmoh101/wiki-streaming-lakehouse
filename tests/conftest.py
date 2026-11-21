import json
import os
import time
from datetime import datetime, timedelta, timezone

import pytest

# Make Python-side datetimes match Spark's UTC session time zone.
os.environ["TZ"] = "UTC"
time.tzset()


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    s = (SparkSession.builder.master("local[2]").appName("tests")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def make_event(i, minute, second=0, wiki="enwiki", bot=False, change="edit", old=100, new=150,
               user=None, base=datetime(2026, 11, 20, 12, 0, tzinfo=timezone.utc)):
    ts = base + timedelta(minutes=minute, seconds=second)
    return {
        "meta": {"id": f"id-{i}", "dt": ts.isoformat().replace("+00:00", "Z"), "domain": f"{wiki}.org"},
        "type": change, "namespace": 0, "title": f"Page {i % 5}", "user": user or f"user{i % 3}",
        "bot": bot, "minor": False, "wiki": wiki, "server_name": f"{wiki}.org",
        "comment": "c", "length": {"old": old, "new": new},
    }


@pytest.fixture
def events_to_lines():
    return lambda events: [json.dumps(e) for e in events]
