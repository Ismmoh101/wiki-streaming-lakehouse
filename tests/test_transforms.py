import json

from pyspark.sql import Row

from conftest import make_event
from streaming.transforms import edits_per_minute, parse_events


def raw_df(spark, events, extra_lines=()):
    lines = [json.dumps(e) for e in events] + list(extra_lines)
    return spark.createDataFrame([Row(value=l) for l in lines])


def test_parse_events_types_and_drops_bad_rows(spark):
    df = parse_events(raw_df(spark, [make_event(1, 0, old=None, new=40)],
                             extra_lines=["{broken", json.dumps({"type": "edit"})]))
    rows = df.collect()
    assert len(rows) == 1
    r = rows[0]
    assert r.event_id == "id-1" and r.wiki == "enwiki" and r.bytes_delta == 40
    assert r.event_ts.isoformat().startswith("2026-11-20T12:00")
    assert r.is_bot is False


def test_edits_per_minute_aggregates(spark):
    events = [
        make_event(1, 0, 5), make_event(2, 0, 30, bot=True), make_event(3, 0, 50, change="new", old=None, new=500),
        make_event(4, 1, 10, wiki="dewiki"), make_event(5, 1, 20),
    ]
    out = {(r.wiki, r.window_start.minute): r for r in edits_per_minute(parse_events(raw_df(spark, events))).collect()}
    en0 = out[("enwiki", 0)]
    assert (en0.edits, en0.bot_edits, en0.human_edits, en0.new_pages) == (3, 1, 2, 1)
    assert en0.bytes_changed == 50 + 50 + 500
    assert out[("dewiki", 1)].edits == 1 and out[("enwiki", 1)].edits == 1
