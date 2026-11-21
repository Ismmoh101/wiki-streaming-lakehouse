"""End-to-end run of the real streaming job in local mode (files -> parquet)."""
import json
import os

from conftest import make_event
from streaming.stream_job import parse_args, start


def write_jsonl(path, events):
    path.write_text("\n".join(json.dumps(e) for e in events) + "\n")


def test_streaming_job_end_to_end(spark, tmp_path):
    inp = tmp_path / "input"
    inp.mkdir()
    # 3 files = 3 micro-batches. Minutes 0-10; 5 events per minute.
    batch1 = [make_event(m * 10 + k, m, k * 10) for m in range(0, 4) for k in range(5)]
    batch2 = [make_event(m * 10 + k, m, k * 10) for m in range(4, 8) for k in range(5)]
    batch3 = [make_event(m * 10 + k, m, k * 10) for m in range(8, 11) for k in range(5)]
    batch2 += batch1[:3]                                   # replayed duplicates
    batch3.append(make_event(999, 6, 30, wiki="dewiki"))   # late but within the 3-min watermark
    batch3.append(make_event(998, 0, 30, wiki="frwiki"))   # too late: minute 0 already closed
    for i, b in enumerate([batch1, batch2, batch3]):
        f = inp / f"part-{i}.json"
        write_jsonl(f, b)
        os.utime(f, (1_700_000_000 + i * 60,) * 2)  # file source reads oldest first

    args = parse_args(["--source", "files", "--sink", "parquet", "--once",
                       "--input-path", str(inp), "--output-path", str(tmp_path / "out"),
                       "--checkpoint-path", str(tmp_path / "chk")])
    for q in start(spark, args):
        q.awaitTermination()
        assert q.exception() is None

    out = tmp_path / "out"
    bronze = spark.read.parquet(str(out / "bronze"))
    silver = spark.read.parquet(str(out / "silver"))
    gold = spark.read.parquet(str(out / "gold"))

    assert bronze.count() == 55 + 3 + 2                    # everything lands raw
    assert silver.count() == silver.select("event_id").distinct().count()  # no duplicates
    assert silver.count() == 55 + 1                        # duplicates dropped, too-late one dropped

    rows = {(r.wiki, r.window_start.minute): r.edits for r in gold.collect()}
    # Watermark = latest event (minute 10) - 3 min, so windows through minute 6 are final.
    for m in range(0, 7):
        assert rows[("enwiki", m)] == 5
    assert rows[("dewiki", 6)] == 1
    assert ("frwiki", 0) not in rows
