import csv

from powerbi.export_for_powerbi import VIEWS, main


class FakeCursor:
    def __init__(self, n):
        self.description, self.rows = [("wiki",), ("edits",)], [("enwiki", i) for i in range(n)]

    def execute(self, sql):
        self.sql = sql

    def fetchmany(self, size):
        out, self.rows = self.rows[:size], self.rows[size:]
        return out


class FakeConn:
    def cursor(self):
        return FakeCursor(25_000)  # forces several fetchmany batches


def test_export_writes_every_view(tmp_path):
    counts = main(FakeConn(), tmp_path)
    assert set(counts) == set(VIEWS) and all(c == 25_000 for c in counts.values())
    with open(tmp_path / "hourly_activity.csv") as f:
        rows = list(csv.reader(f))
    assert rows[0] == ["wiki", "edits"] and len(rows) == 25_001
