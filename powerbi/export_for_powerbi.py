"""Export the Trino analytics views to CSV files that Power BI imports.

Run (stack must be up):  python -m powerbi.export_for_powerbi
Output: powerbi/data/*.csv  ->  open in Power BI Desktop (see powerbi/POWERBI.md)
"""
from __future__ import annotations

import csv
import logging
import os
from pathlib import Path

log = logging.getLogger("export")

VIEWS = ["v_hourly_activity", "v_wiki_summary", "v_top_pages_24h", "v_activity_heatmap", "v_pipeline_latency"]
OUT_DIR = Path(__file__).resolve().parent / "data"


def connect():
    import trino

    return trino.dbapi.connect(
        host=os.getenv("TRINO_HOST", "localhost"),
        port=int(os.getenv("TRINO_PORT", "8080")),
        user=os.getenv("TRINO_USER", "powerbi-export"),
        catalog="iceberg",
        schema="analytics",
    )


def export_view(conn, view: str, out_dir: Path) -> int:
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {view}")
    columns = [d[0] for d in cur.description]
    path = out_dir / f"{view.removeprefix('v_')}.csv"
    rows = 0
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        while batch := cur.fetchmany(10_000):
            writer.writerows(batch)
            rows += len(batch)
    log.info("%s -> %s (%s rows)", view, path.name, rows)
    return rows


def main(conn=None, out_dir: Path = OUT_DIR) -> dict[str, int]:
    out_dir.mkdir(parents=True, exist_ok=True)
    conn = conn or connect()
    return {v: export_view(conn, v, out_dir) for v in VIEWS}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    main()
