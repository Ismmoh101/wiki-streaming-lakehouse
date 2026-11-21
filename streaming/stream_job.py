"""Spark Structured Streaming job: Kafka -> Iceberg lakehouse (+ MongoDB).

Medallion layout in the Iceberg catalog `lake`, namespace `wiki`:
  bronze  lake.wiki.events_raw       raw JSON + Kafka metadata (replayable source of truth)
  silver  lake.wiki.edits            parsed, typed, de-duplicated edits, partitioned by day
  gold    lake.wiki.edits_per_minute 1-minute activity per wiki (MERGE = idempotent)
The gold stream is also upserted into MongoDB for low-latency serving.

Local/dev mode (no Kafka, no Iceberg):  --source files --sink parquet
"""
from __future__ import annotations

import argparse
import logging
import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .transforms import dedupe, edits_per_minute, parse_events

log = logging.getLogger("stream_job")

CATALOG = os.getenv("ICEBERG_CATALOG", "lake")
NAMESPACE = f"{CATALOG}.wiki"
BRONZE, SILVER, GOLD = (f"{NAMESPACE}.events_raw", f"{NAMESPACE}.edits", f"{NAMESPACE}.edits_per_minute")

DDL = [
    f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}",
    f"""CREATE TABLE IF NOT EXISTS {BRONZE} (
          value STRING, topic STRING, kafka_partition INT, kafka_offset BIGINT,
          kafka_ts TIMESTAMP, ingested_at TIMESTAMP)
        USING iceberg PARTITIONED BY (days(ingested_at))""",
    f"""CREATE TABLE IF NOT EXISTS {SILVER} (
          event_id STRING, event_ts TIMESTAMP, wiki STRING, server_name STRING,
          change_type STRING, namespace INT, title STRING, user_name STRING,
          is_bot BOOLEAN, is_minor BOOLEAN, bytes_old BIGINT, bytes_new BIGINT,
          bytes_delta BIGINT, comment STRING, ingested_at TIMESTAMP)
        USING iceberg PARTITIONED BY (days(event_ts))""",
    f"""CREATE TABLE IF NOT EXISTS {GOLD} (
          window_start TIMESTAMP, window_end TIMESTAMP, wiki STRING, edits BIGINT,
          bot_edits BIGINT, human_edits BIGINT, new_pages BIGINT, bytes_changed BIGINT,
          active_editors BIGINT)
        USING iceberg PARTITIONED BY (days(window_start))""",
]


def read_source(spark: SparkSession, args) -> DataFrame:
    if args.source == "kafka":
        return (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", args.kafka_bootstrap)
            .option("subscribe", args.topic)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", args.max_offsets)  # back-pressure
            .option("failOnDataLoss", "false")
            .load()
            .select(
                F.col("value").cast("string").alias("value"),
                "topic",
                F.col("partition").alias("kafka_partition"),
                F.col("offset").alias("kafka_offset"),
                F.col("timestamp").alias("kafka_ts"),
            )
        )
    # Newline-delimited JSON files, one event per line (local dev and tests).
    return (
        spark.readStream.option("maxFilesPerTrigger", 1).text(args.input_path)
        .select(
            "value",
            F.lit("files").alias("topic"),
            F.lit(None).cast("int").alias("kafka_partition"),
            F.lit(None).cast("bigint").alias("kafka_offset"),
            F.lit(None).cast("timestamp").alias("kafka_ts"),
        )
    )


def make_gold_writer(args):
    """foreachBatch handler: idempotent write of each gold micro-batch."""
    mongo_coll = None
    if args.mongo_uri:
        from pymongo import MongoClient

        from .mongo_sink import COLLECTION, ensure_indexes

        mongo_coll = MongoClient(args.mongo_uri)[args.mongo_db][COLLECTION]
        ensure_indexes(mongo_coll)

    def write(batch: DataFrame, batch_id: int) -> None:
        if batch.isEmpty():
            return
        batch = batch.persist()
        if args.sink == "iceberg":
            # MERGE so a replayed micro-batch never double-counts a window.
            batch.createOrReplaceTempView("gold_batch")
            batch.sparkSession.sql(f"""
                MERGE INTO {GOLD} t USING gold_batch s
                ON t.wiki = s.wiki AND t.window_start = s.window_start
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *""")
        else:
            batch.write.mode("append").parquet(os.path.join(args.output_path, "gold"))

        if mongo_coll is not None:
            from .mongo_sink import upsert_minute_stats

            rows = [r.asDict() for r in batch.toLocalIterator()]
            upsert_minute_stats(mongo_coll, rows)
        log.info("Gold batch %s: %s rows", batch_id, batch.count())
        batch.unpersist()

    return write


def start(spark: SparkSession, args):
    if args.sink == "iceberg":
        for stmt in DDL:
            spark.sql(stmt)

    raw = read_source(spark, args).withColumn("ingested_at", F.current_timestamp())
    deduped = dedupe(parse_events(raw.select("value")))
    trigger = {"availableNow": True} if args.once else {"processingTime": args.trigger}

    def sink(df: DataFrame, name: str, table: str):
        w = (df.writeStream.queryName(name).outputMode("append").trigger(**trigger)
             .option("checkpointLocation", os.path.join(args.checkpoint_path, name)))
        if args.sink == "iceberg":
            return w.format("iceberg").option("fanout-enabled", "true").toTable(table)
        return w.format("parquet").start(os.path.join(args.output_path, name))

    queries = [
        sink(raw, "bronze", BRONZE),
        sink(deduped, "silver", SILVER),
        edits_per_minute(deduped).writeStream.queryName("gold").outputMode("append")
        .trigger(**trigger)
        .option("checkpointLocation", os.path.join(args.checkpoint_path, "gold"))
        .foreachBatch(make_gold_writer(args)).start(),
    ]
    return queries


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--source", choices=["kafka", "files"], default=os.getenv("SOURCE", "kafka"))
    p.add_argument("--sink", choices=["iceberg", "parquet"], default=os.getenv("SINK", "iceberg"))
    p.add_argument("--kafka-bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"))
    p.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "wiki.recentchange"))
    p.add_argument("--max-offsets", type=int, default=int(os.getenv("MAX_OFFSETS", "20000")))
    p.add_argument("--input-path", default=os.getenv("INPUT_PATH", "data/input"))
    p.add_argument("--output-path", default=os.getenv("OUTPUT_PATH", "data/output"))
    p.add_argument("--checkpoint-path", default=os.getenv("CHECKPOINT_PATH", "data/checkpoints"))
    p.add_argument("--mongo-uri", default=os.getenv("MONGO_URI"))
    p.add_argument("--mongo-db", default=os.getenv("MONGO_DB", "wiki"))
    p.add_argument("--trigger", default=os.getenv("TRIGGER", "30 seconds"))
    p.add_argument("--once", action="store_true", help="process what's available, then stop")
    return p.parse_args(argv)


def main(argv=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args(argv)
    spark = SparkSession.builder.appName("wiki-streaming-lakehouse").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.shuffle.partitions", os.getenv("SHUFFLE_PARTITIONS", "8"))
    queries = start(spark, args)
    for q in queries:
        q.awaitTermination()


if __name__ == "__main__":
    main()
