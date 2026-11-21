"""Pure DataFrame transformations shared by the streaming job and the tests."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, IntegerType, LongType, StringType, StructField,
                               StructType)

# Subset of the Wikimedia recentchange schema we use.
EVENT_SCHEMA = StructType([
    StructField("meta", StructType([
        StructField("id", StringType()),
        StructField("dt", StringType()),
        StructField("domain", StringType()),
    ])),
    StructField("type", StringType()),
    StructField("namespace", IntegerType()),
    StructField("title", StringType()),
    StructField("user", StringType()),
    StructField("bot", BooleanType()),
    StructField("minor", BooleanType()),
    StructField("wiki", StringType()),
    StructField("server_name", StringType()),
    StructField("comment", StringType()),
    StructField("length", StructType([
        StructField("old", LongType()),
        StructField("new", LongType()),
    ])),
])

WATERMARK = "3 minutes"   # how late an event may arrive and still be counted
WINDOW = "1 minute"


def parse_events(raw: DataFrame) -> DataFrame:
    """Raw Kafka/file rows (a `value` string column) -> typed silver rows."""
    e = F.from_json(F.col("value"), EVENT_SCHEMA).alias("e")
    parsed = raw.select(e, F.current_timestamp().alias("ingested_at"))
    return (
        parsed.select(
            F.col("e.meta.id").alias("event_id"),
            F.to_timestamp(F.col("e.meta.dt")).alias("event_ts"),
            F.col("e.wiki").alias("wiki"),
            F.col("e.server_name").alias("server_name"),
            F.col("e.type").alias("change_type"),
            F.col("e.namespace").alias("namespace"),
            F.col("e.title").alias("title"),
            F.col("e.user").alias("user_name"),
            F.coalesce(F.col("e.bot"), F.lit(False)).alias("is_bot"),
            F.coalesce(F.col("e.minor"), F.lit(False)).alias("is_minor"),
            F.col("e.length.old").alias("bytes_old"),
            F.col("e.length.new").alias("bytes_new"),
            (F.coalesce(F.col("e.length.new"), F.lit(0))
             - F.coalesce(F.col("e.length.old"), F.lit(0))).alias("bytes_delta"),
            F.col("e.comment").alias("comment"),
            "ingested_at",
        )
        # Drop malformed JSON and events missing the fields everything keys on.
        .where(F.col("event_id").isNotNull() & F.col("event_ts").isNotNull() & F.col("wiki").isNotNull())
    )


def dedupe(silver: DataFrame) -> DataFrame:
    """Remove replayed events (e.g. producer reconnects) within the watermark."""
    return silver.withWatermark("event_ts", WATERMARK).dropDuplicatesWithinWatermark(["event_id"])


def edits_per_minute(deduped: DataFrame) -> DataFrame:
    """Gold: 1-minute tumbling-window activity per wiki."""
    return (
        deduped.groupBy(F.window("event_ts", WINDOW).alias("w"), "wiki")
        .agg(
            F.count("*").alias("edits"),
            F.sum(F.col("is_bot").cast("int")).alias("bot_edits"),
            F.sum((~F.col("is_bot")).cast("int")).alias("human_edits"),
            F.sum((F.col("change_type") == "new").cast("int")).alias("new_pages"),
            F.sum(F.abs("bytes_delta")).alias("bytes_changed"),
            F.approx_count_distinct("user_name").alias("active_editors"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            "wiki", "edits", "bot_edits", "human_edits", "new_pages", "bytes_changed", "active_editors",
        )
    )
