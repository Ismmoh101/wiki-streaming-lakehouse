-- Analytics views over the Iceberg lakehouse, queried through Trino.
-- Run:  docker compose exec trino trino -f /dev/stdin < sql/trino_views.sql
-- Power BI reads the exported results of these views (see powerbi/).

CREATE SCHEMA IF NOT EXISTS iceberg.analytics;

-- Hourly activity per wiki (fact table for the dashboard).
CREATE OR REPLACE VIEW iceberg.analytics.v_hourly_activity AS
SELECT
    date_trunc('hour', window_start)          AS hour_utc,
    wiki,
    sum(edits)                                AS edits,
    sum(bot_edits)                            AS bot_edits,
    sum(human_edits)                          AS human_edits,
    sum(new_pages)                            AS new_pages,
    sum(bytes_changed)                        AS bytes_changed,
    max(edits)                                AS peak_minute_edits
FROM iceberg.wiki.edits_per_minute
GROUP BY 1, 2;

-- One row per wiki with lifetime totals (dimension table).
CREATE OR REPLACE VIEW iceberg.analytics.v_wiki_summary AS
SELECT
    wiki,
    regexp_extract(wiki, '^([a-z]+?)(wiki|wiktionary|wikibooks|wikinews|wikiquote|wikisource|wikivoyage)$', 1)
                                              AS language_code,
    sum(edits)                                AS total_edits,
    round(100.0 * sum(bot_edits) / nullif(sum(edits), 0), 1) AS bot_share_pct,
    min(window_start)                         AS first_seen,
    max(window_end)                           AS last_seen
FROM iceberg.wiki.edits_per_minute
GROUP BY wiki;

-- Most-edited pages in the last 24 hours (from the silver layer).
CREATE OR REPLACE VIEW iceberg.analytics.v_top_pages_24h AS
SELECT
    wiki,
    title,
    count(*)                                  AS edits,
    count(DISTINCT user_name)                 AS editors,
    sum(abs(bytes_delta))                     AS bytes_changed
FROM iceberg.wiki.edits
WHERE event_ts >= current_timestamp - INTERVAL '24' HOUR
  AND namespace = 0                           -- articles only
GROUP BY wiki, title
ORDER BY edits DESC
LIMIT 500;

-- Edits by hour-of-day and day-of-week (heatmap).
CREATE OR REPLACE VIEW iceberg.analytics.v_activity_heatmap AS
SELECT
    day_of_week(event_ts)                     AS day_of_week,
    hour(event_ts)                            AS hour_of_day,
    count(*)                                  AS edits,
    sum(CASE WHEN is_bot THEN 1 ELSE 0 END)   AS bot_edits
FROM iceberg.wiki.edits
GROUP BY 1, 2;

-- Pipeline health: end-to-end latency from edit to lakehouse.
CREATE OR REPLACE VIEW iceberg.analytics.v_pipeline_latency AS
SELECT
    date_trunc('hour', ingested_at)           AS hour_utc,
    count(*)                                  AS events,
    approx_percentile(to_milliseconds(ingested_at - event_ts) / 1000.0, 0.5)  AS p50_latency_s,
    approx_percentile(to_milliseconds(ingested_at - event_ts) / 1000.0, 0.95) AS p95_latency_s
FROM iceberg.wiki.edits
GROUP BY 1;
