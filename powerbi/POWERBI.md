# Power BI dashboard

Power BI Desktop is free and Windows-only. The `.pbix` file has to be built in Power BI Desktop itself. These steps take about 30–45 minutes.

## 1. Get the data

With the stack running for at least an hour (more is better):

```bash
docker compose exec -T trino trino < sql/trino_views.sql   # create the views once
python -m powerbi.export_for_powerbi                        # writes powerbi/data/*.csv
```

## 2. Load it

**Get Data → Text/CSV**, and load all five files from `powerbi/data/`:

| File | Role in the model |
|---|---|
| `wiki_summary.csv` | Dimension (one row per wiki) |
| `hourly_activity.csv` | Fact (wiki × hour) |
| `top_pages_24h.csv` | Fact (wiki × page) |
| `activity_heatmap.csv` | Standalone |
| `pipeline_latency.csv` | Standalone |

In **Model view**, create relationships `wiki_summary[wiki]` 1 → * `hourly_activity[wiki]` and `wiki_summary[wiki]` 1 → * `top_pages_24h[wiki]`.

Set `hour_utc` to Date/Time and the count columns to Whole Number.

## 3. DAX measures

Create these on `hourly_activity` (**New measure**):

```DAX
Total Edits = SUM(hourly_activity[edits])

Bot Share % = DIVIDE(SUM(hourly_activity[bot_edits]), [Total Edits])

New Pages = SUM(hourly_activity[new_pages])

Avg Edits per Minute = DIVIDE([Total Edits], COUNTROWS(hourly_activity) * 60)

Peak Minute = MAX(hourly_activity[peak_minute_edits])

Active Wikis = DISTINCTCOUNT(hourly_activity[wiki])

Edits vs Prior Hour % =
VAR cur = [Total Edits]
VAR prev = CALCULATE([Total Edits], DATEADD(hourly_activity[hour_utc], -1, HOUR))
RETURN DIVIDE(cur - prev, prev)
```

Set **Bot Share %** and **Edits vs Prior Hour %** to Percentage format.

## 4. Report layout (one page)

| Area | Visual | Fields |
|---|---|---|
| Top row | 5 **Card** visuals | Total Edits, Avg Edits per Minute, Peak Minute, Bot Share %, Active Wikis |
| Middle left | **Line chart** | X: `hour_utc`, Y: Total Edits, Legend: top 5 `wiki` (Filter pane → Top N 5 by Total Edits) |
| Middle right | **Stacked bar** | Y: `wiki` (top 10), X: `human_edits` and `bot_edits` |
| Bottom left | **Matrix** heatmap | Rows: `day_of_week`, Columns: `hour_of_day`, Values: `edits`, conditional formatting → background color |
| Bottom right | **Table** | `top_pages_24h`: wiki, title, edits, editors |
| Side | **Slicer** | `wiki_summary[wiki]` |

Add a second page, **Pipeline Health**, with a line chart of `p50_latency_s` and `p95_latency_s` by `hour_utc`. This shows how quickly the pipeline gets an edit into the lakehouse, which is worth pointing out in interviews.

## 5. Save it

Save as `powerbi/wiki_activity.pbix`, then take a screenshot for the GitHub README (`powerbi/dashboard.png`).
