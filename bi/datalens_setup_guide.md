# Yandex DataLens Setup Guide - Arkhyz Gold CSV Marts

Stage 3 writes single-file CSVs with headers to
`data\gold\csv\` specifically for DataLens:

- `gold_trail_leaderboard.csv`
- `gold_temporal_trends.csv`
- `gold_arkhyz_trail_map_nodes.csv`
- `gold_cohort_distributions.csv`

Field names, types, and row counts below were checked against those files.
Rebuild them at any time with:

```
python src/stage3_hivesql_analytics.py
```

## 1. Upload the four CSV files

1. Open Yandex DataLens and go to `Connections` -> `Create connection`.
2. Choose the CSV file upload connection type (`File` / `CSV`).
3. Upload `data\gold\csv\gold_trail_leaderboard.csv`
   (comma delimiter, UTF-8, first row is the header).
4. Name the connection `arkhyz_gold_trail_leaderboard`, check the preview, and
   create it.
5. Repeat for the other three files:
   - `gold_temporal_trends.csv` -> `arkhyz_gold_temporal_trends`
   - `gold_arkhyz_trail_map_nodes.csv` -> `arkhyz_gold_arkhyz_trail_map_nodes`
   - `gold_cohort_distributions.csv` -> `arkhyz_gold_cohort_distributions`

Each file upload connection holds exactly one CSV, so you get four
connections. Use `Replace file` on a connection later to swap in a refreshed
export without rebuilding the datasets.

## 2. Create a dataset per upload and set field types

For every connection: `Create dataset` -> choose the connection -> save as
`ds_<mart_name>`. Then open the dataset field list and correct the types.
DataLens type detection is usually right, but `year`/`month` must stay
**Integer** (not date, not string) and the metrics below must be numeric.

### `ds_gold_temporal_trends` (552 rows, 18 columns)

| Type | Fields |
| --- | --- |
| Integer | `year`, `month`, `monthly_pings`, `monthly_runs`, `peak_month_rank` |
| Real (fractional) | `prev_month_runs`, `mom_runs_growth_pct`, `avg_speed_kmh`, `rolling_3m_speed_kmh`, `avg_heart_rate_bpm`, `avg_airtime_ms`, `avg_intensity_score`, `avg_satisfaction`, `rolling_3m_satisfaction`, `pct_high_satisfaction` |
| Text (dimension) | `region`, `activity_type`, `season` |

`prev_month_runs` and `mom_runs_growth_pct` are empty for the first month of
each series (12 rows). Keep them as NULL; do not replace with zero, or the
`MoM` series gains false zero spikes.

### `ds_gold_trail_leaderboard` (3933 rows, 32 columns)

| Type | Fields |
| --- | --- |
| Integer | `way_id`, `node_count`, `start_elev_m`, `end_elev_m`, `min_elev_m`, `max_elev_m`, `total_vertical_drop_m`, `total_vertical_climb_m`, `total_pings`, `unique_runs`, `unique_athletes`, `max_airtime_ms`, `region_popularity_rownum`, `tier_drop_rank`, `region_speed_denserank` |
| Real (fractional) | `mtb_scale_clean`, `trail_length_m`, `avg_speed_kmh`, `max_speed_kmh`, `avg_heart_rate_bpm`, `avg_airtime_ms`, `avg_g_force`, `avg_intensity_score`, `avg_satisfaction`, `intensity_percentile` |
| Text (dimension) | `trail_name`, `region`, `highway_type`, `difficulty_tier`, `surface_clean`, `sac_scale_clean` |

`mtb_scale_clean` is empty for 2889 mostly non-MTB trails; keep NULL. Note
that this mart has **no** `activity_type` column; its cohort column is
`difficulty_tier`.

### `ds_gold_cohort_distributions` (1923 rows, 16 columns)

| Type | Fields |
| --- | --- |
| Integer | `ping_count`, `run_count`, `activity_total_pings` |
| Real (fractional) | `share_of_activity_pct`, `avg_speed_kmh`, `avg_heart_rate_bpm`, `avg_airtime_ms`, `avg_g_force`, `avg_intensity_score`, `avg_satisfaction` |
| Text (dimension) | `region`, `activity_type`, `difficulty_tier`, `weather_cond`, `speed_bucket`, `season` |

This CSV carries four cohort-bucket columns: `difficulty_tier`,
`speed_bucket`, `weather_cond`, and `season`. Use `difficulty_tier` for the
scatter below; the others are drop-in alternatives.

### `ds_gold_arkhyz_trail_map_nodes` (3657 rows, 17 columns)

| Type | Fields |
| --- | --- |
| Integer | `way_id`, `seq_order`, `elevation_m`, `node_pings` |
| Real (fractional) | `lat`, `lon`, `cumulative_dist_m`, `segment_grade_pct`, `avg_speed_kmh`, `avg_airtime_ms`, `avg_g_force`, `avg_heart_rate_bpm`, `avg_satisfaction` |
| Text (dimension) | `region`, `trail_name`, `difficulty_tier`, `surface_clean` |

`segment_grade_pct` is empty for 150 nodes; keep NULL. Keep `lat`/`lon` as
Real (fractional) if you later add a map layer.

General rules for the datasets:

- `year` and `month` are Integer; the file has no real date column.
- All `avg_*`, `rolling_*`, `*_pct`, and `*_percentile` fields are Real.
  Plain counts, IDs, ranks, and integer elevations/distances stay Integer.
- All names, labels, buckets, and scales are Text; do not let DataLens
  convert `difficulty_tier` or `speed_bucket` into numbers.
- Aggregation defaults: Integer/Real metric fields -> `Sum`, except
  `avg_*` values which read better as `Average` (`avg_satisfaction` per
  trail or cohort is already an average, so `Sum` would be meaningless).

## 3. Build the charts

Create a workbook and add the three charts below.

### 3.1 Line chart - monthly runs, Arkhyz, split by activity

Source dataset: `ds_gold_temporal_trends`.

1. Add a `Line chart` widget.
2. X axis: `year` and `month` (nested dimensions, so the axis reads
   year -> month).
3. Y axis: `monthly_runs` with aggregation `Sum`.
4. Colors / split: `activity_type`.
5. Filters: `region` = `Arkhyz`.
6. Sort the X axis ascending by `year`, then `month`.

### 3.2 Bar chart - top-10 trails by vertical drop

Source dataset: `ds_gold_trail_leaderboard`.

1. Add a horizontal `Bar chart` widget.
2. Y axis (categories): `trail_name`.
3. X axis (measure): `total_vertical_drop_m` with aggregation `Sum`.
4. Sort by the measure descending.
5. Set the chart row limit to 10 so only the top-10 trails render.
6. Optional filter: `region` if you want a per-region top-10; the unfiltered
   chart is the all-region leaderboard.

### 3.3 Scatter chart - speed vs satisfaction by cohort

Source dataset: `ds_gold_cohort_distributions`.

1. Add a `Scatter chart` widget.
2. X axis: `avg_speed_kmh` (`Average`).
3. Y axis: `avg_satisfaction` (`Average`).
4. Points / colors: `difficulty_tier` (the cohort bucket in this CSV).
5. Size: `run_count` with `Sum` so bigger cohorts are visibly larger.
6. Optional: add `region` as a second point dimension or as a filter.

Alternative cohort buckets from the same CSV: swap `difficulty_tier` for
`speed_bucket`, `weather_cond`, or `season`.

## 4. Refresh after Stage 3 reruns

The single-file CSVs with headers exist precisely for DataLens; Stage 3
writes them next to the Parquet marts on every run.

1. Re-run `python src/stage3_hivesql_analytics.py` in
   the project root.
2. On each CSV connection, use `Replace file` and upload the refreshed file
   from `data\gold\csv\`.
3. Refresh the workbook charts. Field bindings survive as long as the header
   names are unchanged; do not rename mart columns, or the dataset fields and
   chart bindings break.
