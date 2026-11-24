# BI Layer - Gold Marts and Dashboard Engineering

Stage 3 materializes four Gold marts into `data/gold/<mart>.parquet` (Parquet
Snappy) and single-file CSVs with headers into `data/gold/csv/<mart>.csv`. The
Parquet directories feed Power BI and the Leaflet + Plotly web view; the CSVs
feed Yandex DataLens. This document records each mart, its production row
count, its key columns and its BI purpose, then the exact DAX measures, the
DataLens setup, and how Power BI connects to the Spark output.

Rebuild all marts from the project root:

```
python src/stage3_hivesql_analytics.py
```

## 1. Gold marts

### 1.1 gold_trail_leaderboard

- Production rows: 3,933 (31 columns).
- Grain: one row per trail (`way_id`).
- Key columns:
  - Identity and locale: `way_id`, `trail_name`, `region`, `highway_type`.
  - Classification: `difficulty_tier`, `surface_clean`, `mtb_scale_clean`,
    `sac_scale_clean`.
  - Geometry and elevation: `node_count`, `start_elev_m`, `end_elev_m`,
    `min_elev_m`, `max_elev_m`, `total_vertical_drop_m`,
    `total_vertical_climb_m`, `trail_length_m`.
  - Activity metrics: `total_pings`, `unique_runs`, `unique_athletes`,
    `avg_speed_kmh`, `max_speed_kmh`, `avg_heart_rate_bpm`, `avg_airtime_ms`,
    `max_airtime_ms`, `avg_g_force`, `avg_intensity_score`, `avg_satisfaction`.
  - Window-derived ranks: `region_popularity_rownum`, `tier_drop_rank`,
    `region_speed_denserank`, `intensity_percentile`.
- BI purpose: the primary table and trail dimension. Drives the top-10 vertical
  drop bar chart, the speed-vs-satisfaction scatter, and per-trail rankings.
  This mart has no `activity_type` column; its cohort column is
  `difficulty_tier`.

### 1.2 gold_temporal_trends

- Production rows: 552 (18 columns).
- Grain: one row per region x activity_type x year x month.
- Key columns:
  - Dimensions: `region`, `activity_type`, `season`, `year`, `month`.
  - Volume: `monthly_pings`, `monthly_runs`, `peak_month_rank`.
  - Month-over-month window columns: `prev_month_runs`, `mom_runs_growth_pct`.
  - Metrics: `avg_speed_kmh`, `rolling_3m_speed_kmh`, `avg_heart_rate_bpm`,
    `avg_airtime_ms`, `avg_intensity_score`, `avg_satisfaction`,
    `rolling_3m_satisfaction`, `pct_high_satisfaction`.
- BI purpose: every time series. The monthly runs line chart (split by
  activity type), month-over-month growth, weighted satisfaction, and speed
  premiums are all computed from this mart. `prev_month_runs` and
  `mom_runs_growth_pct` are NULL for the first month of each series (12 rows);
  keep them NULL so the growth series does not gain false zero spikes.

### 1.3 gold_arkhyz_trail_map_nodes

- Production rows: 75,833 (17 columns).
- Grain: one row per trail node, ordered along the trail by `seq_order`.
- Key columns:
  - Identity: `way_id`, `seq_order`, `region`, `trail_name`.
  - Classification: `difficulty_tier`, `surface_clean`.
  - Geometry: `lat`, `lon`, `elevation_m`, `cumulative_dist_m`,
    `segment_grade_pct`.
  - Per-node metrics: `node_pings`, `avg_speed_kmh`, `avg_airtime_ms`,
    `avg_g_force`, `avg_heart_rate_bpm`, `avg_satisfaction`.
- BI purpose: the map layer. The Leaflet trail-map dataset renders `lat`/`lon`
  polylines per `way_id` colored by elevation, grade or satisfaction.
  `segment_grade_pct` is NULL for a small number of nodes; keep it NULL.

### 1.4 gold_cohort_distributions

- Production rows: 1,923 (16 columns).
- Grain: one row per cohort bucket combination (four bucket columns).
- Key columns:
  - Cohort buckets: `difficulty_tier`, `speed_bucket`, `weather_cond`,
    `season`.
  - Dimensions: `region`, `activity_type`.
  - Volume: `ping_count`, `run_count`, `activity_total_pings`,
    `share_of_activity_pct`.
  - Metrics: `avg_speed_kmh`, `avg_heart_rate_bpm`, `avg_airtime_ms`,
    `avg_g_force`, `avg_intensity_score`, `avg_satisfaction`.
- BI purpose: cohort comparison. Powers the speed-vs-satisfaction scatter by
  cohort and the satisfaction-by-difficulty view. Any of the four bucket
  columns can be used as the split; `difficulty_tier` is the default.

## 2. Power BI: connection, model and DAX

### 2.1 Connecting to the marts

Power BI Desktop's built-in Parquet connector reads a Spark output directory
natively, including mart directories that Spark wrote as partitioned folders.
One query per mart directory:

1. `Home` -> `Get Data` -> `More...` -> `File` -> `Parquet` -> `Connect`.
2. Point at the mart directory, for example `data/gold/gold_trail_leaderboard`.
3. In the Navigator, choose the folder node and `Transform Data`.
4. Use `Combine` / `Combine Files` so all `part-*.parquet` files and partition
   subfolders load as one table.
5. Rename the query to the mart name and `Close & Apply`. Repeat for the other
   three marts.

No custom M code is needed, and Delta format is not used. The plain Parquet
connector plus `Combine Files` is the supported path.

### 2.2 Star-schema relationships

`gold_trail_leaderboard` is the primary table (one row per trail). The other
marts are related tables at coarser grains:

- `gold_trail_leaderboard` <-> `gold_arkhyz_trail_map_nodes` on `way_id`
  (many-to-one; `region` as an optional second key).
- `gold_trail_leaderboard` <-> `gold_temporal_trends` on `region`
  (many-to-many, single filter direction).
- `gold_trail_leaderboard` <-> `gold_cohort_distributions` on `region`
  (many-to-many, single filter direction).
- `gold_temporal_trends` <-> `gold_cohort_distributions` on `region` and
  `activity_type`.

Region-only joins are many-to-many because `region` is not unique in any mart.
The leaderboard has no `activity_type`, so join it on `region` only. Mark
`gold_temporal_trends[year]` and `[month]` as whole numbers.

Apply the Alpine theme via `View` -> `Themes` -> `Browse for themes` and select
`bi/arkhyz_powerbi_theme.json`.

### 2.3 DAX measures

Total runs across the trend mart:

```dax
Total Runs = SUM(gold_temporal_trends[monthly_runs])
```

Run-weighted satisfaction, so low-volume months do not dominate:

```dax
Weighted Avg Satisfaction = DIVIDE(SUMX(gold_temporal_trends, gold_temporal_trends[avg_satisfaction] * gold_temporal_trends[monthly_runs]), SUM(gold_temporal_trends[monthly_runs]))
```

Largest single-trail vertical drop:

```dax
Max Vertical Drop (m) = MAX(gold_trail_leaderboard[total_vertical_drop_m])
```

Month-over-month run growth using the Stage 3 window column:

```dax
MoM Run Growth % = DIVIDE(SUM(gold_temporal_trends[monthly_runs]) - SUM(gold_temporal_trends[prev_month_runs]), SUM(gold_temporal_trends[prev_month_runs]))
```

Average-speed ratio of Arkhyz against all other regions (1.0 = parity):

```dax
Arkhyz Speed Premium vs Caucasus = DIVIDE(CALCULATE(AVERAGE(gold_temporal_trends[avg_speed_kmh]), gold_temporal_trends[region]="Arkhyz"), CALCULATE(AVERAGE(gold_temporal_trends[avg_speed_kmh]), gold_temporal_trends[region]<>"Arkhyz")) - 1
```

Formatting hints: `Weighted Avg Satisfaction` to 2 decimals; `Total Runs` and
`Max Vertical Drop (m)` as whole numbers with thousands separators;
`MoM Run Growth %` and `Arkhyz Speed Premium vs Caucasus` as percentages with
1 decimal.

Reference KPI snapshot from the dashboard: total runs 49,473, weighted average
satisfaction 3.63, max vertical drop 3,331 m, MoM +1.01%.

## 3. Yandex DataLens

### 3.1 Upload the CSVs

Stage 3 writes four single-file CSVs with headers to `data/gold/csv/`:

- `gold_trail_leaderboard.csv`
- `gold_temporal_trends.csv`
- `gold_arkhyz_trail_map_nodes.csv`
- `gold_cohort_distributions.csv`

In DataLens: `Connections` -> `Create connection` -> CSV file upload. Upload
one file per connection (comma delimiter, UTF-8, first row is the header) and
name each connection `arkhyz_<mart_name>`. Use `Replace file` later to swap in
a refreshed export without rebuilding datasets.

### 3.2 Dataset field types

For each connection, `Create dataset` and save as `ds_<mart_name>`. Correct
the detected types:

- `ds_gold_temporal_trends` (552 rows, 18 columns):
  - Integer: `year`, `month`, `monthly_pings`, `monthly_runs`,
    `peak_month_rank`.
  - Real: `prev_month_runs`, `mom_runs_growth_pct`, `avg_speed_kmh`,
    `rolling_3m_speed_kmh`, `avg_heart_rate_bpm`, `avg_airtime_ms`,
    `avg_intensity_score`, `avg_satisfaction`, `rolling_3m_satisfaction`,
    `pct_high_satisfaction`.
  - Text: `region`, `activity_type`, `season`.
- `ds_gold_trail_leaderboard` (3,933 rows, 31 columns):
  - Integer: `way_id`, `node_count`, `start_elev_m`, `end_elev_m`,
    `min_elev_m`, `max_elev_m`, `total_vertical_drop_m`,
    `total_vertical_climb_m`, `total_pings`, `unique_runs`, `unique_athletes`,
    `max_airtime_ms`, `region_popularity_rownum`, `tier_drop_rank`,
    `region_speed_denserank`.
  - Real: `mtb_scale_clean`, `trail_length_m`, `avg_speed_kmh`,
    `max_speed_kmh`, `avg_heart_rate_bpm`, `avg_airtime_ms`, `avg_g_force`,
    `avg_intensity_score`, `avg_satisfaction`, `intensity_percentile`.
  - Text: `trail_name`, `region`, `highway_type`, `difficulty_tier`,
    `surface_clean`, `sac_scale_clean`.
- `ds_gold_arkhyz_trail_map_nodes` (75,833 rows, 17 columns):
  - Integer: `way_id`, `seq_order`, `elevation_m`, `node_pings`.
  - Real: `lat`, `lon`, `cumulative_dist_m`, `segment_grade_pct`,
    `avg_speed_kmh`, `avg_airtime_ms`, `avg_g_force`, `avg_heart_rate_bpm`,
    `avg_satisfaction`.
  - Text: `region`, `trail_name`, `difficulty_tier`, `surface_clean`.
- `ds_gold_cohort_distributions` (1,923 rows, 16 columns):
  - Integer: `ping_count`, `run_count`, `activity_total_pings`.
  - Real: `share_of_activity_pct`, `avg_speed_kmh`, `avg_heart_rate_bpm`,
    `avg_airtime_ms`, `avg_g_force`, `avg_intensity_score`, `avg_satisfaction`.
  - Text: `region`, `activity_type`, `difficulty_tier`, `weather_cond`,
    `speed_bucket`, `season`.

General rules: `year` and `month` stay Integer (the file has no real date
column). All `avg_*`, `rolling_*`, `*_pct` and `*_percentile` fields are Real;
counts, IDs, ranks and integer elevations stay Integer; names, labels and
buckets stay Text. Integer/Real metrics aggregate as `Sum`, except `avg_*`
fields which read better as `Average`.

### 3.3 The three core charts

1. Line chart - monthly runs, Arkhyz, split by activity. Source
   `ds_gold_temporal_trends`; X axis `year` then `month` (nested), Y axis
   `monthly_runs` (`Sum`), split by `activity_type`, filter `region` =
   `Arkhyz`, sort X ascending.
2. Bar chart - top-10 trails by vertical drop. Source
   `ds_gold_trail_leaderboard`; horizontal bar, Y axis `trail_name`, X axis
   `total_vertical_drop_m` (`Sum`), sorted descending, row limit 10.
3. Scatter chart - speed vs satisfaction by cohort. Source
   `ds_gold_cohort_distributions`; X axis `avg_speed_kmh` (`Average`), Y axis
   `avg_satisfaction` (`Average`), points colored by `difficulty_tier`, size by
   `run_count` (`Sum`). Swap in `speed_bucket`, `weather_cond` or `season` for
   an alternative cohort view.

## 4. Refresh after Stage 3 reruns

Re-run `python src/stage3_hivesql_analytics.py`, then use `Replace file` on
each CSV connection and upload the refreshed file from `data/gold/csv/`.
DataLens field bindings survive as long as the header names are unchanged, so
do not rename mart columns.
