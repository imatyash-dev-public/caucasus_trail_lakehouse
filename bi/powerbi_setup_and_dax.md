# Power BI Setup and DAX Guide - Arkhyz Gold Marts

This guide connects Power BI Desktop to the Stage 3 gold marts under
`data\gold\` and reproduces the four reference visuals
of the BI kit with native DAX measures.

Prerequisites:

- Power BI Desktop with the built-in Parquet connector (no extra driver).
- Stage 3 completed, so the mart directories below exist:
  - `data\gold\gold_trail_leaderboard`
  - `data\gold\gold_temporal_trends`
  - `data\gold\gold_arkhyz_trail_map_nodes`
  - `data\gold\gold_cohort_distributions`

If the directories are missing, rebuild them first:

```
python src/stage3_hivesql_analytics.py
```

## 1. Connect to the gold marts (Parquet)

One query per mart directory. Power BI's Parquet connector reads a Spark
output directory natively, including mart directories that Spark wrote as
partitioned folders (`region`, `activity_type`).

1. Open Power BI Desktop and start a blank report.
2. `Home` -> `Get Data` -> `More...` -> `File` -> `Parquet` -> `Connect`.
3. In the file dialog, enter or browse to the mart **directory**, e.g.
   `data\gold\gold_trail_leaderboard`, and confirm.
4. In the Navigator preview, select the mart (the `part-*.parquet` files of
   the directory, or the folder node that groups them) and press
   `Transform Data` (or `Load`).
5. In Power Query, click `Combine` / `Combine Files` when the preview lists
   several part files or partition subfolders. The combined query then shows
   the full schema of the mart.
6. Rename the query to the mart name (`gold_trail_leaderboard`,
   `gold_temporal_trends`, `gold_arkhyz_trail_map_nodes`,
   `gold_cohort_distributions`), then `Close & Apply`.
7. Repeat steps 2-6 for the remaining three mart directories.

No custom M code is needed: do not build `Folder.Files` + `Parquet.Document`
chains, and do not use the Delta format option. The plain Parquet connector
plus `Combine Files` is the supported path for these Spark outputs.

Sanity check after loading: `gold_temporal_trends` must show 18 columns
starting with `region`, `activity_type`, `year`, `month`; the leaderboard
must expose `total_vertical_drop_m`.

## 2. Apply the Arkhyz Alpine theme

1. `View` -> `Themes` -> `Browse for themes`.
2. Select `bi\arkhyz_powerbi_theme.json` (relative to the project root).
3. Confirm the palette in `View` -> `Themes` -> `Customize current theme`.

The theme sets the six-colour Alpine series order
(`#1B4332`, `#00B4D8`, `#F77F00`, `#2B2D42`, `#8AB17D`, `#E9C46A`), a white
`background`, the `#2B2D42` `foreground`, and the `#1B4332` `tableAccent`.
Keep a copy of the JSON next to the `.pbix` file so the report remains
portable; otherwise re-browse to the path above after moving the project.

## 3. Build the model (star schema)

`gold_trail_leaderboard` is the primary table: one row per trail (`way_id`),
with trail attributes and trail-level metrics. The other marts are related
tables at coarser grains.

1. Create relationships in `Model view` by dragging the key column of one
   table onto the matching column of another.
2. `gold_trail_leaderboard` <-> `gold_arkhyz_trail_map_nodes` on `way_id`
   (many-to-one, single filter direction: nodes filter from the leaderboard).
   Use `region` as a second, optional join key.
3. `gold_trail_leaderboard` <-> `gold_temporal_trends` on `region`
   (many-to-many, single filter direction).
4. `gold_trail_leaderboard` <-> `gold_cohort_distributions` on `region`
   (many-to-many, single filter direction).
5. `gold_temporal_trends` <-> `gold_cohort_distributions` on `region` and
   `activity_type` (the only pair of marts that both carry `activity_type`).

Model notes:

- `gold_trail_leaderboard` has **no** `activity_type` column. Its cohort
  column is `difficulty_tier`; do not try to join leaderboard to
  `activity_type`-bearing marts on `activity_type`.
- Region-only joins are many-to-many because `region` is not unique in any
  mart. Set the cardinality explicitly to many-to-many with a single filter
  direction (leaderboard-side filters flow to the related tables, not back).
- If Power BI warns about ambiguous paths, keep only the `region` path between
  `gold_temporal_trends` and `gold_cohort_distributions` and deactivate the
  redundant direct paths from the leaderboard to those two tables.
- Mark `gold_temporal_trends[year]` and `[month]` as whole numbers (they are
  integers in the CSV) so axis aggregation stays additive.

## 4. DAX measures

Create each measure with `Home` -> `New measure` (or right-click the
`gold_temporal_trends` table -> `New measure`) and paste the exact text.
Column names below match the verified CSV headers of the gold marts.

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

Formatting hints:

- `Weighted Avg Satisfaction` -> Decimal number, 2 decimals.
- `Total Runs` and `Max Vertical Drop (m)` -> Whole number with thousands
  separator.
- `MoM Run Growth %` and `Arkhyz Speed Premium vs Caucasus` -> Percentage,
  1 decimal.
- Slice `MoM Run Growth %` by `month` to see a single month; unfiltered it is
  a ratio of totals across all months.

## 5. Suggested visuals (4)

1. **Monthly runs line** - `Line chart`.
   - Axis: `gold_temporal_trends[year]` + `gold_temporal_trends[month]`
     (or a `Month = year * 100 + month` calculated column used as the axis).
   - Legend: `gold_temporal_trends[activity_type]`.
   - Values: `Total Runs`.
   - Filter: `region` = `Arkhyz`.
2. **Speed vs satisfaction scatter** - `Scatter chart`.
   - X axis: `gold_trail_leaderboard[avg_speed_kmh]`.
   - Y axis: `gold_trail_leaderboard[avg_satisfaction]`.
   - Details: `gold_trail_leaderboard[trail_name]`.
   - Legend: `gold_trail_leaderboard[difficulty_tier]` (the leaderboard has no
     `activity_type`).
3. **Top-10 vertical drop bar** - `Clustered bar chart` (horizontal).
   - Y axis: `gold_trail_leaderboard[trail_name]`.
   - X axis: `Max Vertical Drop (m)` or
     `SUM(gold_trail_leaderboard[total_vertical_drop_m])`.
   - Visual-level filter on `trail_name`: Top N = 10 by
     `total_vertical_drop_m`; sort descending.
4. **Satisfaction heatmap by region and month** - `Matrix`.
   - Rows: `gold_temporal_trends[region]`.
   - Columns: `gold_temporal_trends[month]`.
   - Values: `Weighted Avg Satisfaction`.
   - Conditional formatting: `Background color` -> gradient from `#FFFFFF`
     to `#1B4332`, applied to the Values field.

Optional KPI cards: `Total Runs`, `Weighted Avg Satisfaction`,
`Max Vertical Drop (m)`, `MoM Run Growth %` on a header row of the report
page, matching the B1 preview dashboard KPIs.
