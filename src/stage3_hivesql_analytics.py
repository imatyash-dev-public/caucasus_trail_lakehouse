"""Stage 3 HiveSQL analytics for the Arkhyz trail big data pipeline.

Registers the Stage 2 Parquet tables as temporary views and materializes the
Gold marts as Parquet directories plus single-file CSVs (one file per mart,
with header) for Power BI and Yandex DataLens. All analytics are multi-line
Spark SQL / HiveQL strings executed through spark.sql(...); the Windows
JVM/Hadoop environment bootstrap is reused from stage2_pyspark_etl.
"""

import argparse
import pathlib
import shutil
import time

from pyspark.storagelevel import StorageLevel

from stage2_pyspark_etl import (
    dir_size_bytes,
    fmt_mb,
    get_spark,
    prepare_windows_env,
)

GOLD_TRAIL_LEADERBOARD_SQL = """
WITH trail_node_deltas AS (
    SELECT
        d.way_id, d.node_id, d.seq_order,
        MAX(d.trail_name) OVER (PARTITION BY d.way_id) AS trail_name,
        MAX(d.region) OVER (PARTITION BY d.way_id) AS region,
        MAX(d.highway_type) OVER (PARTITION BY d.way_id) AS highway_type,
        MAX(d.difficulty_tier) OVER (PARTITION BY d.way_id) AS difficulty_tier,
        MAX(d.surface_clean) OVER (PARTITION BY d.way_id) AS surface_clean,
        MAX(d.mtb_scale_clean) OVER (PARTITION BY d.way_id) AS mtb_scale_clean,
        MAX(d.sac_scale_clean) OVER (PARTITION BY d.way_id) AS sac_scale_clean,
        d.lat, d.lon, d.elevation_m,
        ROW_NUMBER() OVER (PARTITION BY d.way_id ORDER BY d.seq_order) AS rn_asc,
        ROW_NUMBER() OVER (PARTITION BY d.way_id ORDER BY d.seq_order DESC) AS rn_desc,
        d.elevation_m - LAG(d.elevation_m, 1)
            OVER (PARTITION BY d.way_id ORDER BY d.seq_order) AS delta_elev_m,
        LAG(d.lat, 1) OVER (PARTITION BY d.way_id ORDER BY d.seq_order) AS prev_lat,
        LAG(d.lon, 1) OVER (PARTITION BY d.way_id ORDER BY d.seq_order) AS prev_lon
    FROM dim_trails d
),
trail_topography AS (
    SELECT
        way_id, trail_name, region, highway_type, difficulty_tier,
        surface_clean, mtb_scale_clean, sac_scale_clean,
        COUNT(*) AS node_count,
        MAX(CASE WHEN rn_asc = 1 THEN elevation_m END) AS start_elev_m,
        MAX(CASE WHEN rn_desc = 1 THEN elevation_m END) AS end_elev_m,
        MIN(elevation_m) AS min_elev_m,
        MAX(elevation_m) AS max_elev_m,
        ROUND(SUM(CASE WHEN delta_elev_m < 0 THEN -delta_elev_m ELSE 0 END), 1)
            AS total_vertical_drop_m,
        ROUND(SUM(CASE WHEN delta_elev_m > 0 THEN delta_elev_m ELSE 0 END), 1)
            AS total_vertical_climb_m,
        ROUND(SUM(COALESCE(6371000.0 * RADIANS(SQRT(
            POWER((lon - prev_lon) * COS(RADIANS((lat + prev_lat) / 2.0)), 2)
            + POWER(lat - prev_lat, 2))), 0.0)), 1) AS trail_length_m
    FROM trail_node_deltas
    GROUP BY way_id, trail_name, region, highway_type, difficulty_tier,
             surface_clean, mtb_scale_clean, sac_scale_clean
),
trail_telemetry_stats AS (
    SELECT
        region, way_id,
        COUNT(*) AS total_pings,
        COUNT(DISTINCT run_id) AS unique_runs,
        COUNT(DISTINCT athlete_id) AS unique_athletes,
        ROUND(AVG(speed_kmh), 2) AS avg_speed_kmh,
        ROUND(MAX(speed_kmh), 2) AS max_speed_kmh,
        ROUND(AVG(heart_rate_bpm), 1) AS avg_heart_rate_bpm,
        ROUND(AVG(airtime_ms), 1) AS avg_airtime_ms,
        ROUND(MAX(airtime_ms), 1) AS max_airtime_ms,
        ROUND(AVG(g_force), 2) AS avg_g_force,
        ROUND(AVG(intensity_score), 2) AS avg_intensity_score,
        ROUND(AVG(rider_satisfaction), 2) AS avg_satisfaction
    FROM fact_telemetry
    GROUP BY region, way_id
    HAVING COUNT(*) >= 100
)
SELECT
    t.way_id, t.trail_name, t.region, t.highway_type, t.difficulty_tier,
    t.surface_clean, t.mtb_scale_clean, t.sac_scale_clean,
    t.node_count, t.start_elev_m, t.end_elev_m, t.min_elev_m, t.max_elev_m,
    t.total_vertical_drop_m, t.total_vertical_climb_m, t.trail_length_m,
    s.total_pings, s.unique_runs, s.unique_athletes,
    s.avg_speed_kmh, s.max_speed_kmh, s.avg_heart_rate_bpm,
    s.avg_airtime_ms, s.max_airtime_ms, s.avg_g_force,
    s.avg_intensity_score, s.avg_satisfaction,
    ROW_NUMBER() OVER (PARTITION BY t.region
        ORDER BY s.avg_satisfaction DESC, s.total_pings DESC) AS region_popularity_rownum,
    RANK() OVER (PARTITION BY t.region, t.difficulty_tier
        ORDER BY t.total_vertical_drop_m DESC) AS tier_drop_rank,
    DENSE_RANK() OVER (PARTITION BY t.region
        ORDER BY ROUND(s.avg_speed_kmh, 1) DESC) AS region_speed_denserank,
    ROUND(PERCENT_RANK() OVER (PARTITION BY t.region
        ORDER BY s.avg_intensity_score DESC), 4) AS intensity_percentile
FROM trail_topography t
JOIN trail_telemetry_stats s
  ON t.region = s.region AND t.way_id = s.way_id
ORDER BY t.region, region_popularity_rownum
"""

GOLD_TEMPORAL_TRENDS_SQL = """
WITH monthly_base AS (
    SELECT
        region, activity_type, year, month, season,
        COUNT(*) AS monthly_pings,
        COUNT(DISTINCT run_id) AS monthly_runs,
        ROUND(AVG(speed_kmh), 2) AS avg_speed_kmh,
        ROUND(AVG(heart_rate_bpm), 1) AS avg_heart_rate_bpm,
        ROUND(AVG(airtime_ms), 1) AS avg_airtime_ms,
        ROUND(AVG(intensity_score), 2) AS avg_intensity_score,
        ROUND(AVG(rider_satisfaction), 2) AS avg_satisfaction,
        ROUND(100.0 * AVG(CASE WHEN rider_satisfaction >= 4 THEN 1 ELSE 0 END), 2)
            AS pct_high_satisfaction
    FROM fact_telemetry
    GROUP BY region, activity_type, year, month, season
    HAVING COUNT(*) >= 50
)
SELECT
    region, activity_type, year, month, season,
    monthly_pings, monthly_runs,
    LAG(monthly_runs, 1) OVER (PARTITION BY region, activity_type
        ORDER BY year, month) AS prev_month_runs,
    ROUND(100.0 * (monthly_runs - LAG(monthly_runs, 1) OVER (PARTITION BY region, activity_type ORDER BY year, month))
        / LAG(monthly_runs, 1) OVER (PARTITION BY region, activity_type ORDER BY year, month), 2)
        AS mom_runs_growth_pct,
    avg_speed_kmh,
    ROUND(AVG(avg_speed_kmh) OVER (PARTITION BY region, activity_type
        ORDER BY year, month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2)
        AS rolling_3m_speed_kmh,
    avg_heart_rate_bpm, avg_airtime_ms, avg_intensity_score, avg_satisfaction,
    ROUND(AVG(avg_satisfaction) OVER (PARTITION BY region, activity_type
        ORDER BY year, month ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2)
        AS rolling_3m_satisfaction,
    pct_high_satisfaction,
    RANK() OVER (PARTITION BY region, year
        ORDER BY monthly_runs DESC) AS peak_month_rank
FROM monthly_base
ORDER BY region, activity_type, year, month
"""

GOLD_ARKHYZ_TRAIL_MAP_NODES_SQL = """
WITH top_trails AS (
    SELECT
        region, way_id,
        ROW_NUMBER() OVER (PARTITION BY region ORDER BY way_id ASC) AS activity_rn
    FROM (
        SELECT DISTINCT region, way_id
        FROM gold_trail_leaderboard
    ) lb
),
top_trail_nodes AS (
    SELECT
        d.region, d.way_id, d.node_id, d.seq_order, d.lat, d.lon, d.elevation_m,
        MAX(d.trail_name) OVER (PARTITION BY d.way_id) AS trail_name,
        MAX(d.difficulty_tier) OVER (PARTITION BY d.way_id) AS difficulty_tier,
        MAX(d.surface_clean) OVER (PARTITION BY d.way_id) AS surface_clean,
        d.elevation_m - LAG(d.elevation_m, 1)
            OVER (PARTITION BY d.way_id ORDER BY d.seq_order) AS delta_elev_m,
        6371000.0 * RADIANS(SQRT(
            POWER((d.lon - LAG(d.lon, 1) OVER (PARTITION BY d.way_id ORDER BY d.seq_order))
                * COS(RADIANS((d.lat + LAG(d.lat, 1) OVER (PARTITION BY d.way_id ORDER BY d.seq_order)) / 2.0)), 2)
            + POWER(d.lat - LAG(d.lat, 1) OVER (PARTITION BY d.way_id ORDER BY d.seq_order), 2)))
            AS segment_dist_m
    FROM dim_trails d
    JOIN top_trails tt
      ON d.region = tt.region AND d.way_id = tt.way_id
),
node_telemetry AS (
    SELECT
        way_id, node_id,
        COUNT(*) AS node_pings,
        ROUND(AVG(speed_kmh), 2) AS avg_speed_kmh,
        ROUND(AVG(airtime_ms), 1) AS avg_airtime_ms,
        ROUND(AVG(g_force), 2) AS avg_g_force,
        ROUND(AVG(heart_rate_bpm), 1) AS avg_heart_rate_bpm,
        ROUND(AVG(rider_satisfaction), 2) AS avg_satisfaction
    FROM fact_telemetry
    GROUP BY way_id, node_id
)
SELECT
    n.region, n.way_id, n.trail_name, n.difficulty_tier, n.surface_clean,
    n.seq_order, n.lat, n.lon, n.elevation_m,
    ROUND(SUM(COALESCE(n.segment_dist_m, 0.0)) OVER (PARTITION BY n.way_id
        ORDER BY n.seq_order ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 1)
        AS cumulative_dist_m,
    ROUND(100.0 * n.delta_elev_m / NULLIF(n.segment_dist_m, 0.0), 2) AS segment_grade_pct,
    COALESCE(nt.node_pings, 0) AS node_pings,
    nt.avg_speed_kmh, nt.avg_airtime_ms, nt.avg_g_force,
    nt.avg_heart_rate_bpm, nt.avg_satisfaction
FROM top_trail_nodes n
LEFT JOIN node_telemetry nt
  ON nt.way_id = n.way_id AND nt.node_id = n.node_id
ORDER BY n.region, n.way_id, n.seq_order
"""

GOLD_COHORT_DISTRIBUTIONS_SQL = """
WITH cohort_base AS (
    SELECT
        region, activity_type, difficulty_tier, weather_cond, speed_bucket, season,
        COUNT(*) AS ping_count,
        COUNT(DISTINCT run_id) AS run_count,
        ROUND(AVG(speed_kmh), 2) AS avg_speed_kmh,
        ROUND(AVG(heart_rate_bpm), 1) AS avg_heart_rate_bpm,
        ROUND(AVG(airtime_ms), 1) AS avg_airtime_ms,
        ROUND(AVG(g_force), 2) AS avg_g_force,
        ROUND(AVG(intensity_score), 2) AS avg_intensity_score,
        ROUND(AVG(rider_satisfaction), 2) AS avg_satisfaction
    FROM fact_telemetry
    GROUP BY region, activity_type, difficulty_tier, weather_cond, speed_bucket, season
    HAVING COUNT(*) >= 20
)
SELECT
    region, activity_type, difficulty_tier, weather_cond, speed_bucket, season,
    ping_count, run_count,
    SUM(ping_count) OVER (PARTITION BY region, activity_type) AS activity_total_pings,
    ROUND(100.0 * ping_count
        / SUM(ping_count) OVER (PARTITION BY region, activity_type), 4)
        AS share_of_activity_pct,
    avg_speed_kmh, avg_heart_rate_bpm, avg_airtime_ms, avg_g_force,
    avg_intensity_score, avg_satisfaction
FROM cohort_base
ORDER BY region, activity_type, ping_count DESC
"""

ARKHYZ_ACTIVITY_MIX_SQL = """
SELECT activity_type, COUNT(*) AS pings, ROUND(AVG(speed_kmh), 1) AS avg_speed_kmh
FROM fact_telemetry
WHERE region = 'Arkhyz'
GROUP BY activity_type
ORDER BY pings DESC
"""

MARTS = [
    ("gold_trail_leaderboard", GOLD_TRAIL_LEADERBOARD_SQL),
    ("gold_temporal_trends", GOLD_TEMPORAL_TRENDS_SQL),
    ("gold_arkhyz_trail_map_nodes", GOLD_ARKHYZ_TRAIL_MAP_NODES_SQL),
    ("gold_cohort_distributions", GOLD_COHORT_DISTRIBUTIONS_SQL),
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Stage 3 HiveSQL analytics and Gold mart materialization"
    )
    parser.add_argument("--processed-dir", default="data/processed")
    parser.add_argument("--gold-dir", default="data/gold")
    parser.add_argument("--sample", action="store_true", help="Run on data/sample fixtures (raw= data/sample, out= data/sample/processed)")
    parser.add_argument(
        "--explain",
        action="store_true",
        help="print the Catalyst physical plan for the leaderboard mart",
    )
    return parser.parse_args()


def register_source_views(spark, processed_dir):
    """Register the Stage 2 Parquet outputs as temporary SQL views."""
    processed_dir = pathlib.Path(processed_dir)
    spark.read.parquet(str(processed_dir / "dim_trails.parquet")).createOrReplaceTempView(
        "dim_trails"
    )
    spark.read.parquet(
        str(processed_dir / "fact_telemetry.parquet")
    ).createOrReplaceTempView("fact_telemetry")
    print(
        "[stage3] registered views: dim_trails, fact_telemetry (from {})".format(
            processed_dir
        )
    )


def write_single_csv(df, csv_path):
    """Write df as exactly one CSV file with header at csv_path."""
    csv_path = pathlib.Path(csv_path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_dir = csv_path.parent / ("_" + csv_path.stem + "_csv_tmp")
    if tmp_dir.exists():
        shutil.rmtree(str(tmp_dir))
    (df.coalesce(1).write.mode("overwrite").option("header", "true")
        .csv(str(tmp_dir)))
    part_files = sorted(tmp_dir.glob("part-*.csv"))
    if len(part_files) != 1:
        raise RuntimeError("expected exactly one CSV part file, got {}".format(len(part_files)))
    shutil.move(str(part_files[0]), str(csv_path))
    shutil.rmtree(str(tmp_dir))


def run_mart(spark, name, sql, gold_dir, explain=False):
    """Run one mart query and write its Parquet directory and single CSV."""
    gold_dir = pathlib.Path(gold_dir)
    t0 = time.monotonic()
    df = spark.sql(sql)
    if explain and name == "gold_trail_leaderboard":
        print("[stage3] physical plan for gold_trail_leaderboard:")
        df.explain("formatted")
    df = df.persist(StorageLevel.MEMORY_AND_DISK)
    df.write.mode("overwrite").parquet(str(gold_dir / (name + ".parquet")))
    write_single_csv(df, gold_dir / "csv" / (name + ".csv"))
    df.createOrReplaceTempView(name)
    rows = df.count()
    elapsed = time.monotonic() - t0
    print("[stage3] {name}: rows={rows} | took={elapsed:.1f}s".format(
        name=name, rows=rows, elapsed=elapsed
    ))
    return df


def print_console_insights(spark):
    """Print stage 3 console insights from the materialized Gold marts."""
    print("[stage3] top-5 Arkhyz trails by vertical drop:")
    spark.sql(
        "SELECT trail_name, total_vertical_drop_m, difficulty_tier, tier_drop_rank "
        "FROM gold_trail_leaderboard WHERE region = 'Arkhyz' "
        "ORDER BY total_vertical_drop_m DESC, trail_name LIMIT 5"
    ).show(truncate=False)

    print("[stage3] top-5 Arkhyz trails by avg speed:")
    spark.sql(
        "SELECT trail_name, avg_speed_kmh, region_speed_denserank "
        "FROM gold_trail_leaderboard WHERE region = 'Arkhyz' "
        "ORDER BY avg_speed_kmh DESC, trail_name LIMIT 5"
    ).show(truncate=False)

    print("[stage3] latest-month busiest region/activity combos:")
    spark.sql(
        "SELECT region, activity_type, monthly_runs, mom_runs_growth_pct, "
        "rolling_3m_satisfaction FROM gold_temporal_trends "
        "WHERE (year * 100 + month) = "
        "(SELECT MAX(year * 100 + month) FROM gold_temporal_trends) "
        "ORDER BY monthly_runs DESC LIMIT 5"
    ).show(truncate=False)

    print("[stage3] season totals:")
    spark.sql(
        "SELECT season, SUM(ping_count) AS pings, "
        "ROUND(AVG(avg_satisfaction), 2) AS avg_satisfaction "
        "FROM gold_cohort_distributions GROUP BY season ORDER BY pings DESC"
    ).show(truncate=False)


def print_explain_partition_pruning(spark):
    """Show partition pruning in the Catalyst plan."""
    print(
        "[stage3] physical plan for ARKHZYZ_ACTIVITY_MIX_SQL (expect PartitionFilters):"
    )
    spark.sql(ARKHYZ_ACTIVITY_MIX_SQL).explain("formatted")


def main():
    t0 = time.monotonic()
    args = parse_args()
    processed_dir = pathlib.Path(args.processed_dir)
    gold_dir = pathlib.Path(args.gold_dir)
    if args.sample:
        processed_dir = pathlib.Path("data/sample/processed")
        gold_dir = pathlib.Path("data/sample/gold")
    gold_dir.mkdir(parents=True, exist_ok=True)
    (gold_dir / "csv").mkdir(parents=True, exist_ok=True)
    project_root = pathlib.Path(__file__).resolve().parent.parent
    prepare_windows_env(project_root)
    spark = get_spark()
    print(
        "[stage3] spark {} | sources from {}".format(
            spark.version, processed_dir
        )
    )
    register_source_views(spark, processed_dir)

    marts = []
    for name, sql in MARTS:
        marts.append(run_mart(spark, name, sql, gold_dir, explain=args.explain))

    print_console_insights(spark)
    if args.explain:
        print_explain_partition_pruning(spark)

    for (name, _sql), df in zip(MARTS, marts):
        parquet_bytes = dir_size_bytes(gold_dir / (name + ".parquet"))
        csv_path = gold_dir / "csv" / (name + ".csv")
        csv_bytes = csv_path.stat().st_size if csv_path.exists() else 0
        print(
            "[stage3] {name}: parquet {parquet_mb} MB | csv {csv_mb} MB".format(
                name=name,
                parquet_mb=fmt_mb(parquet_bytes),
                csv_mb=fmt_mb(csv_bytes),
            )
        )
        df.unpersist()

    elapsed = time.monotonic() - t0
    print(
        "[stage3] gold marts complete | {n} marts | total {t:.1f}s".format(
            n=len(MARTS), t=elapsed
        )
    )
    spark.stop()


if __name__ == "__main__":
    main()
