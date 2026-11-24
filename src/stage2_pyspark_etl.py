"""Stage 2 PySpark ETL for the Arkhyz MTB and hiking big data pipeline.

Bootstraps a Windows JVM/Hadoop environment (JAVA_HOME probe, winutils.exe and
hadoop.dll download), builds a local SparkSession, and cleans, enriches and
joins the raw Stage 1 CSVs into Parquet tables under the output directory.
Only native pyspark.sql.functions expressions are used (no Python UDFs).
"""

import argparse
import os
import pathlib
import platform
import shutil
import sys
import time
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

SAC_LABELS = [
    "T1_Hiking",
    "T2_Mountain_Hiking",
    "T3_Demanding_Mountain_Hiking",
    "T4_Alpine_Hiking",
    "T5_Demanding_Alpine_Hiking",
    "T6_Difficult_Alpine_Hiking",
]
WINUTILS_BASE_URLS = [  # each appended with "/winutils.exe" and "/hadoop.dll"
    "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin",
    "https://raw.githubusercontent.com/kontext-tech/winutils/master/hadoop-3.3.6/bin",
]
WINUTILS_FILES = ["winutils.exe", "hadoop.dll"]
WINUTILS_MIN_EXE_BYTES = 50000
WINUTILS_DOWNLOAD_TIMEOUT_SECONDS = 30

WINUTILS_HOME = None
ELEVATION_METRICS = {}
TELEMETRY_PERSISTED_DF = None


def parse_args():
    parser = argparse.ArgumentParser(
        description="Stage 2 PySpark ETL for the Arkhyz trail pipeline"
    )
    parser.add_argument("--raw-dir", default="data/raw")
    parser.add_argument("--out-dir", default="data/processed")
    parser.add_argument(
        "--sample-rows",
        type=int,
        default=0,
        help="limit telemetry rows for smoke tests (0 = all rows)",
    )
    parser.add_argument("--sample", action="store_true", help="Run on data/sample fixtures (raw= data/sample, out= data/sample/processed)")
    parser.add_argument(
        "--check-env",
        action="store_true",
        help="verify the Java, winutils and Spark environment and exit",
    )
    return parser.parse_args()


def resolve_java_home():
    """Locate a JDK installation and export it through JAVA_HOME."""
    java_home = os.environ.get("JAVA_HOME")
    if java_home and (pathlib.Path(java_home) / "bin" / "java.exe").exists():
        return java_home
    userprofile = os.environ.get("USERPROFILE")
    if userprofile:
        jdk_matches = sorted(
            pathlib.Path(userprofile).glob(".jdk/jdk*"), reverse=True
        )
        for candidate in jdk_matches:
            if (candidate / "bin" / "java.exe").exists():
                os.environ["JAVA_HOME"] = str(candidate)
                return str(candidate)
    for jdk_parent in (
        "C:/Program Files/Java",
        "C:/Program Files/Eclipse Adoptium",
    ):
        parent = pathlib.Path(jdk_parent)
        if not parent.exists():
            continue
        for candidate in sorted(parent.glob("jdk*"), reverse=True):
            if (candidate / "bin" / "java.exe").exists():
                os.environ["JAVA_HOME"] = str(candidate)
                return str(candidate)
    java_on_path = shutil.which("java")
    if java_on_path:
        print("[stage2] java found on PATH: {}".format(java_on_path))
        return None
    sys.exit("[stage2] ERROR: no Java 17+ runtime found; set JAVA_HOME or install a JDK")


def ensure_winutils(project_root):
    """Locate or download winutils.exe/hadoop.dll and export HADOOP_HOME."""
    if platform.system() != "Windows":
        return None
    candidates = []
    hadoop_env = os.environ.get("HADOOP_HOME")
    if hadoop_env:
        candidates.append(pathlib.Path(hadoop_env))
    candidates.append(pathlib.Path(project_root) / ".hadoop")
    userprofile = os.environ.get("USERPROFILE")
    if userprofile:
        candidates.append(pathlib.Path(userprofile) / ".hadoop")
    hadoop_home = None
    for candidate in candidates:
        if (candidate / "bin" / "winutils.exe").exists():
            hadoop_home = candidate
            break
    if hadoop_home is None:
        bin_dir = pathlib.Path(project_root) / ".hadoop" / "bin"
        bin_dir.mkdir(parents=True, exist_ok=True)
        for base_url in WINUTILS_BASE_URLS:
            ok = True
            for name in WINUTILS_FILES:
                target = bin_dir / name
                if target.exists() and target.stat().st_size > 0:
                    continue
                url = "{}/{}".format(base_url, name)
                print("[stage2] downloading {}".format(url))
                try:
                    with urllib.request.urlopen(
                        url, timeout=WINUTILS_DOWNLOAD_TIMEOUT_SECONDS
                    ) as response:
                        with open(str(target), "wb") as handle:
                            shutil.copyfileobj(response, handle)
                except (OSError, ValueError) as exc:
                    print("[stage2] download failed: {}".format(exc))
                    ok = False
                    break
            exe_path = bin_dir / "winutils.exe"
            if ok and not (
                exe_path.exists()
                and exe_path.stat().st_size > WINUTILS_MIN_EXE_BYTES
                and (bin_dir / "hadoop.dll").exists()
            ):
                print("[stage2] downloaded winutils files look incomplete")
                ok = False
            if ok:
                hadoop_home = bin_dir.parent
                break
            for name in WINUTILS_FILES:
                target = bin_dir / name
                try:
                    if target.exists():
                        target.unlink()
                except OSError:
                    pass
    if hadoop_home is None:
        print(
            "[stage2] WARNING: winutils.exe unavailable; attempting no-winutils fallback"
        )
        return None
    os.environ["HADOOP_HOME"] = str(hadoop_home)
    winutils_bin = str(hadoop_home / "bin")
    current_path = os.environ.get("PATH", "")
    if current_path:
        os.environ["PATH"] = winutils_bin + os.pathsep + current_path
    else:
        os.environ["PATH"] = winutils_bin
    print("[stage2] winutils ready: {}".format(winutils_bin))
    return str(hadoop_home)


def prepare_windows_env(project_root):
    global WINUTILS_HOME
    resolve_java_home()
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    WINUTILS_HOME = ensure_winutils(project_root)
    print(
        "[stage2] env prepared | java_home={} | winutils={}".format(
            os.environ.get("JAVA_HOME"), WINUTILS_HOME
        )
    )


def get_spark():
    builder = (
        SparkSession.builder.master("local[*]")
        .appName("Stage2ArkhyzPysparkETL")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
    )
    if platform.system() == "Windows" and WINUTILS_HOME is None:
        builder = builder.config(
            "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2"
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def clean_elevation(spark, raw_dir):
    """Step 1: drop void/spike elevations and dedup the SRTM grid to 3 decimals."""
    df = spark.read.csv(
        str(pathlib.Path(raw_dir) / "raw_srtm_elevation.csv"),
        header=True,
        schema="grid_lat double, grid_lon double, elevation_m int, sensor_source string",
        mode="PERMISSIVE",
    )
    raw_rows = df.count()
    df = df.filter(F.col("elevation_m").between(300, 5642))
    valid_rows = df.count()
    ELEVATION_METRICS["elev_raw_rows"] = raw_rows
    ELEVATION_METRICS["elev_valid_rows"] = valid_rows
    df = (
        df.withColumn("grid_lat", F.round("grid_lat", 3))
        .withColumn("grid_lon", F.round("grid_lon", 3))
        .groupBy("grid_lat", "grid_lon")
        .agg(
            F.round(F.avg("elevation_m")).cast("int").alias("elevation_m"),
            F.first("sensor_source").alias("sensor_source"),
        )
    )
    return df


def build_dim_trails(spark, raw_dir, out_dir):
    """Step 2: clean OSM tags, join the elevation grid and write dim_trails.parquet."""
    raw_dir = pathlib.Path(raw_dir)
    out_dir = pathlib.Path(out_dir)

    osm = spark.read.csv(
        str(raw_dir / "raw_osm_trails.csv"),
        header=True,
        schema=(
            "way_id long, node_id long, seq_order int, trail_name string, "
            "region string, highway_type string, mtb_scale_raw string, "
            "sac_scale_raw string, surface_raw string, incline_raw string, "
            "lat double, lon double, osm_timestamp string, data_source string"
        ),
        mode="PERMISSIVE",
    )
    metrics = {}
    metrics["osm_raw_rows"] = osm.count()

    osm = osm.dropDuplicates(["way_id", "node_id", "seq_order"])
    metrics["osm_dedup_rows"] = osm.count()

    osm = osm.withColumn(
        "trail_name",
        F.when(
            F.length(F.trim(F.col("trail_name"))) > 0, F.trim(F.col("trail_name"))
        ).otherwise(F.concat(F.lit("Unnamed_Trail_"), F.col("way_id").cast("string"))),
    )

    mtb_num = F.regexp_extract(F.trim(F.col("mtb_scale_raw")), r"^([0-6])", 1)
    osm = osm.withColumn(
        "mtb_scale_clean",
        F.when(mtb_num != "", mtb_num.cast("int")).otherwise(F.lit(None).cast("int")),
    )

    sac_num = F.regexp_extract(
        F.lower(F.trim(F.col("sac_scale_raw"))), r"t\s*([1-6])", 1
    )
    sac_l = F.lower(F.trim(F.col("sac_scale_raw")))
    sac_array = F.array(*[F.lit(label) for label in SAC_LABELS])
    osm = osm.withColumn(
        "sac_scale_clean",
        F.when(sac_num != "", F.element_at(sac_array, sac_num.cast("int")))
        .when(sac_l.contains("difficult_alpine"), F.lit("T6_Difficult_Alpine_Hiking"))
        .when(sac_l.contains("demanding_alpine"), F.lit("T5_Demanding_Alpine_Hiking"))
        .when(sac_l.contains("alpine"), F.lit("T4_Alpine_Hiking"))
        .when(sac_l.contains("demanding_mountain"), F.lit("T3_Demanding_Mountain_Hiking"))
        .when(sac_l.contains("mountain"), F.lit("T2_Mountain_Hiking"))
        .when(sac_l.contains("hiking"), F.lit("T1_Hiking"))
        .otherwise(F.lit("Unspecified")),
    )

    sf = F.lower(F.trim(F.col("surface_raw")))
    osm = osm.withColumn(
        "surface_clean",
        F.when(sf.contains("rock") | sf.contains("root"), F.lit("technical_natural"))
        .when(
            sf.contains("dirt") | sf.contains("ground") | sf.contains("earth"),
            F.lit("dirt"),
        )
        .when(sf.contains("gravel") | sf.contains("compacted"), F.lit("gravel"))
        .when(sf.contains("asphalt") | sf.contains("paved"), F.lit("paved"))
        .otherwise(F.lit("natural_unpaved")),
    )

    inc_num = F.regexp_extract(
        F.trim(F.col("incline_raw")), r"^([-+]?\d+(?:\.\d+)?)", 1
    )
    inc_l = F.lower(F.trim(F.col("incline_raw")))
    osm = osm.withColumn(
        "incline_pct",
        F.when(inc_num != "", inc_num.cast("double"))
        .when(inc_l.contains("steep"), F.lit(20.0))
        .when(inc_l.contains("down"), F.lit(-10.0))
        .when(inc_l.contains("up"), F.lit(10.0))
        .otherwise(F.lit(0.0)),
    )

    osm = osm.withColumn("lat3", F.round("lat", 3)).withColumn(
        "lon3", F.round("lon", 3)
    )

    elev = (
        clean_elevation(spark, raw_dir)
        .withColumnRenamed("grid_lat", "lat3")
        .withColumnRenamed("grid_lon", "lon3")
    )
    metrics["elev_raw_rows"] = ELEVATION_METRICS.get("elev_raw_rows")
    metrics["elev_valid_rows"] = ELEVATION_METRICS.get("elev_valid_rows")
    metrics["elev_unique_cells"] = elev.count()

    dim = osm.join(elev, ["lat3", "lon3"], "left")

    matched = dim.filter(F.col("elevation_m").isNotNull())
    regional = matched.groupBy("region").agg(
        F.percentile_approx("elevation_m", 0.5).alias("regional_median_elev")
    )
    globalm = matched.agg(
        F.percentile_approx("elevation_m", 0.5).alias("global_median_elev")
    )
    dim = dim.join(F.broadcast(regional), "region", "left").crossJoin(
        F.broadcast(globalm)
    )

    elev_filled_by_median = dim.filter(F.col("elevation_m").isNull()).count()

    dim = dim.withColumn(
        "elevation_m",
        F.coalesce(
            F.col("elevation_m"),
            F.round("regional_median_elev").cast("int"),
            F.round("global_median_elev").cast("int"),
        ),
    )
    dim = dim.drop("regional_median_elev", "global_median_elev")

    tier = (
        F.when(
            (F.col("mtb_scale_clean") >= 5)
            | (F.col("sac_scale_clean") == "T6_Difficult_Alpine_Hiking")
            | (F.abs("incline_pct") >= 25),
            F.lit("DoubleBlack_Pro"),
        )
        .when(
            F.col("mtb_scale_clean").between(3, 4)
            | F.col("sac_scale_clean").startswith("T4")
            | F.col("sac_scale_clean").startswith("T5")
            | (F.abs("incline_pct") >= 15),
            F.lit("Black_Advanced"),
        )
        .when(
            F.col("mtb_scale_clean").between(1, 2)
            | F.col("sac_scale_clean").startswith("T2")
            | F.col("sac_scale_clean").startswith("T3")
            | (F.abs("incline_pct") >= 8),
            F.lit("Blue_Intermediate"),
        )
        .otherwise(F.lit("Green_Easy"))
    )
    dim = dim.withColumn("difficulty_tier", tier)

    final_cols = [
        "way_id",
        "node_id",
        "seq_order",
        "trail_name",
        "region",
        "highway_type",
        "mtb_scale_raw",
        "mtb_scale_clean",
        "sac_scale_raw",
        "sac_scale_clean",
        "surface_raw",
        "surface_clean",
        "incline_raw",
        "incline_pct",
        "lat",
        "lon",
        "elevation_m",
        "difficulty_tier",
        "osm_timestamp",
        "data_source",
    ]
    dim = dim.select(*final_cols)

    dim_rows = dim.count()

    metrics["dim_rows"] = dim_rows
    metrics["elev_filled_by_median"] = elev_filled_by_median

    dim.coalesce(4).write.mode("overwrite").parquet(
        str(out_dir / "dim_trails.parquet")
    )

    ordered_metrics = {
        "osm_raw_rows": metrics["osm_raw_rows"],
        "osm_dedup_rows": metrics["osm_dedup_rows"],
        "elev_raw_rows": metrics["elev_raw_rows"],
        "elev_valid_rows": metrics["elev_valid_rows"],
        "elev_unique_cells": metrics["elev_unique_cells"],
        "dim_rows": metrics["dim_rows"],
        "elev_filled_by_median": metrics["elev_filled_by_median"],
    }

    return dim, ordered_metrics


def clean_telemetry(spark, raw_dir, sample_rows):
    """Step 3a: clean the raw rider telemetry and count every applied rule."""
    global TELEMETRY_PERSISTED_DF
    raw_dir = pathlib.Path(raw_dir)

    df = spark.read.csv(
        str(raw_dir / "raw_rider_telemetry.csv"),
        header=True,
        schema=(
            "telemetry_id string, run_id string, athlete_id string, "
            "activity_type string, way_id long, node_id long, timestamp string, "
            "lat_gps double, lon_gps double, speed_kmh double, "
            "heart_rate_bpm int, airtime_ms int, g_force double, "
            "weather_cond string, rider_satisfaction int"
        ),
        mode="PERMISSIVE",
    )
    if sample_rows > 0:
        df = df.limit(sample_rows)

    metrics = {}
    metrics["raw_rows"] = df.count()

    df = df.dropDuplicates(["telemetry_id"])
    df = df.persist(StorageLevel.MEMORY_AND_DISK)
    TELEMETRY_PERSISTED_DF = df
    metrics["after_dedup"] = df.count()

    df = df.withColumn("timestamp", F.try_to_timestamp("timestamp"))
    df = df.filter(
        F.col("timestamp").isNotNull() & F.year("timestamp").between(2023, 2026)
    )
    metrics["after_timestamp"] = df.count()

    df = df.filter(
        F.col("lat_gps").between(43.0, 44.0) & F.col("lon_gps").between(40.0, 42.0)
    )
    metrics["after_gps"] = df.count()

    df = df.filter((F.col("speed_kmh") > 0.5) & (F.col("speed_kmh") <= 95.0))
    metrics["after_speed"] = df.count()

    df = df.filter(
        F.col("airtime_ms").between(0, 6000) & F.col("g_force").between(0.1, 8.0)
    )
    metrics["after_physics"] = df.count()

    hr_invalid = (
        F.col("heart_rate_bpm").isNull()
        | (F.col("heart_rate_bpm") < 40)
        | (F.col("heart_rate_bpm") > 220)
    )
    metrics["imputed_heart_rate"] = df.filter(hr_invalid).count()
    df = df.withColumn(
        "heart_rate_bpm",
        F.when(hr_invalid & (F.col("activity_type") == "MTB_DH"), F.lit(155))
        .when(hr_invalid & (F.col("activity_type") == "MTB_Enduro"), F.lit(162))
        .when(hr_invalid & (F.col("activity_type") == "Trail_Run"), F.lit(148))
        .when(hr_invalid & (F.col("activity_type") == "Hiking"), F.lit(122))
        .otherwise(F.col("heart_rate_bpm")),
    )

    weather_missing = F.col("weather_cond").isNull() | (
        F.trim(F.col("weather_cond")) == ""
    )
    metrics["imputed_weather"] = df.filter(weather_missing).count()
    df = df.withColumn(
        "weather_cond",
        F.when(weather_missing, F.lit("Sunny")).otherwise(F.col("weather_cond")),
    )

    sat_invalid = F.col("rider_satisfaction").isNull() | ~F.col(
        "rider_satisfaction"
    ).between(1, 5)
    metrics["imputed_satisfaction"] = df.filter(sat_invalid).count()
    df = df.withColumn(
        "rider_satisfaction",
        F.when(sat_invalid, F.lit(4)).otherwise(F.col("rider_satisfaction")),
    )

    return df, metrics


def enrich_telemetry(fact_df, dim_df):
    """Step 3b: derive features and broadcast-join the trail dimension."""
    df = fact_df.withColumn(
        "speed_bucket",
        F.when(F.col("speed_kmh") < 8.0, F.lit("Slow_Climb"))
        .when(F.col("speed_kmh") < 20.0, F.lit("Cruising"))
        .when(F.col("speed_kmh") < 35.0, F.lit("Fast_Flow"))
        .otherwise(F.lit("High_Speed_Send")),
    )

    norm_speed = F.least(
        F.greatest(F.col("speed_kmh") / F.lit(95.0), F.lit(0.0)), F.lit(1.0)
    )
    norm_hr = F.least(
        F.greatest(
            (F.col("heart_rate_bpm") - F.lit(60)) / F.lit(140.0), F.lit(0.0)
        ),
        F.lit(1.0),
    )
    norm_g = F.least(
        F.greatest((F.col("g_force") - F.lit(0.1)) / F.lit(7.9), F.lit(0.0)),
        F.lit(1.0),
    )
    df = df.withColumn(
        "intensity_score",
        F.round(
            100.0
            * (
                F.lit(0.4) * norm_speed
                + F.lit(0.4) * norm_hr
                + F.lit(0.2) * norm_g
            ),
            1,
        ),
    )

    df = (
        df.withColumn("year", F.year("timestamp"))
        .withColumn("month", F.month("timestamp"))
        .withColumn(
            "season",
            F.when(F.col("month").isin(12, 1, 2), F.lit("Winter"))
            .when(F.col("month").isin(3, 4, 5), F.lit("Spring"))
            .when(F.col("month").isin(6, 7, 8), F.lit("Summer"))
            .otherwise(F.lit("Autumn")),
        )
    )

    dim_slim = dim_df.select(
        "way_id",
        "node_id",
        "region",
        "trail_name",
        "difficulty_tier",
        "surface_clean",
        "elevation_m",
    ).dropDuplicates(["way_id", "node_id"])
    df = df.join(F.broadcast(dim_slim), ["way_id", "node_id"], "left")

    final_cols = [
        "telemetry_id",
        "run_id",
        "athlete_id",
        "activity_type",
        "way_id",
        "node_id",
        "timestamp",
        "lat_gps",
        "lon_gps",
        "speed_kmh",
        "speed_bucket",
        "heart_rate_bpm",
        "airtime_ms",
        "g_force",
        "intensity_score",
        "weather_cond",
        "rider_satisfaction",
        "year",
        "month",
        "season",
        "region",
        "trail_name",
        "difficulty_tier",
        "surface_clean",
        "elevation_m",
    ]
    return df.select(*final_cols)


def write_fact_parquet(fact_df, out_dir):
    """Step 3c: write the partitioned fact table."""
    out_dir = pathlib.Path(out_dir)
    (
        fact_df.repartition(F.col("region"), F.col("activity_type"))
        .write.mode("overwrite")
        .partitionBy("region", "activity_type")
        .option("compression", "snappy")
        .parquet(str(out_dir / "fact_telemetry.parquet"))
    )


def dir_size_bytes(path):
    path = pathlib.Path(path)
    if not path.exists():
        return 0
    total = 0
    for entry in path.rglob("*"):
        try:
            if entry.is_file():
                total += entry.stat().st_size
        except OSError:
            continue
    return total


def fmt_mb(n_bytes):
    return "{:.1f}".format(n_bytes / (1024 ** 2))


def print_audit_report(metrics, elapsed_s, raw_tel_path, out_dir):
    """Print the final ETL audit table."""
    raw_tel_path = pathlib.Path(raw_tel_path)
    out_dir = pathlib.Path(out_dir)
    raw_bytes = raw_tel_path.stat().st_size if raw_tel_path.exists() else 0
    parquet_bytes = dir_size_bytes(out_dir / "fact_telemetry.parquet")
    if parquet_bytes > 0:
        ratio = "{:.1f}x".format(raw_bytes / float(parquet_bytes))
    else:
        ratio = "n/a"

    raw_rows = metrics.get("raw_rows", 0)
    after_dedup = metrics.get("after_dedup", 0)
    after_timestamp = metrics.get("after_timestamp", 0)
    after_gps = metrics.get("after_gps", 0)
    after_speed = metrics.get("after_speed", 0)
    after_physics = metrics.get("after_physics", 0)

    lines = [
        "=== Stage 2 ETL Audit Report ===",
        "Raw telemetry rows            : {}".format(raw_rows),
        "Dropped duplicate telemetry_id: {}".format(raw_rows - after_dedup),
        "Dropped bad timestamps        : {}".format(after_dedup - after_timestamp),
        "Dropped GPS outliers          : {}".format(after_timestamp - after_gps),
        "Dropped speed outliers        : {}".format(after_gps - after_speed),
        "Dropped airtime/gforce outliers: {}".format(after_speed - after_physics),
        "Imputed heart_rate_bpm        : {}".format(
            metrics.get("imputed_heart_rate", 0)
        ),
        "Imputed weather_cond          : {}".format(metrics.get("imputed_weather", 0)),
        "Imputed rider_satisfaction    : {}".format(
            metrics.get("imputed_satisfaction", 0)
        ),
        "Cleaned telemetry rows        : {}".format(after_physics),
        "dim_trails rows               : {}   (elevation filled by regional median: {})".format(
            metrics.get("dim_rows", 0), metrics.get("elev_filled_by_median", 0)
        ),
        "Execution time                : {} s".format(int(elapsed_s)),
        "Raw telemetry CSV size        : {} MB".format(fmt_mb(raw_bytes)),
        "Fact parquet size on disk     : {} MB".format(fmt_mb(parquet_bytes)),
        "Compression ratio (raw/parquet): {}".format(ratio),
    ]
    for line in lines:
        print("[stage2] {}".format(line))


def main():
    t0 = time.monotonic()
    args = parse_args()
    raw_dir = pathlib.Path(args.raw_dir)
    out_dir = pathlib.Path(args.out_dir)
    if args.sample:
        raw_dir = pathlib.Path("data/sample")
        out_dir = pathlib.Path("data/sample/processed")
    out_dir.mkdir(parents=True, exist_ok=True)
    project_root = pathlib.Path(__file__).resolve().parent.parent
    prepare_windows_env(project_root)
    spark = get_spark()
    if args.check_env:
        print(
            "[stage2] check-env OK | java_home={} | winutils={} | spark={}".format(
                os.environ.get("JAVA_HOME"), WINUTILS_HOME, spark.version
            )
        )
        spark.stop()
        return
    print(
        "[stage2] spark {} | master {} | shuffle partitions {}".format(
            spark.version,
            spark.sparkContext.master,
            spark.conf.get("spark.sql.shuffle.partitions"),
        )
    )

    dim, dim_metrics = build_dim_trails(spark, raw_dir, out_dir)
    for key, value in dim_metrics.items():
        print("[stage2] dim: {} = {}".format(key, value))
    print(
        "[stage2] dim_trails.parquet written ({} MB)".format(
            fmt_mb(dir_size_bytes(out_dir / "dim_trails.parquet"))
        )
    )

    fact, tel_metrics = clean_telemetry(spark, raw_dir, args.sample_rows)
    fact = enrich_telemetry(fact, dim)
    write_fact_parquet(fact, out_dir)
    fact.unpersist()
    if TELEMETRY_PERSISTED_DF is not None:
        TELEMETRY_PERSISTED_DF.unpersist()

    metrics = {}
    metrics.update(dim_metrics)
    metrics.update(tel_metrics)
    print_audit_report(
        metrics,
        time.monotonic() - t0,
        raw_dir / "raw_rider_telemetry.csv",
        out_dir,
    )
    spark.stop()


if __name__ == "__main__":
    main()
