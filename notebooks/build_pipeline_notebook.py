"""Build and execute the Arkhyz big data pipeline notebook.

Constructs ``notebooks/arkhyz_bigdata_pipeline.ipynb`` as a plain nbformat 4
JSON dict (no nbformat/nbclient dependency), then executes the code cells in a
fresh Jupyter kernel through jupyter_client and stores the captured outputs
(streams, execute results, inline PNG figures and errors) back into the
notebook file.

The notebook documents the three pipeline stages: raw CSV lake inspection,
Stage 2 PySpark ETL outputs (Parquet dim/fact), and Stage 3 HiveSQL gold marts,
plus a matplotlib BI preview of the gold CSV exports.
"""

import argparse
import json
import pathlib
import sys
import time

from jupyter_client.manager import start_new_kernel

OUTPUT_NAME = "arkhyz_bigdata_pipeline.ipynb"
CELL_TIMEOUT_S = 900

INTRO_MD = """
# Arkhyz Big Data Pipeline - End-to-End Walkthrough

This notebook walks through the local lakehouse pipeline built for the Arkhyz
trail and rider telemetry dataset: from the raw CSV lake, through the PySpark
ETL, to HiveSQL analytics marts.

## Lakehouse architecture

```
data/raw/ (CSV lake)
        -> Stage 2  PySpark ETL (clean, enrich, join)
data/processed/ (Parquet: dim_trails + fact_telemetry, partitioned by region x activity_type)
        -> Stage 3  HiveSQL (CTEs, window functions, GROUP BY / HAVING)
data/gold/ (Parquet marts for Power BI + single-file CSVs in data/gold/csv for DataLens)
```

- **Stage 1** (`src/stage1_prepare_data.py`) generates the raw CSV lake:
  OSM trail nodes, an SRTM elevation grid and rider telemetry, with
  deterministic injected data quality anomalies.
- **Stage 2** (`src/stage2_pyspark_etl.py`) cleans the tags and voids, joins
  trails with elevation and telemetry, and writes `dim_trails.parquet` plus a
  `fact_telemetry.parquet` table partitioned by `region` and `activity_type`.
- **Stage 3** (`src/stage3_hivesql_analytics.py`) registers the Parquet tables
  as temp views, runs HiveSQL analytics (CTEs, window functions, GROUP BY /
  HAVING) and materializes the gold marts as Parquet and single-file CSV.

## Environment

Verified local setup: Python 3.13, **PySpark 4.3.0**, **JDK 17**, Windows.
The Windows Hadoop helpers (`winutils.exe`, `hadoop.dll`) are auto-bootstrapped
into `<project>/.hadoop/bin` by `stage2_pyspark_etl.prepare_windows_env`, which
this notebook reuses instead of duplicating the JVM setup.

Run this notebook from the project root so the relative `data/...` paths and
the `src` imports resolve.
"""

ENV_CODE = """
import pathlib
import sys

import pandas as pd

PROJECT_ROOT = pathlib.Path.cwd()
GOLD_CSV_DIR = PROJECT_ROOT / "data" / "gold" / "csv"
if not GOLD_CSV_DIR.exists():
    raise RuntimeError(
        "data/gold/csv not found under {}. Run this notebook from the project "
        "root (the folder that contains data/ and src/).".format(PROJECT_ROOT)
    )

SRC_DIR = str(PROJECT_ROOT / "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from stage2_pyspark_etl import prepare_windows_env, get_spark

prepare_windows_env(PROJECT_ROOT)
spark = get_spark()

print("[notebook] project root : {}".format(PROJECT_ROOT))
print("[notebook] Spark version: {}".format(spark.version))
for key in (
    "spark.master",
    "spark.sql.session.timeZone",
    "spark.sql.shuffle.partitions",
    "spark.sql.parquet.compression.codec",
    "spark.sql.execution.arrow.pyspark.enabled",
):
    print("  {} = {}".format(key, spark.conf.get(key, "<unset>")))
"""

STAGE1_MD = """
## Stage 1 - raw CSV lake inspection

Stage 1 produces three raw CSVs under `data/raw/`:

| file | rows | content |
| --- | --- | --- |
| `raw_osm_trails.csv` | 75,833 | OSM trail nodes: way/node ids, tags, coordinates |
| `raw_srtm_elevation.csv` | 154,129 | modeled SRTM elevation grid per lat/lon cell |
| `raw_rider_telemetry.csv` | 12,034,509 (~1.5 GB) | rider telemetry pings: GPS, speed, HR, airtime |

The two small files are read fully with pandas; the 1.5 GB telemetry file is
only touched through `nrows=` previews and a `chunksize=` streaming row count,
so it is never loaded into memory as a single DataFrame.

### Injected data quality anomalies

Trail tags (OSM): `mtb_scale_raw`, `sac_scale_raw`, `surface_raw` and
`incline_raw` carry junk values in otherwise empty cells for **3%** of rows
(e.g. `2+`, `steep`, `unknown`, `paved-unpaved`).

Elevation (SRTM): **1%** void cells at `-32768`, **0.5%** spike cells at
`99999`, and about **0.5%** duplicated grid rows.

Telemetry:

- `hr_null` **1.5%** missing heart rate
- `weather_null` **0.8%** missing weather condition
- `sat_null` **1.0%** missing satellite/GPS quality
- `hr_outlier` **0.3%** impossible values (`-1`, `0`, `350`)
- `speed_outlier` **0.2%** impossible values (`-15.0`, `299.9`)
- `airtime_outlier` **0.2%** impossible values (`-500`, `20000`)
- `lat_zero` **0.15%** zero latitude (`lat_gps = 0.0`)
- `bad_timestamp` **0.25%** (`SYNC_ERROR`, `1970-01-01`, `NULL`)
- `dup_rows` **1.5%** duplicated telemetry rows
"""

STAGE1_CODE = """
import time

RAW_DIR = PROJECT_ROOT / "data" / "raw"


def file_size_mb(path):
    return path.stat().st_size / (1024.0 * 1024.0)


print("[stage1] --- raw_osm_trails.csv ---")
osm_path = RAW_DIR / "raw_osm_trails.csv"
osm_df = pd.read_csv(str(osm_path))
print("  rows    : {:,}".format(len(osm_df)))
print("  columns : {}".format(", ".join(osm_df.columns)))
print("  size    : {:.1f} MB".format(file_size_mb(osm_path)))
print(osm_df.head(3).to_string())
del osm_df

print("")
print("[stage1] --- raw_srtm_elevation.csv ---")
srtm_path = RAW_DIR / "raw_srtm_elevation.csv"
srtm_df = pd.read_csv(str(srtm_path))
print("  rows    : {:,}".format(len(srtm_df)))
print("  columns : {}".format(", ".join(srtm_df.columns)))
print("  size    : {:.1f} MB".format(file_size_mb(srtm_path)))
print(srtm_df.head(3).to_string())
del srtm_df

print("")
print("[stage1] --- raw_rider_telemetry.csv (streamed, never loaded fully) ---")
telemetry_path = RAW_DIR / "raw_rider_telemetry.csv"
telemetry_columns = list(pd.read_csv(str(telemetry_path), nrows=0).columns)
print("  columns : {}".format(", ".join(telemetry_columns)))
print("[stage1] header + first 5 rows:")
print(pd.read_csv(str(telemetry_path), nrows=5).to_string())

row_count = 0
chunk_count = 0
t0 = time.monotonic()
for chunk in pd.read_csv(
    str(telemetry_path), chunksize=500000, usecols=["telemetry_id"]
):
    row_count += len(chunk)
    chunk_count += 1
elapsed = time.monotonic() - t0

print("[stage1] chunked row count: {:,} rows in {} chunks ({:.1f}s)".format(
    row_count, chunk_count, elapsed
))
print("  size    : {:.1f} MB".format(file_size_mb(telemetry_path)))
"""

STAGE2_CODE = """
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
dim_trails_path = PROCESSED_DIR / "dim_trails.parquet"
fact_telemetry_path = PROCESSED_DIR / "fact_telemetry.parquet"

dim_trails = spark.read.parquet(str(dim_trails_path))
fact_telemetry = spark.read.parquet(str(fact_telemetry_path))

print("[stage2] --- dim_trails.parquet ---")
dim_trails.printSchema()
print("[stage2] dim_trails rows    : {:,}".format(dim_trails.count()))

print("")
print("[stage2] --- fact_telemetry.parquet ---")
fact_telemetry.printSchema()
print("[stage2] fact_telemetry rows: {:,}".format(fact_telemetry.count()))

print("")
print("[stage2] partitions under {}".format(fact_telemetry_path))
region_dirs = sorted(
    p for p in fact_telemetry_path.iterdir()
    if p.is_dir() and p.name.startswith("region=")
)
leaf_count = 0
for region_dir in region_dirs:
    activity_dirs = sorted(
        p for p in region_dir.iterdir()
        if p.is_dir() and p.name.startswith("activity_type=")
    )
    leaf_count += len(activity_dirs)
    print("[stage2] {} -> {}".format(
        region_dir.name, ", ".join(d.name for d in activity_dirs)
    ))
print("[stage2] leaf partitions (region x activity_type): {}".format(leaf_count))


def dir_size_bytes(path):
    total = 0
    for entry in path.rglob("*"):
        if entry.is_file():
            total += entry.stat().st_size
    return total


raw_telemetry_bytes = (RAW_DIR / "raw_rider_telemetry.csv").stat().st_size
fact_parquet_bytes = dir_size_bytes(fact_telemetry_path)
ratio = raw_telemetry_bytes / float(fact_parquet_bytes)

print("")
print("[stage2] compression audit (raw CSV vs snappy Parquet):")
print("  raw telemetry CSV : {:.1f} MB".format(raw_telemetry_bytes / (1024.0 * 1024.0)))
print("  fact parquet      : {:.1f} MB".format(fact_parquet_bytes / (1024.0 * 1024.0)))
print("  compression ratio (raw / parquet): {:.2f}x".format(ratio))
"""

STAGE3_CODE = """
from stage3_hivesql_analytics import (
    ARKHYZ_ACTIVITY_MIX_SQL,
    GOLD_TEMPORAL_TRENDS_SQL,
    GOLD_TRAIL_LEADERBOARD_SQL,
)

spark.read.parquet(str(dim_trails_path)).createOrReplaceTempView("dim_trails")
spark.read.parquet(str(fact_telemetry_path)).createOrReplaceTempView(
    "fact_telemetry"
)
print("[stage3] temp views registered: dim_trails, fact_telemetry")

pd.set_option("display.max_columns", 40)
pd.set_option("display.width", 240)

print("[stage3] gold_temporal_trends preview (.limit(20).toPandas()):")
trends_preview = spark.sql(GOLD_TEMPORAL_TRENDS_SQL).limit(20).toPandas()
print(trends_preview.to_string())

print("")
print("[stage3] gold_trail_leaderboard preview (.limit(20).toPandas()):")
board_preview = spark.sql(GOLD_TRAIL_LEADERBOARD_SQL).limit(20).toPandas()
print(board_preview.to_string())

print("")
print("[stage3] physical plan for gold_temporal_trends (expect Window nodes):")
spark.sql(GOLD_TEMPORAL_TRENDS_SQL).explain("formatted")

print("")
print("[stage3] physical plan for ARKHYZ_ACTIVITY_MIX_SQL (expect PartitionFilters):")
spark.sql(ARKHYZ_ACTIVITY_MIX_SQL).explain("formatted")
"""

STAGE4_MD = """
## Stage 4 - BI preview from the gold marts

Stage 3 materialized the gold marts twice: as Parquet directories under
`data/gold/` (loaded by Power BI) and as single-file CSVs under
`data/gold/csv/` (uploaded to Yandex DataLens). This stage reproduces two of
the BI visuals inline, directly from the CSV exports with pandas and
matplotlib:

1. **Monthly runs line chart** - `monthly_runs` over time for the `Arkhyz`
   region, one line per `activity_type` (from `gold_temporal_trends`).
2. **Speed vs satisfaction scatter** - `avg_speed_kmh` vs `avg_satisfaction`
   for every trail, colored by `difficulty_tier` (from
   `gold_trail_leaderboard`).

The kernel's inline matplotlib backend renders `plt.show()` as PNG
`display_data`, so the figures are saved inside this notebook.
"""

STAGE4_CODE = """
import matplotlib.pyplot as plt

GOLD_CSV_DIR = PROJECT_ROOT / "data" / "gold" / "csv"
PALETTE = ["#1B4332", "#00B4D8", "#F77F00", "#2B2D42", "#8AB17D", "#E9C46A"]

trends_df = pd.read_csv(str(GOLD_CSV_DIR / "gold_temporal_trends.csv"))
leaderboard_df = pd.read_csv(str(GOLD_CSV_DIR / "gold_trail_leaderboard.csv"))
print("[stage4] loaded gold CSVs: gold_temporal_trends rows={:,} | "
      "gold_trail_leaderboard rows={:,}".format(len(trends_df), len(leaderboard_df)))

arkhyz = trends_df[trends_df["region"] == "Arkhyz"].copy()
pivot = arkhyz.pivot_table(
    index=["year", "month"],
    columns="activity_type",
    values="monthly_runs",
    aggfunc="sum",
).sort_index()
month_labels = ["{}-{:02d}".format(int(y), int(m)) for y, m in pivot.index]

fig, ax = plt.subplots(figsize=(10, 6))
for i, activity in enumerate(sorted(pivot.columns)):
    ax.plot(
        month_labels,
        pivot[activity].values,
        marker="o",
        linewidth=2,
        color=PALETTE[i % len(PALETTE)],
        label=activity,
    )
ax.set_title("Arkhyz monthly runs by activity type")
ax.set_xlabel("month")
ax.set_ylabel("monthly_runs")
step = max(1, len(month_labels) // 12)
ax.set_xticks(range(0, len(month_labels), step))
ax.set_xticklabels(month_labels[::step], rotation=45, ha="right")
ax.grid(True, alpha=0.3)
ax.legend(title="activity_type")
fig.tight_layout()

fig2, ax2 = plt.subplots(figsize=(10, 6))
tiers = sorted(leaderboard_df["difficulty_tier"].dropna().unique())
for i, tier in enumerate(tiers):
    subset = leaderboard_df[leaderboard_df["difficulty_tier"] == tier]
    ax2.scatter(
        subset["avg_speed_kmh"],
        subset["avg_satisfaction"],
        s=30,
        alpha=0.7,
        color=PALETTE[i % len(PALETTE)],
        label=str(tier),
    )
ax2.set_title("Trail leaderboard: avg speed vs avg satisfaction by difficulty tier")
ax2.set_xlabel("avg_speed_kmh")
ax2.set_ylabel("avg_satisfaction")
ax2.grid(True, alpha=0.3)
ax2.legend(title="difficulty_tier", fontsize=8)
fig2.tight_layout()

print("[stage4] figures built: monthly runs line + speed/satisfaction scatter")
plt.show()
"""

CLOSING_MD = """
## Next steps

- **Power BI** - follow `bi/powerbi_setup_and_dax.md`: connect to the
  `data/gold/gold_*.parquet` directories, import
  `bi/arkhyz_powerbi_theme.json`, and add the documented DAX measures
  (Total Runs, Weighted Avg Satisfaction, Max Vertical Drop, MoM Run Growth).
- **Yandex DataLens** - follow `bi/datalens_setup_guide.md`: upload the four
  `data/gold/csv/*.csv` files, build the datasets and assemble the line, bar
  and scatter charts.
- **Local HTML preview** - open `bi/arkhyz_interactive_dashboard.html` for the
  self-contained dashboard with the exported PNG figures and KPI cards.

All downstream consumers read the same gold marts produced by Stage 3.
"""


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Build (and optionally execute) the Arkhyz pipeline walkthrough "
            "notebook"
        )
    )
    parser.add_argument(
        "--execute",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="execute the notebook in a Jupyter kernel (default: on)",
    )
    return parser.parse_args()


def text_cell(source):
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": source.splitlines(True),
    }


def code_cell(source):
    return {
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": source.splitlines(True),
    }


def build_notebook():
    cells = [
        text_cell(INTRO_MD),
        code_cell(ENV_CODE),
        text_cell(STAGE1_MD),
        code_cell(STAGE1_CODE),
        code_cell(STAGE2_CODE),
        code_cell(STAGE3_CODE),
        text_cell(STAGE4_MD),
        code_cell(STAGE4_CODE),
        text_cell(CLOSING_MD),
    ]
    return {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {"name": "python"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def normalize_text(value):
    if isinstance(value, list):
        return "".join(value)
    return str(value)


def normalize_data(data):
    normalized = {}
    for key, value in data.items():
        if key.startswith("text/"):
            normalized[key] = normalize_text(value)
        else:
            normalized[key] = value
    return normalized


def start_kernel(project_root):
    try:
        km, kc = start_new_kernel(
            kernel_name="python3", cwd=str(project_root)
        )
        return km, kc, "python3 kernelspec"
    except Exception as exc:
        print(
            "[notebook] python3 kernelspec unavailable ({}), retrying with "
            "explicit ipykernel arguments".format(exc)
        )
        km, kc = start_new_kernel(
            kernel_name="python3",
            cwd=str(project_root),
            extra_arguments=["-m", "ipykernel", "-f", "{connection_file}"],
        )
        return km, kc, "explicit ipykernel arguments"


def wait_for_reply(kc, msg_id, timeout):
    while True:
        msg = kc.get_shell_msg(timeout=timeout)
        if msg.get("parent_header", {}).get("msg_id") == msg_id:
            return msg


def collect_cell_output(kc, msg_id):
    outputs = []
    has_error = False
    while True:
        msg = kc.get_iopub_msg(timeout=CELL_TIMEOUT_S)
        if msg.get("parent_header", {}).get("msg_id") != msg_id:
            continue
        msg_type = msg.get("msg_type")
        content = msg.get("content", {})
        if msg_type == "status":
            if content.get("execution_state") == "idle":
                break
        elif msg_type == "stream":
            outputs.append(
                {
                    "output_type": "stream",
                    "name": content.get("name", "stdout"),
                    "text": normalize_text(content.get("text", "")),
                }
            )
        elif msg_type == "execute_result":
            data = normalize_data(content.get("data", {}))
            outputs.append(
                {
                    "output_type": "execute_result",
                    "data": data,
                    "metadata": content.get("metadata", {}),
                    "execution_count": content.get("execution_count"),
                }
            )
        elif msg_type == "display_data":
            data = content.get("data", {})
            if "image/png" in data:
                outputs.append(
                    {
                        "output_type": "display_data",
                        "data": {
                            "image/png": normalize_text(data["image/png"]),
                            "text/plain": normalize_text(
                                data.get("text/plain", "<Figure>")
                            ),
                        },
                        "metadata": content.get("metadata", {}),
                    }
                )
            else:
                outputs.append(
                    {
                        "output_type": "display_data",
                        "data": normalize_data(data),
                        "metadata": content.get("metadata", {}),
                    }
                )
        elif msg_type == "error":
            has_error = True
            outputs.append(
                {
                    "output_type": "error",
                    "ename": content.get("ename", ""),
                    "evalue": content.get("evalue", ""),
                    "traceback": list(content.get("traceback", [])),
                }
            )
    reply = wait_for_reply(kc, msg_id, CELL_TIMEOUT_S)
    return outputs, reply, has_error


def count_png_outputs(notebook):
    count = 0
    for cell in notebook["cells"]:
        for output in cell.get("outputs", []):
            if output.get("output_type") == "display_data":
                if "image/png" in output.get("data", {}):
                    count += 1
    return count


def shutdown_kernel(km, kc):
    if kc is not None:
        try:
            kc.stop_channels()
        except Exception as exc:
            print("[notebook] WARNING channel stop failed: {}".format(exc))
    if km is not None:
        try:
            km.shutdown_kernel(now=True)
        except Exception as exc:
            print("[notebook] WARNING kernel shutdown failed: {}".format(exc))


def write_notebook(notebook, output_path):
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        json.dump(notebook, handle, indent=1, ensure_ascii=False)
        handle.write("\n")


def execute_notebook(notebook, project_root, output_path):
    km = None
    kc = None
    has_error = False
    execution_counter = 0
    try:
        km, kc, mode = start_kernel(project_root)
        print("[notebook] kernel started ({})".format(mode))

        code_index = 0
        for cell in notebook["cells"]:
            if cell.get("cell_type") != "code":
                continue
            code_index += 1
            code = "".join(cell.get("source", []))
            print(
                "[notebook] executing code cell {} ({} chars)".format(
                    code_index, len(code)
                )
            )
            started = time.monotonic()
            msg_id = kc.execute(code, allow_stdin=False, store_history=True)
            outputs, reply, cell_error = collect_cell_output(kc, msg_id)
            elapsed = time.monotonic() - started
            reply_content = reply.get("content", {})
            execution_counter = (
                reply_content.get("execution_count") or execution_counter + 1
            )
            cell["execution_count"] = execution_counter
            cell["outputs"] = outputs
            print(
                "[notebook] code cell {} finished in {:.1f}s "
                "(execution_count={}, status={})".format(
                    code_index,
                    elapsed,
                    execution_counter,
                    reply_content.get("status", "unknown"),
                )
            )
            if cell_error:
                has_error = True
                print(
                    "[notebook] ERROR code cell {} reported an error output".format(
                        code_index
                    )
                )
                break
            if reply_content.get("status") != "ok":
                has_error = True
                print(
                    "[notebook] ERROR code cell {} reply status={}".format(
                        code_index, reply_content.get("status")
                    )
                )
                break

        if not has_error:
            png_count = count_png_outputs(notebook)
            if png_count == 0:
                print(
                    "[notebook] WARNING no image/png output was captured for the "
                    "Stage 4 chart cell"
                )
            else:
                print(
                    "[notebook] captured {} image/png output(s)".format(png_count)
                )
    except Exception as exc:
        has_error = True
        print("[notebook] ERROR {}: {}".format(type(exc).__name__, exc))
    finally:
        shutdown_kernel(km, kc)
        write_notebook(notebook, output_path)
        print("[notebook] notebook written: {}".format(output_path))

    return has_error


def main():
    args = parse_args()
    project_root = pathlib.Path(__file__).resolve().parent.parent
    output_path = project_root / "notebooks" / OUTPUT_NAME

    print("[notebook] building notebook cells")
    notebook = build_notebook()
    print(
        "[notebook] {} cells ({} markdown, {} code)".format(
            len(notebook["cells"]),
            sum(1 for c in notebook["cells"] if c["cell_type"] == "markdown"),
            sum(1 for c in notebook["cells"] if c["cell_type"] == "code"),
        )
    )

    if not args.execute:
        write_notebook(notebook, output_path)
        print("[notebook] execution skipped (--no-execute)")
        print("[notebook] notebook written: {}".format(output_path))
        return 0

    has_error = execute_notebook(notebook, project_root, output_path)
    if has_error:
        print("[notebook] ERROR execution finished with errors")
        return 1
    print("[notebook] execution complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
