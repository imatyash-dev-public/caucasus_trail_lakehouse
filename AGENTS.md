# Arkhyz Big Data Pipeline — project conventions

Three-stage local pipeline: Stage 1 generates raw CSVs
(`src/stage1_prepare_data.py`, complete), Stage 2 is a PySpark ETL
(`src/stage2_pyspark_etl.py`, complete) that cleans/enriches/joins them into
Parquet under `data/processed/`, Stage 3 (`src/stage3_hivesql_analytics.py`)
runs HiveSQL analytics (CTEs, window functions, GROUP BY/HAVING via
`spark.sql(...)`) and materializes Gold marts under `data/gold/`.

## Layout

- `data/raw/` — inputs (`raw_osm_trails.csv`, `raw_srtm_elevation.csv`,
  `raw_rider_telemetry.csv` ~1.5 GB). Never modify.
- `data/processed/` — Stage 2 outputs (`dim_trails.parquet`,
  `fact_telemetry.parquet`, partitioned `region x activity_type`).
- `data/gold/` — Stage 3 outputs: `<mart>.parquet` (for Power BI) and
  `csv/<mart>.csv` single-file CSVs with headers (for Yandex DataLens).
- `plan.md` — stepwise implementation plan + progress log. Update the progress
  log after finishing a step.

## Verified environment (Windows, do not reconfigure)

- Python 3.13 (`python`), packages pinned in `requirements.txt`
  (`pyspark==4.3.0`, `pyarrow`, `pandas`, `numpy`, `requests`).
- JDK 17 (set `JAVA_HOME` to your JDK 17 home; no path is hardcoded in the repo).
- `src/stage2_pyspark_etl.py` auto-bootstraps `winutils.exe`/`hadoop.dll`
  into `<project>\.hadoop\bin` (cdarlint hadoop-3.3.6) — required on Windows
  for Parquet writes; keep that logic intact.
- Spark 4.3 gotchas: set `PYSPARK_PYTHON=sys.executable` (worker looks for
  `python3` otherwise); use `try_to_timestamp` (strict cast throws);
  set `spark.sql.session.timeZone=UTC`.

## Code style

- Match `src/stage1_prepare_data.py`: `str.format()` (no f-strings), `pathlib`,
  `argparse`, module docstring, `if __name__ == "__main__": main()`, no emojis,
  log lines prefixed with the stage tag (e.g. `[stage2]`, `[stage3]`).
- Stage 2 must use only native `pyspark.sql.functions` — no Python `@udf`s.
- Stage 3 keeps all analytics as Spark SQL / HiveQL strings via `spark.sql(...)`
  and reuses Stage 2's env helpers (`from stage2_pyspark_etl import
  prepare_windows_env, get_spark, ...`) — do not duplicate the bootstrap.

## Run

```
python src/stage2_pyspark_etl.py --check-env     # environment smoke test
python src/stage2_pyspark_etl.py                 # full ETL (~10-25 min)
python src/stage2_pyspark_etl.py --sample-rows 50000   # fast smoke run
python src/stage3_hivesql_analytics.py           # gold marts (~3-5 min)
python src/stage3_hivesql_analytics.py --explain # + Catalyst physical plans
```
