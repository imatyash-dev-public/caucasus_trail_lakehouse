"""Portable end-to-end smoke test for the sample pipeline.

Runs the full Bronze -> Silver -> Gold pipeline on the tracked
``data/sample/`` fixtures via subprocess:

1. ``python src/stage2_pyspark_etl.py --sample``  (Silver: dim + fact)
2. ``python src/stage3_hivesql_analytics.py --sample``  (Gold: 4 marts + CSVs)

Then asserts every expected output exists and is non-empty, and that the
whole sample run stays under a hard 180 s bound. The measured runtime on
the reference machine is about 141 s (Spark/JVM startup dominates), so the
bound is generous on purpose and is not a performance target.

Run: python -m pytest tests/test_sample_smoke.py -v
"""

import pathlib
import subprocess
import sys
import time

import pytest

PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent

SAMPLE_DIR = PROJECT_ROOT / "data" / "sample"
PROCESSED_DIR = SAMPLE_DIR / "processed"
GOLD_DIR = SAMPLE_DIR / "gold"
CSV_DIR = GOLD_DIR / "csv"

STAGE2_SCRIPT = "src/stage2_pyspark_etl.py"
STAGE3_SCRIPT = "src/stage3_hivesql_analytics.py"

ELAPSED_HARD_BOUND_S = 180
TAIL_CHARS = 4000


def _tail(text):
    """Return the last TAIL_CHARS characters of captured process output."""
    if not text:
        return "<empty>"
    return text[-TAIL_CHARS:]


def _run_stage(script, label):
    """Run one stage script in --sample mode and return the CompletedProcess."""
    command = [sys.executable, script, "--sample"]
    print("[smoke] running: {0}".format(" ".join(command)))
    result = subprocess.run(
        command,
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print("[smoke] {0} stdout tail:\n{1}".format(label, _tail(result.stdout)))
        print("[smoke] {0} stderr tail:\n{1}".format(label, _tail(result.stderr)))
    return result


def _assert_spark_output(path):
    """Assert a Spark output exists and holds a part file or _SUCCESS marker."""
    assert path.exists(), "[smoke] missing output: {0}".format(path)
    if path.is_dir():
        files = [item for item in path.rglob("*") if item.is_file()]
        has_part = any(item.name.startswith("part-") for item in files)
        has_success = any(item.name == "_SUCCESS" for item in files)
        assert has_part or has_success, (
            "[smoke] output directory has no part file or _SUCCESS: {0}".format(path)
        )
        if has_part:
            non_empty = [
                item for item in files
                if item.name.startswith("part-") and item.stat().st_size > 0
            ]
            assert non_empty, "[smoke] all part files are empty: {0}".format(path)
    else:
        assert path.stat().st_size > 0, "[smoke] output file is empty: {0}".format(path)


@pytest.mark.sample
def test_sample_pipeline_end_to_end():
    """Run stage2 + stage3 on data/sample and assert every output exists."""
    for required_raw in (
        SAMPLE_DIR / "raw_osm_trails.csv",
        SAMPLE_DIR / "raw_srtm_elevation.csv",
        SAMPLE_DIR / "raw_rider_telemetry.csv",
    ):
        assert required_raw.exists(), (
            "[smoke] sample fixture missing before run: {0}".format(required_raw)
        )

    t0 = time.perf_counter()

    stage2 = _run_stage(STAGE2_SCRIPT, "stage2")
    assert stage2.returncode == 0, (
        "[smoke] stage2 failed with exit code {0}\n"
        "stdout tail:\n{1}\n"
        "stderr tail:\n{2}".format(
            stage2.returncode, _tail(stage2.stdout), _tail(stage2.stderr)
        )
    )

    stage3 = _run_stage(STAGE3_SCRIPT, "stage3")
    assert stage3.returncode == 0, (
        "[smoke] stage3 failed with exit code {0}\n"
        "stdout tail:\n{1}\n"
        "stderr tail:\n{2}".format(
            stage3.returncode, _tail(stage3.stdout), _tail(stage3.stderr)
        )
    )

    parquet_outputs = (
        PROCESSED_DIR / "dim_trails.parquet",
        PROCESSED_DIR / "fact_telemetry.parquet",
        GOLD_DIR / "gold_trail_leaderboard.parquet",
        GOLD_DIR / "gold_temporal_trends.parquet",
        GOLD_DIR / "gold_arkhyz_trail_map_nodes.parquet",
        GOLD_DIR / "gold_cohort_distributions.parquet",
    )
    for parquet_output in parquet_outputs:
        _assert_spark_output(parquet_output)

    assert CSV_DIR.exists(), "[smoke] missing CSV directory: {0}".format(CSV_DIR)
    csv_files = sorted(CSV_DIR.glob("*.csv"))
    assert len(csv_files) == 4, (
        "[smoke] expected exactly 4 CSV files in {0}, found {1}: {2}".format(
            CSV_DIR,
            len(csv_files),
            ", ".join(item.name for item in csv_files) or "<none>",
        )
    )
    for csv_file in csv_files:
        assert csv_file.stat().st_size > 0, (
            "[smoke] CSV file is empty: {0}".format(csv_file)
        )

    elapsed = time.perf_counter() - t0
    print("[smoke] sample pipeline elapsed {0:.1f} s".format(elapsed))

    assert elapsed < ELAPSED_HARD_BOUND_S, (
        "[smoke] sample pipeline took {0:.1f} s, exceeding the hard bound of "
        "{1} s".format(elapsed, ELAPSED_HARD_BOUND_S)
    )
