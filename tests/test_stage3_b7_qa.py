"""QA verification tests for Stage 3 Step B7 (Arkhyz big data pipeline).

Checks the Stage 3 contract from plan.md "Step B7 (QA)":

1. Full re-run exits 0 and prints all mart/insight/size/completion lines.
2. --explain prints a Window operator and a PartitionFilters demo with
   `(region = Arkhyz)`.
3. Gold artifacts: 4 parquet dirs (non-empty parts), 4 single CSV files
   (UTF-8, header, rows + 1 lines), no leftover `_..._csv_tmp` dirs.
4. Content invariants of the four marts, read with pyarrow.
5. Source contract: pure spark.sql analytics, required SQL constructs,
   stage2 env reuse, --explain wiring, code style.

Run: python -m pytest tests/test_stage3_b7_qa.py -v
"""

import pathlib
import re
import subprocess
import sys
import tempfile

import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest

PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
STAGE3 = PROJECT_ROOT / "src" / "stage3_hivesql_analytics.py"
GOLD = PROJECT_ROOT / "data" / "gold"
LOG_DIR = pathlib.Path(tempfile.gettempdir()) / "kilo"

EXPECTED_ROWS = {
    "gold_trail_leaderboard": 3933,
    "gold_temporal_trends": 552,
    "gold_arkhyz_trail_map_nodes": 75833,
    "gold_cohort_distributions": 1923,
}
ROW_TOLERANCE = 5

INSIGHT_HEADERS = [
    "[stage3] top-5 Arkhyz trails by vertical drop:",
    "[stage3] top-5 Arkhyz trails by avg speed:",
    "[stage3] latest-month busiest region/activity combos:",
    "[stage3] season totals:",
]


def run_stage3(*extra):
    return subprocess.run(
        [sys.executable, str(STAGE3), *extra],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=1200,
    )


@pytest.fixture(scope="session")
def runs():
    """Run stage3 once normally and once with --explain (cached per session)."""
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    plain = run_stage3()
    (LOG_DIR / "qa_pytest_run.log").write_text(plain.stdout + plain.stderr, encoding="utf-8")
    explained = run_stage3("--explain")
    (LOG_DIR / "qa_pytest_explain.log").write_text(
        explained.stdout + explained.stderr, encoding="utf-8"
    )
    return {"plain": plain, "explain": explained}


@pytest.fixture(scope="session")
def run_log(runs):
    assert runs["plain"].returncode == 0, "stage3 full run failed:\n" + (
        runs["plain"].stdout + runs["plain"].stderr
    )[-4000:]
    return runs["plain"].stdout + runs["plain"].stderr


@pytest.fixture(scope="session")
def explain_log(runs):
    assert runs["explain"].returncode == 0, "stage3 --explain run failed:\n" + (
        runs["explain"].stdout + runs["explain"].stderr
    )[-4000:]
    return runs["explain"].stdout + runs["explain"].stderr


def read_mart(name):
    return pq.read_table(str(GOLD / (name + ".parquet")))


# ---------------------------------------------------------------- check 1

def test_full_run_exit_and_mart_rows(run_log):
    pattern = re.compile(r"\[stage3\] (gold_\w+): rows=(\d+) \| took=([\d.]+)s")
    found = {m.group(1): (int(m.group(2)), float(m.group(3))) for m in pattern.finditer(run_log)}
    assert set(found) == set(EXPECTED_ROWS), "mart rows lines: {}".format(found)
    for name, expected in EXPECTED_ROWS.items():
        actual = found[name][0]
        assert abs(actual - expected) <= ROW_TOLERANCE, "{} rows={} expected {}".format(
            name, actual, expected
        )


def test_full_run_insight_tables(run_log):
    lines = run_log.splitlines()
    for header in INSIGHT_HEADERS:
        assert header in lines, "missing insight header: {}".format(header)
        idx = lines.index(header)
        following = [ln for ln in lines[idx + 1 : idx + 4] if ln.strip()]
        assert following and following[0].lstrip().startswith("+"), (
            "no table rows printed after insight header {!r}: {!r}".format(header, following[:2])
        )


def test_full_run_size_and_completion_lines(run_log):
    size_matches = re.findall(
        r"\[stage3\] (gold_\w+): parquet [\d.]+ MB \| csv [\d.]+ MB", run_log
    )
    assert sorted(size_matches) == sorted(EXPECTED_ROWS), "size lines: {}".format(size_matches)
    complete = re.search(
        r"\[stage3\] gold marts complete \| 4 marts \| total ([\d.]+)s", run_log
    )
    assert complete, "missing completion line"
    assert float(complete.group(1)) > 0


# ---------------------------------------------------------------- check 2

def test_explain_has_window_and_partition_filters(explain_log):
    assert "physical plan for gold_trail_leaderboard:" in explain_log
    assert "Window" in explain_log.split("physical plan for ARKHZYZ_ACTIVITY_MIX_SQL")[0], (
        "no Window operator in the leaderboard plan"
    )
    demo = explain_log.split("physical plan for ARKHZYZ_ACTIVITY_MIX_SQL", 1)[1]
    assert "PartitionFilters:" in demo, "no PartitionFilters in the demo plan"
    filter_line = next(ln for ln in demo.splitlines() if "PartitionFilters:" in ln)
    assert re.search(r"\(region(#\d+)? = Arkhyz\)", filter_line), filter_line


# ---------------------------------------------------------------- check 3

def test_parquet_artifacts():
    dirs = sorted(
        p for p in GOLD.iterdir() if p.is_dir() and p.name.endswith(".parquet")
    )
    names = {p.name[: -len(".parquet")] for p in dirs}
    assert names == set(EXPECTED_ROWS), "parquet dirs: {}".format(sorted(names))
    for path in dirs:
        name = path.name[: -len(".parquet")]
        parts = list(path.glob("*.parquet"))
        assert parts, "{} has no part files".format(name)
        assert all(p.stat().st_size > 0 for p in parts), "{} has empty part files".format(name)
        rows = pq.read_table(str(path)).num_rows
        assert abs(rows - EXPECTED_ROWS[name]) <= ROW_TOLERANCE, "{} rows={}".format(name, rows)


def test_csv_artifacts():
    csv_dir = GOLD / "csv"
    files = sorted(p for p in csv_dir.iterdir() if p.is_file())
    assert {p.name for p in files} == {n + ".csv" for n in EXPECTED_ROWS}, (
        "csv files: {}".format([p.name for p in files])
    )
    for path in files:
        name = path.stem
        raw = path.read_bytes()
        text = raw.decode("utf-8")  # raises if not valid UTF-8
        assert not raw.startswith(b"\xef\xbb\xbf"), "{} has a UTF-8 BOM".format(name)
        lines = text.splitlines()
        rows = pq.read_table(str(GOLD / (name + ".parquet"))).num_rows
        assert lines, "{} is empty".format(name)
        header = lines[0].rstrip("\r").split(",")
        assert header == pq.read_table(str(GOLD / (name + ".parquet"))).schema.names, (
            "{} header mismatch: {}".format(name, header)
        )
        assert len(lines) == rows + 1, "{} lines={} rows={}".format(name, len(lines), rows)


def test_no_csv_tmp_dirs():
    leftovers = [p for p in GOLD.rglob("_*_csv_tmp") if p.is_dir()]
    assert not leftovers, "leftover tmp dirs: {}".format(leftovers)


# ---------------------------------------------------------------- check 4a

def test_leaderboard_content():
    table = read_mart("gold_trail_leaderboard")
    cols = table.to_pydict()
    way_ids = cols["way_id"]
    assert len(way_ids) == len(set(way_ids)), "duplicate way_id values"

    by_region = {}
    for i, region in enumerate(cols["region"]):
        by_region.setdefault(region, []).append(cols["region_popularity_rownum"][i])
    for region, ranks in by_region.items():
        assert sorted(ranks) == list(range(1, len(ranks) + 1)), (
            "{} popularity rownum not 1..n unique".format(region)
        )

    assert pc.min(table["total_vertical_drop_m"]).as_py() >= 0
    assert pc.min(table["total_vertical_climb_m"]).as_py() >= 0
    assert pc.min(table["intensity_percentile"]).as_py() >= 0.0
    assert pc.max(table["intensity_percentile"]).as_py() <= 1.0
    assert pc.sum(pc.is_null(table["intensity_percentile"])).as_py() == 0

    by_tier = {}
    for i in range(table.num_rows):
        key = (cols["region"][i], cols["difficulty_tier"][i])
        by_tier.setdefault(key, []).append(cols["tier_drop_rank"][i])
    for key, ranks in by_tier.items():
        assert min(ranks) == 1, "{} tier_drop_rank min={}".format(key, min(ranks))


# ---------------------------------------------------------------- check 4b

def test_temporal_content():
    table = read_mart("gold_temporal_trends")
    cols = table.to_pydict()
    combos = {}
    for i in range(table.num_rows):
        combos.setdefault((cols["region"][i], cols["activity_type"][i]), []).append(
            (
                cols["year"][i],
                cols["month"][i],
                cols["prev_month_runs"][i],
                cols["mom_runs_growth_pct"][i],
                cols["rolling_3m_satisfaction"][i],
                cols["peak_month_rank"][i],
            )
        )
    assert len(combos) == 12, "expected 12 region/activity combos, got {}".format(len(combos))
    assert table.num_rows == len(combos) * 46 == 552

    expected_months = [
        (y, m) for y in range(2023, 2027) for m in range(1, 13) if (y, m) <= (2026, 10)
    ]
    assert len(expected_months) == 46
    for key, rows in combos.items():
        rows.sort()
        first = rows[0]
        assert first[0:2] == (2023, 1), "{} earliest month {}".format(key, first[0:2])
        assert first[2] is None, "{} first prev_month_runs={}".format(key, first[2])
        assert first[3] is None, "{} first mom_runs_growth_pct={}".format(key, first[3])
        assert [r[0:2] for r in rows] == expected_months, (
            "{} months not contiguous 2023-01..2026-10".format(key)
        )
        assert all(1.0 <= r[4] <= 5.0 for r in rows), "{} rolling_3m_satisfaction out of [1,5]".format(key)

    assert pc.sum(pc.is_null(table["rolling_3m_satisfaction"])).as_py() == 0

    by_year = {}
    for i in range(table.num_rows):
        by_year.setdefault((cols["region"][i], cols["year"][i]), []).append(
            cols["peak_month_rank"][i]
        )
    for key, ranks in by_year.items():
        assert min(ranks) == 1, "{} peak_month_rank min={}".format(key, min(ranks))


# ---------------------------------------------------------------- check 4c

def test_map_nodes_content():
    table = read_mart("gold_arkhyz_trail_map_nodes")
    cols = table.to_pydict()
    leaderboard = read_mart("gold_trail_leaderboard")
    lb_cols = leaderboard.to_pydict()
    assert pc.min(table["node_pings"]).as_py() >= 0
    assert pc.sum(pc.is_null(table["node_pings"])).as_py() == 0

    by_way = {}
    for i in range(table.num_rows):
        by_way.setdefault(cols["way_id"][i], []).append(i)

    grade_nulls = []
    for way_id, idxs in by_way.items():
        idxs.sort(key=lambda i: cols["seq_order"][i])
        cums = [cols["cumulative_dist_m"][i] for i in idxs]
        assert cums[0] == 0.0, "way {} first cumulative_dist_m={}".format(way_id, cums[0])
        for prev, cur in zip(cums, cums[1:]):
            assert cur >= prev, "way {} cumulative_dist_m decreased {} -> {}".format(
                way_id, prev, cur
            )
        for pos, i in enumerate(idxs):
            if cols["segment_grade_pct"][i] is None:
                grade_nulls.append(i)
                if pos > 0:
                    assert cums[pos] == cums[pos - 1], (
                        "way {} grade NULL on non-zero-length segment at seq_order {}".format(
                            way_id, cols["seq_order"][i]
                        )
                    )

    ways_per_region = {}
    for i, region in enumerate(cols["region"]):
        ways_per_region.setdefault(region, set()).add(cols["way_id"][i])
    lb_ways_per_region = {}
    for i, region in enumerate(lb_cols["region"]):
        lb_ways_per_region.setdefault(region, set()).add(lb_cols["way_id"][i])
    for region in sorted(set(ways_per_region) | set(lb_ways_per_region)):
        map_ways = ways_per_region.get(region, set())
        lb_ways = lb_ways_per_region.get(region, set())
        assert lb_ways.issubset(map_ways), (
            "{}: {} leaderboard way_ids missing from map nodes".format(
                region, len(lb_ways - map_ways)
            )
        )
        assert map_ways == lb_ways, (
            "{}: map node way_ids {} != leaderboard way_ids {}".format(
                region, len(map_ways), len(lb_ways)
            )
        )

    assert grade_nulls, "expected NULL segment_grade_pct on first node of each way"


# ---------------------------------------------------------------- check 4d

def test_cohort_content():
    table = read_mart("gold_cohort_distributions")
    cols = table.to_pydict()
    assert pc.min(table["ping_count"]).as_py() >= 20

    shares = [float(s) for s in cols["share_of_activity_pct"]]
    assert max(shares) <= 100.0
    illegal = [
        (cols["region"][i], cols["activity_type"][i], cols["ping_count"][i], shares[i])
        for i in range(table.num_rows)
        if not shares[i] > 0.0
    ]
    assert not illegal, (
        "{} of {} rows have share_of_activity_pct == 0.0 (must be in (0, 100]); "
        "examples (region, activity, ping_count, share): {}".format(
            len(illegal), table.num_rows, illegal[:5]
        )
    )

    by_combo = {}
    for i in range(table.num_rows):
        by_combo.setdefault((cols["region"][i], cols["activity_type"][i]), []).append(shares[i])
    for key, values in by_combo.items():
        assert abs(sum(values) - 100.0) <= 0.5, "{} share sum={}".format(key, sum(values))


# ---------------------------------------------------------------- check 5

def source_text():
    return STAGE3.read_text(encoding="utf-8")


def mart_sql_text():
    src = source_text()
    names = re.findall(r"^(GOLD_\w+_SQL|ARKHYZ_ACTIVITY_MIX_SQL) = \"\"\"(.*?)\"\"\"", src, re.S | re.M)
    return {name: body for name, body in names}


def test_code_review_pure_sql():
    src = source_text()
    for needle in (".select(", ".filter(", ".withColumn(", ".groupBy("):
        assert needle not in src, "DataFrame API transformation found: {}".format(needle)
    assert "@udf" not in src
    assert not re.search(r"\budf\s*\(", src), "udf() usage found"
    sqls = mart_sql_text()
    assert set(sqls) == {
        "GOLD_TRAIL_LEADERBOARD_SQL",
        "GOLD_TEMPORAL_TRENDS_SQL",
        "GOLD_ARKHYZ_TRAIL_MAP_NODES_SQL",
        "GOLD_COHORT_DISTRIBUTIONS_SQL",
        "ARKHYZ_ACTIVITY_MIX_SQL",
    }, "mart SQL constants: {}".format(sorted(sqls))
    assert len(re.findall(r"spark\.sql\(", src)) >= 4


def test_code_review_sql_constructs():
    sqls = mart_sql_text()
    mart_text = "\n".join(
        body for name, body in sqls.items() if name != "ARKHYZ_ACTIVITY_MIX_SQL"
    )
    assert len(re.findall(r"WITH\s+\w+\s+AS\s*\(", mart_text)) >= 4, "expected one CTE per mart"
    for token in ("ROW_NUMBER", "RANK", "DENSE_RANK", "LAG", "PERCENT_RANK"):
        assert token in mart_text, "missing {}".format(token)
    assert "ROWS BETWEEN 2 PRECEDING AND CURRENT ROW" in mart_text
    assert "ROWS BETWEEN UNBOUNDED PRECEDING" in mart_text
    assert len(re.findall(r"\bHAVING\b", mart_text)) >= 3


def test_code_review_env_reuse():
    src = source_text()
    assert re.search(
        r"from stage2_pyspark_etl import \(.*prepare_windows_env.*get_spark.*\)", src, re.S
    ), "stage2 env helpers not imported"
    assert "def prepare_windows_env" not in src
    assert "def get_spark" not in src
    for needle in ("winutils", "hadoop.dll", "JAVA_HOME", "jdk"):
        assert needle.lower() not in src.lower(), "duplicated env bootstrap: {}".format(needle)


def test_code_review_explain_wiring():
    src = source_text()
    assert '"--explain"' in src
    assert "df.explain(\"formatted\")" in src
    assert 'name == "gold_trail_leaderboard"' in src
    assert "def print_explain_partition_pruning" in src
    assert "spark.sql(ARKHYZ_ACTIVITY_MIX_SQL).explain" in src
    assert re.search(r"if args\.explain:\s*\n\s*print_explain_partition_pruning", src)


def test_code_review_style():
    src = source_text()
    assert not re.search(r"(?<![A-Za-z0-9_])f(['\"])", src), "f-string found"
    assert not [c for c in src if ord(c) >= 0x1F000 or 0x2600 <= ord(c) <= 0x27BF], (
        "emoji/dingbat character found"
    )
    for match in re.finditer(r"print\(", src):
        fragment = src[match.end() : match.end() + 200].lstrip()
        assert fragment.startswith('"[stage3]'), (
            "print without [stage3] prefix near: {}".format(fragment[:60])
        )
