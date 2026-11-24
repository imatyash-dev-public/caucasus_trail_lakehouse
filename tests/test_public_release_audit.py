"""Release audit tests for the public GitHub release.

Scans the set of files that would be committed to git (tracked plus
untracked-but-not-ignored, or an os.walk fallback that mirrors .gitignore)
and asserts:

1. No file leaks a user-identifying absolute path.
2. No file leaks a credential-like token or a corporate cloud URL.
3. No file exceeds GitHub's 50 MB per-file limit.
4. The release set still contains the expected anchor files.

Every forbidden pattern is assembled by string concatenation so that the
scan definitions never match themselves. This module is still part of the
release set for the size and anchor checks, but the token scans skip the
module itself: the required test name below unavoidably contains one
credential token as a substring, which would be a permanent false positive.

Run: python -m pytest tests/test_public_release_audit.py -v
"""

import os
import pathlib
import subprocess

PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
SELF_FILE = pathlib.Path(__file__).resolve()

MAX_FILE_BYTES = 50 * 1024 * 1024

# Hardcoded ignore set mirroring .gitignore, used only by the os.walk
# fallback when git is unavailable or the scan fails.
IGNORED_DIR_NAMES = (
    ".git",
    ".hadoop",
    "__pycache__",
    ".pytest_cache",
    "spark-warehouse",
    "metastore_db",
    ".ivy2",
    "venv",
    ".venv",
)

IGNORED_PREFIXES = (
    "data/raw",
    "data/processed",
    "data/gold",
    "data/sample/processed",
    "data/sample/gold",
    "bi/figures",
)

IGNORED_FILES = (
    "bi/arkhyz_interactive_dashboard.html",
    "plan.md",
)

# Forbidden patterns, assembled by concatenation so the audit file does not
# contain any forbidden token verbatim.
USER_PATH_TOKENS = [
    "c:" + os.sep + "users",
    "c:" + "/" + "users",
    "/" + "home" + "/",
]

CREDENTIAL_TOKENS = [
    "pass" + "word",
    "pass" + "wd",
    "sec" + "ret",
    "api" + "_key",
    "access" + "_key",
    "ak" + "ia",
    "begin private " + "key",
    "bear" + "er ",
    "s" + "k-",
]

CORPORATE_URL_TOKENS = [
    "app.powerbi" + ".com",
    "datalens." + "yandex",
    "blob.core." + "windows.net",
    "amazonaws" + ".com",
    "s" + "3://",
    "data." + "yandex",
]


def _is_ignored(relative_posix):
    """Return True when a project-relative posix path is gitignored."""
    for part in relative_posix.split("/"):
        if part in IGNORED_DIR_NAMES:
            return True
    for prefix in IGNORED_PREFIXES:
        if relative_posix == prefix:
            return True
        if relative_posix.startswith(prefix + "/"):
            return True
    if relative_posix in IGNORED_FILES:
        return True
    if relative_posix.lower().endswith(".ipynb"):
        return True
    return False


def _walk_release_files():
    """os.walk fallback that mirrors the .gitignore ignore set."""
    files = []
    for dirpath, dirnames, filenames in os.walk(str(PROJECT_ROOT)):
        dir_path = pathlib.Path(dirpath)
        rel_dir = dir_path.relative_to(PROJECT_ROOT).as_posix()
        kept_dirs = []
        for name in dirnames:
            rel = name if rel_dir == "." else rel_dir + "/" + name
            if _is_ignored(rel):
                continue
            kept_dirs.append(name)
        dirnames[:] = sorted(kept_dirs)
        for name in filenames:
            rel = name if rel_dir == "." else rel_dir + "/" + name
            if _is_ignored(rel):
                continue
            candidate = dir_path / name
            if candidate.is_file():
                files.append(candidate)
    print("[audit] os.walk fallback found {0} release file(s)".format(len(files)))
    return files


def release_files():
    """Return the list of pathlib.Path files that would be committed."""
    result = None
    try:
        result = subprocess.run(
            ["git", "ls-files", "--cached", "--others", "--exclude-standard"],
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            check=False,
        )
    except OSError:
        result = None

    if result is not None and result.returncode == 0:
        files = []
        for line in result.stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            candidate = PROJECT_ROOT / line
            if candidate.is_file():
                files.append(candidate)
        print("[audit] git ls-files reported {0} release file(s)".format(len(files)))
        return files

    print("[audit] git unavailable or failed; falling back to os.walk")
    return _walk_release_files()


def _read(path):
    """Read a file as lowercased text, replacing undecodable bytes."""
    return path.read_bytes().decode("utf-8", errors="replace").lower()


def _is_binary(path):
    """Return True when a file looks binary (NUL byte in the first block).

    Binary assets (PNG/Parquet-style payloads) are still size-checked, but
    they are skipped by the token scan: lossy decoding of compressed bytes
    produces accidental substring matches that are not real leaks.
    """
    with path.open("rb") as handle:
        chunk = handle.read(4096)
    return b"\x00" in chunk


def _scan(tokens):
    """Return [(relative_path, token)] for every release file hit.

    The audit module itself is skipped: its mandatory test name contains
    one credential token as a substring, so scanning it would always fail.
    """
    hits = []
    for path in release_files():
        if path.resolve() == SELF_FILE:
            continue
        if _is_binary(path):
            continue
        text = _read(path)
        for token in tokens:
            if token in text:
                hits.append((path.relative_to(PROJECT_ROOT).as_posix(), token))
    return hits


def test_no_user_paths():
    """No release file may contain a user-identifying absolute path."""
    hits = _scan(USER_PATH_TOKENS)
    report = ["[audit] user-path token(s) found in release files:"]
    for relative_path, token in hits:
        report.append("[audit]   {0} -> {1}".format(relative_path, token))
    assert not hits, "\n".join(report)


def test_no_secrets_or_corporate_urls():
    """No release file may contain a credential token or corporate cloud URL."""
    hits = _scan(CREDENTIAL_TOKENS + CORPORATE_URL_TOKENS)
    report = ["[audit] credential/corporate-url token(s) found in release files:"]
    for relative_path, token in hits:
        report.append("[audit]   {0} -> {1}".format(relative_path, token))
    assert not hits, "\n".join(report)


def test_no_oversized_files():
    """Every release file must stay below GitHub's 50 MB per-file limit."""
    files = release_files()
    largest_path = None
    largest_size = -1
    oversized = []
    for path in files:
        size = path.stat().st_size
        if size > largest_size:
            largest_size = size
            largest_path = path
        if size >= MAX_FILE_BYTES:
            oversized.append((path.relative_to(PROJECT_ROOT).as_posix(), size))

    report = ["[audit] file(s) at or above the 50 MB per-file limit:"]
    for relative_path, size in oversized:
        report.append(
            "[audit]   {0} -> {1} bytes".format(relative_path, size)
        )
    if largest_path is not None:
        report.append(
            "[audit] largest release file: {0} -> {1} bytes".format(
                largest_path.relative_to(PROJECT_ROOT).as_posix(), largest_size
            )
        )
    assert not oversized, "\n".join(report)


def test_release_set_sane():
    """The release set must still contain the expected anchor files."""
    files = release_files()
    relative = set()
    for path in files:
        relative.add(path.relative_to(PROJECT_ROOT).as_posix())

    required = [
        "README.md",
        "requirements.txt",
        "src/stage2_pyspark_etl.py",
        "tests/test_public_release_audit.py",
    ]
    missing = [name for name in required if name not in relative]

    sample_csvs = []
    for name in sorted(relative):
        parent = pathlib.PurePosixPath(name).parent.as_posix()
        if parent == "data/sample" and name.lower().endswith(".csv"):
            sample_csvs.append(name)

    report = ["[audit] release set is missing expected file(s):"]
    for name in missing:
        report.append("[audit]   {0}".format(name))
    if not sample_csvs:
        report.append("[audit]   no data/sample/*.csv fixture in the release set")

    assert not missing, "\n".join(report)
    assert sample_csvs, "\n".join(report)
