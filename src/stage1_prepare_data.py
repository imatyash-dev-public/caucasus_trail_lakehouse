"""Stage 1 data generator for the Arkhyz MTB and hiking big data pipeline.

Fetches OSM trail data (Overpass with deterministic offline fallback), builds a
modeled SRTM elevation grid, and streams a chunked rider telemetry fact table
to CSV under the output directory. All randomness is seeded and deterministic.
"""

import argparse
import json
import os
import pathlib
import time
from datetime import datetime, timezone
from urllib.parse import urlsplit

import numpy as np
import pandas as pd
import requests

REGIONS = {
    "Arkhyz": (43.45, 41.05, 43.65, 41.35),
    "Dombay_Teberda": (43.25, 41.55, 43.48, 41.80),
    "Krasnaya_Polyana": (43.60, 40.15, 43.75, 40.38),
}
ELEV_PARAMS = {
    "Arkhyz": dict(base=1450, span=1750, phase=0.0),
    "Dombay_Teberda": dict(base=1600, span=1800, phase=1.7),
    "Krasnaya_Polyana": dict(base=600, span=1800, phase=3.4),
}
CHUNK_ROWS = 500_000
OSM_TAG_COLS = ["mtb_scale_raw", "sac_scale_raw", "surface_raw", "incline_raw"]
OSM_COLUMNS = [
    "way_id",
    "node_id",
    "seq_order",
    "trail_name",
    "region",
    "highway_type",
    "mtb_scale_raw",
    "sac_scale_raw",
    "surface_raw",
    "incline_raw",
    "lat",
    "lon",
    "osm_timestamp",
]
ELEVATION_COLUMNS = ["grid_lat", "grid_lon", "elevation_m", "sensor_source"]
TELEMETRY_COLUMNS = [
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
    "heart_rate_bpm",
    "airtime_ms",
    "g_force",
    "weather_cond",
    "rider_satisfaction",
]
NOISE_POOLS = {
    "mtb_scale_raw": ["2+", "3", "unknown", "0-1", "6?"],
    "sac_scale_raw": ["T2", "alpine_hiking?", "unknown", "demanding_mountain_hiking;T4"],
    "surface_raw": ["rock;roots", "ground;dirt", "unknown", "gravel;gravel", "paved-unpaved"],
    "incline_raw": ["-15%", "steep", "+8%", "up", "12%?", "downhill"],
}
FALLBACK_NAMES = {
    "Arkhyz": [
        "Romantic DH",
        "Lunnaya Polyana Flow",
        "Sofia Waterfalls Trail",
        "Dukka Lakes Path",
        "Semitsvetnoe Lake Trail",
        "Kyzgych Gorge Track",
        "Tabunshchyk Ridge Trail",
        "Silver Spring Footway",
        "Belaya Skala Path",
        "Romanovsky Bridge Track",
    ],
    "Dombay_Teberda": [
        "Dombay Classic DH",
        "Mushtra Trail",
        "Gorodetskoye Lake Path",
        "Chuchkhur Waterfalls Trail",
        "Alibek Valley Track",
        "Teberda Reserve Footway",
        "Dzhailik Ridge Trail",
        "Ine Ridge Path",
    ],
    "Krasnaya_Polyana": [
        "Rosa Khutor Bike Park DH",
        "Gazprom Flow Trail",
        "Oleniy Ruchey Path",
        "Chaltura Track",
        "Aibga Ridge Trail",
        "Krasnaya Polyana Valley Footway",
        "Laura Lakes Path",
        "Eagles Rock Trail",
    ],
}
OVERPASS_ENDPOINTS = [
    {
        "url": "https://maps.mail.ru/osm/tools/overpass/api/interpreter",
        "read_timeout": 120,
        "retries": 3,
    },
    {
        "url": "https://overpass-api.de/api/interpreter",
        "read_timeout": 120,
        "retries": 0,
    },
    {
        "url": "https://overpass.kumi.systems/api/interpreter",
        "read_timeout": 25,
        "retries": 0,
    },
]
OVERPASS_HEADERS = {
    "User-Agent": "ArkhyzBigDataAcademy/1.0 (PySpark-ETL-Student-Project; +https://openstreetmap.org)",
    "Accept": "application/json",
}
CONNECT_TIMEOUT = 30
TRANSIENT_STATUSES = {429, 502, 503, 504}
RETRY_BASE_SECONDS = 2.0
OSM_CACHE_SUBDIR = "cache"
SENSOR_VALUES = ["SRTM_30m", "SRTM_V3.0", "ASTER_GDEM_V3", "Copernicus_DEM_30"]
SENSOR_PROBS = [0.75, 0.15, 0.05, 0.05]
ACTIVITIES = ["MTB_DH", "MTB_Enduro", "Trail_Run", "Hiking"]
ACTIVITY_WEIGHTS = [0.25, 0.25, 0.15, 0.35]
SPEED_MEAN = np.array([30.0, 22.0, 9.0, 3.8])
SPEED_SD = np.array([8.0, 6.0, 2.0, 0.9])
SPEED_LO = np.array([15.0, 10.0, 5.0, 2.0])
SPEED_HI = np.array([55.0, 40.0, 14.0, 6.0])
HR_MEAN = np.array([162.0, 155.0, 158.0, 128.0])
HR_SD = np.array([12.0, 12.0, 10.0, 12.0])
P_AIRTIME = np.array([0.30, 0.15, 0.02, 0.005])
WEATHER = ["Sunny", "Rain", "Snow", "Muddy", "Fog"]
WEATHER_WEIGHTS = [0.45, 0.15, 0.10, 0.15, 0.15]
SATISFACTION_VALUES = np.array([1, 2, 3, 4, 5])
SATISFACTION_PROBS = [0.06, 0.10, 0.24, 0.35, 0.25]
TS_MIN = int(datetime(2023, 1, 1, tzinfo=timezone.utc).timestamp())
TS_MAX = int(datetime(2026, 10, 31, tzinfo=timezone.utc).timestamp())
FALLBACK_TS_MIN = int(datetime(2021, 1, 1, tzinfo=timezone.utc).timestamp())
FALLBACK_TS_MAX = int(datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc).timestamp())


def parse_args():
    parser = argparse.ArgumentParser(
        description="Stage 1 OSM/elevation/telemetry data generator"
    )
    parser.add_argument("--size-gb", type=float, default=1.5)
    parser.add_argument("--output-dir", default="data/raw")
    parser.add_argument("--seed", type=int, default=42)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--offline", action="store_true")
    group.add_argument(
        "--refresh-osm",
        action="store_true",
        help="ignore cached Overpass region files and re-fetch them",
    )
    args = parser.parse_args()
    if args.size_gb <= 0:
        parser.error("--size-gb must be > 0")
    return args


def build_overpass_query(region):
    south, west, north, east = REGIONS[region]
    bbox = "({},{},{},{})".format(south, west, north, east)
    return (
        "[out:json][timeout:120];\n"
        "(\n"
        '  way["highway"~"^(path|track|footway|cycleway|bridleway)$"]' + bbox + ";\n"
        '  way["mtb:scale"]' + bbox + ";\n"
        '  way["sac_scale"]' + bbox + ";\n"
        ");\n"
        "(._;>;);\n"
        "out meta;"
    )


def parse_overpass(elements, region):
    node_map = {}
    for element in elements:
        if element.get("type") == "node":
            lat = element.get("lat")
            lon = element.get("lon")
            if lat is not None and lon is not None:
                node_map[element["id"]] = (lat, lon)
    rows = []
    for element in elements:
        if element.get("type") != "way":
            continue
        tags = element.get("tags") or {}
        resolved = []
        for node_id in element.get("nodes", []):
            coords = node_map.get(node_id)
            if coords is not None:
                resolved.append((node_id, coords[0], coords[1]))
        if len(resolved) < 2:
            continue
        trail_name = ""
        for key in ("name", "name:ru", "name:en"):
            value = tags.get(key)
            if value:
                trail_name = value
                break
        highway_type = tags.get("highway", "")
        mtb_scale_raw = tags.get("mtb:scale", "")
        sac_scale_raw = tags.get("sac_scale", "")
        surface_raw = tags.get("surface", "")
        incline_raw = tags.get("incline", "")
        osm_timestamp = element.get("timestamp", "") or ""
        for seq_order, (node_id, lat, lon) in enumerate(resolved, start=1):
            rows.append(
                (
                    element["id"],
                    node_id,
                    seq_order,
                    trail_name,
                    region,
                    highway_type,
                    mtb_scale_raw,
                    sac_scale_raw,
                    surface_raw,
                    incline_raw,
                    round(lat, 7),
                    round(lon, 7),
                    osm_timestamp,
                )
            )
    return pd.DataFrame(rows, columns=OSM_COLUMNS)


def inject_osm_tag_noise(df, rng):
    df = df.copy()
    mask = rng.random(len(df)) < 0.03
    for col in OSM_TAG_COLS:
        pool = np.asarray(NOISE_POOLS[col], dtype=object)
        empty = (df[col].isna() | (df[col] == "")).to_numpy()
        targets = np.nonzero(mask & empty)[0]
        if len(targets) > 0:
            values = rng.choice(pool, size=len(targets))
            df.iloc[targets, df.columns.get_loc(col)] = values
    return df


def host_of(url):
    return urlsplit(url).hostname


def cache_file_path(out_dir, region):
    return pathlib.Path(out_dir) / OSM_CACHE_SUBDIR / "overpass_{}.json".format(region)


def load_region_cache(path):
    try:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, ValueError):
        return None
    elements = payload.get("elements")
    if isinstance(elements, list) and len(elements) > 0 and payload.get("source"):
        return payload
    return None


def save_region_cache(path, region, source, elements):
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "region": region,
        "source": source,
        "fetched_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "elements": elements,
    }
    tmp_path = path.with_suffix(".json.tmp")
    with open(tmp_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle)
    os.replace(tmp_path, path)


def fetch_region_overpass(region, jitter_rng):
    query = build_overpass_query(region)
    for endpoint_index, endpoint in enumerate(OVERPASS_ENDPOINTS):
        host = host_of(endpoint["url"])
        retries_used = 0
        for attempt in range(endpoint["retries"] + 1):
            started = time.monotonic()
            status_text = ""
            retryable = False
            elements = None
            try:
                response = requests.post(
                    endpoint["url"],
                    data={"data": query},
                    timeout=(CONNECT_TIMEOUT, endpoint["read_timeout"]),
                    headers=OVERPASS_HEADERS,
                )
                status_text = "HTTP {}".format(response.status_code)
                if response.status_code == 200:
                    payload = response.json()
                    if isinstance(payload.get("elements"), list) and len(payload["elements"]) > 0:
                        elements = payload["elements"]
                    else:
                        status_text = "HTTP 200 empty"
                        retryable = True
                elif response.status_code in TRANSIENT_STATUSES:
                    retryable = True
            except requests.exceptions.RequestException as exc:
                status_text = exc.__class__.__name__
                retryable = True
            elapsed = time.monotonic() - started
            if elements is not None:
                print(
                    "[Overpass] {} | attempt {} | {} | {:.1f}s | success".format(
                        host, attempt + 1, status_text, elapsed
                    )
                )
                return elements, endpoint["url"]
            if retryable and retries_used < endpoint["retries"]:
                cap = RETRY_BASE_SECONDS * (2 ** retries_used)
                delay = 0.5 * cap + jitter_rng.random() * 0.5 * cap
                print(
                    "[Overpass] {} | attempt {} | {} | {:.1f}s | retry in {:.1f}s".format(
                        host, attempt + 1, status_text, elapsed, delay
                    )
                )
                time.sleep(delay)
                retries_used += 1
            elif endpoint_index + 1 < len(OVERPASS_ENDPOINTS):
                next_host = host_of(OVERPASS_ENDPOINTS[endpoint_index + 1]["url"])
                print(
                    "[Overpass] {} | attempt {} | {} | {:.1f}s | rotate to {}".format(
                        host, attempt + 1, status_text, elapsed, next_host
                    )
                )
                break
            else:
                print(
                    "[Overpass] {} | attempt {} | {} | {:.1f}s | region failed -> fallback".format(
                        host, attempt + 1, status_text, elapsed
                    )
                )
                return None, None
    return None, None


def generate_fallback_osm(rng):
    highway_values = ["path", "track", "footway", "cycleway", "bridleway"]
    highway_probs = [0.40, 0.25, 0.20, 0.10, 0.05]
    mtb_values = ["0", "1", "2", "3", "4", "5", "6"]
    mtb_probs = [0.25, 0.20, 0.18, 0.15, 0.10, 0.08, 0.04]
    sac_values = ["hiking", "mountain_hiking", "demanding_mountain_hiking"]
    surface_values = ["ground", "dirt", "gravel", "rock", "grass", "unpaved"]
    incline_values = ["up", "down", "5%", "-8%"]
    rows = []
    node_counter = 0
    way_counter = 0
    for region, (south, west, north, east) in REGIONS.items():
        names = FALLBACK_NAMES[region]
        for way_index in range(16):
            way_id = 100000 + way_counter
            way_counter += 1
            n_nodes = int(rng.integers(15, 81))
            trail_name = "" if rng.random() < 0.35 else names[way_index % len(names)]
            highway_type = str(rng.choice(highway_values, p=highway_probs))
            mtb_scale_raw = str(rng.choice(mtb_values, p=mtb_probs)) if rng.random() < 0.40 else ""
            sac_scale_raw = str(rng.choice(sac_values)) if rng.random() < 0.25 else ""
            surface_raw = str(rng.choice(surface_values)) if rng.random() < 0.50 else ""
            incline_raw = str(rng.choice(incline_values)) if rng.random() < 0.20 else ""
            ts_seconds = int(rng.integers(FALLBACK_TS_MIN, FALLBACK_TS_MAX + 1))
            osm_timestamp = datetime.fromtimestamp(
                ts_seconds, tz=timezone.utc
            ).strftime("%Y-%m-%dT%H:%M:%SZ")
            lat = south + rng.random() * (north - south)
            lon = west + rng.random() * (east - west)
            for seq_order in range(1, n_nodes + 1):
                node_counter += 1
                rows.append(
                    (
                        way_id,
                        10000000 + node_counter,
                        seq_order,
                        trail_name,
                        region,
                        highway_type,
                        mtb_scale_raw,
                        sac_scale_raw,
                        surface_raw,
                        incline_raw,
                        round(lat, 7),
                        round(lon, 7),
                        osm_timestamp,
                    )
                )
                lat = min(max(lat + rng.uniform(-0.0015, 0.0015), south), north)
                lon = min(max(lon + rng.uniform(-0.0020, 0.0020), west), east)
    return pd.DataFrame(rows, columns=OSM_COLUMNS)


def fetch_osm_trails(rng, offline, out_dir, refresh_osm):
    if offline:
        print("[Overpass] offline mode -> synthetic fallback for all regions")
    jitter_rng = np.random.default_rng()
    fallback_full = None
    region_frames = {}
    region_sources = {}
    region_elements = {}
    for region in REGIONS:
        elements = None
        source = None
        if not offline:
            cache_file = cache_file_path(out_dir, region)
            if refresh_osm:
                print("[Overpass] {} | refresh requested -> fetch".format(region))
            else:
                cached = load_region_cache(cache_file)
                if cached is not None:
                    elements = cached["elements"]
                    source = "cache:" + host_of(cached["source"])
                    print("[Overpass] {} | cache hit {}".format(region, cache_file))
                else:
                    print("[Overpass] {} | cache miss -> fetch".format(region))
            if elements is None:
                elements, fetched_url = fetch_region_overpass(region, jitter_rng)
                if elements is not None:
                    source = "overpass:" + host_of(fetched_url)
                    save_region_cache(cache_file, region, fetched_url, elements)
        if elements is not None:
            df = parse_overpass(elements, region)
            df["data_source"] = source
            region_frames[region] = inject_osm_tag_noise(df, rng)
            region_elements[region] = len(elements)
        else:
            if fallback_full is None:
                fallback_full = inject_osm_tag_noise(generate_fallback_osm(rng), rng)
            print(
                "WARNING: Overpass unavailable for {} -> synthetic fallback for this region".format(
                    region
                )
            )
            fallback_slice = fallback_full[fallback_full["region"] == region].copy()
            fallback_slice["data_source"] = "fallback:synthetic"
            region_frames[region] = fallback_slice
            region_elements[region] = None
        region_sources[region] = source if source is not None else "fallback:synthetic"
    osm_df = pd.concat([region_frames[region] for region in REGIONS], ignore_index=True)
    if all(value == "fallback:synthetic" for value in region_sources.values()):
        mode = "fallback"
    elif any(value == "fallback:synthetic" for value in region_sources.values()):
        mode = "mixed"
    else:
        mode = "overpass"
    region_stats = []
    for region in REGIONS:
        sub = osm_df[osm_df["region"] == region]
        region_stats.append(
            {
                "region": region,
                "data_source": region_sources[region],
                "elements": region_elements[region],
                "ways": int(sub["way_id"].nunique()),
                "nodes": int(sub["node_id"].nunique()),
            }
        )
    return osm_df, mode, region_stats


def build_elevation_grid(osm_df, rng):
    frames = []
    for region, (south, west, north, east) in REGIONS.items():
        params = ELEV_PARAMS[region]
        n_lat = int(round((north - south) / 0.001)) + 1
        n_lon = int(round((east - west) / 0.001)) + 1
        grid_lat = np.round(south + 0.001 * np.arange(n_lat), 3)
        grid_lon = np.round(west + 0.001 * np.arange(n_lon), 3)
        lat_mesh, lon_mesh = np.meshgrid(grid_lat, grid_lon, indexing="ij")
        t = (lat_mesh - south) / (north - south)
        lon_t = (lon_mesh - west) / (east - west)
        ridge = 0.6 * np.sin(2 * np.pi * 3 * lon_t + params["phase"]) + 0.4 * np.sin(
            lat_mesh * 40 + lon_mesh * 17
        )
        elevation = params["base"] + params["span"] * (0.75 * t + 0.25 * ridge)
        elevation_m = np.clip(
            elevation, params["base"] - 100, params["base"] + params["span"]
        ).astype(np.int64)
        n_rows = n_lat * n_lon
        sensor_source = rng.choice(SENSOR_VALUES, size=n_rows, p=SENSOR_PROBS)
        frames.append(
            pd.DataFrame(
                {
                    "grid_lat": lat_mesh.reshape(-1),
                    "grid_lon": lon_mesh.reshape(-1),
                    "elevation_m": elevation_m.reshape(-1),
                    "sensor_source": sensor_source,
                }
            )
        )
    return pd.concat(frames, ignore_index=True)


def inject_elevation_anomalies(df, rng):
    n = len(df)
    void_mask = rng.random(n) < 0.010
    spike_mask = rng.random(n) < 0.005
    elevation_m = df["elevation_m"].to_numpy().copy()
    elevation_m[void_mask] = -32768
    elevation_m[spike_mask] = 99999
    df = df.copy()
    df["elevation_m"] = elevation_m.astype(np.int64)
    dup_count = int(0.005 * n)
    if dup_count > 0:
        dup_index = rng.choice(n, dup_count, replace=False)
        df = pd.concat([df, df.iloc[dup_index]], ignore_index=True)
    return df


class TelemetryGenerator:
    def __init__(self, osm_df, rng):
        self.rng = rng
        source = osm_df.loc[:, ["way_id", "node_id", "seq_order", "lat", "lon"]]
        counts = source.groupby("way_id", sort=False).size()
        keep = counts[counts >= 2].index
        source = source[source["way_id"].isin(keep)]
        source = source.sort_values(["way_id", "seq_order"], kind="stable")
        self.way_len = source.groupby("way_id", sort=False).size().to_numpy()
        self.way_start = np.cumsum(self.way_len) - self.way_len
        self.way_ids = source["way_id"].to_numpy()[self.way_start]
        self.flat_node_id = source["node_id"].to_numpy()
        self.flat_lat = source["lat"].to_numpy()
        self.flat_lon = source["lon"].to_numpy()
        self.n_ways = len(self.way_len)
        self.telemetry_counter = 0
        self.run_counter = 0

    def generate_chunk(self, n_rows):
        rng = self.rng
        lengths = []
        total = 0
        while total < n_rows:
            length = int(rng.integers(80, 400))
            lengths.append(length)
            total += length
        lengths[-1] = n_rows - (total - lengths[-1])
        lens = np.asarray(lengths, dtype=np.int64)
        n_runs = len(lens)
        run_offsets = np.cumsum(lens) - lens
        row_run = np.repeat(np.arange(n_runs), lens)
        off = np.arange(n_rows) - np.repeat(run_offsets, lens)
        run_way = rng.integers(0, self.n_ways, n_runs)
        run_start = (rng.random(n_runs) * self.way_len[run_way]).astype(np.int64)
        flat_idx = np.repeat(self.way_start[run_way], lens) + (
            np.repeat(run_start, lens) + off
        ) % np.repeat(self.way_len[run_way], lens)
        node_id = self.flat_node_id[flat_idx]
        lat = self.flat_lat[flat_idx]
        lon = self.flat_lon[flat_idx]
        way_id = np.repeat(self.way_ids[run_way], lens)
        run_numbers = self.run_counter + np.arange(n_runs, dtype=np.int64)
        self.run_counter += n_runs
        run_id = np.repeat(np.char.add("RUN_", np.char.mod("%08d", run_numbers)), lens)
        athlete_numbers = rng.integers(1, 5001, n_runs)
        athlete_id = np.repeat(
            np.char.add("ATH_", np.char.mod("%04d", athlete_numbers)), lens
        )
        activity_codes = rng.choice(len(ACTIVITIES), size=n_runs, p=ACTIVITY_WEIGHTS)
        activity_row = np.repeat(activity_codes, lens)
        activity_type = np.repeat(np.asarray(ACTIVITIES, dtype=object)[activity_codes], lens)
        weather_codes = rng.choice(len(WEATHER), size=n_runs, p=WEATHER_WEIGHTS)
        weather_cond = np.repeat(np.asarray(WEATHER, dtype=object)[weather_codes], lens)
        start_sec = rng.integers(TS_MIN, TS_MAX, n_runs)
        step_sec = rng.integers(1, 4, n_runs)
        row_sec = np.repeat(start_sec, lens) + off * np.repeat(step_sec, lens)
        timestamp = pd.to_datetime(row_sec, unit="s").strftime("%Y-%m-%dT%H:%M:%SZ")
        speed_kmh = np.clip(
            rng.normal(SPEED_MEAN[activity_row], SPEED_SD[activity_row]),
            SPEED_LO[activity_row],
            SPEED_HI[activity_row],
        ).round(1)
        heart_rate_bpm = np.clip(
            rng.normal(HR_MEAN[activity_row], HR_SD[activity_row]), 90, 195
        ).astype(np.int64)
        jump = rng.random(n_rows) < P_AIRTIME[activity_row]
        airtime_ms = np.where(jump, rng.integers(100, 2500, n_rows), 0).astype(np.int64)
        g_base = np.clip(rng.normal(1.3, 0.35, n_rows), 0.8, 4.5)
        g_jump = np.clip(rng.normal(2.8, 0.7, n_rows), 1.5, 4.5)
        g_force = np.where(jump, g_jump, g_base).round(2)
        lat_gps = (lat + rng.normal(0, 2.5e-5, n_rows)).round(6)
        lon_gps = (lon + rng.normal(0, 2.5e-5, n_rows)).round(6)
        rider_satisfaction = rng.choice(
            SATISFACTION_VALUES, size=n_rows, p=SATISFACTION_PROBS
        )
        telemetry_numbers = np.arange(
            self.telemetry_counter, self.telemetry_counter + n_rows, dtype=np.int64
        )
        self.telemetry_counter += n_rows
        telemetry_id = np.char.add("TEL_", np.char.mod("%012d", telemetry_numbers))
        df = pd.DataFrame(
            {
                "telemetry_id": telemetry_id,
                "run_id": run_id,
                "athlete_id": athlete_id,
                "activity_type": activity_type,
                "way_id": way_id,
                "node_id": node_id,
                "timestamp": timestamp,
                "lat_gps": lat_gps,
                "lon_gps": lon_gps,
                "speed_kmh": speed_kmh,
                "heart_rate_bpm": heart_rate_bpm,
                "airtime_ms": airtime_ms,
                "g_force": g_force,
                "weather_cond": weather_cond,
                "rider_satisfaction": rider_satisfaction,
            }
        )
        return self.inject_telemetry_anomalies(df)

    def inject_telemetry_anomalies(self, df):
        rng = self.rng
        n = len(df)
        mask_hr_null = rng.random(n) < 0.015
        mask_weather_null = rng.random(n) < 0.008
        mask_sat_null = rng.random(n) < 0.010
        mask_hr_out = rng.random(n) < 0.003
        mask_speed_out = rng.random(n) < 0.002
        mask_air_out = rng.random(n) < 0.002
        mask_lat_zero = rng.random(n) < 0.0015
        mask_bad_ts = rng.random(n) < 0.0025
        counts = {
            "hr_null": int(mask_hr_null.sum()),
            "weather_null": int(mask_weather_null.sum()),
            "sat_null": int(mask_sat_null.sum()),
            "hr_outlier": int(mask_hr_out.sum()),
            "speed_outlier": int(mask_speed_out.sum()),
            "airtime_outlier": int(mask_air_out.sum()),
            "lat_zero": int(mask_lat_zero.sum()),
            "bad_timestamp": int(mask_bad_ts.sum()),
            "dup_rows": int(0.015 * n),
        }
        df["heart_rate_bpm"] = df["heart_rate_bpm"].astype("Int64")
        df["airtime_ms"] = df["airtime_ms"].astype("Int64")
        df["rider_satisfaction"] = df["rider_satisfaction"].astype("Int64")
        if counts["hr_null"] > 0:
            df.loc[mask_hr_null, "heart_rate_bpm"] = pd.NA
        if counts["weather_null"] > 0:
            df.loc[mask_weather_null, "weather_cond"] = ""
        if counts["sat_null"] > 0:
            df.loc[mask_sat_null, "rider_satisfaction"] = pd.NA
        if counts["hr_outlier"] > 0:
            df.loc[mask_hr_out, "heart_rate_bpm"] = rng.choice(
                np.array([-1, 0, 350], dtype=np.int64), size=counts["hr_outlier"]
            )
        if counts["speed_outlier"] > 0:
            df.loc[mask_speed_out, "speed_kmh"] = rng.choice(
                np.array([-15.0, 299.9]), size=counts["speed_outlier"]
            )
        if counts["airtime_outlier"] > 0:
            df.loc[mask_air_out, "airtime_ms"] = rng.choice(
                np.array([-500, 20000], dtype=np.int64), size=counts["airtime_outlier"]
            )
        if counts["lat_zero"] > 0:
            df.loc[mask_lat_zero, "lat_gps"] = 0.0
        if counts["bad_timestamp"] > 0:
            df.loc[mask_bad_ts, "timestamp"] = rng.choice(
                np.array(["SYNC_ERROR", "1970-01-01", "NULL"], dtype=object),
                size=counts["bad_timestamp"],
            )
        if counts["dup_rows"] > 0:
            dup_index = rng.choice(n, counts["dup_rows"], replace=False)
            df = pd.concat([df, df.iloc[dup_index]], ignore_index=True)
        return df, counts


def calibrate_bytes_per_row(gen_factory, seed):
    rng = np.random.default_rng([seed, 99])
    gen = gen_factory(rng)
    df, _ = gen.generate_chunk(50_000)
    csv_str = df.to_csv(index=False)
    return len(csv_str.encode("utf-8")) / 50_000


def fmt_size(n_bytes):
    return "{:.2f} GB ({:.1f} MB)".format(n_bytes / (1024 ** 3), n_bytes / (1024 ** 2))


def generate_telemetry_file(osm_df, size_gb, out_path, seed):
    target_bytes = size_gb * (1024 ** 3)
    gen_factory = lambda r: TelemetryGenerator(osm_df, r)
    bytes_per_row = calibrate_bytes_per_row(gen_factory, seed)
    total_rows = max(int(target_bytes / bytes_per_row), 1)
    print("Calibration: bytes_per_row={:.2f} -> total_rows={}".format(bytes_per_row, total_rows))
    rng = np.random.default_rng(seed)
    gen = TelemetryGenerator(osm_df, rng)
    full_chunks = total_rows // CHUNK_ROWS
    chunk_sizes = [CHUNK_ROWS] * full_chunks
    remainder = total_rows - full_chunks * CHUNK_ROWS
    if remainder > 0 or not chunk_sizes:
        chunk_sizes.append(remainder)
    counts = {
        "hr_null": 0,
        "weather_null": 0,
        "sat_null": 0,
        "hr_outlier": 0,
        "speed_outlier": 0,
        "airtime_outlier": 0,
        "lat_zero": 0,
        "bad_timestamp": 0,
        "dup_rows": 0,
    }
    rows_written = 0
    started = time.time()
    for index, n_rows in enumerate(chunk_sizes, start=1):
        chunk, chunk_counts = gen.generate_chunk(n_rows)
        for key in counts:
            counts[key] += chunk_counts[key]
        if index == 1:
            chunk.to_csv(out_path, index=False, encoding="utf-8")
        else:
            chunk.to_csv(out_path, mode="a", header=False, index=False, encoding="utf-8")
        rows_written += len(chunk)
        size_gb_now = out_path.stat().st_size / (1024 ** 3)
        elapsed = int(time.time() - started)
        print(
            "chunk {}/{} | rows={:.2f}M | size={:.2f} GB | elapsed={}s".format(
                index, len(chunk_sizes), len(chunk) / 1e6, size_gb_now, elapsed
            )
        )
    return rows_written, counts


def main():
    started = time.time()
    args = parse_args()
    out_dir = pathlib.Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    rng = np.random.default_rng(args.seed)
    osm_df, mode, region_stats = fetch_osm_trails(rng, args.offline, out_dir, args.refresh_osm)
    osm_path = out_dir / "raw_osm_trails.csv"
    osm_df.to_csv(osm_path, index=False, encoding="utf-8")
    elev_df = build_elevation_grid(osm_df, rng)
    base_elev_rows = len(elev_df)
    elev_df = inject_elevation_anomalies(elev_df, rng)
    elev_path = out_dir / "raw_srtm_elevation.csv"
    elev_df.to_csv(elev_path, index=False, encoding="utf-8")
    print("raw_srtm_elevation.csv rows={}".format(len(elev_df)))
    tel_path = out_dir / "raw_rider_telemetry.csv"
    tel_rows, counts = generate_telemetry_file(osm_df, args.size_gb, tel_path, args.seed)
    voids = int((elev_df["elevation_m"] == -32768).sum())
    spikes = int((elev_df["elevation_m"] == 99999).sum())
    elev_dup_rows = len(elev_df) - base_elev_rows
    osm_mb = osm_path.stat().st_size / (1024 ** 2)
    elev_mb = elev_path.stat().st_size / (1024 ** 2)
    tel_bytes = tel_path.stat().st_size
    print("=== Stage 1 complete ===")
    print("Mode: {}".format(mode))
    print(
        "{:<20}{:<32}{:>9}{:>7}{:>8}".format(
            "region", "data_source", "elements", "ways", "nodes"
        )
    )
    for stat in region_stats:
        elements_text = "-" if stat["elements"] is None else str(stat["elements"])
        print(
            "{:<20}{:<32}{:>9}{:>7}{:>8}".format(
                stat["region"],
                stat["data_source"],
                elements_text,
                stat["ways"],
                stat["nodes"],
            )
        )
    print("raw_osm_trails.csv        rows={}   size={:.1f} MB".format(len(osm_df), osm_mb))
    print(
        "raw_srtm_elevation.csv    rows={}   size={:.1f} MB   (voids={}, spikes={}, dup_rows={})".format(
            len(elev_df), elev_mb, voids, spikes, elev_dup_rows
        )
    )
    print(
        "raw_rider_telemetry.csv   rows={}   size={} (target {:.2f} GB)".format(
            tel_rows, fmt_size(tel_bytes), args.size_gb
        )
    )
    print(
        "Anomaly counts: hr_null={}, weather_null={}, sat_null={}, hr_outlier={},".format(
            counts["hr_null"], counts["weather_null"], counts["sat_null"], counts["hr_outlier"]
        )
    )
    print(
        "speed_outlier={}, airtime_outlier={}, lat_zero={}, bad_timestamp={}, dup_rows={}".format(
            counts["speed_outlier"],
            counts["airtime_outlier"],
            counts["lat_zero"],
            counts["bad_timestamp"],
            counts["dup_rows"],
        )
    )
    print("Elapsed: {}s".format(int(time.time() - started)))


if __name__ == "__main__":
    main()
