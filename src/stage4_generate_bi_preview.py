"""Stage 4 BI preview generator for the Arkhyz big data pipeline.

Reads the Stage 3 gold CSV marts under data/gold/csv, renders four static
matplotlib figures into bi/figures, and writes a self-contained HTML dashboard
at bi/arkhyz_interactive_dashboard.html. pandas + matplotlib only, no Spark.
"""

import argparse
import json
import pathlib
import sys

PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
GOLD_CSV_DIR = PROJECT_ROOT / "data" / "gold" / "csv"
BI_DIR = PROJECT_ROOT / "bi"
FIGURES_DIR = BI_DIR / "figures"
DASHBOARD_PATH = BI_DIR / "arkhyz_interactive_dashboard.html"

EXPECTED_CSVS = [
    "gold_trail_leaderboard.csv",
    "gold_temporal_trends.csv",
    "gold_arkhyz_trail_map_nodes.csv",
    "gold_cohort_distributions.csv",
]

# Alpine palette
DEEP_PINE = "#1B4332"
GLACIAL_CYAN = "#00B4D8"
SUNSET_AMBER = "#F77F00"
SLATE_PEAK = "#2B2D42"
PALETTE = [DEEP_PINE, GLACIAL_CYAN, SUNSET_AMBER, SLATE_PEAK, "#8AB17D", "#E9C46A"]

FIG_MONTHLY = "fig_monthly_runs.png"
FIG_SPEED_SAT = "fig_speed_satisfaction.png"
FIG_COHORT = "fig_cohort_satisfaction.png"
FIG_DROP_TOP = "fig_vertical_drop_top.png"

STYLE_BLOCK = """<style>
    :root {
        --deep-pine: #1B4332;
        --glacial-cyan: #00B4D8;
        --sunset-amber: #F77F00;
        --slate-peak: #2B2D42;
        --mist: #F4F7F5;
        --paper: #FFFFFF;
        --line: #D8E2DC;
    }
    * { box-sizing: border-box; }
    body {
        margin: 0;
        font-family: "Segoe UI", "Helvetica Neue", Arial, sans-serif;
        background: var(--mist);
        color: var(--slate-peak);
        line-height: 1.5;
    }
    .hero {
        background: linear-gradient(135deg, var(--deep-pine) 0%, var(--slate-peak) 100%);
        color: var(--paper);
        padding: 34px 40px 30px;
        border-bottom: 6px solid var(--glacial-cyan);
    }
    .eyebrow {
        margin: 0 0 8px;
        letter-spacing: 0.18em;
        font-size: 12px;
        font-weight: 600;
        color: var(--glacial-cyan);
    }
    h1 {
        margin: 0 0 10px;
        font-size: 32px;
        line-height: 1.15;
        font-weight: 700;
    }
    .subtitle {
        margin: 0;
        max-width: 980px;
        font-size: 14px;
        color: #D5E3DD;
    }
    .control-bar {
        position: sticky;
        top: 0;
        z-index: 1200;
        background: rgba(255, 255, 255, 0.97);
        border-bottom: 1px solid var(--line);
        box-shadow: 0 2px 10px rgba(43, 45, 66, 0.08);
        padding: 10px 24px;
        display: flex;
        flex-wrap: wrap;
        gap: 18px;
        align-items: center;
    }
    .control-group {
        display: flex;
        flex-wrap: wrap;
        gap: 6px;
        align-items: center;
    }
    .control-label {
        font-size: 11px;
        font-weight: 700;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        color: #5C677D;
        margin-right: 2px;
    }
    .region-btn, .diff-pill, .color-btn {
        font-family: inherit;
        font-size: 12px;
        font-weight: 600;
        color: var(--slate-peak);
        background: var(--paper);
        border: 1px solid var(--line);
        border-radius: 999px;
        padding: 6px 12px;
        cursor: pointer;
        transition: background 0.15s, color 0.15s, border-color 0.15s;
    }
    .region-btn:hover, .diff-pill:hover, .color-btn:hover {
        border-color: var(--glacial-cyan);
    }
    .region-btn.active, .diff-pill.active, .color-btn.active {
        background: var(--deep-pine);
        border-color: var(--deep-pine);
        color: #FFFFFF;
    }
    main {
        max-width: 1360px;
        margin: 0 auto;
        padding: 24px 24px 12px;
    }
    .kpi-row {
        display: flex;
        flex-wrap: wrap;
        gap: 14px;
        margin-bottom: 22px;
    }
    .kpi {
        flex: 1 1 180px;
        background: var(--paper);
        border: 1px solid var(--line);
        border-left: 5px solid var(--glacial-cyan);
        border-radius: 10px;
        padding: 14px 16px;
        box-shadow: 0 2px 8px rgba(43, 45, 66, 0.06);
    }
    .kpi:nth-child(2) { border-left-color: var(--sunset-amber); }
    .kpi:nth-child(3) { border-left-color: var(--deep-pine); }
    .kpi:nth-child(4) { border-left-color: var(--slate-peak); }
    .kpi:nth-child(5) { border-left-color: #8AB17D; }
    .kpi-label {
        display: block;
        font-size: 11px;
        font-weight: 700;
        letter-spacing: 0.06em;
        text-transform: uppercase;
        color: #5C677D;
        margin-bottom: 6px;
    }
    .kpi-value {
        display: block;
        font-size: 26px;
        font-weight: 700;
        color: var(--deep-pine);
        line-height: 1.1;
    }
    .kpi-note {
        display: block;
        margin-top: 6px;
        font-size: 11px;
        color: #6B7280;
        min-height: 14px;
    }
    .map-row {
        display: flex;
        gap: 18px;
        margin-bottom: 22px;
    }
    .map-panel, .profile-panel {
        background: var(--paper);
        border: 1px solid var(--line);
        border-radius: 10px;
        padding: 10px;
        box-shadow: 0 2px 8px rgba(43, 45, 66, 0.06);
    }
    .map-panel { flex: 0 0 60%; }
    .profile-panel { flex: 1 1 40%; min-width: 0; }
    #map {
        width: 100%;
        height: 460px;
        border-radius: 6px;
        z-index: 0;
    }
    #profile {
        width: 100%;
        height: 460px;
    }
    .panel {
        background: var(--paper);
        border: 1px solid var(--line);
        border-radius: 10px;
        padding: 16px 18px;
        box-shadow: 0 2px 8px rgba(43, 45, 66, 0.06);
        margin-bottom: 22px;
    }
    .panel h2 {
        margin: 0 0 12px;
        font-size: 16px;
        color: var(--deep-pine);
        letter-spacing: 0.01em;
    }
    #panel-a {
        width: 100%;
        height: 380px;
    }
    .panel-b-row {
        display: flex;
        gap: 18px;
    }
    .cohort-cell {
        flex: 1 1 50%;
        min-width: 0;
    }
    #cohort-bar, #cohort-heatmap {
        width: 100%;
        height: 360px;
    }
    .lb-toolbar {
        display: flex;
        flex-wrap: wrap;
        gap: 12px;
        align-items: center;
        margin-bottom: 10px;
    }
    #lb-search {
        font-family: inherit;
        font-size: 13px;
        padding: 7px 12px;
        border: 1px solid var(--line);
        border-radius: 8px;
        min-width: 240px;
    }
    .lb-table-wrap {
        max-height: 360px;
        overflow: auto;
        border: 1px solid var(--line);
        border-radius: 8px;
    }
    table {
        border-collapse: collapse;
        width: 100%;
        font-size: 13px;
    }
    thead th {
        position: sticky;
        top: 0;
        background: var(--deep-pine);
        color: #FFFFFF;
        text-align: left;
        padding: 8px 10px;
        font-size: 11px;
        letter-spacing: 0.05em;
        text-transform: uppercase;
        cursor: pointer;
        user-select: none;
        white-space: nowrap;
    }
    thead th:hover { background: #245741; }
    tbody td {
        padding: 7px 10px;
        border-bottom: 1px solid var(--line);
    }
    tbody tr { cursor: pointer; }
    tbody tr:hover { background: #EEF5F1; }
    tbody tr.selected { background: #D8F0E5; }
    #leaderboard-bar {
        width: 100%;
        height: 420px;
        margin-top: 14px;
    }
    .empty-state {
        display: flex;
        align-items: center;
        justify-content: center;
        height: 100%;
        color: #8895A7;
        font-size: 14px;
    }
    details.legacy {
        margin: 0 0 20px;
        background: #EEF2EF;
        border: 1px solid var(--line);
        border-radius: 10px;
        padding: 12px 18px;
    }
    details.legacy summary {
        cursor: pointer;
        font-weight: 700;
        color: var(--deep-pine);
        font-size: 14px;
    }
    .kpi-snapshot {
        font-size: 13px;
        color: var(--slate-peak);
        background: var(--paper);
        border: 1px dashed var(--line);
        border-radius: 8px;
        padding: 8px 12px;
        margin: 12px 0 0;
    }
    .chart-grid {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 16px;
        margin-top: 14px;
    }
    .chart {
        margin: 0;
        background: var(--paper);
        border: 1px solid var(--line);
        border-radius: 8px;
        padding: 10px;
    }
    .chart img {
        display: block;
        width: 100%;
        height: auto;
        border-radius: 4px;
    }
    .chart figcaption {
        margin-top: 8px;
        font-size: 12px;
        font-weight: 600;
        color: var(--slate-peak);
        border-top: 1px solid var(--line);
        padding-top: 8px;
    }
    .toast {
        position: fixed;
        left: 50%;
        bottom: 26px;
        transform: translateX(-50%);
        background: var(--slate-peak);
        color: #FFFFFF;
        padding: 10px 18px;
        border-radius: 8px;
        font-size: 13px;
        z-index: 5000;
        box-shadow: 0 6px 20px rgba(0, 0, 0, 0.25);
    }
    footer {
        max-width: 1360px;
        margin: 0 auto;
        padding: 10px 24px 40px;
        font-size: 12px;
        color: #5C677D;
    }
    .lineage strong { color: var(--deep-pine); }
    .meta { margin-top: 6px; }
    @media (max-width: 980px) {
        .map-row { flex-direction: column; }
        .map-panel, .profile-panel { flex: 1 1 auto; }
        .panel-b-row { flex-direction: column; }
        .chart-grid { grid-template-columns: 1fr; }
    }
    @media (max-width: 620px) {
        .hero { padding: 24px 20px; }
        h1 { font-size: 24px; }
        main { padding: 18px 14px 8px; }
        .control-bar { padding: 8px 14px; }
    }
</style>"""

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Arkhyz Alpine Interactive Trail Analytics</title>
<link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css">
__STYLE__
</head>
<body>
<header class="hero">
<p class="eyebrow">ARKHYZ ALPINE ANALYTICS</p>
<h1>Arkhyz Alpine Interactive Trail Analytics</h1>
<p class="subtitle">Interactive local dashboard of the Stage 3 HiveSQL gold marts: Leaflet trail map, elevation and telemetry profiles, monthly run trends, cohort distributions and a sortable trail leaderboard. Filter by region, difficulty tier and trail color mode.</p>
</header>
<div class="control-bar">
<div class="control-group">
<span class="control-label">Region</span>
<button type="button" class="region-btn active" data-region="ALL">All Caucasus (3 Regions)</button>
<button type="button" class="region-btn" data-region="Arkhyz">Arkhyz (Karachay-Cherkessia)</button>
<button type="button" class="region-btn" data-region="Dombay_Teberda">Dombay &amp; Teberda</button>
<button type="button" class="region-btn" data-region="Krasnaya_Polyana">Krasnaya Polyana</button>
</div>
<div class="control-group">
<span class="control-label">Difficulty</span>
<button type="button" class="diff-pill active" data-diff="All_Tiers">All tiers</button>
<button type="button" class="diff-pill" data-diff="Green_Easy">Green Easy</button>
<button type="button" class="diff-pill" data-diff="Blue_Intermediate">Blue Intermediate</button>
<button type="button" class="diff-pill" data-diff="Black_Advanced">Black Advanced</button>
<button type="button" class="diff-pill" data-diff="DoubleBlack_Pro">Double Black Pro</button>
</div>
<div class="control-group">
<span class="control-label">Color by</span>
<button type="button" class="color-btn active" data-color="difficulty_tier">Difficulty</button>
<button type="button" class="color-btn" data-color="avg_speed_kmh">Avg speed</button>
<button type="button" class="color-btn" data-color="elevation_m">Elevation</button>
</div>
</div>
<main>
<section class="kpi-row" aria-label="Key performance indicators">
<div class="kpi"><span class="kpi-label">Total runs</span><span class="kpi-value" id="kpi-total-runs">-</span><span class="kpi-note">Unique runs in the current filters</span></div>
<div class="kpi"><span class="kpi-label">Weighted avg satisfaction</span><span class="kpi-value" id="kpi-satisfaction">-</span><span class="kpi-note">Run-weighted, scale 1-5</span></div>
<div class="kpi"><span class="kpi-label">Max vertical drop</span><span class="kpi-value" id="kpi-max-drop">-</span><span class="kpi-note" id="kpi-drop-note"></span></div>
<div class="kpi"><span class="kpi-label">MoM run growth</span><span class="kpi-value" id="kpi-mom">-</span><span class="kpi-note" id="kpi-mom-note"></span></div>
<div class="kpi"><span class="kpi-label">Mapped trails</span><span class="kpi-value" id="kpi-mapped-trails">-</span><span class="kpi-note">Distinct trails with GPS nodes</span></div>
</section>
<section class="map-row">
<div class="map-panel"><div id="map"></div></div>
<div class="profile-panel"><div id="profile"></div></div>
</section>
<section class="panel">
<h2>Panel A - Monthly Runs and Rolling Satisfaction / Speed</h2>
<div id="panel-a"></div>
</section>
<section class="panel">
<h2>Panel B - Cohort Intensity and Satisfaction (weather x speed bucket)</h2>
<div class="panel-b-row">
<div class="cohort-cell"><div id="cohort-bar"></div></div>
<div class="cohort-cell"><div id="cohort-heatmap"></div></div>
</div>
</section>
<section class="panel">
<h2>Panel C - Trail Leaderboard</h2>
<div class="lb-toolbar"><input id="lb-search" type="search" placeholder="Search trail name..."></div>
<div class="lb-table-wrap">
<table>
<thead>
<tr>
<th data-col="rank">Rank</th>
<th data-col="trail_name">Trail</th>
<th data-col="region">Region</th>
<th data-col="difficulty_tier">Tier</th>
<th data-col="unique_runs">Runs</th>
<th data-col="avg_satisfaction">Satisfaction</th>
<th data-col="avg_speed_kmh">Speed</th>
<th data-col="total_vertical_drop_m">Vert Drop</th>
</tr>
</thead>
<tbody id="lb-tbody"></tbody>
</table>
</div>
<div id="leaderboard-bar"></div>
</section>
<details class="legacy">
<summary>Static PNG exports (legacy)</summary>
<p class="kpi-snapshot">__KPI_SNAPSHOT__</p>
<div class="chart-grid">
__STATIC_FIGURES__
</div>
</details>
</main>
<footer>
<p class="lineage"><strong>Data lineage:</strong> raw OSM, SRTM and telemetry CSVs -&gt; Stage 2 PySpark ETL (Parquet) -&gt; Stage 3 HiveSQL gold marts -&gt; this interactive dashboard. Regenerate with src/stage4_generate_bi_preview.py.</p>
<p class="meta">Data period: __PERIOD__ - generated by the Stage 4 BI preview generator.</p>
</footer>
<script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
<script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
__DATA_SCRIPT__
<script>
const DATA = window.ARKHYZ_DATA;
const DIFFICULTY_COLORS = {Green_Easy:"#2ecc71",Blue_Intermediate:"#00b4d8",Black_Advanced:"#f77f00",DoubleBlack_Pro:"#e63946"};
const REGION_LABELS = {Arkhyz:"Arkhyz (Karachay-Cherkessia)",Dombay_Teberda:"Dombay & Teberda",Krasnaya_Polyana:"Krasnaya Polyana"};
const REGIONS = ["Arkhyz","Dombay_Teberda","Krasnaya_Polyana"];
const WEATHER_ORDER = ["Sunny","Muddy","Rain","Fog","Snow"];
const SPEED_ORDER = ["Slow_Climb","Cruising","Fast_Flow","High_Speed_Send"];
const ACTIVITY_ORDER = ["MTB_DH","MTB_Enduro","Trail_Run","Hiking"];
const state = {region:"ALL",difficulty:"All_Tiers",colorBy:"difficulty_tier",selectedWayId:null};
let TRAILS = {};
let LEADERBOARD_BY_WAY = {};
let LEADERBOARD_BAR_ROWS = [];
let REGION_BOUNDS = {};
let ALL_BOUNDS = null;
let map = null;
let trailLayer = null;
let selectionLayer = null;
let sortState = {col:"unique_runs",dir:"desc"};
let searchTerm = "";

function pad2(n) { n = Number(n); return n < 10 ? "0" + n : String(n); }
function isNum(v) { return v !== null && v !== undefined && v !== "" && !isNaN(Number(v)); }
function fmt(v) { if (!isNum(v)) return "n/a"; return Number(v).toFixed(2); }
function fmt0(v) { if (!isNum(v)) return "n/a"; return Math.round(Number(v)).toLocaleString(); }
function esc(s) {
  if (s === null || s === undefined) return "";
  return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

function buildTrails() {
  TRAILS = {};
  const rows = DATA.trail_map_nodes || [];
  rows.forEach(function(n) {
    const wid = String(n.way_id);
    let t = TRAILS[wid];
    if (!t) {
      t = {way_id:wid, region:n.region, trail_name:n.trail_name, difficulty_tier:n.difficulty_tier, surface_clean:n.surface_clean, nodes:[]};
      TRAILS[wid] = t;
    }
    t.nodes.push(n);
  });
  Object.keys(TRAILS).forEach(function(wid) {
    const t = TRAILS[wid];
    t.nodes.sort(function(a, b) { return Number(a.seq_order) - Number(b.seq_order); });
    t.latlngs = t.nodes.map(function(n) { return [Number(n.lat), Number(n.lon)]; });
    t.min_elev = t.nodes.length ? Math.min.apply(null, t.nodes.map(function(n) { return Number(n.elevation_m); })) : null;
    t.max_elev = t.nodes.length ? Math.max.apply(null, t.nodes.map(function(n) { return Number(n.elevation_m); })) : null;
    const nodeSpeeds = t.nodes.map(function(n) { return Number(n.avg_speed_kmh); }).filter(function(v) { return !isNaN(v); });
    t.avg_speed = nodeSpeeds.length ? nodeSpeeds.reduce(function(a, b) { return a + b; }, 0) / nodeSpeeds.length : null;
    t.center = t.latlngs.length ? L.latLngBounds(t.latlngs).getCenter() : null;
  });
  LEADERBOARD_BY_WAY = {};
  (DATA.trail_leaderboard || []).forEach(function(r) { LEADERBOARD_BY_WAY[String(r.way_id)] = r; });
}

function buildBounds() {
  REGION_BOUNDS = {};
  const regionPoints = {};
  const all = [];
  Object.keys(TRAILS).forEach(function(wid) {
    const t = TRAILS[wid];
    if (!t.latlngs.length) return;
    if (!regionPoints[t.region]) regionPoints[t.region] = [];
    t.latlngs.forEach(function(p) {
      regionPoints[t.region].push(p);
      all.push(p);
    });
  });
  Object.keys(regionPoints).forEach(function(region) {
    REGION_BOUNDS[region] = L.latLngBounds(regionPoints[region]);
  });
  ALL_BOUNDS = all.length ? L.latLngBounds(all) : L.latLngBounds([[43.0, 40.5], [43.9, 41.9]]);
}

function initMap() {
  map = L.map("map").setView([43.45, 41.3], 9);
  const topo = L.tileLayer("https://{s}.tile.opentopomap.org/{z}/{x}/{y}.png", {maxZoom:17, attribution:"Map data: (c) OpenStreetMap contributors, SRTM | Style: (c) OpenTopoMap (CC-BY-SA)"});
  const osm = L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {maxZoom:19, attribution:"(c) OpenStreetMap contributors"});
  const esri = L.tileLayer("https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}", {maxZoom:19, attribution:"Tiles (c) Esri, Maxar, Earthstar Geographics"});
  topo.addTo(map);
  const baseLayers = {"OpenTopoMap":topo, "OpenStreetMap":osm, "Esri World Imagery":esri};
  L.control.layers(baseLayers, null, {position:"topright"}).addTo(map);
  trailLayer = L.layerGroup().addTo(map);
  selectionLayer = L.layerGroup().addTo(map);
}

function regionMatch(r) { return state.region === "ALL" || r.region === state.region; }
function difficultyMatch(r) { return state.difficulty === "All_Tiers" || r.difficulty_tier === state.difficulty; }
function trailMatchesFilters(t) { return regionMatch(t) && difficultyMatch(t); }
function filteredLeaderboard() { return (DATA.trail_leaderboard || []).filter(function(r) { return regionMatch(r) && difficultyMatch(r); }); }
function filteredNodes() { return (DATA.trail_map_nodes || []).filter(function(r) { return regionMatch(r) && difficultyMatch(r); }); }
function filteredTrends() { return (DATA.temporal_trends || []).filter(function(r) { return regionMatch(r); }); }
function filteredCohorts() { return (DATA.cohort_distributions || []).filter(function(r) { return regionMatch(r) && difficultyMatch(r); }); }

function speedColor(v) {
  if (!isNum(v)) return "#888";
  const n = Number(v);
  if (n < 8) return "#457b9d";
  if (n < 15) return "#2ecc71";
  if (n < 25) return "#f77f00";
  return "#e63946";
}

function elevationColor(v) {
  if (!isNum(v)) return "#888";
  const n = Number(v);
  if (n < 1800) return "#2ecc71";
  if (n < 2500) return "#f77f00";
  if (n < 3200) return "#e63946";
  return "#7b2cbf";
}

function colorFor(t) {
  if (state.colorBy === "avg_speed_kmh") {
    const lb = LEADERBOARD_BY_WAY[t.way_id];
    const speed = lb && isNum(lb.avg_speed_kmh) ? lb.avg_speed_kmh : t.avg_speed;
    return speedColor(speed);
  }
  if (state.colorBy === "elevation_m") return elevationColor(t.max_elev);
  return DIFFICULTY_COLORS[t.difficulty_tier] || "#888";
}

function tooltipHtml(t) {
  const lb = LEADERBOARD_BY_WAY[t.way_id] || {};
  const minE = isNum(lb.min_elev_m) ? lb.min_elev_m : t.min_elev;
  const maxE = isNum(lb.max_elev_m) ? lb.max_elev_m : t.max_elev;
  return "<div><b>" + esc(t.trail_name) + "</b></div>" +
    "<div>Region: " + esc(REGION_LABELS[t.region] || t.region) + "</div>" +
    "<div>Difficulty Tier: " + esc(t.difficulty_tier) + "</div>" +
    "<div>Surface: " + esc(t.surface_clean) + "</div>" +
    "<div>Elevation: " + fmt0(minE) + " - " + fmt0(maxE) + " m</div>" +
    "<div>Avg Speed: " + fmt(lb.avg_speed_kmh) + " km/h</div>" +
    "<div>Avg Airtime: " + fmt(lb.avg_airtime_ms) + " ms</div>" +
    "<div>Avg G-Force: " + fmt(lb.avg_g_force) + "</div>" +
    "<div>Satisfaction: " + fmt(lb.avg_satisfaction) + "</div>";
}

function renderTrails() {
  trailLayer.clearLayers();
  Object.keys(TRAILS).forEach(function(wid) {
    const t = TRAILS[wid];
    if (!t.latlngs.length || !trailMatchesFilters(t)) return;
    const poly = L.polyline(t.latlngs, {color:colorFor(t), weight:3, opacity:0.85});
    poly.bindTooltip(tooltipHtml(t), {sticky:true});
    poly.on("click", function() { selectTrail(wid); });
    poly.on("mouseover", function() { poly.setStyle({weight:6}); });
    poly.on("mouseout", function() { poly.setStyle({weight:3}); });
    trailLayer.addLayer(poly);
  });
}

function selectTrail(wid) {
  wid = String(wid);
  state.selectedWayId = wid;
  selectionLayer.clearLayers();
  const t = TRAILS[wid];
  if (!t) {
    showToast("No GPS node data for this trail");
    return;
  }
  selectionLayer.addLayer(L.polyline(t.latlngs, {color:"#111", weight:6, opacity:0.9}));
  t.nodes.forEach(function(n) {
    const marker = L.circleMarker([Number(n.lat), Number(n.lon)], {radius:5, color:"#111", weight:1, fillColor:DIFFICULTY_COLORS[t.difficulty_tier] || "#888", fillOpacity:0.9});
    marker.bindTooltip("Seq " + n.seq_order + " | " + fmt0(n.elevation_m) + " m | " + fmt(n.avg_speed_kmh) + " km/h");
    selectionLayer.addLayer(marker);
  });
  map.flyToBounds(L.latLngBounds(t.latlngs), {padding:[40, 40], duration:0.8});
  renderElevationProfile(wid);
  highlightLeaderboardRow(wid);
}

function flyToRegion(region) {
  if (region === "ALL" && ALL_BOUNDS) {
    map.flyToBounds(ALL_BOUNDS, {padding:[30, 30], duration:1.2});
  } else if (REGION_BOUNDS[region]) {
    map.flyToBounds(REGION_BOUNDS[region], {padding:[30, 30], duration:1.2});
  }
}

function setEmpty(id, msg) {
  const el = document.getElementById(id);
  try { Plotly.purge(id); } catch (e) {}
  if (el) el.innerHTML = '<div class="empty-state">' + msg + "</div>";
}

function renderElevationProfile(wid) {
  const t = TRAILS[wid];
  if (!t || !t.nodes.length) {
    setEmpty("profile", "No GPS node data for this trail.");
    return;
  }
  const nodes = t.nodes;
  const x = nodes.map(function(n) { return Number(n.cumulative_dist_m); });
  const elev = nodes.map(function(n) { return Number(n.elevation_m); });
  const speed = nodes.map(function(n) { return Number(n.avg_speed_kmh); });
  const airtime = nodes.map(function(n) { return Number(n.avg_airtime_ms); });
  const grade = nodes.map(function(n) { return Number(n.segment_grade_pct); });
  const t1 = {x:x, y:elev, type:"scatter", mode:"lines", fill:"tozeroy", fillcolor:"rgba(27,67,50,0.25)", line:{color:"#1B4332", width:2}, name:"Elevation (m)"};
  const t2 = {x:x, y:speed, type:"scatter", mode:"lines+markers", line:{color:"#F77F00"}, name:"Speed (km/h)", yaxis:"y2"};
  const t3 = {x:x, y:airtime, type:"scatter", mode:"lines+markers", line:{color:"#00B4D8"}, name:"Airtime (ms)", yaxis:"y2"};
  const t4 = {x:x, y:grade, type:"scatter", mode:"lines", line:{color:"#2B2D42", dash:"dot"}, name:"Grade (%)", yaxis:"y2"};
  const layout = {
    title:{text:t.trail_name + " - Elevation & Telemetry Profile", font:{size:15, color:"#1B4332"}},
    xaxis:{title:"Distance along trail (m)"},
    yaxis:{title:"Elevation (m)"},
    yaxis2:{title:"Speed / Airtime / Grade", overlaying:"y", side:"right"},
    legend:{orientation:"h", x:0.5, xanchor:"center", y:-0.35, font:{size:11}, itemwidth:90, tracegroupgap:25},
    margin:{l:60, r:60, t:60, b:100},
    paper_bgcolor:"#FFFFFF",
    plot_bgcolor:"#FFFFFF"
  };
  Plotly.newPlot("profile", [t1, t2, t3, t4], layout, {responsive:true, displaylogo:false});
}

function renderKpis() {
  const lb = filteredLeaderboard();
  let runs = 0;
  let satWeighted = 0;
  let runWeight = 0;
  let maxDrop = 0;
  let topTrail = "";
  lb.forEach(function(r) {
    const ur = Number(r.unique_runs) || 0;
    runs += ur;
    satWeighted += (Number(r.avg_satisfaction) || 0) * ur;
    runWeight += ur;
    const d = Number(r.total_vertical_drop_m);
    if (!isNaN(d) && d > maxDrop) {
      maxDrop = d;
      topTrail = r.trail_name;
    }
  });
  document.getElementById("kpi-total-runs").textContent = runs.toLocaleString();
  document.getElementById("kpi-satisfaction").textContent = runWeight > 0 ? (satWeighted / runWeight).toFixed(2) : "n/a";
  document.getElementById("kpi-max-drop").textContent = maxDrop > 0 ? Math.round(maxDrop).toLocaleString() + " m" : "n/a";
  document.getElementById("kpi-drop-note").textContent = topTrail ? "Top trail: " + topTrail : "";

  const trends = filteredTrends();
  let latest = null;
  trends.forEach(function(r) {
    const k = Number(r.year) * 100 + Number(r.month);
    if (latest === null || k > latest) latest = k;
  });
  let latestRuns = 0;
  let prevRuns = 0;
  if (latest !== null) {
    trends.forEach(function(r) {
      if (Number(r.year) * 100 + Number(r.month) === latest) {
        latestRuns += Number(r.monthly_runs) || 0;
        prevRuns += Number(r.prev_month_runs) || 0;
      }
    });
  }
  const growth = prevRuns > 0 ? (latestRuns - prevRuns) / prevRuns * 100 : 0;
  const label = latest !== null ? Math.floor(latest / 100) + "-" + pad2(latest % 100) : "n/a";
  document.getElementById("kpi-mom").textContent = (growth >= 0 ? "+" : "") + growth.toFixed(2) + "%";
  document.getElementById("kpi-mom-note").textContent = latest !== null ? (Math.round(latestRuns).toLocaleString() + " runs vs " + Math.round(prevRuns).toLocaleString() + " prior month (" + label + ")") : "";

  const nodeWays = {};
  filteredNodes().forEach(function(n) { nodeWays[String(n.way_id)] = true; });
  document.getElementById("kpi-mapped-trails").textContent = Object.keys(nodeWays).length.toLocaleString();
}

function renderTemporalPanel() {
  const rows = filteredTrends();
  if (!rows.length) {
    setEmpty("panel-a", "No temporal data for the current filters.");
    return;
  }
  const keySet = {};
  rows.forEach(function(r) { keySet[Number(r.year) * 100 + Number(r.month)] = true; });
  const keys = Object.keys(keySet).map(Number).sort(function(a, b) { return a - b; });
  const labels = keys.map(function(k) { return Math.floor(k / 100) + "-" + pad2(k % 100); });
  const runs = {};
  const sat = {};
  const speed = {};
  ACTIVITY_ORDER.forEach(function(a) {
    runs[a] = {};
    sat[a] = {};
    speed[a] = {};
  });
  rows.forEach(function(r) {
    const a = r.activity_type;
    if (!runs[a]) return;
    const k = Number(r.year) * 100 + Number(r.month);
    runs[a][k] = (runs[a][k] || 0) + (Number(r.monthly_runs) || 0);
    if (isNum(r.rolling_3m_satisfaction)) sat[a][k] = Number(r.rolling_3m_satisfaction);
    if (isNum(r.rolling_3m_speed_kmh)) speed[a][k] = Number(r.rolling_3m_speed_kmh);
  });
  const traces = ACTIVITY_ORDER.map(function(a) {
    return {x:labels, y:keys.map(function(k) { return runs[a][k] || 0; }), type:"scatter", mode:"lines+markers", name:a};
  });
  const satAvg = keys.map(function(k) {
    let sum = 0;
    let count = 0;
    ACTIVITY_ORDER.forEach(function(a) {
      if (sat[a][k] !== undefined) {
        sum += sat[a][k];
        count++;
      }
    });
    return count ? sum / count : null;
  });
  const speedAvg = keys.map(function(k) {
    let sum = 0;
    let count = 0;
    ACTIVITY_ORDER.forEach(function(a) {
      if (speed[a][k] !== undefined) {
        sum += speed[a][k];
        count++;
      }
    });
    return count ? sum / count : null;
  });
  traces.push({x:labels, y:satAvg, type:"scatter", mode:"lines", name:"Rolling 3m satisfaction", yaxis:"y2", line:{color:"#2B2D42", width:2}});
  traces.push({x:labels, y:speedAvg, type:"scatter", mode:"lines", name:"Rolling 3m speed", yaxis:"y2", line:{color:"#F77F00", width:2}});
  const layout = {
    title:{text:"Monthly Runs and Rolling Satisfaction / Speed (selected region)", font:{size:15, color:"#1B4332"}},
    xaxis:{title:"Month"},
    yaxis:{title:"Monthly runs"},
    yaxis2:{title:"Satisfaction (1-5) / Speed (km/h)", overlaying:"y", side:"right"},
    legend:{orientation:"h"},
    margin:{l:60, r:60, t:60, b:60}
  };
  Plotly.newPlot("panel-a", traces, layout, {responsive:true, displaylogo:false});
}

function renderCohortPanel() {
  const rows = filteredCohorts();
  if (!rows.length) {
    setEmpty("cohort-bar", "No cohort rows for the current filters.");
    setEmpty("cohort-heatmap", "No cohort rows for the current filters.");
    return;
  }
  const agg = {};
  WEATHER_ORDER.forEach(function(w) {
    SPEED_ORDER.forEach(function(s) {
      agg[w + "|" + s] = {intWeighted:0, satWeighted:0, runWeight:0};
    });
  });
  rows.forEach(function(r) {
    const w = r.weather_cond;
    const s = r.speed_bucket;
    if (WEATHER_ORDER.indexOf(w) < 0 || SPEED_ORDER.indexOf(s) < 0) return;
    const cell = agg[w + "|" + s];
    const rc = Number(r.run_count) || 0;
    cell.intWeighted += (Number(r.avg_intensity_score) || 0) * rc;
    cell.satWeighted += (Number(r.avg_satisfaction) || 0) * rc;
    cell.runWeight += rc;
  });
  const barTraces = SPEED_ORDER.map(function(s) {
    return {
      x:WEATHER_ORDER,
      y:WEATHER_ORDER.map(function(w) {
        const cell = agg[w + "|" + s];
        return cell.runWeight > 0 ? cell.intWeighted / cell.runWeight : null;
      }),
      type:"bar",
      name:s
    };
  });
  const barLayout = {
    barmode:"group",
    title:{text:"Weighted Avg Intensity by Weather and Speed Bucket", font:{size:14, color:"#1B4332"}},
    xaxis:{title:"Weather"},
    yaxis:{title:"Run-weighted intensity score"},
    legend:{orientation:"h"},
    margin:{l:60, r:20, t:60, b:60}
  };
  Plotly.newPlot("cohort-bar", barTraces, barLayout, {responsive:true, displaylogo:false});

  const heatRows = SPEED_ORDER.slice().reverse();
  const heatZ = heatRows.map(function(s) {
    return WEATHER_ORDER.map(function(w) {
      const cell = agg[w + "|" + s];
      return cell.runWeight > 0 ? cell.satWeighted / cell.runWeight : null;
    });
  });
  const heatTrace = [{z:heatZ, x:WEATHER_ORDER, y:heatRows, type:"heatmap", colorscale:"YlGnBu", colorbar:{title:"Satisfaction"}}];
  const heatLayout = {
    title:{text:"Run-weighted Satisfaction by Weather and Speed Bucket", font:{size:14, color:"#1B4332"}},
    xaxis:{title:"Weather"},
    yaxis:{title:"Speed bucket"},
    margin:{l:90, r:20, t:60, b:60}
  };
  Plotly.newPlot("cohort-heatmap", heatTrace, heatLayout, {responsive:true, displaylogo:false});
}

function compareRows(a, b, col) {
  let av = a[col];
  let bv = b[col];
  if (col === "trail_name" || col === "region" || col === "difficulty_tier") {
    return String(av).localeCompare(String(bv));
  }
  av = Number(av);
  bv = Number(bv);
  if (isNaN(av)) av = -Infinity;
  if (isNaN(bv)) bv = -Infinity;
  return av - bv;
}

function renderLeaderboard() {
  const tbody = document.getElementById("lb-tbody");
  let rows = filteredLeaderboard().filter(function(r) {
    if (!searchTerm) return true;
    return String(r.trail_name).toLowerCase().indexOf(searchTerm) >= 0;
  });
  if (sortState.col === "rank") {
    rows.sort(function(a, b) { return Number(b.unique_runs) - Number(a.unique_runs); });
  } else {
    rows.sort(function(a, b) {
      const cmp = compareRows(a, b, sortState.col);
      return sortState.dir === "asc" ? cmp : -cmp;
    });
  }
  const top = rows.slice(0, 50);
  const html = top.map(function(r, i) {
    return '<tr data-way="' + esc(r.way_id) + '">' +
      "<td>" + (i + 1) + "</td>" +
      "<td>" + esc(r.trail_name) + "</td>" +
      "<td>" + esc(REGION_LABELS[r.region] || r.region) + "</td>" +
      "<td>" + esc(r.difficulty_tier) + "</td>" +
      "<td>" + fmt0(r.unique_runs) + "</td>" +
      "<td>" + fmt(r.avg_satisfaction) + "</td>" +
      "<td>" + fmt(r.avg_speed_kmh) + "</td>" +
      "<td>" + fmt0(r.total_vertical_drop_m) + "</td>" +
      "</tr>";
  }).join("");
  tbody.innerHTML = html || '<tr><td colspan="8">No trails match the current filters.</td></tr>';
  Array.prototype.forEach.call(tbody.querySelectorAll("tr[data-way]"), function(tr) {
    tr.addEventListener("click", function() { selectTrail(tr.getAttribute("data-way")); });
  });
  if (state.selectedWayId) highlightLeaderboardRow(state.selectedWayId);

  const barRows = filteredLeaderboard().slice().sort(function(a, b) { return Number(b.unique_runs) - Number(a.unique_runs); }).slice(0, 15);
  LEADERBOARD_BAR_ROWS = barRows;
  if (!barRows.length) {
    setEmpty("leaderboard-bar", "No leaderboard rows for the current filters.");
    return;
  }
  const names = barRows.map(function(r) { return r.trail_name; }).reverse();
  const values = barRows.map(function(r) { return Number(r.unique_runs); }).reverse();
  const colors = barRows.map(function(r) { return DIFFICULTY_COLORS[r.difficulty_tier] || "#888"; }).reverse();
  const hover = barRows.map(function(r) { return r.trail_name + " - " + fmt0(r.unique_runs) + " runs"; }).reverse();
  const barTrace = {x:values, y:names, type:"bar", orientation:"h", marker:{color:colors}, hovertext:hover, hoverinfo:"text"};
  const barLayout = {
    title:{text:"Top 15 Trails by Unique Runs", font:{size:14, color:"#1B4332"}},
    xaxis:{title:"Unique runs"},
    yaxis:{automargin:true},
    margin:{l:160, r:30, t:60, b:50}
  };
  Plotly.newPlot("leaderboard-bar", [barTrace], barLayout, {responsive:true, displaylogo:false}).then(function(gd) {
    if (gd._arkhyzClickBound) return;
    gd.on("plotly_click", function(ev) {
      if (!ev || !ev.points || !ev.points.length) return;
      const idx = ev.points[0].pointIndex;
      const revIndex = LEADERBOARD_BAR_ROWS.length - 1 - idx;
      const row = LEADERBOARD_BAR_ROWS[revIndex];
      if (row) selectTrail(String(row.way_id));
    });
    gd._arkhyzClickBound = true;
  });
}

function highlightLeaderboardRow(wid) {
  const tbody = document.getElementById("lb-tbody");
  if (!tbody) return;
  Array.prototype.forEach.call(tbody.querySelectorAll("tr[data-way]"), function(tr) {
    tr.classList.toggle("selected", tr.getAttribute("data-way") === String(wid));
  });
}

function showToast(msg) {
  const div = document.createElement("div");
  div.className = "toast";
  div.textContent = msg;
  document.body.appendChild(div);
  setTimeout(function() {
    if (div.parentNode) div.parentNode.removeChild(div);
  }, 2500);
}

function refresh() {
  Array.prototype.forEach.call(document.querySelectorAll(".region-btn"), function(b) {
    b.classList.toggle("active", b.getAttribute("data-region") === state.region);
  });
  Array.prototype.forEach.call(document.querySelectorAll(".diff-pill"), function(b) {
    b.classList.toggle("active", b.getAttribute("data-diff") === state.difficulty);
  });
  Array.prototype.forEach.call(document.querySelectorAll(".color-btn"), function(b) {
    b.classList.toggle("active", b.getAttribute("data-color") === state.colorBy);
  });
  renderKpis();
  renderTrails();
  renderTemporalPanel();
  renderCohortPanel();
  renderLeaderboard();
}

function init() {
  buildTrails();
  buildBounds();
  initMap();

  Array.prototype.forEach.call(document.querySelectorAll(".region-btn"), function(btn) {
    btn.addEventListener("click", function() {
      state.region = btn.getAttribute("data-region");
      refresh();
      flyToRegion(state.region);
    });
  });
  Array.prototype.forEach.call(document.querySelectorAll(".diff-pill"), function(btn) {
    btn.addEventListener("click", function() {
      state.difficulty = btn.getAttribute("data-diff");
      refresh();
    });
  });
  Array.prototype.forEach.call(document.querySelectorAll(".color-btn"), function(btn) {
    btn.addEventListener("click", function() {
      state.colorBy = btn.getAttribute("data-color");
      Array.prototype.forEach.call(document.querySelectorAll(".color-btn"), function(b) {
        b.classList.toggle("active", b.getAttribute("data-color") === state.colorBy);
      });
      renderTrails();
    });
  });
  const search = document.getElementById("lb-search");
  if (search) {
    search.addEventListener("input", function() {
      searchTerm = search.value.toLowerCase();
      renderLeaderboard();
    });
  }
  Array.prototype.forEach.call(document.querySelectorAll("th[data-col]"), function(th) {
    th.addEventListener("click", function() {
      const col = th.getAttribute("data-col");
      if (col === "rank") return;
      if (sortState.col === col) {
        sortState.dir = sortState.dir === "asc" ? "desc" : "asc";
      } else {
        sortState.col = col;
        sortState.dir = (col === "trail_name" || col === "region" || col === "difficulty_tier") ? "asc" : "desc";
      }
      renderLeaderboard();
    });
  });

  refresh();

  let bestWid = null;
  let bestRuns = -1;
  Object.keys(TRAILS).forEach(function(wid) {
    const t = TRAILS[wid];
    if (t.region !== "Arkhyz") return;
    const lb = LEADERBOARD_BY_WAY[wid];
    const runs = lb ? Number(lb.unique_runs) : -1;
    if (runs > bestRuns) {
      bestRuns = runs;
      bestWid = wid;
    }
  });
  if (!bestWid) {
    Object.keys(TRAILS).forEach(function(wid) {
      if (!bestWid && TRAILS[wid].region === "Arkhyz") bestWid = wid;
    });
  }
  if (!bestWid) {
    const keys = Object.keys(TRAILS);
    if (keys.length) bestWid = keys[0];
  }
  if (bestWid) selectTrail(bestWid);
}

init();
</script>
</body>
</html>
"""

FIGURE_CARDS = [
    (FIG_MONTHLY, "Monthly runs by activity type for the Arkhyz region"),
    (FIG_SPEED_SAT, "Average speed versus average satisfaction per trail"),
    (FIG_COHORT, "Average satisfaction by difficulty cohort (run weighted)"),
    (FIG_DROP_TOP, "Top 10 trails by total vertical drop"),
]


def log(message):
    """Print a stage-prefixed log line."""
    print("[stage4] {0}".format(message), flush=True)


def load_pandas():
    """Import pandas, returning None when it is unavailable."""
    try:
        import pandas as pd
    except Exception as exc:
        log("pandas import failed: {0}".format(exc))
        return None
    return pd


def load_pyplot():
    """Import matplotlib with the non-interactive Agg backend before pyplot."""
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception as exc:
        log("matplotlib import failed: {0}".format(exc))
        return None, None
    return plt, matplotlib


def read_gold_csv(pd, filename):
    """Load one gold CSV mart from data/gold/csv."""
    path = GOLD_CSV_DIR / filename
    if not path.is_file():
        raise FileNotFoundError("missing gold CSV: {0}".format(path))
    return pd.read_csv(str(path))


def save_figure(fig, plt, path):
    """Apply tight layout and write a 150 dpi PNG."""
    fig.tight_layout()
    fig.savefig(str(path), dpi=150)
    plt.close(fig)
    log("wrote {0} ({1:,} bytes)".format(path.name, path.stat().st_size))


def build_monthly_runs_figure(pd, plt):
    """Line chart of monthly runs by activity type for Arkhyz."""
    trends = read_gold_csv(pd, "gold_temporal_trends.csv")
    arkhyz = trends[trends["region"] == "Arkhyz"].copy()
    arkhyz["month_label"] = (
        arkhyz["year"].astype(int).astype(str).str.zfill(4)
        + "-"
        + arkhyz["month"].astype(int).astype(str).str.zfill(2)
    )
    pivot = arkhyz.pivot_table(
        index="month_label",
        columns="activity_type",
        values="monthly_runs",
        aggfunc="sum",
    )
    pivot = pivot.reindex(sorted(pivot.index))

    fig, ax = plt.subplots(figsize=(10, 6))
    for position, activity in enumerate(pivot.columns):
        ax.plot(
            pivot.index,
            pivot[activity],
            label=str(activity),
            color=PALETTE[position % len(PALETTE)],
            linewidth=2.2,
            marker="o",
            markersize=3.2,
        )
    ax.set_title("Monthly Runs by Activity Type - Arkhyz", fontsize=14, color=SLATE_PEAK)
    ax.set_xlabel("Month (year-month)")
    ax.set_ylabel("Runs")
    ax.grid(True, axis="y", alpha=0.3)
    ax.legend(
        title="Activity type",
        loc="upper center",
        bbox_to_anchor=(0.5, -0.22),
        ncol=4,
        frameon=False,
    )
    step = max(1, len(pivot.index) // 12)
    ax.set_xticks(range(0, len(pivot.index), step))
    ax.set_xticklabels([pivot.index[i] for i in range(0, len(pivot.index), step)], rotation=45, ha="right")
    save_figure(fig, plt, FIGURES_DIR / FIG_MONTHLY)


def build_speed_satisfaction_figure(pd, plt):
    """Scatter of trail average speed vs average satisfaction."""
    leaderboard = read_gold_csv(pd, "gold_trail_leaderboard.csv")
    tiers = list(leaderboard["difficulty_tier"].dropna().unique())
    tiers.sort()

    fig, ax = plt.subplots(figsize=(10, 6))
    for position, tier in enumerate(tiers):
        subset = leaderboard[leaderboard["difficulty_tier"] == tier]
        ax.scatter(
            subset["avg_speed_kmh"],
            subset["avg_satisfaction"],
            s=26,
            alpha=0.6,
            linewidths=0,
            color=PALETTE[position % len(PALETTE)],
            label=str(tier),
        )
    ax.set_title("Trail Average Speed vs Satisfaction", fontsize=14, color=SLATE_PEAK)
    ax.set_xlabel("Average speed (km/h)")
    ax.set_ylabel("Average satisfaction (1-5)")
    ax.grid(True, alpha=0.3)
    ax.legend(
        title="Difficulty tier",
        loc="upper left",
        bbox_to_anchor=(1.01, 1.0),
        frameon=False,
        fontsize=9,
    )
    save_figure(fig, plt, FIGURES_DIR / FIG_SPEED_SAT)


def build_cohort_satisfaction_figure(pd, plt):
    """Bar chart of run-weighted satisfaction by difficulty cohort."""
    cohorts = read_gold_csv(pd, "gold_cohort_distributions.csv")
    work = cohorts[["difficulty_tier", "avg_satisfaction", "run_count"]].copy()
    work["satisfaction_weighted"] = work["avg_satisfaction"] * work["run_count"]
    grouped = work.groupby("difficulty_tier").agg(
        satisfaction_weighted=("satisfaction_weighted", "sum"),
        run_count=("run_count", "sum"),
    )
    grouped["avg_satisfaction"] = grouped["satisfaction_weighted"] / grouped["run_count"]
    grouped = grouped.sort_values("avg_satisfaction")

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(
        grouped.index,
        grouped["avg_satisfaction"],
        color=[PALETTE[i % len(PALETTE)] for i in range(len(grouped))],
        width=0.6,
    )
    for bar, value in zip(bars, grouped["avg_satisfaction"]):
        ax.text(
            bar.get_x() + bar.get_width() / 2.0,
            value + 0.002,
            "{0:.3f}".format(value),
            ha="center",
            va="bottom",
            fontsize=10,
            color=SLATE_PEAK,
        )
    ax.set_ylim(0, 5)
    ax.set_title("Average Satisfaction by Difficulty Cohort", fontsize=14, color=SLATE_PEAK)
    ax.set_xlabel("Difficulty cohort")
    ax.set_ylabel("Run-weighted average satisfaction (1-5)")
    ax.grid(True, axis="y", alpha=0.3)
    plt.setp(ax.get_xticklabels(), rotation=20, ha="right")
    save_figure(fig, plt, FIGURES_DIR / FIG_COHORT)


def build_vertical_drop_figure(pd, plt):
    """Horizontal bar chart of the top 10 trails by vertical drop."""
    leaderboard = read_gold_csv(pd, "gold_trail_leaderboard.csv")
    top = leaderboard.nlargest(10, "total_vertical_drop_m").sort_values("total_vertical_drop_m")

    fig, ax = plt.subplots(figsize=(10, 6))
    colors = [DEEP_PINE] * len(top)
    if colors:
        colors[-1] = SUNSET_AMBER
    bars = ax.barh(top["trail_name"], top["total_vertical_drop_m"], color=colors, height=0.62)
    for bar, value in zip(bars, top["total_vertical_drop_m"]):
        ax.text(
            value + 15,
            bar.get_y() + bar.get_height() / 2.0,
            "{0:,.0f} m".format(value),
            va="center",
            fontsize=9,
            color=SLATE_PEAK,
        )
    ax.set_title("Top 10 Trails by Total Vertical Drop", fontsize=14, color=SLATE_PEAK)
    ax.set_xlabel("Total vertical drop (m)")
    ax.set_ylabel("Trail")
    ax.grid(True, axis="x", alpha=0.3)
    ax.set_xlim(0, float(top["total_vertical_drop_m"].max()) * 1.15)
    save_figure(fig, plt, FIGURES_DIR / FIG_DROP_TOP)


def build_figures(pd, plt):
    """Render all four preview figures into bi/figures."""
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)
    build_monthly_runs_figure(pd, plt)
    build_speed_satisfaction_figure(pd, plt)
    build_cohort_satisfaction_figure(pd, plt)
    build_vertical_drop_figure(pd, plt)


def compute_kpis(pd):
    """Compute dashboard KPI values from the gold CSV marts."""
    trends = read_gold_csv(pd, "gold_temporal_trends.csv")
    leaderboard = read_gold_csv(pd, "gold_trail_leaderboard.csv")

    total_runs = int(trends["monthly_runs"].sum())
    weighted_satisfaction = float(
        (trends["avg_satisfaction"] * trends["monthly_runs"]).sum()
        / trends["monthly_runs"].sum()
    )
    max_drop_row = leaderboard.loc[leaderboard["total_vertical_drop_m"].idxmax()]
    max_drop = float(max_drop_row["total_vertical_drop_m"])

    trends = trends.copy()
    trends["period"] = trends["year"].astype(int) * 100 + trends["month"].astype(int)
    latest_period = int(trends["period"].max())
    latest_rows = trends[trends["period"] == latest_period]
    latest_runs = int(latest_rows["monthly_runs"].sum())
    previous_runs = float(latest_rows["prev_month_runs"].sum())
    mom_growth = (latest_runs - previous_runs) / previous_runs * 100.0
    latest_label = "{0:04d}-{1:02d}".format(latest_period // 100, latest_period % 100)

    return {
        "total_runs": total_runs,
        "weighted_satisfaction": weighted_satisfaction,
        "max_drop": max_drop,
        "max_drop_trail": str(max_drop_row["trail_name"]),
        "mom_growth": mom_growth,
        "latest_label": latest_label,
        "latest_runs": latest_runs,
        "previous_runs": previous_runs,
    }


def load_dashboard_data(pd):
    """Load the four gold CSV marts used by the interactive dashboard."""
    sources = [
        ("temporal_trends", "gold_temporal_trends.csv"),
        ("cohort_distributions", "gold_cohort_distributions.csv"),
        ("trail_leaderboard", "gold_trail_leaderboard.csv"),
        ("trail_map_nodes", "gold_arkhyz_trail_map_nodes.csv"),
    ]
    frames = {}
    for key, filename in sources:
        frames[key] = read_gold_csv(pd, filename)
    return frames


def serialize_dashboard_data(pd):
    """Serialize the dashboard marts into validated JSON record strings."""
    frames = load_dashboard_data(pd)
    serialized = {}
    for key, frame in frames.items():
        json_str = frame.to_json(orient="records", double_precision=15)
        json.loads(json_str)
        log(
            "serialized {0}: {1} rows, {2} bytes".format(
                key, len(frame.index), len(json_str.encode("utf-8"))
            )
        )
        serialized[key] = json_str
    return serialized


def build_data_script(serialized):
    """Wrap the serialized dashboard marts in a script tag for the dashboard."""
    entries = []
    for key in (
        "temporal_trends",
        "cohort_distributions",
        "trail_leaderboard",
        "trail_map_nodes",
    ):
        entries.append('"{0}": {1}'.format(key, serialized[key]))
    payload = ",".join(entries)
    return "<script>\nwindow.ARKHYZ_DATA = {{{0}}};\n</script>".format(payload)


def build_dashboard(pd, kpis):
    """Write the self-contained interactive HTML dashboard."""
    serialized = serialize_dashboard_data(pd)
    data_script = build_data_script(serialized)

    kpi_snapshot = (
        "Total runs {0:,} | Weighted avg satisfaction {1:.2f} | "
        "Max vertical drop {2:,.0f} m | MoM growth ({3}) {4:+.2f}%"
    ).format(
        kpis["total_runs"],
        kpis["weighted_satisfaction"],
        kpis["max_drop"],
        kpis["latest_label"],
        kpis["mom_growth"],
    )

    figure_blocks = "\n".join(
        [
            '<figure class="chart"><img src="figures/{0}" alt="{1}"><figcaption>{1}</figcaption></figure>'.format(
                filename, caption
            )
            for filename, caption in FIGURE_CARDS
        ]
    )

    html = HTML_TEMPLATE
    html = html.replace("__STYLE__", STYLE_BLOCK)
    html = html.replace("__KPI_SNAPSHOT__", kpi_snapshot)
    html = html.replace("__STATIC_FIGURES__", figure_blocks)
    html = html.replace("__PERIOD__", kpis["latest_label"])
    html = html.replace("__DATA_SCRIPT__", data_script)

    DASHBOARD_PATH.write_text(html, encoding="utf-8")
    log("wrote {0} ({1:,} bytes)".format(DASHBOARD_PATH.name, DASHBOARD_PATH.stat().st_size))


def check_environment(pd, plt, matplotlib):
    """Verify dependencies and the gold CSV inputs, then report status."""
    log("checking stage 4 environment")
    if pd is None:
        log("FAIL: pandas is not importable")
        return 1
    if plt is None or matplotlib is None:
        log("FAIL: matplotlib is not importable")
        return 1
    log("pandas {0} ok".format(pd.__version__))
    log("matplotlib {0} ok (backend=Agg)".format(matplotlib.__version__))
    if not GOLD_CSV_DIR.is_dir():
        log("FAIL: gold CSV directory missing: {0}".format(GOLD_CSV_DIR))
        return 1
    log("gold CSV directory: {0}".format(GOLD_CSV_DIR))
    missing = []
    for name in EXPECTED_CSVS:
        path = GOLD_CSV_DIR / name
        if path.is_file():
            log("found {0} ({1:,} bytes)".format(name, path.stat().st_size))
        else:
            missing.append(name)
    if missing:
        log("FAIL: missing gold CSVs: {0}".format(", ".join(missing)))
        return 1
    log("environment check passed")
    return 0


def parse_args():
    """Parse command line flags."""
    parser = argparse.ArgumentParser(
        description="Stage 4 BI preview generator: figures and HTML dashboard from gold CSV marts."
    )
    parser.add_argument(
        "--check-env",
        action="store_true",
        help="verify pandas/matplotlib imports and the gold CSV directory, then exit",
    )
    parser.add_argument(
        "--html-only",
        action="store_true",
        help="skip figure generation and rebuild only the HTML dashboard",
    )
    return parser.parse_args()


def main():
    """Entry point."""
    args = parse_args()
    pd = load_pandas()

    if args.check_env:
        plt, matplotlib = (None, None)
        if pd is not None:
            plt, matplotlib = load_pyplot()
        return check_environment(pd, plt, matplotlib)

    if pd is None:
        log("FAIL: pandas is not importable")
        return 1

    log("generating BI preview from {0}".format(GOLD_CSV_DIR))
    BI_DIR.mkdir(parents=True, exist_ok=True)

    if args.html_only:
        log("--html-only: skipping figure generation")
    else:
        plt, matplotlib = load_pyplot()
        if plt is None:
            log("FAIL: matplotlib is required for figure generation")
            return 1
        build_figures(pd, plt)

    kpis = compute_kpis(pd)
    log(
        "KPIs: runs={0:,} satisfaction={1:.2f} max_drop={2:,.0f} m mom={3:+.2f}% ({4})".format(
            kpis["total_runs"],
            kpis["weighted_satisfaction"],
            kpis["max_drop"],
            kpis["mom_growth"],
            kpis["latest_label"],
        )
    )
    build_dashboard(pd, kpis)
    log("done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
