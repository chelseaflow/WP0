"""
AgriFlow WP0 — Collector: Distance Matrix (REAL ROAD DISTANCES)
=================================================================
Strategy (ordered by accuracy):
  1. OSRM Public Demo Server (router.project-osrm.org) — real road data from OpenStreetMap
  2. OSRM Self-hosted — same data, no rate limit
  3. Haversine × 1.3 — fallback if no internet / OSRM down

Why Haversine alone is NOT enough:
  - Surabaya → Bangkalan: Haversine ~15 km, real road ~40 km (must cross Suramadu bridge)
  - Mountainous areas (Malang highlands): roads wind significantly
  - Coastal routes don't follow straight lines

OSRM Public Demo:
  - URL: router.project-osrm.org
  - Limit: 10,000 elements per query (we need 38×38 = 1,444 — safe)
  - Rate: max 1 request/second
  - Data: OpenStreetMap road network (worldwide, updated regularly)

Usage:
    python collectors/distance_collector.py                     # auto (tries OSRM public first)
    python collectors/distance_collector.py --method haversine  # fallback only
    python collectors/distance_collector.py --method osrm --osrm http://localhost:5000  # self-hosted
"""

import os, sys, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import pandas as pd
from typing import Optional

from config.settings import (
    JATIM_KABUPATEN, ROAD_FACTOR, DATA_PROCESSED_DIR
)
from utils.helpers import setup_logger, ensure_dir, haversine_km

logger = setup_logger("distance_collector")

# OSRM Public demo server (maintained by FOSSGIS, uses OSM data)
OSRM_PUBLIC = "https://router.project-osrm.org"


def build_osrm_matrix(osrm_url=OSRM_PUBLIC):
    """
    Build distance matrix using OSRM Table API.
    38 locations = 1,444 elements — well within the 10,000 limit.
    Returns distance (meters) and duration (seconds) for all pairs.
    """
    # Build coordinate string: lon1,lat1;lon2,lat2;...
    coords = ";".join(f"{k['lon']},{k['lat']}" for k in JATIM_KABUPATEN)

    url = f"{osrm_url}/table/v1/driving/{coords}"
    params = {"annotations": "distance,duration"}

    logger.info(f"Requesting OSRM table from {osrm_url} for {len(JATIM_KABUPATEN)} locations...")

    try:
        resp = requests.get(url, params=params, timeout=120)
        resp.raise_for_status()
        result = resp.json()

        if result.get("code") != "Ok":
            logger.error(f"OSRM error: {result.get('code')} — {result.get('message', '')}")
            return None

        distances = result.get("distances", [])  # meters
        durations = result.get("durations", [])   # seconds

        if not distances or not durations:
            logger.error("OSRM returned empty distance/duration arrays")
            return None

        records = []
        for i, a in enumerate(JATIM_KABUPATEN):
            for j, b in enumerate(JATIM_KABUPATEN):
                dist_m = distances[i][j]
                dur_s = durations[i][j]
                records.append({
                    "from_kode": a["kode_bps"], "from_nama": a["nama"],
                    "from_lat": a["lat"], "from_lon": a["lon"],
                    "to_kode": b["kode_bps"], "to_nama": b["nama"],
                    "to_lat": b["lat"], "to_lon": b["lon"],
                    "straight_km": round(haversine_km(a["lat"], a["lon"], b["lat"], b["lon"]), 2),
                    "road_km": round(dist_m / 1000, 2) if dist_m is not None else None,
                    "travel_minutes": round(dur_s / 60, 1) if dur_s is not None else None,
                    "method": "osrm",
                })

        df = pd.DataFrame(records)

        # Sanity check: compare a known pair
        sby_bangkalan = df[(df["from_kode"] == "3578") & (df["to_kode"] == "3526")]
        if not sby_bangkalan.empty:
            row = sby_bangkalan.iloc[0]
            logger.info(f"Sanity check — Surabaya→Bangkalan: "
                       f"straight={row['straight_km']}km, road={row['road_km']}km, "
                       f"time={row['travel_minutes']}min")

        logger.info(f"Built OSRM matrix: {len(df)} pairs, "
                   f"max road distance: {df['road_km'].max():.1f} km")
        return df

    except requests.exceptions.RequestException as e:
        logger.error(f"OSRM request failed: {e}")
        return None


def build_haversine_matrix(road_factor=ROAD_FACTOR):
    """Fallback: Haversine × road_factor. Quick but approximate."""
    logger.warning(f"Using Haversine × {road_factor} approximation (less accurate)")
    records = []
    for a in JATIM_KABUPATEN:
        for b in JATIM_KABUPATEN:
            straight = haversine_km(a["lat"], a["lon"], b["lat"], b["lon"])
            road = straight * road_factor
            records.append({
                "from_kode": a["kode_bps"], "from_nama": a["nama"],
                "from_lat": a["lat"], "from_lon": a["lon"],
                "to_kode": b["kode_bps"], "to_nama": b["nama"],
                "to_lat": b["lat"], "to_lon": b["lon"],
                "straight_km": round(straight, 2),
                "road_km": round(road, 2),
                "travel_minutes": round(road / 50.0 * 60, 1),  # assume 50 km/h avg
                "method": "haversine",
            })
    df = pd.DataFrame(records)
    logger.info(f"Built Haversine matrix: {len(df)} pairs, max {df['road_km'].max():.1f} km")
    return df


def get_nearest_kabupaten(kode_bps, distance_df, n=5):
    """Get N nearest kabupaten to a given kabupaten."""
    f = distance_df[(distance_df["from_kode"] == kode_bps) & (distance_df["to_kode"] != kode_bps)]
    return f.nsmallest(n, "road_km")


def collect_distance_data(method="auto", osrm_url=None):
    """
    Build and save distance matrix.

    method: 'auto' (try OSRM public first, fallback to haversine)
            'osrm' (use specific OSRM server)
            'haversine' (only haversine)
    """
    out_dir = os.path.join(DATA_PROCESSED_DIR, "distance")
    ensure_dir(out_dir)

    df = None

    if method == "auto":
        # Try OSRM public server first
        logger.info("Trying OSRM public server (router.project-osrm.org)...")
        df = build_osrm_matrix(OSRM_PUBLIC)
        if df is None:
            logger.info("OSRM public failed. Falling back to Haversine.")
            df = build_haversine_matrix()
    elif method == "osrm":
        url = osrm_url or OSRM_PUBLIC
        df = build_osrm_matrix(url)
        if df is None:
            df = build_haversine_matrix()
    else:
        df = build_haversine_matrix()

    # Save full matrix
    out_path = os.path.join(out_dir, "distance_matrix_jatim.csv")
    df.to_csv(out_path, index=False)
    logger.info(f"Saved: {out_path} ({len(df)} rows)")

    # Save pivot table for quick lookup
    pivot = df.pivot_table(index="from_kode", columns="to_kode",
                           values="road_km", aggfunc="first")
    pivot.to_csv(os.path.join(out_dir, "distance_pivot_km.csv"))

    return df


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Build distance matrix for Jatim kabupaten")
    parser.add_argument("--method", choices=["auto", "osrm", "haversine"], default="auto")
    parser.add_argument("--osrm", type=str, default=None, help="Custom OSRM server URL")
    args = parser.parse_args()

    df = collect_distance_data(method=args.method, osrm_url=args.osrm)

    print(f"\nDistance matrix: {len(df)} pairs | Method: {df['method'].iloc[0]}")
    print(f"Max road distance: {df['road_km'].max():.1f} km")

    # Show comparison for validation
    print(f"\nNearest to Kota Surabaya:")
    print(get_nearest_kabupaten("3578", df, 5)[
        ["to_nama", "straight_km", "road_km", "travel_minutes"]
    ].to_string(index=False))

    # Show Surabaya → Bangkalan (good test: must use Suramadu bridge)
    sb = df[(df["from_kode"] == "3578") & (df["to_kode"] == "3526")]
    if not sb.empty:
        r = sb.iloc[0]
        print(f"\nValidation — Surabaya → Bangkalan (Madura):")
        print(f"  Straight line: {r['straight_km']} km")
        print(f"  Road distance: {r['road_km']} km")
        print(f"  Travel time:   {r['travel_minutes']} min")