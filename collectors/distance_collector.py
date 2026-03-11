"""
AgriFlow WP0 — Collector: Distance Matrix (Antar Kabupaten Jatim)
===================================================================
MVP: Haversine × 1.3 | Upgrade: OSRM self-hosted

Usage:
    python collectors/distance_collector.py
    python collectors/distance_collector.py --method osrm --osrm http://localhost:5000
"""

import os
import sys

# --- Path fix ---
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import pandas as pd
from typing import Optional

from config.settings import (
    JATIM_KABUPATEN, ROAD_FACTOR, MAX_DISTANCE_KM,
    DATA_RAW_DIR, DATA_PROCESSED_DIR
)
from utils.helpers import setup_logger, ensure_dir, save_json, haversine_km

logger = setup_logger("distance_collector")


def build_haversine_matrix(road_factor=ROAD_FACTOR):
    records = []
    for kab_a in JATIM_KABUPATEN:
        for kab_b in JATIM_KABUPATEN:
            straight = haversine_km(kab_a["lat"], kab_a["lon"], kab_b["lat"], kab_b["lon"])
            road = straight * road_factor
            records.append({
                "from_kode": kab_a["kode_bps"], "from_nama": kab_a["nama"],
                "from_lat": kab_a["lat"], "from_lon": kab_a["lon"],
                "to_kode": kab_b["kode_bps"], "to_nama": kab_b["nama"],
                "to_lat": kab_b["lat"], "to_lon": kab_b["lon"],
                "straight_km": round(straight, 2), "road_km": round(road, 2),
                "travel_minutes": round(road / 50.0 * 60, 1),
                "method": "haversine",
            })
    df = pd.DataFrame(records)
    logger.info(f"Built Haversine matrix: {len(df)} pairs, max {df['road_km'].max():.1f} km")
    return df


def build_osrm_matrix(osrm_base_url="http://localhost:5000"):
    try:
        test_url = f"{osrm_base_url}/route/v1/driving/112.75,-7.25;112.76,-7.26"
        requests.get(test_url, timeout=5)
    except Exception as e:
        logger.error(f"OSRM not reachable at {osrm_base_url}: {e}")
        logger.info("Falling back to Haversine")
        return build_haversine_matrix()

    coords = ";".join(f"{k['lon']},{k['lat']}" for k in JATIM_KABUPATEN)
    url = f"{osrm_base_url}/table/v1/driving/{coords}"
    try:
        resp = requests.get(url, params={"annotations": "distance,duration"}, timeout=120)
        result = resp.json()
        if result.get("code") != "Ok":
            return build_haversine_matrix()
        distances = result.get("distances", [])
        durations = result.get("durations", [])
        records = []
        for i, a in enumerate(JATIM_KABUPATEN):
            for j, b in enumerate(JATIM_KABUPATEN):
                records.append({
                    "from_kode": a["kode_bps"], "from_nama": a["nama"],
                    "from_lat": a["lat"], "from_lon": a["lon"],
                    "to_kode": b["kode_bps"], "to_nama": b["nama"],
                    "to_lat": b["lat"], "to_lon": b["lon"],
                    "straight_km": round(haversine_km(a["lat"], a["lon"], b["lat"], b["lon"]), 2),
                    "road_km": round(distances[i][j] / 1000, 2) if distances else None,
                    "travel_minutes": round(durations[i][j] / 60, 1) if durations else None,
                    "method": "osrm",
                })
        return pd.DataFrame(records)
    except Exception as e:
        logger.error(f"OSRM failed: {e}")
        return build_haversine_matrix()


def get_nearest_kabupaten(kode_bps, distance_df, n=5):
    f = distance_df[(distance_df["from_kode"] == kode_bps) & (distance_df["to_kode"] != kode_bps)]
    return f.nsmallest(n, "road_km")


def collect_distance_data(method="haversine", osrm_url=None):
    out_dir = os.path.join(DATA_PROCESSED_DIR, "distance")
    ensure_dir(out_dir)
    df = build_osrm_matrix(osrm_url) if method == "osrm" and osrm_url else build_haversine_matrix()

    df.to_csv(os.path.join(out_dir, "distance_matrix_jatim.csv"), index=False)
    pivot = df.pivot_table(index="from_kode", columns="to_kode", values="road_km", aggfunc="first")
    pivot.to_csv(os.path.join(out_dir, "distance_pivot_km.csv"))
    logger.info(f"Saved distance matrix ({len(df)} rows)")
    return df


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Build distance matrix")
    parser.add_argument("--method", choices=["haversine", "osrm"], default="haversine")
    parser.add_argument("--osrm", type=str, default=None)
    args = parser.parse_args()

    df = collect_distance_data(method=args.method, osrm_url=args.osrm)
    print(f"\nDistance matrix: {len(df)} pairs, max {df['road_km'].max():.1f} km")
    print(f"\nNearest to Surabaya:")
    print(get_nearest_kabupaten("3578", df, 5)[["to_nama", "road_km", "travel_minutes"]].to_string(index=False))
