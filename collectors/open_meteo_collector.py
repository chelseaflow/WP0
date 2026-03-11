"""
AgriFlow WP0 — Collector: Open-Meteo Weather Data
====================================================
Fetches daily weather data for all 38 kabupaten/kota Jawa Timur.
No API key needed. REST API, returns JSON.

Usage:
    python -m collectors.open_meteo_collector
    # or from WP0 root:
    python collectors/open_meteo_collector.py
"""

import os
import sys
import time
import json

# --- Path fix: ensure WP0 root is in sys.path ---
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional

from config.settings import (
    JATIM_KABUPATEN, OPEN_METEO_BASE_URL, OPEN_METEO_DAILY_PARAMS,
    OPEN_METEO_TIMEZONE, DATA_RAW_DIR, DATA_PROCESSED_DIR
)
from utils.helpers import setup_logger, ensure_dir, save_json, save_checkpoint, load_checkpoint

logger = setup_logger("open_meteo")


def fetch_weather_historical(lat, lon, start_date, end_date, daily_params=None):
    params = daily_params or OPEN_METEO_DAILY_PARAMS
    url = f"{OPEN_METEO_BASE_URL}/archive"
    query = {
        "latitude": lat, "longitude": lon,
        "start_date": start_date, "end_date": end_date,
        "daily": ",".join(params), "timezone": OPEN_METEO_TIMEZONE,
    }
    try:
        resp = requests.get(url, params=query, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch weather for ({lat}, {lon}): {e}")
        return None


def fetch_weather_forecast(lat, lon, forecast_days=7, daily_params=None):
    params = daily_params or OPEN_METEO_DAILY_PARAMS
    url = f"{OPEN_METEO_BASE_URL}/forecast"
    query = {
        "latitude": lat, "longitude": lon,
        "daily": ",".join(params), "timezone": OPEN_METEO_TIMEZONE,
        "forecast_days": forecast_days,
    }
    try:
        resp = requests.get(url, params=query, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch forecast for ({lat}, {lon}): {e}")
        return None


def _parse_weather_response(data, kode, nama, source):
    if not data or "daily" not in data:
        return []
    daily = data["daily"]
    dates = daily.get("time", [])
    records = []
    for i, date in enumerate(dates):
        record = {"kode_bps": kode, "nama_kabupaten": nama, "date": date, "source": source}
        for param in OPEN_METEO_DAILY_PARAMS:
            values = daily.get(param, [])
            record[param] = values[i] if i < len(values) else None
        records.append(record)
    return records


def collect_all_jatim_weather(start_date=None, end_date=None,
                              include_forecast=True, delay_seconds=0.5):
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")
    if start_date is None:
        start_date = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")

    raw_dir = os.path.join(DATA_RAW_DIR, "weather")
    ensure_dir(raw_dir)

    checkpoint = load_checkpoint("open_meteo", raw_dir)
    completed_codes = set(checkpoint.get("completed", [])) if checkpoint else set()
    if completed_codes:
        logger.info(f"Resuming: {len(completed_codes)} already done")

    all_records = []
    total = len(JATIM_KABUPATEN)

    for idx, kab in enumerate(JATIM_KABUPATEN):
        kode, nama = kab["kode_bps"], kab["nama"]

        if kode in completed_codes:
            logger.info(f"[{idx+1}/{total}] SKIP {nama} (checkpoint)")
            raw_file = os.path.join(raw_dir, f"weather_{kode}.json")
            if os.path.exists(raw_file):
                with open(raw_file) as f:
                    cached = json.load(f)
                all_records.extend(_parse_weather_response(cached.get("historical"), kode, nama, "historical"))
                if cached.get("forecast"):
                    all_records.extend(_parse_weather_response(cached["forecast"], kode, nama, "forecast"))
            continue

        logger.info(f"[{idx+1}/{total}] Fetching weather for {nama}...")

        hist_data = fetch_weather_historical(kab["lat"], kab["lon"], start_date, end_date)
        time.sleep(delay_seconds)

        fc_data = None
        if include_forecast:
            fc_data = fetch_weather_forecast(kab["lat"], kab["lon"])
            time.sleep(delay_seconds)

        raw_payload = {
            "kode_bps": kode, "nama": nama,
            "lat": kab["lat"], "lon": kab["lon"],
            "historical": hist_data, "forecast": fc_data,
            "fetched_at": datetime.now().isoformat(),
        }
        save_json(raw_payload, os.path.join(raw_dir, f"weather_{kode}.json"))

        if hist_data:
            all_records.extend(_parse_weather_response(hist_data, kode, nama, "historical"))
        if fc_data:
            all_records.extend(_parse_weather_response(fc_data, kode, nama, "forecast"))

        completed_codes.add(kode)
        save_checkpoint("open_meteo", {"completed": list(completed_codes)}, raw_dir)

    if not all_records:
        logger.warning("No weather data collected!")
        return pd.DataFrame()

    df = pd.DataFrame(all_records)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["kode_bps", "date"]).reset_index(drop=True)
    df = df.drop_duplicates(subset=["kode_bps", "date"], keep="first")

    out_dir = os.path.join(DATA_PROCESSED_DIR, "weather")
    ensure_dir(out_dir)
    out_path = os.path.join(out_dir, "weather_jatim_combined.csv")
    df.to_csv(out_path, index=False)
    logger.info(f"Saved: {out_path} ({len(df)} rows)")
    return df


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Collect Open-Meteo weather data for Jatim")
    parser.add_argument("--start", type=str, default=None)
    parser.add_argument("--end", type=str, default=None)
    parser.add_argument("--no-forecast", action="store_true")
    parser.add_argument("--delay", type=float, default=0.5)
    args = parser.parse_args()

    df = collect_all_jatim_weather(
        start_date=args.start, end_date=args.end,
        include_forecast=not args.no_forecast, delay_seconds=args.delay,
    )
    if not df.empty:
        print(f"\nWeather data: {len(df)} rows, {df['kode_bps'].nunique()} kabupaten")
        print(f"Date range: {df['date'].min()} to {df['date'].max()}")
