"""
AgriFlow WP0 — Collector: Open-Meteo Weather Data (FIXED)
============================================================
TWO strategies:
  1. Forecast API with past_days=92 → ~3 months historical + 7 days forecast in ONE call
  2. Archive API at archive-api.open-meteo.com → older data (separate subdomain!)

Key fixes from v1:
  - Archive API is at archive-api.open-meteo.com NOT api.open-meteo.com
  - Forecast API's daily params differ from archive (no temperature_2m_mean, use max/min)
  - past_days approach is simpler and more reliable for recent data
"""

import os, sys, time, json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional

from config.settings import (
    JATIM_KABUPATEN, OPEN_METEO_TIMEZONE, DATA_RAW_DIR, DATA_PROCESSED_DIR
)
from utils.helpers import setup_logger, ensure_dir, save_json, save_checkpoint, load_checkpoint

logger = setup_logger("open_meteo")

FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"


def fetch_weather_combined(lat, lon, past_days=92, forecast_days=7):
    """Use Forecast API with past_days. Most reliable for recent data."""
    daily_params = [
        "temperature_2m_max", "temperature_2m_min",
        "precipitation_sum", "wind_speed_10m_max",
    ]
    query = {
        "latitude": lat, "longitude": lon,
        "daily": ",".join(daily_params),
        "timezone": OPEN_METEO_TIMEZONE,
        "past_days": past_days, "forecast_days": forecast_days,
    }
    try:
        resp = requests.get(FORECAST_URL, params=query, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if data.get("error"):
            logger.error(f"API error: {data.get('reason')}")
            return None
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"Forecast API failed ({lat}, {lon}): {e}")
        return None


def fetch_weather_archive(lat, lon, start_date, end_date):
    """Use Archive API (archive-api.open-meteo.com) for older data."""
    daily_params = [
        "temperature_2m_mean", "precipitation_sum",
        "relative_humidity_2m_mean", "wind_speed_10m_max",
    ]
    query = {
        "latitude": lat, "longitude": lon,
        "start_date": start_date, "end_date": end_date,
        "daily": ",".join(daily_params),
        "timezone": OPEN_METEO_TIMEZONE,
    }
    try:
        resp = requests.get(ARCHIVE_URL, params=query, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        if data.get("error"):
            logger.error(f"Archive error: {data.get('reason')}")
            return None
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"Archive API failed ({lat}, {lon}): {e}")
        return None


def _parse_weather_response(data, kode, nama, source):
    if not data or "daily" not in data:
        return []
    daily = data["daily"]
    dates = daily.get("time", [])
    records = []
    for i, date in enumerate(dates):
        rec = {"kode_bps": kode, "nama_kabupaten": nama, "date": date, "source": source}
        for key in daily:
            if key == "time":
                continue
            vals = daily[key]
            rec[key] = vals[i] if i < len(vals) else None
        # Calculate mean from max/min if not available
        if "temperature_2m_mean" not in rec:
            tmax = rec.get("temperature_2m_max")
            tmin = rec.get("temperature_2m_min")
            if tmax is not None and tmin is not None:
                rec["temperature_2m_mean"] = round((tmax + tmin) / 2, 1)
        records.append(rec)
    return records


def collect_all_jatim_weather(start_date=None, end_date=None,
                              include_forecast=True, delay_seconds=0.3):
    raw_dir = os.path.join(DATA_RAW_DIR, "weather")
    ensure_dir(raw_dir)

    checkpoint = load_checkpoint("open_meteo", raw_dir)
    completed_codes = set(checkpoint.get("completed", [])) if checkpoint else set()
    if completed_codes:
        logger.info(f"Resuming: {len(completed_codes)} done")

    use_archive = False
    past_days = 92
    if start_date and end_date:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        days_ago = (datetime.now() - start_dt).days
        if days_ago > 92:
            use_archive = True
            # Archive data is delayed ~5 days, cap end_date
            safe_end = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
            if end_date > safe_end:
                end_date = safe_end
            logger.info(f"Using Archive API: {start_date} to {end_date}")
        else:
            past_days = min(days_ago, 92)
            logger.info(f"Using Forecast API with past_days={past_days}")

    all_records = []
    total = len(JATIM_KABUPATEN)

    for idx, kab in enumerate(JATIM_KABUPATEN):
        kode, nama = kab["kode_bps"], kab["nama"]

        if kode in completed_codes:
            raw_file = os.path.join(raw_dir, f"weather_{kode}.json")
            if os.path.exists(raw_file):
                with open(raw_file) as f:
                    cached = json.load(f)
                all_records.extend(
                    _parse_weather_response(cached.get("data"), kode, nama, "cached"))
            continue

        logger.info(f"[{idx+1}/{total}] {nama}...")

        if use_archive:
            data = fetch_weather_archive(kab["lat"], kab["lon"], start_date, end_date)
            source = "archive"
        else:
            data = fetch_weather_combined(kab["lat"], kab["lon"], past_days=past_days)
            source = "forecast+past"

        save_json({"kode_bps": kode, "nama": nama, "lat": kab["lat"], "lon": kab["lon"],
                   "data": data, "source": source, "fetched_at": datetime.now().isoformat()},
                  os.path.join(raw_dir, f"weather_{kode}.json"))

        if data:
            all_records.extend(_parse_weather_response(data, kode, nama, source))

        completed_codes.add(kode)
        save_checkpoint("open_meteo", {"completed": list(completed_codes)}, raw_dir)
        time.sleep(delay_seconds)

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
    logger.info(f"Saved: {out_path} ({len(df)} rows, {df['kode_bps'].nunique()} kabupaten)")
    return df


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", type=str, default=None)
    parser.add_argument("--end", type=str, default=None)
    parser.add_argument("--no-forecast", action="store_true")
    parser.add_argument("--delay", type=float, default=0.3)
    args = parser.parse_args()
    df = collect_all_jatim_weather(args.start, args.end, not args.no_forecast, args.delay)
    if not df.empty:
        print(f"\n{len(df)} rows, {df['kode_bps'].nunique()} kabupaten, "
              f"{df['date'].min()} to {df['date'].max()}")