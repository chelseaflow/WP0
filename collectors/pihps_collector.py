"""
AgriFlow WP0 — Collector: PIHPS Bank Indonesia (Harga Pangan)
================================================================
3 approaches: API discovery → HTML scraping → manual CSV upload + sample data generator.

Usage:
    python collectors/pihps_collector.py --sample
    python collectors/pihps_collector.py --approach manual --file data.csv
"""

import os
import sys
import time
import json
import re

# --- Path fix ---
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from bs4 import BeautifulSoup

from config.settings import (
    JATIM_KABUPATEN, PIHPS_COMMODITIES, PIHPS_BASE_URL,
    PIHPS_JATIM_CITIES, DATA_RAW_DIR, DATA_PROCESSED_DIR
)
from utils.helpers import setup_logger, ensure_dir, save_json, clean_price_string, validate_price

logger = setup_logger("pihps_collector")

PIHPS_PROVINSI_JATIM = "15"


def discover_pihps_api(session=None):
    if session is None:
        session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept-Language": "id-ID,id;q=0.9,en-US;q=0.8",
    })
    try:
        resp = session.get(PIHPS_BASE_URL, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        ajax_urls = []
        for script in soup.find_all("script"):
            if script.string:
                urls = re.findall(r'["\'](/hargapangan/[^"\']+)["\']', script.string)
                ajax_urls.extend(urls)
                urls = re.findall(r'url\s*:\s*["\']([^"\']+)["\']', script.string)
                ajax_urls.extend(urls)
        if ajax_urls:
            logger.info(f"Discovered {len(set(ajax_urls))} potential AJAX endpoints")
            return list(set(ajax_urls))
        logger.warning("No AJAX endpoints found")
        return None
    except Exception as e:
        logger.error(f"API discovery failed: {e}")
        return None


def fetch_pihps_api(session, provinsi=PIHPS_PROVINSI_JATIM,
                    start_date=None, end_date=None):
    if start_date is None:
        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")
    endpoints = [
        f"{PIHPS_BASE_URL}/TabelHarga/PasarTradisionalData",
        f"{PIHPS_BASE_URL}/TabelHarga/GetData",
    ]
    for endpoint in endpoints:
        try:
            logger.info(f"Trying: {endpoint}")
            payload = {"provinsi": provinsi, "tanggalAwal": start_date,
                       "tanggalAkhir": end_date, "jenisPasar": "1"}
            resp = session.post(endpoint, data=payload, headers={
                "X-Requested-With": "XMLHttpRequest",
            }, timeout=30)
            if resp.status_code == 200:
                try:
                    data = resp.json()
                    if data:
                        return _parse_pihps_json(data)
                except json.JSONDecodeError:
                    if "<table" in resp.text.lower():
                        return _parse_pihps_html(resp.text)
        except Exception as e:
            logger.debug(f"Endpoint failed: {e}")
    return None


def scrape_pihps_html(start_date=None, end_date=None):
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    try:
        resp = session.get(f"{PIHPS_BASE_URL}/TabelHarga/PasarTradisional", timeout=30)
        resp.raise_for_status()
        return _parse_pihps_html(resp.text)
    except Exception as e:
        logger.error(f"HTML scraping failed: {e}")
        return None


def _parse_pihps_html(html):
    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    if not tables:
        return None
    best_table = max(tables, key=lambda t: len(t.find_all("tr")))
    rows = best_table.find_all("tr")
    if len(rows) < 2:
        return None
    headers = [th.get_text(strip=True) for th in rows[0].find_all(["th", "td"])]
    data = [[td.get_text(strip=True) for td in row.find_all(["th", "td"])] for row in rows[1:]]
    data = [d for d in data if d]
    if data:
        return pd.DataFrame(data, columns=headers[:len(data[0])] if headers else None)
    return None


def _parse_pihps_json(data):
    if isinstance(data, list):
        return pd.DataFrame(data)
    elif isinstance(data, dict):
        for key in ["data", "result", "items", "rows"]:
            if key in data and isinstance(data[key], list):
                return pd.DataFrame(data[key])
    return None


def process_manual_pihps(filepath):
    logger.info(f"Processing manual file: {filepath}")
    ext = os.path.splitext(filepath)[1].lower()
    if ext == ".csv":
        for enc in ["utf-8", "latin-1", "cp1252"]:
            try:
                df = pd.read_csv(filepath, encoding=enc)
                break
            except UnicodeDecodeError:
                continue
        else:
            raise ValueError(f"Cannot read CSV: {filepath}")
    elif ext in [".xlsx", ".xls"]:
        df = pd.read_excel(filepath)
    else:
        raise ValueError(f"Unsupported format: {ext}")
    logger.info(f"Raw: {len(df)} rows, columns: {list(df.columns)}")
    return _normalize_pihps_dataframe(df)


def _normalize_pihps_dataframe(df):
    df.columns = [str(c).strip().lower() for c in df.columns]
    commodity_keywords = ["beras", "cabai", "bawang", "daging", "telur", "gula", "minyak"]
    commodity_cols = [c for c in df.columns if any(k in c for k in commodity_keywords)]

    if commodity_cols:
        date_col = _find_column(df.columns, ["tanggal", "date", "tgl", "periode"])
        city_col = _find_column(df.columns, ["kota", "kabupaten", "city", "wilayah"])
        id_vars = [c for c in [date_col, city_col] if c is not None] or list(df.columns[:2])
        df_long = df.melt(id_vars=id_vars, value_vars=commodity_cols,
                          var_name="komoditas", value_name="harga_raw")
        col_map = {}
        if date_col: col_map[date_col] = "tanggal"
        if city_col: col_map[city_col] = "kota"
        df = df_long.rename(columns=col_map)
    else:
        rename_map = {}
        for col in df.columns:
            if col in ["tanggal", "date", "tgl"]: rename_map[col] = "tanggal"
            elif col in ["kota", "kabupaten", "city", "wilayah"]: rename_map[col] = "kota"
            elif col in ["komoditas", "commodity"]: rename_map[col] = "komoditas"
            elif col in ["harga", "price"]: rename_map[col] = "harga_raw"
        df = df.rename(columns=rename_map)

    if "harga_raw" in df.columns:
        df["harga"] = df["harga_raw"].astype(str).apply(clean_price_string)
    elif "harga" in df.columns:
        df["harga"] = df["harga"].astype(str).apply(clean_price_string)

    if "tanggal" in df.columns:
        df["tanggal"] = pd.to_datetime(df["tanggal"], errors="coerce", dayfirst=True)

    if "harga" in df.columns:
        df = df[df["harga"].notna() & (df["harga"] > 0)]

    df["provinsi"] = "Jawa Timur"
    df["source"] = "PIHPS_BI"
    df["processed_at"] = datetime.now().isoformat()
    return df


def _find_column(columns, keywords):
    for col in columns:
        for kw in keywords:
            if kw in str(col).lower():
                return col
    return None


def generate_sample_pihps_data(n_days=90, cities=None, commodities=None):
    import numpy as np
    if cities is None:
        cities = PIHPS_JATIM_CITIES
    if commodities is None:
        commodities = list(PIHPS_COMMODITIES.keys())[:9]

    base_prices = {
        "beras_premium": 15_500, "beras_medium": 13_200,
        "cabai_merah_besar": 55_000, "cabai_merah_keriting": 50_000,
        "cabai_rawit_merah": 65_000, "cabai_rawit_hijau": 45_000,
        "bawang_merah": 42_000, "bawang_putih": 40_000,
        "daging_ayam_ras": 35_000, "telur_ayam_ras": 28_000,
        "daging_sapi_murni": 135_000, "gula_pasir_lokal": 18_500,
        "minyak_goreng_curah": 17_000,
    }
    volatility = {
        "beras_premium": 0.02, "beras_medium": 0.02,
        "cabai_merah_besar": 0.15, "cabai_merah_keriting": 0.15,
        "cabai_rawit_merah": 0.18, "cabai_rawit_hijau": 0.14,
        "bawang_merah": 0.12, "bawang_putih": 0.08,
        "daging_ayam_ras": 0.05, "telur_ayam_ras": 0.04,
        "daging_sapi_murni": 0.03, "gula_pasir_lokal": 0.02,
        "minyak_goreng_curah": 0.03,
    }
    city_premium = {
        "Surabaya": 1.0, "Malang": 0.95, "Kediri": 0.92, "Jember": 0.93,
        "Madiun": 0.91, "Probolinggo": 0.94, "Mojokerto": 0.93,
        "Blitar": 0.90, "Pasuruan": 0.94, "Batu": 0.96,
    }

    np.random.seed(42)
    records = []
    end_date = datetime.now()

    for day_offset in range(n_days):
        date = end_date - timedelta(days=n_days - 1 - day_offset)
        for commodity in commodities:
            bp = base_prices.get(commodity, 30_000)
            vol = volatility.get(commodity, 0.05)
            seasonal = 1 + 0.1 * np.sin(2 * np.pi * day_offset / 30)
            trend = 1 + 0.001 * day_offset
            for city in cities:
                premium = city_premium.get(city, 0.95)
                noise = np.random.normal(0, vol)
                price = bp * seasonal * trend * premium * (1 + noise)
                price = round(max(price, bp * 0.5), -2)
                records.append({
                    "tanggal": date.strftime("%Y-%m-%d"),
                    "kota": city, "komoditas": commodity,
                    "nama_komoditas": PIHPS_COMMODITIES.get(commodity, {}).get("nama", commodity),
                    "harga": price, "satuan": "Rp/kg",
                    "jenis_pasar": "tradisional", "provinsi": "Jawa Timur",
                    "source": "SAMPLE_DATA",
                })

    df = pd.DataFrame(records)
    df["tanggal"] = pd.to_datetime(df["tanggal"])
    logger.info(f"Generated sample: {len(df)} records, {len(cities)} cities, "
                f"{len(commodities)} commodities, {n_days} days")
    return df


def collect_pihps_data(approach="auto", manual_file=None,
                       start_date=None, end_date=None, use_sample=False):
    out_dir = os.path.join(DATA_PROCESSED_DIR, "pihps")
    ensure_dir(out_dir)
    df = None

    if use_sample or approach == "sample":
        df = generate_sample_pihps_data()
    elif approach == "manual" and manual_file:
        df = process_manual_pihps(manual_file)
    elif approach in ["api", "auto"]:
        session = requests.Session()
        discover_pihps_api(session)
        df = fetch_pihps_api(session, start_date=start_date, end_date=end_date)
        if df is None and approach == "auto":
            df = scrape_pihps_html(start_date, end_date)
        if df is None and approach == "auto":
            logger.warning("All automated approaches failed. Generating sample data.")
            df = generate_sample_pihps_data()
    elif approach == "scrape":
        df = scrape_pihps_html(start_date, end_date)

    if df is not None and not df.empty:
        out_path = os.path.join(out_dir, "harga_pangan_jatim.csv")
        df.to_csv(out_path, index=False)
        logger.info(f"Saved: {out_path} ({len(df)} rows)")
    else:
        logger.error("No PIHPS data collected!")
    return df


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Collect PIHPS food price data")
    parser.add_argument("--approach", choices=["api", "scrape", "manual", "sample", "auto"], default="auto")
    parser.add_argument("--file", type=str, default=None)
    parser.add_argument("--start", type=str, default=None)
    parser.add_argument("--end", type=str, default=None)
    parser.add_argument("--sample", action="store_true")
    args = parser.parse_args()

    df = collect_pihps_data(approach=args.approach, manual_file=args.file,
                            start_date=args.start, end_date=args.end, use_sample=args.sample)
    if df is not None and not df.empty:
        print(f"\nPIHPS data: {len(df)} rows")
        if "tanggal" in df.columns:
            print(f"Date range: {df['tanggal'].min()} to {df['tanggal'].max()}")
        if "kota" in df.columns:
            print(f"Cities: {sorted(df['kota'].unique())}")
