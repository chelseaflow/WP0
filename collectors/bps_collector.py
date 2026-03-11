"""
AgriFlow WP0 — Collector: BPS Jawa Timur (Produksi Pertanian)
================================================================
API: https://webapi.bps.go.id — register free at https://webapi.bps.go.id/developer/

Usage:
    python collectors/bps_collector.py --sample
    python collectors/bps_collector.py --api-key YOUR_KEY
"""

import os
import sys
import time
import json

# --- Path fix ---
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional, List, Dict

from config.settings import (
    JATIM_KABUPATEN, BPS_API_BASE_URL, BPS_API_KEY,
    BPS_JATIM_DOMAIN, DATA_RAW_DIR, DATA_PROCESSED_DIR,
    POPULASI_JATIM_2024, KONSUMSI_PER_KAPITA_KG
)
from utils.helpers import setup_logger, ensure_dir, save_json

logger = setup_logger("bps_collector")


class BPSClient:
    def __init__(self, api_key=None):
        self.api_key = api_key or BPS_API_KEY
        self.base_url = BPS_API_BASE_URL
        self.session = requests.Session()
        if self.api_key == "YOUR_BPS_API_KEY_HERE":
            logger.warning("BPS API key not set! Register at: https://webapi.bps.go.id/developer/")

    def _request(self, model, domain, **params):
        url = f"{self.base_url}/list/model/{model}/domain/{domain}/key/{self.api_key}/"
        parts = [f"{k}/{v}" for k, v in params.items()]
        if parts:
            url += "/".join(parts) + "/"
        try:
            resp = self.session.get(url, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"BPS API request failed: {e}")
            return None

    def search_variables(self, domain, keyword):
        data = self._request("var", domain, keyword=keyword)
        if data and "data" in data:
            return data["data"][1] if isinstance(data["data"], list) and len(data["data"]) > 1 else data["data"]
        return None

    def get_dynamic_data(self, domain, var_id):
        return self._request("data", domain, var=var_id)


def discover_production_variables(client):
    logger.info("Discovering BPS production variables...")
    found = {}
    for term in ["produksi padi", "produksi cabai", "produksi bawang", "luas panen"]:
        results = client.search_variables(BPS_JATIM_DOMAIN, term)
        if results:
            for item in results[:3]:
                var_id = item.get("var_id") or item.get("id")
                title = item.get("title") or item.get("judul") or str(item)
                logger.info(f"  [{var_id}] {title}")
                found[title] = var_id
        time.sleep(0.5)
    return found


def generate_sample_production_data():
    np.random.seed(42)
    commodities_production = {
        "padi": {"base_range": (50_000, 400_000),
                 "top_producers": ["3524", "3522", "3509", "3510", "3507"]},
        "jagung": {"base_range": (20_000, 250_000),
                   "top_producers": ["3504", "3509", "3505", "3524", "3522"]},
        "cabai_merah": {"base_range": (1_000, 30_000),
                        "top_producers": ["3505", "3506", "3509", "3504", "3507"]},
        "cabai_rawit": {"base_range": (500, 15_000),
                        "top_producers": ["3505", "3509", "3510", "3504", "3506"]},
        "bawang_merah": {"base_range": (500, 50_000),
                         "top_producers": ["3513", "3518", "3505", "3509", "3507"]},
        "bawang_putih": {"base_range": (100, 5_000),
                         "top_producers": ["3507", "3579", "3504"]},
    }
    productivity = {"padi": 5.5, "jagung": 5.0, "cabai_merah": 8.0,
                    "cabai_rawit": 6.0, "bawang_merah": 10.0, "bawang_putih": 7.0}
    records = []
    for year in [2022, 2023, 2024]:
        for commodity, info in commodities_production.items():
            bmin, bmax = info["base_range"]
            for kab in JATIM_KABUPATEN:
                kode = kab["kode_bps"]
                if kode in info["top_producers"]:
                    prod = np.random.uniform(bmax * 0.6, bmax)
                elif kode.startswith("357"):
                    prod = np.random.uniform(0, bmin * 0.1)
                else:
                    prod = np.random.uniform(bmin * 0.3, bmax * 0.4)
                prod *= (1 + np.random.normal(0, 0.08))
                prod = max(0, round(prod, 1))
                pprod = productivity.get(commodity, 6.0)
                records.append({
                    "tahun": year, "kode_bps": kode,
                    "nama_kabupaten": kab["nama"], "komoditas": commodity,
                    "produksi_ton": prod,
                    "luas_panen_ha": round(prod / pprod, 1) if prod > 0 else 0,
                    "produktivitas_ton_ha": round(pprod * (1 + np.random.normal(0, 0.05)), 2),
                    "source": "SAMPLE_DATA",
                })
    df = pd.DataFrame(records)
    logger.info(f"Generated sample BPS data: {len(df)} records")
    return df


def calculate_supply_balance(production_df, year=2024):
    prod = production_df[production_df["tahun"] == year].copy()
    if prod.empty:
        latest = production_df["tahun"].max()
        logger.warning(f"No data for {year}, using {latest}")
        prod = production_df[production_df["tahun"] == latest].copy()

    commodity_to_consumption = {
        "padi": "beras", "cabai_merah": "cabai", "cabai_rawit": "cabai",
        "bawang_merah": "bawang_merah", "bawang_putih": "bawang_putih",
    }
    PADI_TO_BERAS = 0.62
    records = []

    for _, row in prod.iterrows():
        kode = row["kode_bps"]
        commodity = row["komoditas"]
        cons_cat = commodity_to_consumption.get(commodity)
        if not cons_cat:
            continue
        konsumsi_pk = KONSUMSI_PER_KAPITA_KG.get(cons_cat, 0)
        populasi = POPULASI_JATIM_2024.get(kode, 0)
        if populasi == 0:
            continue
        konsumsi_ton = (konsumsi_pk * populasi) / 1000
        eff_prod = row["produksi_ton"] * PADI_TO_BERAS if commodity == "padi" else row["produksi_ton"]
        balance = eff_prod - konsumsi_ton
        threshold = 0.05
        status = "SURPLUS" if balance > konsumsi_ton * threshold else (
            "DEFICIT" if balance < -konsumsi_ton * threshold else "BALANCED")
        records.append({
            "kode_bps": kode, "nama_kabupaten": row["nama_kabupaten"],
            "komoditas": commodity, "kategori_konsumsi": cons_cat,
            "produksi_ton": round(row["produksi_ton"], 1),
            "produksi_efektif_ton": round(eff_prod, 1),
            "konsumsi_ton": round(konsumsi_ton, 1),
            "balance_ton": round(balance, 1),
            "balance_pct": round((balance / konsumsi_ton * 100) if konsumsi_ton > 0 else 0, 1),
            "status": status, "populasi": populasi, "tahun": year,
        })
    df = pd.DataFrame(records)
    if not df.empty:
        logger.info(f"Balance: {len(df)} records | "
                    f"Surplus: {(df['status']=='SURPLUS').sum()} | "
                    f"Deficit: {(df['status']=='DEFICIT').sum()}")
    return df


def collect_bps_data(api_key=None, use_sample=False, calculate_balance=True):
    out_dir = os.path.join(DATA_PROCESSED_DIR, "bps")
    ensure_dir(out_dir)
    results = {}

    if use_sample:
        df_prod = generate_sample_production_data()
    else:
        client = BPSClient(api_key=api_key)
        found = discover_production_variables(client)
        # Try to get data from found variables
        df_prod = None
        if found:
            all_data = []
            for name, var_id in found.items():
                data = client.get_dynamic_data(BPS_JATIM_DOMAIN, str(var_id))
                if data:
                    save_json(data, os.path.join(DATA_RAW_DIR, "bps", f"bps_var_{var_id}.json"))
            # If API data parsing is complex, fall back to sample
            if not all_data:
                logger.warning("API data parsing needs customization. Using sample data.")
                df_prod = generate_sample_production_data()
        else:
            logger.warning("No variables found. Using sample data.")
            df_prod = generate_sample_production_data()

    prod_path = os.path.join(out_dir, "produksi_pertanian_jatim.csv")
    df_prod.to_csv(prod_path, index=False)
    logger.info(f"Saved: {prod_path}")
    results["production"] = df_prod

    if calculate_balance:
        df_bal = calculate_supply_balance(df_prod)
        bal_path = os.path.join(out_dir, "supply_balance_jatim.csv")
        df_bal.to_csv(bal_path, index=False)
        logger.info(f"Saved: {bal_path}")
        results["balance"] = df_bal

    return results


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Collect BPS production data")
    parser.add_argument("--api-key", type=str, default=None)
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--no-balance", action="store_true")
    args = parser.parse_args()

    results = collect_bps_data(api_key=args.api_key, use_sample=args.sample,
                               calculate_balance=not args.no_balance)
    for name, df in results.items():
        print(f"\n{name.upper()}: {len(df)} rows")
        print(df.head(5).to_string(index=False))
