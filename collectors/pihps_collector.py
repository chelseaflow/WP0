#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AgriFlow WP0 — Collector: PIHPS Bank Indonesia (Harga Pangan)
================================================================
PIHPS = Pusat Informasi Harga Pangan Strategis Nasional
Endpoint: https://www.bi.go.id/hargapangan/WebSite/Home/GetChartData

CONFIRMED WORKING — Scrapes real daily price data from Bank Indonesia.

Key discoveries:
  - Price is in 'nominal' field (NOT 'harga' which is always 0)
  - Each commodity has a unique tempId (GUID)
  - Loop: per kab/kota × per month × per commodity
  - Requires valid Cookie + Xsrf-Token from browser session

Speed optimizations vs original scraper:
  - ThreadPoolExecutor: 5 concurrent kab/kota (was sequential)
  - requests.Session: connection pooling (reuse TCP)
  - Delay: 0.5s per request (was 2.0s)
  - Resume: skip already-fetched location-months
  - Original: 38 kota × 60 bulan × 2s = ~76 menit
  - Optimized: same workload in ~8-12 menit

Compatible with:
  - run_all.py: collect_pihps_data(approach="auto") / collect_pihps_data(use_sample=True)
  - config.settings: uses JATIM_KABUPATEN, DATA_RAW_DIR, DATA_PROCESSED_DIR
  - utils.helpers: uses setup_logger, ensure_dir, save_json

Usage:
    python collectors/pihps_collector.py --sample
    python collectors/pihps_collector.py --approach api --days 90
    python collectors/pihps_collector.py --approach api --all-commodities
    python collectors/pihps_collector.py --approach api --start 2024-01-01 --end 2025-12-31
    python collectors/pihps_collector.py --approach manual --file data.csv
    python collectors/pihps_collector.py --refresh-tokens
    python collectors/pihps_collector.py --workers 3 --delay 1.0   # slower but safer
"""

import os
import sys
import time
import json
import csv
import re
from datetime import datetime, timedelta, date
from typing import Optional, List, Dict, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# ============================================================
# Path fix — ensure project root is importable
# ============================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd
import numpy as np

try:
    from dateutil.relativedelta import relativedelta
    HAS_DATEUTIL = True
except ImportError:
    HAS_DATEUTIL = False

from config.settings import (
    JATIM_KABUPATEN, PIHPS_COMMODITIES, PIHPS_BASE_URL,
    PIHPS_JATIM_CITIES, DATA_RAW_DIR, DATA_PROCESSED_DIR
)
from utils.helpers import (
    setup_logger, ensure_dir, save_json,
    clean_price_string, validate_price
)

logger = setup_logger("pihps_collector")

# ============================================================
# CONFIGURATION
# ============================================================
PIHPS_BASE = "https://www.bi.go.id/hargapangan"
GETCHART_ENDPOINT = f"{PIHPS_BASE}/WebSite/Home/GetChartData"

# --- Speed tuning ---
DEFAULT_WORKERS = 5       # Concurrent kab/kota threads
DEFAULT_DELAY = 0.5       # Seconds between requests per thread
MAX_RETRIES = 2           # Retries on timeout/5xx

# --- Token persistence ---
TOKEN_FILE = os.path.join(SCRIPT_DIR, ".pihps_tokens.json")

# Thread-safe auth error flag
_auth_error = threading.Event()

# ============================================================
# COMMODITY TEMPLATE IDs (tempId = GUID per komoditas)
# ============================================================
# HOW TO DISCOVER NEW tempIds:
#   1. Open https://www.bi.go.id/hargapangan/ in Chrome
#   2. F12 → Network → XHR filter
#   3. Select a commodity from the dropdown
#   4. Find GetChartData request → copy "tempId" param value
#   5. Paste below

COMMODITY_TEMPLATES = {
    "beras_kualitas_medium_2": {
        "tempId": "ae94ec7c-32b3-466b-a068-554d5d0c7116",  # CONFIRMED
        "comName": "Beras Kualitas Medium II",
        "satuan": "Rp/kg",
    },
    # ---- ADD MORE AS YOU DISCOVER THEM ----
    # "cabai_merah_besar": {
    #     "tempId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    #     "comName": "Cabai Merah Besar",
    #     "satuan": "Rp/kg",
    # },
    # "bawang_merah": {
    #     "tempId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    #     "comName": "Bawang Merah Ukuran Sedang",
    #     "satuan": "Rp/kg",
    # },
}

# ============================================================
# ALL KAB/KOTA JAWA TIMUR (locationId for GetChartData)
# ============================================================
KAB_KOTA_JATIM = {
    "3501": "Kab. Pacitan",     "3502": "Kab. Ponorogo",
    "3503": "Kab. Trenggalek",  "3504": "Kab. Tulungagung",
    "3505": "Kab. Blitar",      "3506": "Kab. Kediri",
    "3507": "Kab. Malang",      "3508": "Kab. Lumajang",
    "3509": "Kab. Jember",      "3510": "Kab. Banyuwangi",
    "3511": "Kab. Bondowoso",   "3512": "Kab. Situbondo",
    "3513": "Kab. Probolinggo", "3514": "Kab. Pasuruan",
    "3515": "Kab. Sidoarjo",    "3516": "Kab. Mojokerto",
    "3517": "Kab. Jombang",     "3518": "Kab. Nganjuk",
    "3519": "Kab. Madiun",      "3520": "Kab. Magetan",
    "3521": "Kab. Ngawi",       "3522": "Kab. Bojonegoro",
    "3523": "Kab. Tuban",       "3524": "Kab. Lamongan",
    "3525": "Kab. Gresik",      "3526": "Kab. Bangkalan",
    "3527": "Kab. Sampang",     "3528": "Kab. Pamekasan",
    "3529": "Kab. Sumenep",
    "3571": "Kota Kediri",      "3572": "Kota Blitar",
    "3573": "Kota Malang",      "3574": "Kota Probolinggo",
    "3575": "Kota Pasuruan",    "3576": "Kota Mojokerto",
    "3577": "Kota Madiun",      "3578": "Kota Surabaya",
    "3579": "Kota Batu",
}


# ============================================================
# SESSION FACTORY (connection pooling + retries)
# ============================================================
def create_fast_session(cookie: str = "", xsrf_token: str = "") -> requests.Session:
    """
    Session with connection pooling + auto-retry.
    Reusing TCP connections saves ~100ms per request.
    """
    session = requests.Session()

    adapter = HTTPAdapter(
        pool_connections=10,
        pool_maxsize=10,
        max_retries=Retry(
            total=MAX_RETRIES,
            backoff_factor=0.5,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET"],
        ),
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    session.headers.update({
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "id-ID,id;q=0.9,en-US;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": f"{PIHPS_BASE}/",
        "Connection": "keep-alive",
    })

    if cookie:
        session.headers["Cookie"] = cookie
    if xsrf_token:
        session.headers["Xsrf-Token"] = xsrf_token

    return session


# ============================================================
# TOKEN MANAGEMENT
# ============================================================
def load_tokens() -> Dict[str, str]:
    """Load saved tokens from disk."""
    if os.path.exists(TOKEN_FILE):
        try:
            with open(TOKEN_FILE) as f:
                data = json.load(f)
            age_h = (
                datetime.now() -
                datetime.fromisoformat(data.get("saved_at", "2000-01-01"))
            ).total_seconds() / 3600
            if age_h > 24:
                logger.warning(f"Tokens are {age_h:.0f}h old — may be expired")
            return data
        except Exception:
            pass
    return {}


def save_tokens_to_file(cookie: str, xsrf_token: str):
    """Persist tokens for reuse."""
    with open(TOKEN_FILE, "w") as f:
        json.dump({
            "Cookie": cookie,
            "Xsrf-Token": xsrf_token,
            "saved_at": datetime.now().isoformat(),
        }, f, indent=2)
    logger.info(f"Tokens saved → {TOKEN_FILE}")


def obtain_fresh_tokens() -> Dict[str, str]:
    """Visit PIHPS site to grab fresh cookies + XSRF token."""
    logger.info("Obtaining fresh tokens from PIHPS website...")
    s = requests.Session()
    s.headers["User-Agent"] = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    )
    try:
        s.get(PIHPS_BASE, timeout=30)
        s.get(f"{PIHPS_BASE}/TabelHarga/PasarTradisionalDaerah", timeout=30)

        cookies = s.cookies.get_dict()
        cookie_str = "; ".join(f"{k}={v}" for k, v in cookies.items())
        xsrf = cookies.get("XSRF-TOKEN", cookies.get("Xsrf-Token", ""))

        if cookie_str:
            save_tokens_to_file(cookie_str, xsrf)
            logger.info(f"Got {len(cookies)} cookies" +
                        (" + XSRF" if xsrf else " (no XSRF found)"))
            return {"Cookie": cookie_str, "Xsrf-Token": xsrf}
    except Exception as e:
        logger.error(f"Token fetch failed: {e}")
    return {}


def resolve_tokens(cookie: Optional[str] = None,
                   xsrf: Optional[str] = None) -> Tuple[str, str]:
    """Resolve tokens: CLI args → saved file → fresh fetch."""
    if cookie and xsrf:
        return cookie, xsrf
    saved = load_tokens()
    if saved.get("Cookie"):
        return saved["Cookie"], saved.get("Xsrf-Token", "")
    fresh = obtain_fresh_tokens()
    return fresh.get("Cookie", ""), fresh.get("Xsrf-Token", "")


# ============================================================
# MONTHLY PERIOD GENERATOR
# ============================================================
def generate_monthly_periods(start: date, end: date) -> List[Tuple[date, date]]:
    """Split date range into monthly chunks."""
    periods = []
    if HAS_DATEUTIL:
        cur = start.replace(day=1)
        while cur <= end:
            month_end = (cur + relativedelta(months=1)) - relativedelta(days=1)
            month_end = min(month_end, end)
            period_start = max(cur, start)
            periods.append((period_start, month_end))
            cur += relativedelta(months=1)
    else:
        cur = start
        while cur <= end:
            chunk_end = min(cur + timedelta(days=29), end)
            periods.append((cur, chunk_end))
            cur = chunk_end + timedelta(days=1)
    return periods


# ============================================================
# CORE: Fetch one kab/kota × one month
# ============================================================
def fetch_one_month(
    session: requests.Session,
    temp_id: str,
    com_name: str,
    loc_id: str,
    loc_name: str,
    start: date,
    end: date,
) -> Optional[List[Dict]]:
    """
    Single GET to GetChartData.
    Returns list[dict] on success, empty list if no data, None on auth error.
    """
    if _auth_error.is_set():
        return None

    params = {
        "tempId":     temp_id,
        "comName":    com_name,
        "locationId": loc_id,
        "tipeHarga":  "1",
        "startDate":  start.strftime("%Y-%m-%d"),
        "endDate":    end.strftime("%Y-%m-%d"),
        "_":          int(time.time() * 1000),
    }

    try:
        r = session.get(GETCHART_ENDPOINT, params=params, timeout=20)

        if r.status_code in [401, 403]:
            logger.error(f"AUTH ERROR {r.status_code} — tokens expired!")
            _auth_error.set()
            return None

        if r.status_code != 200:
            return []

        data = r.json()
        rows = data.get("data", [])
        if not rows:
            return []

        results = []
        for row in rows:
            harga = row.get("nominal")  # CRITICAL: 'nominal', NOT 'harga'
            if harga is None or harga == 0:
                continue
            results.append({
                "tanggal":       row.get("date", "")[:10],
                "provinsi":      "Jawa Timur",
                "kode_bps":      loc_id,
                "kota":          loc_name,
                "komoditas":     com_name,
                "satuan":        row.get("denomination", "kg"),
                "harga":         harga,
                "fluktuasi":     row.get("fluc"),
                "is_min":        row.get("isMin"),
                "is_max":        row.get("isMax"),
                "is_tetap":      row.get("isTetap"),
                "source":        "PIHPS_API",
            })
        return results

    except requests.exceptions.Timeout:
        logger.debug(f"Timeout: {loc_name} {start}")
        return []
    except json.JSONDecodeError:
        logger.debug(f"Bad JSON: {loc_name} {start}")
        return []
    except Exception as e:
        logger.debug(f"Error: {loc_name} {start}: {e}")
        return []


# ============================================================
# WORKER: All months for ONE kab/kota (runs in thread)
# ============================================================
def _worker_fetch_location(
    session: requests.Session,
    temp_id: str,
    com_name: str,
    loc_id: str,
    loc_name: str,
    periods: List[Tuple[date, date]],
    completed_keys: set,
    delay: float,
) -> Tuple[str, List[Dict], List[str]]:
    """Thread worker: fetch all months for one location."""
    rows_all = []
    failed = []

    for period_start, period_end in periods:
        if _auth_error.is_set():
            break

        ckpt_key = f"{loc_id}_{period_start.strftime('%Y%m')}"
        if ckpt_key in completed_keys:
            continue

        rows = fetch_one_month(
            session, temp_id, com_name,
            loc_id, loc_name,
            period_start, period_end,
        )

        if rows is None:
            break  # Auth error
        elif rows:
            rows_all.extend(rows)
        else:
            failed.append(period_start.strftime("%Y-%m"))

        time.sleep(delay)

    return loc_id, rows_all, failed


# ============================================================
# MAIN API COLLECTOR (concurrent)
# ============================================================
def collect_via_api(
    commodity_key: str = "beras_kualitas_medium_2",
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    n_days: int = 90,
    cookie: Optional[str] = None,
    xsrf_token: Optional[str] = None,
    workers: int = DEFAULT_WORKERS,
    delay: float = DEFAULT_DELAY,
) -> Optional[pd.DataFrame]:
    """
    Concurrent GetChartData scraper.

    Speed: 5 workers × 0.5s delay = ~7× faster than sequential 2.0s
    """
    comm = COMMODITY_TEMPLATES.get(commodity_key)
    if not comm or not comm.get("tempId"):
        logger.error(
            f"No tempId for '{commodity_key}'.\n"
            f"Available: {[k for k,v in COMMODITY_TEMPLATES.items() if v.get('tempId')]}\n"
            "Discover new tempIds: F12 → Network → select commodity → copy tempId"
        )
        return None

    temp_id = comm["tempId"]
    com_name = comm["comName"]

    if end_date is None:
        end_date = date.today()
    if start_date is None:
        start_date = end_date - timedelta(days=n_days)

    periods = generate_monthly_periods(start_date, end_date)
    locations = KAB_KOTA_JATIM
    total_requests = len(locations) * len(periods)

    logger.info("=" * 60)
    logger.info(f"PIHPS GetChartData — {com_name}")
    logger.info(f"Range     : {start_date} → {end_date} ({len(periods)} months)")
    logger.info(f"Locations : {len(locations)} kab/kota")
    logger.info(f"Requests  : ~{total_requests}")
    logger.info(f"Workers   : {workers} concurrent | delay {delay}s")
    est_min = (total_requests * delay) / workers / 60
    logger.info(f"Est. time : ~{est_min:.1f} min")
    logger.info("=" * 60)

    # Auth
    ck, xr = resolve_tokens(cookie, xsrf_token)
    if not ck:
        logger.warning("No auth tokens — API will likely return 401/403.")

    session = create_fast_session(ck, xr)

    # Checkpoint / resume
    ckpt_dir = os.path.join(DATA_RAW_DIR, "pihps", "checkpoints")
    ensure_dir(ckpt_dir)
    progress_file = os.path.join(ckpt_dir, f"progress_{commodity_key}.json")
    completed_keys = set()
    if os.path.exists(progress_file):
        try:
            with open(progress_file) as f:
                completed_keys = set(json.load(f).get("completed", []))
            if completed_keys:
                logger.info(f"Resuming: {len(completed_keys)} location-months already done")
        except Exception:
            pass

    _auth_error.clear()

    # ---- Concurrent execution ----
    all_records = []
    all_failed = []
    loc_items = list(locations.items())

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {}
        for loc_id, loc_name in loc_items:
            f = pool.submit(
                _worker_fetch_location,
                session, temp_id, com_name,
                loc_id, loc_name,
                periods, completed_keys, delay,
            )
            futures[f] = (loc_id, loc_name)

        done_count = 0
        for future in as_completed(futures):
            loc_id, loc_name = futures[future]
            done_count += 1

            try:
                lid, rows, failed = future.result()
            except Exception as e:
                logger.error(f"Worker crash {loc_name}: {e}")
                continue

            if rows:
                all_records.extend(rows)
                logger.info(
                    f"[{done_count}/{len(loc_items)}] {loc_name}: "
                    f"{len(rows)} rows"
                )
                for r in rows:
                    tgl = r.get("tanggal", "")[:7].replace("-", "")
                    completed_keys.add(f"{lid}_{tgl}")

            if failed:
                all_failed.extend((lid, loc_name, lbl) for lbl in failed)

            # Save progress every 5 locations
            if done_count % 5 == 0:
                _save_progress(progress_file, completed_keys)

    _save_progress(progress_file, completed_keys)

    # Auth error message
    if _auth_error.is_set():
        logger.error(
            "\n" + "=" * 60 +
            "\n  AUTH ERROR — Cookie/Xsrf-Token expired!\n\n"
            "  Fix:\n"
            "  1. Open https://www.bi.go.id/hargapangan/ in Chrome\n"
            "  2. F12 → Network → XHR → click commodity on chart\n"
            "  3. Find GetChartData → copy Cookie & Xsrf-Token\n"
            "  4. Run: python collectors/pihps_collector.py --refresh-tokens\n"
            "     OR:  --cookie \"...\" --xsrf \"...\"\n\n"
            "  Script will RESUME from where it stopped.\n" +
            "=" * 60
        )
        if not all_records:
            return None

    if not all_records:
        logger.warning("No data from GetChartData")
        return None

    df = pd.DataFrame(all_records)
    df["tanggal"] = pd.to_datetime(df["tanggal"], errors="coerce")
    df = df.dropna(subset=["tanggal"])
    df = df.sort_values(["kode_bps", "tanggal"]).reset_index(drop=True)
    df = df.drop_duplicates(subset=["kode_bps", "tanggal"], keep="first")

    logger.info(
        f"Result: {len(df)} rows | "
        f"{df['kode_bps'].nunique()} locations | "
        f"{df['tanggal'].nunique()} days | "
        f"{df['tanggal'].min().date()} → {df['tanggal'].max().date()}"
    )
    return df


def collect_via_api_multi(commodity_keys=None, **kwargs):
    """Collect multiple commodities."""
    if commodity_keys is None:
        commodity_keys = [k for k, v in COMMODITY_TEMPLATES.items() if v.get("tempId")]
    if not commodity_keys:
        logger.error("No commodities with known tempId!")
        return None

    dfs = []
    for key in commodity_keys:
        _auth_error.clear()
        df = collect_via_api(commodity_key=key, **kwargs)
        if df is not None and not df.empty:
            dfs.append(df)
    return pd.concat(dfs, ignore_index=True) if dfs else None


def _save_progress(filepath, completed_keys):
    try:
        with open(filepath, "w") as f:
            json.dump({"completed": sorted(completed_keys),
                        "updated_at": datetime.now().isoformat()}, f)
    except Exception:
        pass


# ============================================================
# MANUAL FILE PROCESSING
# ============================================================
def process_manual_file(filepath: str) -> Optional[pd.DataFrame]:
    """
    Process downloaded PIHPS data (CSV/Excel/JSON).
    Download: bi.go.id/hargapangan → Tabel Harga → Download
    """
    logger.info(f"Processing: {filepath}")
    if not os.path.exists(filepath):
        logger.error(f"Not found: {filepath}")
        return None

    ext = os.path.splitext(filepath)[1].lower()
    try:
        if ext == ".csv":
            df = _read_csv_flex(filepath)
        elif ext in [".xlsx", ".xls"]:
            df = pd.read_excel(filepath)
        elif ext == ".json":
            with open(filepath) as f:
                data = json.load(f)
            raw = data.get("data", data) if isinstance(data, dict) else data
            df = pd.DataFrame(raw)
        else:
            logger.error(f"Unsupported: {ext}")
            return None
    except Exception as e:
        logger.error(f"Read error: {e}")
        return None

    if df is None or df.empty:
        return None
    logger.info(f"Raw: {len(df)} rows, cols: {list(df.columns)}")
    return _normalize_df(df)


def _read_csv_flex(filepath):
    for enc in ["utf-8", "utf-8-sig", "latin-1", "cp1252"]:
        for sep in [",", ";", "\t"]:
            try:
                df = pd.read_csv(filepath, encoding=enc, sep=sep,
                                 on_bad_lines="skip", engine="python")
                if len(df.columns) > 1 and len(df) > 0:
                    return df
            except Exception:
                continue
    return None


# ============================================================
# DATA NORMALIZATION
# ============================================================
def _normalize_df(df):
    """Normalize any raw PIHPS format → standard schema."""
    if df is None or df.empty:
        return df

    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]

    comm_kw = ["beras", "cabai", "bawang", "daging", "telur", "gula", "minyak"]
    comm_cols = [c for c in df.columns if any(k in c for k in comm_kw)]

    if len(comm_cols) >= 3:
        # Wide → long
        date_col = _find_col(df.columns, ["tanggal", "date", "tgl"])
        city_col = _find_col(df.columns, ["kota", "kabupaten", "kab_kota_nama"])
        id_vars = [c for c in [date_col, city_col] if c]
        if not id_vars:
            id_vars = [c for c in df.columns if c not in comm_cols][:2]
        df = df.melt(id_vars=id_vars, value_vars=comm_cols,
                     var_name="komoditas", value_name="harga")
        rn = {}
        if date_col: rn[date_col] = "tanggal"
        if city_col: rn[city_col] = "kota"
        df = df.rename(columns=rn)
    else:
        rn = {}
        for c in df.columns:
            cl = c.lower()
            if cl in ["tanggal", "date", "tgl"]:          rn[c] = "tanggal"
            elif cl in ["kota", "kabupaten", "kab_kota_nama"]: rn[c] = "kota"
            elif cl in ["komoditas", "commodity"]:         rn[c] = "komoditas"
            elif cl in ["harga", "price", "nominal"]:      rn[c] = "harga"
            elif cl in ["kab_kota_id", "kode_bps"]:        rn[c] = "kode_bps"
            elif cl in ["satuan", "denomination"]:         rn[c] = "satuan"
        df = df.rename(columns=rn)

    if "harga" in df.columns and df["harga"].dtype == object:
        df["harga"] = df["harga"].astype(str).apply(clean_price_string)
    if "harga" in df.columns:
        df["harga"] = pd.to_numeric(df["harga"], errors="coerce")
        df = df[df["harga"].notna() & (df["harga"] > 0)]
    if "tanggal" in df.columns:
        df["tanggal"] = pd.to_datetime(df["tanggal"], errors="coerce", dayfirst=True)
    if "provinsi" not in df.columns:
        df["provinsi"] = "Jawa Timur"
    if "source" not in df.columns:
        df["source"] = "PIHPS_BI"
    df["processed_at"] = datetime.now().isoformat()
    return df


def _find_col(columns, keywords):
    for c in columns:
        for k in keywords:
            if k in str(c).lower():
                return c
    return None


# ============================================================
# SAMPLE DATA (for development only)
# ============================================================
def generate_sample_pihps_data(n_days=90, cities=None, commodities=None):
    """Synthetic data — clearly marked source=SAMPLE_DATA."""
    if cities is None:
        cities = PIHPS_JATIM_CITIES if PIHPS_JATIM_CITIES else [
            "Kota Surabaya", "Kota Malang", "Kota Kediri",
            "Kab. Jember", "Kota Madiun", "Kota Probolinggo",
            "Kota Mojokerto", "Kota Blitar", "Kota Pasuruan", "Kota Batu",
        ]
    if commodities is None:
        commodities = [
            "Beras Kualitas Medium II", "Cabai Merah Besar",
            "Cabai Rawit Merah", "Bawang Merah Ukuran Sedang",
            "Bawang Putih Ukuran Sedang", "Daging Ayam Ras Segar",
            "Telur Ayam Ras Segar", "Gula Pasir Lokal", "Minyak Goreng Curah",
        ]

    base = {
        "Beras Kualitas Medium II": 13500, "Cabai Merah Besar": 55000,
        "Cabai Rawit Merah": 65000, "Bawang Merah Ukuran Sedang": 42000,
        "Bawang Putih Ukuran Sedang": 40000, "Daging Ayam Ras Segar": 36000,
        "Telur Ayam Ras Segar": 29000, "Gula Pasir Lokal": 18500,
        "Minyak Goreng Curah": 17500,
    }
    vol = {
        "Beras Kualitas Medium II": 0.015, "Cabai Merah Besar": 0.12,
        "Cabai Rawit Merah": 0.15, "Bawang Merah Ukuran Sedang": 0.10,
        "Bawang Putih Ukuran Sedang": 0.06, "Daging Ayam Ras Segar": 0.04,
        "Telur Ayam Ras Segar": 0.03, "Gula Pasir Lokal": 0.015,
        "Minyak Goreng Curah": 0.02,
    }

    np.random.seed(42)
    records = []
    end_dt = datetime.now()
    for d in range(n_days):
        dt = end_dt - timedelta(days=n_days - 1 - d)
        if dt.weekday() >= 5:
            continue
        for comm in commodities:
            bp = base.get(comm, 30000)
            v = vol.get(comm, 0.05)
            seasonal = 1.15 if (comm.startswith("Cabai") and dt.month in [11,12,1,2]) else 1.0
            for city in cities:
                noise = np.random.normal(0, v * 0.3)
                price = round(bp * seasonal * (1 + noise) * (1 + 0.0001 * d), -2)
                records.append({
                    "tanggal": dt.strftime("%Y-%m-%d"), "kota": city,
                    "komoditas": comm, "harga": max(price, bp * 0.4),
                    "satuan": "Rp/kg", "provinsi": "Jawa Timur",
                    "source": "SAMPLE_DATA",
                })

    df = pd.DataFrame(records)
    df["tanggal"] = pd.to_datetime(df["tanggal"])
    logger.info(f"Sample: {len(df)} rows | {len(cities)} cities | "
                f"{len(commodities)} commodities")
    return df


# ============================================================
# MAIN ENTRY POINT — called by run_all.py
# ============================================================
def collect_pihps_data(
    approach="auto", manual_file=None,
    start_date=None, end_date=None, n_days=90,
    use_sample=False,
    commodity_key="beras_kualitas_medium_2",
    all_commodities=False,
    cookie=None, xsrf_token=None,
    workers=DEFAULT_WORKERS, delay=DEFAULT_DELAY,
):
    """
    Main entry point.

    run_all.py calls:
      collect_pihps_data(approach="auto")       → try API, fallback sample
      collect_pihps_data(use_sample=True)       → sample only
    """
    out_dir = os.path.join(DATA_PROCESSED_DIR, "pihps")
    ensure_dir(out_dir)
    ensure_dir(os.path.join(DATA_RAW_DIR, "pihps"))

    df = None
    sd = date.fromisoformat(start_date) if start_date else None
    ed = date.fromisoformat(end_date) if end_date else None

    # ---- Sample ----
    if use_sample or approach == "sample":
        logger.info("Generating SAMPLE data (not real)...")
        df = generate_sample_pihps_data(n_days=n_days)

    # ---- Manual file ----
    elif approach == "manual" and manual_file:
        df = process_manual_file(manual_file)

    # ---- API (real scraping) ----
    elif approach in ["api", "auto"]:
        logger.info("Scraping REAL data from PIHPS Bank Indonesia...")
        if all_commodities:
            df = collect_via_api_multi(
                start_date=sd, end_date=ed, n_days=n_days,
                cookie=cookie, xsrf_token=xsrf_token,
                workers=workers, delay=delay,
            )
        else:
            df = collect_via_api(
                commodity_key=commodity_key,
                start_date=sd, end_date=ed, n_days=n_days,
                cookie=cookie, xsrf_token=xsrf_token,
                workers=workers, delay=delay,
            )

        # Auto fallback
        if (df is None or df.empty) and approach == "auto":
            logger.warning(
                "API failed (auth error?). Falling back to SAMPLE data.\n"
                "  → Fix: python collectors/pihps_collector.py --refresh-tokens"
            )
            df = generate_sample_pihps_data(n_days=n_days)

    # ---- Save ----
    if df is not None and not df.empty:
        out_path = os.path.join(out_dir, "harga_pangan_jatim.csv")
        df.to_csv(out_path, index=False)
        logger.info(f"Saved: {out_path} ({len(df)} rows)")

        save_json({
            "total_rows": len(df),
            "source": df["source"].value_counts().to_dict() if "source" in df.columns else {},
            "date_range": {
                "min": str(df["tanggal"].min()) if "tanggal" in df.columns else None,
                "max": str(df["tanggal"].max()) if "tanggal" in df.columns else None,
            },
            "cities": sorted(df["kota"].unique().tolist()) if "kota" in df.columns else [],
            "commodities": sorted(df["komoditas"].unique().tolist()) if "komoditas" in df.columns else [],
            "saved_at": datetime.now().isoformat(),
        }, os.path.join(out_dir, "pihps_stats.json"))
    else:
        logger.error("No PIHPS data collected!")

    return df


# ============================================================
# CLI
# ============================================================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="AgriFlow WP0 — PIHPS Food Price Collector",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Speed: 38 kota × 12 months × 0.5s / 5 workers = ~2 min (was ~15 min)

Examples:
  %(prog)s --sample                              # Fake data for dev
  %(prog)s --approach api                        # REAL data from BI
  %(prog)s --approach api --days 30              # Last 30 days
  %(prog)s --approach api --workers 3 --delay 1  # Slower, safer
  %(prog)s --approach manual --file data.csv     # Downloaded file
  %(prog)s --refresh-tokens                      # Get fresh auth
        """
    )
    parser.add_argument("--approach", choices=["api","manual","sample","auto"], default="auto")
    parser.add_argument("--file", type=str)
    parser.add_argument("--start", type=str, help="YYYY-MM-DD")
    parser.add_argument("--end", type=str, help="YYYY-MM-DD")
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--commodity", default="beras_kualitas_medium_2")
    parser.add_argument("--all-commodities", action="store_true")
    parser.add_argument("--refresh-tokens", action="store_true")
    parser.add_argument("--cookie", type=str)
    parser.add_argument("--xsrf", type=str)
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY)

    args = parser.parse_args()

    if args.refresh_tokens:
        tokens = obtain_fresh_tokens()
        if tokens.get("Cookie"):
            print(f"Saved to {TOKEN_FILE}")
            print(f"Cookie:     {tokens['Cookie'][:60]}...")
            print(f"Xsrf-Token: {tokens.get('Xsrf-Token','N/A')[:40]}...")
        else:
            print("Failed. Extract manually from browser DevTools (F12).")
        sys.exit(0)

    df = collect_pihps_data(
        approach=args.approach, manual_file=args.file,
        start_date=args.start, end_date=args.end, n_days=args.days,
        use_sample=args.sample, commodity_key=args.commodity,
        all_commodities=args.all_commodities,
        cookie=args.cookie, xsrf_token=args.xsrf,
        workers=args.workers, delay=args.delay,
    )

    if df is not None and not df.empty:
        is_real = "PIHPS_API" in df.get("source", pd.Series()).values
        tag = "REAL DATA" if is_real else "SAMPLE DATA"
        print(f"\n{'='*60}")
        print(f"  PIHPS Collection Complete ({tag})")
        print(f"{'='*60}")
        print(f"  Rows:        {len(df):,}")
        if "tanggal" in df.columns:
            print(f"  Range:       {df['tanggal'].min().date()} → {df['tanggal'].max().date()}")
            print(f"  Days:        {df['tanggal'].nunique()}")
        if "kota" in df.columns:
            print(f"  Cities:      {df['kota'].nunique()}")
        if "komoditas" in df.columns:
            print(f"  Commodities: {df['komoditas'].nunique()}")
        if "source" in df.columns:
            print(f"  Source:      {df['source'].value_counts().to_dict()}")
        print(f"{'='*60}\n")
    else:
        print("\nNo data. Check logs.")
