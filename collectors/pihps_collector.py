"""
AgriFlow WP0 — Collector: PIHPS Bank Indonesia (Harga Pangan)
================================================================
PIHPS = Pusat Informasi Harga Pangan Strategis Nasional
Website: https://www.bi.go.id/hargapangan/

PROVEN STRATEGY (ordered by reliability):
  1. GetChartData API — CONFIRMED WORKING endpoint
     - URL: /hargapangan/WebSite/Home/GetChartData
     - Loops per kab/kota × per month × per commodity
     - Key insight: price is in 'nominal' field (NOT 'harga' which is always 0)
     - Requires: valid Cookie + Xsrf-Token (from browser session)
  2. Session-based AJAX — Try with fresh session tokens
  3. HTML Scraping — Parse rendered table pages
  4. Manual CSV/Excel — User downloads from BI website
  5. Sample Data — Realistic synthetic data for development/demo

COMMODITY TEMPLATE IDs (tempId):
  Each commodity on PIHPS has a unique GUID (tempId).
  These can be discovered by inspecting Network tab when selecting
  a commodity on the PIHPS chart page.

Usage:
    python collectors/pihps_collector.py --sample
    python collectors/pihps_collector.py --approach api
    python collectors/pihps_collector.py --approach api --days 30
    python collectors/pihps_collector.py --approach api --commodity beras_medium_2
    python collectors/pihps_collector.py --approach api --all-commodities
    python collectors/pihps_collector.py --approach manual --file data.csv
    python collectors/pihps_collector.py --approach auto
    python collectors/pihps_collector.py --refresh-tokens
"""

import os
import sys
import time
import json
import re
import csv

# --- Path fix: ensure project root is importable ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import requests
import pandas as pd
from datetime import datetime, timedelta, date
from typing import Optional, List, Dict, Tuple

try:
    from dateutil.relativedelta import relativedelta
    HAS_DATEUTIL = True
except ImportError:
    HAS_DATEUTIL = False

try:
    from bs4 import BeautifulSoup
    HAS_BS4 = True
except ImportError:
    HAS_BS4 = False

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
# CONSTANTS & CONFIGURATION
# ============================================================
PIHPS_BASE = "https://www.bi.go.id/hargapangan"

# THE WORKING ENDPOINT (confirmed via browser Network tab inspection)
GETCHART_ENDPOINT = f"{PIHPS_BASE}/WebSite/Home/GetChartData"

# Province code for Jawa Timur
PIHPS_PROVINSI_JATIM = "15"

# Delay between API requests (be polite to BI servers)
REQUEST_DELAY = 2.0

# ============================================================
# COMMODITY TEMPLATE IDs
# ============================================================
# Each commodity has a unique GUID on the PIHPS system.
# HOW TO FIND NEW tempIds:
#   1. Open https://www.bi.go.id/hargapangan/ in Chrome
#   2. Open DevTools (F12) → Network tab → filter XHR
#   3. Select a commodity from the dropdown on the chart page
#   4. Look at the request to GetChartData → copy "tempId" parameter
#
# These IDs appear to be stable but may change if BI rebuilds the site.
# Last verified: 2025

COMMODITY_TEMPLATES = {
    "beras_kualitas_bawah_1": {
        "tempId": None,  # TODO: discover via browser
        "nama": "Beras Kualitas Bawah I",
        "comName": "Beras Kualitas Bawah I",
        "satuan": "Rp/kg",
    },
    "beras_kualitas_bawah_2": {
        "tempId": None,
        "nama": "Beras Kualitas Bawah II",
        "comName": "Beras Kualitas Bawah II",
        "satuan": "Rp/kg",
    },
    "beras_kualitas_medium_1": {
        "tempId": None,
        "nama": "Beras Kualitas Medium I",
        "comName": "Beras Kualitas Medium I",
        "satuan": "Rp/kg",
    },
    "beras_kualitas_medium_2": {
        # This one is confirmed from pihps_scraper_final.py
        "tempId": "ae94ec7c-32b3-466b-a068-554d5d0c7116",
        "nama": "Beras Kualitas Medium II",
        "comName": "Beras Kualitas Medium II",
        "satuan": "Rp/kg",
    },
    "beras_kualitas_super_1": {
        "tempId": None,
        "nama": "Beras Kualitas Super I",
        "comName": "Beras Kualitas Super I",
        "satuan": "Rp/kg",
    },
    "beras_kualitas_super_2": {
        "tempId": None,
        "nama": "Beras Kualitas Super II",
        "comName": "Beras Kualitas Super II",
        "satuan": "Rp/kg",
    },
    "cabai_merah_besar": {
        "tempId": None,
        "nama": "Cabai Merah Besar",
        "comName": "Cabai Merah Besar",
        "satuan": "Rp/kg",
    },
    "cabai_merah_keriting": {
        "tempId": None,
        "nama": "Cabai Merah Keriting",
        "comName": "Cabai Merah Keriting",
        "satuan": "Rp/kg",
    },
    "cabai_rawit_hijau": {
        "tempId": None,
        "nama": "Cabai Rawit Hijau",
        "comName": "Cabai Rawit Hijau",
        "satuan": "Rp/kg",
    },
    "cabai_rawit_merah": {
        "tempId": None,
        "nama": "Cabai Rawit Merah",
        "comName": "Cabai Rawit Merah",
        "satuan": "Rp/kg",
    },
    "bawang_merah": {
        "tempId": None,
        "nama": "Bawang Merah Ukuran Sedang",
        "comName": "Bawang Merah Ukuran Sedang",
        "satuan": "Rp/kg",
    },
    "bawang_putih": {
        "tempId": None,
        "nama": "Bawang Putih Ukuran Sedang",
        "comName": "Bawang Putih Ukuran Sedang",
        "satuan": "Rp/kg",
    },
    "daging_sapi_kualitas_1": {
        "tempId": None,
        "nama": "Daging Sapi Kualitas 1",
        "comName": "Daging Sapi Kualitas 1",
        "satuan": "Rp/kg",
    },
    "daging_sapi_kualitas_2": {
        "tempId": None,
        "nama": "Daging Sapi Kualitas 2",
        "comName": "Daging Sapi Kualitas 2",
        "satuan": "Rp/kg",
    },
    "daging_ayam_ras": {
        "tempId": None,
        "nama": "Daging Ayam Ras Segar",
        "comName": "Daging Ayam Ras Segar",
        "satuan": "Rp/kg",
    },
    "telur_ayam_ras": {
        "tempId": None,
        "nama": "Telur Ayam Ras Segar",
        "comName": "Telur Ayam Ras Segar",
        "satuan": "Rp/kg",
    },
    "gula_pasir_lokal": {
        "tempId": None,
        "nama": "Gula Pasir Lokal",
        "comName": "Gula Pasir Lokal",
        "satuan": "Rp/kg",
    },
    "gula_pasir_premium": {
        "tempId": None,
        "nama": "Gula Pasir Kualitas Premium",
        "comName": "Gula Pasir Kualitas Premium",
        "satuan": "Rp/kg",
    },
    "minyak_goreng_curah": {
        "tempId": None,
        "nama": "Minyak Goreng Curah",
        "comName": "Minyak Goreng Curah",
        "satuan": "Rp/liter",
    },
    "minyak_goreng_kemasan_1": {
        "tempId": None,
        "nama": "Minyak Goreng Kemasan Bermerk 1",
        "comName": "Minyak Goreng Kemasan Bermerk 1",
        "satuan": "Rp/liter",
    },
    "minyak_goreng_kemasan_2": {
        "tempId": None,
        "nama": "Minyak Goreng Kemasan Bermerk 2",
        "comName": "Minyak Goreng Kemasan Bermerk 2",
        "satuan": "Rp/liter",
    },
}

# ============================================================
# KAB/KOTA JAWA TIMUR — locationId for GetChartData
# ============================================================
# These are the BPS kode used as locationId in the API.
# Verified from pihps_scraper_final.py.
# IMPORTANT: Not all kab/kota may have data on PIHPS — PIHPS covers
# 82 cities nationwide, ~10 in Jawa Timur. But the API accepts
# all kab/kota codes and returns empty if no data.

KAB_KOTA_JATIM = {
    "3501": "Kab. Pacitan",
    "3502": "Kab. Ponorogo",
    "3503": "Kab. Trenggalek",
    "3504": "Kab. Tulungagung",
    "3505": "Kab. Blitar",
    "3506": "Kab. Kediri",
    "3507": "Kab. Malang",
    "3508": "Kab. Lumajang",
    "3509": "Kab. Jember",
    "3510": "Kab. Banyuwangi",
    "3511": "Kab. Bondowoso",
    "3512": "Kab. Situbondo",
    "3513": "Kab. Probolinggo",
    "3514": "Kab. Pasuruan",
    "3515": "Kab. Sidoarjo",
    "3516": "Kab. Mojokerto",
    "3517": "Kab. Jombang",
    "3518": "Kab. Nganjuk",
    "3519": "Kab. Madiun",
    "3520": "Kab. Magetan",
    "3521": "Kab. Ngawi",
    "3522": "Kab. Bojonegoro",
    "3523": "Kab. Tuban",
    "3524": "Kab. Lamongan",
    "3525": "Kab. Gresik",
    "3526": "Kab. Bangkalan",
    "3527": "Kab. Sampang",
    "3528": "Kab. Pamekasan",
    "3529": "Kab. Sumenep",
    "3571": "Kota Kediri",
    "3572": "Kota Blitar",
    "3573": "Kota Malang",
    "3574": "Kota Probolinggo",
    "3575": "Kota Pasuruan",
    "3576": "Kota Mojokerto",
    "3577": "Kota Madiun",
    "3578": "Kota Surabaya",
    "3579": "Kota Batu",
}

# Standard browser headers
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": f"{PIHPS_BASE}/",
    "Connection": "keep-alive",
}

# Token file path for persisting Cookie + Xsrf-Token between runs
TOKEN_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    ".pihps_tokens.json"
)


# ============================================================
# TOKEN MANAGEMENT
# ============================================================
def load_saved_tokens() -> Optional[Dict[str, str]]:
    """Load saved Cookie + Xsrf-Token from disk."""
    if os.path.exists(TOKEN_FILE):
        try:
            with open(TOKEN_FILE) as f:
                tokens = json.load(f)
            age_hours = (
                datetime.now() -
                datetime.fromisoformat(tokens.get("saved_at", "2000-01-01"))
            ).total_seconds() / 3600
            if age_hours > 24:
                logger.warning(
                    f"Saved tokens are {age_hours:.0f}h old — may be expired"
                )
            return tokens
        except Exception as e:
            logger.debug(f"Could not load tokens: {e}")
    return None


def save_tokens(cookie: str, xsrf_token: str):
    """Save tokens to disk for reuse."""
    try:
        with open(TOKEN_FILE, "w") as f:
            json.dump({
                "Cookie": cookie,
                "Xsrf-Token": xsrf_token,
                "saved_at": datetime.now().isoformat(),
            }, f, indent=2)
        logger.info(f"Tokens saved to {TOKEN_FILE}")
    except Exception as e:
        logger.warning(f"Could not save tokens: {e}")


def obtain_fresh_tokens() -> Optional[Dict[str, str]]:
    """
    Try to obtain fresh Cookie + Xsrf-Token by visiting the PIHPS site.
    The site sets cookies on first visit, and the Xsrf-Token may be
    in a cookie or in a meta tag / hidden form field.
    """
    logger.info("Attempting to obtain fresh tokens from PIHPS...")
    session = requests.Session()
    session.headers.update({
        "User-Agent": BROWSER_HEADERS["User-Agent"],
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "id-ID,id;q=0.9",
    })

    try:
        # Step 1: Visit homepage to get initial cookies
        resp = session.get(PIHPS_BASE, timeout=30)
        resp.raise_for_status()

        # Step 2: Visit the chart/table page to trigger full cookie set
        resp2 = session.get(
            f"{PIHPS_BASE}/TabelHarga/PasarTradisionalDaerah",
            timeout=30
        )

        # Collect all cookies
        all_cookies = session.cookies.get_dict()
        cookie_str = "; ".join(f"{k}={v}" for k, v in all_cookies.items())

        # Look for Xsrf-Token in cookies
        xsrf = all_cookies.get("XSRF-TOKEN", "")
        if not xsrf:
            xsrf = all_cookies.get("Xsrf-Token", "")
        if not xsrf:
            xsrf = all_cookies.get(".AspNetCore.Antiforgery", "")

        # Also check for token in HTML meta tags
        if not xsrf and HAS_BS4:
            soup = BeautifulSoup(resp.text, "html.parser")
            # Meta tag
            meta = soup.find("meta", {"name": "csrf-token"})
            if meta:
                xsrf = meta.get("content", "")
            # Hidden input
            if not xsrf:
                inp = soup.find("input", {"name": "__RequestVerificationToken"})
                if inp:
                    xsrf = inp.get("value", "")

        if cookie_str:
            logger.info(f"Got cookies: {len(all_cookies)} entries")
            if xsrf:
                logger.info("Got Xsrf-Token")
                save_tokens(cookie_str, xsrf)
            else:
                logger.warning(
                    "No Xsrf-Token found in cookies or HTML. "
                    "API calls may fail with 401/403."
                )
                save_tokens(cookie_str, "")
            return {"Cookie": cookie_str, "Xsrf-Token": xsrf}
        else:
            logger.warning("No cookies received from PIHPS site")
            return None

    except Exception as e:
        logger.error(f"Token acquisition failed: {e}")
        return None


def get_api_headers(
    cookie: Optional[str] = None,
    xsrf_token: Optional[str] = None,
) -> Dict[str, str]:
    """Build headers for GetChartData requests."""
    headers = dict(BROWSER_HEADERS)

    # Try provided tokens first, then saved, then fresh
    if cookie and xsrf_token:
        headers["Cookie"] = cookie
        headers["Xsrf-Token"] = xsrf_token
    else:
        saved = load_saved_tokens()
        if saved:
            headers["Cookie"] = saved.get("Cookie", "")
            headers["Xsrf-Token"] = saved.get("Xsrf-Token", "")
        else:
            fresh = obtain_fresh_tokens()
            if fresh:
                headers["Cookie"] = fresh.get("Cookie", "")
                headers["Xsrf-Token"] = fresh.get("Xsrf-Token", "")

    return headers


# ============================================================
# HELPER: Monthly Period Generator
# ============================================================
def generate_monthly_periods(
    start_date: date,
    end_date: date,
) -> List[Tuple[date, date]]:
    """
    Split a date range into monthly chunks.
    GetChartData works best with 1-month windows.
    """
    if HAS_DATEUTIL:
        periods = []
        cur = start_date.replace(day=1)
        while cur <= end_date:
            month_end = (cur + relativedelta(months=1)) - relativedelta(days=1)
            month_end = min(month_end, end_date)
            period_start = max(cur, start_date)
            periods.append((period_start, month_end))
            cur += relativedelta(months=1)
        return periods
    else:
        # Fallback without dateutil: use ~30 day chunks
        periods = []
        cur = start_date
        while cur <= end_date:
            chunk_end = min(cur + timedelta(days=29), end_date)
            periods.append((cur, chunk_end))
            cur = chunk_end + timedelta(days=1)
        return periods


# ============================================================
# APPROACH 1: GetChartData API (PRIMARY — CONFIRMED WORKING)
# ============================================================
def fetch_getchart_one_month(
    headers: Dict[str, str],
    temp_id: str,
    com_name: str,
    location_id: str,
    location_name: str,
    start_date: date,
    end_date: date,
) -> Optional[List[Dict]]:
    """
    Fetch daily price data for one kab/kota, one month, one commodity.

    Returns:
        List of dicts — success
        Empty list     — no data for this period
        None           — auth error (stop everything)
    """
    params = {
        "tempId": temp_id,
        "comName": com_name,
        "locationId": location_id,
        "tipeHarga": "1",  # 1 = pasar tradisional
        "startDate": start_date.strftime("%Y-%m-%d"),
        "endDate": end_date.strftime("%Y-%m-%d"),
        "_": int(time.time() * 1000),  # Cache buster
    }

    try:
        resp = requests.get(
            GETCHART_ENDPOINT,
            headers=headers,
            params=params,
            timeout=20,
        )

        # Auth errors — signal to stop
        if resp.status_code in [401, 403]:
            logger.error(
                f"AUTH ERROR ({resp.status_code}) — "
                "Cookie/Xsrf-Token expired or invalid!"
            )
            return None

        if resp.status_code != 200:
            logger.debug(f"HTTP {resp.status_code} for {location_name}")
            return []

        data = resp.json()
        rows = data.get("data", [])

        if not rows:
            return []

        results = []
        for row in rows:
            # CRITICAL: price is in 'nominal', NOT 'harga' (which is always 0)
            harga = row.get("nominal")
            if harga is None or harga == 0:
                continue

            results.append({
                "tanggal": row.get("date", "")[:10],
                "provinsi": "Jawa Timur",
                "kode_bps": location_id,
                "kota": location_name,
                "komoditas": com_name,
                "satuan": row.get("denomination", "kg"),
                "harga": harga,
                "fluktuasi": row.get("fluc"),
                "is_min": row.get("isMin"),
                "is_max": row.get("isMax"),
                "is_tetap": row.get("isTetap"),
                "source": "PIHPS_API",
            })

        return results

    except requests.exceptions.Timeout:
        logger.warning(f"Timeout for {location_name} ({start_date})")
        return []
    except json.JSONDecodeError:
        logger.warning(f"Invalid JSON for {location_name} ({start_date})")
        return []
    except Exception as e:
        logger.error(f"Error fetching {location_name}: {e}")
        return []


def collect_via_getchart(
    commodity_key: str = "beras_kualitas_medium_2",
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    n_days: int = 90,
    locations: Optional[Dict[str, str]] = None,
    cookie: Optional[str] = None,
    xsrf_token: Optional[str] = None,
    delay: float = REQUEST_DELAY,
    save_checkpoints: bool = True,
) -> Optional[pd.DataFrame]:
    """
    Collect price data via GetChartData API for one commodity.

    This is the PRIMARY and most reliable method. It loops through
    all kab/kota × monthly periods and aggregates the results.

    Args:
        commodity_key: Key from COMMODITY_TEMPLATES
        start_date: Start date (default: n_days ago)
        end_date: End date (default: today)
        n_days: Days of history if start_date not given
        locations: Dict of {locationId: name} (default: all Jatim)
        cookie: Browser cookie string
        xsrf_token: XSRF token string
        delay: Seconds between requests
        save_checkpoints: Save per-kota checkpoint files

    Returns:
        DataFrame with price data, or None if auth error
    """
    # Resolve commodity
    comm_info = COMMODITY_TEMPLATES.get(commodity_key)
    if not comm_info:
        logger.error(f"Unknown commodity: {commodity_key}")
        logger.info(f"Available: {list(COMMODITY_TEMPLATES.keys())}")
        return None

    temp_id = comm_info.get("tempId")
    if not temp_id:
        logger.error(
            f"No tempId for '{commodity_key}'. "
            "You need to discover it from the PIHPS website:\n"
            "  1. Open https://www.bi.go.id/hargapangan/ in Chrome\n"
            "  2. Open DevTools (F12) → Network tab → filter XHR\n"
            "  3. Select this commodity on the chart page\n"
            "  4. Copy the 'tempId' parameter from the GetChartData request\n"
            "  5. Add it to COMMODITY_TEMPLATES in this file"
        )
        return None

    com_name = comm_info["comName"]

    # Resolve dates
    if end_date is None:
        end_date = date.today()
    if start_date is None:
        start_date = end_date - timedelta(days=n_days)

    # Resolve locations
    if locations is None:
        locations = KAB_KOTA_JATIM

    # Generate monthly periods
    periods = generate_monthly_periods(start_date, end_date)

    logger.info("=" * 60)
    logger.info(f"PIHPS GetChartData — {com_name}")
    logger.info(f"Range: {start_date} → {end_date}")
    logger.info(f"Locations: {len(locations)} kab/kota")
    logger.info(f"Periods: {len(periods)} months")
    logger.info(f"Total requests: ~{len(locations) * len(periods)}")
    logger.info("=" * 60)

    # Get auth headers
    headers = get_api_headers(cookie, xsrf_token)

    # Setup checkpoint directory
    ckpt_dir = os.path.join(DATA_RAW_DIR, "pihps", "checkpoints")
    ensure_dir(ckpt_dir)

    # Load existing checkpoint to enable resume
    ckpt_file = os.path.join(ckpt_dir, f"ckpt_{commodity_key}_progress.json")
    completed_keys = set()
    if os.path.exists(ckpt_file):
        try:
            with open(ckpt_file) as f:
                completed_keys = set(json.load(f).get("completed", []))
            if completed_keys:
                logger.info(f"Resuming: {len(completed_keys)} location-months done")
        except Exception:
            pass

    all_records = []
    failed = []
    auth_error = False
    total_requests = len(locations) * len(periods)
    counter = 0

    for loc_id, loc_name in locations.items():
        if auth_error:
            break

        loc_records = []

        for period_start, period_end in periods:
            counter += 1
            ckpt_key = f"{loc_id}_{period_start.strftime('%Y%m')}"

            # Skip if already done (resume support)
            if ckpt_key in completed_keys:
                continue

            label = period_start.strftime("%Y-%m")
            logger.debug(
                f"[{counter}/{total_requests}] {loc_name} {label}"
            )

            rows = fetch_getchart_one_month(
                headers, temp_id, com_name,
                loc_id, loc_name,
                period_start, period_end,
            )

            if rows is None:
                # Auth error — stop everything
                auth_error = True
                logger.error("Auth error — stopping collection")
                break
            elif rows:
                all_records.extend(rows)
                loc_records.extend(rows)
                logger.info(
                    f"  [{counter}/{total_requests}] {loc_name} {label}: "
                    f"{len(rows)} rows"
                )
            else:
                logger.debug(
                    f"  [{counter}/{total_requests}] {loc_name} {label}: "
                    "empty"
                )
                failed.append((loc_id, loc_name, label))

            # Mark as completed
            completed_keys.add(ckpt_key)
            time.sleep(delay)

        # Save per-location checkpoint
        if save_checkpoints and loc_records:
            loc_ckpt = os.path.join(
                ckpt_dir,
                f"ckpt_{commodity_key}_{loc_id}.csv"
            )
            _save_records_csv(loc_records, loc_ckpt)

        # Update progress checkpoint
        if save_checkpoints:
            with open(ckpt_file, "w") as f:
                json.dump({
                    "completed": list(completed_keys),
                    "last_updated": datetime.now().isoformat(),
                }, f)

    # Handle auth error
    if auth_error:
        logger.error(
            "\n" + "=" * 60 + "\n"
            "  AUTHENTICATION ERROR\n"
            "  Cookie/Xsrf-Token are expired or invalid.\n\n"
            "  To fix:\n"
            "  1. Open https://www.bi.go.id/hargapangan/ in Chrome\n"
            "  2. Open DevTools (F12) → Network tab\n"
            "  3. Click any commodity on the chart\n"
            "  4. Find the GetChartData request\n"
            "  5. Right-click → Copy as cURL\n"
            "  6. Extract Cookie and Xsrf-Token values\n"
            "  7. Run: python collectors/pihps_collector.py --refresh-tokens\n"
            "     OR update TOKEN_FILE manually\n\n"
            "  The script will resume from where it stopped.\n"
            + "=" * 60
        )
        if not all_records:
            return None

    # Build DataFrame
    if not all_records:
        logger.warning("No data collected via GetChartData")
        return None

    df = pd.DataFrame(all_records)
    df["tanggal"] = pd.to_datetime(df["tanggal"], errors="coerce")
    df = df.dropna(subset=["tanggal"])
    df = df.sort_values(["kode_bps", "tanggal"]).reset_index(drop=True)
    df = df.drop_duplicates(subset=["kode_bps", "tanggal"], keep="first")

    logger.info(
        f"\nGetChartData results: {len(df)} rows | "
        f"{df['kode_bps'].nunique()} locations | "
        f"{df['tanggal'].nunique()} days"
    )

    if failed:
        logger.info(f"Empty responses: {len(failed)}")

    return df


def collect_via_getchart_multi(
    commodity_keys: Optional[List[str]] = None,
    **kwargs,
) -> Optional[pd.DataFrame]:
    """
    Collect data for MULTIPLE commodities via GetChartData.
    Only commodities with a known tempId will be collected.
    """
    if commodity_keys is None:
        # Collect all commodities that have a tempId
        commodity_keys = [
            k for k, v in COMMODITY_TEMPLATES.items()
            if v.get("tempId")
        ]

    if not commodity_keys:
        logger.error(
            "No commodities with known tempId! "
            "Currently only 'beras_kualitas_medium_2' has a confirmed tempId. "
            "See COMMODITY_TEMPLATES for instructions on discovering more."
        )
        return None

    logger.info(f"Collecting {len(commodity_keys)} commodities: {commodity_keys}")

    all_dfs = []
    for key in commodity_keys:
        logger.info(f"\n{'='*60}")
        logger.info(f"Commodity: {key}")
        logger.info(f"{'='*60}")
        df = collect_via_getchart(commodity_key=key, **kwargs)
        if df is not None and not df.empty:
            all_dfs.append(df)

    if not all_dfs:
        return None

    combined = pd.concat(all_dfs, ignore_index=True)
    logger.info(
        f"\nCombined: {len(combined)} rows, "
        f"{combined['komoditas'].nunique()} commodities"
    )
    return combined


# ============================================================
# APPROACH 2: AJAX Discovery + Other Endpoints
# ============================================================
def discover_ajax_endpoints(session: requests.Session) -> List[str]:
    """Crawl PIHPS pages to discover AJAX endpoints from JavaScript."""
    if not HAS_BS4:
        logger.warning("BeautifulSoup not available — skipping AJAX discovery")
        return []

    logger.info("Discovering AJAX endpoints...")
    discovered = set()

    pages = [
        PIHPS_BASE,
        f"{PIHPS_BASE}/TabelHarga/PasarTradisionalDaerah",
        f"{PIHPS_BASE}/TabelHarga/PasarTradisionalKomoditas",
    ]

    for page_url in pages:
        try:
            resp = session.get(page_url, timeout=30)
            if resp.status_code != 200:
                continue

            # Pattern: url: "/hargapangan/..."
            urls = re.findall(
                r'url\s*:\s*["\'](/hargapangan/[^"\']+)["\']',
                resp.text
            )
            discovered.update(urls)

            # Pattern: $.ajax/$.post/$.get
            urls = re.findall(
                r'\$\.(?:ajax|post|get)\s*\(\s*["\']([^"\']+)["\']',
                resp.text
            )
            discovered.update(u for u in urls if "/hargapangan/" in u)

            # Pattern: fetch(...)
            urls = re.findall(
                r'fetch\s*\(\s*["\']([^"\']+)["\']',
                resp.text
            )
            discovered.update(u for u in urls if "/hargapangan/" in u)

            time.sleep(0.5)

        except Exception as e:
            logger.debug(f"Error checking {page_url}: {e}")

    if discovered:
        logger.info(f"Discovered {len(discovered)} endpoint(s):")
        for url in sorted(discovered):
            logger.info(f"  -> {url}")

    return sorted(discovered)


def try_alternative_ajax(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    n_days: int = 30,
) -> Optional[pd.DataFrame]:
    """
    Try alternative AJAX endpoints (besides GetChartData).
    Used as a fallback when GetChartData auth fails.
    """
    session = requests.Session()
    session.headers.update(BROWSER_HEADERS)

    try:
        session.get(PIHPS_BASE, timeout=30)
    except Exception:
        pass

    if start_date is None:
        start_date = (datetime.now() - timedelta(days=n_days)).strftime("%Y-%m-%d")
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    # Known candidate endpoints
    endpoints = [
        f"{PIHPS_BASE}/TabelHarga/PasarTradisionalDaerahData",
        f"{PIHPS_BASE}/TabelHarga/PasarTradisionalKomoditasData",
        f"{PIHPS_BASE}/TabelHarga/GetData",
        f"{PIHPS_BASE}/Home/GetHargaData",
    ]

    # Add discovered ones
    discovered = discover_ajax_endpoints(session)
    for ep in discovered:
        full = f"https://www.bi.go.id{ep}" if ep.startswith("/") else ep
        if full not in endpoints:
            endpoints.append(full)

    payloads = [
        {
            "provinsi": PIHPS_PROVINSI_JATIM,
            "tanggalAwal": start_date,
            "tanggalAkhir": end_date,
            "jenisPasar": "1",
        },
        {
            "draw": "1", "start": "0", "length": "1000",
            "provinsi": PIHPS_PROVINSI_JATIM,
            "tanggalAwal": start_date,
            "tanggalAkhir": end_date,
        },
    ]

    ajax_headers = {
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Accept": "application/json, text/javascript, */*; q=0.01",
    }

    for endpoint in endpoints:
        for payload in payloads:
            try:
                resp = session.post(
                    endpoint, data=payload,
                    headers=ajax_headers, timeout=30,
                    allow_redirects=False,
                )
                if resp.status_code != 200:
                    continue

                ct = resp.headers.get("Content-Type", "")

                if "json" in ct or resp.text.strip().startswith(("{", "[")):
                    try:
                        data = resp.json()
                        df = _parse_generic_json(data)
                        if df is not None and not df.empty:
                            logger.info(
                                f"Alt AJAX success: {endpoint} → {len(df)} rows"
                            )
                            return df
                    except json.JSONDecodeError:
                        pass

                if HAS_BS4 and "<table" in resp.text.lower():
                    df = _parse_html_table(resp.text)
                    if df is not None and not df.empty:
                        logger.info(
                            f"Alt AJAX HTML: {endpoint} → {len(df)} rows"
                        )
                        return df

            except Exception as e:
                logger.debug(f"Alt endpoint error: {e}")

            time.sleep(0.3)

    return None


# ============================================================
# APPROACH 3: HTML SCRAPING
# ============================================================
def scrape_pihps_html(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    n_days: int = 30,
) -> Optional[pd.DataFrame]:
    """Scrape PIHPS by loading full pages and parsing HTML tables."""
    if not HAS_BS4:
        logger.warning("BeautifulSoup not available — cannot scrape HTML")
        return None

    session = requests.Session()
    session.headers.update(BROWSER_HEADERS)

    pages = [
        f"{PIHPS_BASE}/TabelHarga/PasarTradisionalDaerah",
        f"{PIHPS_BASE}/TabelHarga/PasarTradisionalKomoditas",
    ]

    for page_url in pages:
        try:
            logger.info(f"Scraping: {page_url}")
            resp = session.get(page_url, timeout=30)
            resp.raise_for_status()

            df = _parse_html_table(resp.text)
            if df is not None and not df.empty:
                logger.info(f"Scraped {len(df)} rows from {page_url}")
                return df

        except Exception as e:
            logger.warning(f"Scraping {page_url} failed: {e}")
        time.sleep(1)

    return None


def _parse_html_table(html: str) -> Optional[pd.DataFrame]:
    """Extract the best data table from HTML."""
    if not HAS_BS4:
        return None

    soup = BeautifulSoup(html, "html.parser")
    tables = soup.find_all("table")
    if not tables:
        return None

    best_table = max(tables, key=lambda t: len(t.find_all("tr")))
    rows = best_table.find_all("tr")
    if len(rows) < 2:
        return None

    header_row = rows[0]
    headers = [c.get_text(strip=True) for c in header_row.find_all(["th", "td"])]
    has_th = len(header_row.find_all("th")) > 0
    data_start = 1 if has_th else 0

    # Handle multi-row headers
    if has_th and len(rows) > 1:
        sub_ths = rows[1].find_all("th")
        if sub_ths:
            headers.extend([th.get_text(strip=True) for th in sub_ths])
            data_start = 2

    data = []
    for row in rows[data_start:]:
        cells = [td.get_text(strip=True) for td in row.find_all("td")]
        if cells and any(c for c in cells):
            data.append(cells)

    if not data:
        return None

    max_cols = max(len(r) for r in data)
    if len(headers) < max_cols:
        headers.extend([f"col_{i}" for i in range(len(headers), max_cols)])
    headers = headers[:max_cols]
    data = [r + [""] * (max_cols - len(r)) for r in data]

    df = pd.DataFrame(data, columns=headers)
    return _normalize_pihps_dataframe(df)


# ============================================================
# APPROACH 4: MANUAL FILE PROCESSING
# ============================================================
def process_manual_file(filepath: str) -> Optional[pd.DataFrame]:
    """
    Process manually downloaded PIHPS data.

    How to get the file:
    1. Go to https://www.bi.go.id/hargapangan/TabelHarga/PasarTradisionalDaerah
    2. Select Provinsi = Jawa Timur, set date range
    3. Click "Lihat Laporan" then "Download"
    4. Run: python collectors/pihps_collector.py --approach manual --file yourfile.csv
    """
    logger.info(f"Processing manual file: {filepath}")

    if not os.path.exists(filepath):
        logger.error(f"File not found: {filepath}")
        return None

    ext = os.path.splitext(filepath)[1].lower()

    try:
        if ext == ".csv":
            df = _read_csv_flexible(filepath)
        elif ext in [".xlsx", ".xls"]:
            df = pd.read_excel(filepath)
        elif ext == ".json":
            with open(filepath) as f:
                data = json.load(f)
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict) and "data" in data:
                df = pd.DataFrame(data["data"])
            else:
                df = pd.DataFrame([data])
        else:
            logger.error(f"Unsupported format: {ext}")
            return None
    except Exception as e:
        logger.error(f"Error reading file: {e}")
        return None

    if df is None or df.empty:
        logger.error("File produced empty DataFrame")
        return None

    logger.info(f"Raw file: {len(df)} rows, columns: {list(df.columns)}")
    return _normalize_pihps_dataframe(df)


def _read_csv_flexible(filepath: str) -> Optional[pd.DataFrame]:
    """Read CSV with auto-detected encoding and delimiter."""
    for enc in ["utf-8", "utf-8-sig", "latin-1", "cp1252", "iso-8859-1"]:
        for sep in [",", ";", "\t", "|"]:
            try:
                df = pd.read_csv(
                    filepath, encoding=enc, sep=sep,
                    on_bad_lines="skip", engine="python"
                )
                if len(df.columns) > 1 and len(df) > 0:
                    return df
            except Exception:
                continue
    return None


# ============================================================
# DATA NORMALIZATION
# ============================================================
def _normalize_pihps_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize raw PIHPS data into consistent schema."""
    if df is None or df.empty:
        return df

    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]

    # Detect wide vs long format
    commodity_kw = [
        "beras", "cabai", "bawang", "daging", "telur",
        "gula", "minyak", "ayam", "sapi", "rawit",
    ]
    commodity_cols = [
        c for c in df.columns
        if any(kw in c for kw in commodity_kw)
    ]

    if len(commodity_cols) >= 3:
        logger.info(f"Wide format detected ({len(commodity_cols)} commodity cols)")
        df = _melt_wide_format(df, commodity_cols)
    else:
        df = _rename_long_format(df)

    # Clean prices
    if "harga_raw" in df.columns:
        df["harga"] = df["harga_raw"].astype(str).apply(clean_price_string)
    elif "harga" in df.columns and df["harga"].dtype == object:
        df["harga"] = df["harga"].astype(str).apply(clean_price_string)

    # Parse dates
    if "tanggal" in df.columns:
        df["tanggal"] = pd.to_datetime(df["tanggal"], errors="coerce", dayfirst=True)

    # Filter invalid prices
    if "harga" in df.columns:
        df["harga"] = pd.to_numeric(df["harga"], errors="coerce")
        df = df[df["harga"].notna() & (df["harga"] > 0)]

    # Metadata
    if "provinsi" not in df.columns:
        df["provinsi"] = "Jawa Timur"
    if "source" not in df.columns:
        df["source"] = "PIHPS_BI"
    df["processed_at"] = datetime.now().isoformat()

    return df


def _melt_wide_format(df, commodity_cols):
    """Convert wide-format to long format."""
    date_col = _find_column(df.columns, ["tanggal", "date", "tgl", "periode"])
    city_col = _find_column(df.columns, ["kota", "kabupaten", "city", "wilayah", "kab/kota"])

    id_vars = [c for c in [date_col, city_col] if c is not None]
    if not id_vars:
        id_vars = [c for c in df.columns if c not in commodity_cols][:2]

    df_long = df.melt(
        id_vars=id_vars, value_vars=commodity_cols,
        var_name="komoditas", value_name="harga_raw",
    )
    rename = {}
    if date_col: rename[date_col] = "tanggal"
    if city_col: rename[city_col] = "kota"
    if rename:
        df_long = df_long.rename(columns=rename)

    df_long["nama_komoditas"] = df_long["komoditas"].apply(
        lambda x: x.replace("_", " ").title()
    )
    return df_long


def _rename_long_format(df):
    """Rename columns in long-format data."""
    rename = {}
    for col in df.columns:
        cl = col.lower()
        if cl in ["tanggal", "date", "tgl", "periode"]:
            rename[col] = "tanggal"
        elif cl in ["kota", "kabupaten", "city", "wilayah", "daerah",
                     "kab_kota_nama"]:
            rename[col] = "kota"
        elif cl in ["komoditas", "commodity", "nama_komoditas", "item"]:
            rename[col] = "komoditas"
        elif cl in ["harga", "price", "nilai", "nominal"]:
            rename[col] = "harga"
        elif cl in ["satuan", "unit", "denomination"]:
            rename[col] = "satuan"
        elif cl in ["kab_kota_id", "kode_bps", "location_id"]:
            rename[col] = "kode_bps"
    return df.rename(columns=rename)


def _find_column(columns, keywords):
    for col in columns:
        for kw in keywords:
            if kw in str(col).lower():
                return col
    return None


def _parse_generic_json(data) -> Optional[pd.DataFrame]:
    """Parse various JSON response formats."""
    if isinstance(data, dict):
        for key in ["data", "result", "items", "rows", "aaData"]:
            if key in data and isinstance(data[key], list) and data[key]:
                return _normalize_pihps_dataframe(pd.DataFrame(data[key]))
    if isinstance(data, list) and data:
        return _normalize_pihps_dataframe(pd.DataFrame(data))
    return None


def _save_records_csv(records: List[Dict], filepath: str):
    """Save list of dicts to CSV."""
    if not records:
        return
    ensure_dir(os.path.dirname(filepath))
    with open(filepath, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)


# ============================================================
# APPROACH 5: REALISTIC SAMPLE DATA
# ============================================================
def generate_sample_pihps_data(
    n_days: int = 90,
    cities: Optional[List[str]] = None,
    commodities: Optional[List[str]] = None,
    seed: int = 42,
) -> pd.DataFrame:
    """Generate realistic sample PIHPS data for development."""
    import numpy as np

    if cities is None:
        cities = (
            PIHPS_JATIM_CITIES
            if PIHPS_JATIM_CITIES
            else list(KAB_KOTA_JATIM.values())[:10]
        )
    if commodities is None:
        commodities = [
            k for k in COMMODITY_TEMPLATES.keys()
            if not k.startswith("minyak_goreng_kemasan")
        ][:13]

    base_prices = {
        "beras_kualitas_bawah_1": 12_500,
        "beras_kualitas_bawah_2": 11_800,
        "beras_kualitas_medium_1": 14_200,
        "beras_kualitas_medium_2": 13_500,
        "beras_kualitas_super_1": 16_000,
        "beras_kualitas_super_2": 15_200,
        "cabai_merah_besar": 55_000,
        "cabai_merah_keriting": 50_000,
        "cabai_rawit_hijau": 45_000,
        "cabai_rawit_merah": 65_000,
        "bawang_merah": 42_000,
        "bawang_putih": 40_000,
        "daging_sapi_kualitas_1": 140_000,
        "daging_sapi_kualitas_2": 125_000,
        "daging_ayam_ras": 36_000,
        "telur_ayam_ras": 29_000,
        "gula_pasir_lokal": 18_500,
        "gula_pasir_premium": 20_500,
        "minyak_goreng_curah": 17_500,
    }
    volatility = {
        "beras_kualitas_bawah_1": 0.015,
        "beras_kualitas_bawah_2": 0.015,
        "beras_kualitas_medium_1": 0.015,
        "beras_kualitas_medium_2": 0.015,
        "beras_kualitas_super_1": 0.02,
        "beras_kualitas_super_2": 0.02,
        "cabai_merah_besar": 0.12,
        "cabai_merah_keriting": 0.12,
        "cabai_rawit_hijau": 0.14,
        "cabai_rawit_merah": 0.15,
        "bawang_merah": 0.10,
        "bawang_putih": 0.06,
        "daging_sapi_kualitas_1": 0.02,
        "daging_sapi_kualitas_2": 0.02,
        "daging_ayam_ras": 0.04,
        "telur_ayam_ras": 0.03,
        "gula_pasir_lokal": 0.015,
        "gula_pasir_premium": 0.015,
        "minyak_goreng_curah": 0.02,
    }
    city_premium = {
        "Kota Surabaya": 1.05, "Kota Malang": 0.98,
        "Kota Kediri": 0.95, "Kab. Jember": 0.93,
        "Kota Madiun": 0.94, "Kota Probolinggo": 0.96,
        "Kota Mojokerto": 0.95, "Kota Blitar": 0.92,
        "Kota Pasuruan": 0.96, "Kota Batu": 0.99,
        # Fallback for PIHPS_JATIM_CITIES (short names)
        "Surabaya": 1.05, "Malang": 0.98, "Kediri": 0.95,
        "Jember": 0.93, "Madiun": 0.94, "Probolinggo": 0.96,
        "Mojokerto": 0.95, "Blitar": 0.92, "Pasuruan": 0.96,
        "Batu": 0.99,
    }

    np.random.seed(seed)
    records = []
    end_dt = datetime.now()

    for day_offset in range(n_days):
        dt = end_dt - timedelta(days=n_days - 1 - day_offset)
        if dt.weekday() >= 5:
            continue  # Skip weekends

        for commodity in commodities:
            bp = base_prices.get(commodity, 30_000)
            vol = volatility.get(commodity, 0.05)
            info = COMMODITY_TEMPLATES.get(commodity, {})

            month = dt.month
            if commodity.startswith("cabai") or commodity.startswith("bawang"):
                seasonal = 1.0 + (0.15 if month in [11, 12, 1, 2] else -0.05)
            else:
                seasonal = 1.0 + 0.02 * (1 if month in [3, 4, 5] else 0)

            trend = 1.0 + 0.0001 * day_offset

            for city in cities:
                premium = city_premium.get(city, 0.95)
                noise = np.random.normal(0, vol * 0.3)
                price = bp * seasonal * trend * premium * (1 + noise)
                price = round(max(price, bp * 0.4), -2)

                records.append({
                    "tanggal": dt.strftime("%Y-%m-%d"),
                    "kota": city,
                    "komoditas": commodity,
                    "nama_komoditas": info.get("nama",
                                               commodity.replace("_", " ").title()),
                    "harga": price,
                    "satuan": info.get("satuan", "Rp/kg"),
                    "jenis_pasar": "tradisional",
                    "provinsi": "Jawa Timur",
                    "source": "SAMPLE_DATA",
                })

    df = pd.DataFrame(records)
    df["tanggal"] = pd.to_datetime(df["tanggal"])

    logger.info(
        f"Sample data: {len(df)} records | "
        f"{len(cities)} cities | {len(commodities)} commodities | "
        f"{df['tanggal'].nunique()} trading days"
    )
    return df


# ============================================================
# MAIN ORCHESTRATOR
# ============================================================
def collect_pihps_data(
    approach: str = "auto",
    manual_file: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    n_days: int = 90,
    use_sample: bool = False,
    commodity_key: str = "beras_kualitas_medium_2",
    all_commodities: bool = False,
    cookie: Optional[str] = None,
    xsrf_token: Optional[str] = None,
) -> Optional[pd.DataFrame]:
    """
    Main entry point for PIHPS data collection.

    Args:
        approach: 'auto' | 'api' | 'scrape' | 'manual' | 'sample'
        manual_file: Path to CSV/Excel file
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        n_days: Days of history (default 90)
        use_sample: Force sample data
        commodity_key: Which commodity to collect
        all_commodities: Collect all commodities with known tempId
        cookie: Browser cookie (override saved)
        xsrf_token: XSRF token (override saved)
    """
    out_dir = os.path.join(DATA_PROCESSED_DIR, "pihps")
    ensure_dir(out_dir)
    raw_dir = os.path.join(DATA_RAW_DIR, "pihps")
    ensure_dir(raw_dir)

    df = None

    # Parse date strings to date objects
    sd = (
        date.fromisoformat(start_date) if start_date
        else date.today() - timedelta(days=n_days)
    )
    ed = date.fromisoformat(end_date) if end_date else date.today()

    # ---- Sample data ----
    if use_sample or approach == "sample":
        logger.info("Generating sample PIHPS data...")
        df = generate_sample_pihps_data(n_days=n_days)

    # ---- Manual file ----
    elif approach == "manual" and manual_file:
        df = process_manual_file(manual_file)

    # ---- API (GetChartData primary) ----
    elif approach in ["api", "auto"]:
        logger.info("=" * 60)
        logger.info("STEP 1: GetChartData API (primary method)")
        logger.info("=" * 60)

        if all_commodities:
            df = collect_via_getchart_multi(
                start_date=sd, end_date=ed,
                cookie=cookie, xsrf_token=xsrf_token,
            )
        else:
            df = collect_via_getchart(
                commodity_key=commodity_key,
                start_date=sd, end_date=ed,
                cookie=cookie, xsrf_token=xsrf_token,
            )

        # Fallback: try alternative AJAX endpoints
        if (df is None or df.empty) and approach == "auto":
            logger.info("=" * 60)
            logger.info("STEP 2: Alternative AJAX endpoints")
            logger.info("=" * 60)
            df = try_alternative_ajax(
                start_date=start_date, end_date=end_date, n_days=n_days,
            )

        # Fallback: HTML scraping
        if (df is None or df.empty) and approach == "auto":
            logger.info("=" * 60)
            logger.info("STEP 3: HTML scraping")
            logger.info("=" * 60)
            df = scrape_pihps_html(
                start_date=start_date, end_date=end_date, n_days=n_days,
            )

        # Final fallback: sample data
        if (df is None or df.empty) and approach == "auto":
            logger.warning("=" * 60)
            logger.warning(
                "All methods failed. Generating sample data for development."
            )
            logger.warning(
                "To get REAL data, either:\n"
                "  a) Update Cookie/Xsrf-Token (see --refresh-tokens)\n"
                "  b) Download from https://www.bi.go.id/hargapangan/ "
                "and use --approach manual --file <file>\n"
                "  c) Discover more tempIds for other commodities"
            )
            logger.warning("=" * 60)
            df = generate_sample_pihps_data(n_days=n_days)

    # ---- Scrape only ----
    elif approach == "scrape":
        df = scrape_pihps_html(start_date, end_date, n_days)

    # ---- Save output ----
    if df is not None and not df.empty:
        out_path = os.path.join(out_dir, "harga_pangan_jatim.csv")
        df.to_csv(out_path, index=False)
        logger.info(f"Saved: {out_path} ({len(df)} rows)")

        stats = {
            "total_rows": len(df),
            "source": (
                df["source"].value_counts().to_dict()
                if "source" in df.columns else {}
            ),
            "date_range": {
                "min": str(df["tanggal"].min()) if "tanggal" in df.columns else None,
                "max": str(df["tanggal"].max()) if "tanggal" in df.columns else None,
            },
            "cities": (
                sorted(df["kota"].unique().tolist())
                if "kota" in df.columns else []
            ),
            "commodities": (
                sorted(df["komoditas"].unique().tolist())
                if "komoditas" in df.columns else []
            ),
            "saved_at": datetime.now().isoformat(),
        }
        save_json(stats, os.path.join(out_dir, "pihps_stats.json"))
    else:
        logger.error("No PIHPS data collected!")

    return df


# ============================================================
# CLI
# ============================================================
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="AgriFlow WP0 — Collect PIHPS Food Price Data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Development (sample data)
  python collectors/pihps_collector.py --sample

  # Live API — single commodity (requires valid tokens)
  python collectors/pihps_collector.py --approach api
  python collectors/pihps_collector.py --approach api --days 30

  # Live API — all commodities with known tempId
  python collectors/pihps_collector.py --approach api --all-commodities

  # Auto (tries API → scrape → sample fallback)
  python collectors/pihps_collector.py --approach auto

  # Manual file
  python collectors/pihps_collector.py --approach manual --file download.csv

  # Refresh auth tokens
  python collectors/pihps_collector.py --refresh-tokens

How to get auth tokens:
  1. Open https://www.bi.go.id/hargapangan/ in Chrome
  2. Open DevTools (F12) → Network → XHR filter
  3. Interact with the chart (select commodity)
  4. Find GetChartData request → copy Cookie & Xsrf-Token
  5. Save to .pihps_tokens.json or use --refresh-tokens
        """
    )
    parser.add_argument(
        "--approach",
        choices=["api", "scrape", "manual", "sample", "auto"],
        default="auto",
    )
    parser.add_argument("--file", type=str, default=None)
    parser.add_argument("--start", type=str, default=None)
    parser.add_argument("--end", type=str, default=None)
    parser.add_argument("--days", type=int, default=90)
    parser.add_argument("--sample", action="store_true")
    parser.add_argument(
        "--commodity",
        type=str, default="beras_kualitas_medium_2",
        help="Commodity key (default: beras_kualitas_medium_2)",
    )
    parser.add_argument(
        "--all-commodities", action="store_true",
        help="Collect all commodities with known tempId",
    )
    parser.add_argument(
        "--refresh-tokens", action="store_true",
        help="Attempt to obtain fresh tokens from PIHPS website",
    )
    parser.add_argument(
        "--cookie", type=str, default=None,
        help="Browser Cookie header value",
    )
    parser.add_argument(
        "--xsrf", type=str, default=None,
        help="Xsrf-Token header value",
    )

    args = parser.parse_args()

    # Handle --refresh-tokens
    if args.refresh_tokens:
        print("Attempting to obtain fresh tokens...")
        tokens = obtain_fresh_tokens()
        if tokens:
            print(f"Tokens saved to {TOKEN_FILE}")
            print(f"Cookie: {tokens.get('Cookie', '')[:60]}...")
            print(f"Xsrf-Token: {tokens.get('Xsrf-Token', '')[:40]}...")
        else:
            print("Could not obtain tokens automatically.")
            print("Please extract them manually from browser DevTools.")
        sys.exit(0)

    # Run collection
    df = collect_pihps_data(
        approach=args.approach,
        manual_file=args.file,
        start_date=args.start,
        end_date=args.end,
        n_days=args.days,
        use_sample=args.sample,
        commodity_key=args.commodity,
        all_commodities=args.all_commodities,
        cookie=args.cookie,
        xsrf_token=args.xsrf,
    )

    if df is not None and not df.empty:
        print(f"\n{'='*60}")
        print(f"  PIHPS Data Collection Complete")
        print(f"{'='*60}")
        print(f"  Total rows:   {len(df)}")
        if "tanggal" in df.columns:
            print(f"  Date range:   {df['tanggal'].min()} → {df['tanggal'].max()}")
            print(f"  Trading days: {df['tanggal'].nunique()}")
        if "kota" in df.columns:
            print(f"  Cities:       {df['kota'].nunique()}")
        if "komoditas" in df.columns:
            print(f"  Commodities:  {df['komoditas'].nunique()}")
        if "source" in df.columns:
            print(f"  Source:       {df['source'].value_counts().to_dict()}")

        # Price preview
        if "harga" in df.columns and "komoditas" in df.columns:
            print(f"\n  Price summary (top 5 commodities):")
            for comm in sorted(df["komoditas"].unique())[:5]:
                sub = df[df["komoditas"] == comm]
                print(
                    f"    {comm}: avg Rp {sub['harga'].mean():,.0f} "
                    f"({sub['harga'].min():,.0f}–{sub['harga'].max():,.0f})"
                )
        print(f"{'='*60}\n")
    else:
        print("\nNo data collected. Check logs for details.")
