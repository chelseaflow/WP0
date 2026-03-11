"""
AgriFlow WP0 — Configuration & Constants
==========================================
Master config for all data collectors.
Covers: 38 kabupaten/kota Jawa Timur, commodity codes, API endpoints.
"""

import os
from datetime import datetime

# ============================================================
# PROJECT PATHS — resolves relative to WP0 root
# ============================================================
# This file lives at: WP0/config/settings.py
# WP0 root = parent of config/
WP0_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_RAW_DIR = os.path.join(WP0_ROOT, "data", "raw")
DATA_PROCESSED_DIR = os.path.join(WP0_ROOT, "data", "processed")

# ============================================================
# 38 KABUPATEN/KOTA JAWA TIMUR
# with BPS domain codes, latitude/longitude (centroid)
# ============================================================
JATIM_KABUPATEN = [
    {"kode_bps": "3501", "nama": "Kabupaten Pacitan", "lat": -8.1953, "lon": 111.1012},
    {"kode_bps": "3502", "nama": "Kabupaten Ponorogo", "lat": -7.8654, "lon": 111.4595},
    {"kode_bps": "3503", "nama": "Kabupaten Trenggalek", "lat": -8.0505, "lon": 111.7082},
    {"kode_bps": "3504", "nama": "Kabupaten Tulungagung", "lat": -8.0654, "lon": 111.9024},
    {"kode_bps": "3505", "nama": "Kabupaten Blitar", "lat": -8.0994, "lon": 112.1600},
    {"kode_bps": "3506", "nama": "Kabupaten Kediri", "lat": -7.8489, "lon": 112.0108},
    {"kode_bps": "3507", "nama": "Kabupaten Malang", "lat": -8.0515, "lon": 112.6409},
    {"kode_bps": "3508", "nama": "Kabupaten Lumajang", "lat": -8.1339, "lon": 113.2246},
    {"kode_bps": "3509", "nama": "Kabupaten Jember", "lat": -8.1725, "lon": 113.6883},
    {"kode_bps": "3510", "nama": "Kabupaten Banyuwangi", "lat": -8.2193, "lon": 114.3520},
    {"kode_bps": "3511", "nama": "Kabupaten Bondowoso", "lat": -7.9136, "lon": 113.8214},
    {"kode_bps": "3512", "nama": "Kabupaten Situbondo", "lat": -7.7067, "lon": 114.0049},
    {"kode_bps": "3513", "nama": "Kabupaten Probolinggo", "lat": -7.8662, "lon": 113.2158},
    {"kode_bps": "3514", "nama": "Kabupaten Pasuruan", "lat": -7.6469, "lon": 112.9075},
    {"kode_bps": "3515", "nama": "Kabupaten Sidoarjo", "lat": -7.4518, "lon": 112.7181},
    {"kode_bps": "3516", "nama": "Kabupaten Mojokerto", "lat": -7.4726, "lon": 112.4341},
    {"kode_bps": "3517", "nama": "Kabupaten Jombang", "lat": -7.5467, "lon": 112.2354},
    {"kode_bps": "3518", "nama": "Kabupaten Nganjuk", "lat": -7.6051, "lon": 111.9040},
    {"kode_bps": "3519", "nama": "Kabupaten Madiun", "lat": -7.5438, "lon": 111.5247},
    {"kode_bps": "3520", "nama": "Kabupaten Magetan", "lat": -7.6462, "lon": 111.3592},
    {"kode_bps": "3521", "nama": "Kabupaten Ngawi", "lat": -7.4047, "lon": 111.4475},
    {"kode_bps": "3522", "nama": "Kabupaten Bojonegoro", "lat": -7.1506, "lon": 111.8802},
    {"kode_bps": "3523", "nama": "Kabupaten Tuban", "lat": -6.9667, "lon": 112.0500},
    {"kode_bps": "3524", "nama": "Kabupaten Lamongan", "lat": -7.1197, "lon": 112.4178},
    {"kode_bps": "3525", "nama": "Kabupaten Gresik", "lat": -7.1641, "lon": 112.6508},
    {"kode_bps": "3526", "nama": "Kabupaten Bangkalan", "lat": -7.0456, "lon": 112.9352},
    {"kode_bps": "3527", "nama": "Kabupaten Sampang", "lat": -7.0486, "lon": 113.2390},
    {"kode_bps": "3528", "nama": "Kabupaten Pamekasan", "lat": -7.1576, "lon": 113.4741},
    {"kode_bps": "3529", "nama": "Kabupaten Sumenep", "lat": -7.0167, "lon": 113.8667},
    {"kode_bps": "3571", "nama": "Kota Kediri", "lat": -7.8164, "lon": 112.0180},
    {"kode_bps": "3572", "nama": "Kota Blitar", "lat": -8.0984, "lon": 112.1683},
    {"kode_bps": "3573", "nama": "Kota Malang", "lat": -7.9786, "lon": 112.6305},
    {"kode_bps": "3574", "nama": "Kota Probolinggo", "lat": -7.7543, "lon": 113.2159},
    {"kode_bps": "3575", "nama": "Kota Pasuruan", "lat": -7.6455, "lon": 112.9077},
    {"kode_bps": "3576", "nama": "Kota Mojokerto", "lat": -7.4704, "lon": 112.4380},
    {"kode_bps": "3577", "nama": "Kota Madiun", "lat": -7.6298, "lon": 111.5237},
    {"kode_bps": "3578", "nama": "Kota Surabaya", "lat": -7.2575, "lon": 112.7521},
    {"kode_bps": "3579", "nama": "Kota Batu", "lat": -7.8672, "lon": 112.5261},
]

BPS_JATIM_DOMAIN = "35"

# ============================================================
# PIHPS BI — Commodity Codes
# ============================================================
PIHPS_COMMODITIES = {
    "beras_premium": {"nama": "Beras Premium", "kategori": "beras"},
    "beras_medium": {"nama": "Beras Medium", "kategori": "beras"},
    "cabai_merah_besar": {"nama": "Cabai Merah Besar", "kategori": "cabai"},
    "cabai_merah_keriting": {"nama": "Cabai Merah Keriting", "kategori": "cabai"},
    "cabai_rawit_merah": {"nama": "Cabai Rawit Merah", "kategori": "cabai"},
    "cabai_rawit_hijau": {"nama": "Cabai Rawit Hijau", "kategori": "cabai"},
    "bawang_merah": {"nama": "Bawang Merah", "kategori": "bawang"},
    "bawang_putih": {"nama": "Bawang Putih", "kategori": "bawang"},
    "daging_sapi_murni": {"nama": "Daging Sapi Murni", "kategori": "daging"},
    "daging_sapi_has_luar": {"nama": "Daging Sapi Has Luar", "kategori": "daging"},
    "daging_ayam_ras": {"nama": "Daging Ayam Ras", "kategori": "daging"},
    "telur_ayam_ras": {"nama": "Telur Ayam Ras", "kategori": "telur"},
    "gula_pasir_lokal": {"nama": "Gula Pasir Lokal", "kategori": "gula"},
    "gula_pasir_impor": {"nama": "Gula Pasir Impor", "kategori": "gula"},
    "minyak_goreng_curah": {"nama": "Minyak Goreng Curah", "kategori": "minyak_goreng"},
    "minyak_goreng_kemasan_1": {"nama": "Minyak Goreng Kemasan Bermerk 1", "kategori": "minyak_goreng"},
    "minyak_goreng_kemasan_2": {"nama": "Minyak Goreng Kemasan Bermerk 2", "kategori": "minyak_goreng"},
}

PRIORITY_COMMODITIES = [
    "cabai_merah_besar", "cabai_merah_keriting", "cabai_rawit_merah",
    "bawang_merah", "bawang_putih", "beras_premium", "beras_medium",
    "telur_ayam_ras", "daging_ayam_ras",
]

# ============================================================
# OPEN-METEO API CONFIG
# ============================================================
OPEN_METEO_BASE_URL = "https://api.open-meteo.com/v1"
OPEN_METEO_DAILY_PARAMS = [
    "temperature_2m_mean",
    "precipitation_sum",
    "relative_humidity_2m_mean",
    "wind_speed_10m_max",
]
OPEN_METEO_TIMEZONE = "Asia/Jakarta"

# ============================================================
# BPS API CONFIG
# Register at: https://webapi.bps.go.id/developer/
# ============================================================
BPS_API_BASE_URL = "https://webapi.bps.go.id/v1/api"
BPS_API_KEY = os.environ.get("BPS_API_KEY", "YOUR_BPS_API_KEY_HERE")

BPS_PRODUCTION_VARS = {
    "padi": None,
    "jagung": None,
    "kedelai": None,
    "cabai": None,
    "bawang_merah": None,
}

# ============================================================
# PIHPS BI SCRAPER CONFIG
# ============================================================
PIHPS_BASE_URL = "https://www.bi.go.id/hargapangan"
PIHPS_JATIM_CITIES = [
    "Surabaya", "Malang", "Kediri", "Jember", "Madiun",
    "Probolinggo", "Mojokerto", "Blitar", "Pasuruan", "Batu",
]

# ============================================================
# DISTANCE CALCULATION CONFIG
# ============================================================
ROAD_FACTOR = 1.3
MAX_DISTANCE_KM = 300

# ============================================================
# DATA FRESHNESS
# ============================================================
PRICE_DATA_MAX_AGE_HOURS = 24
WEATHER_DATA_MAX_AGE_HOURS = 6
PRODUCTION_DATA_MAX_AGE_DAYS = 365

# ============================================================
# KONSUMSI PER KAPITA (kg/tahun) — from BPS/Susenas
# ============================================================
KONSUMSI_PER_KAPITA_KG = {
    "beras": 94.0,
    "cabai": 2.5,
    "bawang_merah": 2.8,
    "bawang_putih": 1.5,
    "daging_sapi": 2.6,
    "daging_ayam": 12.0,
    "telur_ayam": 10.5,
    "gula_pasir": 8.0,
    "minyak_goreng": 10.5,
}

# Populasi per kabupaten Jatim (est. 2024, from BPS)
POPULASI_JATIM_2024 = {
    "3501": 596_000, "3502": 870_000, "3503": 706_000, "3504": 1_060_000,
    "3505": 1_170_000, "3506": 1_580_000, "3507": 2_630_000, "3508": 1_040_000,
    "3509": 2_440_000, "3510": 1_630_000, "3511": 790_000, "3512": 690_000,
    "3513": 1_160_000, "3514": 1_610_000, "3515": 2_260_000, "3516": 1_120_000,
    "3517": 1_280_000, "3518": 1_060_000, "3519": 690_000, "3520": 640_000,
    "3521": 840_000, "3522": 1_260_000, "3523": 1_180_000, "3524": 1_200_000,
    "3525": 1_320_000, "3526": 980_000, "3527": 960_000, "3528": 870_000,
    "3529": 1_110_000, "3571": 290_000, "3572": 140_000, "3573": 870_000,
    "3574": 240_000, "3575": 200_000, "3576": 135_000, "3577": 180_000,
    "3578": 2_870_000, "3579": 210_000,
}
