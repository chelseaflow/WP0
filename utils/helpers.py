"""
AgriFlow WP0 — Utility Functions
"""

import math
import logging
import os
import json
from datetime import datetime
from typing import Optional


def setup_logger(name: str, level=logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:
        handler = logging.StreamHandler()
        fmt = logging.Formatter(
            "[%(asctime)s] %(name)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        handler.setFormatter(fmt)
        logger.addHandler(handler)
    return logger


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def road_distance_km(lat1, lon1, lat2, lon2, factor=1.3):
    return haversine_km(lat1, lon1, lat2, lon2) * factor


def validate_price(price, commodity=""):
    if price is None or price <= 0:
        return False
    if price > 500_000:
        return False
    return True


def clean_price_string(price_str: str) -> Optional[float]:
    if not price_str or price_str.strip() in ["-", "", "N/A", "n/a"]:
        return None
    s = price_str.strip()
    s = s.replace("Rp", "").replace("rp", "").replace(" ", "")
    if "." in s and "," not in s:
        s = s.replace(".", "")
    elif "," in s:
        s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def normalize_kabupaten_name(name: str) -> str:
    s = name.strip().lower()
    for prefix in ["kabupaten ", "kab. ", "kab ", "kota ", "kt. "]:
        if s.startswith(prefix):
            s = s[len(prefix):]
    replacements = {
        "sby": "surabaya", "mlg": "malang", "kdiri": "kediri",
        "jbr": "jember", "bwi": "banyuwangi",
    }
    return replacements.get(s, s).strip()


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def save_json(data, filepath: str):
    ensure_dir(os.path.dirname(filepath))
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)


def load_json(filepath: str):
    with open(filepath, "r", encoding="utf-8") as f:
        return json.load(f)


def save_checkpoint(collector_name: str, checkpoint_data: dict, base_dir: str):
    filepath = os.path.join(base_dir, f".checkpoint_{collector_name}.json")
    checkpoint_data["saved_at"] = datetime.now().isoformat()
    save_json(checkpoint_data, filepath)


def load_checkpoint(collector_name: str, base_dir: str) -> Optional[dict]:
    filepath = os.path.join(base_dir, f".checkpoint_{collector_name}.json")
    if os.path.exists(filepath):
        return load_json(filepath)
    return None
