"""
AgriFlow WP0 — Master Data Collection Runner
===============================================
Usage:
    python run_all.py                    # Dev mode (sample data)
    python run_all.py --live             # Live API calls
    python run_all.py --only weather     # Only weather
    python run_all.py --only distance    # Only distance matrix
    python run_all.py --only pihps --sample
    python run_all.py --report
"""

import os
import sys
import time
import json
import argparse
from datetime import datetime

# ============================================================
# CRITICAL: Fix Python path BEFORE any local imports
# This ensures 'config', 'collectors', 'utils' are findable
# regardless of where you run this script from.
# ============================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

from config.settings import DATA_RAW_DIR, DATA_PROCESSED_DIR
from utils.helpers import setup_logger, ensure_dir, save_json

logger = setup_logger("agriflow_wp0")


def run_weather(live=False, **kwargs):
    from collectors.open_meteo_collector import collect_all_jatim_weather
    logger.info("=" * 60)
    logger.info("STEP 1/4: WEATHER DATA (Open-Meteo)")
    logger.info("=" * 60)
    df = collect_all_jatim_weather(delay_seconds=0.3)
    return {"rows": len(df) if df is not None and not df.empty else 0,
            "status": "OK" if df is not None and not df.empty else "FAILED"}


def run_pihps(live=False, **kwargs):
    from collectors.pihps_collector import collect_pihps_data
    logger.info("=" * 60)
    logger.info("STEP 2/4: FOOD PRICES (PIHPS Bank Indonesia)")
    logger.info("=" * 60)
    if live:
        df = collect_pihps_data(approach="auto")
    else:
        df = collect_pihps_data(use_sample=True)
    return {"rows": len(df) if df is not None and not df.empty else 0,
            "status": "OK" if df is not None and not df.empty else "FAILED"}


def run_bps(live=False, api_key=None, **kwargs):
    from collectors.bps_collector import collect_bps_data
    logger.info("=" * 60)
    logger.info("STEP 3/4: PRODUCTION DATA (BPS Jawa Timur)")
    logger.info("=" * 60)
    if live and api_key:
        results = collect_bps_data(api_key=api_key)
    else:
        results = collect_bps_data(use_sample=True)
    total = sum(len(df) for df in results.values())
    return {"rows": total, "tables": list(results.keys()), "status": "OK"}


def run_distance(**kwargs):
    from collectors.distance_collector import collect_distance_data
    logger.info("=" * 60)
    logger.info("STEP 4/4: DISTANCE MATRIX")
    logger.info("=" * 60)
    osrm_url = kwargs.get("osrm_url")
    if osrm_url:
        method = "osrm"
    else:
        method = "auto"  # tries OSRM public server first, fallback to haversine
    df = collect_distance_data(method=method, osrm_url=osrm_url)
    return {"rows": len(df), "method": df["method"].iloc[0] if not df.empty else method, "status": "OK"}


def generate_report(results):
    report = {"collected_at": datetime.now().isoformat(), "results": results, "data_files": []}
    for root, dirs, files in os.walk(DATA_PROCESSED_DIR):
        for f in files:
            fp = os.path.join(root, f)
            report["data_files"].append({"path": fp, "size_kb": round(os.path.getsize(fp) / 1024, 1)})
    save_json(report, os.path.join(DATA_PROCESSED_DIR, "COLLECTION_REPORT.json"))
    return report


def print_report(report):
    print(f"\n{'='*70}")
    print(f"  AGRIFLOW WP0 — DATA COLLECTION REPORT")
    print(f"  Generated: {report['collected_at']}")
    print(f"{'='*70}\n")
    for name, result in report["results"].items():
        icon = "OK" if result.get("status") == "OK" else "FAIL"
        print(f"  [{icon}] {name.upper()}: {result.get('rows', 0)} rows")
    print(f"\n  Generated Files:")
    total = 0
    for f in report.get("data_files", []):
        print(f"     {f['path']} ({f['size_kb']:.1f} KB)")
        total += f["size_kb"]
    print(f"\n  Total: {total:.1f} KB")
    print(f"\n{'='*70}")
    print(f"  NEXT STEPS:")
    print(f"  -> WP1: PostgreSQL schema + FastAPI endpoints")
    print(f"  -> WP2: ML model training (XGBoost + Prophet)")
    print(f"  -> WP3: Dashboard visualization (Leaflet + Recharts)")
    print(f"{'='*70}\n")


def main():
    parser = argparse.ArgumentParser(description="AgriFlow WP0 — Data Collection")
    parser.add_argument("--live", action="store_true", help="Use live API calls")
    parser.add_argument("--sample", action="store_true", help="Force sample data")
    parser.add_argument("--only", choices=["weather", "pihps", "bps", "distance"])
    parser.add_argument("--bps-key", type=str, default=None)
    parser.add_argument("--osrm", type=str, default=None)
    parser.add_argument("--report", action="store_true", help="Show report only")
    args = parser.parse_args()

    ensure_dir(DATA_RAW_DIR)
    ensure_dir(DATA_PROCESSED_DIR)

    if args.report:
        rp = os.path.join(DATA_PROCESSED_DIR, "COLLECTION_REPORT.json")
        if os.path.exists(rp):
            with open(rp) as f:
                print_report(json.load(f))
        else:
            print("No report found. Run collection first.")
        return

    start_time = time.time()
    mode = "LIVE" if args.live else "DEV (sample data)"
    logger.info(f"AgriFlow WP0 — Data Collection Starting... Mode: {mode}")

    collectors = {"weather": run_weather, "pihps": run_pihps,
                  "bps": run_bps, "distance": run_distance}
    if args.only:
        collectors = {args.only: collectors[args.only]}

    results = {}
    for name, func in collectors.items():
        try:
            results[name] = func(live=args.live, api_key=args.bps_key, osrm_url=args.osrm)
        except Exception as e:
            logger.error(f"'{name}' failed: {e}", exc_info=True)
            results[name] = {"status": "ERROR", "error": str(e)}

    logger.info(f"Total time: {time.time() - start_time:.1f}s")
    print_report(generate_report(results))


if __name__ == "__main__":
    main()