"""
AgriFlow WP0 — Distance Matrix Validator
==========================================
Cross-checks OSRM distance results against known real-world distances.

These reference distances are from Google Maps / public sources and serve
as ground truth to validate that OSRM routing is correct.

Usage:
    python validate_distances.py
    
Run this AFTER running the distance collector:
    python run_all.py --only distance
    python validate_distances.py
"""

import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pandas as pd
from config.settings import DATA_PROCESSED_DIR

# ============================================================
# GROUND TRUTH — known distances from Google Maps / public data
# Format: (from_kode, to_kode, from_name, to_name, 
#          expected_road_km, tolerance_pct, source)
# ============================================================
KNOWN_DISTANCES = [
    # Well-known routes with verified distances
    ("3578", "3573", "Surabaya", "Kota Malang",
     92, 15, "Google Maps ~90-100 km via tol"),

    ("3578", "3526", "Surabaya", "Bangkalan (Madura)",
     42, 20, "Google Maps ~40-45 km via Suramadu"),

    ("3578", "3515", "Surabaya", "Sidoarjo",
     25, 20, "Google Maps ~22-28 km"),

    ("3578", "3509", "Surabaya", "Jember",
     200, 15, "Google Maps ~195-210 km"),

    ("3578", "3510", "Surabaya", "Banyuwangi",
     290, 15, "Google Maps ~280-300 km"),

    ("3573", "3579", "Kota Malang", "Kota Batu",
     20, 25, "Google Maps ~18-22 km"),

    ("3578", "3524", "Surabaya", "Lamongan",
     47, 20, "Google Maps ~42-52 km"),

    ("3578", "3577", "Surabaya", "Kota Madiun",
     170, 15, "Google Maps ~165-180 km"),

    ("3501", "3510", "Pacitan", "Banyuwangi",
     350, 20, "Google Maps ~330-380 km, across Jatim"),

    ("3573", "3509", "Kota Malang", "Jember",
     140, 15, "Google Maps ~130-150 km"),
]


def validate():
    # Load distance matrix
    csv_path = os.path.join(DATA_PROCESSED_DIR, "distance", "distance_matrix_jatim.csv")
    if not os.path.exists(csv_path):
        print("ERROR: distance_matrix_jatim.csv not found!")
        print("Run first: python run_all.py --only distance")
        return

    df = pd.read_csv(csv_path)
    # Ensure kode columns are strings for comparison
    df["from_kode"] = df["from_kode"].astype(str)
    df["to_kode"] = df["to_kode"].astype(str)
    method = df["method"].iloc[0]
    
    print(f"{'='*75}")
    print(f"  DISTANCE MATRIX VALIDATION")
    print(f"  Method used: {method}")
    print(f"  Total pairs: {len(df)}")
    print(f"{'='*75}\n")

    results = []
    all_pass = True

    for from_k, to_k, from_n, to_n, expected_km, tol_pct, source in KNOWN_DISTANCES:
        row = df[(df["from_kode"] == from_k) & (df["to_kode"] == to_k)]
        
        if row.empty:
            print(f"  [??] {from_n} -> {to_n}: NOT FOUND in matrix")
            continue
        
        actual_km = row.iloc[0]["road_km"]
        straight_km = row.iloc[0]["straight_km"]
        travel_min = row.iloc[0]["travel_minutes"]
        
        # Calculate deviation
        deviation_pct = abs(actual_km - expected_km) / expected_km * 100
        passed = deviation_pct <= tol_pct
        
        # Road factor (how much longer than straight line)
        road_factor = actual_km / straight_km if straight_km > 0 else 0
        
        status = "OK" if passed else "WARN"
        if not passed:
            all_pass = False
        
        print(f"  [{status:4s}] {from_n:12s} -> {to_n:18s} | "
              f"Expected: {expected_km:5.0f} km | Got: {actual_km:6.1f} km | "
              f"Deviation: {deviation_pct:4.1f}% | "
              f"Road factor: {road_factor:.2f}x")
        
        results.append({
            "route": f"{from_n} -> {to_n}",
            "expected_km": expected_km,
            "actual_km": actual_km,
            "straight_km": straight_km,
            "road_factor": round(road_factor, 2),
            "deviation_pct": round(deviation_pct, 1),
            "travel_min": travel_min,
            "passed": passed,
            "source": source,
        })

    # Summary
    n_pass = sum(1 for r in results if r["passed"])
    n_total = len(results)
    avg_deviation = sum(r["deviation_pct"] for r in results) / n_total if n_total > 0 else 0
    avg_road_factor = sum(r["road_factor"] for r in results) / n_total if n_total > 0 else 0

    print(f"\n{'='*75}")
    print(f"  SUMMARY")
    print(f"  Passed: {n_pass}/{n_total} routes within tolerance")
    print(f"  Average deviation: {avg_deviation:.1f}%")
    print(f"  Average road factor: {avg_road_factor:.2f}x (straight line → road)")
    print(f"{'='*75}")

    if method == "haversine":
        print(f"\n  NOTE: You are using Haversine approximation.")
        print(f"  For more accurate results, run with OSRM:")
        print(f"    python run_all.py --only distance")
        print(f"  (requires internet to reach router.project-osrm.org)")
    
    if all_pass:
        print(f"\n  [OK] All routes within expected tolerance. Data is VALID.")
    else:
        print(f"\n  [WARN] Some routes deviate beyond tolerance.")
        print(f"  Check if OSRM server has up-to-date Indonesia road data.")
        print(f"  Small deviations are normal — OSRM uses centroid coordinates,")
        print(f"  while Google Maps uses specific addresses.")
    
    # Save validation report
    report_df = pd.DataFrame(results)
    report_path = os.path.join(DATA_PROCESSED_DIR, "distance", "validation_report.csv")
    report_df.to_csv(report_path, index=False)
    print(f"\n  Report saved: {report_path}")


if __name__ == "__main__":
    validate()