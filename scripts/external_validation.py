#!/usr/bin/env python3
"""
External Validation: State Population & Enrollment Checks (Refined)

**AUDIT FIXES APPLIED:**
- Replaced circular validation (dataset mean) with hardcoded MACPAC National Average.
- Added explicit MACPAC benchmark ($8,825/enrollee).
"""

import sys
from pathlib import Path

# Ensure project root is on the path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import polars as pl
import time
from scripts.lib.data import (
    load_medicaid, load_npi,
    OUTPUT_DIR, get_mem_mb, track,
)

# ---------------------------------------------------------------------------
# Reference Data (2023 Estimates)
# Sources: Census Bureau, KFF (Kaiser Family Foundation)
# ---------------------------------------------------------------------------
STATE_REF_DATA = {
    "CA": {"pop": 38965000, "medicaid_enrollees": 13300000},
    "TX": {"pop": 30500000, "medicaid_enrollees": 5900000},
    "FL": {"pop": 22600000, "medicaid_enrollees": 4200000},
    "NY": {"pop": 19570000, "medicaid_enrollees": 7600000},
    "PA": {"pop": 12960000, "medicaid_enrollees": 3600000},
    "IL": {"pop": 12540000, "medicaid_enrollees": 3700000},
    "OH": {"pop": 11780000, "medicaid_enrollees": 3400000},
    "GA": {"pop": 11020000, "medicaid_enrollees": 2900000},
    "NC": {"pop": 10830000, "medicaid_enrollees": 2900000},
    "MI": {"pop": 10030000, "medicaid_enrollees": 3000000},
    "NJ": {"pop": 9290000,  "medicaid_enrollees": 2200000},
    "VA": {"pop": 8710000,  "medicaid_enrollees": 2100000},
    "WA": {"pop": 7810000,  "medicaid_enrollees": 2000000},
    "AZ": {"pop": 7430000,  "medicaid_enrollees": 2400000},
    "MA": {"pop": 7000000,  "medicaid_enrollees": 1800000},
    "TN": {"pop": 7120000,  "medicaid_enrollees": 1700000},
    "IN": {"pop": 6860000,  "medicaid_enrollees": 2100000},
    "MO": {"pop": 6190000,  "medicaid_enrollees": 1400000},
    "MD": {"pop": 6180000,  "medicaid_enrollees": 1700000},
    "WI": {"pop": 5910000,  "medicaid_enrollees": 1600000},
    "CO": {"pop": 5870000,  "medicaid_enrollees": 1600000},
    "MN": {"pop": 5730000,  "medicaid_enrollees": 1400000},
    "SC": {"pop": 5370000,  "medicaid_enrollees": 1300000},
    "AL": {"pop": 5100000,  "medicaid_enrollees": 1200000},
    "LA": {"pop": 4570000,  "medicaid_enrollees": 2000000},
    "KY": {"pop": 4520000,  "medicaid_enrollees": 1600000},
    "OR": {"pop": 4230000,  "medicaid_enrollees": 1400000},
    "OK": {"pop": 4050000,  "medicaid_enrollees": 1200000},
    "CT": {"pop": 3610000,  "medicaid_enrollees": 1200000},
    "UT": {"pop": 3410000,  "medicaid_enrollees": 500000},
    "IA": {"pop": 3200000,  "medicaid_enrollees": 800000},
    "NV": {"pop": 3190000,  "medicaid_enrollees": 900000},
    "AR": {"pop": 3060000,  "medicaid_enrollees": 1100000},
    "MS": {"pop": 2930000,  "medicaid_enrollees": 800000},
    "KS": {"pop": 2930000,  "medicaid_enrollees": 500000},
    "NM": {"pop": 2110000,  "medicaid_enrollees": 900000},
    "NE": {"pop": 1970000,  "medicaid_enrollees": 400000},
    "ID": {"pop": 1960000,  "medicaid_enrollees": 450000},
    "WV": {"pop": 1770000,  "medicaid_enrollees": 650000},
    "HI": {"pop": 1430000,  "medicaid_enrollees": 450000},
    "NH": {"pop": 1400000,  "medicaid_enrollees": 250000},
    "ME": {"pop": 1390000,  "medicaid_enrollees": 350000},
    "MT": {"pop": 1130000,  "medicaid_enrollees": 300000},
    "RI": {"pop": 1090000,  "medicaid_enrollees": 350000},
    "DE": {"pop": 1030000,  "medicaid_enrollees": 300000},
    "SD": {"pop": 919000,   "medicaid_enrollees": 150000},
    "ND": {"pop": 783000,   "medicaid_enrollees": 130000},
    "AK": {"pop": 733000,   "medicaid_enrollees": 260000},
    "DC": {"pop": 678000,   "medicaid_enrollees": 300000},
    "VT": {"pop": 647000,   "medicaid_enrollees": 200000},
    "WY": {"pop": 584000,   "medicaid_enrollees": 80000},
}

# MACPAC 2023 National Average Spending per Enrollee (Full Benefit)
# Source: MACPAC MACStats
NATIONAL_AVG_COST_PER_ENROLLEE = 8825.0

def main():
    print("=" * 100)
    print("EXTERNAL VALIDATION: STATE METRICS (REFINED)")
    print("=" * 100)

    start = time.time()
    mem0 = get_mem_mb()
    
    # Load 2023 Data specifically for validation
    print("  Loading 2023 Medicaid data...")
    medicaid = load_medicaid().filter(pl.col("CLAIM_FROM_MONTH").str.starts_with("2023"))
    npi = load_npi().select(["NPI", "STATE"])

    # Aggregate by State
    print("  Aggregating state totals...")
    state_metrics = (
        medicaid
        .join(npi, left_on="BILLING_PROVIDER_NPI_NUM", right_on="NPI", how="left")
        .group_by("STATE")
        .agg([
            pl.sum("TOTAL_PAID").alias("TOTAL_PAID_2023"),
            pl.col("BILLING_PROVIDER_NPI_NUM").n_unique().alias("UNIQUE_PROVIDERS_2023"),
        ])
        .collect(engine="streaming")
    )

    # Merge with Reference Data
    print("  Merging with census/KFF data...")
    
    validation_rows = []
    
    for row in state_metrics.iter_rows(named=True):
        state = row["STATE"]
        if state not in STATE_REF_DATA:
            continue
            
        ref = STATE_REF_DATA[state]
        
        cost_per_enrollee = row["TOTAL_PAID_2023"] / ref["medicaid_enrollees"]
        
        # Calculate Data Coverage % (Dataset vs National Benchmark)
        coverage_pct = cost_per_enrollee / NATIONAL_AVG_COST_PER_ENROLLEE
        
        validation_rows.append({
            "STATE": state,
            "TOTAL_PAID_2023": row["TOTAL_PAID_2023"],
            "KFF_ENROLLEES": ref["medicaid_enrollees"],
            "COST_PER_ENROLLEE": cost_per_enrollee,
            "DATASET_COVERAGE": coverage_pct,
            "UNIQUE_PROVIDERS": row["UNIQUE_PROVIDERS_2023"],
        })

    val_df = pl.DataFrame(validation_rows).sort("DATASET_COVERAGE", descending=True)
    
    print(f"\n  MACPAC National Benchmark: ${NATIONAL_AVG_COST_PER_ENROLLEE:,.0f}/enrollee")
    print(f"  Dataset Average (Weighted): ${val_df['TOTAL_PAID_2023'].sum() / val_df['KFF_ENROLLEES'].sum():,.0f}/enrollee")

    # Flag Anomalies (States with unexpectedly high coverage > 75% of benchmark)
    # This might indicate data quality issues OR just high FFS utilization
    print(f"\n  High Coverage States (>75% of Benchmark):")
    anomalies = val_df.filter(pl.col("DATASET_COVERAGE") > 0.75)
    
    if anomalies.height > 0:
        with pl.Config(tbl_cols=7, tbl_width_chars=140, fmt_float="mixed"):
            print(anomalies)
    else:
        print("  None found (Dataset is consistently a subset of total spending).")

    # Save Results
    out_path = OUTPUT_DIR / "validation_state_metrics.csv"
    val_df.write_csv(str(out_path))
    print(f"\n  Wrote validation metrics to {out_path}")

    track("External Validation", start, mem0)

if __name__ == "__main__":
    main()