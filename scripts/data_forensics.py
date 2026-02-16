#!/usr/bin/env python3
"""
Dataset Integrity & Forensics Audit

Tests:
1.  **Benford's Law Analysis:** Checks leading digit distribution of TOTAL_PAID.
2.  **Round Number Bias:** Checks % of payments that are exact integers.
3.  **State Continuity:** Checks for missing months/data gaps by state.
4.  **Extreme Value Check:** Identifies statistically impossible single claims.

Output:
  output/forensics_benford.csv
  output/forensics_state_gaps.csv
  output/forensics_summary.txt
"""

import sys
from pathlib import Path

# Ensure project root is on the path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import polars as pl

import numpy as np

import time

from scripts.lib.data import (

    load_medicaid, load_npi,

    OUTPUT_DIR, get_mem_mb, track,

)



# Benford's Law Expected Probabilities (Digits 1-9)

BENFORD_PROBS = {

    1: 0.301, 2: 0.176, 3: 0.125, 4: 0.097, 

    5: 0.079, 6: 0.067, 7: 0.058, 8: 0.051, 9: 0.046

}



def main():

    print("=" * 100)

    print("DATASET INTEGRITY & FORENSICS AUDIT")

    print("=" * 100)



    start = time.time()

    mem0 = get_mem_mb()

    medicaid = load_medicaid()



    # ==================================================================

    # TEST 1: Benford's Law Analysis

    # ==================================================================
    print("\n--- Test 1: Benford's Law (First Digit Analysis) ---")
    
    # We take a large sample for efficiency, or run on full dataset if streaming works well
    # Using TOTAL_PAID > 10 to avoid small integer bias (like $5.00)
    benford_data = (
        medicaid
        .filter(pl.col("TOTAL_PAID") >= 10)
        .select(
            pl.col("TOTAL_PAID")
            .cast(pl.Utf8)
            .str.slice(0, 1)
            .cast(pl.Int8)
            .alias("LEADING_DIGIT")
        )
        .filter((pl.col("LEADING_DIGIT") >= 1) & (pl.col("LEADING_DIGIT") <= 9))
        .group_by("LEADING_DIGIT")
        .agg(pl.count().alias("COUNT"))
        .sort("LEADING_DIGIT")
        .collect(engine="streaming")
    )

    total_count = benford_data["COUNT"].sum()
    
    print(f"  Analyzed {total_count:,} records for Benford's Law.")
    print(f"  Digit | Observed % | Expected % | Diff %")
    print(f"  ------|------------|------------|-------")

    mad_sum = 0 # Mean Absolute Deviation
    results = []

    for row in benford_data.iter_rows(named=True):
        digit = row["LEADING_DIGIT"]
        obs_pct = row["COUNT"] / total_count
        exp_pct = BENFORD_PROBS[digit]
        diff = abs(obs_pct - exp_pct)
        mad_sum += diff
        
        results.append({
            "DIGIT": digit,
            "OBSERVED_PCT": obs_pct,
            "EXPECTED_PCT": exp_pct,
            "DIFF": diff
        })
        print(f"      {digit} |      {obs_pct*100:4.1f}% |      {exp_pct*100:4.1f}% | {diff*100:+.1f}%")

    # MAD Score Interpretation (Drake's Rule of Thumb for Forensics)
    # < 0.006: Close conformity
    # 0.006 - 0.012: Acceptable conformity
    # 0.012 - 0.015: Marginally acceptable
    # > 0.015: Nonconformity (Potential manipulation)
    mad = mad_sum / 9
    print(f"\n  Mean Absolute Deviation (MAD): {mad:.4f}")
    if mad < 0.006:
        print("  RESULT: PASS (Close Conformity to Natural Data)")
    elif mad < 0.012:
        print("  RESULT: PASS (Acceptable Conformity)")
    else:
        print("  RESULT: FAIL (Nonconformity - Data may be manipulated or filtered)")

    track("Benford's Law", start, mem0)

    # ==================================================================
    # TEST 2: Round Number Bias
    # ==================================================================
    print("\n--- Test 2: Round Number Bias ---")
    
    round_counts = (
        medicaid
        .select([
            pl.count().alias("TOTAL_ROWS"),
            pl.col("TOTAL_PAID").filter((pl.col("TOTAL_PAID") % 1) == 0).count().alias("INTEGER_ROWS"),
            pl.col("TOTAL_PAID").filter((pl.col("TOTAL_PAID") % 100) == 0).count().alias("MOD100_ROWS"),
            pl.col("TOTAL_PAID").filter((pl.col("TOTAL_PAID") % 1000) == 0).count().alias("MOD1000_ROWS"),
        ])
        .collect(engine="streaming")
    )
    
    total = round_counts["TOTAL_ROWS"][0]
    integers = round_counts["INTEGER_ROWS"][0]
    mod100 = round_counts["MOD100_ROWS"][0]
    
    int_pct = integers / total
    mod100_pct = mod100 / total
    
    print(f"  Total Rows: {total:,}")
    print(f"  Exact Dollar Amounts: {integers:,} ({int_pct:.2%})")
    print(f"  Exact $100 Multiples: {mod100:,} ({mod100_pct:.2%})")
    
    # Interpretation: Medical payments are rarely round. 
    # If exact integers > 5%, it's suspicious (unless capitated payments).
    if int_pct > 0.10:
        print("  RESULT: WARNING (High prevalence of round numbers - >10%)")
    else:
        print("  RESULT: PASS (Round numbers within expected range)")

    track("Round Number Bias", start, mem0)

    # ==================================================================
    # TEST 3: State Reporting Continuity
    # ==================================================================
    print("\n--- Test 3: State Reporting Continuity ---")
    
    # Join with NPI data to get STATE
    npi = load_npi().select(["NPI", "STATE"])
    
    # Count rows per state per month
    state_timeline = (
        medicaid
        .join(npi, left_on="BILLING_PROVIDER_NPI_NUM", right_on="NPI", how="left")
        .group_by("STATE", "CLAIM_FROM_MONTH")
        .agg(pl.count().alias("ROW_COUNT"))
        .sort("CLAIM_FROM_MONTH")
        .collect(engine="streaming")
    )
    
    # Analyze for gaps
    states = state_timeline["STATE"].unique().sort()
    gaps_found = 0
    
    print("  Checking for dropped months (reporting gaps)...")
    for state in states:
        if state is None: continue
        
        st_data = state_timeline.filter(pl.col("STATE") == state)
        months = st_data["CLAIM_FROM_MONTH"].to_list()
        
        if len(months) < 12:
            print(f"    WARNING: {state} has very few reporting months ({len(months)})")
            gaps_found += 1
            continue
            
        # Check for suspiciously low volume months (< 10% of median)
        median_vol = st_data["ROW_COUNT"].median()
        low_vol = st_data.filter(pl.col("ROW_COUNT") < (median_vol * 0.10))
        
        if low_vol.height > 0:
            print(f"    WARNING: {state} has {low_vol.height} months with <10% median volume.")
            for row in low_vol.iter_rows(named=True):
                print(f"      - {row['CLAIM_FROM_MONTH']}: {row['ROW_COUNT']:,} rows (Median: {median_vol:,.0f})")
            gaps_found += 1

    if gaps_found == 0:
        print("  RESULT: PASS (No significant reporting gaps detected)")
    else:
        print(f"  RESULT: WARNING ({gaps_found} states with potential reporting anomalies)")

    # ==================================================================
    # TEST 4: Extreme Value Check
    # ==================================================================
    print("\n--- Test 4: Extreme Value Check ---")
    
    # Check for single rows with absurdly high payments (e.g., >$500M in one month)
    # While aggregates can be high, a SINGLE ROW (Provider x Code x Month) > $100M is suspect.
    
    extreme_threshold = 100_000_000 # $100M
    
    extremes = (
        medicaid
        .filter(pl.col("TOTAL_PAID") > extreme_threshold)
        .select(["BILLING_PROVIDER_NPI_NUM", "HCPCS_CODE", "CLAIM_FROM_MONTH", "TOTAL_PAID", "TOTAL_CLAIMS"])
        .collect()
    )
    
    print(f"  Rows exceeding ${extreme_threshold/1e6:.0f}M: {extremes.height}")
    if extremes.height > 0:
        print("  Top 5 Extreme Rows:")
        with pl.Config(tbl_cols=5, tbl_width_chars=120, fmt_float="mixed"):
            print(extremes.sort("TOTAL_PAID", descending=True).head(5))
            
    track("Extreme Values", start, mem0)

    print("\n" + "=" * 100)
    print("FORENSICS AUDIT COMPLETE")
    print("=" * 100)

if __name__ == "__main__":
    main()
