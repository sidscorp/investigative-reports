#!/usr/bin/env python3
"""
Investigation 6: New Leads (Refined)

Analyses:
1. "The Tooth Fairy": Impossible dental volume (>32 extractions/patient/month).
2. "Liquid Gold": Urine drug testing mills (>15 tests/patient/month).
3. "Code Creep": Evaluation & Management (E&M) upcoding (>90% Level 5).
4. "The $10,000 Swab": Genetic testing abuse (High volume labs).

**AUDIT FIXES APPLIED:**
- Removed cross-row summation of UNIQUE_BENEFICIARIES to prevent double-counting.
- Ratios are now calculated at the `NPI x Month` or `NPI x HCPCS x Month` grain.
- Standardized minimum claim thresholds.
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
# Constants / Thresholds
# ---------------------------------------------------------------------------

DENTAL_CODES_PREFIX = "D7"
DENTAL_IMPOSSIBLE_THRESHOLD = 32

DRUG_TEST_CODES = [
    "G0480", "G0481", "G0482", "G0483",
    "80305", "80306", "80307",
]
DRUG_TEST_THRESHOLD = 15

# E&M Codes
EM_CODES_LEVEL_5 = ["99205", "99215"]
EM_CODES_ALL = [
    "99202", "99203", "99204", "99205",
    "99211", "99212", "99213", "99214", "99215",
]
EM_UPCODING_THRESHOLD = 0.90
EM_MIN_CLAIMS = 500  # Standardized to 500

GENETIC_CODES_PREFIX = "81"
# $1M threshold targets ~top 1% of genetic testing billers by total Medicaid spend;
# aligns with DOJ/OIG enforcement actions which typically involve $1M+ schemes.
GENETIC_SPENDING_THRESHOLD = 1_000_000


def main():
    print("=" * 100)
    print("INVESTIGATION 6: NEW LEADS (REFINED)")
    print("=" * 100)

    start = time.time()
    mem0 = get_mem_mb()

    medicaid = load_medicaid()
    npi = load_npi().select(["NPI", "PROVIDER_NAME", "ENTITY_LABEL", "STATE"])

    # ==================================================================
    # ANALYSIS 1: The Tooth Fairy (Dental Extractions)
    # ==================================================================
    print("\n--- Analysis 1: The Tooth Fairy (Dental Extractions) ---")
    
    # Fix: Calculate ratio at the ROW level (Month x Code), then filter
    dental_leads = (
        medicaid
        .filter(pl.col("HCPCS_CODE").str.starts_with(DENTAL_CODES_PREFIX))
        .join(npi, left_on="BILLING_PROVIDER_NPI_NUM", right_on="NPI", how="left")
        .with_columns(
            (pl.col("TOTAL_CLAIMS") / pl.col("TOTAL_UNIQUE_BENEFICIARIES")).alias("CLAIMS_PER_BENE")
        )
        .filter(
            (pl.col("CLAIMS_PER_BENE") > DENTAL_IMPOSSIBLE_THRESHOLD) &
            (pl.col("TOTAL_UNIQUE_BENEFICIARIES") > 10) # Minimum sample size
        )
        .select([
            "BILLING_PROVIDER_NPI_NUM", "PROVIDER_NAME", "ENTITY_LABEL", "STATE",
            "HCPCS_CODE", "CLAIM_FROM_MONTH",
            "TOTAL_CLAIMS", "TOTAL_UNIQUE_BENEFICIARIES", "CLAIMS_PER_BENE"
        ])
        .sort("CLAIMS_PER_BENE", descending=True)
        .collect(engine="streaming")
    )

    print(f"  Found {dental_leads.height:,} provider-months with >{DENTAL_IMPOSSIBLE_THRESHOLD} extractions/patient")
    if dental_leads.height > 0:
        dental_leads.write_csv(OUTPUT_DIR / "new_leads_dental.csv")
        print(dental_leads.head(5))

    track("Analysis 1 - Dental", start, mem0)

    # ==================================================================
    # ANALYSIS 2: Liquid Gold (Drug Testing)
    # ==================================================================
    print("\n--- Analysis 2: Liquid Gold (Drug Testing) ---")
    
    # Fix: Group by NPI x MONTH to get monthly test volume
    # Note: Summing beneficiaries across different codes in the same month 
    # is still risky, so we take the MAX beneficiary count of any single code
    # as a conservative lower bound for unique patients that month.
    
    drug_leads = (
        medicaid
        .filter(pl.col("HCPCS_CODE").is_in(DRUG_TEST_CODES))
        .join(npi, left_on="BILLING_PROVIDER_NPI_NUM", right_on="NPI", how="left")
        .group_by("BILLING_PROVIDER_NPI_NUM", "PROVIDER_NAME", "ENTITY_LABEL", "STATE", "CLAIM_FROM_MONTH")
        .agg([
            pl.sum("TOTAL_CLAIMS").alias("MONTHLY_TESTS"),
            pl.max("TOTAL_UNIQUE_BENEFICIARIES").alias("EST_UNIQUE_PATIENTS"), # Max of any single code row
            pl.sum("TOTAL_PAID").alias("MONTHLY_PAID")
        ])
        .with_columns(
            (pl.col("MONTHLY_TESTS") / pl.col("EST_UNIQUE_PATIENTS")).alias("TESTS_PER_PATIENT")
        )
        .filter(
            (pl.col("TESTS_PER_PATIENT") > DRUG_TEST_THRESHOLD) &
            (pl.col("EST_UNIQUE_PATIENTS") > 10)
        )
        .sort("TESTS_PER_PATIENT", descending=True)
        .collect(engine="streaming")
    )

    print(f"  Found {drug_leads.height:,} provider-months with >{DRUG_TEST_THRESHOLD} tests/patient")
    if drug_leads.height > 0:
        drug_leads.write_csv(OUTPUT_DIR / "new_leads_drug_testing.csv")
        print(drug_leads.head(5))
        
    track("Analysis 2 - Drug Testing", start, mem0)

    # ==================================================================
    # ANALYSIS 3: Code Creep (E&M Upcoding)
    # ==================================================================
    print("\n--- Analysis 3: Code Creep (E&M Upcoding) ---")
    
    # Logic remains similar (ratio of totals), but we standardize min claims
    em_upcoding = (
        medicaid
        .filter(pl.col("HCPCS_CODE").is_in(EM_CODES_ALL))
        .join(npi, left_on="BILLING_PROVIDER_NPI_NUM", right_on="NPI", how="left")
        .group_by("BILLING_PROVIDER_NPI_NUM", "PROVIDER_NAME", "ENTITY_LABEL", "STATE")
        .agg([
            pl.sum("TOTAL_CLAIMS").alias("TOTAL_EM_CLAIMS"),
            pl.col("TOTAL_CLAIMS").filter(pl.col("HCPCS_CODE").is_in(EM_CODES_LEVEL_5)).sum().fill_null(0).alias("LEVEL_5_CLAIMS"),
        ])
        .filter(pl.col("TOTAL_EM_CLAIMS") > EM_MIN_CLAIMS)
        .with_columns(
            (pl.col("LEVEL_5_CLAIMS") / pl.col("TOTAL_EM_CLAIMS")).alias("LEVEL_5_RATIO")
        )
        .filter(pl.col("LEVEL_5_RATIO") > EM_UPCODING_THRESHOLD)
        .sort("LEVEL_5_RATIO", descending=True)
        .collect(engine="streaming")
    )
    
    print(f"  Found {em_upcoding.height:,} providers with >{EM_UPCODING_THRESHOLD:.0%} Level 5 claims")
    if em_upcoding.height > 0:
        em_upcoding.write_csv(OUTPUT_DIR / "new_leads_em_upcoding.csv")
        print(em_upcoding.head(5))

    track("Analysis 3 - E&M Upcoding", start, mem0)

    # ==================================================================
    # ANALYSIS 4: Genetic Testing
    # ==================================================================
    print("\n--- Analysis 4: Genetic Testing ---")
    
    # Fix: Do not calculate per-patient ratios across months/codes.
    # Just report total volume for now.
    genetic_leads = (
        medicaid
        .filter(pl.col("HCPCS_CODE").str.starts_with(GENETIC_CODES_PREFIX))
        .join(npi, left_on="BILLING_PROVIDER_NPI_NUM", right_on="NPI", how="left")
        .group_by("BILLING_PROVIDER_NPI_NUM", "PROVIDER_NAME", "ENTITY_LABEL", "STATE")
        .agg([
            pl.sum("TOTAL_PAID").alias("TOTAL_GENETIC_PAID"),
            pl.sum("TOTAL_CLAIMS").alias("TOTAL_GENETIC_CLAIMS")
        ])
        .filter(pl.col("TOTAL_GENETIC_PAID") > GENETIC_SPENDING_THRESHOLD)
        .sort("TOTAL_GENETIC_PAID", descending=True)
        .collect(engine="streaming")
    )
    
    print(f"  Found {genetic_leads.height:,} providers with >${GENETIC_SPENDING_THRESHOLD/1e6:.1f}M genetic spending")
    if genetic_leads.height > 0:
        genetic_leads.write_csv(OUTPUT_DIR / "new_leads_genetic_testing.csv")
        print(genetic_leads.head(5))

    track("Analysis 4 - Genetic Testing", start, mem0)
    
    print(f"\nCompleted in {time.time() - start:.1f}s")

if __name__ == "__main__":
    main()