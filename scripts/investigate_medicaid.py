#!/usr/bin/env python3
"""
Investigative analysis of Medicaid spending data.
Focuses on finding anomalies, outliers, and story-worthy patterns.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import polars as pl
from scripts.lib.data import load_medicaid, MEDICAID_PATH


def investigate_medicaid():
    """
    Deep dive investigative analysis of Medicaid spending.
    """
    print("=" * 100)
    print("INVESTIGATIVE ANALYSIS: MEDICAID PROVIDER SPENDING")
    print("=" * 100)

    # Use lazy loading
    lazy_df = load_medicaid()

    # ============================================================================
    # 1. THE WHALE HUNT - Find the Mega-Payments
    # ============================================================================
    print("\n" + "=" * 100)
    print("PART 1: THE WHALE HUNT - Top Payments")
    print("=" * 100)
    print("\nQuestion: Did one doctor really get $118M? Or is this insurance flow?")

    # Get top 20 payments
    whales = (
        lazy_df
        .sort("TOTAL_PAID", descending=True)
        .head(20)
        .with_columns([
            (pl.col("TOTAL_PAID") / pl.col("TOTAL_UNIQUE_BENEFICIARIES")).alias("COST_PER_BENEFICIARY"),
            (pl.col("TOTAL_CLAIMS") / pl.col("TOTAL_UNIQUE_BENEFICIARIES")).alias("CLAIMS_PER_BENEFICIARY")
        ])
        .collect(engine="streaming")
    )

    print("\nTOP 20 PAYMENTS:")
    print(whales.select([
        "BILLING_PROVIDER_NPI_NUM",
        "SERVICING_PROVIDER_NPI_NUM",
        "HCPCS_CODE",
        "CLAIM_FROM_MONTH",
        "TOTAL_UNIQUE_BENEFICIARIES",
        "TOTAL_CLAIMS",
        "TOTAL_PAID",
        "COST_PER_BENEFICIARY"
    ]))

    # Mega-payment threshold analysis
    mega_payments = lazy_df.filter(pl.col("TOTAL_PAID") > 1_000_000).collect(engine="streaming")
    print(f"\nPAYMENTS OVER $1M: {len(mega_payments):,} transactions")
    print(f"   Total value: ${mega_payments['TOTAL_PAID'].sum():,.2f}")

    # ============================================================================
    # 2. THE NEGATIVE MONEY MYSTERY
    # ============================================================================
    print("\n" + "=" * 100)
    print("PART 2: THE NEGATIVE MONEY MYSTERY - Reversals & Clawbacks")
    print("=" * 100)
    print("\nQuestion: How chaotic is the billing environment?")

    # Analyze negative payments
    total_rows = lazy_df.select(pl.len()).collect().item()

    negative_analysis = (
        lazy_df
        .select([
            pl.len().alias("total_records"),
            pl.col("TOTAL_PAID").filter(pl.col("TOTAL_PAID") < 0).len().alias("negative_count"),
            pl.col("TOTAL_PAID").filter(pl.col("TOTAL_PAID") < 0).sum().alias("total_reversals"),
            pl.col("TOTAL_PAID").filter(pl.col("TOTAL_PAID") < 0).min().alias("largest_reversal"),
        ])
        .collect(engine="streaming")
    )

    neg_count = negative_analysis["negative_count"][0]
    neg_pct = (neg_count / total_rows * 100) if total_rows > 0 else 0

    print(f"\nREVERSAL STATISTICS:")
    print(f"   Negative payment records: {neg_count:,} ({neg_pct:.2f}% of all records)")
    print(f"   Total money reversed: ${abs(negative_analysis['total_reversals'][0]):,.2f}")
    print(f"   Largest single reversal: ${abs(negative_analysis['largest_reversal'][0]):,.2f}")

    if neg_pct > 5:
        print("\nWARNING: >5% reversals suggests chaotic billing!")
    else:
        print("\nReversal rate is within normal accounting range")

    # Show examples of biggest reversals
    print("\nTOP 10 REVERSALS (Clawbacks):")
    reversals = (
        lazy_df
        .filter(pl.col("TOTAL_PAID") < 0)
        .sort("TOTAL_PAID")
        .head(10)
        .collect(engine="streaming")
    )
    print(reversals.select([
        "BILLING_PROVIDER_NPI_NUM",
        "HCPCS_CODE",
        "CLAIM_FROM_MONTH",
        "TOTAL_PAID"
    ]))

    # ============================================================================
    # 3. CALCULATED METRICS - The "Red Flag" Generators
    # ============================================================================
    print("\n" + "=" * 100)
    print("PART 3: RED FLAG ANALYSIS - Anomaly Detection via Ratios")
    print("=" * 100)
    print("\nQuestion: Who's charging way more per patient than their peers?")

    # Calculate key metrics
    print("\nComputing cost-per-beneficiary and claims-per-beneficiary metrics...")

    metrics_df = (
        lazy_df
        .filter(pl.col("TOTAL_UNIQUE_BENEFICIARIES") > 0)  # Avoid division by zero
        .filter(pl.col("TOTAL_PAID") > 0)  # Focus on actual payments, not reversals
        .with_columns([
            (pl.col("TOTAL_PAID") / pl.col("TOTAL_UNIQUE_BENEFICIARIES")).alias("COST_PER_BENEFICIARY"),
            (pl.col("TOTAL_CLAIMS") / pl.col("TOTAL_UNIQUE_BENEFICIARIES")).alias("CLAIMS_PER_BENEFICIARY")
        ])
    )

    # Get overall statistics for these metrics
    metric_stats = (
        metrics_df
        .select([
            pl.col("COST_PER_BENEFICIARY").mean().alias("avg_cost_per_patient"),
            pl.col("COST_PER_BENEFICIARY").median().alias("median_cost_per_patient"),
            pl.col("COST_PER_BENEFICIARY").std().alias("std_cost_per_patient"),
            pl.col("CLAIMS_PER_BENEFICIARY").mean().alias("avg_claims_per_patient"),
            pl.col("CLAIMS_PER_BENEFICIARY").median().alias("median_claims_per_patient"),
        ])
        .collect(engine="streaming")
    )

    print("\nBASELINE METRICS (Across All Providers):")
    print(f"   Average cost per beneficiary: ${metric_stats['avg_cost_per_patient'][0]:,.2f}")
    print(f"   Median cost per beneficiary:  ${metric_stats['median_cost_per_patient'][0]:,.2f}")
    print(f"   Std dev cost per beneficiary: ${metric_stats['std_cost_per_patient'][0]:,.2f}")
    print(f"   Average claims per beneficiary: {metric_stats['avg_claims_per_patient'][0]:.2f}")
    print(f"   Median claims per beneficiary:  {metric_stats['median_claims_per_patient'][0]:.2f}")

    # Find extreme outliers (>10x median)
    median_cost = metric_stats['median_cost_per_patient'][0]
    print(f"\nFINDING EXTREME OUTLIERS (>10x median of ${median_cost:.2f})...")

    outliers = (
        metrics_df
        .filter(pl.col("COST_PER_BENEFICIARY") > median_cost * 10)
        .sort("COST_PER_BENEFICIARY", descending=True)
        .head(25)
        .collect(engine="streaming")
    )

    print(f"\nTOP 25 COST-PER-BENEFICIARY OUTLIERS:")
    print(f"   (These providers charge >10x the median per patient)")
    print(outliers.select([
        "BILLING_PROVIDER_NPI_NUM",
        "HCPCS_CODE",
        "TOTAL_UNIQUE_BENEFICIARIES",
        "TOTAL_CLAIMS",
        "TOTAL_PAID",
        "COST_PER_BENEFICIARY",
        "CLAIMS_PER_BENEFICIARY"
    ]))

    # Claims-per-beneficiary outliers
    median_claims = metric_stats['median_claims_per_patient'][0]
    print(f"\nFINDING HIGH-VOLUME BILLERS (>20x median of {median_claims:.2f} claims/patient)...")

    high_volume = (
        metrics_df
        .filter(pl.col("CLAIMS_PER_BENEFICIARY") > median_claims * 20)
        .sort("CLAIMS_PER_BENEFICIARY", descending=True)
        .head(25)
        .collect(engine="streaming")
    )

    print(f"\nTOP 25 CLAIMS-PER-BENEFICIARY OUTLIERS:")
    print(f"   (These providers bill >20x the median claims per patient)")
    print(high_volume.select([
        "BILLING_PROVIDER_NPI_NUM",
        "HCPCS_CODE",
        "TOTAL_UNIQUE_BENEFICIARIES",
        "TOTAL_CLAIMS",
        "TOTAL_PAID",
        "COST_PER_BENEFICIARY",
        "CLAIMS_PER_BENEFICIARY"
    ]))

    # ============================================================================
    # 4. HCPCS CODE ANALYSIS - What services cost the most?
    # ============================================================================
    print("\n" + "=" * 100)
    print("PART 4: SERVICE CODE ANALYSIS - Where's the Money Going?")
    print("=" * 100)

    # Top services by total spending
    print("\nTOP 20 HCPCS CODES BY TOTAL SPENDING:")
    print("   (These are the most expensive services overall)")

    top_codes = (
        lazy_df
        .filter(pl.col("TOTAL_PAID") > 0)
        .group_by("HCPCS_CODE")
        .agg([
            pl.sum("TOTAL_PAID").alias("TOTAL_SPENT"),
            pl.sum("TOTAL_UNIQUE_BENEFICIARIES").alias("BENE_SUM"),
            pl.sum("TOTAL_CLAIMS").alias("TOTAL_CLAIMS"),
            pl.len().alias("RECORD_COUNT")
        ])
        .with_columns([
            (pl.col("TOTAL_SPENT") / pl.col("BENE_SUM")).alias("AVG_COST_PER_BENE")
        ])
        .sort("TOTAL_SPENT", descending=True)
        .head(20)
        .collect(engine="streaming")
    )

    print(top_codes)

    # ============================================================================
    # 5. PROVIDER NPI ANALYSIS - Who are the biggest spenders?
    # ============================================================================
    print("\n" + "=" * 100)
    print("PART 5: PROVIDER ANALYSIS - Top Billing Entities")
    print("=" * 100)

    print("\nTOP 20 BILLING PROVIDERS BY TOTAL SPENDING:")

    top_providers = (
        lazy_df
        .filter(pl.col("TOTAL_PAID") > 0)
        .group_by("BILLING_PROVIDER_NPI_NUM")
        .agg([
            pl.sum("TOTAL_PAID").alias("TOTAL_SPENT"),
            pl.sum("TOTAL_UNIQUE_BENEFICIARIES").alias("BENE_SUM"),
            pl.sum("TOTAL_CLAIMS").alias("TOTAL_CLAIMS"),
            pl.len().alias("RECORD_COUNT"),
            pl.col("HCPCS_CODE").n_unique().alias("UNIQUE_SERVICES")
        ])
        .with_columns([
            (pl.col("TOTAL_SPENT") / pl.col("BENE_SUM")).alias("AVG_COST_PER_BENE"),
            (pl.col("TOTAL_CLAIMS") / pl.col("BENE_SUM")).alias("AVG_CLAIMS_PER_BENE")
        ])
        .sort("TOTAL_SPENT", descending=True)
        .head(20)
        .collect(engine="streaming")
    )

    print(top_providers)

    # ============================================================================
    # 6. ENRICHMENT GUIDE
    # ============================================================================
    print("\n" + "=" * 100)
    print("PART 6: ENRICHMENT RECOMMENDATIONS - Making NPIs and Codes Human-Readable")
    print("=" * 100)

    print("""
    TO UNLOCK THE FULL STORY, YOU NEED TWO LOOKUP TABLES:

    1. HCPCS/CPT CODE DESCRIPTIONS (The "Menu")
       • Download from: https://www.cms.gov/medicare/coding-billing/healthcare-common-procedure-system
       * What it does: Converts "G0438" -> "Annual Wellness Visit"
       • Why: Flags what services are being billed (Surgery vs. Office Visit vs. Lab Test)

    2. NPI REGISTRY (The "Phonebook")
       • Download from: https://download.cms.gov/nppes/NPI_Files.html (weekly full file)
       • API option: https://npiregistry.cms.hhs.gov/api/ (for small samples)
       * What it does: Converts "1234567890" -> "Johns Hopkins Hospital"
       • Why: Distinguishes Hospitals ($5M normal) vs. Individual Doctors ($5M suspicious)
       • Critical fields: Provider Name, Entity Type (Individual/Organization), Specialty

    NEXT STEPS:


    a) Download the NPI registry and join on BILLING_PROVIDER_NPI_NUM
    b) Segment analysis by Entity Type (Organization vs Individual)
    c) Download HCPCS codes and join on HCPCS_CODE
    d) Re-run outlier analysis within specialty groups (compare dentists to dentists, not to hospitals)

    STORY-READY OUTPUTS:

    • "Provider X (a solo practitioner) billed $50K per patient while the specialty average is $2K"
    • "HCPCS Code Y (cosmetic procedure) had $10M in Medicaid claims despite not being covered"
    • "The top 10 providers account for X% of all Medicaid spending in this dataset"
    """)

    # ============================================================================
    # SUMMARY
    # ============================================================================
    print("\n" + "=" * 100)
    print("INVESTIGATION COMPLETE")
    print("=" * 100)
    print("""
    KEY TAKEAWAYS FOR YOUR REPORT:


    1. Check the $118M payment - look up that NPI to see if it's an institution or individual
    2. Negative payments represent X% of records (normal or chaotic?)
    3. Outliers charging >10x median per patient need individual investigation
    4. High-volume billers with >20x median claims per patient warrant scrutiny
    5. Top HCPCS codes and top providers identified - ready for enrichment

 REPORTER'S WARNING: Do NOT publish raw outliers without context!
       - A $5M bill from "University Hospital" = normal
       - A $5M bill from "Dr. Smith's Clinic" = front-page news

    Get the NPI and HCPCS lookup files to avoid "fake news" accusations.
    """)


if __name__ == "__main__":
    print(f"\n  Starting investigative analysis of: {MEDICAID_PATH}")
    print(f"  File size: {MEDICAID_PATH.stat().st_size / (1024**3):.2f} GB\n")

    investigate_medicaid()
