#!/usr/bin/env python3
"""
Investigation 2: Minnesota Fraud Pattern Detection

Public question: "Can we independently identify the billing anomalies
behind the $9B+ Minnesota fraud indictments?"

Analyses:
1. MN behavioral health / autism-related billing (H2012, H2014, H0032, 97153, 97155, T1019, S5108)
2. Rank MN providers by spending, beneficiary growth, claims-per-bene
3. Flag explosive enrollment (>100% YoY beneficiary growth)
4. Flag claims-per-bene >3 std dev above MN mean
5. Address clustering for MN providers
6. Temporal billing curves for top 25 flagged providers
7. Validate: check if known indicted entities (Star Autism Center LLC) appear

Output:
  output/minnesota_anomalies.csv
  output/minnesota_behavioral_health.csv
  output/minnesota_temporal.csv
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import polars as pl
import time
from scripts.lib.data import (
    load_medicaid, load_npi, load_npi_address, load_hcpcs,
    OUTPUT_DIR, get_mem_mb, track,
)

# Behavioral health / autism-related HCPCS codes
BH_CODES = ["H2012", "H2014", "H0032", "97153", "97155", "T1019", "S5108"]

# Known indicted entities for validation
KNOWN_INDICTED = [
    "STAR AUTISM CENTER",
    "CARING COMMUNITY",
    "AUTISM CENTER OF EXCELLENCE",
]


def main():
    print("=" * 100)
    print("INVESTIGATION 2: MINNESOTA FRAUD PATTERN DETECTION")
    print("=" * 100)

    start = time.time()
    mem0 = get_mem_mb()

    medicaid = load_medicaid()
    npi = load_npi()
    npi_addr = load_npi_address()
    hcpcs = load_hcpcs()

    # ==================================================================
    # STEP 1: Filter to MN providers, behavioral health codes
    # ==================================================================
    print("\n--- Step 1: Minnesota Behavioral Health Overview ---")

    mn_bh = (
        medicaid
        .filter(pl.col("HCPCS_CODE").is_in(BH_CODES))
        .join(
            npi.select(["NPI", "PROVIDER_NAME", "ENTITY_LABEL", "STATE"]),
            left_on="BILLING_PROVIDER_NPI_NUM",
            right_on="NPI",
            how="left",
        )
        .filter(pl.col("STATE") == "MN")
        .join(
            hcpcs.select(["HCPCS_CODE", "SHORT_DESCRIPTION"]),
            on="HCPCS_CODE",
            how="left",
        )
        .collect(engine="streaming")
    )

    print(f"  MN behavioral health records: {mn_bh.height:,}")
    print(f"  Total MN BH spending: ${mn_bh['TOTAL_PAID'].sum()/1e9:.2f}B")
    print(f"  Unique MN BH providers: {mn_bh['BILLING_PROVIDER_NPI_NUM'].n_unique():,}")

    # Spending by code
    code_summary = (
        mn_bh
        .group_by("HCPCS_CODE", "SHORT_DESCRIPTION")
        .agg([
            pl.sum("TOTAL_PAID").alias("TOTAL_PAID"),
            pl.sum("TOTAL_CLAIMS").alias("TOTAL_CLAIMS"),
            pl.col("BILLING_PROVIDER_NPI_NUM").n_unique().alias("PROVIDERS"),
        ])
        .sort("TOTAL_PAID", descending=True)
    )

    print(f"\n  MN Behavioral Health Spending by Code:")
    with pl.Config(tbl_cols=5, tbl_width_chars=120, fmt_str_lengths=30, fmt_float="mixed"):
        print(code_summary)

    # Write behavioral health summary
    out_path = OUTPUT_DIR / "minnesota_behavioral_health.csv"
    code_summary.write_csv(str(out_path))
    print(f"  Wrote {out_path}")

    track("Step 1 - MN BH overview", start, mem0)

    # ==================================================================
    # STEP 2: Rank providers, compute growth rates
    # ==================================================================
    print("\n--- Step 2: Provider Ranking and Growth Analysis ---")

    # Parse CLAIM_FROM_MONTH to extract year
    mn_bh_with_year = mn_bh.with_columns(
        pl.col("CLAIM_FROM_MONTH").cast(pl.String).str.slice(0, 4).cast(pl.Int32).alias("YEAR")
    )

    # Annual aggregation per provider
    annual = (
        mn_bh_with_year
        .group_by("BILLING_PROVIDER_NPI_NUM", "PROVIDER_NAME", "ENTITY_LABEL", "YEAR")
        .agg([
            pl.sum("TOTAL_PAID").alias("ANNUAL_PAID"),
            pl.sum("TOTAL_UNIQUE_BENEFICIARIES").alias("ANNUAL_BENE_SUM"),  # sum, not true uniques
            pl.sum("TOTAL_CLAIMS").alias("ANNUAL_CLAIMS"),
        ])
        .sort(["BILLING_PROVIDER_NPI_NUM", "YEAR"])
    )

    # Compute YoY beneficiary growth
    # CAVEAT: ANNUAL_BENE_SUM double-counts patients across codes/months,
    # so YoY growth may be inflated by code diversification, not real enrollment.
    annual_growth = (
        annual
        .with_columns(
            pl.col("ANNUAL_BENE_SUM")
            .shift(1)
            .over("BILLING_PROVIDER_NPI_NUM")
            .alias("PREV_YEAR_BENES")
        )
        .with_columns(
            pl.when(pl.col("PREV_YEAR_BENES") > 0)
            .then(
                (pl.col("ANNUAL_BENE_SUM") - pl.col("PREV_YEAR_BENES"))
                / pl.col("PREV_YEAR_BENES")
                * 100
            )
            .otherwise(None)
            .alias("BENE_GROWTH_PCT")
        )
    )

    # Flag: >100% YoY beneficiary growth
    explosive_growth = (
        annual_growth
        .filter(pl.col("BENE_GROWTH_PCT") > 100)
        .filter(pl.col("ANNUAL_BENE_SUM") >= 50)  # min volume
        .sort("BENE_GROWTH_PCT", descending=True)
    )

    print(f"  Provider-years with >100% beneficiary growth (min 50 benes): {explosive_growth.height}")

    if explosive_growth.height > 0:
        print(f"\n  Top 15 by growth rate:")
        with pl.Config(tbl_cols=8, tbl_width_chars=150, fmt_str_lengths=30, fmt_float="mixed"):
            print(explosive_growth.select([
                "PROVIDER_NAME", "YEAR", "ANNUAL_PAID",
                "ANNUAL_BENE_SUM", "PREV_YEAR_BENES", "BENE_GROWTH_PCT",
            ]).head(15))

    track("Step 2 - Growth analysis", start, mem0)

    # ==================================================================
    # STEP 3: Claims-per-beneficiary outliers
    # ==================================================================
    print("\n--- Step 3: Claims-per-Beneficiary Outliers ---")

    # Per-provider, per-code claims/bene
    provider_code_stats = (
        mn_bh
        .filter(pl.col("TOTAL_UNIQUE_BENEFICIARIES") > 0)
        .group_by("BILLING_PROVIDER_NPI_NUM", "PROVIDER_NAME", "HCPCS_CODE", "SHORT_DESCRIPTION")
        .agg([
            pl.sum("TOTAL_CLAIMS").alias("TOTAL_CLAIMS"),
            pl.sum("TOTAL_UNIQUE_BENEFICIARIES").alias("BENE_SUM"),  # sum, not true uniques
            pl.sum("TOTAL_PAID").alias("TOTAL_PAID"),
        ])
        .with_columns(
            (pl.col("TOTAL_CLAIMS") / pl.col("BENE_SUM")).alias("CLAIMS_PER_BENE")
        )
    )

    # Compute mean and std per code
    code_norms = (
        provider_code_stats
        .group_by("HCPCS_CODE")
        .agg([
            pl.mean("CLAIMS_PER_BENE").alias("MEAN_CPB"),
            pl.std("CLAIMS_PER_BENE").alias("STD_CPB"),
        ])
    )

    # Flag >3 std deviations
    cpb_outliers = (
        provider_code_stats
        .join(code_norms, on="HCPCS_CODE")
        .with_columns(
            ((pl.col("CLAIMS_PER_BENE") - pl.col("MEAN_CPB")) / pl.col("STD_CPB")).alias("Z_SCORE")
        )
        .filter(pl.col("Z_SCORE") > 3)
        .sort("Z_SCORE", descending=True)
    )

    print(f"  Provider-codes with claims/bene >3 std dev above MN mean: {cpb_outliers.height}")
    if cpb_outliers.height > 0:
        print(f"\n  Top 15 by z-score:")
        with pl.Config(tbl_cols=9, tbl_width_chars=150, fmt_str_lengths=25, fmt_float="mixed"):
            print(cpb_outliers.select([
                "PROVIDER_NAME", "HCPCS_CODE", "SHORT_DESCRIPTION",
                "CLAIMS_PER_BENE", "MEAN_CPB", "Z_SCORE", "TOTAL_PAID",
            ]).head(15))

    track("Step 3 - Claims/bene outliers", start, mem0)

    # ==================================================================
    # STEP 4: Combine flags into anomaly score
    # ==================================================================
    print("\n--- Step 4: Combined Anomaly Scoring ---")

    # Provider-level aggregation
    provider_totals = (
        mn_bh
        .group_by("BILLING_PROVIDER_NPI_NUM", "PROVIDER_NAME", "ENTITY_LABEL")
        .agg([
            pl.sum("TOTAL_PAID").alias("TOTAL_PAID"),
            pl.sum("TOTAL_CLAIMS").alias("TOTAL_CLAIMS"),
            pl.sum("TOTAL_UNIQUE_BENEFICIARIES").alias("TOTAL_BENE_SUM"),
            pl.col("HCPCS_CODE").n_unique().alias("UNIQUE_CODES"),
        ])
        .sort("TOTAL_PAID", descending=True)
    )

    # Score: each flag = 1 point
    growth_flagged = set(explosive_growth["BILLING_PROVIDER_NPI_NUM"].unique().to_list()) if explosive_growth.height > 0 else set()
    cpb_flagged = set(cpb_outliers["BILLING_PROVIDER_NPI_NUM"].unique().to_list()) if cpb_outliers.height > 0 else set()

    anomaly_scored = (
        provider_totals
        .with_columns([
            pl.col("BILLING_PROVIDER_NPI_NUM").is_in(list(growth_flagged)).cast(pl.Int8).alias("FLAG_GROWTH"),
            pl.col("BILLING_PROVIDER_NPI_NUM").is_in(list(cpb_flagged)).cast(pl.Int8).alias("FLAG_CPB"),
        ])
        .with_columns(
            (pl.col("FLAG_GROWTH") + pl.col("FLAG_CPB")).alias("ANOMALY_SCORE")
        )
        .sort(["ANOMALY_SCORE", "TOTAL_PAID"], descending=True)
    )

    flagged = anomaly_scored.filter(pl.col("ANOMALY_SCORE") > 0)
    print(f"  Providers with at least one anomaly flag: {flagged.height}")
    print(f"  Providers with both flags: {anomaly_scored.filter(pl.col('ANOMALY_SCORE') == 2).height}")

    out_path = OUTPUT_DIR / "minnesota_anomalies.csv"
    anomaly_scored.write_csv(str(out_path))
    print(f"  Wrote {out_path} ({anomaly_scored.height} rows)")

    print(f"\n  Top 25 MN BH providers (with flags):")
    with pl.Config(tbl_cols=9, tbl_width_chars=150, fmt_str_lengths=30, fmt_float="mixed"):
        print(anomaly_scored.head(25))

    track("Step 4 - Anomaly scoring", start, mem0)

    # ==================================================================
    # STEP 5: Address clustering
    # ==================================================================
    print("\n--- Step 5: MN Address Clustering ---")

    mn_addr = (
        npi_addr
        .filter(pl.col("STATE") == "MN")
        .select(["NPI", "ADDRESS", "CITY", "ZIP", "PROVIDER_NAME"])
        .collect()
    )

    mn_billing_npis = set(provider_totals["BILLING_PROVIDER_NPI_NUM"].to_list())

    mn_addr_billing = mn_addr.filter(
        pl.col("NPI").is_in(list(mn_billing_npis))
    )

    # Normalize and cluster
    mn_clusters = (
        mn_addr_billing
        .filter(pl.col("ADDRESS").is_not_null())
        .with_columns(
            pl.concat_str(
                [pl.col("ADDRESS").str.to_uppercase().str.strip_chars(), pl.col("ZIP").str.slice(0, 5)],
                separator="|",
            ).alias("NORM_ADDR")
        )
        .group_by("NORM_ADDR", "ADDRESS", "CITY", "ZIP")
        .agg([
            pl.col("NPI").n_unique().alias("NPI_COUNT"),
            pl.col("PROVIDER_NAME").alias("NAME_LIST"),
        ])
        .filter(pl.col("NPI_COUNT") > 3)
        .with_columns(
            pl.col("NAME_LIST").list.unique().list.join("; ").str.slice(0, 500).alias("PROVIDERS")
        )
        .drop(["NORM_ADDR", "NAME_LIST"])
        .sort("NPI_COUNT", descending=True)
    )

    print(f"  MN addresses with >3 BH billing NPIs: {mn_clusters.height}")
    if mn_clusters.height > 0:
        with pl.Config(tbl_cols=5, tbl_width_chars=150, fmt_str_lengths=60):
            print(mn_clusters.head(15))

    track("Step 5 - Address clustering", start, mem0)

    # ==================================================================
    # STEP 6: Temporal billing curves for top 25 flagged
    # ==================================================================
    print("\n--- Step 6: Temporal Billing Curves ---")

    top25_flagged_npis = flagged.head(25)["BILLING_PROVIDER_NPI_NUM"].to_list()

    if top25_flagged_npis:
        temporal = (
            medicaid
            .filter(pl.col("BILLING_PROVIDER_NPI_NUM").is_in(top25_flagged_npis))
            .filter(pl.col("HCPCS_CODE").is_in(BH_CODES))
            .join(
                npi.select(["NPI", "PROVIDER_NAME"]),
                left_on="BILLING_PROVIDER_NPI_NUM",
                right_on="NPI",
                how="left",
            )
            .group_by("BILLING_PROVIDER_NPI_NUM", "PROVIDER_NAME", "CLAIM_FROM_MONTH")
            .agg([
                pl.sum("TOTAL_PAID").alias("MONTHLY_PAID"),
                pl.sum("TOTAL_CLAIMS").alias("MONTHLY_CLAIMS"),
                pl.sum("TOTAL_UNIQUE_BENEFICIARIES").alias("MONTHLY_BENE_SUM"),
            ])
            .sort(["BILLING_PROVIDER_NPI_NUM", "CLAIM_FROM_MONTH"])
            .collect(engine="streaming")
        )

        out_path = OUTPUT_DIR / "minnesota_temporal.csv"
        temporal.write_csv(str(out_path))
        print(f"  Wrote {out_path} ({temporal.height} rows)")

        # Show a few examples
        for npi_num in top25_flagged_npis[:3]:
            prov = temporal.filter(pl.col("BILLING_PROVIDER_NPI_NUM") == npi_num)
            if prov.height > 0:
                name = prov["PROVIDER_NAME"][0]
                total = prov["MONTHLY_PAID"].sum()
                print(f"\n    {name} — ${total:,.0f} total, {prov.height} months active")
                with pl.Config(tbl_cols=4, tbl_width_chars=80, fmt_float="mixed"):
                    print(prov.select(["CLAIM_FROM_MONTH", "MONTHLY_PAID", "MONTHLY_BENE_SUM"]).tail(6))

    track("Step 6 - Temporal curves", start, mem0)

    # ==================================================================
    # STEP 7: Validate against known indicted entities
    # ==================================================================
    print("\n--- Step 7: Validation Against Known Indicted Entities ---")

    for entity_name in KNOWN_INDICTED:
        matches = anomaly_scored.filter(
            pl.col("PROVIDER_NAME").is_not_null()
            & pl.col("PROVIDER_NAME").str.to_uppercase().str.contains(entity_name)
        )
        if matches.height > 0:
            score = matches["ANOMALY_SCORE"][0]
            paid = matches["TOTAL_PAID"][0]
            print(f"  FOUND: '{entity_name}' — Score: {score}, Paid: ${paid:,.0f}")
        else:
            print(f"  NOT FOUND: '{entity_name}' (may use different NPI or name)")

    # ==================================================================
    # SUMMARY
    # ==================================================================
    print("\n" + "=" * 100)
    print("INVESTIGATION 2 COMPLETE")
    print("=" * 100)
    total_time = time.time() - start
    print(f"  Total runtime: {total_time:.0f}s | Peak RSS: {get_mem_mb():.0f} MB")
    print(f"\n  Key findings:")
    print(f"    - MN BH total spending: ${mn_bh['TOTAL_PAID'].sum()/1e9:.2f}B")
    print(f"    - Providers with explosive growth: {len(growth_flagged)}")
    print(f"    - Providers with outlier claims/bene: {len(cpb_flagged)}")
    print(f"    - Combined flagged providers: {flagged.height}")
    print(f"\n  Output files:")
    for f in OUTPUT_DIR.glob("minnesota_*.csv"):
        print(f"    {f.name} ({f.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
