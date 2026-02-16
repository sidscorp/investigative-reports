#!/usr/bin/env python3
"""
Investigation 5: Ghost Provider / Impossible Volume Test

Public question: "Are there providers billing for more services than
could physically be delivered?"

Analyses:
1. Individual T1019 providers exceeding physical capacity (704 claims/month max)
2. Claims-per-beneficiary-per-month exceeding clinical plausibility
3. Address clustering — multiple NPIs at same practice address

Output:
  output/ghost_providers_impossible_volume.csv
  output/ghost_providers_address_clustering.csv
"""

import sys
from pathlib import Path

# Ensure project root is on the path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import polars as pl
import time
from scripts.lib.data import (
    load_medicaid, load_npi, load_npi_address, load_hcpcs,
    OUTPUT_DIR, get_mem_mb, track,
)

# Physical capacity ceiling for T1019 (15-min personal care units)
# 8 hours/day * 22 working days/month * 4 units/hour = 704 units/month
MAX_T1019_CLAIMS_PER_MONTH = 704

# Clinical plausibility: >30 T1019 claims/beneficiary/month
# = 30 * 15 min = 7.5 hours/day of care per patient
T1019_CLAIMS_PER_BENE_THRESHOLD = 30


def main():
    print("=" * 100)
    print("INVESTIGATION 5: GHOST PROVIDER / IMPOSSIBLE VOLUME TEST")
    print("=" * 100)

    start = time.time()
    mem0 = get_mem_mb()

    medicaid = load_medicaid()
    npi = load_npi()
    npi_addr = load_npi_address()

    # ==================================================================
    # ANALYSIS 1: Individual T1019 providers exceeding physical capacity
    # ==================================================================
    print("\n--- Analysis 1: Impossible Volume (Individual T1019 Providers) ---")

    impossible_volume = (
        medicaid
        .filter(pl.col("HCPCS_CODE") == "T1019")
        .join(
            npi.select(["NPI", "PROVIDER_NAME", "ENTITY_LABEL", "STATE"]),
            left_on="BILLING_PROVIDER_NPI_NUM",
            right_on="NPI",
            how="left",
        )
        .filter(pl.col("ENTITY_LABEL") == "Individual")
        .group_by("BILLING_PROVIDER_NPI_NUM", "PROVIDER_NAME", "STATE", "CLAIM_FROM_MONTH")
        .agg([
            pl.sum("TOTAL_CLAIMS").alias("MONTHLY_CLAIMS"),
            pl.sum("TOTAL_PAID").alias("MONTHLY_PAID"),
            pl.sum("TOTAL_UNIQUE_BENEFICIARIES").alias("MONTHLY_BENE_SUM"),  # sum, not true uniques
        ])
        .filter(pl.col("MONTHLY_CLAIMS") > MAX_T1019_CLAIMS_PER_MONTH)
        .with_columns([
            (pl.col("MONTHLY_CLAIMS") / MAX_T1019_CLAIMS_PER_MONTH).alias("CAPACITY_RATIO"),
            (pl.col("MONTHLY_CLAIMS") / pl.col("MONTHLY_BENE_SUM")).alias("CLAIMS_PER_BENE"),
        ])
        .sort("MONTHLY_CLAIMS", descending=True)
        .collect(engine="streaming")
    )

    print(f"  Found {impossible_volume.height:,} provider-months exceeding physical capacity ({MAX_T1019_CLAIMS_PER_MONTH} claims/month)")
    n_providers = impossible_volume["BILLING_PROVIDER_NPI_NUM"].n_unique()
    print(f"  Spanning {n_providers:,} unique individual providers")

    if impossible_volume.height > 0:
        # Summary per provider
        impossible_summary = (
            impossible_volume
            .group_by("BILLING_PROVIDER_NPI_NUM", "PROVIDER_NAME", "STATE")
            .agg([
                pl.len().alias("MONTHS_OVER_CAPACITY"),
                pl.max("MONTHLY_CLAIMS").alias("MAX_MONTHLY_CLAIMS"),
                pl.max("CAPACITY_RATIO").alias("MAX_CAPACITY_RATIO"),
                pl.sum("MONTHLY_PAID").alias("TOTAL_PAID_OVER_CAPACITY"),
                pl.max("CLAIMS_PER_BENE").alias("MAX_CLAIMS_PER_BENE"),
            ])
            .sort("MAX_MONTHLY_CLAIMS", descending=True)
        )
        print(f"\n  Top 10 by max monthly claims:")
        with pl.Config(tbl_cols=8, tbl_width_chars=140, fmt_str_lengths=30, fmt_float="mixed"):
            print(impossible_summary.head(10))

        out_path = OUTPUT_DIR / "ghost_providers_impossible_volume.csv"
        impossible_summary.write_csv(str(out_path))
        print(f"\n  Wrote {out_path} ({impossible_summary.height} rows)")

    track("Analysis 1 - Impossible Volume", start, mem0)

    # ==================================================================
    # ANALYSIS 2: Claims-per-beneficiary-per-month plausibility
    # ==================================================================
    print("\n--- Analysis 2: Implausible Claims per Beneficiary (T1019, All Providers) ---")

    implausible_cpb = (
        medicaid
        .filter(pl.col("HCPCS_CODE") == "T1019")
        .join(
            npi.select(["NPI", "PROVIDER_NAME", "ENTITY_LABEL", "STATE"]),
            left_on="BILLING_PROVIDER_NPI_NUM",
            right_on="NPI",
            how="left",
        )
        .with_columns(
            (pl.col("TOTAL_CLAIMS") / pl.col("TOTAL_UNIQUE_BENEFICIARIES"))
            .alias("CLAIMS_PER_BENE_MONTH")
        )
        .filter(pl.col("CLAIMS_PER_BENE_MONTH") > T1019_CLAIMS_PER_BENE_THRESHOLD)
        .select([
            "BILLING_PROVIDER_NPI_NUM", "PROVIDER_NAME", "ENTITY_LABEL", "STATE",
            "CLAIM_FROM_MONTH", "TOTAL_UNIQUE_BENEFICIARIES", "TOTAL_CLAIMS",
            "TOTAL_PAID", "CLAIMS_PER_BENE_MONTH",
        ])
        .sort("CLAIMS_PER_BENE_MONTH", descending=True)
        .collect(engine="streaming")
    )

    print(f"  Found {implausible_cpb.height:,} provider-months with >{T1019_CLAIMS_PER_BENE_THRESHOLD} T1019 claims/beneficiary/month")
    n_providers2 = implausible_cpb["BILLING_PROVIDER_NPI_NUM"].n_unique()
    print(f"  Spanning {n_providers2:,} unique providers")

    if implausible_cpb.height > 0:
        print(f"\n  Top 10 by claims/beneficiary/month:")
        with pl.Config(tbl_cols=9, tbl_width_chars=150, fmt_str_lengths=30, fmt_float="mixed"):
            print(implausible_cpb.head(10))

    track("Analysis 2 - Implausible Claims/Bene", start, mem0)

    # ==================================================================
    # ANALYSIS 3: Address clustering — multiple NPIs at same address
    # ==================================================================
    print("\n--- Analysis 3: Address Clustering (Multiple NPIs per Address) ---")

    # Get all billing NPIs from Medicaid data with total spending
    medicaid_totals = (
        medicaid
        .filter(pl.col("TOTAL_PAID") > 0)
        .group_by("BILLING_PROVIDER_NPI_NUM")
        .agg([
            pl.sum("TOTAL_PAID").alias("TOTAL_PAID"),
            pl.sum("TOTAL_CLAIMS").alias("TOTAL_CLAIMS"),
        ])
        .collect(engine="streaming")
    )

    # Join with address data
    address_data = (
        npi_addr
        .select(["NPI", "ADDRESS", "CITY", "STATE", "ZIP", "PROVIDER_NAME", "ENTITY_LABEL"])
        .filter(pl.col("ADDRESS").is_not_null())
        .collect()
    )

    # Join: only providers who billed Medicaid
    addr_with_billing = (
        medicaid_totals
        .join(
            address_data,
            left_on="BILLING_PROVIDER_NPI_NUM",
            right_on="NPI",
            how="inner",
        )
    )

    # Normalize address for grouping
    addr_with_billing = addr_with_billing.with_columns(
        pl.concat_str(
            [
                pl.col("ADDRESS").str.to_uppercase().str.strip_chars(),
                pl.col("CITY").str.to_uppercase().str.strip_chars(),
                pl.col("STATE").str.to_uppercase().str.strip_chars(),
            ],
            separator="|",
            ignore_nulls=True,
        ).alias("NORM_ADDRESS")
    )

    # Group by normalized address
    address_clusters = (
        addr_with_billing
        .group_by("NORM_ADDRESS", "ADDRESS", "CITY", "STATE")
        .agg([
            pl.col("BILLING_PROVIDER_NPI_NUM").n_unique().alias("NPI_COUNT"),
            pl.sum("TOTAL_PAID").alias("TOTAL_PAID_AT_ADDRESS"),
            pl.sum("TOTAL_CLAIMS").alias("TOTAL_CLAIMS_AT_ADDRESS"),
            pl.col("PROVIDER_NAME").unique().alias("PROVIDERS_AT_ADDRESS"),
            pl.col("ENTITY_LABEL").value_counts().alias("ENTITY_TYPES"),
        ])
        .filter(pl.col("NPI_COUNT") > 10)
        .sort("NPI_COUNT", descending=True)
    )

    # Flatten for CSV output
    address_clusters_flat = (
        address_clusters
        .with_columns(
            pl.col("PROVIDERS_AT_ADDRESS")
            .list.join("; ")
            .str.slice(0, 500)
            .alias("PROVIDERS_SAMPLE"),
        )
        .drop(["PROVIDERS_AT_ADDRESS", "ENTITY_TYPES", "NORM_ADDRESS"])
    )

    print(f"  Found {address_clusters_flat.height:,} addresses with >10 distinct billing NPIs")

    if address_clusters_flat.height > 0:
        print(f"\n  Top 15 by NPI count:")
        with pl.Config(tbl_cols=7, tbl_width_chars=150, fmt_str_lengths=50, fmt_float="mixed"):
            print(address_clusters_flat.head(15))

        total_at_clusters = address_clusters_flat["TOTAL_PAID_AT_ADDRESS"].sum()
        print(f"\n  Total billing at clustered addresses: ${total_at_clusters/1e9:.1f}B")

        out_path = OUTPUT_DIR / "ghost_providers_address_clustering.csv"
        address_clusters_flat.write_csv(str(out_path))
        print(f"  Wrote {out_path} ({address_clusters_flat.height} rows)")

    track("Analysis 3 - Address Clustering", start, mem0)

    # ==================================================================
    # SUMMARY
    # ==================================================================
    print("\n" + "=" * 100)
    print("INVESTIGATION 5 COMPLETE")
    print("=" * 100)
    total_time = time.time() - start
    print(f"  Total runtime: {total_time:.0f}s | Peak RSS: {get_mem_mb():.0f} MB")
    print(f"\n  Output files:")
    for f in OUTPUT_DIR.glob("ghost_providers_*.csv"):
        print(f"    {f.name} ({f.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
