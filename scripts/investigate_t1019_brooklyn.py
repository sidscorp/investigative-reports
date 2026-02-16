#!/usr/bin/env python3
"""
Investigation 1: Brooklyn T1019 Concentration

Public question: "Why are 7 of the top 20 personal care billers nationwide
in a single Brooklyn borough? Is this entity proliferation fraud?"

Analyses:
1. National T1019 ranking by billing NPI
2. Brooklyn/NY filter with address data
3. Shared address / authorized official flagging
4. OIG exclusion list cross-reference
5. Cost-per-claim Brooklyn vs. national median
6. Temporal pattern — when did each entity start billing?

Output:
  output/t1019_brooklyn_analysis.csv
  output/t1019_shared_addresses.csv
  output/t1019_oig_matches.csv
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import polars as pl
import time
from scripts.lib.data import (
    load_medicaid, load_npi_address, load_oig,
    OUTPUT_DIR, get_mem_mb, track,
)

# Brooklyn zip code prefixes (112xx)
BROOKLYN_ZIP_PREFIX = "112"


def main():
    print("=" * 100)
    print("INVESTIGATION 1: BROOKLYN T1019 CONCENTRATION")
    print("=" * 100)

    start = time.time()
    mem0 = get_mem_mb()

    medicaid = load_medicaid()
    npi_addr = load_npi_address()

    # ==================================================================
    # STEP 1: National T1019 ranking
    # ==================================================================
    print("\n--- Step 1: National T1019 Provider Ranking ---")

    t1019_national = (
        medicaid
        .filter(pl.col("HCPCS_CODE") == "T1019")
        .filter(pl.col("TOTAL_PAID") > 0)
        .group_by("BILLING_PROVIDER_NPI_NUM")
        .agg([
            pl.sum("TOTAL_PAID").alias("TOTAL_PAID"),
            pl.sum("TOTAL_CLAIMS").alias("TOTAL_CLAIMS"),
            pl.sum("TOTAL_UNIQUE_BENEFICIARIES").alias("BENE_SUM"),  # sum, not true uniques
            pl.min("CLAIM_FROM_MONTH").alias("FIRST_BILLING_MONTH"),
            pl.max("CLAIM_FROM_MONTH").alias("LAST_BILLING_MONTH"),
            pl.len().alias("RECORD_COUNT"),
        ])
        .with_columns([
            (pl.col("TOTAL_PAID") / pl.col("TOTAL_CLAIMS")).alias("COST_PER_CLAIM"),
            (pl.col("TOTAL_PAID") / pl.col("BENE_SUM")).alias("COST_PER_BENE"),
        ])
        .sort("TOTAL_PAID", descending=True)
        .with_row_index("NATIONAL_RANK", offset=1)
        .collect(engine="streaming")
    )

    national_median_cpc = t1019_national["COST_PER_CLAIM"].median()
    print(f"  Total T1019 providers nationally: {t1019_national.height:,}")
    print(f"  National median cost-per-claim: ${national_median_cpc:,.2f}")
    print(f"  Top 10 nationally by total T1019 spending:")
    with pl.Config(tbl_cols=10, tbl_width_chars=140, fmt_float="mixed"):
        print(t1019_national.head(10).drop("RECORD_COUNT"))

    track("Step 1 - National ranking", start, mem0)

    # ==================================================================
    # STEP 2: Join with NPI address, filter to NY/Brooklyn
    # ==================================================================
    print("\n--- Step 2: Brooklyn Filter ---")

    addr = npi_addr.select([
        "NPI", "PROVIDER_NAME", "ENTITY_LABEL", "ADDRESS", "CITY", "STATE", "ZIP",
        "TAXONOMY_CODE", "AUTH_OFFICIAL_LAST", "AUTH_OFFICIAL_FIRST",
    ]).collect()

    t1019_enriched = t1019_national.join(
        addr,
        left_on="BILLING_PROVIDER_NPI_NUM",
        right_on="NPI",
        how="left",
    )

    # NY state providers
    ny_t1019 = t1019_enriched.filter(pl.col("STATE") == "NY")
    print(f"  NY state T1019 providers: {ny_t1019.height:,}")
    print(f"  NY T1019 total spending: ${ny_t1019['TOTAL_PAID'].sum()/1e9:.2f}B")

    # Brooklyn filter (zip starts with 112)
    brooklyn = ny_t1019.filter(
        pl.col("ZIP").is_not_null() & pl.col("ZIP").str.starts_with(BROOKLYN_ZIP_PREFIX)
    )
    print(f"  Brooklyn T1019 providers: {brooklyn.height:,}")
    print(f"  Brooklyn T1019 total spending: ${brooklyn['TOTAL_PAID'].sum()/1e9:.2f}B")

    # How many Brooklyn providers are in the national top 20?
    top20_brooklyn = brooklyn.filter(pl.col("NATIONAL_RANK") <= 20)
    print(f"  Brooklyn providers in national top 20: {top20_brooklyn.height}")

    # Cost-per-claim ratio
    brooklyn_median_cpc = brooklyn["COST_PER_CLAIM"].median()
    print(f"\n  Brooklyn median cost-per-claim: ${brooklyn_median_cpc:,.2f}")
    print(f"  National median cost-per-claim: ${national_median_cpc:,.2f}")
    if national_median_cpc and national_median_cpc > 0:
        print(f"  Brooklyn/National ratio: {brooklyn_median_cpc / national_median_cpc:.2f}x")

    # Write Brooklyn analysis
    brooklyn_out = brooklyn.select([
        "NATIONAL_RANK", "BILLING_PROVIDER_NPI_NUM", "PROVIDER_NAME", "ENTITY_LABEL",
        "ADDRESS", "CITY", "ZIP", "TOTAL_PAID", "TOTAL_CLAIMS", "BENE_SUM",
        "COST_PER_CLAIM", "COST_PER_BENE", "FIRST_BILLING_MONTH", "LAST_BILLING_MONTH",
        "AUTH_OFFICIAL_LAST", "AUTH_OFFICIAL_FIRST",
    ]).sort("TOTAL_PAID", descending=True)

    out_path = OUTPUT_DIR / "t1019_brooklyn_analysis.csv"
    brooklyn_out.write_csv(str(out_path))
    print(f"\n  Wrote {out_path} ({brooklyn_out.height} rows)")

    print(f"\n  Top 20 Brooklyn T1019 providers:")
    with pl.Config(tbl_cols=10, tbl_width_chars=160, fmt_str_lengths=35, fmt_float="mixed"):
        print(brooklyn_out.select([
            "NATIONAL_RANK", "PROVIDER_NAME", "ZIP", "TOTAL_PAID",
            "TOTAL_CLAIMS", "COST_PER_CLAIM", "FIRST_BILLING_MONTH",
        ]).head(20))

    track("Step 2 - Brooklyn filter", start, mem0)

    # ==================================================================
    # STEP 3: Shared address / authorized official flagging
    # ==================================================================
    print("\n--- Step 3: Shared Address / Authorized Official Flagging ---")

    # Normalize address for matching
    brooklyn_addrs = brooklyn.with_columns(
        pl.concat_str(
            [
                pl.col("ADDRESS").str.to_uppercase().str.strip_chars(),
                pl.col("ZIP").str.slice(0, 5),
            ],
            separator="|",
        ).alias("NORM_ADDR")
    )

    # Addresses with multiple NPIs
    shared_addr = (
        brooklyn_addrs
        .group_by("NORM_ADDR", "ADDRESS", "ZIP")
        .agg([
            pl.col("BILLING_PROVIDER_NPI_NUM").n_unique().alias("NPI_COUNT"),
            pl.sum("TOTAL_PAID").alias("COMBINED_PAID"),
            pl.col("PROVIDER_NAME").alias("PROVIDERS_LIST"),
            pl.col("AUTH_OFFICIAL_LAST").alias("AUTH_LIST"),
        ])
        .filter(pl.col("NPI_COUNT") > 1)
        .with_columns([
            pl.col("PROVIDERS_LIST").list.unique().list.join("; ").str.slice(0, 500).alias("PROVIDERS"),
            pl.col("AUTH_LIST").list.unique().list.join("; ").alias("AUTH_OFFICIALS"),
        ])
        .drop(["NORM_ADDR", "PROVIDERS_LIST", "AUTH_LIST"])
        .sort("COMBINED_PAID", descending=True)
    )

    print(f"  Brooklyn addresses with multiple T1019 NPIs: {shared_addr.height}")
    if shared_addr.height > 0:
        print(f"  Combined spending at shared addresses: ${shared_addr['COMBINED_PAID'].sum()/1e9:.2f}B")
        with pl.Config(tbl_cols=6, tbl_width_chars=160, fmt_str_lengths=60, fmt_float="mixed"):
            print(shared_addr.head(15))

        out_path = OUTPUT_DIR / "t1019_shared_addresses.csv"
        shared_addr.write_csv(str(out_path))
        print(f"  Wrote {out_path} ({shared_addr.height} rows)")

    # Shared authorized officials across different addresses
    auth_officials = (
        brooklyn_addrs
        .filter(pl.col("AUTH_OFFICIAL_LAST").is_not_null())
        .with_columns(
            pl.concat_str(
                [pl.col("AUTH_OFFICIAL_FIRST"), pl.col("AUTH_OFFICIAL_LAST")],
                separator=" ",
                ignore_nulls=True,
            ).str.to_uppercase().alias("AUTH_OFFICIAL_NAME")
        )
        .group_by("AUTH_OFFICIAL_NAME")
        .agg([
            pl.col("BILLING_PROVIDER_NPI_NUM").n_unique().alias("NPI_COUNT"),
            pl.sum("TOTAL_PAID").alias("COMBINED_PAID"),
            pl.col("ADDRESS").alias("ADDR_LIST"),
            pl.col("PROVIDER_NAME").alias("NAME_LIST"),
        ])
        .filter(pl.col("NPI_COUNT") > 1)
        .with_columns([
            pl.col("ADDR_LIST").list.unique().list.join("; ").alias("ADDRESSES"),
            pl.col("NAME_LIST").list.unique().list.join("; ").str.slice(0, 300).alias("ENTITIES"),
        ])
        .drop(["ADDR_LIST", "NAME_LIST"])
        .sort("COMBINED_PAID", descending=True)
    )

    print(f"\n  Authorized officials controlling multiple Brooklyn T1019 NPIs: {auth_officials.height}")
    if auth_officials.height > 0:
        with pl.Config(tbl_cols=5, tbl_width_chars=160, fmt_str_lengths=60, fmt_float="mixed"):
            print(auth_officials.head(10))

    track("Step 3 - Shared addresses", start, mem0)

    # ==================================================================
    # STEP 4: OIG Exclusion List cross-reference
    # ==================================================================
    print("\n--- Step 4: OIG Exclusion List Cross-Reference ---")

    oig = load_oig()
    print(f"  OIG exclusion list: {oig.height:,} entries")

    # Match on NPI
    oig_with_npi = oig.filter(
        pl.col("NPI").is_not_null() & (pl.col("NPI") != "") & (pl.col("NPI") != "0000000000")
    )

    # Cross-reference Brooklyn providers
    brooklyn_npis = brooklyn["BILLING_PROVIDER_NPI_NUM"].to_list()
    oig_matches = oig_with_npi.filter(
        pl.col("NPI").is_in(brooklyn_npis)
    )

    print(f"  OIG matches against Brooklyn T1019 providers (by NPI): {oig_matches.height}")

    # Also try name matching for all NY T1019 providers
    ny_npis = ny_t1019["BILLING_PROVIDER_NPI_NUM"].to_list()
    oig_ny_matches = oig_with_npi.filter(
        pl.col("NPI").is_in(ny_npis)
    )
    print(f"  OIG matches against all NY T1019 providers (by NPI): {oig_ny_matches.height}")

    # Also match by business name for Brooklyn
    brooklyn_names = brooklyn.filter(
        pl.col("ENTITY_LABEL") == "Organization"
    )["PROVIDER_NAME"].unique().to_list()
    brooklyn_names_upper = [n.upper() for n in brooklyn_names if n is not None]

    oig_name_matches = oig.filter(
        pl.col("BUSNAME").is_not_null()
        & pl.col("BUSNAME").str.to_uppercase().is_in(brooklyn_names_upper)
    )
    print(f"  OIG matches against Brooklyn T1019 providers (by business name): {oig_name_matches.height}")

    # Combine all OIG matches
    all_oig_matches = pl.concat([
        oig_matches.with_columns(pl.lit("NPI match").alias("MATCH_TYPE")),
        oig_name_matches.with_columns(pl.lit("Name match").alias("MATCH_TYPE")),
    ]) if oig_name_matches.height > 0 or oig_matches.height > 0 else pl.DataFrame()

    if isinstance(all_oig_matches, pl.DataFrame) and all_oig_matches.height > 0:
        out_path = OUTPUT_DIR / "t1019_oig_matches.csv"
        all_oig_matches.write_csv(str(out_path))
        print(f"\n  Wrote {out_path} ({all_oig_matches.height} rows)")
        with pl.Config(tbl_cols=10, tbl_width_chars=150, fmt_str_lengths=30):
            print(all_oig_matches.head(20))
    else:
        print("  No OIG matches found (writing empty file)")
        out_path = OUTPUT_DIR / "t1019_oig_matches.csv"
        pl.DataFrame({"NOTE": ["No matches found"]}).write_csv(str(out_path))

    track("Step 4 - OIG cross-reference", start, mem0)

    # ==================================================================
    # SUMMARY
    # ==================================================================
    print("\n" + "=" * 100)
    print("INVESTIGATION 1 COMPLETE")
    print("=" * 100)
    total_time = time.time() - start
    print(f"  Total runtime: {total_time:.0f}s | Peak RSS: {get_mem_mb():.0f} MB")
    print(f"\n  Key findings:")
    print(f"    - Brooklyn T1019 providers in national top 20: {top20_brooklyn.height}")
    print(f"    - Brooklyn T1019 total spending: ${brooklyn['TOTAL_PAID'].sum()/1e9:.2f}B")
    if national_median_cpc and national_median_cpc > 0:
        print(f"    - Brooklyn/National cost-per-claim ratio: {brooklyn_median_cpc / national_median_cpc:.2f}x")
    print(f"    - Shared addresses in Brooklyn: {shared_addr.height}")
    print(f"    - OIG exclusion matches: {all_oig_matches.height if isinstance(all_oig_matches, pl.DataFrame) else 0}")
    print(f"\n  Output files:")
    for f in OUTPUT_DIR.glob("t1019_*.csv"):
        print(f"    {f.name} ({f.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
