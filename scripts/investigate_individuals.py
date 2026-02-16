#!/usr/bin/env python3
"""
Investigation 3: Individual Provider Deep Dive

Public question: "Which solo practitioners are billing like hospitals?
Are any of them actually fraudulent?"

Analyses:
1. Top individual spenders aggregated
2. Specialty peer comparison using NUCC taxonomy
3. Flag individuals whose cost-per-beneficiary exceeds 5x specialty median
4. OIG exclusion list cross-reference
5. Billing profiles for top 50 flagged individuals

Output:
  output/individual_top_spenders.csv
  output/individual_specialty_outliers.csv
  output/individual_oig_matches.csv
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import polars as pl
import time
from scripts.lib.data import (
    load_medicaid, load_npi, load_hcpcs, load_oig, load_nucc,
    OUTPUT_DIR, get_mem_mb, track,
)

SPECIALTY_MIN_PRACTITIONERS = 100
OUTLIER_MULTIPLIER = 5


def main():
    print("=" * 100)
    print("INVESTIGATION 3: INDIVIDUAL PROVIDER DEEP DIVE")
    print("=" * 100)

    start = time.time()
    mem0 = get_mem_mb()

    medicaid = load_medicaid()
    npi = load_npi()
    hcpcs = load_hcpcs()

    # Load NUCC taxonomy for specialty descriptions
    nucc = load_nucc()
    print(f"  NUCC taxonomy: {nucc.height:,} codes")

    # ==================================================================
    # STEP 1: Aggregate individual providers
    # ==================================================================
    print("\n--- Step 1: Aggregate Individual Providers ---")

    individuals = (
        medicaid
        .join(
            npi.select(["NPI", "PROVIDER_NAME", "ENTITY_LABEL", "STATE", "TAXONOMY_CODE"]),
            left_on="BILLING_PROVIDER_NPI_NUM",
            right_on="NPI",
            how="left",
        )
        .filter(pl.col("ENTITY_LABEL") == "Individual")
        .filter(pl.col("TOTAL_PAID") > 0)
        .group_by("BILLING_PROVIDER_NPI_NUM", "PROVIDER_NAME", "STATE", "TAXONOMY_CODE")
        .agg([
            pl.sum("TOTAL_PAID").alias("TOTAL_SPENT"),
            pl.sum("TOTAL_UNIQUE_BENEFICIARIES").alias("BENE_SUM"),  # sum, not true uniques
            pl.sum("TOTAL_CLAIMS").alias("TOTAL_CLAIMS"),
            pl.col("HCPCS_CODE").n_unique().alias("UNIQUE_HCPCS"),
            pl.col("CLAIM_FROM_MONTH").n_unique().alias("MONTHS_ACTIVE"),
            pl.min("CLAIM_FROM_MONTH").alias("FIRST_MONTH"),
            pl.max("CLAIM_FROM_MONTH").alias("LAST_MONTH"),
        ])
        .with_columns([
            (pl.col("TOTAL_SPENT") / pl.col("BENE_SUM")).alias("COST_PER_BENE"),
            (pl.col("TOTAL_CLAIMS") / pl.col("BENE_SUM")).alias("CLAIMS_PER_BENE"),
            (pl.col("TOTAL_SPENT") / pl.col("MONTHS_ACTIVE")).alias("AVG_MONTHLY_SPENDING"),
        ])
        .sort("TOTAL_SPENT", descending=True)
        .collect(engine="streaming")
    )

    print(f"  Total individual providers: {individuals.height:,}")
    print(f"  Total individual spending: ${individuals['TOTAL_SPENT'].sum()/1e9:.2f}B")

    # Write top spenders
    top_spenders = individuals.head(500)
    out_path = OUTPUT_DIR / "individual_top_spenders.csv"
    top_spenders.write_csv(str(out_path))
    print(f"  Wrote {out_path} ({top_spenders.height} rows)")

    print(f"\n  Top 25 individual providers by total spending:")
    with pl.Config(tbl_cols=10, tbl_width_chars=150, fmt_str_lengths=25, fmt_float="mixed"):
        print(individuals.select([
            "PROVIDER_NAME", "STATE", "TAXONOMY_CODE",
            "TOTAL_SPENT", "BENE_SUM", "TOTAL_CLAIMS",
            "COST_PER_BENE", "MONTHS_ACTIVE",
        ]).head(25))

    track("Step 1 - Aggregate", start, mem0)

    # ==================================================================
    # STEP 2: Join NUCC taxonomy for specialty descriptions
    # ==================================================================
    print("\n--- Step 2: Specialty Peer Comparison ---")

    # Join NUCC descriptions
    nucc_lookup = nucc.select([
        pl.col("Code").alias("TAXONOMY_CODE"),
        pl.col("Classification").alias("SPECIALTY_CLASS"),
        pl.col("Specialization").alias("SPECIALTY_DETAIL"),
        pl.col("Display Name").alias("SPECIALTY_NAME"),
    ])

    individuals_with_spec = individuals.join(
        nucc_lookup,
        on="TAXONOMY_CODE",
        how="left",
    )

    # Compute specialty medians (only specialties with enough practitioners)
    specialty_stats = (
        individuals_with_spec
        .filter(pl.col("SPECIALTY_CLASS").is_not_null())
        .group_by("SPECIALTY_CLASS")
        .agg([
            pl.len().alias("PRACTITIONER_COUNT"),
            pl.median("COST_PER_BENE").alias("MEDIAN_COST_PER_BENE"),
            pl.mean("COST_PER_BENE").alias("MEAN_COST_PER_BENE"),
            pl.std("COST_PER_BENE").alias("STD_COST_PER_BENE"),
            pl.median("TOTAL_SPENT").alias("MEDIAN_TOTAL_SPENT"),
            pl.sum("TOTAL_SPENT").alias("SPECIALTY_TOTAL_SPENT"),
        ])
        .filter(pl.col("PRACTITIONER_COUNT") >= SPECIALTY_MIN_PRACTITIONERS)
        .sort("SPECIALTY_TOTAL_SPENT", descending=True)
    )

    print(f"  Specialties with >={SPECIALTY_MIN_PRACTITIONERS} practitioners: {specialty_stats.height}")
    print(f"\n  Top 15 specialties by total spending:")
    with pl.Config(tbl_cols=7, tbl_width_chars=140, fmt_str_lengths=30, fmt_float="mixed"):
        print(specialty_stats.head(15))

    track("Step 2 - Specialty stats", start, mem0)

    # ==================================================================
    # STEP 3: Flag outliers (>5x specialty median cost-per-beneficiary)
    # ==================================================================
    print(f"\n--- Step 3: Flag Outliers (>{OUTLIER_MULTIPLIER}x Specialty Median) ---")

    # Join specialty medians back to individuals
    outlier_candidates = (
        individuals_with_spec
        .join(
            specialty_stats.select(["SPECIALTY_CLASS", "MEDIAN_COST_PER_BENE", "PRACTITIONER_COUNT"]),
            on="SPECIALTY_CLASS",
            how="inner",
        )
        .with_columns(
            (pl.col("COST_PER_BENE") / pl.col("MEDIAN_COST_PER_BENE")).alias("COST_RATIO")
        )
        .filter(pl.col("COST_RATIO") > OUTLIER_MULTIPLIER)
        .sort("COST_RATIO", descending=True)
    )

    print(f"  Individuals exceeding {OUTLIER_MULTIPLIER}x specialty median: {outlier_candidates.height:,}")
    if outlier_candidates.height > 0:
        total_outlier_spend = outlier_candidates["TOTAL_SPENT"].sum()
        print(f"  Combined outlier spending: ${total_outlier_spend/1e9:.2f}B")

        out_path = OUTPUT_DIR / "individual_specialty_outliers.csv"
        outlier_candidates.select([
            "PROVIDER_NAME", "STATE", "SPECIALTY_CLASS", "SPECIALTY_NAME",
            "TOTAL_SPENT", "BENE_SUM", "COST_PER_BENE",
            "MEDIAN_COST_PER_BENE", "COST_RATIO",
            "MONTHS_ACTIVE", "FIRST_MONTH", "LAST_MONTH",
            "BILLING_PROVIDER_NPI_NUM",
        ]).write_csv(str(out_path))
        print(f"  Wrote {out_path} ({outlier_candidates.height} rows)")

        print(f"\n  Top 25 outliers by cost ratio:")
        with pl.Config(tbl_cols=9, tbl_width_chars=150, fmt_str_lengths=25, fmt_float="mixed"):
            print(outlier_candidates.select([
                "PROVIDER_NAME", "STATE", "SPECIALTY_CLASS",
                "TOTAL_SPENT", "COST_PER_BENE", "MEDIAN_COST_PER_BENE",
                "COST_RATIO", "BENE_SUM",
            ]).head(25))

    track("Step 3 - Outlier flagging", start, mem0)

    # ==================================================================
    # STEP 4: OIG Exclusion List cross-reference
    # ==================================================================
    print("\n--- Step 4: OIG Exclusion List Cross-Reference ---")

    oig = load_oig()

    # Match flagged individuals by NPI
    oig_with_npi = oig.filter(
        pl.col("NPI").is_not_null() & (pl.col("NPI") != "") & (pl.col("NPI") != "0000000000")
    )

    # Check all top 500 + all outliers
    check_npis = set(
        top_spenders["BILLING_PROVIDER_NPI_NUM"].to_list()
        + (outlier_candidates["BILLING_PROVIDER_NPI_NUM"].to_list() if outlier_candidates.height > 0 else [])
    )

    oig_matches = oig_with_npi.filter(pl.col("NPI").is_in(list(check_npis)))

    print(f"  Checked {len(check_npis):,} individual NPIs against OIG exclusion list")
    print(f"  Matches found: {oig_matches.height}")

    if oig_matches.height > 0:
        out_path = OUTPUT_DIR / "individual_oig_matches.csv"
        oig_matches.write_csv(str(out_path))
        print(f"  Wrote {out_path} ({oig_matches.height} rows)")
        with pl.Config(tbl_cols=10, tbl_width_chars=150, fmt_str_lengths=30):
            print(oig_matches.head(20))
    else:
        print("  No direct NPI matches in OIG exclusion list")
        out_path = OUTPUT_DIR / "individual_oig_matches.csv"
        pl.DataFrame({"NOTE": ["No NPI matches found"]}).write_csv(str(out_path))

    track("Step 4 - OIG cross-reference", start, mem0)

    # ==================================================================
    # STEP 5: Billing profiles for top 50 flagged individuals
    # ==================================================================
    print("\n--- Step 5: Billing Profiles for Top 50 Flagged Individuals ---")

    if outlier_candidates.height > 0:
        top50_npis = outlier_candidates.head(50)["BILLING_PROVIDER_NPI_NUM"].to_list()

        profiles = (
            medicaid
            .filter(pl.col("BILLING_PROVIDER_NPI_NUM").is_in(top50_npis))
            .filter(pl.col("TOTAL_PAID") > 0)
            .join(
                hcpcs.select(["HCPCS_CODE", "SHORT_DESCRIPTION"]),
                on="HCPCS_CODE",
                how="left",
            )
            .join(
                npi.select(["NPI", "PROVIDER_NAME", "STATE"]),
                left_on="BILLING_PROVIDER_NPI_NUM",
                right_on="NPI",
                how="left",
            )
            .select([
                "BILLING_PROVIDER_NPI_NUM", "PROVIDER_NAME", "STATE",
                "HCPCS_CODE", "SHORT_DESCRIPTION", "CLAIM_FROM_MONTH",
                "TOTAL_CLAIMS", "TOTAL_UNIQUE_BENEFICIARIES", "TOTAL_PAID",
            ])
            .sort(["BILLING_PROVIDER_NPI_NUM", "CLAIM_FROM_MONTH"])
            .collect(engine="streaming")
        )

        print(f"  Billing detail records for top 50 outliers: {profiles.height:,}")
        # Summarize by provider and code
        profile_summary = (
            profiles
            .group_by("BILLING_PROVIDER_NPI_NUM", "PROVIDER_NAME", "STATE", "HCPCS_CODE", "SHORT_DESCRIPTION")
            .agg([
                pl.sum("TOTAL_PAID").alias("TOTAL_PAID"),
                pl.sum("TOTAL_CLAIMS").alias("TOTAL_CLAIMS"),
                pl.len().alias("MONTHS_BILLED"),
            ])
            .sort(["BILLING_PROVIDER_NPI_NUM", "TOTAL_PAID"], descending=[False, True])
        )

        print(f"\n  Sample profiles (first 3 providers):")
        sample_npis = outlier_candidates.head(3)["BILLING_PROVIDER_NPI_NUM"].to_list()
        for npi_num in sample_npis:
            prov = profile_summary.filter(pl.col("BILLING_PROVIDER_NPI_NUM") == npi_num)
            if prov.height > 0:
                name = prov["PROVIDER_NAME"][0]
                state = prov["STATE"][0]
                total = prov["TOTAL_PAID"].sum()
                print(f"\n    {name} ({state}) — ${total:,.0f}")
                with pl.Config(tbl_cols=6, tbl_width_chars=120, fmt_str_lengths=30, fmt_float="mixed"):
                    print(prov.select(["HCPCS_CODE", "SHORT_DESCRIPTION", "TOTAL_PAID", "TOTAL_CLAIMS", "MONTHS_BILLED"]).head(5))

    track("Step 5 - Billing profiles", start, mem0)

    # ==================================================================
    # SUMMARY
    # ==================================================================
    print("\n" + "=" * 100)
    print("INVESTIGATION 3 COMPLETE")
    print("=" * 100)
    total_time = time.time() - start
    print(f"  Total runtime: {total_time:.0f}s | Peak RSS: {get_mem_mb():.0f} MB")
    print(f"\n  Key findings:")
    print(f"    - Total individual providers analyzed: {individuals.height:,}")
    print(f"    - Outliers (>{OUTLIER_MULTIPLIER}x specialty median): {outlier_candidates.height:,}")
    if outlier_candidates.height > 0:
        print(f"    - Highest cost ratio: {outlier_candidates['COST_RATIO'][0]:.1f}x")
    print(f"    - OIG exclusion matches: {oig_matches.height}")
    print(f"\n  Output files:")
    for f in OUTPUT_DIR.glob("individual_*.csv"):
        print(f"    {f.name} ({f.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
