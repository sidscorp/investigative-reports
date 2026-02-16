#!/usr/bin/env python3
"""
Investigation 7: E&M Upcoding Analysis v2

Methodology (9 improvements over v1):
  1. Price-weighted index (national avg price per code, not ordinal 1-5)
  2. National uniform pricing for excess revenue (eliminates geographic bias)
  3. Sub-specialty benchmarking (NUCC Specialization → Classification fallback)
  4. New-patient and established-patient codes benchmarked separately
  5. Two independent eras: pre-2021 (<2021-01) and post-2021 (>=2021-01)
  6. Z-score primary + absolute deviation secondary for outlier detection
  7. Both clipped and unclipped excess revenue; aggregates use unclipped
  8. Beneficiary-to-claim ratio as reporting column
  9. Case-mix disclaimer: statistical outliers, not confirmed fraud

Output (15 files):
  Per-era combined:      em_upcoding_providers_{pre2021,post2021}.csv
  Per-era per-family:    em_upcoding_providers_{new,est}_{pre2021,post2021}.csv  (4)
  Aggregate tables:      em_upcoding_by_{specialty,state,type}_{pre2021,post2021}.csv  (6)
  Cross-era summary:     em_upcoding_cross_era_summary.csv
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import polars as pl
import time
from scripts.lib.data import (
    load_medicaid, load_npi, load_nucc,
    OUTPUT_DIR, get_mem_mb, track,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
NEW_CODES = ["99202", "99203", "99204", "99205"]
EST_CODES = ["99211", "99212", "99213", "99214", "99215"]
ALL_EM_CODES = NEW_CODES + EST_CODES

MIN_CLAIMS = 500        # Minimum claims per provider per era per code family
MIN_PEERS = 20          # Minimum peer providers for a valid benchmark
Z_THRESHOLD = 2.5       # Z-score threshold for outlier flag
# $5/encounter: sensitivity analysis shows this threshold is non-binding —
# P10 of ABS_DEVIATION among Z≥2.5 providers is ~$10 (pre-2021) to ~$16
# (post-2021). At $3, $5, or $8 the outlier set is identical. The threshold
# only starts to bite at ~$10-15/encounter, pruning the lowest-deviation tail.
MIN_ABS_DEV = 5.0

DISCLAIMER = (
    "DISCLAIMER: This is a screening tool that identifies statistical outliers in E&M\n"
    "coding patterns. It does NOT constitute evidence of fraud, waste, or abuse.\n"
    "\n"
    "KNOWN LIMITATION — NO CASE-MIX ADJUSTMENT: This analysis has no access to\n"
    "diagnosis codes, patient acuity scores, or chart-level data. Providers treating\n"
    "sicker, more complex, or referral-heavy patient populations will legitimately\n"
    "bill higher-level E&M codes. Academic medical centers, children's hospitals, and\n"
    "tertiary referral centers are EXPECTED to appear disproportionately on this list.\n"
    "Their presence reflects patient complexity, not necessarily billing impropriety.\n"
    "\n"
    "ORGANIZATIONAL NPIs: Large health systems may bill under multiple NPIs for what\n"
    "is effectively one clinical practice. Each NPI is benchmarked independently,\n"
    "which may undercount or overcount system-level patterns.\n"
    "\n"
    "These results should be interpreted as flags warranting chart review and further\n"
    "investigation, not as conclusions about any individual provider's billing conduct."
)


# ---------------------------------------------------------------------------
# Taxonomy helper
# ---------------------------------------------------------------------------
def build_taxonomy_map(nucc: pl.DataFrame) -> pl.LazyFrame:
    """
    Build a taxonomy lookup: TAXONOMY_CODE → BENCHMARK_SPECIALTY, Classification, PROVIDER_TYPE.

    BENCHMARK_SPECIALTY uses Specialization where non-null (~73%), falls back to
    Classification (~27%).
    """
    return (
        nucc
        .select([
            pl.col("Code").alias("TAXONOMY_CODE"),
            pl.col("Grouping").alias("PROVIDER_TYPE"),
            pl.col("Classification"),
            pl.col("Specialization"),
        ])
        .unique(subset=["TAXONOMY_CODE"])
        .with_columns(
            pl.when(pl.col("Specialization").is_not_null() & (pl.col("Specialization") != ""))
            .then(pl.col("Specialization"))
            .otherwise(pl.col("Classification"))
            .alias("BENCHMARK_SPECIALTY")
        )
        .lazy()
    )


# ---------------------------------------------------------------------------
# National prices
# ---------------------------------------------------------------------------
def compute_national_prices(em_data: pl.LazyFrame) -> pl.LazyFrame:
    """Compute national average price per E&M code for an era slice."""
    return (
        em_data
        .group_by("HCPCS_CODE")
        .agg(
            (pl.sum("TOTAL_PAID") / pl.sum("TOTAL_CLAIMS")).alias("NATIONAL_AVG_PRICE"),
        )
    )


# ---------------------------------------------------------------------------
# Core analysis for one code family within one era
# ---------------------------------------------------------------------------
def analyze_code_family(
    em_data: pl.LazyFrame,
    national_prices: pl.LazyFrame,
    codes: list[str],
    family_label: str,  # "new" or "est"
    era_label: str,     # "pre2021" or "post2021"
) -> pl.DataFrame | None:
    """
    Analyze one code family (new or established) for one era.

    Returns a DataFrame of flagged + unflagged providers, or None if no data qualifies.
    """

    # 1. Filter to this code family
    family_data = em_data.filter(pl.col("HCPCS_CODE").is_in(codes))

    # 2. Provider-level aggregation: claims per code + totals + bene sum
    provider_codes = (
        family_data
        .group_by("BILLING_PROVIDER_NPI_NUM", "PROVIDER_NAME", "STATE",
                   "BENCHMARK_SPECIALTY", "Classification", "PROVIDER_TYPE", "HCPCS_CODE")
        .agg([
            pl.sum("TOTAL_CLAIMS").alias("CODE_CLAIMS"),
            pl.sum("TOTAL_PAID").alias("CODE_PAID"),
            pl.sum("TOTAL_UNIQUE_BENEFICIARIES").alias("CODE_BENE"),
        ])
    )

    provider_totals = (
        provider_codes
        .group_by("BILLING_PROVIDER_NPI_NUM")
        .agg([
            pl.sum("CODE_CLAIMS").alias("TOTAL_EM_CLAIMS"),
            pl.sum("CODE_PAID").alias("TOTAL_PAID"),
            pl.sum("CODE_BENE").alias("TOTAL_BENE_SUM"),
        ])
        .filter(pl.col("TOTAL_EM_CLAIMS") >= MIN_CLAIMS)
    )

    # 3. Compute each provider's price-weighted index (PWI)
    #    PWI = sum(code_claims * national_price) / total_claims
    provider_pwi = (
        provider_codes
        .join(provider_totals, on="BILLING_PROVIDER_NPI_NUM", how="inner")
        .join(national_prices, on="HCPCS_CODE", how="left")
        .with_columns(
            (pl.col("CODE_CLAIMS") * pl.col("NATIONAL_AVG_PRICE")).alias("WEIGHTED_PRICE")
        )
        .group_by("BILLING_PROVIDER_NPI_NUM", "PROVIDER_NAME", "STATE",
                   "BENCHMARK_SPECIALTY", "Classification", "PROVIDER_TYPE",
                   "TOTAL_EM_CLAIMS", "TOTAL_PAID", "TOTAL_BENE_SUM")
        .agg([
            pl.sum("WEIGHTED_PRICE").alias("TOTAL_WEIGHTED_PRICE"),
        ])
        .with_columns([
            (pl.col("TOTAL_WEIGHTED_PRICE") / pl.col("TOTAL_EM_CLAIMS")).alias("PRICE_WEIGHTED_INDEX"),
            (pl.col("TOTAL_BENE_SUM") / pl.col("TOTAL_EM_CLAIMS")).alias("BENE_CLAIM_RATIO"),
        ])
    )

    # 4. Specialty benchmarks: median, std, P95 of PWI among peers
    specialty_stats = (
        provider_pwi
        .group_by("BENCHMARK_SPECIALTY")
        .agg([
            pl.median("PRICE_WEIGHTED_INDEX").alias("MEDIAN_PWI"),
            pl.col("PRICE_WEIGHTED_INDEX").std().alias("STD_PWI"),
            pl.quantile("PRICE_WEIGHTED_INDEX", 0.95).alias("P95_PWI"),
            pl.len().alias("PEER_COUNT"),
        ])
        .filter(pl.col("PEER_COUNT") >= MIN_PEERS)
    )

    # 5. Join benchmarks, compute Z-score and excess revenue
    #    Excess revenue = (provider PWI - median PWI) * total_claims
    #    This uses national uniform prices, so it represents code-mix deviation only.
    result = (
        provider_pwi
        .join(specialty_stats, on="BENCHMARK_SPECIALTY", how="inner")
        .with_columns([
            # Z-score with STD=0 guard
            pl.when(pl.col("STD_PWI") > 0)
            .then((pl.col("PRICE_WEIGHTED_INDEX") - pl.col("MEDIAN_PWI")) / pl.col("STD_PWI"))
            .otherwise(0.0)
            .alias("Z_SCORE"),
            # Absolute deviation in $ per encounter
            (pl.col("PRICE_WEIGHTED_INDEX") - pl.col("MEDIAN_PWI")).alias("ABS_DEVIATION"),
        ])
        .with_columns([
            # Excess revenue (unclipped — can be negative for downcoders)
            (pl.col("ABS_DEVIATION") * pl.col("TOTAL_EM_CLAIMS")).alias("EST_EXCESS_REVENUE"),
            # Outlier flag: Z >= threshold AND abs deviation >= min
            ((pl.col("Z_SCORE") >= Z_THRESHOLD) & (pl.col("ABS_DEVIATION") >= MIN_ABS_DEV))
            .alias("IS_OUTLIER"),
            (pl.col("PRICE_WEIGHTED_INDEX") > pl.col("P95_PWI")).alias("ABOVE_P95"),
        ])
        .with_columns(
            # Clipped excess (floor at 0 for provider-level reporting)
            pl.col("EST_EXCESS_REVENUE").clip(lower_bound=0).alias("EST_EXCESS_REVENUE_CLIPPED"),
        )
        .with_columns([
            pl.lit(family_label).alias("CODE_FAMILY"),
            pl.lit(era_label).alias("ERA"),
        ])
        .select([
            "BILLING_PROVIDER_NPI_NUM", "PROVIDER_NAME", "STATE",
            "BENCHMARK_SPECIALTY", "Classification", "PROVIDER_TYPE",
            "CODE_FAMILY", "ERA",
            "TOTAL_EM_CLAIMS", "TOTAL_PAID",
            "PRICE_WEIGHTED_INDEX", "MEDIAN_PWI", "P95_PWI", "STD_PWI",
            "Z_SCORE", "ABS_DEVIATION",
            "EST_EXCESS_REVENUE", "EST_EXCESS_REVENUE_CLIPPED",
            "BENE_CLAIM_RATIO", "PEER_COUNT",
            "IS_OUTLIER", "ABOVE_P95",
        ])
        .sort("EST_EXCESS_REVENUE", descending=True)
        .collect(engine="streaming")
    )

    if result.height == 0:
        print(f"    [{era_label}/{family_label}] No qualifying providers")
        return None

    outlier_count = result.filter(pl.col("IS_OUTLIER")).height
    print(f"    [{era_label}/{family_label}] {result.height:,} providers, {outlier_count:,} outliers")

    # Write per-family file
    out_path = OUTPUT_DIR / f"em_upcoding_providers_{family_label}_{era_label}.csv"
    result.write_csv(out_path)
    print(f"    Wrote {out_path.name}")

    return result


# ---------------------------------------------------------------------------
# Aggregate tables for one era
# ---------------------------------------------------------------------------
def write_aggregates(combined: pl.DataFrame, era_label: str):
    """Write specialty, state, and provider-type aggregates using unclipped excess."""

    # By specialty — sorted by outlier rate (not dollar sums, which reflect
    # Medicaid composition more than billing behavior)
    by_specialty = (
        combined
        .group_by("BENCHMARK_SPECIALTY")
        .agg([
            pl.len().alias("PROVIDER_COUNT"),
            pl.mean("PRICE_WEIGHTED_INDEX").alias("AVG_PWI"),
            pl.sum("EST_EXCESS_REVENUE").alias("NET_EXCESS_REVENUE"),
            pl.mean("EST_EXCESS_REVENUE").alias("AVG_EXCESS_PER_PROVIDER"),
            pl.col("IS_OUTLIER").sum().alias("OUTLIER_COUNT"),
            (pl.col("IS_OUTLIER").sum() / pl.len()).alias("OUTLIER_PCT"),
        ])
        .filter(pl.col("PROVIDER_COUNT") >= MIN_PEERS)
        .sort("OUTLIER_PCT", descending=True)
    )
    by_specialty.write_csv(OUTPUT_DIR / f"em_upcoding_by_specialty_{era_label}.csv")

    # By state
    by_state = (
        combined
        .group_by("STATE")
        .agg([
            pl.len().alias("PROVIDER_COUNT"),
            pl.mean("PRICE_WEIGHTED_INDEX").alias("AVG_PWI"),
            pl.sum("EST_EXCESS_REVENUE").alias("NET_EXCESS_REVENUE"),
            pl.mean("EST_EXCESS_REVENUE").alias("AVG_EXCESS_PER_PROVIDER"),
            pl.col("IS_OUTLIER").sum().alias("OUTLIER_COUNT"),
        ])
        .sort("NET_EXCESS_REVENUE", descending=True)
    )
    by_state.write_csv(OUTPUT_DIR / f"em_upcoding_by_state_{era_label}.csv")

    # By provider type
    by_type = (
        combined
        .group_by("PROVIDER_TYPE")
        .agg([
            pl.len().alias("PROVIDER_COUNT"),
            pl.mean("PRICE_WEIGHTED_INDEX").alias("AVG_PWI"),
            pl.sum("EST_EXCESS_REVENUE").alias("NET_EXCESS_REVENUE"),
            pl.col("IS_OUTLIER").sum().alias("OUTLIER_COUNT"),
            (pl.col("IS_OUTLIER").sum() / pl.len()).alias("OUTLIER_PCT"),
        ])
        .filter(pl.col("PROVIDER_COUNT") > 50)
        .sort("OUTLIER_PCT", descending=True)
    )
    by_type.write_csv(OUTPUT_DIR / f"em_upcoding_by_type_{era_label}.csv")

    print(f"    Wrote aggregate tables for {era_label}")


# ---------------------------------------------------------------------------
# Era analysis: runs the full pipeline for one time period
# ---------------------------------------------------------------------------
def run_era_analysis(
    em_base: pl.LazyFrame,
    era_label: str,
    date_filter: pl.Expr,
) -> pl.DataFrame | None:
    """
    Run the complete analysis for one era (pre2021 or post2021).

    Returns the combined provider DataFrame for this era, or None.
    """
    print(f"\n  === Era: {era_label} ===")

    em_data = em_base.filter(date_filter)

    # Compute national prices for this era
    nat_prices = compute_national_prices(em_data)

    # Analyze both code families
    df_new = analyze_code_family(em_data, nat_prices, NEW_CODES, "new", era_label)
    df_est = analyze_code_family(em_data, nat_prices, EST_CODES, "est", era_label)

    # Combine into era-level provider file
    parts = [df for df in [df_new, df_est] if df is not None]
    if not parts:
        print(f"    No qualifying providers in {era_label}")
        return None

    combined = pl.concat(parts)
    combined_path = OUTPUT_DIR / f"em_upcoding_providers_{era_label}.csv"
    combined.write_csv(combined_path)
    print(f"    Wrote {combined_path.name} ({combined.height:,} rows)")

    # Print national prices for verification
    prices_df = nat_prices.sort("HCPCS_CODE").collect()
    print(f"\n    National avg prices ({era_label}):")
    for row in prices_df.iter_rows(named=True):
        print(f"      {row['HCPCS_CODE']}: ${row['NATIONAL_AVG_PRICE']:.2f}")

    # Aggregate tables
    write_aggregates(combined, era_label)

    # Summary stats
    outliers = combined.filter(pl.col("IS_OUTLIER"))
    total_excess = outliers["EST_EXCESS_REVENUE_CLIPPED"].sum()
    print(f"\n    {era_label} summary: {outliers.height:,} outliers, "
          f"${total_excess:,.0f} total clipped excess revenue")

    return combined


# ---------------------------------------------------------------------------
# Cross-era summary
# ---------------------------------------------------------------------------
def cross_era_summary(pre: pl.DataFrame | None, post: pl.DataFrame | None):
    """Find providers flagged as outliers in both eras."""
    if pre is None or post is None:
        print("\n  Cross-era summary: skipped (missing era data)")
        return

    pre_outliers = (
        pre.filter(pl.col("IS_OUTLIER"))
        .select("BILLING_PROVIDER_NPI_NUM")
        .unique()
    )
    post_outliers = (
        post.filter(pl.col("IS_OUTLIER"))
        .select("BILLING_PROVIDER_NPI_NUM")
        .unique()
    )

    both_npis = pre_outliers.join(post_outliers, on="BILLING_PROVIDER_NPI_NUM", how="inner")

    if both_npis.height == 0:
        print("\n  Cross-era summary: 0 providers flagged in both eras")
        # Write empty file with schema
        empty = pl.DataFrame({
            "BILLING_PROVIDER_NPI_NUM": pl.Series([], dtype=pl.String),
            "PROVIDER_NAME": pl.Series([], dtype=pl.String),
            "STATE": pl.Series([], dtype=pl.String),
            "BENCHMARK_SPECIALTY": pl.Series([], dtype=pl.String),
            "PRE2021_Z_SCORE": pl.Series([], dtype=pl.Float64),
            "PRE2021_EXCESS": pl.Series([], dtype=pl.Float64),
            "POST2021_Z_SCORE": pl.Series([], dtype=pl.Float64),
            "POST2021_EXCESS": pl.Series([], dtype=pl.Float64),
        })
        empty.write_csv(OUTPUT_DIR / "em_upcoding_cross_era_summary.csv")
        return

    # Build summary: one row per provider with both-era metrics
    # Aggregate across code families within each era (take max Z, sum excess)
    def era_agg(df: pl.DataFrame, prefix: str) -> pl.DataFrame:
        return (
            df.filter(pl.col("IS_OUTLIER"))
            .group_by("BILLING_PROVIDER_NPI_NUM", "PROVIDER_NAME", "STATE", "BENCHMARK_SPECIALTY")
            .agg([
                pl.max("Z_SCORE").alias(f"{prefix}_Z_SCORE"),
                pl.sum("EST_EXCESS_REVENUE_CLIPPED").alias(f"{prefix}_EXCESS"),
                pl.sum("TOTAL_EM_CLAIMS").alias(f"{prefix}_CLAIMS"),
            ])
        )

    pre_agg = era_agg(pre, "PRE2021")
    post_agg = era_agg(post, "POST2021")

    summary = (
        pre_agg
        .join(
            post_agg.select([
                "BILLING_PROVIDER_NPI_NUM",
                "POST2021_Z_SCORE", "POST2021_EXCESS", "POST2021_CLAIMS",
            ]),
            on="BILLING_PROVIDER_NPI_NUM",
            how="inner",
        )
        .sort("POST2021_EXCESS", descending=True)
    )

    summary.write_csv(OUTPUT_DIR / "em_upcoding_cross_era_summary.csv")
    total_combined = summary["PRE2021_EXCESS"].sum() + summary["POST2021_EXCESS"].sum()
    print(f"\n  Cross-era summary: {summary.height:,} providers flagged in BOTH eras")
    print(f"  Combined clipped excess: ${total_combined:,.0f}")

    if summary.height > 0:
        print("\n  Top 5 cross-era outliers:")
        print(summary.select([
            "PROVIDER_NAME", "STATE", "BENCHMARK_SPECIALTY",
            "PRE2021_Z_SCORE", "PRE2021_EXCESS",
            "POST2021_Z_SCORE", "POST2021_EXCESS",
        ]).head(5))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("=" * 100)
    print("INVESTIGATION 7: E&M UPCODING ANALYSIS v2")
    print("=" * 100)
    print()
    print(DISCLAIMER)
    print()

    start = time.time()
    mem0 = get_mem_mb()

    # ── Load data ──────────────────────────────────────────────────────────
    print("  Loading datasets...")
    medicaid = load_medicaid()
    npi = load_npi()
    nucc = load_nucc()

    taxonomy_map = build_taxonomy_map(nucc)

    # ── Build base E&M LazyFrame ───────────────────────────────────────────
    print("  Filtering E&M claims and joining taxonomy...")
    em_base = (
        medicaid
        .filter(pl.col("HCPCS_CODE").is_in(ALL_EM_CODES))
        .filter(pl.col("TOTAL_PAID") > 0)
        .join(
            npi.select(["NPI", "PROVIDER_NAME", "STATE", "TAXONOMY_CODE"]),
            left_on="BILLING_PROVIDER_NPI_NUM",
            right_on="NPI",
            how="left",
        )
        .join(taxonomy_map, on="TAXONOMY_CODE", how="left")
        # Exclude providers with no taxonomy match
        .filter(pl.col("BENCHMARK_SPECIALTY").is_not_null())
    )

    track("Data prep", start, mem0)

    # ── Run both eras ──────────────────────────────────────────────────────
    pre = run_era_analysis(
        em_base,
        era_label="pre2021",
        date_filter=pl.col("CLAIM_FROM_MONTH") < "2021-01",
    )

    track("Pre-2021 era", start, mem0)

    post = run_era_analysis(
        em_base,
        era_label="post2021",
        date_filter=pl.col("CLAIM_FROM_MONTH") >= "2021-01",
    )

    track("Post-2021 era", start, mem0)

    # ── Cross-era summary ──────────────────────────────────────────────────
    cross_era_summary(pre, post)

    track("Cross-era summary", start, mem0)

    # ── Final output ───────────────────────────────────────────────────────
    print("\n" + "=" * 100)
    print("OUTPUT FILES:")
    for f in sorted(OUTPUT_DIR.glob("em_upcoding_*.csv")):
        size_kb = f.stat().st_size / 1024
        print(f"  {f.name:50s} {size_kb:>8.1f} KB")
    print("=" * 100)
    print(f"\nCompleted in {time.time() - start:.1f}s")
    print(f"\n{DISCLAIMER}")


if __name__ == "__main__":
    main()
