#!/usr/bin/env python3
"""
Investigation 7c: E&M Upcoding — Cross-Investigation Convergence Analysis

Joins E&M outlier flags against signals from 5 prior investigations to identify
providers with converging evidence from independent methodologies.

Signal types (not just counts — categorized):
  BILLING_ANOMALY:
    - E&M upcoding outlier (this analysis)
    - Individual specialty cost-ratio outlier (Inv 3)
    - Temporal spending spike (Inv 4)
  FRAUD_INFRASTRUCTURE:
    - Ghost provider / impossible volume (Inv 5)
    - Shell company connection (Inv 5)
  REGULATORY:
    - OIG exclusion list match (LEIE)
  TEMPORAL:
    - Temporal disappearance (Inv 4)
    - Fast-starter / new entrant (Inv 4)

Output:
  output/em_upcoding_convergence.csv   — all E&M outliers with signal columns
  output/em_upcoding_convergence_flagged.csv — providers with 2+ signal types
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import polars as pl
import time
from scripts.lib.data import OUTPUT_DIR, get_mem_mb, track


def load_signal_npis(filename: str, npi_col: str, signal_name: str,
                     extra_cols: list[str] | None = None) -> pl.DataFrame:
    """Load a prior investigation output and extract NPI set with signal flag."""
    path = OUTPUT_DIR / filename
    if not path.exists():
        print(f"    SKIP: {filename} not found")
        return pl.DataFrame({"NPI": pl.Series([], dtype=pl.String)})

    df = pl.read_csv(str(path), schema_overrides={npi_col: pl.String},
                     infer_schema_length=5000)
    print(f"    {filename}: {df.height:,} rows")

    result = df.select(pl.col(npi_col).alias("NPI")).unique()

    if extra_cols:
        extras = df.select([pl.col(npi_col).alias("NPI")] +
                           [pl.col(c) for c in extra_cols if c in df.columns])
        return extras

    return result


def main():
    print("=" * 100)
    print("INVESTIGATION 7c: E&M UPCODING — CROSS-INVESTIGATION CONVERGENCE")
    print("=" * 100)

    start = time.time()
    mem0 = get_mem_mb()

    # ── Load E&M outliers (post-2021 as primary, pre-2021 for cross-era) ──
    print("\n  Loading E&M outlier data...")
    post = pl.read_csv(str(OUTPUT_DIR / "em_upcoding_providers_post2021.csv"),
                       schema_overrides={"BILLING_PROVIDER_NPI_NUM": pl.String})
    pre = pl.read_csv(str(OUTPUT_DIR / "em_upcoding_providers_pre2021.csv"),
                      schema_overrides={"BILLING_PROVIDER_NPI_NUM": pl.String})

    # Cross-era outliers
    cross_era = pl.read_csv(str(OUTPUT_DIR / "em_upcoding_cross_era_summary.csv"),
                            schema_overrides={"BILLING_PROVIDER_NPI_NUM": pl.String})

    # Combined unique outlier NPIs (flagged in either era)
    post_outlier_npis = (post.filter(pl.col("IS_OUTLIER"))
                         .select("BILLING_PROVIDER_NPI_NUM").unique())
    pre_outlier_npis = (pre.filter(pl.col("IS_OUTLIER"))
                        .select("BILLING_PROVIDER_NPI_NUM").unique())
    all_outlier_npis = pl.concat([post_outlier_npis, pre_outlier_npis]).unique()
    print(f"  Unique E&M outlier NPIs (either era): {all_outlier_npis.height:,}")

    # ── Load prior investigation signals ──────────────────────────────────
    print("\n  Loading prior investigation signals...")

    # BILLING ANOMALY signals
    specialty_outliers = load_signal_npis(
        "individual_specialty_outliers.csv", "BILLING_PROVIDER_NPI_NUM",
        "SPECIALTY_OUTLIER"
    )
    temporal_spikes = load_signal_npis(
        "temporal_spikes.csv", "BILLING_PROVIDER_NPI_NUM",
        "TEMPORAL_SPIKE"
    )

    # FRAUD INFRASTRUCTURE signals
    ghost_impossible = load_signal_npis(
        "ghost_providers_impossible_volume.csv", "BILLING_PROVIDER_NPI_NUM",
        "GHOST_IMPOSSIBLE_VOLUME"
    )
    # Shell company connections (NPIs embedded in PROVIDERS_SAMPLE text)
    # These are address-level, not directly NPI-joinable in a clean way.
    # Skip for now — flag if ghost_impossible overlaps.

    # REGULATORY signals
    oig_matches = load_signal_npis(
        "individual_oig_matches.csv", "NPI", "OIG_EXCLUDED"
    )

    # TEMPORAL signals
    temporal_disappear = load_signal_npis(
        "temporal_disappearances.csv", "BILLING_PROVIDER_NPI_NUM",
        "TEMPORAL_DISAPPEARANCE"
    )
    temporal_fast_start = load_signal_npis(
        "temporal_new_entrants.csv", "BILLING_PROVIDER_NPI_NUM",
        "FAST_STARTER"
    )

    track("Signal loading", start, mem0)

    # ── Build convergence table ───────────────────────────────────────────
    print("\n  Building convergence table...")

    # Start with ALL E&M outlier NPIs and their best-era info
    # Use post-2021 as primary, fall back to pre-2021
    post_outliers = (
        post.filter(pl.col("IS_OUTLIER"))
        .sort("EST_EXCESS_REVENUE_CLIPPED", descending=True)
        .unique(subset=["BILLING_PROVIDER_NPI_NUM"], keep="first")
        .select([
            "BILLING_PROVIDER_NPI_NUM", "PROVIDER_NAME", "STATE",
            "BENCHMARK_SPECIALTY", "PROVIDER_TYPE", "CODE_FAMILY",
            "TOTAL_EM_CLAIMS", "Z_SCORE", "EST_EXCESS_REVENUE_CLIPPED",
            "BENE_CLAIM_RATIO",
        ])
        .with_columns(pl.lit("post2021").alias("PRIMARY_ERA"))
    )

    pre_only = (
        pre.filter(pl.col("IS_OUTLIER"))
        .filter(~pl.col("BILLING_PROVIDER_NPI_NUM").is_in(
            post_outliers["BILLING_PROVIDER_NPI_NUM"]))
        .sort("EST_EXCESS_REVENUE_CLIPPED", descending=True)
        .unique(subset=["BILLING_PROVIDER_NPI_NUM"], keep="first")
        .select([
            "BILLING_PROVIDER_NPI_NUM", "PROVIDER_NAME", "STATE",
            "BENCHMARK_SPECIALTY", "PROVIDER_TYPE", "CODE_FAMILY",
            "TOTAL_EM_CLAIMS", "Z_SCORE", "EST_EXCESS_REVENUE_CLIPPED",
            "BENE_CLAIM_RATIO",
        ])
        .with_columns(pl.lit("pre2021").alias("PRIMARY_ERA"))
    )

    base = pl.concat([post_outliers, pre_only])
    print(f"  Base outlier set: {base.height:,} unique NPIs")

    # Join each signal as a boolean column
    def add_signal(df: pl.DataFrame, signal_npis: pl.DataFrame,
                   col_name: str) -> pl.DataFrame:
        signal_set = set(signal_npis["NPI"].to_list())
        return df.with_columns(
            pl.col("BILLING_PROVIDER_NPI_NUM").is_in(signal_set).alias(col_name)
        )

    conv = base
    conv = add_signal(conv, specialty_outliers, "SIG_SPECIALTY_OUTLIER")
    conv = add_signal(conv, temporal_spikes, "SIG_TEMPORAL_SPIKE")
    conv = add_signal(conv, ghost_impossible, "SIG_GHOST_PROVIDER")
    conv = add_signal(conv, oig_matches, "SIG_OIG_EXCLUDED")
    conv = add_signal(conv, temporal_disappear, "SIG_TEMPORAL_DISAPPEARANCE")
    conv = add_signal(conv, temporal_fast_start, "SIG_FAST_STARTER")

    # Cross-era flag
    cross_era_set = set(cross_era["BILLING_PROVIDER_NPI_NUM"].to_list())
    conv = conv.with_columns(
        pl.col("BILLING_PROVIDER_NPI_NUM").is_in(cross_era_set).alias("SIG_CROSS_ERA")
    )

    # Compute signal type counts (not just total signals)
    conv = conv.with_columns([
        # Billing anomaly signals (E&M is always true here, so count the others)
        (pl.col("SIG_SPECIALTY_OUTLIER").cast(pl.Int8) +
         pl.col("SIG_TEMPORAL_SPIKE").cast(pl.Int8))
        .alias("BILLING_ANOMALY_COUNT"),

        # Fraud infrastructure
        pl.col("SIG_GHOST_PROVIDER").cast(pl.Int8).alias("FRAUD_INFRA_COUNT"),

        # Regulatory
        pl.col("SIG_OIG_EXCLUDED").cast(pl.Int8).alias("REGULATORY_COUNT"),

        # Temporal pattern
        (pl.col("SIG_TEMPORAL_DISAPPEARANCE").cast(pl.Int8) +
         pl.col("SIG_FAST_STARTER").cast(pl.Int8))
        .alias("TEMPORAL_COUNT"),
    ])

    # Total distinct signal types (excluding E&M itself and cross-era which is E&M-derived)
    conv = conv.with_columns(
        (pl.when(pl.col("BILLING_ANOMALY_COUNT") > 0).then(1).otherwise(0) +
         pl.when(pl.col("FRAUD_INFRA_COUNT") > 0).then(1).otherwise(0) +
         pl.when(pl.col("REGULATORY_COUNT") > 0).then(1).otherwise(0) +
         pl.when(pl.col("TEMPORAL_COUNT") > 0).then(1).otherwise(0))
        .alias("SIGNAL_TYPE_COUNT")
    )

    # Total individual signal count
    conv = conv.with_columns(
        (pl.col("SIG_SPECIALTY_OUTLIER").cast(pl.Int8) +
         pl.col("SIG_TEMPORAL_SPIKE").cast(pl.Int8) +
         pl.col("SIG_GHOST_PROVIDER").cast(pl.Int8) +
         pl.col("SIG_OIG_EXCLUDED").cast(pl.Int8) +
         pl.col("SIG_TEMPORAL_DISAPPEARANCE").cast(pl.Int8) +
         pl.col("SIG_FAST_STARTER").cast(pl.Int8) +
         pl.col("SIG_CROSS_ERA").cast(pl.Int8))
        .alias("TOTAL_SIGNAL_COUNT")
    )

    conv = conv.sort("TOTAL_SIGNAL_COUNT", descending=True)

    # Write full convergence table
    conv.write_csv(OUTPUT_DIR / "em_upcoding_convergence.csv")
    print(f"  Wrote em_upcoding_convergence.csv ({conv.height:,} rows)")

    # Flagged subset: any signal beyond E&M itself
    flagged = conv.filter(pl.col("TOTAL_SIGNAL_COUNT") > 0)
    flagged.write_csv(OUTPUT_DIR / "em_upcoding_convergence_flagged.csv")
    print(f"  Wrote em_upcoding_convergence_flagged.csv ({flagged.height:,} rows)")

    track("Convergence build", start, mem0)

    # ── Summary statistics ────────────────────────────────────────────────
    print("\n  === CONVERGENCE SUMMARY ===")
    print(f"  Total E&M outliers: {conv.height:,}")
    print(f"  With any additional signal: {flagged.height:,} "
          f"({flagged.height/conv.height*100:.1f}%)")

    # By signal type
    print("\n  Signal prevalence among E&M outliers:")
    for sig_col, label in [
        ("SIG_SPECIALTY_OUTLIER", "Specialty cost-ratio outlier (Inv 3)"),
        ("SIG_TEMPORAL_SPIKE", "Temporal spending spike (Inv 4)"),
        ("SIG_GHOST_PROVIDER", "Ghost provider / impossible volume (Inv 5)"),
        ("SIG_OIG_EXCLUDED", "OIG exclusion list match (LEIE)"),
        ("SIG_TEMPORAL_DISAPPEARANCE", "Temporal disappearance (Inv 4)"),
        ("SIG_FAST_STARTER", "Fast starter / new entrant (Inv 4)"),
        ("SIG_CROSS_ERA", "Cross-era E&M outlier (Inv 7)"),
    ]:
        n = conv.filter(pl.col(sig_col)).height
        pct = n / conv.height * 100
        print(f"    {label}: {n:,} ({pct:.1f}%)")

    # By signal type count
    print("\n  Distinct signal type count:")
    for n_types in range(5, -1, -1):
        n = conv.filter(pl.col("SIGNAL_TYPE_COUNT") >= n_types).height
        if n > 0:
            print(f"    >= {n_types} signal types: {n:,}")

    # Highest convergence providers
    multi = conv.filter(pl.col("SIGNAL_TYPE_COUNT") >= 2)
    if multi.height > 0:
        print(f"\n  === PROVIDERS WITH 2+ SIGNAL TYPES ({multi.height}) ===")
        print(multi.select([
            "PROVIDER_NAME", "STATE", "BENCHMARK_SPECIALTY", "Z_SCORE",
            "SIG_SPECIALTY_OUTLIER", "SIG_TEMPORAL_SPIKE", "SIG_GHOST_PROVIDER",
            "SIG_OIG_EXCLUDED", "SIG_TEMPORAL_DISAPPEARANCE", "SIG_FAST_STARTER",
            "SIG_CROSS_ERA", "SIGNAL_TYPE_COUNT",
        ]).head(20))

    # OIG matches specifically
    oig = conv.filter(pl.col("SIG_OIG_EXCLUDED"))
    if oig.height > 0:
        print(f"\n  === OIG-EXCLUDED + E&M OUTLIER ({oig.height}) ===")
        print(oig.select([
            "PROVIDER_NAME", "STATE", "BENCHMARK_SPECIALTY",
            "Z_SCORE", "EST_EXCESS_REVENUE_CLIPPED",
            "TOTAL_SIGNAL_COUNT",
        ]))

    # Ghost providers
    ghosts = conv.filter(pl.col("SIG_GHOST_PROVIDER"))
    if ghosts.height > 0:
        print(f"\n  === GHOST PROVIDER + E&M OUTLIER ({ghosts.height}) ===")
        print(ghosts.select([
            "PROVIDER_NAME", "STATE", "BENCHMARK_SPECIALTY",
            "Z_SCORE", "EST_EXCESS_REVENUE_CLIPPED",
            "TOTAL_SIGNAL_COUNT",
        ]))

    print(f"\nCompleted in {time.time() - start:.1f}s")


if __name__ == "__main__":
    main()
