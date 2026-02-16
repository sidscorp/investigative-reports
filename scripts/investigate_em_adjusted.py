#!/usr/bin/env python3
"""
Investigation 7b: E&M Upcoding — Practice-Profile-Adjusted Analysis

Augments the base E&M upcoding analysis (Investigation 7) with practice-profile
covariates and residual-based flagging. Reports BOTH raw and adjusted Z-scores
side by side — the adjusted scores do not replace the raw ones.

Covariates (computed from the full Medicaid dataset per provider per era):
  1. HCPCS_DIVERSITY  — distinct HCPCS codes billed (log-transformed)
  2. EM_RATIO          — E&M claims / total claims
  3. NEW_EST_RATIO     — new-patient E&M claims / total E&M claims
  4. LOG_VOLUME        — log(total claims) — controls for precision inflation

Model:  PWI ~ BENCHMARK_SPECIALTY + covariates  (OLS per era×family)
Flag on: residual Z-scores (residual / specialty residual std)

Output:
  output/em_upcoding_adjusted_post2021.csv
  output/em_upcoding_adjusted_pre2021.csv
  output/em_upcoding_adjustment_summary.csv  — who moved and who stayed
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import polars as pl
import numpy as np
import time
from scripts.lib.data import (
    load_medicaid, OUTPUT_DIR, get_mem_mb, track,
)

ALL_EM_CODES = ["99202", "99203", "99204", "99205",
                "99211", "99212", "99213", "99214", "99215"]
NEW_CODES = ["99202", "99203", "99204", "99205"]
MIN_PEERS = 20


def compute_provider_profiles(medicaid: pl.LazyFrame, era_filter: pl.Expr) -> pl.DataFrame:
    """
    Compute practice-profile covariates for every provider in an era.

    Returns a DataFrame with one row per BILLING_PROVIDER_NPI_NUM:
      HCPCS_DIVERSITY, EM_RATIO, NEW_EST_RATIO, LOG_VOLUME
    """
    era_data = medicaid.filter(era_filter).filter(pl.col("TOTAL_PAID") > 0)

    profiles = (
        era_data
        .group_by("BILLING_PROVIDER_NPI_NUM")
        .agg([
            pl.col("HCPCS_CODE").n_unique().alias("HCPCS_DIVERSITY"),
            pl.sum("TOTAL_CLAIMS").alias("TOTAL_ALL_CLAIMS"),
            # E&M claims
            pl.col("TOTAL_CLAIMS").filter(
                pl.col("HCPCS_CODE").is_in(ALL_EM_CODES)
            ).sum().fill_null(0).alias("EM_CLAIMS"),
            # New-patient E&M claims
            pl.col("TOTAL_CLAIMS").filter(
                pl.col("HCPCS_CODE").is_in(NEW_CODES)
            ).sum().fill_null(0).alias("NEW_EM_CLAIMS"),
        ])
        .with_columns([
            (pl.col("EM_CLAIMS") / pl.col("TOTAL_ALL_CLAIMS")).alias("EM_RATIO"),
            pl.when(pl.col("EM_CLAIMS") > 0)
            .then(pl.col("NEW_EM_CLAIMS") / pl.col("EM_CLAIMS"))
            .otherwise(0.0)
            .alias("NEW_EST_RATIO"),
            pl.col("TOTAL_ALL_CLAIMS").log().alias("LOG_VOLUME"),
        ])
        .collect(engine="streaming")
    )

    return profiles


def fit_and_residualize(
    providers: pl.DataFrame,
    profiles: pl.DataFrame,
    era_label: str,
) -> pl.DataFrame:
    """
    Fit OLS: PWI ~ specialty dummies + covariates, compute residual Z-scores.

    Returns the input DataFrame augmented with:
      ADJ_RESIDUAL, ADJ_Z_SCORE, ADJ_IS_OUTLIER, MOVEMENT
    """
    # Join profiles
    df = providers.join(
        profiles.select([
            "BILLING_PROVIDER_NPI_NUM", "HCPCS_DIVERSITY",
            "EM_RATIO", "NEW_EST_RATIO", "LOG_VOLUME",
        ]),
        on="BILLING_PROVIDER_NPI_NUM",
        how="left",
    )

    # Fill nulls (providers with missing profiles — shouldn't happen, but guard)
    for col in ["HCPCS_DIVERSITY", "EM_RATIO", "NEW_EST_RATIO", "LOG_VOLUME"]:
        median_val = df[col].median()
        df = df.with_columns(pl.col(col).fill_null(median_val))

    # Convert to numpy for OLS
    # Encode specialty as integer codes for dummy matrix
    specialties = df["BENCHMARK_SPECIALTY"].unique().sort().to_list()
    spec_map = {s: i for i, s in enumerate(specialties)}

    y = df["PRICE_WEIGHTED_INDEX"].to_numpy().astype(np.float64)
    spec_idx = df["BENCHMARK_SPECIALTY"].replace_strict(spec_map).to_numpy()

    # Build design matrix: specialty dummies (drop first) + 4 covariates
    n = len(y)
    n_spec = len(specialties)

    # Specialty dummies (one-hot, drop first for identifiability)
    X_spec = np.zeros((n, n_spec - 1), dtype=np.float64)
    for i in range(n):
        s = spec_idx[i]
        if s > 0:
            X_spec[i, s - 1] = 1.0

    # Covariates
    X_cov = np.column_stack([
        df["HCPCS_DIVERSITY"].to_numpy().astype(np.float64),
        df["EM_RATIO"].to_numpy().astype(np.float64),
        df["NEW_EST_RATIO"].to_numpy().astype(np.float64),
        df["LOG_VOLUME"].to_numpy().astype(np.float64),
    ])

    # Standardize covariates for numerical stability
    cov_means = X_cov.mean(axis=0)
    cov_stds = X_cov.std(axis=0)
    cov_stds[cov_stds == 0] = 1.0
    X_cov_std = (X_cov - cov_means) / cov_stds

    # Intercept + dummies + covariates
    X = np.column_stack([np.ones(n), X_spec, X_cov_std])

    # OLS via normal equations (with regularization for numerical stability)
    XtX = X.T @ X
    XtX += 1e-8 * np.eye(XtX.shape[0])  # Tikhonov regularization
    Xty = X.T @ y
    beta = np.linalg.solve(XtX, Xty)

    residuals = y - X @ beta
    r_squared = 1 - np.var(residuals) / np.var(y)
    print(f"    [{era_label}] OLS R² = {r_squared:.4f} "
          f"(covariates explain {r_squared*100:.1f}% of PWI variance)")

    # Print covariate coefficients (unstandardized for interpretation)
    cov_names = ["HCPCS_DIVERSITY", "EM_RATIO", "NEW_EST_RATIO", "LOG_VOLUME"]
    cov_betas = beta[-(len(cov_names)):]
    print(f"    Covariate effects (standardized):")
    for name, b in zip(cov_names, cov_betas):
        print(f"      {name}: {b:+.4f}")

    # Compute residual Z-scores within specialty
    df = df.with_columns(pl.Series("ADJ_RESIDUAL", residuals))

    spec_resid_stats = (
        df.group_by("BENCHMARK_SPECIALTY")
        .agg([
            pl.median("ADJ_RESIDUAL").alias("SPEC_RESID_MEDIAN"),
            pl.col("ADJ_RESIDUAL").std().alias("SPEC_RESID_STD"),
            pl.len().alias("SPEC_N"),
        ])
        .filter(pl.col("SPEC_N") >= MIN_PEERS)
    )

    df = df.join(spec_resid_stats, on="BENCHMARK_SPECIALTY", how="inner")

    df = df.with_columns([
        pl.when(pl.col("SPEC_RESID_STD") > 0)
        .then((pl.col("ADJ_RESIDUAL") - pl.col("SPEC_RESID_MEDIAN")) / pl.col("SPEC_RESID_STD"))
        .otherwise(0.0)
        .alias("ADJ_Z_SCORE"),
    ])

    adj_outlier_expr = pl.col("ADJ_Z_SCORE") >= 2.5
    df = df.with_columns(
        adj_outlier_expr.alias("ADJ_IS_OUTLIER"),
    )
    df = df.with_columns(
        pl.when(pl.col("IS_OUTLIER") & pl.col("ADJ_IS_OUTLIER"))
        .then(pl.lit("PERSISTENT"))          # outlier in both raw and adjusted
        .when(pl.col("IS_OUTLIER") & ~pl.col("ADJ_IS_OUTLIER"))
        .then(pl.lit("EXPLAINED"))           # raw outlier absorbed by covariates
        .when(~pl.col("IS_OUTLIER") & pl.col("ADJ_IS_OUTLIER"))
        .then(pl.lit("UNMASKED"))            # covariates reveal hidden outlier
        .otherwise(pl.lit("NORMAL"))
        .alias("MOVEMENT"),
    )

    return df.drop(["SPEC_RESID_MEDIAN", "SPEC_RESID_STD", "SPEC_N"])


def main():
    print("=" * 100)
    print("INVESTIGATION 7b: E&M UPCODING — PRACTICE-PROFILE ADJUSTMENT")
    print("=" * 100)
    print()

    start = time.time()
    mem0 = get_mem_mb()

    medicaid = load_medicaid()

    results = {}

    for era_label, era_filter in [
        ("pre2021", pl.col("CLAIM_FROM_MONTH") < "2021-01"),
        ("post2021", pl.col("CLAIM_FROM_MONTH") >= "2021-01"),
    ]:
        print(f"\n  === Era: {era_label} ===")

        # Load the base analysis results
        base_path = OUTPUT_DIR / f"em_upcoding_providers_{era_label}.csv"
        base = pl.read_csv(str(base_path),
                           schema_overrides={"BILLING_PROVIDER_NPI_NUM": pl.String})
        print(f"    Loaded {base.height:,} providers from {base_path.name}")

        # Compute profiles
        print(f"    Computing practice profiles...")
        profiles = compute_provider_profiles(medicaid, era_filter)
        # Cast NPI to match
        profiles = profiles.with_columns(
            pl.col("BILLING_PROVIDER_NPI_NUM").cast(pl.String)
        )
        print(f"    Profiles computed for {profiles.height:,} providers")

        track(f"Profiles {era_label}", start, mem0)

        # Fit and residualize
        print(f"    Fitting OLS model...")
        adjusted = fit_and_residualize(base, profiles, era_label)

        # Summary stats
        for cat in ["PERSISTENT", "EXPLAINED", "UNMASKED", "NORMAL"]:
            n = adjusted.filter(pl.col("MOVEMENT") == cat).height
            print(f"    {cat}: {n:,}")

        raw_outliers = adjusted.filter(pl.col("IS_OUTLIER")).height
        adj_outliers = adjusted.filter(pl.col("ADJ_IS_OUTLIER")).height
        persistent = adjusted.filter(pl.col("MOVEMENT") == "PERSISTENT").height
        explained = adjusted.filter(pl.col("MOVEMENT") == "EXPLAINED").height
        unmasked = adjusted.filter(pl.col("MOVEMENT") == "UNMASKED").height

        print(f"\n    Raw outliers: {raw_outliers:,}")
        print(f"    Adjusted outliers: {adj_outliers:,}")
        print(f"    Persistent (both): {persistent:,}")
        print(f"    Explained (raw only): {explained:,}")
        print(f"    Unmasked (adj only): {unmasked:,}")

        # Write output
        out_cols = [
            "BILLING_PROVIDER_NPI_NUM", "PROVIDER_NAME", "STATE",
            "BENCHMARK_SPECIALTY", "Classification", "PROVIDER_TYPE",
            "CODE_FAMILY", "ERA",
            "TOTAL_EM_CLAIMS", "TOTAL_PAID",
            "PRICE_WEIGHTED_INDEX", "MEDIAN_PWI", "Z_SCORE",
            "EST_EXCESS_REVENUE", "EST_EXCESS_REVENUE_CLIPPED",
            "BENE_CLAIM_RATIO", "PEER_COUNT", "IS_OUTLIER",
            # New columns
            "HCPCS_DIVERSITY", "EM_RATIO", "NEW_EST_RATIO", "LOG_VOLUME",
            "ADJ_RESIDUAL", "ADJ_Z_SCORE", "ADJ_IS_OUTLIER", "MOVEMENT",
        ]
        adjusted.select(out_cols).sort("ADJ_Z_SCORE", descending=True).write_csv(
            OUTPUT_DIR / f"em_upcoding_adjusted_{era_label}.csv"
        )

        results[era_label] = adjusted

        track(f"Adjustment {era_label}", start, mem0)

    # Adjustment summary: who moved the most?
    print("\n  === ADJUSTMENT SUMMARY ===")
    all_adjusted = pl.concat([results[e] for e in results])

    # Top 10 providers whose Z-score dropped the most (EXPLAINED)
    explained = (
        all_adjusted
        .filter(pl.col("MOVEMENT") == "EXPLAINED")
        .with_columns(
            (pl.col("Z_SCORE") - pl.col("ADJ_Z_SCORE")).alias("Z_DROP")
        )
        .sort("Z_DROP", descending=True)
    )
    print(f"\n  Top 10 EXPLAINED (raw outlier → no longer outlier after adjustment):")
    print(explained.select([
        "PROVIDER_NAME", "STATE", "BENCHMARK_SPECIALTY", "ERA", "CODE_FAMILY",
        "Z_SCORE", "ADJ_Z_SCORE", "HCPCS_DIVERSITY", "EM_RATIO",
    ]).head(10))

    # Top 10 PERSISTENT with highest adjusted Z
    persistent = (
        all_adjusted
        .filter(pl.col("MOVEMENT") == "PERSISTENT")
        .sort("ADJ_Z_SCORE", descending=True)
    )
    print(f"\n  Top 10 PERSISTENT (outlier in both raw and adjusted):")
    print(persistent.select([
        "PROVIDER_NAME", "STATE", "BENCHMARK_SPECIALTY", "ERA", "CODE_FAMILY",
        "Z_SCORE", "ADJ_Z_SCORE", "HCPCS_DIVERSITY", "EM_RATIO",
    ]).head(10))

    # Top 10 UNMASKED
    unmasked = (
        all_adjusted
        .filter(pl.col("MOVEMENT") == "UNMASKED")
        .sort("ADJ_Z_SCORE", descending=True)
    )
    if unmasked.height > 0:
        print(f"\n  Top 10 UNMASKED (not raw outlier, but outlier after adjustment):")
        print(unmasked.select([
            "PROVIDER_NAME", "STATE", "BENCHMARK_SPECIALTY", "ERA", "CODE_FAMILY",
            "Z_SCORE", "ADJ_Z_SCORE", "HCPCS_DIVERSITY", "EM_RATIO",
        ]).head(10))

    # Write summary
    summary_rows = []
    for era_label in ["pre2021", "post2021"]:
        df = results[era_label]
        for fam in df["CODE_FAMILY"].unique().to_list():
            sub = df.filter(pl.col("CODE_FAMILY") == fam)
            summary_rows.append({
                "ERA": era_label,
                "CODE_FAMILY": fam,
                "TOTAL_PROVIDERS": sub.height,
                "RAW_OUTLIERS": sub.filter(pl.col("IS_OUTLIER")).height,
                "ADJ_OUTLIERS": sub.filter(pl.col("ADJ_IS_OUTLIER")).height,
                "PERSISTENT": sub.filter(pl.col("MOVEMENT") == "PERSISTENT").height,
                "EXPLAINED": sub.filter(pl.col("MOVEMENT") == "EXPLAINED").height,
                "UNMASKED": sub.filter(pl.col("MOVEMENT") == "UNMASKED").height,
            })

    summary = pl.DataFrame(summary_rows)
    summary.write_csv(OUTPUT_DIR / "em_upcoding_adjustment_summary.csv")
    print("\n  Wrote em_upcoding_adjustment_summary.csv")
    print(summary)

    print(f"\nCompleted in {time.time() - start:.1f}s")


if __name__ == "__main__":
    main()
