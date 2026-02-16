#!/usr/bin/env python3
"""
Investigation 4: Temporal Anomaly Detection

Public question: "Which providers had suspicious billing spikes that
could indicate the start of a fraud scheme?"

Analyses:
1. Monthly time series per billing NPI
2. Flag >5x month-over-month spending increase ("sudden ramp-up")
3. Flag post-2022 entrants who immediately bill >$1M/month ("fast starters")
4. Flag providers whose billing dropped to $0 after sustained activity ("sudden disappearance")
5. Correlate disappearances with known enforcement dates

Output:
  output/temporal_spikes.csv
  output/temporal_new_entrants.csv
  output/temporal_disappearances.csv
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import polars as pl
import time
from scripts.lib.data import (
    load_medicaid, load_npi, load_hcpcs,
    OUTPUT_DIR, get_mem_mb, track,
)

# Thresholds
MOM_SPIKE_RATIO = 5       # >5x month-over-month increase
FAST_STARTER_THRESHOLD = 1_000_000  # >$1M/month
FAST_STARTER_AFTER = "2022-01"  # first appeared after Jan 2022
SUSTAINED_MIN_MONTHS = 6   # min months of billing before disappearance
RECENT_CUTOFF = "2024-06"  # consider billing ending before this as "disappeared"

# Known enforcement dates (YYYY-MM format matching CLAIM_FROM_MONTH)
ENFORCEMENT_DATES = {
    "MN autism indictments (Dec 2025)": "2025-12",
    "DOJ national takedown (Jun 2024)": "2024-06",
}


def main():
    print("=" * 100)
    print("INVESTIGATION 4: TEMPORAL ANOMALY DETECTION")
    print("=" * 100)

    start = time.time()
    mem0 = get_mem_mb()

    medicaid = load_medicaid()
    npi = load_npi()

    # ==================================================================
    # STEP 1: Build monthly time series per NPI
    # ==================================================================
    print("\n--- Step 1: Building Monthly Time Series ---")

    monthly = (
        medicaid
        .filter(pl.col("TOTAL_PAID") > 0)
        .group_by("BILLING_PROVIDER_NPI_NUM", "CLAIM_FROM_MONTH")
        .agg([
            pl.sum("TOTAL_PAID").alias("MONTHLY_PAID"),
            pl.sum("TOTAL_CLAIMS").alias("MONTHLY_CLAIMS"),
            pl.sum("TOTAL_UNIQUE_BENEFICIARIES").alias("MONTHLY_BENE_SUM"),
        ])
        .sort(["BILLING_PROVIDER_NPI_NUM", "CLAIM_FROM_MONTH"])
        .collect(engine="streaming")
    )

    n_providers = monthly["BILLING_PROVIDER_NPI_NUM"].n_unique()
    n_months = monthly["CLAIM_FROM_MONTH"].n_unique()
    print(f"  Time series built: {monthly.height:,} provider-months")
    print(f"  Unique providers: {n_providers:,}")
    print(f"  Unique months: {n_months}")

    track("Step 1 - Time series", start, mem0)

    # ==================================================================
    # STEP 2: Flag >5x month-over-month spikes
    # ==================================================================
    print(f"\n--- Step 2: Month-over-Month Spikes (>{MOM_SPIKE_RATIO}x) ---")

    with_lag = monthly.with_columns([
        pl.col("MONTHLY_PAID")
        .shift(1)
        .over("BILLING_PROVIDER_NPI_NUM")
        .alias("PREV_MONTH_PAID"),
        pl.col("CLAIM_FROM_MONTH")
        .shift(1)
        .over("BILLING_PROVIDER_NPI_NUM")
        .alias("PREV_CLAIM_MONTH"),
    ])

    # Compute month gap to ensure consecutive months only
    # Parse YYYY-MM to integer months-since-epoch for gap calculation
    with_lag = with_lag.with_columns([
        (pl.col("CLAIM_FROM_MONTH").str.slice(0, 4).cast(pl.Int32) * 12
         + pl.col("CLAIM_FROM_MONTH").str.slice(5, 2).cast(pl.Int32))
        .alias("_cur_months"),
        (pl.col("PREV_CLAIM_MONTH").str.slice(0, 4).cast(pl.Int32) * 12
         + pl.col("PREV_CLAIM_MONTH").str.slice(5, 2).cast(pl.Int32))
        .alias("_prev_months"),
    ]).with_columns(
        (pl.col("_cur_months") - pl.col("_prev_months")).alias("MONTH_GAP")
    )

    spikes = (
        with_lag
        .filter(pl.col("PREV_MONTH_PAID") > 0)
        .filter(pl.col("MONTH_GAP") == 1)  # consecutive months only
        .with_columns(
            (pl.col("MONTHLY_PAID") / pl.col("PREV_MONTH_PAID")).alias("MOM_RATIO")
        )
        .filter(pl.col("MOM_RATIO") > MOM_SPIKE_RATIO)
        .filter(pl.col("MONTHLY_PAID") > 100_000)  # min absolute threshold
        .drop(["_cur_months", "_prev_months", "PREV_CLAIM_MONTH"])
        .sort("MOM_RATIO", descending=True)
    )

    # Enrich with provider names
    npi_names = npi.select(["NPI", "PROVIDER_NAME", "ENTITY_LABEL", "STATE"]).collect()

    spikes_enriched = spikes.join(
        npi_names,
        left_on="BILLING_PROVIDER_NPI_NUM",
        right_on="NPI",
        how="left",
    )

    n_spike_providers = spikes_enriched["BILLING_PROVIDER_NPI_NUM"].n_unique()
    print(f"  Provider-months with >{MOM_SPIKE_RATIO}x MoM spike (>$100K): {spikes_enriched.height:,}")
    print(f"  Unique providers with spikes: {n_spike_providers:,}")

    if spikes_enriched.height > 0:
        # One row per provider (worst spike)
        spike_summary = (
            spikes_enriched
            .group_by("BILLING_PROVIDER_NPI_NUM", "PROVIDER_NAME", "ENTITY_LABEL", "STATE")
            .agg([
                pl.max("MOM_RATIO").alias("MAX_MOM_RATIO"),
                pl.len().alias("SPIKE_COUNT"),
                pl.max("MONTHLY_PAID").alias("MAX_SPIKE_AMOUNT"),
                pl.col("CLAIM_FROM_MONTH")
                .sort_by("MOM_RATIO", descending=True)
                .first()
                .alias("WORST_SPIKE_MONTH"),
            ])
            .sort("MAX_MOM_RATIO", descending=True)
        )

        out_path = OUTPUT_DIR / "temporal_spikes.csv"
        spike_summary.write_csv(str(out_path))
        print(f"  Wrote {out_path} ({spike_summary.height} rows)")

        print(f"\n  Top 20 providers by largest MoM spike:")
        with pl.Config(tbl_cols=8, tbl_width_chars=150, fmt_str_lengths=30, fmt_float="mixed"):
            print(spike_summary.head(20))

    track("Step 2 - MoM spikes", start, mem0)

    # ==================================================================
    # STEP 3: Fast starters (post-2022, >$1M/month immediately)
    # ==================================================================
    print(f"\n--- Step 3: Fast Starters (after {FAST_STARTER_AFTER}, >{FAST_STARTER_THRESHOLD/1e6:.0f}M/month) ---")

    # Find each provider's first billing month
    first_months = (
        monthly
        .group_by("BILLING_PROVIDER_NPI_NUM")
        .agg(pl.min("CLAIM_FROM_MONTH").alias("FIRST_MONTH"))
    )

    # Filter to those who first appeared after the cutoff
    new_entrants = first_months.filter(pl.col("FIRST_MONTH") > FAST_STARTER_AFTER)

    # Join back to get their monthly data
    new_entrant_monthly = (
        monthly
        .join(new_entrants, on="BILLING_PROVIDER_NPI_NUM")
        .filter(pl.col("MONTHLY_PAID") > FAST_STARTER_THRESHOLD)
    )

    # Enrich
    fast_starters = (
        new_entrant_monthly
        .group_by("BILLING_PROVIDER_NPI_NUM")
        .agg([
            pl.min("FIRST_MONTH").alias("FIRST_MONTH"),
            pl.max("MONTHLY_PAID").alias("MAX_MONTHLY_PAID"),
            pl.len().alias("MONTHS_OVER_1M"),
            pl.sum("MONTHLY_PAID").alias("TOTAL_PAID"),
        ])
        .join(npi_names, left_on="BILLING_PROVIDER_NPI_NUM", right_on="NPI", how="left")
        .sort("MAX_MONTHLY_PAID", descending=True)
    )

    print(f"  Providers first appearing after {FAST_STARTER_AFTER}: {new_entrants.height:,}")
    print(f"  Fast starters (>{FAST_STARTER_THRESHOLD/1e6:.0f}M in any month): {fast_starters.height:,}")

    if fast_starters.height > 0:
        out_path = OUTPUT_DIR / "temporal_new_entrants.csv"
        fast_starters.write_csv(str(out_path))
        print(f"  Wrote {out_path} ({fast_starters.height} rows)")

        print(f"\n  Top 20 fast starters:")
        with pl.Config(tbl_cols=8, tbl_width_chars=150, fmt_str_lengths=30, fmt_float="mixed"):
            print(fast_starters.select([
                "PROVIDER_NAME", "ENTITY_LABEL", "STATE",
                "FIRST_MONTH", "MAX_MONTHLY_PAID", "MONTHS_OVER_1M", "TOTAL_PAID",
            ]).head(20))

    track("Step 3 - Fast starters", start, mem0)

    # ==================================================================
    # STEP 4: Sudden disappearances
    # ==================================================================
    print(f"\n--- Step 4: Sudden Disappearances ---")

    # Providers with sustained billing who then stopped
    provider_spans = (
        monthly
        .group_by("BILLING_PROVIDER_NPI_NUM")
        .agg([
            pl.min("CLAIM_FROM_MONTH").alias("FIRST_MONTH"),
            pl.max("CLAIM_FROM_MONTH").alias("LAST_MONTH"),
            pl.len().alias("MONTHS_ACTIVE"),
            pl.sum("MONTHLY_PAID").alias("TOTAL_PAID"),
            pl.mean("MONTHLY_PAID").alias("AVG_MONTHLY_PAID"),
        ])
    )

    disappearances = (
        provider_spans
        .filter(pl.col("MONTHS_ACTIVE") >= SUSTAINED_MIN_MONTHS)
        .filter(pl.col("LAST_MONTH") < RECENT_CUTOFF)
        .filter(pl.col("AVG_MONTHLY_PAID") > 50_000)  # meaningful volume
        .join(npi_names, left_on="BILLING_PROVIDER_NPI_NUM", right_on="NPI", how="left")
        .sort("TOTAL_PAID", descending=True)
    )

    print(f"  Providers who stopped billing before {RECENT_CUTOFF} (>={SUSTAINED_MIN_MONTHS} months active, >$50K avg): {disappearances.height:,}")

    if disappearances.height > 0:
        # Check correlation with enforcement dates
        # Since CLAIM_FROM_MONTH is YYYY-MM string format, string comparison works for ordering
        for label, enforcement_month in ENFORCEMENT_DATES.items():
            # Parse year-month for window computation
            ey, em = int(enforcement_month[:4]), int(enforcement_month[5:7])
            # 3 months before
            sm, sy = em - 3, ey
            if sm <= 0:
                sm += 12
                sy -= 1
            # 3 months after
            am, ay = em + 3, ey
            if am > 12:
                am -= 12
                ay += 1
            window_start = f"{sy:04d}-{sm:02d}"
            window_end = f"{ay:04d}-{am:02d}"
            correlated = disappearances.filter(
                (pl.col("LAST_MONTH") >= window_start)
                & (pl.col("LAST_MONTH") <= window_end)
            )
            print(f"  Disappeared near {label}: {correlated.height}")

        out_path = OUTPUT_DIR / "temporal_disappearances.csv"
        disappearances.write_csv(str(out_path))
        print(f"  Wrote {out_path} ({disappearances.height} rows)")

        print(f"\n  Top 20 disappearances by total spending:")
        with pl.Config(tbl_cols=8, tbl_width_chars=150, fmt_str_lengths=30, fmt_float="mixed"):
            print(disappearances.select([
                "PROVIDER_NAME", "ENTITY_LABEL", "STATE",
                "FIRST_MONTH", "LAST_MONTH", "MONTHS_ACTIVE",
                "TOTAL_PAID", "AVG_MONTHLY_PAID",
            ]).head(20))

    track("Step 4 - Disappearances", start, mem0)

    # ==================================================================
    # SUMMARY
    # ==================================================================
    print("\n" + "=" * 100)
    print("INVESTIGATION 4 COMPLETE")
    print("=" * 100)
    total_time = time.time() - start
    print(f"  Total runtime: {total_time:.0f}s | Peak RSS: {get_mem_mb():.0f} MB")
    print(f"\n  Key findings:")
    print(f"    - Providers with >{MOM_SPIKE_RATIO}x MoM spikes: {n_spike_providers:,}")
    print(f"    - Fast starters (post-2022, >$1M/month): {fast_starters.height:,}")
    print(f"    - Sudden disappearances: {disappearances.height:,}")
    print(f"\n  Output files:")
    for f in OUTPUT_DIR.glob("temporal_*.csv"):
        print(f"    {f.name} ({f.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
