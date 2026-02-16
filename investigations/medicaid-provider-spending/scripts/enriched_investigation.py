#!/usr/bin/env python3
"""
Enriched Investigative Analysis of Medicaid Provider Spending.

Joins the Medicaid spending parquet with:
  - NPI Registry (provider names, types, specialties)
  - HCPCS Code descriptions (what services were billed)

Memory-optimized: preprocesses the 10GB NPI CSV into a slim parquet once,
then uses Polars lazy evaluation and streaming for all analysis.
"""

import polars as pl
from pathlib import Path
import time
import os
import resource


def get_mem_mb() -> float:
    """Get current process peak RSS memory in MB (macOS/Linux)."""
    usage = resource.getrusage(resource.RUSAGE_SELF)
    # macOS reports in bytes, Linux in KB
    if os.uname().sysname == "Darwin":
        return usage.ru_maxrss / (1024 * 1024)
    return usage.ru_maxrss / 1024


def track(label: str, start_time: float, start_mem: float):
    """Print elapsed time and memory delta for a phase."""
    elapsed = time.time() - start_time
    current_mem = get_mem_mb()
    print(f"\n  ⏱️  {label}: {elapsed:.1f}s | Peak RSS: {current_mem:.0f} MB (+{current_mem - start_mem:.0f} MB)")
    return current_mem

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
INVESTIGATION_ROOT = Path(__file__).resolve().parent.parent
MEDICAID_PATH = str(INVESTIGATION_ROOT / "data" / "medicaid-provider-spending.parquet")
NPI_CSV_PATH = str(INVESTIGATION_ROOT / "data" / "npidata_pfile_20050523-20260208.csv")
NPI_SLIM_PATH = str(INVESTIGATION_ROOT / "data" / "npi_slim.parquet")
HCPCS_PATH = str(INVESTIGATION_ROOT / "data" / "hcpcs_codes.csv")

# Columns to extract from the 330-column NPI CSV
NPI_COLUMNS = [
    "NPI",
    "Entity Type Code",
    "Provider Organization Name (Legal Business Name)",
    "Provider Last Name (Legal Name)",
    "Provider First Name",
    "Provider Business Practice Location Address State Name",
    "Healthcare Provider Taxonomy Code_1",
]

# Rename map for cleaner column names
NPI_RENAME = {
    "NPI": "NPI",
    "Entity Type Code": "ENTITY_TYPE",
    "Provider Organization Name (Legal Business Name)": "ORG_NAME",
    "Provider Last Name (Legal Name)": "LAST_NAME",
    "Provider First Name": "FIRST_NAME",
    "Provider Business Practice Location Address State Name": "STATE",
    "Healthcare Provider Taxonomy Code_1": "TAXONOMY_CODE",
}


# ===========================================================================
# PHASE 0: Preprocess NPI CSV → Slim Parquet (one-time)
# ===========================================================================
def preprocess_npi():
    """
    Read only the needed columns from the 10GB NPI CSV and write a slim parquet.
    This reduces ~10GB / 330 columns down to ~200MB / 7 columns.
    Only runs if the slim parquet doesn't already exist.
    """
    slim_path = Path(NPI_SLIM_PATH)
    if slim_path.exists():
        size_mb = slim_path.stat().st_size / (1024 ** 2)
        print(f"  ✓ Slim NPI parquet already exists: {slim_path} ({size_mb:.0f} MB)")
        return

    print(f"  ⏳ Preprocessing NPI CSV → slim parquet (one-time operation)...")
    print(f"     Source: {NPI_CSV_PATH} ({Path(NPI_CSV_PATH).stat().st_size / (1024**3):.1f} GB)")
    start = time.time()
    start_mem = get_mem_mb()

    # Read only the columns we need - Polars handles this efficiently
    # Use batch reading to avoid loading full 10GB into memory
    npi_df = pl.read_csv(
        NPI_CSV_PATH,
        columns=NPI_COLUMNS,
        schema_overrides={"NPI": pl.String, "Entity Type Code": pl.String},
        infer_schema_length=10000,
        low_memory=True,
    )

    # Rename columns
    npi_df = npi_df.rename(NPI_RENAME)

    # Create a human-readable provider name column
    npi_df = npi_df.with_columns(
        pl.when(pl.col("ENTITY_TYPE") == "2")
        .then(pl.col("ORG_NAME"))
        .otherwise(
            pl.concat_str(
                [pl.col("FIRST_NAME"), pl.col("LAST_NAME")],
                separator=" ",
                ignore_nulls=True,
            )
        )
        .alias("PROVIDER_NAME"),
        # Map entity type codes to labels
        pl.when(pl.col("ENTITY_TYPE") == "1")
        .then(pl.lit("Individual"))
        .when(pl.col("ENTITY_TYPE") == "2")
        .then(pl.lit("Organization"))
        .otherwise(pl.lit("Unknown"))
        .alias("ENTITY_LABEL"),
    )

    # Write slim parquet
    npi_df.write_parquet(NPI_SLIM_PATH, compression="zstd", compression_level=3)

    size_mb = slim_path.stat().st_size / (1024 ** 2)
    print(f"  ✓ Wrote {slim_path} ({size_mb:.0f} MB, {len(npi_df):,} providers)")
    del npi_df  # free memory before investigation phase
    track("NPI preprocessing", start, start_mem)


# ===========================================================================
# PHASE 1: Load reference data
# ===========================================================================
def load_hcpcs() -> pl.LazyFrame:
    """Load HCPCS code descriptions as a LazyFrame."""
    return pl.scan_csv(HCPCS_PATH)


def load_npi() -> pl.LazyFrame:
    """Load the slim NPI parquet as a LazyFrame."""
    return pl.scan_parquet(NPI_SLIM_PATH)


# ===========================================================================
# PHASE 2: Enriched Investigation
# ===========================================================================
def run_investigation():
    print("\n" + "=" * 110)
    print("🔍 ENRICHED INVESTIGATIVE ANALYSIS: MEDICAID PROVIDER SPENDING")
    print("=" * 110)

    phase_start = time.time()
    mem_baseline = get_mem_mb()
    print(f"\n  📏 Memory baseline: {mem_baseline:.0f} MB")

    medicaid = pl.scan_parquet(MEDICAID_PATH)
    npi = load_npi()
    hcpcs = load_hcpcs()

    # ------------------------------------------------------------------
    # Build enriched base LazyFrame (no collection yet)
    # ------------------------------------------------------------------
    enriched = (
        medicaid
        # Join NPI on billing provider
        .join(
            npi.select(["NPI", "PROVIDER_NAME", "ENTITY_LABEL", "STATE", "TAXONOMY_CODE"]),
            left_on="BILLING_PROVIDER_NPI_NUM",
            right_on="NPI",
            how="left",
        )
        # Join HCPCS descriptions
        .join(
            hcpcs.select(["HCPCS_CODE", "SHORT_DESCRIPTION"]),
            on="HCPCS_CODE",
            how="left",
        )
        # Calculated metrics
        .with_columns([
            (pl.col("TOTAL_PAID") / pl.col("TOTAL_UNIQUE_BENEFICIARIES"))
            .alias("COST_PER_BENEFICIARY"),
            (pl.col("TOTAL_CLAIMS") / pl.col("TOTAL_UNIQUE_BENEFICIARIES"))
            .alias("CLAIMS_PER_BENEFICIARY"),
        ])
    )

    # ------------------------------------------------------------------
    # PART 1: THE WHALE HUNT (enriched)
    # ------------------------------------------------------------------
    print("\n" + "=" * 110)
    print("🐋 PART 1: THE WHALE HUNT — Who Are the Top Payments Going To?")
    print("=" * 110)

    whales = (
        enriched
        .sort("TOTAL_PAID", descending=True)
        .select([
            "PROVIDER_NAME", "ENTITY_LABEL", "STATE",
            "SHORT_DESCRIPTION", "HCPCS_CODE",
            "CLAIM_FROM_MONTH",
            "TOTAL_UNIQUE_BENEFICIARIES", "TOTAL_CLAIMS",
            "TOTAL_PAID", "COST_PER_BENEFICIARY",
        ])
        .head(20)
        .collect(engine="streaming")
    )

    print("\n📊 TOP 20 SINGLE PAYMENTS (now with names):\n")
    with pl.Config(
        tbl_cols=10,
        tbl_width_chars=140,
        fmt_str_lengths=40,
        fmt_float="mixed",
    ):
        print(whales)
    track("Part 1 — Whale Hunt", phase_start, mem_baseline)

    # ------------------------------------------------------------------
    # PART 2: ENTITY SEGMENTATION — Organizations vs Individuals
    # ------------------------------------------------------------------
    print("\n" + "=" * 110)
    print("🏥 PART 2: ENTITY SEGMENTATION — Organizations vs. Individual Providers")
    print("=" * 110)
    print("\nContext: A hospital billing $5M is a Tuesday. A solo doctor billing $5M is a front page.")

    segment_stats = (
        enriched
        .filter(pl.col("TOTAL_PAID") > 0)
        .group_by("ENTITY_LABEL")
        .agg([
            pl.len().alias("RECORDS"),
            pl.sum("TOTAL_PAID").alias("TOTAL_SPENT"),
            pl.mean("TOTAL_PAID").alias("AVG_PAYMENT"),
            pl.median("TOTAL_PAID").alias("MEDIAN_PAYMENT"),
            pl.sum("TOTAL_UNIQUE_BENEFICIARIES").alias("TOTAL_BENEFICIARIES"),
            pl.mean("COST_PER_BENEFICIARY").alias("AVG_COST_PER_BENEFICIARY"),
            pl.median("COST_PER_BENEFICIARY").alias("MEDIAN_COST_PER_BENEFICIARY"),
        ])
        .sort("TOTAL_SPENT", descending=True)
        .collect(engine="streaming")
    )

    print("\n📊 SPENDING BREAKDOWN BY ENTITY TYPE:\n")
    with pl.Config(fmt_float="mixed"):
        print(segment_stats)
    track("Part 2 — Entity Segmentation", phase_start, mem_baseline)

    # ------------------------------------------------------------------
    # PART 3: INDIVIDUAL PROVIDER OUTLIERS (the real story)
    # ------------------------------------------------------------------
    print("\n" + "=" * 110)
    print("🚨 PART 3: INDIVIDUAL PROVIDER RED FLAGS")
    print("=" * 110)
    print("\nThese are INDIVIDUAL practitioners (not hospitals/orgs) with the highest spending.")
    print("This is where the anomalies hide.\n")

    # Top individuals by total spending (aggregated across all their records)
    top_individuals = (
        enriched
        .filter(pl.col("ENTITY_LABEL") == "Individual")
        .filter(pl.col("TOTAL_PAID") > 0)
        .group_by("BILLING_PROVIDER_NPI_NUM", "PROVIDER_NAME", "STATE", "TAXONOMY_CODE")
        .agg([
            pl.sum("TOTAL_PAID").alias("TOTAL_SPENT"),
            pl.sum("TOTAL_UNIQUE_BENEFICIARIES").alias("TOTAL_BENEFICIARIES"),
            pl.sum("TOTAL_CLAIMS").alias("TOTAL_CLAIMS"),
            pl.len().alias("RECORD_COUNT"),
            pl.col("HCPCS_CODE").n_unique().alias("UNIQUE_SERVICES"),
        ])
        .with_columns([
            (pl.col("TOTAL_SPENT") / pl.col("TOTAL_BENEFICIARIES")).alias("AVG_COST_PER_BENEFICIARY"),
            (pl.col("TOTAL_CLAIMS") / pl.col("TOTAL_BENEFICIARIES")).alias("AVG_CLAIMS_PER_BENEFICIARY"),
        ])
        .sort("TOTAL_SPENT", descending=True)
        .head(25)
        .collect(engine="streaming")
    )

    print("💰 TOP 25 INDIVIDUAL PROVIDERS BY TOTAL SPENDING:\n")
    with pl.Config(
        tbl_cols=11,
        tbl_width_chars=150,
        fmt_str_lengths=30,
        fmt_float="mixed",
    ):
        print(top_individuals)

    # Top individuals by cost per beneficiary (with min volume filter)
    print("\n🎯 TOP 25 INDIVIDUALS BY COST-PER-BENEFICIARY (min 50 beneficiaries):\n")

    individual_outliers = (
        enriched
        .filter(pl.col("ENTITY_LABEL") == "Individual")
        .filter(pl.col("TOTAL_PAID") > 0)
        .filter(pl.col("TOTAL_UNIQUE_BENEFICIARIES") >= 50)
        .sort("COST_PER_BENEFICIARY", descending=True)
        .select([
            "PROVIDER_NAME", "STATE", "TAXONOMY_CODE",
            "SHORT_DESCRIPTION", "HCPCS_CODE",
            "TOTAL_UNIQUE_BENEFICIARIES", "TOTAL_CLAIMS",
            "TOTAL_PAID", "COST_PER_BENEFICIARY",
        ])
        .head(25)
        .collect(engine="streaming")
    )

    with pl.Config(
        tbl_cols=9,
        tbl_width_chars=150,
        fmt_str_lengths=30,
        fmt_float="mixed",
    ):
        print(individual_outliers)
    track("Part 3 — Individual Red Flags", phase_start, mem_baseline)

    # ------------------------------------------------------------------
    # PART 4: ORGANIZATION DEEP DIVE
    # ------------------------------------------------------------------
    print("\n" + "=" * 110)
    print("🏢 PART 4: ORGANIZATION DEEP DIVE — Top Organizational Spenders")
    print("=" * 110)

    top_orgs = (
        enriched
        .filter(pl.col("ENTITY_LABEL") == "Organization")
        .filter(pl.col("TOTAL_PAID") > 0)
        .group_by("BILLING_PROVIDER_NPI_NUM", "PROVIDER_NAME", "STATE")
        .agg([
            pl.sum("TOTAL_PAID").alias("TOTAL_SPENT"),
            pl.sum("TOTAL_UNIQUE_BENEFICIARIES").alias("TOTAL_BENEFICIARIES"),
            pl.sum("TOTAL_CLAIMS").alias("TOTAL_CLAIMS"),
            pl.col("HCPCS_CODE").n_unique().alias("UNIQUE_SERVICES"),
        ])
        .with_columns([
            (pl.col("TOTAL_SPENT") / pl.col("TOTAL_BENEFICIARIES")).alias("AVG_COST_PER_BENEFICIARY"),
        ])
        .sort("TOTAL_SPENT", descending=True)
        .head(25)
        .collect(engine="streaming")
    )

    print("\n💰 TOP 25 ORGANIZATIONS BY TOTAL SPENDING:\n")
    with pl.Config(
        tbl_cols=8,
        tbl_width_chars=150,
        fmt_str_lengths=45,
        fmt_float="mixed",
    ):
        print(top_orgs)
    track("Part 4 — Organization Deep Dive", phase_start, mem_baseline)

    # ------------------------------------------------------------------
    # PART 5: SERVICE CODE ANALYSIS (enriched)
    # ------------------------------------------------------------------
    print("\n" + "=" * 110)
    print("💊 PART 5: WHERE'S THE MONEY GOING? — Top Services with Descriptions")
    print("=" * 110)

    top_services = (
        enriched
        .filter(pl.col("TOTAL_PAID") > 0)
        .group_by("HCPCS_CODE", "SHORT_DESCRIPTION")
        .agg([
            pl.sum("TOTAL_PAID").alias("TOTAL_SPENT"),
            pl.sum("TOTAL_UNIQUE_BENEFICIARIES").alias("TOTAL_BENEFICIARIES"),
            pl.sum("TOTAL_CLAIMS").alias("TOTAL_CLAIMS"),
            pl.len().alias("RECORD_COUNT"),
        ])
        .with_columns([
            (pl.col("TOTAL_SPENT") / pl.col("TOTAL_BENEFICIARIES")).alias("AVG_COST_PER_BENEFICIARY"),
        ])
        .sort("TOTAL_SPENT", descending=True)
        .head(30)
        .collect(engine="streaming")
    )

    print("\n💰 TOP 30 SERVICES BY TOTAL SPENDING:\n")
    with pl.Config(
        tbl_cols=7,
        tbl_width_chars=140,
        fmt_str_lengths=35,
        fmt_float="mixed",
    ):
        print(top_services)
    track("Part 5 — Service Code Analysis", phase_start, mem_baseline)

    # ------------------------------------------------------------------
    # PART 6: NEGATIVE PAYMENTS — Enriched Reversal Analysis
    # ------------------------------------------------------------------
    print("\n" + "=" * 110)
    print("💸 PART 6: REVERSAL ANALYSIS — Who's Getting Money Clawed Back?")
    print("=" * 110)

    top_reversals = (
        enriched
        .filter(pl.col("TOTAL_PAID") < 0)
        .sort("TOTAL_PAID")
        .select([
            "PROVIDER_NAME", "ENTITY_LABEL", "STATE",
            "SHORT_DESCRIPTION", "HCPCS_CODE",
            "CLAIM_FROM_MONTH", "TOTAL_PAID",
        ])
        .head(15)
        .collect(engine="streaming")
    )

    print("\n📉 TOP 15 REVERSALS (with provider names):\n")
    with pl.Config(
        tbl_cols=7,
        tbl_width_chars=140,
        fmt_str_lengths=40,
        fmt_float="mixed",
    ):
        print(top_reversals)
    track("Part 6 — Reversal Analysis", phase_start, mem_baseline)

    # ------------------------------------------------------------------
    # PART 7: STATE-LEVEL ANALYSIS
    # ------------------------------------------------------------------
    print("\n" + "=" * 110)
    print("🗺️  PART 7: STATE-LEVEL SPENDING ANALYSIS")
    print("=" * 110)

    state_spending = (
        enriched
        .filter(pl.col("TOTAL_PAID") > 0)
        .filter(pl.col("STATE").is_not_null())
        .group_by("STATE")
        .agg([
            pl.sum("TOTAL_PAID").alias("TOTAL_SPENT"),
            pl.sum("TOTAL_UNIQUE_BENEFICIARIES").alias("TOTAL_BENEFICIARIES"),
            pl.len().alias("RECORDS"),
            pl.col("BILLING_PROVIDER_NPI_NUM").n_unique().alias("UNIQUE_PROVIDERS"),
        ])
        .with_columns([
            (pl.col("TOTAL_SPENT") / pl.col("TOTAL_BENEFICIARIES")).alias("AVG_COST_PER_BENEFICIARY"),
        ])
        .sort("TOTAL_SPENT", descending=True)
        .head(20)
        .collect(engine="streaming")
    )

    print("\n🏛️ TOP 20 STATES BY TOTAL MEDICAID SPENDING:\n")
    with pl.Config(
        tbl_cols=6,
        tbl_width_chars=120,
        fmt_float="mixed",
    ):
        print(state_spending)
    track("Part 7 — State Analysis", phase_start, mem_baseline)

    # ------------------------------------------------------------------
    # PART 8: CONCENTRATION ANALYSIS
    # ------------------------------------------------------------------
    print("\n" + "=" * 110)
    print("📊 PART 8: SPENDING CONCENTRATION — How Top-Heavy Is This?")
    print("=" * 110)

    # Total spending
    total_spent = (
        medicaid
        .filter(pl.col("TOTAL_PAID") > 0)
        .select(pl.sum("TOTAL_PAID"))
        .collect(engine="streaming")
        .item()
    )

    # Top N provider concentration
    for n in [10, 50, 100]:
        top_n_spent = (
            enriched
            .filter(pl.col("TOTAL_PAID") > 0)
            .group_by("BILLING_PROVIDER_NPI_NUM", "PROVIDER_NAME", "ENTITY_LABEL")
            .agg(pl.sum("TOTAL_PAID").alias("TOTAL_SPENT"))
            .sort("TOTAL_SPENT", descending=True)
            .head(n)
            .select(pl.sum("TOTAL_SPENT"))
            .collect(engine="streaming")
            .item()
        )
        pct = (top_n_spent / total_spent) * 100
        print(f"\n  Top {n:>3} providers account for ${top_n_spent/1e9:.1f}B"
              f"  =  {pct:.1f}% of all spending (${total_spent/1e9:.1f}B total)")
    track("Part 8 — Concentration Analysis", phase_start, mem_baseline)

    # ------------------------------------------------------------------
    # SUMMARY
    # ------------------------------------------------------------------
    print("\n\n" + "=" * 110)
    print("✅ ENRICHED INVESTIGATION COMPLETE")
    print("=" * 110)
    print("""
    You now have human-readable results across all dimensions:

    • Provider names and entity types (Organization vs Individual)
    • Service descriptions (what was actually billed)
    • State-level geographic patterns
    • Spending concentration metrics

    📰 STORY-READY ANGLES:

    1. Whale Hunt:     The top payments are identified by name — are they managed care or anomalies?
    2. Individual Red Flags:  Solo practitioners billing millions deserve scrutiny
    3. Service Patterns:  Which HCPCS codes dominate? (Home health? Office visits? Drugs?)
    4. State Disparities:  Which states have the highest per-beneficiary costs?
    5. Concentration:   Do the top 100 providers control a disproportionate share?
    """)


# ===========================================================================
# Main
# ===========================================================================
if __name__ == "__main__":
    overall_start = time.time()

    print("=" * 110)
    print("📦 PHASE 0: Preprocessing Reference Data")
    print("=" * 110)
    preprocess_npi()

    run_investigation()

    elapsed = time.time() - overall_start
    print(f"\n⏱️  Total runtime: {elapsed:.0f}s")
