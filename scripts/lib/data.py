"""
Shared data loading and enrichment utilities for Medicaid investigations.

Extracted from enriched_investigation.py to avoid duplication across scripts.
"""

import polars as pl
from pathlib import Path
import time
import os
import resource

# ---------------------------------------------------------------------------
# Paths (relative to project root)
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

MEDICAID_PATH = PROJECT_ROOT / "medicaid-provider-spending" / "data" / "medicaid-provider-spending.parquet"
NPI_CSV_PATH = PROJECT_ROOT / "data" / "npidata_pfile_20050523-20260208.csv"
NPI_SLIM_PATH = PROJECT_ROOT / "data" / "npi_slim.parquet"
NPI_ADDRESS_PATH = PROJECT_ROOT / "data" / "npi_address.parquet"
HCPCS_PATH = PROJECT_ROOT / "data" / "hcpcs_codes.csv"
OIG_PATH = PROJECT_ROOT / "medicaid-provider-spending" / "data" / "UPDATED.csv"
NUCC_PATH = PROJECT_ROOT / "medicaid-provider-spending" / "data" / "nucc_taxonomy_251.csv"
OUTPUT_DIR = PROJECT_ROOT / "output"

# Columns to extract from the 330-column NPI CSV for the slim parquet
NPI_SLIM_COLUMNS = [
    "NPI",
    "Entity Type Code",
    "Provider Organization Name (Legal Business Name)",
    "Provider Last Name (Legal Name)",
    "Provider First Name",
    "Provider Business Practice Location Address State Name",
    "Healthcare Provider Taxonomy Code_1",
]

# Additional columns for the address parquet
NPI_ADDRESS_COLUMNS = [
    "NPI",
    "Entity Type Code",
    "Provider Organization Name (Legal Business Name)",
    "Provider Last Name (Legal Name)",
    "Provider First Name",
    "Provider First Line Business Practice Location Address",
    "Provider Business Practice Location Address City Name",
    "Provider Business Practice Location Address State Name",
    "Provider Business Practice Location Address Postal Code",
    "Healthcare Provider Taxonomy Code_1",
    "Authorized Official Last Name",
    "Authorized Official First Name",
]

NPI_ADDRESS_RENAME = {
    "NPI": "NPI",
    "Entity Type Code": "ENTITY_TYPE",
    "Provider Organization Name (Legal Business Name)": "ORG_NAME",
    "Provider Last Name (Legal Name)": "LAST_NAME",
    "Provider First Name": "FIRST_NAME",
    "Provider First Line Business Practice Location Address": "ADDRESS",
    "Provider Business Practice Location Address City Name": "CITY",
    "Provider Business Practice Location Address State Name": "STATE",
    "Provider Business Practice Location Address Postal Code": "ZIP",
    "Healthcare Provider Taxonomy Code_1": "TAXONOMY_CODE",
    "Authorized Official Last Name": "AUTH_OFFICIAL_LAST",
    "Authorized Official First Name": "AUTH_OFFICIAL_FIRST",
}


# ---------------------------------------------------------------------------
# Memory / timing utilities
# ---------------------------------------------------------------------------
def get_mem_mb() -> float:
    """Get current process peak RSS memory in MB (macOS/Linux)."""
    usage = resource.getrusage(resource.RUSAGE_SELF)
    if os.uname().sysname == "Darwin":
        return usage.ru_maxrss / (1024 * 1024)
    return usage.ru_maxrss / 1024


def track(label: str, start_time: float, start_mem: float):
    """Print elapsed time and memory delta for a phase."""
    elapsed = time.time() - start_time
    current_mem = get_mem_mb()
    print(f"\n  >> {label}: {elapsed:.1f}s | Peak RSS: {current_mem:.0f} MB (+{current_mem - start_mem:.0f} MB)")
    return current_mem


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------
def load_medicaid() -> pl.LazyFrame:
    """Load the Medicaid provider spending parquet as a LazyFrame."""
    return pl.scan_parquet(str(MEDICAID_PATH))


def load_npi() -> pl.LazyFrame:
    """Load the slim NPI parquet as a LazyFrame."""
    return pl.scan_parquet(str(NPI_SLIM_PATH))


def load_npi_address() -> pl.LazyFrame:
    """Load the NPI address parquet as a LazyFrame."""
    if not NPI_ADDRESS_PATH.exists():
        print("  Building NPI address parquet (one-time)...")
        preprocess_npi_address()
    return pl.scan_parquet(str(NPI_ADDRESS_PATH))


def load_hcpcs() -> pl.LazyFrame:
    """Load HCPCS code descriptions as a LazyFrame."""
    return pl.scan_csv(str(HCPCS_PATH))


def load_oig() -> pl.DataFrame:
    """Load OIG exclusion list (LEIE) as an eager DataFrame."""
    return pl.read_csv(
        str(OIG_PATH),
        schema_overrides={"NPI": pl.String, "ZIP": pl.String},
        infer_schema_length=10000,
    )


def load_nucc() -> pl.DataFrame:
    """Load NUCC taxonomy codes as an eager DataFrame."""
    return pl.read_csv(str(NUCC_PATH), infer_schema_length=5000)


# ---------------------------------------------------------------------------
# Preprocessing
# ---------------------------------------------------------------------------
def preprocess_npi_address():
    """
    Build an NPI parquet with address columns for geographic investigations.
    ~400 MB output with address, city, state, zip, authorized official.
    """
    path = NPI_ADDRESS_PATH
    if path.exists():
        size_mb = path.stat().st_size / (1024 ** 2)
        print(f"  NPI address parquet already exists: {path} ({size_mb:.0f} MB)")
        return

    print(f"  Preprocessing NPI CSV -> address parquet (one-time)...")
    start = time.time()

    npi_df = pl.read_csv(
        str(NPI_CSV_PATH),
        columns=NPI_ADDRESS_COLUMNS,
        schema_overrides={"NPI": pl.String, "Entity Type Code": pl.String},
        infer_schema_length=10000,
        low_memory=True,
    )

    npi_df = npi_df.rename(NPI_ADDRESS_RENAME)

    # Create provider name and entity label columns
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
        pl.when(pl.col("ENTITY_TYPE") == "1")
        .then(pl.lit("Individual"))
        .when(pl.col("ENTITY_TYPE") == "2")
        .then(pl.lit("Organization"))
        .otherwise(pl.lit("Unknown"))
        .alias("ENTITY_LABEL"),
    )

    npi_df.write_parquet(str(path), compression="zstd", compression_level=3)

    size_mb = path.stat().st_size / (1024 ** 2)
    elapsed = time.time() - start
    print(f"  Wrote {path} ({size_mb:.0f} MB, {len(npi_df):,} providers) in {elapsed:.0f}s")
    del npi_df


def build_enriched() -> pl.LazyFrame:
    """
    Build the standard enriched LazyFrame: Medicaid + NPI + HCPCS with calculated metrics.
    Returns a LazyFrame (nothing collected yet).

    CAVEAT — TOTAL_UNIQUE_BENEFICIARIES is unique per raw row (NPI × HCPCS × month).
    Summing it across months or codes double-counts patients who appear in
    multiple rows.  Downstream aggregations therefore produce a *beneficiary
    sum* (an upper-bound proxy), NOT true unique counts.  Per-beneficiary
    ratios computed here (COST_PER_BENEFICIARY, CLAIMS_PER_BENEFICIARY) are
    row-level and accurate; aggregated versions are estimates.
    """
    medicaid = load_medicaid()
    npi = load_npi()
    hcpcs = load_hcpcs()

    return (
        medicaid
        .join(
            npi.select(["NPI", "PROVIDER_NAME", "ENTITY_LABEL", "STATE", "TAXONOMY_CODE"]),
            left_on="BILLING_PROVIDER_NPI_NUM",
            right_on="NPI",
            how="left",
        )
        .join(
            hcpcs.select(["HCPCS_CODE", "SHORT_DESCRIPTION"]),
            on="HCPCS_CODE",
            how="left",
        )
        .with_columns([
            (pl.col("TOTAL_PAID") / pl.col("TOTAL_UNIQUE_BENEFICIARIES"))
            .alias("COST_PER_BENEFICIARY"),
            (pl.col("TOTAL_CLAIMS") / pl.col("TOTAL_UNIQUE_BENEFICIARIES"))
            .alias("CLAIMS_PER_BENEFICIARY"),
        ])
    )
