"""
Precompute lightweight summary CSVs for the Streamlit dashboard.

Reads the 2.7 GB Medicaid parquet once via build_enriched(), writes ~11
summary CSVs to output/dashboard/. Designed to be run once (or re-run
whenever source data changes).

Usage:
    python -m scripts.precompute_dashboard_data
"""

import polars as pl
import time
from pathlib import Path

from scripts.lib.data import (
    build_enriched,
    load_medicaid,
    load_npi_address,
    track,
    OUTPUT_DIR,
)

DASH_DIR = OUTPUT_DIR / "dashboard"


def main():
    DASH_DIR.mkdir(parents=True, exist_ok=True)
    t0 = time.time()
    mem0 = 0.0

    print("Building enriched LazyFrame...")
    enriched = build_enriched()

    # ── 1. State spending ────────────────────────────────────────────────
    print("Computing state_spending...")
    (
        enriched
        .filter(pl.col("STATE").is_not_null())
        .group_by("STATE")
        .agg(
            pl.col("TOTAL_PAID").sum().alias("TOTAL_SPENT"),
            pl.col("TOTAL_UNIQUE_BENEFICIARIES").sum().alias("BENE_SUM"),
            pl.len().alias("RECORDS"),
            pl.col("BILLING_PROVIDER_NPI_NUM").n_unique().alias("UNIQUE_PROVIDERS"),
        )
        .with_columns(
            (pl.col("TOTAL_SPENT") / pl.col("BENE_SUM")).alias("AVG_COST_PER_BENE"),
        )
        .sort("TOTAL_SPENT", descending=True)
        .collect(engine="streaming")
        .write_csv(str(DASH_DIR / "state_spending.csv"))
    )
    mem0 = track("state_spending", t0, mem0)

    # ── 2. Entity segmentation ───────────────────────────────────────────
    print("Computing entity_segmentation...")
    (
        enriched
        .filter(pl.col("ENTITY_LABEL").is_not_null())
        .group_by("ENTITY_LABEL")
        .agg(
            pl.len().alias("RECORDS"),
            pl.col("TOTAL_PAID").sum().alias("TOTAL_SPENT"),
            pl.col("TOTAL_PAID").mean().alias("AVG_PAYMENT"),
            pl.col("TOTAL_PAID").median().alias("MEDIAN_PAYMENT"),
            pl.col("TOTAL_UNIQUE_BENEFICIARIES").sum().alias("BENE_SUM"),
            pl.col("COST_PER_BENEFICIARY").mean().alias("AVG_COST_PER_BENE"),
            pl.col("COST_PER_BENEFICIARY").median().alias("MEDIAN_COST_PER_BENE"),
        )
        .sort("TOTAL_SPENT", descending=True)
        .collect(engine="streaming")
        .write_csv(str(DASH_DIR / "entity_segmentation.csv"))
    )
    mem0 = track("entity_segmentation", t0, mem0)

    # ── 3. Top services ─────────────────────────────────────────────────
    print("Computing top_services...")
    (
        enriched
        .group_by("HCPCS_CODE", "SHORT_DESCRIPTION")
        .agg(
            pl.col("TOTAL_PAID").sum().alias("TOTAL_SPENT"),
            pl.col("TOTAL_UNIQUE_BENEFICIARIES").sum().alias("BENE_SUM"),
            pl.col("TOTAL_CLAIMS").sum().alias("TOTAL_CLAIMS"),
            pl.len().alias("RECORD_COUNT"),
        )
        .sort("TOTAL_SPENT", descending=True)
        .head(50)
        .collect(engine="streaming")
        .write_csv(str(DASH_DIR / "top_services.csv"))
    )
    mem0 = track("top_services", t0, mem0)

    # ── 4. Top organizations ─────────────────────────────────────────────
    print("Computing top_organizations...")
    (
        enriched
        .filter(pl.col("ENTITY_LABEL") == "Organization")
        .group_by("BILLING_PROVIDER_NPI_NUM", "PROVIDER_NAME", "STATE")
        .agg(
            pl.col("TOTAL_PAID").sum().alias("TOTAL_SPENT"),
            pl.col("TOTAL_UNIQUE_BENEFICIARIES").sum().alias("BENE_SUM"),
            pl.col("TOTAL_CLAIMS").sum().alias("TOTAL_CLAIMS"),
            pl.col("HCPCS_CODE").n_unique().alias("UNIQUE_SERVICES"),
        )
        .sort("TOTAL_SPENT", descending=True)
        .head(100)
        .collect(engine="streaming")
        .write_csv(str(DASH_DIR / "top_organizations.csv"))
    )
    mem0 = track("top_organizations", t0, mem0)

    # ── 5. Top individuals summary ───────────────────────────────────────
    print("Computing top_individuals_summary...")
    (
        enriched
        .filter(pl.col("ENTITY_LABEL") == "Individual")
        .group_by("BILLING_PROVIDER_NPI_NUM", "PROVIDER_NAME", "STATE")
        .agg(
            pl.col("TOTAL_PAID").sum().alias("TOTAL_SPENT"),
            pl.col("TOTAL_UNIQUE_BENEFICIARIES").sum().alias("BENE_SUM"),
            pl.col("TOTAL_CLAIMS").sum().alias("TOTAL_CLAIMS"),
            pl.col("HCPCS_CODE").n_unique().alias("UNIQUE_SERVICES"),
        )
        .sort("TOTAL_SPENT", descending=True)
        .head(100)
        .collect(engine="streaming")
        .write_csv(str(DASH_DIR / "top_individuals_summary.csv"))
    )
    mem0 = track("top_individuals_summary", t0, mem0)

    # ── 6. Concentration ─────────────────────────────────────────────────
    print("Computing concentration...")
    provider_totals = (
        enriched
        .group_by("BILLING_PROVIDER_NPI_NUM")
        .agg(pl.col("TOTAL_PAID").sum().alias("TOTAL_SPENT"))
        .sort("TOTAL_SPENT", descending=True)
        .collect(engine="streaming")
    )
    grand_total = provider_totals["TOTAL_SPENT"].sum()
    rows = []
    for n in [10, 50, 100, 500]:
        top_n_sum = provider_totals["TOTAL_SPENT"].head(n).sum()
        rows.append({
            "TOP_N": n,
            "TOTAL_SPENT": top_n_sum,
            "SHARE_OF_TOTAL": top_n_sum / grand_total if grand_total else 0,
            "GRAND_TOTAL": grand_total,
        })
    pl.DataFrame(rows).write_csv(str(DASH_DIR / "concentration.csv"))
    del provider_totals
    mem0 = track("concentration", t0, mem0)

    # ── 7. T1019 national top 100 ────────────────────────────────────────
    print("Computing t1019_national_top100...")
    npi_addr = load_npi_address()
    (
        enriched
        .filter(pl.col("HCPCS_CODE") == "T1019")
        .group_by("BILLING_PROVIDER_NPI_NUM")
        .agg(
            pl.col("TOTAL_PAID").sum().alias("TOTAL_PAID"),
            pl.col("TOTAL_CLAIMS").sum().alias("TOTAL_CLAIMS"),
            pl.col("TOTAL_UNIQUE_BENEFICIARIES").sum().alias("BENE_SUM"),
        )
        .sort("TOTAL_PAID", descending=True)
        .head(100)
        .join(
            npi_addr.select(["NPI", "PROVIDER_NAME", "ENTITY_LABEL", "STATE", "CITY"]),
            left_on="BILLING_PROVIDER_NPI_NUM",
            right_on="NPI",
            how="left",
        )
        .collect(engine="streaming")
        .write_csv(str(DASH_DIR / "t1019_national_top100.csv"))
    )
    mem0 = track("t1019_national_top100", t0, mem0)

    # ── Time series ──────────────────────────────────────────────────────

    # ── 8. National monthly ──────────────────────────────────────────────
    print("Computing ts_national_monthly...")
    (
        enriched
        .group_by("CLAIM_FROM_MONTH")
        .agg(
            pl.col("TOTAL_PAID").sum().alias("TOTAL_PAID"),
            pl.col("TOTAL_CLAIMS").sum().alias("TOTAL_CLAIMS"),
            pl.col("TOTAL_UNIQUE_BENEFICIARIES").sum().alias("BENE_SUM"),
        )
        .sort("CLAIM_FROM_MONTH")
        .collect(engine="streaming")
        .write_csv(str(DASH_DIR / "ts_national_monthly.csv"))
    )
    mem0 = track("ts_national_monthly", t0, mem0)

    # ── 9. State monthly (top 10 states) ─────────────────────────────────
    print("Computing ts_state_monthly...")
    top10_states = pl.read_csv(str(DASH_DIR / "state_spending.csv"))["STATE"].head(10).to_list()
    (
        enriched
        .filter(pl.col("STATE").is_in(top10_states))
        .group_by("STATE", "CLAIM_FROM_MONTH")
        .agg(
            pl.col("TOTAL_PAID").sum().alias("TOTAL_PAID"),
            pl.col("TOTAL_CLAIMS").sum().alias("TOTAL_CLAIMS"),
            pl.col("TOTAL_UNIQUE_BENEFICIARIES").sum().alias("BENE_SUM"),
        )
        .sort("STATE", "CLAIM_FROM_MONTH")
        .collect(engine="streaming")
        .write_csv(str(DASH_DIR / "ts_state_monthly.csv"))
    )
    mem0 = track("ts_state_monthly", t0, mem0)

    # ── 10. Entity monthly ───────────────────────────────────────────────
    print("Computing ts_entity_monthly...")
    (
        enriched
        .filter(pl.col("ENTITY_LABEL").is_not_null())
        .group_by("ENTITY_LABEL", "CLAIM_FROM_MONTH")
        .agg(
            pl.col("TOTAL_PAID").sum().alias("TOTAL_PAID"),
            pl.col("TOTAL_CLAIMS").sum().alias("TOTAL_CLAIMS"),
            pl.col("TOTAL_UNIQUE_BENEFICIARIES").sum().alias("BENE_SUM"),
        )
        .sort("ENTITY_LABEL", "CLAIM_FROM_MONTH")
        .collect(engine="streaming")
        .write_csv(str(DASH_DIR / "ts_entity_monthly.csv"))
    )
    mem0 = track("ts_entity_monthly", t0, mem0)

    # ── 11. Top services monthly (top 10 codes) ─────────────────────────
    print("Computing ts_top_services_monthly...")
    top10_codes = pl.read_csv(str(DASH_DIR / "top_services.csv"))["HCPCS_CODE"].head(10).to_list()
    (
        enriched
        .filter(pl.col("HCPCS_CODE").is_in(top10_codes))
        .group_by("HCPCS_CODE", "SHORT_DESCRIPTION", "CLAIM_FROM_MONTH")
        .agg(
            pl.col("TOTAL_PAID").sum().alias("TOTAL_PAID"),
            pl.col("TOTAL_CLAIMS").sum().alias("TOTAL_CLAIMS"),
            pl.col("TOTAL_UNIQUE_BENEFICIARIES").sum().alias("BENE_SUM"),
        )
        .sort("HCPCS_CODE", "CLAIM_FROM_MONTH")
        .collect(engine="streaming")
        .write_csv(str(DASH_DIR / "ts_top_services_monthly.csv"))
    )
    mem0 = track("ts_top_services_monthly", t0, mem0)

    elapsed = time.time() - t0
    print(f"\nDone. All 11 CSVs written to {DASH_DIR} in {elapsed:.0f}s")


if __name__ == "__main__":
    main()
