#!/usr/bin/env python3
"""
Investigation 6: The Shell Company Test

Follow the impossible billers to the organizations hiding behind them.

Analyses:
1. Connect impossible individuals to corporations via shared addresses
2. Traveling fraudsters — vanished company officials reappearing elsewhere
3. Cross-state billing anomalies (billing NPI state ≠ servicing NPI state)

Output:
  output/shell_company_connections.csv
  output/traveling_fraudsters.csv
  output/cross_state_billing.csv
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import polars as pl
import time
from scripts.lib.data import (
    load_medicaid, load_npi, load_npi_address, load_hcpcs,
    OUTPUT_DIR, get_mem_mb, track,
)

IMPOSSIBLE_VOLUME_PATH = OUTPUT_DIR / "ghost_providers_impossible_volume.csv"
DISAPPEARANCES_PATH = OUTPUT_DIR / "temporal_disappearances.csv"

# Government / institutional keywords to exclude from Part 1
GOVT_KEYWORDS = [
    "DEPARTMENT OF", "STATE OF", "COUNTY OF", "CITY OF",
    "GOVERNMENT", "VA MEDICAL", "VETERANS AFFAIRS",
    "BOARD OF", "PUBLIC HEALTH", "CORRECTIONS",
]

# Corporate family brands to exclude from traveling fraudster matches
CORPORATE_FAMILIES = [
    "ADDUS", "LABCORP", "CONSUMER DIRECT", "MAXIM", "AMEDISYS",
    "KINDRED", "GENTIVA", "BAYADA", "INTERIM", "RESCARE",
    "BANNER", "GIRLING",
]


def main():
    t0 = time.time()
    m0 = get_mem_mb()
    print("=" * 70)
    print("INVESTIGATION 6: THE SHELL COMPANY TEST")
    print("=" * 70)

    # ------------------------------------------------------------------
    # Load core data
    # ------------------------------------------------------------------
    print("\n[1/6] Loading NPI address registry...")
    npi_addr = load_npi_address().collect()
    m0 = track("NPI address loaded", t0, m0)
    print(f"  {len(npi_addr):,} providers in registry")

    # ------------------------------------------------------------------
    # PART 1: Connect impossible individuals to corporations
    # ------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("PART 1: CONNECTING IMPOSSIBLE INDIVIDUALS TO CORPORATE SHELLS")
    print("=" * 70)
    t1 = time.time()

    # Load the impossible billers
    impossible = pl.read_csv(str(IMPOSSIBLE_VOLUME_PATH),
                             schema_overrides={"BILLING_PROVIDER_NPI_NUM": pl.String})
    impossible_npis = impossible["BILLING_PROVIDER_NPI_NUM"].to_list()
    print(f"\n  {len(impossible_npis)} impossible individual billers loaded")

    # Get addresses of impossible individuals from NPI registry
    impossible_providers = npi_addr.filter(
        pl.col("NPI").is_in(impossible_npis)
    )
    print(f"  {len(impossible_providers)} matched in NPI registry")

    # Get their addresses (normalized: upper, stripped)
    impossible_addresses = (
        impossible_providers
        .filter(
            pl.col("ADDRESS").is_not_null()
            & (pl.col("ADDRESS") != "")
        )
        .select([
            "NPI", "PROVIDER_NAME", "ADDRESS", "CITY", "STATE", "ZIP",
            "ENTITY_LABEL"
        ])
        .with_columns([
            pl.col("ADDRESS").str.to_uppercase().str.strip_chars().alias("ADDR_NORM"),
            pl.col("CITY").str.to_uppercase().str.strip_chars().alias("CITY_NORM"),
            pl.col("ZIP").str.slice(0, 5).alias("ZIP5"),
        ])
    )
    print(f"  {len(impossible_addresses)} have valid addresses")

    # Now find ALL organizations at those same addresses
    # Match on normalized address + city + zip5
    orgs = npi_addr.filter(pl.col("ENTITY_TYPE") == "2").with_columns([
        pl.col("ADDRESS").str.to_uppercase().str.strip_chars().alias("ADDR_NORM"),
        pl.col("CITY").str.to_uppercase().str.strip_chars().alias("CITY_NORM"),
        pl.col("ZIP").str.slice(0, 5).alias("ZIP5"),
    ])

    # Join: impossible individual addresses → organizations at same location
    shell_matches = (
        impossible_addresses
        .join(
            orgs.select([
                "NPI", "PROVIDER_NAME", "ORG_NAME", "ADDR_NORM", "CITY_NORM",
                "ZIP5", "STATE", "AUTH_OFFICIAL_LAST", "AUTH_OFFICIAL_FIRST",
            ]).rename({
                "NPI": "ORG_NPI",
                "PROVIDER_NAME": "ORG_PROVIDER_NAME",
                "STATE": "ORG_STATE",
            }),
            on=["ADDR_NORM", "CITY_NORM", "ZIP5"],
            how="inner",
        )
        .filter(pl.col("NPI") != pl.col("ORG_NPI"))  # exclude self-match
    )

    print(f"\n  >>> FOUND {len(shell_matches)} individual-to-org address matches (pre-filter)")

    # Exclude government/institutional organizations
    govt_filter = pl.lit(False)
    for kw in GOVT_KEYWORDS:
        govt_filter = govt_filter | pl.col("ORG_PROVIDER_NAME").str.to_uppercase().str.contains(kw)
    n_govt = shell_matches.filter(govt_filter).height
    shell_matches = shell_matches.filter(~govt_filter)
    print(f"  Excluded {n_govt} government/institutional org matches")

    # Add ORG_TYPE classification
    shell_matches = shell_matches.with_columns(
        pl.when(pl.col("ORG_PROVIDER_NAME").str.to_uppercase().str.contains("HOME HEALTH|HOME CARE|HOMECARE"))
        .then(pl.lit("Home Health"))
        .when(pl.col("ORG_PROVIDER_NAME").str.to_uppercase().str.contains("STAFFING|PERSONNEL|WORKFORCE"))
        .then(pl.lit("Staffing"))
        .when(pl.col("ORG_PROVIDER_NAME").str.to_uppercase().str.contains("CLINIC|MEDICAL CENTER|HEALTH CENTER"))
        .then(pl.lit("Clinic"))
        .otherwise(pl.lit("Other"))
        .alias("ORG_TYPE")
    )

    n_individuals = shell_matches["NPI"].n_unique()
    n_orgs = shell_matches["ORG_NPI"].n_unique()
    print(f"  >>> {n_individuals} impossible individuals share addresses with {n_orgs} organizations (post-filter)")

    # Enrich with billing data for the orgs
    medicaid = load_medicaid()
    org_npis = shell_matches["ORG_NPI"].unique().to_list()

    org_billing = (
        medicaid
        .filter(pl.col("BILLING_PROVIDER_NPI_NUM").is_in(org_npis))
        .group_by("BILLING_PROVIDER_NPI_NUM")
        .agg([
            pl.col("TOTAL_PAID").sum().alias("ORG_TOTAL_PAID"),
            pl.col("TOTAL_CLAIMS").sum().alias("ORG_TOTAL_CLAIMS"),
            pl.col("TOTAL_UNIQUE_BENEFICIARIES").sum().alias("ORG_BENE_SUM"),
            pl.col("CLAIM_FROM_MONTH").min().alias("ORG_FIRST_MONTH"),
            pl.col("CLAIM_FROM_MONTH").max().alias("ORG_LAST_MONTH"),
        ])
        .collect()
    )

    # Also get individual billing totals
    ind_billing = (
        medicaid
        .filter(pl.col("BILLING_PROVIDER_NPI_NUM").is_in(impossible_npis))
        .group_by("BILLING_PROVIDER_NPI_NUM")
        .agg([
            pl.col("TOTAL_PAID").sum().alias("IND_TOTAL_PAID"),
            pl.col("TOTAL_CLAIMS").sum().alias("IND_TOTAL_CLAIMS"),
        ])
        .collect()
    )

    shell_output = (
        shell_matches
        .join(org_billing, left_on="ORG_NPI",
              right_on="BILLING_PROVIDER_NPI_NUM", how="left")
        .join(ind_billing, left_on="NPI",
              right_on="BILLING_PROVIDER_NPI_NUM", how="left")
        .select([
            pl.col("NPI").alias("INDIVIDUAL_NPI"),
            pl.col("PROVIDER_NAME").alias("INDIVIDUAL_NAME"),
            pl.col("ADDRESS"),
            pl.col("CITY"),
            pl.col("STATE"),
            "ZIP5",
            pl.col("ORG_NPI"),
            pl.col("ORG_PROVIDER_NAME").alias("ORG_NAME"),
            "AUTH_OFFICIAL_FIRST",
            "AUTH_OFFICIAL_LAST",
            "IND_TOTAL_PAID",
            "IND_TOTAL_CLAIMS",
            "ORG_TYPE",
            "ORG_TOTAL_PAID",
            "ORG_TOTAL_CLAIMS",
            "ORG_FIRST_MONTH",
            "ORG_LAST_MONTH",
        ])
        .sort("IND_TOTAL_PAID", descending=True)
    )

    shell_path = OUTPUT_DIR / "shell_company_connections.csv"
    shell_output.write_csv(str(shell_path))
    print(f"\n  Wrote {shell_path.name}: {len(shell_output)} rows")

    # Summary: which individuals are connected to the most orgs?
    print("\n  TOP INDIVIDUAL-TO-ORG CLUSTERS:")
    print("  " + "-" * 60)
    ind_summary = (
        shell_output
        .group_by(["INDIVIDUAL_NPI", "INDIVIDUAL_NAME"])
        .agg([
            pl.col("ORG_NPI").n_unique().alias("NUM_ORGS_AT_ADDRESS"),
            pl.col("ORG_NAME").first().alias("SAMPLE_ORG"),
            pl.col("IND_TOTAL_PAID").first(),
            pl.col("ADDRESS").first(),
            pl.col("CITY").first(),
            pl.col("STATE").first(),
        ])
        .sort("NUM_ORGS_AT_ADDRESS", descending=True)
    )
    for row in ind_summary.head(20).iter_rows(named=True):
        paid = row["IND_TOTAL_PAID"] or 0
        print(f"  {row['INDIVIDUAL_NAME']:30s} | {row['NUM_ORGS_AT_ADDRESS']:3d} orgs | "
              f"${paid:>14,.2f} | {row['ADDRESS']}, {row['CITY']}, {row['STATE']}")
        print(f"    Sample org: {row['SAMPLE_ORG']}")

    # Check: do the individual and org share authorized officials or names?
    print("\n  CHECKING FOR NAME MATCHES (individual name ↔ org authorized official):")
    print("  " + "-" * 60)
    name_matches = shell_output.filter(
        (pl.col("AUTH_OFFICIAL_LAST").is_not_null())
        & (pl.col("INDIVIDUAL_NAME").is_not_null())
    ).with_columns(
        pl.col("INDIVIDUAL_NAME").str.to_uppercase().alias("IND_NAME_UPPER"),
        pl.col("AUTH_OFFICIAL_LAST").str.to_uppercase().alias("AUTH_LAST_UPPER"),
    ).filter(
        pl.col("IND_NAME_UPPER").str.contains(pl.col("AUTH_LAST_UPPER"))
    )
    if len(name_matches) > 0:
        print(f"  >>> {len(name_matches)} cases where individual's name contains the org's authorized official last name!")
        for row in name_matches.head(15).iter_rows(named=True):
            print(f"    {row['INDIVIDUAL_NAME']} → {row['ORG_NAME']} "
                  f"(auth: {row['AUTH_OFFICIAL_FIRST']} {row['AUTH_OFFICIAL_LAST']})")
    else:
        print("  No direct name matches found")

    m0 = track("Part 1 complete", t1, m0)

    # ------------------------------------------------------------------
    # PART 2: Traveling Fraudsters (with 4-layer noise filtering)
    # ------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("PART 2: THE TRAVELING FRAUDSTER SEARCH")
    print("=" * 70)
    t2 = time.time()

    # Load disappearances — focus on organizations that vanished with
    # significant billing (>$1M) and stopped before end of dataset
    disappearances = pl.read_csv(
        str(DISAPPEARANCES_PATH),
        schema_overrides={"BILLING_PROVIDER_NPI_NUM": pl.String}
    )

    # Filter to orgs that stopped billing before 2024-06 with >$1M total
    vanished_orgs = disappearances.filter(
        (pl.col("ENTITY_LABEL") == "Organization")
        & (pl.col("LAST_MONTH") < "2024-06")
        & (pl.col("TOTAL_PAID") > 1_000_000)
    )
    print(f"\n  {len(vanished_orgs)} vanished organizations (>$1M, stopped before 2024-06)")

    # Get the authorized officials for these vanished orgs
    vanished_npis = vanished_orgs["BILLING_PROVIDER_NPI_NUM"].to_list()
    vanished_with_officials = (
        npi_addr
        .filter(pl.col("NPI").is_in(vanished_npis))
        .filter(
            pl.col("AUTH_OFFICIAL_LAST").is_not_null()
            & (pl.col("AUTH_OFFICIAL_LAST") != "")
        )
        .select([
            "NPI", "PROVIDER_NAME", "STATE", "ADDRESS", "CITY",
            "AUTH_OFFICIAL_LAST", "AUTH_OFFICIAL_FIRST",
        ])
    )
    print(f"  {len(vanished_with_officials)} have named authorized officials")

    # Build a lookup: (auth_official_last, auth_official_first) -> vanished org info
    officials = vanished_with_officials.select([
        pl.col("AUTH_OFFICIAL_LAST").str.to_uppercase().str.strip_chars().alias("LAST"),
        pl.col("AUTH_OFFICIAL_FIRST").str.to_uppercase().str.strip_chars().alias("FIRST"),
        pl.col("NPI").alias("VANISHED_NPI"),
        pl.col("PROVIDER_NAME").alias("VANISHED_ORG_NAME"),
        pl.col("STATE").alias("VANISHED_STATE"),
    ]).unique()

    # Search entire NPI registry for orgs with matching authorized officials
    all_orgs_with_auth = (
        npi_addr
        .filter(
            (pl.col("ENTITY_TYPE") == "2")
            & pl.col("AUTH_OFFICIAL_LAST").is_not_null()
            & (pl.col("AUTH_OFFICIAL_LAST") != "")
        )
        .with_columns([
            pl.col("AUTH_OFFICIAL_LAST").str.to_uppercase().str.strip_chars().alias("LAST"),
            pl.col("AUTH_OFFICIAL_FIRST").str.to_uppercase().str.strip_chars().alias("FIRST"),
        ])
    )

    # Join: vanished officials -> all orgs they run
    traveler_matches = (
        officials
        .join(
            all_orgs_with_auth.select([
                "NPI", "PROVIDER_NAME", "STATE", "ADDRESS", "CITY", "LAST", "FIRST",
            ]).rename({
                "NPI": "NEW_NPI",
                "PROVIDER_NAME": "NEW_ORG_NAME",
                "STATE": "NEW_STATE",
                "ADDRESS": "NEW_ADDRESS",
                "CITY": "NEW_CITY",
            }),
            on=["LAST", "FIRST"],
            how="inner",
        )
        .filter(pl.col("VANISHED_NPI") != pl.col("NEW_NPI"))  # exclude self
    )

    n_raw = len(traveler_matches)
    print(f"\n  Raw official-to-new-org matches: {n_raw:,}")

    # ---- Enrich with billing data BEFORE filtering (needed for temporal filter) ----
    new_org_npis = traveler_matches["NEW_NPI"].unique().to_list()
    new_org_billing = (
        medicaid
        .filter(pl.col("BILLING_PROVIDER_NPI_NUM").is_in(new_org_npis))
        .group_by("BILLING_PROVIDER_NPI_NUM")
        .agg([
            pl.col("TOTAL_PAID").sum().alias("NEW_ORG_TOTAL_PAID"),
            pl.col("TOTAL_CLAIMS").sum().alias("NEW_ORG_TOTAL_CLAIMS"),
            pl.col("CLAIM_FROM_MONTH").min().alias("NEW_ORG_FIRST_MONTH"),
            pl.col("CLAIM_FROM_MONTH").max().alias("NEW_ORG_LAST_MONTH"),
        ])
        .collect()
    )

    vanished_billing = (
        medicaid
        .filter(pl.col("BILLING_PROVIDER_NPI_NUM").is_in(vanished_npis))
        .group_by("BILLING_PROVIDER_NPI_NUM")
        .agg([
            pl.col("TOTAL_PAID").sum().alias("VANISHED_ORG_TOTAL_PAID"),
            pl.col("CLAIM_FROM_MONTH").max().alias("VANISHED_ORG_LAST_MONTH"),
        ])
        .collect()
    )

    traveler_matches = (
        traveler_matches
        .join(new_org_billing, left_on="NEW_NPI",
              right_on="BILLING_PROVIDER_NPI_NUM", how="left")
        .join(vanished_billing, left_on="VANISHED_NPI",
              right_on="BILLING_PROVIDER_NPI_NUM", how="left")
    )

    # ---- FILTER 1: Temporal sequence — new org started AFTER old one died ----
    before_f1 = len(traveler_matches)
    traveler_matches = traveler_matches.filter(
        pl.col("NEW_ORG_FIRST_MONTH").is_not_null()
        & pl.col("VANISHED_ORG_LAST_MONTH").is_not_null()
        & (pl.col("NEW_ORG_FIRST_MONTH") > pl.col("VANISHED_ORG_LAST_MONTH"))
    )
    print(f"  Filter 1 (temporal sequence): {before_f1:,} -> {len(traveler_matches):,}")

    # ---- FILTER 2: Corporate family exclusion ----
    before_f2 = len(traveler_matches)
    corp_filter = pl.lit(False)
    for brand in CORPORATE_FAMILIES:
        corp_filter = corp_filter | (
            pl.col("VANISHED_ORG_NAME").str.to_uppercase().str.contains(brand)
            & pl.col("NEW_ORG_NAME").str.to_uppercase().str.contains(brand)
        )
    traveler_matches = traveler_matches.filter(~corp_filter)
    print(f"  Filter 2 (corporate families): {before_f2:,} -> {len(traveler_matches):,}")

    # ---- FILTER 3: Size cap — exclude large legitimate new orgs (>$100M) ----
    before_f3 = len(traveler_matches)
    traveler_matches = traveler_matches.filter(
        pl.col("NEW_ORG_TOTAL_PAID").is_null()
        | (pl.col("NEW_ORG_TOTAL_PAID") <= 100_000_000)
    )
    print(f"  Filter 3 (size cap $100M): {before_f3:,} -> {len(traveler_matches):,}")

    # ---- FILTER 4: Name rarity — exclude common names controlling >50 orgs ----
    # Count how many orgs each (FIRST, LAST) pair controls in the NPI registry
    name_org_counts = (
        all_orgs_with_auth
        .group_by(["LAST", "FIRST"])
        .agg(pl.col("NPI").n_unique().alias("NAME_ORG_COUNT"))
    )
    traveler_matches = traveler_matches.join(
        name_org_counts, on=["LAST", "FIRST"], how="left"
    )
    before_f4 = len(traveler_matches)
    traveler_matches = traveler_matches.filter(
        pl.col("NAME_ORG_COUNT").is_null() | (pl.col("NAME_ORG_COUNT") <= 50)
    )
    print(f"  Filter 4 (name rarity <=50 orgs): {before_f4:,} -> {len(traveler_matches):,}")

    # Add NAME_RARITY classification
    traveler_matches = traveler_matches.with_columns(
        pl.when(pl.col("NAME_ORG_COUNT") <= 3)
        .then(pl.lit("RARE"))
        .when(pl.col("NAME_ORG_COUNT") <= 15)
        .then(pl.lit("MODERATE"))
        .otherwise(pl.lit("COMMON"))
        .alias("NAME_RARITY")
    )

    n_officials = traveler_matches.select(["LAST", "FIRST"]).unique().height
    print(f"\n  >>> FINAL: {len(traveler_matches):,} matches from {n_officials} officials (was {n_raw:,})")

    # Focus on cross-state moves (most suspicious)
    cross_state = traveler_matches.filter(
        pl.col("VANISHED_STATE") != pl.col("NEW_STATE")
    )
    print(f"  >>> {len(cross_state)} are CROSS-STATE moves (different state)")

    traveler_output = (
        traveler_matches
        .select([
            pl.concat_str(["FIRST", "LAST"], separator=" ").alias("OFFICIAL_NAME"),
            "VANISHED_NPI",
            "VANISHED_ORG_NAME",
            "VANISHED_STATE",
            "VANISHED_ORG_TOTAL_PAID",
            "VANISHED_ORG_LAST_MONTH",
            "NEW_NPI",
            "NEW_ORG_NAME",
            "NEW_STATE",
            "NEW_ADDRESS",
            "NEW_CITY",
            "NEW_ORG_TOTAL_PAID",
            "NEW_ORG_FIRST_MONTH",
            "NEW_ORG_LAST_MONTH",
            "NAME_ORG_COUNT",
            "NAME_RARITY",
        ])
        .sort("VANISHED_ORG_TOTAL_PAID", descending=True, nulls_last=True)
    )

    traveler_path = OUTPUT_DIR / "traveling_fraudsters.csv"
    traveler_output.write_csv(str(traveler_path))
    print(f"\n  Wrote {traveler_path.name}: {len(traveler_output)} rows")

    # Highlight the most suspicious cross-state travelers
    timeline_suspicious = traveler_output.filter(
        pl.col("VANISHED_STATE") != pl.col("NEW_STATE")
    )
    print("\n  MOST SUSPICIOUS CROSS-STATE TRAVELERS:")
    print("  " + "-" * 60)
    top_travelers = (
        timeline_suspicious
        .sort("NEW_ORG_TOTAL_PAID", descending=True, nulls_last=True)
        .head(25)
    )
    for row in top_travelers.iter_rows(named=True):
        v_paid = row["VANISHED_ORG_TOTAL_PAID"] or 0
        n_paid = row["NEW_ORG_TOTAL_PAID"] or 0
        print(f"  {row['OFFICIAL_NAME']} [{row['NAME_RARITY']}]")
        print(f"    OLD: {row['VANISHED_ORG_NAME']} ({row['VANISHED_STATE']}) "
              f"${v_paid:>14,.2f} — last billed {row['VANISHED_ORG_LAST_MONTH']}")
        print(f"    NEW: {row['NEW_ORG_NAME']} ({row['NEW_STATE']}) "
              f"${n_paid:>14,.2f} — first billed {row['NEW_ORG_FIRST_MONTH']}")
        print()

    m0 = track("Part 2 complete", t2, m0)

    # ------------------------------------------------------------------
    # PART 3: Cross-State Billing Anomalies
    # ------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("PART 3: CROSS-STATE BILLING ANOMALIES (REMOTE CONTROL CHECK)")
    print("=" * 70)
    t3 = time.time()

    # Build a lookup: NPI → practice state from NPI registry
    npi_state_lookup = (
        npi_addr
        .filter(pl.col("STATE").is_not_null() & (pl.col("STATE") != ""))
        .select(["NPI", "STATE", "ENTITY_LABEL", "PROVIDER_NAME"])
    )

    # Get all Medicaid billing where billing NPI ≠ servicing NPI
    # Then check if their states differ
    print("\n  Scanning Medicaid for billing/servicing NPI pairs with different states...")

    cross_state_raw = (
        medicaid
        .filter(
            pl.col("BILLING_PROVIDER_NPI_NUM").is_not_null()
            & pl.col("SERVICING_PROVIDER_NPI_NUM").is_not_null()
            & (pl.col("BILLING_PROVIDER_NPI_NUM") != pl.col("SERVICING_PROVIDER_NPI_NUM"))
        )
        .group_by(["BILLING_PROVIDER_NPI_NUM", "SERVICING_PROVIDER_NPI_NUM"])
        .agg([
            pl.col("TOTAL_PAID").sum().alias("TOTAL_PAID"),
            pl.col("TOTAL_CLAIMS").sum().alias("TOTAL_CLAIMS"),
            pl.col("TOTAL_UNIQUE_BENEFICIARIES").sum().alias("BENE_SUM"),
            pl.col("CLAIM_FROM_MONTH").min().alias("FIRST_MONTH"),
            pl.col("CLAIM_FROM_MONTH").max().alias("LAST_MONTH"),
            pl.col("HCPCS_CODE").n_unique().alias("NUM_HCPCS"),
        ])
        .collect()
    )
    print(f"  {len(cross_state_raw):,} unique billing-servicing NPI pairs")

    # Join billing NPI state
    cross_enriched = (
        cross_state_raw
        .join(
            npi_state_lookup.rename({
                "NPI": "BILLING_NPI",
                "STATE": "BILLING_STATE",
                "ENTITY_LABEL": "BILLING_ENTITY",
                "PROVIDER_NAME": "BILLING_NAME",
            }),
            left_on="BILLING_PROVIDER_NPI_NUM",
            right_on="BILLING_NPI",
            how="left",
        )
        .join(
            npi_state_lookup.rename({
                "NPI": "SERVICING_NPI",
                "STATE": "SERVICING_STATE",
                "ENTITY_LABEL": "SERVICING_ENTITY",
                "PROVIDER_NAME": "SERVICING_NAME",
            }),
            left_on="SERVICING_PROVIDER_NPI_NUM",
            right_on="SERVICING_NPI",
            how="left",
        )
    )

    # Filter to cross-state pairs
    cross_state_diff = cross_enriched.filter(
        pl.col("BILLING_STATE").is_not_null()
        & pl.col("SERVICING_STATE").is_not_null()
        & (pl.col("BILLING_STATE") != pl.col("SERVICING_STATE"))
    )
    print(f"  {len(cross_state_diff):,} pairs where billing state ≠ servicing state")

    # Aggregate by billing provider to find the biggest cross-state billers
    cross_state_by_biller = (
        cross_state_diff
        .group_by(["BILLING_PROVIDER_NPI_NUM", "BILLING_NAME", "BILLING_STATE", "BILLING_ENTITY"])
        .agg([
            pl.col("TOTAL_PAID").sum().alias("CROSS_STATE_PAID"),
            pl.col("TOTAL_CLAIMS").sum().alias("CROSS_STATE_CLAIMS"),
            pl.col("SERVICING_STATE").n_unique().alias("NUM_SERVICING_STATES"),
            pl.col("SERVICING_PROVIDER_NPI_NUM").n_unique().alias("NUM_SERVICING_PROVIDERS"),
            pl.col("FIRST_MONTH").min().alias("FIRST_MONTH"),
            pl.col("LAST_MONTH").max().alias("LAST_MONTH"),
        ])
        .sort("CROSS_STATE_PAID", descending=True)
    )

    cross_state_path = OUTPUT_DIR / "cross_state_billing.csv"
    cross_state_by_biller.write_csv(str(cross_state_path))
    print(f"\n  Wrote {cross_state_path.name}: {len(cross_state_by_biller)} rows")

    # Summary stats
    total_cross_state = cross_state_by_biller["CROSS_STATE_PAID"].sum()
    print(f"\n  Total cross-state billing: ${total_cross_state:,.2f}")

    print("\n  TOP 25 CROSS-STATE BILLERS (by total paid across state lines):")
    print("  " + "-" * 60)
    for row in cross_state_by_biller.head(25).iter_rows(named=True):
        print(f"  {row['BILLING_NAME'] or 'UNKNOWN':45s} | {row['BILLING_STATE'] or '??':2s} | "
              f"{row['BILLING_ENTITY'] or '':12s} | "
              f"${row['CROSS_STATE_PAID']:>14,.2f} | "
              f"{row['NUM_SERVICING_STATES']} states, "
              f"{row['NUM_SERVICING_PROVIDERS']} providers")

    # Focus on individuals billing cross-state with high volume (most suspicious)
    suspicious_individuals = (
        cross_state_by_biller
        .filter(
            (pl.col("BILLING_ENTITY") == "Individual")
            & (pl.col("CROSS_STATE_PAID") > 500_000)
        )
        .sort("CROSS_STATE_PAID", descending=True)
    )
    print(f"\n  SUSPICIOUS INDIVIDUALS billing >$500K cross-state: {len(suspicious_individuals)}")
    print("  " + "-" * 60)
    for row in suspicious_individuals.head(20).iter_rows(named=True):
        print(f"  {row['BILLING_NAME'] or 'UNKNOWN':40s} | {row['BILLING_STATE'] or '??':2s} | "
              f"${row['CROSS_STATE_PAID']:>14,.2f} | "
              f"{row['NUM_SERVICING_STATES']} states, "
              f"{row['NUM_SERVICING_PROVIDERS']} servicing providers")

    m0 = track("Part 3 complete", t3, m0)

    # ------------------------------------------------------------------
    # FINAL SUMMARY
    # ------------------------------------------------------------------
    print("\n" + "=" * 70)
    print("INVESTIGATION 6 — FINAL SUMMARY")
    print("=" * 70)

    print(f"""
  PART 1 — Shell Company Connections:
    {len(shell_output)} individual-to-org address matches
    {n_individuals} impossible individuals linked to {n_orgs} organizations
    Output: {shell_path.name}

  PART 2 — Traveling Fraudsters:
    {len(traveler_output)} official-to-new-org matches total
    {len(cross_state)} cross-state moves
    Output: {traveler_path.name}

  PART 3 — Cross-State Billing:
    {len(cross_state_by_biller):,} billing providers with cross-state activity
    ${total_cross_state:,.2f} total cross-state payments
    {len(suspicious_individuals)} suspicious individuals (>$500K cross-state)
    Output: {cross_state_path.name}
""")

    elapsed = time.time() - t0
    print(f"  Total runtime: {elapsed:.0f}s | Peak RSS: {get_mem_mb():.0f} MB")


if __name__ == "__main__":
    main()
