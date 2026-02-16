"""
Dataset Identifiability & Ethics Analysis

Analyzes the CMS Medicaid Provider Utilization dataset and investigation outputs
across 5 dimensions:
  1. Raw Data Privacy Assessment
  2. Provider Identifiability in Outputs
  3. Patient Re-identification Risk
  4. Accusation Risk Assessment
  5. Ethical Framework & Recommendations

Outputs:
  - Console report (structured)
  - output/identifiability_analysis.csv  (per-file risk table)
  - reports/identifiability_report.md    (full narrative)
"""

import polars as pl
from pathlib import Path
from datetime import datetime
import sys, csv

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lib.data import (
    load_medicaid,
    MEDICAID_PATH,
    OUTPUT_DIR,
    PROJECT_ROOT,
)

REPORTS_DIR = PROJECT_ROOT / "reports"
REPORTS_DIR.mkdir(exist_ok=True)


# ── Helpers ──────────────────────────────────────────────────────────────────

def fmt(n: int | float) -> str:
    """Format a number with commas."""
    if isinstance(n, float):
        return f"{n:,.2f}"
    return f"{n:,}"


def read_output_csv(name: str) -> pl.DataFrame | None:
    """Read an output CSV, return None if it doesn't exist."""
    path = OUTPUT_DIR / name
    if not path.exists():
        return None
    try:
        return pl.read_csv(str(path), infer_schema_length=5000,
                           schema_overrides={"BILLING_PROVIDER_NPI_NUM": pl.String,
                                             "NPI": pl.String,
                                             "INDIVIDUAL_NPI": pl.String,
                                             "ORG_NPI": pl.String,
                                             "VANISHED_NPI": pl.String,
                                             "NEW_NPI": pl.String,
                                             "ZIP": pl.String})
    except Exception as e:
        print(f"  Warning: could not read {name}: {e}")
        return None


# ── Section 1: Raw Data Privacy Assessment ───────────────────────────────────

def section1_raw_data(medicaid: pl.LazyFrame) -> dict:
    """Analyze raw Medicaid data privacy properties."""
    print("\n" + "=" * 70)
    print("SECTION 1: RAW DATA PRIVACY ASSESSMENT")
    print("=" * 70)

    # Schema
    schema = medicaid.collect_schema()
    col_names = list(schema.names())
    print(f"\n  Columns ({len(col_names)}): {', '.join(col_names)}")

    # Total rows
    total_rows = medicaid.select(pl.len()).collect().item()
    print(f"  Total rows: {fmt(total_rows)}")

    # Beneficiary stats — min, max, distribution
    bene_stats = (
        medicaid
        .select(
            pl.col("TOTAL_UNIQUE_BENEFICIARIES").min().alias("min_bene"),
            pl.col("TOTAL_UNIQUE_BENEFICIARIES").max().alias("max_bene"),
            pl.col("TOTAL_UNIQUE_BENEFICIARIES").median().alias("median_bene"),
            pl.col("TOTAL_UNIQUE_BENEFICIARIES").mean().alias("mean_bene"),
            # Distribution buckets
            (pl.col("TOTAL_UNIQUE_BENEFICIARIES") <= 11).sum().alias("bene_le_11"),
            ((pl.col("TOTAL_UNIQUE_BENEFICIARIES") >= 12) & (pl.col("TOTAL_UNIQUE_BENEFICIARIES") <= 20)).sum().alias("bene_12_20"),
            ((pl.col("TOTAL_UNIQUE_BENEFICIARIES") >= 21) & (pl.col("TOTAL_UNIQUE_BENEFICIARIES") <= 50)).sum().alias("bene_21_50"),
            ((pl.col("TOTAL_UNIQUE_BENEFICIARIES") >= 51) & (pl.col("TOTAL_UNIQUE_BENEFICIARIES") <= 100)).sum().alias("bene_51_100"),
            (pl.col("TOTAL_UNIQUE_BENEFICIARIES") > 100).sum().alias("bene_gt_100"),
        )
        .collect()
    )

    min_bene = bene_stats["min_bene"][0]
    max_bene = bene_stats["max_bene"][0]
    median_bene = bene_stats["median_bene"][0]
    mean_bene = bene_stats["mean_bene"][0]

    print(f"\n  Beneficiary count per row:")
    print(f"    Min:    {min_bene}")
    print(f"    Max:    {fmt(max_bene)}")
    print(f"    Median: {fmt(median_bene)}")
    print(f"    Mean:   {fmt(mean_bene)}")

    print(f"\n  Beneficiary distribution:")
    print(f"    ≤11 (suppressed range): {fmt(bene_stats['bene_le_11'][0])} rows")
    print(f"    12–20:                  {fmt(bene_stats['bene_12_20'][0])} rows")
    print(f"    21–50:                  {fmt(bene_stats['bene_21_50'][0])} rows")
    print(f"    51–100:                 {fmt(bene_stats['bene_51_100'][0])} rows")
    print(f"    >100:                   {fmt(bene_stats['bene_gt_100'][0])} rows")

    # Unique quasi-identifier combos: NPI x HCPCS x MONTH
    qi_count = (
        medicaid
        .select(
            pl.struct(["BILLING_PROVIDER_NPI_NUM", "HCPCS_CODE", "CLAIM_FROM_MONTH"])
            .n_unique()
            .alias("qi_combos")
        )
        .collect()
        .item()
    )
    print(f"\n  Unique quasi-identifier combos (NPI × HCPCS × Month): {fmt(qi_count)}")

    # Patient identifiers present?
    patient_id_cols = {"SSN", "DOB", "DATE_OF_BIRTH", "PATIENT_NAME", "PATIENT_ID",
                       "GENDER", "SEX", "RACE", "ETHNICITY", "DIAGNOSIS", "ICD"}
    present_patient_cols = patient_id_cols.intersection(set(col_names))
    print(f"\n  Patient identifier columns present: {present_patient_cols if present_patient_cols else 'NONE'}")

    suppression_confirmed = min_bene >= 11
    print(f"\n  CMS k=11 cell suppression confirmed: {'YES' if suppression_confirmed else 'NO'} (min beneficiary = {min_bene})")
    print(f"\n  >> ASSESSMENT: RAW DATA = LOW PATIENT RISK")
    print(f"     No patient-level data. k≥{min_bene} suppression floor. Public Use File.")

    return {
        "total_rows": total_rows,
        "col_count": len(col_names),
        "columns": col_names,
        "min_bene": min_bene,
        "max_bene": max_bene,
        "median_bene": median_bene,
        "mean_bene": mean_bene,
        "bene_le_11": bene_stats["bene_le_11"][0],
        "bene_12_20": bene_stats["bene_12_20"][0],
        "bene_21_50": bene_stats["bene_21_50"][0],
        "bene_51_100": bene_stats["bene_51_100"][0],
        "bene_gt_100": bene_stats["bene_gt_100"][0],
        "qi_combos": qi_count,
        "suppression_confirmed": suppression_confirmed,
    }


# ── Section 2: Provider Identifiability in Outputs ──────────────────────────

# Per-file metadata: (filename, investigation, description, accusation_level)
OUTPUT_FILE_META = [
    ("ghost_providers_impossible_volume.csv", "Ghost Providers", "Individuals billing beyond physical capacity", "HIGH"),
    ("ghost_providers_address_clustering.csv", "Ghost Providers", "Addresses with multiple flagged NPIs", "MODERATE"),
    ("shell_company_connections.csv", "Shell Companies", "Individual-organization linkages at shared addresses", "HIGH"),
    ("individual_oig_matches.csv", "OIG Matches", "Providers matched to federal exclusion list", "LOW"),
    ("individual_top_spenders.csv", "Individual Deep Dive", "Top 500 individual spenders nationally", "MODERATE"),
    ("individual_specialty_outliers.csv", "Individual Deep Dive", "Providers spending >5x specialty median", "MODERATE"),
    ("t1019_brooklyn_analysis.csv", "Brooklyn T1019", "Top T1019 billers with addresses", "MODERATE"),
    ("t1019_shared_addresses.csv", "Brooklyn T1019", "Shared billing addresses for T1019", "MODERATE"),
    ("t1019_oig_matches.csv", "Brooklyn T1019", "T1019 OIG exclusion matches", "LOW"),
    ("minnesota_anomalies.csv", "Minnesota", "MN providers flagged by growth/CPB anomalies", "MODERATE"),
    ("minnesota_behavioral_health.csv", "Minnesota", "MN behavioral health HCPCS summary (aggregate)", "LOW"),
    ("minnesota_temporal.csv", "Minnesota", "Monthly time series for MN flagged providers", "LOW"),
    ("temporal_spikes.csv", "Temporal", "Providers with sudden billing spikes", "LOW"),
    ("temporal_new_entrants.csv", "Temporal", "Fast-starting new providers", "LOW"),
    ("temporal_disappearances.csv", "Temporal", "Providers who stopped billing", "LOW"),
    ("cross_state_billing.csv", "Cross-State", "Providers billing across many states", "MODERATE"),
    ("traveling_fraudsters.csv", "Traveling Fraudsters", "Auth officials linked to vanished+new orgs", "HIGH"),
]


def classify_identifiability(columns: list[str]) -> str:
    """Classify identifiability tier based on columns present."""
    has_name = any(c in columns for c in ["PROVIDER_NAME", "BILLING_NAME", "INDIVIDUAL_NAME",
                                            "LASTNAME", "OFFICIAL_NAME", "VANISHED_ORG_NAME"])
    has_npi = any(c in columns for c in ["BILLING_PROVIDER_NPI_NUM", "NPI", "INDIVIDUAL_NPI", "VANISHED_NPI"])
    has_address = any(c in columns for c in ["ADDRESS", "NEW_ADDRESS"])

    if has_name and has_npi:
        return "DIRECT"
    elif has_address or has_npi:
        return "QUASI"
    else:
        return "AGGREGATE"


def count_entity_types(df: pl.DataFrame, columns: list[str]) -> tuple[int, int]:
    """Count individual vs org providers in a dataframe. Returns (individuals, orgs)."""
    # Check for entity label column
    entity_col = None
    for c in ["ENTITY_LABEL", "BILLING_ENTITY"]:
        if c in columns:
            entity_col = c
            break

    if entity_col:
        vc = df.group_by(entity_col).len()
        individuals = 0
        orgs = 0
        for row in vc.iter_rows():
            label, count = row[0], row[1]
            if label and "individual" in str(label).lower():
                individuals = count
            elif label and "organization" in str(label).lower():
                orgs = count
        return individuals, orgs

    # For files without entity type, heuristic: if it has INDIVIDUAL_NPI it's individuals
    if "INDIVIDUAL_NPI" in columns:
        return len(df), 0
    # OIG matches — all individuals
    if "LASTNAME" in columns:
        return len(df), 0
    # traveling_fraudsters — organizations primarily
    if "OFFICIAL_NAME" in columns:
        return 0, len(df)

    # Default: count all as unknown (report as individuals if has personal name patterns)
    if any(c in columns for c in ["PROVIDER_NAME", "BILLING_NAME"]):
        return len(df), 0

    return 0, 0


def section2_output_identifiability() -> list[dict]:
    """Analyze identifiability of each output file."""
    print("\n" + "=" * 70)
    print("SECTION 2: PROVIDER IDENTIFIABILITY IN OUTPUTS")
    print("=" * 70)

    results = []
    total_individuals = set()
    total_orgs = set()

    for filename, investigation, description, accusation_level in OUTPUT_FILE_META:
        df = read_output_csv(filename)
        if df is None:
            print(f"\n  {filename}: NOT FOUND")
            results.append({
                "file": filename, "investigation": investigation,
                "description": description, "rows": 0,
                "individuals": 0, "organizations": 0,
                "identifiability": "N/A", "accusation_risk": accusation_level,
                "fields": "",
            })
            continue

        columns = df.columns
        rows = len(df)
        identifiability = classify_identifiability(columns)
        individuals, orgs = count_entity_types(df, columns)

        # Collect unique NPIs for deduplication
        for npi_col in ["BILLING_PROVIDER_NPI_NUM", "NPI", "INDIVIDUAL_NPI"]:
            if npi_col in columns:
                # Check if we can determine entity type
                entity_col = None
                for ec in ["ENTITY_LABEL", "BILLING_ENTITY"]:
                    if ec in columns:
                        entity_col = ec
                        break
                if entity_col:
                    ind_npis = df.filter(pl.col(entity_col).str.to_lowercase().str.contains("individual"))[npi_col].drop_nulls().to_list()
                    org_npis = df.filter(pl.col(entity_col).str.to_lowercase().str.contains("organization"))[npi_col].drop_nulls().to_list()
                    total_individuals.update(ind_npis)
                    total_orgs.update(org_npis)
                elif individuals > 0:
                    total_individuals.update(df[npi_col].drop_nulls().to_list())
                break

        # Determine which sensitive fields are present
        sensitive_fields = []
        for f in ["PROVIDER_NAME", "BILLING_NAME", "INDIVIDUAL_NAME", "LASTNAME",
                   "OFFICIAL_NAME", "ADDRESS", "NEW_ADDRESS", "CITY", "ZIP", "ZIP5",
                   "AUTH_OFFICIAL_LAST", "AUTH_OFFICIAL_FIRST", "DOB",
                   "BILLING_PROVIDER_NPI_NUM", "NPI", "INDIVIDUAL_NPI"]:
            if f in columns:
                sensitive_fields.append(f)

        print(f"\n  {filename}")
        print(f"    Rows: {fmt(rows)} | Individuals: {fmt(individuals)} | Orgs: {fmt(orgs)}")
        print(f"    Identifiability: {identifiability} | Accusation Risk: {accusation_level}")
        print(f"    Sensitive fields: {', '.join(sensitive_fields)}")

        results.append({
            "file": filename,
            "investigation": investigation,
            "description": description,
            "rows": rows,
            "individuals": individuals,
            "organizations": orgs,
            "identifiability": identifiability,
            "accusation_risk": accusation_level,
            "fields": ", ".join(sensitive_fields),
        })

    print(f"\n  TOTALS ACROSS ALL OUTPUTS:")
    print(f"    Unique individual NPIs named: {fmt(len(total_individuals))}")
    print(f"    Unique organization NPIs named: {fmt(len(total_orgs))}")

    return results


# ── Section 3: Patient Re-identification Risk ───────────────────────────────

def section3_patient_reidentification(medicaid: pl.LazyFrame, s1: dict) -> dict:
    """Assess patient re-identification risk from the raw data."""
    print("\n" + "=" * 70)
    print("SECTION 3: PATIENT RE-IDENTIFICATION RISK")
    print("=" * 70)

    # Rows at suppression floor (beneficiary count = min observed)
    min_bene = s1["min_bene"]
    at_floor = (
        medicaid
        .filter(pl.col("TOTAL_UNIQUE_BENEFICIARIES") == min_bene)
        .select(pl.len())
        .collect()
        .item()
    )
    print(f"\n  Rows at suppression floor (bene = {min_bene}): {fmt(at_floor)}")

    # Rare HCPCS codes: codes with < 100 total national beneficiaries
    rare_hcpcs = (
        medicaid
        .group_by("HCPCS_CODE")
        .agg(pl.col("TOTAL_UNIQUE_BENEFICIARIES").sum().alias("national_bene_sum"))
        .filter(pl.col("national_bene_sum") < 100)
        .select(pl.len())
        .collect()
        .item()
    )
    total_hcpcs = (
        medicaid
        .select(pl.col("HCPCS_CODE").n_unique())
        .collect()
        .item()
    )
    print(f"  Rare HCPCS codes (<100 national bene sum): {fmt(rare_hcpcs)} / {fmt(total_hcpcs)}")

    # K-anonymity by (HCPCS, MONTH) — minimum beneficiary count per group
    # Using HCPCS × MONTH since state is not in the raw data
    k_anon = (
        medicaid
        .group_by(["HCPCS_CODE", "CLAIM_FROM_MONTH"])
        .agg(pl.col("TOTAL_UNIQUE_BENEFICIARIES").sum().alias("group_bene_sum"))
        .select(
            pl.col("group_bene_sum").min().alias("min_k"),
            pl.col("group_bene_sum").median().alias("median_k"),
            (pl.col("group_bene_sum") < 50).sum().alias("groups_lt_50"),
            pl.len().alias("total_groups"),
        )
        .collect()
    )
    min_k = k_anon["min_k"][0]
    median_k = k_anon["median_k"][0]
    groups_lt_50 = k_anon["groups_lt_50"][0]
    total_groups = k_anon["total_groups"][0]

    print(f"\n  K-anonymity by (HCPCS × Month):")
    print(f"    Total groups: {fmt(total_groups)}")
    print(f"    Min group beneficiary sum: {fmt(min_k)}")
    print(f"    Median group beneficiary sum: {fmt(median_k)}")
    print(f"    Groups with <50 bene sum: {fmt(groups_lt_50)}")

    # Risk assessment
    if min_bene >= 11 and rare_hcpcs < total_hcpcs * 0.1:
        risk = "LOW"
        rationale = (
            f"CMS k={min_bene} suppression floor prevents small-cell identification. "
            f"No patient demographics, diagnoses, or identifiers in the data. "
            f"Rare HCPCS codes exist ({rare_hcpcs}) but still report only aggregate counts ≥{min_bene}."
        )
    elif rare_hcpcs >= total_hcpcs * 0.1:
        risk = "MODERATE"
        rationale = (
            f"Significant number of rare HCPCS codes ({rare_hcpcs}/{total_hcpcs}). "
            f"While k≥{min_bene} applies, combining rare service + NPI + month could narrow "
            f"beneficiary populations to small groups."
        )
    else:
        risk = "LOW"
        rationale = f"Standard protections in place. k≥{min_bene}."

    print(f"\n  >> ASSESSMENT: PATIENT RE-IDENTIFICATION RISK = {risk}")
    print(f"     {rationale}")

    return {
        "at_floor": at_floor,
        "rare_hcpcs": rare_hcpcs,
        "total_hcpcs": total_hcpcs,
        "min_k": min_k,
        "median_k": median_k,
        "groups_lt_50": groups_lt_50,
        "total_groups": total_groups,
        "risk": risk,
        "rationale": rationale,
    }


# ── Section 4: Accusation Risk Assessment ───────────────────────────────────

INVESTIGATION_RISK_META = [
    {
        "investigation": "Ghost Providers",
        "files": ["ghost_providers_impossible_volume.csv", "ghost_providers_address_clustering.csv"],
        "risk_tier": "HIGH",
        "rationale": "Names 37 individuals as billing beyond physical capacity — direct fraud accusation by name.",
    },
    {
        "investigation": "Shell Companies",
        "files": ["shell_company_connections.csv"],
        "risk_tier": "HIGH",
        "rationale": "Links 41 named individuals to organizations at shared addresses — implies coordinated fraud.",
    },
    {
        "investigation": "OIG Matches",
        "files": ["individual_oig_matches.csv", "t1019_oig_matches.csv"],
        "risk_tier": "LOW",
        "rationale": "Matches against the publicly-available federal exclusion list. These individuals are already publicly identified by HHS-OIG.",
    },
    {
        "investigation": "Individual Deep Dive",
        "files": ["individual_top_spenders.csv", "individual_specialty_outliers.csv"],
        "risk_tier": "MODERATE",
        "rationale": "Names ~6,400 providers as statistical outliers. Not direct accusations, but 'outlier' framing implies wrongdoing.",
    },
    {
        "investigation": "Brooklyn T1019",
        "files": ["t1019_brooklyn_analysis.csv", "t1019_shared_addresses.csv"],
        "risk_tier": "MODERATE",
        "rationale": "Names ~316 providers in geographic cluster analysis. Shared-address finding implies coordination.",
    },
    {
        "investigation": "Minnesota",
        "files": ["minnesota_anomalies.csv", "minnesota_behavioral_health.csv", "minnesota_temporal.csv"],
        "risk_tier": "MODERATE",
        "rationale": "Names ~566 MN providers flagged by anomaly scoring. Anomaly flags are statistical, not conclusive.",
    },
    {
        "investigation": "Temporal Patterns",
        "files": ["temporal_spikes.csv", "temporal_new_entrants.csv", "temporal_disappearances.csv"],
        "risk_tier": "LOW",
        "rationale": "~14,700 providers flagged for temporal patterns. Framed as billing behavior changes, not accusations.",
    },
    {
        "investigation": "Cross-State",
        "files": ["cross_state_billing.csv"],
        "risk_tier": "MODERATE",
        "rationale": "~63,500 providers billing across states. Large pool dilutes individual risk, but naming + framing matters.",
    },
    {
        "investigation": "Traveling Fraudsters",
        "files": ["traveling_fraudsters.csv"],
        "risk_tier": "HIGH",
        "rationale": "~1,002 authorized officials linked to vanished-then-new organizations. 'Fraudster' label in filename is accusatory.",
    },
]


def section4_accusation_risk(s2_results: list[dict]) -> list[dict]:
    """Assess accusation/reputational risk per investigation."""
    print("\n" + "=" * 70)
    print("SECTION 4: ACCUSATION RISK ASSESSMENT")
    print("=" * 70)

    # Build lookup from s2 results
    file_lookup = {r["file"]: r for r in s2_results}

    inv_results = []
    for meta in INVESTIGATION_RISK_META:
        total_ind = 0
        total_org = 0
        total_rows = 0
        for f in meta["files"]:
            if f in file_lookup:
                total_ind += file_lookup[f]["individuals"]
                total_org += file_lookup[f]["organizations"]
                total_rows += file_lookup[f]["rows"]

        mitigations = []
        if meta["risk_tier"] == "HIGH":
            mitigations = [
                "Use anonymized IDs instead of names",
                "Add disclaimer: statistical flag ≠ confirmed fraud",
                "Recommend referral to CMS/OIG rather than public naming",
            ]
        elif meta["risk_tier"] == "MODERATE":
            mitigations = [
                "Present as statistical patterns, not accusations",
                "Include base rates and methodology context",
                "Consider aggregating to ZIP/region level instead of naming",
            ]
        else:
            mitigations = [
                "Low risk — publicly available or aggregate data",
                "Standard disclaimers sufficient",
            ]

        print(f"\n  {meta['investigation']} [{meta['risk_tier']}]")
        print(f"    Rows: {fmt(total_rows)} | Individuals: {fmt(total_ind)} | Orgs: {fmt(total_org)}")
        print(f"    {meta['rationale']}")
        print(f"    Mitigations: {'; '.join(mitigations)}")

        inv_results.append({
            "investigation": meta["investigation"],
            "individual_count": total_ind,
            "org_count": total_org,
            "total_rows": total_rows,
            "risk_tier": meta["risk_tier"],
            "rationale": meta["rationale"],
            "mitigation": "; ".join(mitigations),
        })

    return inv_results


# ── Section 5: Ethical Framework & Recommendations ──────────────────────────

def section5_ethical_framework(s1: dict, s3: dict, s4: list[dict]) -> str:
    """Generate ethical framework text."""
    print("\n" + "=" * 70)
    print("SECTION 5: ETHICAL FRAMEWORK & RECOMMENDATIONS")
    print("=" * 70)

    text = []

    # Q1: Is the dataset ethically analyzable?
    text.append("### 5.1 Is the Dataset Ethically Analyzable?")
    text.append("")
    text.append("**YES.** The CMS Medicaid Provider Utilization and Payment dataset is a Public Use File")
    text.append("published by the Centers for Medicare & Medicaid Services on data.medicaid.gov specifically")
    text.append("for transparency and public accountability purposes. No Data Use Agreement is required.")
    text.append("")
    text.append("CMS publishes this data under the authority of the Affordable Care Act §1311(e)(3) and")
    text.append("subsequent transparency mandates. The dataset has been pre-processed by CMS to remove")
    text.append("patient-identifying information and apply k=11 cell suppression per the Privacy Act of 1974")
    text.append("and HIPAA Privacy Rule (45 CFR §164.514).")
    text.append("")
    text.append("Analyzing public spending data for patterns of waste, fraud, and abuse is not only ethical —")
    text.append("it is the stated purpose for which CMS releases this data.")

    # Q2: Where is the ethical line?
    text.append("")
    text.append("### 5.2 Where the Ethical Line Sits")
    text.append("")
    text.append("The ethical concern is not data *access* (it is all public) but *responsible use* —")
    text.append("specifically, the gap between statistical anomaly and confirmed fraud:")
    text.append("")
    text.append("| Action | Ethical Status |")
    text.append("|--------|---------------|")
    text.append("| Analyzing aggregate billing patterns | **Clearly ethical** — public accountability |")
    text.append("| Identifying statistical outliers by NPI | **Ethical with caveats** — outlier ≠ fraud |")
    text.append("| Naming individuals as 'outliers' | **Ethically borderline** — reputational harm risk |")
    text.append("| Naming individuals as 'fraudsters' or 'ghosts' | **Ethically problematic** — accusation without adjudication |")
    text.append("| Publishing named lists publicly | **Requires strong justification** — due process concerns |")
    text.append("")
    text.append("**Key principle**: A billing pattern that looks anomalous from data alone may have legitimate")
    text.append("explanations (group practices billing under one NPI, authorized billing arrangements, specialty")
    text.append("case mix differences, state Medicaid policy variations).")

    # Q3: Specific recommendations
    text.append("")
    text.append("### 5.3 Recommendations for Reporting")
    text.append("")
    text.append("#### Can name providers freely:")
    text.append("- **OIG exclusion matches** — These individuals are already publicly identified by HHS-OIG")
    text.append("  in the List of Excluded Individuals/Entities (LEIE). Naming them adds no new harm.")
    text.append("- **Aggregate statistics** (e.g., 'X providers in Brooklyn bill Y% above national average')")
    text.append("")
    text.append("#### Should use anonymized identifiers:")
    text.append("- **Ghost providers** — 'Provider A in State X billed Z claims' rather than naming")
    text.append("- **Shell company connections** — Describe patterns without naming individuals")
    text.append("- **Traveling fraudsters** — Rename file and use anonymized official identifiers")
    text.append("")
    text.append("#### Should present as statistical patterns, not accusations:")
    text.append("- **Specialty outliers** — Frame as 'providers warranting further review' not 'fraudsters'")
    text.append("- **Top spenders** — High spending is not inherently wrong; context matters")
    text.append("- **Temporal anomalies** — Billing spikes may reflect practice changes, not fraud")
    text.append("")
    text.append("#### Language recommendations:")
    text.append("- Replace 'ghost provider' with 'provider with implausible billing volume'")
    text.append("- Replace 'fraudster' with 'provider warranting further review'")
    text.append("- Replace 'shell company' with 'organization with shared-address billing patterns'")
    text.append("- Add universal disclaimer: 'These findings reflect statistical patterns in public billing data.")
    text.append("  They do not constitute evidence of fraud and should not be interpreted as accusations.")
    text.append("  Definitive determination of fraud requires investigation by CMS, OIG, or law enforcement.'")

    # Q4: CMS stated purpose
    text.append("")
    text.append("### 5.4 CMS's Stated Purpose")
    text.append("")
    text.append("CMS publishes provider utilization data to:")
    text.append("1. **Promote transparency** in how Medicaid dollars are spent")
    text.append("2. **Enable research** into healthcare delivery patterns")
    text.append("3. **Support program integrity** by making billing patterns visible")
    text.append("4. **Inform policy** decisions about Medicaid program design")
    text.append("")
    text.append("CMS explicitly does NOT publish this data for the purpose of individual provider accusation.")
    text.append("The data is designed for aggregate analysis and pattern identification, with the expectation")
    text.append("that suspected anomalies will be referred to appropriate oversight bodies (OIG, state Medicaid")
    text.append("Fraud Control Units) for proper investigation with due process protections.")

    full_text = "\n".join(text)
    print(f"\n{full_text}")
    return full_text


# ── Report Generation ────────────────────────────────────────────────────────

def generate_csv(s2_results: list[dict], s4_results: list[dict]):
    """Write the per-file risk assessment CSV."""
    path = OUTPUT_DIR / "identifiability_analysis.csv"
    rows = []
    for r in s2_results:
        # Find the matching s4 investigation-level risk
        inv_risk = "N/A"
        for s4r in s4_results:
            if s4r["investigation"] in r.get("investigation", ""):
                inv_risk = s4r["risk_tier"]
                break
        rows.append({
            "file": r["file"],
            "investigation": r["investigation"],
            "description": r["description"],
            "rows": r["rows"],
            "individuals": r["individuals"],
            "organizations": r["organizations"],
            "identifiability_tier": r["identifiability"],
            "accusation_risk": r["accusation_risk"],
            "sensitive_fields": r["fields"],
        })

    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n  Wrote {path} ({len(rows)} rows)")


def generate_report(s1: dict, s2_results: list[dict], s3: dict,
                    s4_results: list[dict], s5_text: str):
    """Write the full narrative markdown report."""
    path = REPORTS_DIR / "identifiability_report.md"
    lines = []

    lines.append("# Dataset Identifiability & Ethics Analysis")
    lines.append(f"\n*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}*")
    lines.append("")
    lines.append("---")

    # Section 1
    lines.append("")
    lines.append("## 1. Raw Data Privacy Assessment")
    lines.append("")
    lines.append("### Dataset: CMS Medicaid Provider Utilization and Payment")
    lines.append(f"- **Source**: data.medicaid.gov (Public Use File, no DUA required)")
    lines.append(f"- **Total records**: {fmt(s1['total_rows'])}")
    lines.append(f"- **Columns** ({s1['col_count']}): `{'`, `'.join(s1['columns'])}`")
    lines.append(f"- **Date range**: 2018–2024")
    lines.append("")
    lines.append("### Patient Identifiers Present")
    lines.append("**None.** The dataset contains no patient names, SSNs, dates of birth, diagnoses,")
    lines.append("demographics, or any other patient-level identifier.")
    lines.append("")
    lines.append("### CMS Cell Suppression")
    lines.append(f"- **CMS suppression threshold**: k=11 (cells with <11 beneficiaries are suppressed)")
    lines.append(f"- **Minimum beneficiaries observed**: {s1['min_bene']}")
    lines.append(f"- **Suppression confirmed**: {'YES' if s1['suppression_confirmed'] else 'NO'}")
    lines.append("")
    lines.append("### Beneficiary Count Distribution")
    lines.append("")
    lines.append("| Range | Row Count | % of Total |")
    lines.append("|-------|-----------|-----------|")
    total = s1["total_rows"]
    for label, key in [("≤11", "bene_le_11"), ("12–20", "bene_12_20"),
                       ("21–50", "bene_21_50"), ("51–100", "bene_51_100"), (">100", "bene_gt_100")]:
        val = s1[key]
        pct = val / total * 100 if total > 0 else 0
        lines.append(f"| {label} | {fmt(val)} | {pct:.1f}% |")
    lines.append("")
    lines.append(f"**Unique quasi-identifier combinations** (NPI × HCPCS × Month): {fmt(s1['qi_combos'])}")
    lines.append("")
    lines.append("> **Assessment: LOW PATIENT RISK** — No patient-level data exists in the dataset.")
    lines.append(f"> k≥{s1['min_bene']} suppression floor. Public Use File published for transparency.")

    # Section 2
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 2. Provider Identifiability in Investigation Outputs")
    lines.append("")
    lines.append("The investigation scripts enrich the raw data by joining NPI registry information,")
    lines.append("adding provider names, addresses, and authorized officials. This section catalogs")
    lines.append("the identifiability of each output file.")
    lines.append("")
    lines.append("| File | Investigation | Rows | Individuals | Organizations | Identifiability | Risk |")
    lines.append("|------|--------------|------|-------------|---------------|-----------------|------|")
    for r in s2_results:
        lines.append(f"| {r['file']} | {r['investigation']} | {fmt(r['rows'])} | {fmt(r['individuals'])} | {fmt(r['organizations'])} | {r['identifiability']} | {r['accusation_risk']} |")
    lines.append("")

    # Count total unique across files
    total_ind = sum(r["individuals"] for r in s2_results)
    total_org = sum(r["organizations"] for r in s2_results)
    lines.append(f"**Total named across all files**: ~{fmt(total_ind)} individual provider rows,")
    lines.append(f"~{fmt(total_org)} organization rows (with overlap across files).")
    lines.append("")
    lines.append("### Identifiability Tiers")
    lines.append("- **DIRECT**: Provider name + NPI — uniquely identifies a real person/entity")
    lines.append("- **QUASI**: Address, ZIP, or NPI without name — can be linked to identity via public registries")
    lines.append("- **AGGREGATE**: Summary statistics only — no individual identification possible")

    # Section 3
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 3. Patient Re-identification Risk")
    lines.append("")
    lines.append("Even though the dataset contains no patient identifiers, we assess whether")
    lines.append("combining dimensions could narrow to identifiable patient populations.")
    lines.append("")
    lines.append(f"- **Rows at suppression floor** (bene = {s1['min_bene']}): {fmt(s3['at_floor'])}")
    lines.append(f"- **Rare HCPCS codes** (<100 national bene sum): {fmt(s3['rare_hcpcs'])} / {fmt(s3['total_hcpcs'])}")
    lines.append("")
    lines.append("### K-anonymity by (HCPCS × Month)")
    lines.append(f"- Total groups: {fmt(s3['total_groups'])}")
    lines.append(f"- Minimum group beneficiary sum: {fmt(s3['min_k'])}")
    lines.append(f"- Median group beneficiary sum: {fmt(s3['median_k'])}")
    lines.append(f"- Groups with <50 beneficiary sum: {fmt(s3['groups_lt_50'])}")
    lines.append("")
    lines.append(f"> **Assessment: {s3['risk']}** — {s3['rationale']}")
    lines.append("")
    lines.append("**Important caveat**: The dataset does not contain any patient demographics (age, sex,")
    lines.append("race, diagnosis). Without these linkage keys, even rare service + provider + month")
    lines.append("combinations cannot be connected to specific individuals. The re-identification risk")
    lines.append("is theoretical and would require an external dataset with patient-provider-service")
    lines.append("linkages that is not publicly available.")

    # Section 4
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 4. Accusation Risk Assessment")
    lines.append("")
    lines.append("This section evaluates the reputational risk to providers named in investigation outputs.")
    lines.append("A statistical anomaly is not proof of fraud — this table assesses how each investigation's")
    lines.append("framing and output might be perceived.")
    lines.append("")
    lines.append("| Investigation | Individuals | Organizations | Risk Tier | Rationale |")
    lines.append("|--------------|-------------|---------------|-----------|-----------|")
    for r in s4_results:
        lines.append(f"| {r['investigation']} | {fmt(r['individual_count'])} | {fmt(r['org_count'])} | **{r['risk_tier']}** | {r['rationale']} |")
    lines.append("")
    lines.append("### Recommended Mitigations")
    lines.append("")
    for r in s4_results:
        lines.append(f"**{r['investigation']}** ({r['risk_tier']})")
        for m in r["mitigation"].split("; "):
            lines.append(f"- {m}")
        lines.append("")

    # Section 5
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 5. Ethical Framework & Recommendations")
    lines.append("")
    lines.append(s5_text)

    # Final summary
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append("| Dimension | Assessment |")
    lines.append("|-----------|-----------|")
    lines.append(f"| Raw data patient privacy | **LOW** risk — k≥{s1['min_bene']}, no patient identifiers |")
    lines.append(f"| Patient re-identification | **{s3['risk']}** risk — {s3['rationale'][:80]}... |")
    lines.append(f"| Provider identifiability | **HIGH** in outputs — names, NPIs, addresses added via NPI join |")
    high_risk_invs = [r["investigation"] for r in s4_results if r["risk_tier"] == "HIGH"]
    lines.append(f"| Accusation risk | **HIGH** for: {', '.join(high_risk_invs)} |")
    lines.append(f"| Ethical to analyze | **YES** — Public Use File, published for transparency |")
    lines.append(f"| Ethical to name individuals | **CONDITIONAL** — OIG matches only; all others should anonymize |")

    report_text = "\n".join(lines)

    with open(path, "w") as f:
        f.write(report_text)

    print(f"\n  Wrote {path} ({len(lines)} lines)")
    return path


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("DATASET IDENTIFIABILITY & ETHICS ANALYSIS")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 70)

    # Load raw data
    print("\nLoading Medicaid data...")
    medicaid = load_medicaid()

    # Section 1: Raw data privacy
    s1 = section1_raw_data(medicaid)

    # Section 2: Output file identifiability
    s2 = section2_output_identifiability()

    # Section 3: Patient re-identification
    s3 = section3_patient_reidentification(medicaid, s1)

    # Section 4: Accusation risk
    s4 = section4_accusation_risk(s2)

    # Section 5: Ethical framework
    s5 = section5_ethical_framework(s1, s3, s4)

    # Generate outputs
    print("\n" + "=" * 70)
    print("GENERATING OUTPUTS")
    print("=" * 70)

    generate_csv(s2, s4)
    report_path = generate_report(s1, s2, s3, s4, s5)

    print("\n" + "=" * 70)
    print("ANALYSIS COMPLETE")
    print("=" * 70)
    print(f"\n  CSV:    output/identifiability_analysis.csv")
    print(f"  Report: {report_path.relative_to(PROJECT_ROOT)}")


if __name__ == "__main__":
    main()
