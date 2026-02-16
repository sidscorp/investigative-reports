# Dataset Identifiability & Ethics Analysis

*Generated: 2026-02-15 20:12*

---

## 1. Raw Data Privacy Assessment

### Dataset: CMS Medicaid Provider Utilization and Payment
- **Source**: data.medicaid.gov (Public Use File, no DUA required)
- **Total records**: 227,083,361
- **Columns** (7): `BILLING_PROVIDER_NPI_NUM`, `SERVICING_PROVIDER_NPI_NUM`, `HCPCS_CODE`, `CLAIM_FROM_MONTH`, `TOTAL_UNIQUE_BENEFICIARIES`, `TOTAL_CLAIMS`, `TOTAL_PAID`
- **Date range**: 2018–2024

### Patient Identifiers Present
**None.** The dataset contains no patient names, SSNs, dates of birth, diagnoses,
demographics, or any other patient-level identifier.

### CMS Cell Suppression
- **CMS suppression threshold**: k=11 (cells with <11 beneficiaries are suppressed)
- **Minimum beneficiaries observed**: 12
- **Suppression confirmed**: YES

### Beneficiary Count Distribution

| Range | Row Count | % of Total |
|-------|-----------|-----------|
| ≤11 | 0 | 0.0% |
| 12–20 | 103,843,451 | 45.7% |
| 21–50 | 81,846,641 | 36.0% |
| 51–100 | 24,729,627 | 10.9% |
| >100 | 16,663,642 | 7.3% |

**Unique quasi-identifier combinations** (NPI × HCPCS × Month): 129,311,270

> **Assessment: LOW PATIENT RISK** — No patient-level data exists in the dataset.
> k≥12 suppression floor. Public Use File published for transparency.

---

## 2. Provider Identifiability in Investigation Outputs

The investigation scripts enrich the raw data by joining NPI registry information,
adding provider names, addresses, and authorized officials. This section catalogs
the identifiability of each output file.

| File | Investigation | Rows | Individuals | Organizations | Identifiability | Risk |
|------|--------------|------|-------------|---------------|-----------------|------|
| ghost_providers_impossible_volume.csv | Ghost Providers | 37 | 37 | 0 | DIRECT | HIGH |
| ghost_providers_address_clustering.csv | Ghost Providers | 2,544 | 0 | 0 | QUASI | MODERATE |
| shell_company_connections.csv | Shell Companies | 41 | 41 | 0 | DIRECT | HIGH |
| individual_oig_matches.csv | OIG Matches | 14 | 14 | 0 | DIRECT | LOW |
| individual_top_spenders.csv | Individual Deep Dive | 500 | 500 | 0 | DIRECT | MODERATE |
| individual_specialty_outliers.csv | Individual Deep Dive | 5,949 | 5,949 | 0 | DIRECT | MODERATE |
| t1019_brooklyn_analysis.csv | Brooklyn T1019 | 316 | 2 | 314 | DIRECT | MODERATE |
| t1019_shared_addresses.csv | Brooklyn T1019 | 47 | 0 | 0 | QUASI | MODERATE |
| t1019_oig_matches.csv | Brooklyn T1019 | 1 | 0 | 0 | AGGREGATE | LOW |
| minnesota_anomalies.csv | Minnesota | 566 | 4 | 562 | DIRECT | MODERATE |
| minnesota_behavioral_health.csv | Minnesota | 6 | 0 | 0 | AGGREGATE | LOW |
| minnesota_temporal.csv | Minnesota | 1,705 | 1,705 | 0 | DIRECT | LOW |
| temporal_spikes.csv | Temporal | 5,756 | 202 | 5,477 | DIRECT | LOW |
| temporal_new_entrants.csv | Temporal | 249 | 2 | 245 | DIRECT | LOW |
| temporal_disappearances.csv | Temporal | 8,715 | 477 | 7,251 | DIRECT | LOW |
| cross_state_billing.csv | Cross-State | 63,524 | 1,131 | 62,393 | DIRECT | MODERATE |
| traveling_fraudsters.csv | Traveling Fraudsters | 1,002 | 0 | 1,002 | DIRECT | HIGH |

**Total named across all files**: ~10,064 individual provider rows,
~77,244 organization rows (with overlap across files).

### Identifiability Tiers
- **DIRECT**: Provider name + NPI — uniquely identifies a real person/entity
- **QUASI**: Address, ZIP, or NPI without name — can be linked to identity via public registries
- **AGGREGATE**: Summary statistics only — no individual identification possible

---

## 3. Patient Re-identification Risk

Even though the dataset contains no patient identifiers, we assess whether
combining dimensions could narrow to identifiable patient populations.

- **Rows at suppression floor** (bene = 12): 18,197,015
- **Rare HCPCS codes** (<100 national bene sum): 1,954 / 10,881

### K-anonymity by (HCPCS × Month)
- Total groups: 535,922
- Minimum group beneficiary sum: 12
- Median group beneficiary sum: 390.00
- Groups with <50 beneficiary sum: 121,728

> **Assessment: MODERATE** — Significant number of rare HCPCS codes (1954/10881). While k≥12 applies, combining rare service + NPI + month could narrow beneficiary populations to small groups.

**Important caveat**: The dataset does not contain any patient demographics (age, sex,
race, diagnosis). Without these linkage keys, even rare service + provider + month
combinations cannot be connected to specific individuals. The re-identification risk
is theoretical and would require an external dataset with patient-provider-service
linkages that is not publicly available.

---

## 4. Accusation Risk Assessment

This section evaluates the reputational risk to providers named in investigation outputs.
A statistical anomaly is not proof of fraud — this table assesses how each investigation's
framing and output might be perceived.

| Investigation | Individuals | Organizations | Risk Tier | Rationale |
|--------------|-------------|---------------|-----------|-----------|
| Ghost Providers | 37 | 0 | **HIGH** | Names 37 individuals as billing beyond physical capacity — direct fraud accusation by name. |
| Shell Companies | 41 | 0 | **HIGH** | Links 41 named individuals to organizations at shared addresses — implies coordinated fraud. |
| OIG Matches | 14 | 0 | **LOW** | Matches against the publicly-available federal exclusion list. These individuals are already publicly identified by HHS-OIG. |
| Individual Deep Dive | 6,449 | 0 | **MODERATE** | Names ~6,400 providers as statistical outliers. Not direct accusations, but 'outlier' framing implies wrongdoing. |
| Brooklyn T1019 | 2 | 314 | **MODERATE** | Names ~316 providers in geographic cluster analysis. Shared-address finding implies coordination. |
| Minnesota | 1,709 | 562 | **MODERATE** | Names ~566 MN providers flagged by anomaly scoring. Anomaly flags are statistical, not conclusive. |
| Temporal Patterns | 681 | 12,973 | **LOW** | ~14,700 providers flagged for temporal patterns. Framed as billing behavior changes, not accusations. |
| Cross-State | 1,131 | 62,393 | **MODERATE** | ~63,500 providers billing across states. Large pool dilutes individual risk, but naming + framing matters. |
| Traveling Fraudsters | 0 | 1,002 | **HIGH** | ~1,002 authorized officials linked to vanished-then-new organizations. 'Fraudster' label in filename is accusatory. |

### Recommended Mitigations

**Ghost Providers** (HIGH)
- Use anonymized IDs instead of names
- Add disclaimer: statistical flag ≠ confirmed fraud
- Recommend referral to CMS/OIG rather than public naming

**Shell Companies** (HIGH)
- Use anonymized IDs instead of names
- Add disclaimer: statistical flag ≠ confirmed fraud
- Recommend referral to CMS/OIG rather than public naming

**OIG Matches** (LOW)
- Low risk — publicly available or aggregate data
- Standard disclaimers sufficient

**Individual Deep Dive** (MODERATE)
- Present as statistical patterns, not accusations
- Include base rates and methodology context
- Consider aggregating to ZIP/region level instead of naming

**Brooklyn T1019** (MODERATE)
- Present as statistical patterns, not accusations
- Include base rates and methodology context
- Consider aggregating to ZIP/region level instead of naming

**Minnesota** (MODERATE)
- Present as statistical patterns, not accusations
- Include base rates and methodology context
- Consider aggregating to ZIP/region level instead of naming

**Temporal Patterns** (LOW)
- Low risk — publicly available or aggregate data
- Standard disclaimers sufficient

**Cross-State** (MODERATE)
- Present as statistical patterns, not accusations
- Include base rates and methodology context
- Consider aggregating to ZIP/region level instead of naming

**Traveling Fraudsters** (HIGH)
- Use anonymized IDs instead of names
- Add disclaimer: statistical flag ≠ confirmed fraud
- Recommend referral to CMS/OIG rather than public naming


---

## 5. Ethical Framework & Recommendations

### 5.1 Is the Dataset Ethically Analyzable?

**YES.** The CMS Medicaid Provider Utilization and Payment dataset is a Public Use File
published by the Centers for Medicare & Medicaid Services on data.medicaid.gov specifically
for transparency and public accountability purposes. No Data Use Agreement is required.

CMS publishes this data under the authority of the Affordable Care Act §1311(e)(3) and
subsequent transparency mandates. The dataset has been pre-processed by CMS to remove
patient-identifying information and apply k=11 cell suppression per the Privacy Act of 1974
and HIPAA Privacy Rule (45 CFR §164.514).

Analyzing public spending data for patterns of waste, fraud, and abuse is not only ethical —
it is the stated purpose for which CMS releases this data.

### 5.2 Where the Ethical Line Sits

The ethical concern is not data *access* (it is all public) but *responsible use* —
specifically, the gap between statistical anomaly and confirmed fraud:

| Action | Ethical Status |
|--------|---------------|
| Analyzing aggregate billing patterns | **Clearly ethical** — public accountability |
| Identifying statistical outliers by NPI | **Ethical with caveats** — outlier ≠ fraud |
| Naming individuals as 'outliers' | **Ethically borderline** — reputational harm risk |
| Naming individuals as 'fraudsters' or 'ghosts' | **Ethically problematic** — accusation without adjudication |
| Publishing named lists publicly | **Requires strong justification** — due process concerns |

**Key principle**: A billing pattern that looks anomalous from data alone may have legitimate
explanations (group practices billing under one NPI, authorized billing arrangements, specialty
case mix differences, state Medicaid policy variations).

### 5.3 Recommendations for Reporting

#### Can name providers freely:
- **OIG exclusion matches** — These individuals are already publicly identified by HHS-OIG
  in the List of Excluded Individuals/Entities (LEIE). Naming them adds no new harm.
- **Aggregate statistics** (e.g., 'X providers in Brooklyn bill Y% above national average')

#### Should use anonymized identifiers:
- **Ghost providers** — 'Provider A in State X billed Z claims' rather than naming
- **Shell company connections** — Describe patterns without naming individuals
- **Traveling fraudsters** — Rename file and use anonymized official identifiers

#### Should present as statistical patterns, not accusations:
- **Specialty outliers** — Frame as 'providers warranting further review' not 'fraudsters'
- **Top spenders** — High spending is not inherently wrong; context matters
- **Temporal anomalies** — Billing spikes may reflect practice changes, not fraud

#### Language recommendations:
- Replace 'ghost provider' with 'provider with implausible billing volume'
- Replace 'fraudster' with 'provider warranting further review'
- Replace 'shell company' with 'organization with shared-address billing patterns'
- Add universal disclaimer: 'These findings reflect statistical patterns in public billing data.
  They do not constitute evidence of fraud and should not be interpreted as accusations.
  Definitive determination of fraud requires investigation by CMS, OIG, or law enforcement.'

### 5.4 CMS's Stated Purpose

CMS publishes provider utilization data to:
1. **Promote transparency** in how Medicaid dollars are spent
2. **Enable research** into healthcare delivery patterns
3. **Support program integrity** by making billing patterns visible
4. **Inform policy** decisions about Medicaid program design

CMS explicitly does NOT publish this data for the purpose of individual provider accusation.
The data is designed for aggregate analysis and pattern identification, with the expectation
that suspected anomalies will be referred to appropriate oversight bodies (OIG, state Medicaid
Fraud Control Units) for proper investigation with due process protections.

---

## Summary

| Dimension | Assessment |
|-----------|-----------|
| Raw data patient privacy | **LOW** risk — k≥12, no patient identifiers |
| Patient re-identification | **MODERATE** risk — Significant number of rare HCPCS codes (1954/10881). While k≥12 applies, combini... |
| Provider identifiability | **HIGH** in outputs — names, NPIs, addresses added via NPI join |
| Accusation risk | **HIGH** for: Ghost Providers, Shell Companies, Traveling Fraudsters |
| Ethical to analyze | **YES** — Public Use File, published for transparency |
| Ethical to name individuals | **CONDITIONAL** — OIG matches only; all others should anonymize |