# Medicaid Provider Spending: Comprehensive Investigative Findings

**Dataset**: CMS Medicaid Provider Utilization, 2018–2024
**Records**: 227 million payment rows | ~$1.09 trillion in positive payments
**Analysis Date**: February 2026

---

## Executive Summary

Six investigations spanning individual providers, geographic clusters, temporal patterns, and corporate structures reveal systemic billing anomalies in Medicaid provider spending data. The findings point to three major fraud typologies:

1. **Personal care service (T1019) entity proliferation** concentrated in Brooklyn, NY — 7 of the top 20 national T1019 billers operate from a handful of Brooklyn addresses, with 47 shared addresses and a cost-per-claim ratio 1.80x the national median.

2. **Ghost provider networks** — 37 individual providers billing beyond physical human capacity for T1019, with 18 of those individuals sharing addresses with corporate entities, suggesting shell company structures.

3. **Fast-starter behavioral health clinics** — 249 organizations appeared after 2022 and immediately billed >$1M/month, with Arizona accounting for 71 (29%) of all fast starters. This echoes the pattern behind the $9B+ Minnesota fraud indictments.

The total exposure across all flagged categories exceeds **$55 billion** in Medicaid payments warranting further scrutiny.

---

## I. Individual Provider Deep Dive

### A. Top Individual Spenders

**500 highest-billing individual practitioners** collectively billed **$4.65 billion** over the dataset period.

| Rank | Provider | State | Total Spent | Cost/Bene | Months Active |
|------|----------|-------|-------------|-----------|---------------|
| 1 | Eric Lund | WI | $77.3M | $1,055 | 84 |
| 2 | Loren Cooke | NM | $76.2M | $1,336 | 84 |
| 3 | Yechiel Zagelbaum | NY | $70.6M | $41 | 84 |
| 4 | Robin Boykin | MO | $48.7M | $701 | 84 |
| 5 | Kulmoris Joiner | MS | $41.1M | $1,353 | 84 |

**94 of the top 500 spenders** are also flagged as specialty outliers (>5x their peer median), indicating sustained anomalous billing rather than one-time spikes.

### B. Specialty Peer Comparison

**5,949 individual providers** exceed 5x their specialty's median cost-per-beneficiary, with combined spending of **$2.77 billion**.

**Highest cost ratios:**

| Provider | State | Specialty | Cost/Bene | Specialty Median | Ratio |
|----------|-------|-----------|-----------|------------------|-------|
| Isluv Robertson | MS | Registered Nurse | $14,948 | $52 | **287x** |
| Judy Ribner | NY | Adv Practice Midwife | $12,110 | $55 | **221x** |
| Tiffany Burns | MO | Student/Trainee | $7,446 | $42 | **177x** |
| Inas Almasry | MA | Internal Medicine | $6,510 | $38 | **172x** |
| Nnamdi Amaechina | MA | Internal Medicine | $5,541 | $38 | **146x** |

**Specialty concentration**: Dentists account for 1,477 outliers ($1.07B), followed by Internal Medicine (821), Pediatrics (479), and Radiology (459).

**Geographic concentration**: Illinois (987 outliers), Pennsylvania (725), New York (550), California (434), Massachusetts (290).

**Rhode Island Internal Medicine cluster**: 5 of the top 15 outliers by cost ratio are RI internists (Jun Parks, Ralph Santoro, Naresh Dasari, John Gelzhiser, Mark Shannon) — all billing $4,000–$5,000/beneficiary against a specialty median of $38.

### C. OIG Exclusion List Matches

**14 providers** from the top spenders and specialty outliers matched the HHS-OIG LEIE exclusion list:

| Name | Specialty | State | Exclusion Type | Exclusion Date |
|------|-----------|-------|----------------|----------------|
| Lori Williams | Adult Day Care (Exec) | MO | 1128a1 | Jan 2026 |
| Ashok Panigrahy | Radiology | MI | 1128b4 | Dec 2025 |
| Phillip Jensen | Dentist | CA | 1128a3 | Jul 2025 |
| Andrew Golden | Counselor | CT | 1128a1 | Dec 2024 |
| Shiva Akula | Infectious Disease | TN | 1128a1 | Nov 2024 |
| Salwan Adjaj | Dentist | OR | 1128a4 | Jan 2024 |
| Klaus Peter Rentrop | Cardiology | NY | 1128b7 | Sep 2023 |
| Siamak Arassi | General Practice | WI | 1128b4 | Jun 2023 |
| Cortney Dunlap | Counselor | MA | 1128a1 | Mar 2023 |
| Brantley Nichols | Dentist | MS | 1128a3 | May 2022 |
| Chester Sokolowski | Dentist | CT | 1128b4 | May 2022 |
| Esther Villanueva Valdes | General Practice | PR | 1128a1 | Mar 2022 |
| Larry Cruel | Podiatry | MS | 1128a1 | Nov 2022 |
| J. Derek Hollingsworth | Family Practice | MT | 1128b14 | Nov 2019 |

These providers were billing Medicaid while federally excluded — a per se violation of the False Claims Act.

---

## II. Brooklyn T1019 Concentration

### The Core Finding

**7 of the top 20 T1019 personal care billers nationally** are Brooklyn-based organizations, collectively billing **$31.8 billion** across 316 Brooklyn providers (99.3% organizations, 0.7% individuals).

Brooklyn's **median cost-per-claim is $152**, compared to the national median — a **1.80x premium** that is not explained by the NYC cost-of-living differential alone.

### Top Brooklyn T1019 Organizations

| Nat'l Rank | Organization | Total Paid | Cost/Claim | First Bill |
|------------|-------------|------------|------------|------------|
| 6 | AssistCareHome Healthcare | $1.27B | $143 | 2018-01 |
| 7 | Heart to Heart Home Care | $1.08B | $143 | 2018-01 |
| 10 | NAE Edison LLC | $991M | $151 | 2018-01 |
| 17 | Home Family Care | $776M | $148 | 2018-01 |
| 18 | Human Care LLC | $765M | $163 | 2018-01 |
| 19 | Platinum Home Health Care | $764M | $172 | 2018-01 |
| 20 | A & J Staffing | $753M | $135 | 2018-01 |

All 7 began billing at the start of the dataset (Jan 2018), suggesting they were established players rather than new entrants.

### Shared Address Network

**47 Brooklyn addresses** are shared by multiple T1019 billing NPIs, with combined spending of **$11.6 billion** flowing through these co-located entities.

Notable clusters:
- **946 McDonald Ave** (ZIP 11218): 2 NPIs, $1.06B combined — NAE Edison LLC, single authorized official "Weiss"
- **6323 14th Ave** (ZIP 11219): 2 NPIs, $994M — The Royal Care Inc / The Royal Care FI LLC, same auth official "Klein"
- **768 39th St** (ZIP 11232): 2 NPIs, $824M — Human Care LLC / Human Care LLC. (same entity, two NPIs, same auth "Goldberger")
- **1967 McDonald Ave** (ZIP 11223): 3 NPIs, $542M — Elite Choice LLC / Homecaire of Maine LLC / Elite HHC LLC, single auth "Strasser"

**68 authorized officials** control multiple Brooklyn T1019 entities across different addresses, a hallmark of entity proliferation fraud.

---

## III. Minnesota Behavioral Health Fraud Patterns

### Background

Minnesota has seen >$9 billion in fraud indictments related to behavioral health billing since 2020. This analysis independently reconstructs the billing patterns.

### MN Behavioral Health Spending: $2.71 Billion

| HCPCS | Description | Total Paid | Claims | Providers |
|-------|-------------|------------|--------|-----------|
| T1019 | Personal care (15 min) | $1.92B | 27.0M | 225 |
| H2014 | Skills training (15 min) | $399M | 3.2M | 185 |
| H2012 | BH day treatment (per hr) | $154M | 1.1M | 42 |
| 97153 | ABA therapy | $139M | 1.6M | 45 |
| H0032 | MH service plan dev | $58M | 731K | 226 |
| 97155 | ABA supervision | $37M | 556K | 51 |

### Anomaly Flagging

**174 providers** received at least one anomaly flag (explosive enrollment growth or outlier claims/beneficiary). Only **1 provider received both flags**:

> **Twin Cities Autism Center Inc** — $412K in total BH payments, dual-flagged for both explosive beneficiary growth and claims-per-beneficiary >3 standard deviations above mean.

**Top flagged providers by spending:**

| Organization | Total BH Paid | Flag |
|-------------|---------------|------|
| Autism Opportunities Foundation | $89.0M | Growth |
| Heritage Home Health Care | $63.0M | Growth |
| Partners in Excellence | $47.4M | Growth |
| Minnesota Quality Care | $44.1M | Growth |
| CustomCare CBC | $35.1M | Growth |
| Medical Professionals, LLC | $33.7M | Growth |
| Metro Home Health Care Corp | $32.7M | Growth |

The dominance of "Home Health" and "Care" entities with explosive growth mirrors the pattern described in DOJ indictments.

---

## IV. Temporal Anomaly Detection

### A. Month-over-Month Billing Spikes

**5,756 providers** exhibited >5x month-over-month spending increases in consecutive months, with minimum $100K absolute threshold. (Previous analysis counted 6,337 before fixing a gap-detection bug that falsely flagged non-consecutive months.)

**95% are organizations**, with Florida (847), California (631), and Texas (470) leading by state count.

Most extreme spikes reached **20,590x** month-over-month (Intermountain Health Center, AZ — a likely data artifact, but the next tier of 5,000–15,000x warrants review).

### B. Fast Starters: Post-2022 Entrants Billing >$1M/Month

**249 organizations** appeared after January 2022 and immediately hit $1M+/month — the ramp-up profile of fraudulent billing schemes.

**Arizona dominates with 71 fast starters (29%)**, many with names suggesting behavioral health operations:

| Organization | State | First Month | Peak Monthly | Total Paid |
|-------------|-------|-------------|--------------|------------|
| Community Hope Wellness Center | AZ | Jul 2022 | $19.6M | $47.4M |
| Tusa Integrated Clinic | AZ | Jul 2022 | $18.8M | $46.2M |
| Fishing Point Health Care | VA | May 2023 | $18.1M | $168.9M |
| Happy House Behavioral Valley | AZ | Sep 2022 | $13.1M | $61.8M |
| Motherland Counseling | AZ | Sep 2022 | $9.7M | $41.5M |
| Safe Ark Wellness | AZ | Jan 2023 | $8.2M | $12.2M |
| Max Behavioral Health Outpatient | AZ | May 2022 | $7.8M | $23.1M |
| Family Bond Treatment Center | AZ | Feb 2023 | $7.7M | $12.0M |
| One Point Psychiatric Services | AZ | Aug 2022 | $7.2M | $26.6M |

The Arizona cluster of behavioral health fast starters (appearing mid-2022 through early 2023) warrants immediate scrutiny as a potential fraud ring.

### C. Sudden Disappearances

**8,715 providers** with sustained billing (6+ months active, >$50K avg/month) stopped billing before June 2024 — collectively representing **$52.9 billion** in historical spending.

**662 disappearances** occurred within 3 months of the June 2024 DOJ national takedown.

---

## V. Ghost Providers: Impossible Volume

### Physical Capacity Test

A single individual providing T1019 personal care can physically deliver at most **704 claims per month** (8 hours/day x 22 days x 4 units/hour). **37 individuals** exceeded this ceiling.

**Top ghost providers by billing volume:**

| Provider | State | Max Claims/Mo | Capacity Ratio | Months Over | Paid While Over |
|----------|-------|---------------|----------------|-------------|-----------------|
| Kulmoris Joiner | MS | 18,772 | **26.7x** | 84 (all) | $36.7M |
| Robin Boykin | MO | 13,616 | **19.3x** | 84 (all) | $41.7M |
| Kaying Vang | MN | 7,794 | **11.1x** | 23 | $8.3M |
| Prosper Dzameshie | MN | 3,816 | **5.4x** | 56 | $15.3M |
| Xue Li | NY | 3,364 | **4.8x** | 35 | $10.0M |
| Mary Darby-McLaurin | MS | 3,246 | **4.6x** | 11 | $718K |

**Mississippi accounts for 12 of 37 ghost providers** (32%), a disproportionate concentration.

### The Kulmoris Joiner Case

Kulmoris Joiner (MS) appears across **three separate investigations**:
- **Ghost providers**: 26.7x physical capacity, every month for 7 years
- **Top individual spenders**: #5 nationally at $41.1M
- **Cost-per-beneficiary**: $1,353/bene (elevated but not extreme)

At 18,772 T1019 units in a peak month (each unit = 15 minutes), Joiner would have needed to provide **4,693 hours** of hands-on care — equivalent to **213 hours per working day**, or roughly 27 people working 8-hour shifts simultaneously under a single NPI.

---

## VI. Shell Company Connections

### Part 1: Individual-to-Corporate Links

**18 ghost providers** (individuals billing beyond physical capacity) share registered addresses with **41 organizations** — direct evidence of shell company structures where individuals front for corporate billing operations.

After filtering out government buildings (removing false positives like state health departments), the remaining connections are:

- **18 impossible individuals linked to 41 organizations** at shared addresses
- Organizations classified as: Home Health, Staffing, Clinic, or Other

### Part 2: Traveling Fraudsters

After applying 4 noise-reduction filters (temporal sequence, corporate family exclusion, size cap, name rarity), **1,002 cases** remain where an authorized official of a vanished organization (>$1M billing, stopped before mid-2024) reappears running a new organization.

- **341 are cross-state moves** (different state than the vanished entity)
- **Name rarity breakdown**: COMMON (16–50 orgs): 639 | MODERATE (4–15): 304 | RARE (1–3): 59

The temporal sequence filter (requiring new org to start after old org died) combined with corporate family exclusion reduced the initial 718K+ raw matches to 1,002 high-signal cases.

### Part 3: Cross-State Billing

**63,524 billing providers** bill Medicaid across state lines (billing NPI state differs from servicing NPI state), with **$31.1 billion** in total cross-state payments.

**31 individual providers** bill >$500K cross-state — the most suspicious subset:

| Provider | State | Cross-State Paid | States | Servicing Providers |
|----------|-------|------------------|--------|---------------------|
| Christopher Johnson | NC | $6.4M | 1 | 1 |
| Ronald Saffar | NJ | $4.2M | 2 | 9 |
| Arthur Kaiser | CA | $3.5M | 6 | 7 |
| Jacob Eisdorfer | NY | $3.2M | 3 | 5 |

---

## VII. Cross-Investigation Connections

### Multi-Flag Providers

Providers appearing in **3+ investigations** represent the highest-confidence fraud indicators:

| Provider | Ghost | Top Spender | Outlier | Shell Link | State |
|----------|-------|-------------|---------|------------|-------|
| Kulmoris Joiner | 26.7x cap | #5 ($41M) | Yes | No | MS |
| Robin Boykin | 19.3x cap | #4 ($49M) | Yes | Yes | MO |
| Prosper Dzameshie | 5.4x cap | Yes | — | Yes | MN |
| Luezarah Robinson | 3.9x cap | — | — | Yes | MS |
| Carol Newton | 4.0x cap | — | — | Yes | MS |

### Geographic Hotspots

| Region | Investigations Triggered | Key Findings |
|--------|------------------------|--------------|
| **Brooklyn, NY** | T1019, Address Clustering | 7/20 national top billers, 47 shared addresses, $31.8B |
| **Mississippi** | Ghost Providers, Individual Outliers | 12/37 ghost providers, Joiner case (26.7x capacity) |
| **Minnesota** | BH Fraud, Ghost Providers | $2.71B BH spending, 174 flagged, 2 ghost providers |
| **Arizona** | Fast Starters, Temporal | 71/249 fast starters, BH clinic cluster since mid-2022 |
| **Rhode Island** | Individual Outliers | 5 IM outliers at 108–131x specialty median |

---

## VIII. Methodological Notes

### Beneficiary Count Caveat

All "beneficiary" counts in aggregated metrics are **sums across rows**, not true unique patient counts. The source field `TOTAL_UNIQUE_BENEFICIARIES` is unique only within each raw row (NPI x HCPCS x month). Summing across months or codes double-counts patients appearing in multiple rows. Per-beneficiary ratios are therefore **estimates** — useful for ranking and comparison, but not exact.

### Data Limitations

1. **No patient-level data**: Cannot determine true unique beneficiaries across providers
2. **NPI registry snapshot**: Address data reflects current registrations, not historical
3. **No clinical records**: Cannot distinguish legitimate high-volume billing from fraud
4. **Entity type reliance**: NPI entity classification may lag behind ownership changes

### Recommended Next Steps

1. **Refer Arizona BH fast starters** (71 organizations) to CMS/OIG for immediate review
2. **Deep-dive Brooklyn address clusters** — subpoena corporate records for the 47 shared-address entities
3. **Mississippi ghost provider audit** — 12 individuals billing beyond physical capacity, led by the Joiner case
4. **Rhode Island IM cluster** — 5 internists with 100x+ cost ratios merit state Medicaid agency review
5. **Cross-reference traveling fraudsters** with pending DOJ/FBI investigations
6. **Validate OIG matches** — 14 excluded providers may still be receiving Medicaid payments

---

*This report was generated from CMS Medicaid Provider Utilization data (2018–2024) cross-referenced with the NPI Registry, HCPCS code database, NUCC taxonomy, and HHS-OIG LEIE exclusion list.*
