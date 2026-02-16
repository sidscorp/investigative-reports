# Evaluation and Management Upcoding in Medicaid: A Multi-Method Statistical Outlier Detection Framework Applied to 1.53 Billion Office Visit Claims

**Dataset**: CMS Medicaid Provider Utilization and Payment Data, 2018–2024
**Records Analyzed**: 1.53 billion E&M claims | $78.5 billion in payments | 219,664 unique billing NPIs
**Analysis Date**: February 2026

> **DISCLAIMER**: This analysis is a screening tool that identifies statistical outliers in Evaluation & Management (E&M) coding patterns. It does not constitute evidence of fraud, waste, or abuse. Providers treating sicker, more complex, or referral-heavy patient populations will legitimately bill higher-level codes. No diagnosis-code or chart-review data is available in this dataset. Results should be interpreted as flags warranting further clinical review, not as conclusions about any provider's billing conduct.

---

## Abstract

Evaluation and Management (E&M) office visit codes represent the single largest category of professional service claims in the U.S. healthcare system, with annual Medicare Part B improper payments for E&M services alone estimated at $6.7 billion (OIG, 2014). Despite this significance, systematic analysis of E&M coding patterns in Medicaid — a program serving over 90 million enrollees — has received comparatively little attention in the literature. We present a multi-method framework for detecting anomalous E&M coding intensity among Medicaid providers, analyzing 1.53 billion claims across 219,664 billing entities between 2018 and 2024. Our approach introduces several methodological refinements over standard distributional screening: (1) a price-weighted coding intensity index that avoids arbitrary ordinal weights, (2) independent estimation across the pre- and post-2021 eras to isolate CMS's January 2021 E&M code redefinition from provider behavior, (3) sub-specialty benchmarking at the NUCC Specialization level, (4) separate treatment of new-patient and established-patient code families, and (5) a practice-profile regression adjustment that partially deconfounds case-mix from coding behavior. We further validate the highest-confidence flags through cross-investigation convergence with five independent fraud-detection methodologies. The analysis identifies 2,390 post-2021 outliers (2.4%), of whom 890 (45% of established-patient outliers) survive practice-profile adjustment — a population with narrower practice scope, higher E&M billing concentration, and more extreme statistical deviations than the explained set. A tiered confidence framework — incorporating cross-era persistence, cross-family breadth, adjustment survival, and multi-methodology convergence — yields 3 Tier A providers, 890 Tier B persistent outliers, and 29 Tier C cross-family/cross-era outliers for prioritized review. We discuss the irreducible limitation of case-mix confounding in the absence of diagnosis data and outline three paths toward full risk adjustment.

---

## 1. Introduction

### 1.1 The Problem of E&M Upcoding

Evaluation and Management (E&M) codes — the Current Procedural Terminology (CPT) codes used to bill for physician office visits — are the most commonly billed procedure codes in ambulatory medicine. They are also, by the nature of their design, among the most susceptible to upcoding. Unlike surgical procedures or diagnostic imaging, where the service performed is largely verifiable from the claim itself, E&M code selection rests on a physician's self-reported assessment of medical decision-making complexity — a judgment that is inherently subjective and difficult to audit at scale.

This subjectivity has not escaped the attention of regulators. The HHS Office of Inspector General's landmark 2014 study found that 42% of Medicare E&M claims were incorrectly coded, with improper payments totaling $6.7 billion in a single year — representing a rate 50% higher than other Part B services (OIG-04-10-00181). More recently, OIG identified CPT 99214 as the single most frequently misreported E&M code to Medicare in 2023, generating $564.6 million in improper payments for that code alone. These figures reflect Medicare; analogous Medicaid-specific estimates do not exist in the published literature, a gap this analysis begins to address.

The economic incentives for upcoding are straightforward. In the established-patient office visit series (99211–99215), the difference between a Level 3 visit (99213, national average Medicaid reimbursement ~$50.86) and a Level 4 visit (99214, ~$66.55) is approximately $15.69 per encounter. For a provider conducting 10,000 office visits annually — a realistic volume for a busy primary care practice — systematically coding one level higher than warranted produces approximately $157,000 in excess annual revenue. At the population level, the dominance of 99213 and 99214 (together comprising 83–85% of all E&M claims) means that even a marginal shift in the boundary between these two codes translates into billions of dollars in aggregate spending.

The academic literature on upcoding reflects this concern. Geruso and Layton (2020, *Journal of Political Economy*) demonstrated that Medicare Advantage enrollees generate 6–16% higher diagnosis-based risk scores than equivalent fee-for-service patients, costing Medicare an estimated $10.5 billion annually. Silverman and Skinner (2004, *Journal of Health Economics*) documented systematic coding intensity variation across hospitals following the introduction of DRG-based prospective payment. Bastani, Goh, and Bayati (2019, *Management Science*) identified upcoding in pay-for-performance programs, estimating that 18.5% of present-on-admission infection claims were upcoded hospital-acquired infections, costing Medicare $200 million annually. While these studies focus on Medicare inpatient and risk-adjustment settings, the underlying principal-agent dynamic — providers selecting from a menu of self-reported complexity levels with financial consequences — applies directly to ambulatory E&M coding.

Enforcement actions confirm that E&M upcoding is not merely a theoretical concern. In November 2024, the Department of Justice announced a $23 million settlement with UCHealth (University of Colorado Health) for systematically applying the highest emergency department E&M code (99285) using an automated billing rule that did not reflect actual clinical complexity. Bluestone Physician Services settled for $14.9 million over E&M upcoding in chronic care settings. IPC Hospitalists of Michigan paid $4.4 million for allowing physicians to bill for more inpatient E&M services than they could plausibly provide in a single day. These cases illustrate a range of upcoding mechanisms — from automated billing system design to individual physician coding behavior — all exploiting the subjectivity inherent in E&M code selection.

### 1.2 The Medicaid Context

While Medicare program integrity has been the subject of extensive academic and regulatory attention, Medicaid — which covers over 90 million Americans and spent approximately $805 billion in federal and state funds in fiscal year 2023 — presents distinct analytical challenges. Medicaid program integrity responsibility is split between state Medicaid agencies (responsible for provider enrollment, claims processing, and data analytics) and Medicaid Fraud Control Units (MFCUs, typically housed in state attorneys general offices and responsible for criminal prosecution). In fiscal year 2024, the 53 MFCUs collectively secured 1,151 convictions, $961 million in criminal recoveries, and $407 million in civil recoveries — with California alone accounting for $513 million (53%) of criminal recoveries.

This federated structure means that no single entity routinely conducts the kind of national, cross-state E&M coding analysis that CMS and OIG perform for Medicare. State-level analyses are constrained by their own program data and may miss patterns that are visible only in the aggregate — such as the California geographic concentration we identify in this analysis. The CMS Medicaid Provider Utilization and Payment Data, which aggregates state-level claims into a national dataset, creates an opportunity for the kind of systematic, population-level coding analysis that has historically been available only for Medicare.

### 1.3 What Are E&M Office Visit Codes?

The office visit subset of E&M codes consists of two families:

| Family | Codes | Description |
|--------|-------|-------------|
| **New Patient** | 99202–99205 | First visit with a provider; higher documentation and clinical assessment requirements |
| **Established Patient** | 99211–99215 | Follow-up visits; ranging from minimal complexity (99211, often nurse-only) to high complexity (99215) |

Higher-numbered codes reimburse at higher rates and are intended for encounters requiring greater clinical decision-making, more extensive data review, or management of higher-risk conditions. The system is explicitly ordinal: a Level 4 visit (99214) should involve more complex medical decision-making than a Level 3 visit (99213), and the reimbursement differential is designed to compensate for that additional cognitive work.

### 1.4 The 2021 Structural Break

Effective January 1, 2021, CMS implemented the most significant redesign of E&M office visit coding since the codes were introduced. The changes, developed jointly by the CPT Editorial Panel and the AMA's Relative Value Scale Update Committee (RUC) with input from over 1,700 surveyed physicians across more than 50 specialties, were motivated by three objectives: reducing administrative documentation burden (CMS estimated 180 hours of annual paperwork savings per physician), promoting coding consistency, and eliminating documentation requirements not relevant to patient care.

The substantive changes were:

- **Pre-2021**: Code selection required satisfying documentation thresholds across three "pillars" — history, physical examination, and medical decision-making (MDM). All three had to be documented at the appropriate level. This created extensive checkbox-style documentation with limited clinical value but significant compliance risk.
- **Post-2021**: Code selection is based on MDM complexity **or** total time only. History and physical examination are no longer components of code-level selection (though they remain part of good clinical practice). The MDM framework was simplified to three elements (two of three required): number and complexity of problems addressed, amount and complexity of data reviewed and analyzed, and risk of complications and/or morbidity or mortality of patient management.

This produced measurable shifts in the national code mix:

| Code | Pre-2021 Share | Post-2021 Share | Change |
|------|---------------|-----------------|--------|
| 99213 | 51.5% | 47.9% | -3.6pp |
| 99214 | 32.8% | 34.6% | +1.8pp |
| 99215 | 1.7% | 2.2% | +0.5pp |

The observed shift toward higher-level codes is consistent with the stated intent of the redefinition: many encounters that were previously documented at Level 3 (99213) to avoid audit risk under the rigid three-pillar system were, under the simplified MDM criteria, more appropriately classified as Level 4 (99214). Whether this represents "upcoding" or "more accurate coding" is precisely the ambiguity that makes E&M analysis challenging — and is the reason this analysis treats the pre-2021 and post-2021 periods as independent analytical universes.

The accompanying price adjustments reflected both the code redefinition and general Medicare Physician Fee Schedule updates. The national average reimbursement for 99215 increased from $84.56 (pre-2021) to $105.43 (post-2021), a 24.7% increase. MedPAC noted that the changes tended to narrow the income gap between procedure-heavy specialists and primary care providers, as E&M services represent a larger share of primary care revenue.

**Analytical consequence:** Any methodology that pools data across the 2021 boundary will confuse structural code migration with provider-level behavioral deviation. A provider whose coding appears to "worsen" from 2019 to 2022 may simply be responding appropriately to the new guidelines. This analysis treats the two eras as fully independent — with separate prices, benchmarks, thresholds, and outlier determinations — and uses cross-era persistence as a *signal of provider behavior* rather than as a baseline comparison.

### 1.5 Prior Work on E&M Coding Analysis

The literature on E&M coding analysis falls into three broad categories, each with distinct methodological commitments:

**Chart-review audits** (the gold standard) compare billed codes against independent physician review of the medical record. OIG's 2014 study used this approach on a random sample of Medicare claims, finding a 42% incorrect coding rate. Chart review is definitive but expensive, slow, and not scalable to population-level screening.

**Claims-based distributional methods** compare a provider's code-level distribution against peers. This includes simple "percentage of high-level codes" metrics (used by many state Medicaid agencies and commercial payers), distributional divergence measures (Kullback-Leibler divergence, chi-squared tests), and the approach used here — price-weighted indexing with Z-score-based flagging. These methods are scalable but cannot distinguish case-mix variation from coding behavior without additional data.

**Risk-adjustment approaches** incorporate diagnosis codes or other patient-level covariates to control for case mix. Geruso and Layton's (2020) work on Medicare Advantage upcoding used risk scores computed from diagnosis codes. Bauder, Khoshgoftaar, and Seliya (2017) surveyed data mining techniques for upcoding detection, noting that supervised learning approaches require audit-labeled training data that is difficult to obtain, pushing the field toward unsupervised and semi-supervised methods.

This analysis occupies the second category — claims-based distributional analysis — while incorporating elements of the third through practice-profile covariates. We make explicit the limitations of this positioning throughout and detail paths toward full risk adjustment in Sections 4.6 and 7.3.

### 1.6 Scale of E&M in Medicaid

E&M office visits represent a substantial fraction of Medicaid professional service utilization:

- **1.53 billion claims** over the 2018–2024 dataset period
- **$78.5 billion** in total payments
- **219,664** unique billing NPIs across all states and territories
- **93.6%** of claims are established-patient visits; **6.4%** are new-patient visits

The 94:6 established-to-new ratio reflects Medicaid's role as a coverage program for ongoing chronic disease management (diabetes, asthma, behavioral health) rather than episodic acute care. This has methodological implications: the established-patient code family is the primary analytical focus by volume, while the new-patient family, though smaller, provides an independent signal that strengthens confidence when a provider is flagged in both.

---

## 2. Methodology

### 2.1 Data Sources and Linkages

**Source data:**
- **CMS Medicaid Provider Utilization and Payment Data (2018–2024)**: 227 million total records aggregated at the provider × HCPCS code × month level, filtered to the 9 E&M office visit codes (99202–99205, 99211–99215) with positive payments. This publicly available dataset reports total claims, total unique beneficiaries, and total paid amounts for each billing NPI by procedure code and service month.
- **National Plan and Provider Enumeration System (NPPES)**: 9.4 million provider records mapping National Provider Identifiers (NPIs) to provider name, practice state, taxonomy code, and entity type (individual vs. organization).
- **National Uniform Claim Committee (NUCC) Health Care Provider Taxonomy**: 906 taxonomy codes mapping each provider's self-reported specialty to a three-level hierarchy: Grouping (e.g., "Allopathic & Osteopathic Physicians"), Classification (e.g., "Internal Medicine"), and Specialization (e.g., "Cardiovascular Disease").

**Inclusion criteria:**
- E&M codes 99202–99205 (new patient) and 99211–99215 (established patient)
- Positive total paid amount (excluding zero-pay administrative claims)
- Valid NUCC taxonomy match (providers with missing or unrecognized taxonomy codes excluded)
- Minimum **500 claims** per provider per era per code family (ensures stable distributional estimates; providers with fewer claims have high sampling variance in their code mix)
- Minimum **20 peer providers** per benchmark specialty (ensures meaningful reference distribution; specialties with fewer peers are excluded from analysis)

### 2.2 Price-Weighted Index (PWI)

A central methodological choice in any E&M coding analysis is how to reduce a provider's code-level distribution to a single intensity metric. Prior approaches have used the percentage of high-level codes (e.g., fraction of claims at Level 4 or 5), ordinal weights (coding Level 1 through 5 as integers 1 through 5), or distributional divergence measures. Each has limitations: percentage-of-high-level ignores the full distribution; ordinal weights impose an assumption that the clinical complexity gap between adjacent levels is uniform; divergence measures are sensitive to reference distribution choice and do not produce an interpretable dollar metric.

We adopt a **price-weighted index** that uses national average reimbursement as the natural weight for each code:

$$\text{PWI}_i = \frac{\sum_{c \in \text{codes}} n_{ic} \times \bar{p}_c}{\sum_{c \in \text{codes}} n_{ic}}$$

where $n_{ic}$ is the number of claims provider $i$ billed for code $c$, and $\bar{p}_c$ is the national average price per claim for code $c$ within the relevant era.

The PWI has a direct economic interpretation: it is the average reimbursement per encounter that a provider would generate if all providers were paid at national average rates. This eliminates geographic and contract-level price variation — a Medicaid provider in New York and one in Mississippi billing the identical code mix will have the identical PWI, even though their actual reimbursements differ substantially. This isolation of code-mix deviation from payment-rate variation is critical for national benchmarking.

**National average prices by era:**

| Code | Pre-2021 | Post-2021 |
|------|----------|-----------|
| 99202 | $57.28 | $68.67 |
| 99203 | $68.15 | $71.98 |
| 99204 | $90.11 | $96.72 |
| 99205 | $129.20 | $146.55 |
| 99211 | $44.63 | $42.26 |
| 99212 | $45.45 | $55.08 |
| 99213 | $44.46 | $50.86 |
| 99214 | $58.09 | $66.55 |
| 99215 | $84.56 | $105.43 |

The price shifts between eras — particularly the 24.7% increase in 99215 reimbursement and the narrowing of the 99213–99214 gap — reflect both the 2021 code redefinition and general payment schedule updates. Because PWI is computed within each era using that era's prices, these shifts do not affect cross-era comparability of relative provider rankings.

### 2.3 Sub-Specialty Benchmarking

The validity of any outlier detection methodology depends on the appropriateness of the reference population. A sports medicine orthopedist who bills predominantly Level 4 and 5 visits because sports medicine patients present with complex musculoskeletal complaints should not be compared against the full orthopedic surgery peer group, which includes providers performing high volumes of routine follow-up visits. We benchmark each provider against the most granular specialty designation available:

- **Specialization** (e.g., "Sports Medicine," "Pediatric Cardiology"): available for approximately 73% of taxonomy codes in the NUCC hierarchy
- **Classification** fallback (e.g., "Internal Medicine," "Pediatrics"): used for the approximately 27% of providers where the Specialization field is null

This approach ensures that a pediatric cardiologist is compared against other pediatric cardiologists — not all pediatricians and not all cardiologists — since different sub-specialties serve fundamentally different patient populations with legitimately different coding baselines.

### 2.4 Separate Analysis by Code Family

New-patient (99202–99205) and established-patient (99211–99215) codes are analyzed as **independent code families**. This separation serves two purposes:

1. **Prevents conflation of patient-mix with code-mix.** A provider with an unusually high share of new patients (e.g., a referral center or a provider in a transient population area) would otherwise appear to code "higher" simply because new-patient codes reimburse at higher rates — even if their within-family code distribution is unremarkable.

2. **Provides an independent signal for cross-validation.** A provider flagged as an outlier in *both* new-patient and established-patient families is a qualitatively stronger signal than one flagged in a single family, as it suggests a pervasive coding pattern rather than a family-specific artifact.

The practical consequence is that some specialties may not meet the 20-peer threshold for new-patient analysis (given the 6.4% new-patient share, specialties with fewer than ~300 total qualifying providers may have fewer than 20 with 500+ new-patient claims) and are excluded from that family.

### 2.5 Outlier Detection

For each provider, within each era and code family:

1. **Z-score**: $(PWI_i - \text{median}_s) / \text{std}_s$, where $s$ is the provider's benchmark specialty. If $\text{std}_s = 0$ (all providers in the specialty have identical PWI), the Z-score is set to 0. The median is used rather than the mean as the central tendency measure because the mean is sensitive to the very tail behavior we seek to detect — using the mean as benchmark would mechanically reduce the Z-scores of the most extreme outliers.

2. **Absolute deviation**: $PWI_i - \text{median}_s$, in dollars per encounter.

3. **Outlier flag**: A provider is flagged if **both**:
   - Z-score $\geq$ 2.5 (statistically extreme relative to specialty peers)
   - Absolute deviation $\geq$ $5.00/encounter (economic significance floor)

The $5/encounter threshold was designed as a safety valve to prevent flagging providers in very tight specialties where Z = 2.5 might correspond to trivially small dollar differences. **In practice, this threshold is entirely non-binding.** Sensitivity analysis reveals that among providers with Z $\geq$ 2.5, the 10th percentile of absolute deviation is $10.54 (pre-2021) and $16.21 (post-2021). Identical outlier sets are produced at thresholds of $0, $1, $2, $3, $5, and $8. The threshold begins to prune outliers only above approximately $10–15/encounter. This means the system is **effectively single-threshold** — Z $\geq$ 2.5 alone determines the outlier set. The absolute deviation floor is retained as a structural safeguard against edge cases in future data but plays no role in the current results.

### 2.6 Excess Revenue Estimation

Estimated excess revenue quantifies the dollar impact of a provider's code-mix deviation:

$$\text{Excess}_i = (PWI_i - \text{median}_s) \times N_i$$

where $N_i$ is the provider's total claims. This is reported in two forms:

- **Unclipped** (can be negative): Used in aggregate tables. A provider who codes *below* their specialty median has negative excess, representing lower-than-expected revenue.
- **Clipped** (floored at $0): Used for provider-level ranking. Negative excess is zeroed out since we focus on potential upcoding.

**Conservation note and interpretive caution:** Because the benchmark is the specialty **median** (not mean), unclipped excess does not sum to zero across providers within a specialty. In right-skewed distributions — which E&M coding distributions universally are, with a small number of high-intensity coders pulling the right tail — the mean exceeds the median, producing systematically positive net excess across the population. The overall median deviation is $0.00 by construction; the mean deviation ranges from $0.77 to $2.33 per encounter depending on era and code family. Total net (unclipped) excess across all providers in the post-2021 established-patient analysis is $290 million — representing the aggregate asymmetry of the coding distribution, not "excess billing" at the population level. The median was chosen over the mean precisely because it is robust to the tail behavior we are trying to detect.

**This has a critical interpretive consequence.** The "estimated excess revenue" figures reported in this analysis ($131M pre-2021, $252M post-2021) are **not estimates of money lost to upcoding**. They are a mechanical consequence of summing right-tail deviations from a median benchmark in a right-skewed distribution. Even in a hypothetical world with zero intentional upcoding — where all coding variation reflects legitimate patient complexity — this sum would be large and positive. The dollar figures are useful for *ordering* providers by magnitude of deviation but should not be interpreted as "Medicaid overpaid by $X." This distinction is fundamental to responsible use of the analysis.

### 2.7 Beneficiary-to-Claim Ratio

For each provider, we report:

$$\text{BCR}_i = \frac{\sum \text{TOTAL\_UNIQUE\_BENEFICIARIES}_i}{\sum \text{TOTAL\_CLAIMS}_i}$$

A low BCR (e.g., < 0.3) indicates a provider seeing the same patients repeatedly — consistent with chronic disease management (diabetes, heart failure, serious mental illness), where higher-complexity codes may be clinically justified by the ongoing management of multiple interacting conditions. A high BCR (approaching 1.0) indicates mostly unique encounters, consistent with referral-based or episodic care.

**Important caveat for referral centers:** A high BCR at an academic medical center or specialty referral center does not rule out legitimate complexity. A patient referred to a tertiary dermatology clinic for a diagnostic workup is both a unique encounter *and* a genuinely complex visit. Similarly, a patient referred to a subspecialist for a second opinion presents a novel, high-acuity clinical question despite being a one-time encounter. The BCR is informative for distinguishing chronic-care from episodic-care practices but does not disambiguate complexity from upcoding in referral-heavy settings.

---

## 3. Results

### 3.1 Provider-Level Summary

| Metric | Pre-2021 | Post-2021 |
|--------|----------|-----------|
| Providers meeting inclusion criteria | 78,324 | 97,990 |
| Outliers flagged | 1,941 (2.5%) | 2,390 (2.4%) |
| Estimated excess revenue (clipped) | $130,714,128 | $251,949,791 |
| Median outlier Z-score | 3.40 | 3.57 |
| Outliers from established-patient codes | 1,655 (85%) | 1,988 (83%) |
| Outliers from new-patient codes | 286 (15%) | 402 (17%) |

The stable outlier rate across eras (~2.5%) provides an important internal validation. If the methodology were conflating the 2021 structural code migration with provider-level behavior, we would expect a sharp increase in the post-2021 outlier rate as the national shift toward higher codes produced a broader right tail. Instead, the independent era-specific benchmarking absorbs the structural shift, and the outlier rate — the fraction of providers whose deviation is extreme *relative to their peers within the same era* — remains essentially constant. This suggests the methodology is measuring a persistent behavioral phenomenon rather than an artifact of the code redefinition.

The increase in qualifying providers from 78,324 to 97,990 across eras reflects both general growth in Medicaid enrollment (accelerated by the Affordable Care Act's continuous enrollment provisions during the COVID-19 public health emergency) and the tendency for the simplified post-2021 coding framework to produce a broader distribution of billing patterns.

### 3.2 Z-Score Distribution

The Z-score distribution confirms appropriate calibration against standard statistical expectations:

| Threshold | Pre-2021 | Post-2021 |
|-----------|----------|-----------|
| Z $\geq$ 2.0 | 3,550 (4.5%) | 3,530 (3.6%) |
| Z $\geq$ 2.5 | 1,942 (2.5%) | 2,390 (2.4%) |
| Z $\geq$ 3.0 | 1,299 (1.7%) | 1,704 (1.7%) |
| Z $\geq$ 4.0 | 603 (0.8%) | 872 (0.9%) |
| Z $\geq$ 5.0 | 255 (0.3%) | 413 (0.4%) |

The fractions are broadly consistent with what would be expected from a heavy-tailed distribution benchmarked against the specialty median. For a normal distribution, 0.62% of observations would exceed Z = 2.5; the observed rate of 2.4–2.5% indicates right-skewness (more providers in the right tail than Gaussian assumptions predict), which is expected given the bounded-left, unbounded-right nature of coding distributions.

The mean Z-score across all providers is approximately 0.1–0.2 (not exactly 0, because the benchmark is the specialty median and the distribution is right-skewed). The median Z-score is 0.00 by construction.

### 3.3 Specialty Analysis

Specialty tables are sorted by **outlier rate** (percentage of qualifying providers flagged) rather than aggregate dollar totals. This design choice reflects a substantive concern: aggregate dollar totals primarily measure Medicaid enrollment composition. Pediatrics dominates the dollar rankings not because pediatricians upcode more than other specialists, but because Medicaid disproportionately covers children — approximately 40% of all children in the United States are covered by Medicaid or CHIP. Sorting by outlier rate controls for this compositional effect and identifies specialties where *within-specialty deviation* is most pronounced.

**Top 15 specialties by outlier rate, post-2021:**

| Specialty | Providers | Outliers | Outlier Rate | Avg Excess/Provider |
|-----------|-----------|----------|--------------|---------------------|
| Developmental Disabilities | 29 | 3 | 10.3% | $15,977 |
| Oral & Maxillofacial Surgery | 20 | 2 | 10.0% | $15,670 |
| Adv Heart Failure/Transplant Cardiology | 21 | 2 | 9.5% | $5,466 |
| Midwife | 54 | 5 | 9.3% | $9,789 |
| Pediatric Infectious Diseases | 23 | 2 | 8.7% | -$1,212 |
| Speech-Language Pathologist | 36 | 3 | 8.3% | $50,885 |
| Acupuncturist | 62 | 5 | 8.1% | $6,060 |
| Hepatology | 26 | 2 | 7.7% | $12,597 |
| Rehabilitation | 40 | 3 | 7.5% | $138,115 |
| Urogynecology | 28 | 2 | 7.1% | $1,366 |
| Hematology | 56 | 4 | 7.1% | $15,641 |
| Point of Service | 30 | 2 | 6.7% | $10,809 |
| Pediatric Cardiology | 204 | 13 | 6.4% | -$8,124 |
| Pediatric Pulmonology | 80 | 5 | 6.3% | $9,924 |
| Primary Podiatric Medicine | 116 | 7 | 6.0% | $3,595 |

**Interpretation:** The highest outlier rates appear in small, niche specialties where the peer pool is small and variance is high — a well-known property of Z-score-based detection in small samples. Some specialties (Pediatric Infectious Diseases, Pediatric Cardiology) show negative average excess despite having outliers, meaning the outliers are offset by a larger mass of providers coding below the median. The large-volume specialties with meaningful outlier rates include Rehabilitation ($138K average excess per provider), Speech-Language Pathology ($51K), and Hematology ($16K).

**Pre-2021 pattern** is broadly similar, with Facial Plastic Surgery (11.1%), Public Health (10.3%), and Pediatric Orthopaedic Surgery (10.0%) showing the highest rates.

### 3.4 Entity Type Analysis

A critical finding for interpreting this analysis concerns the relationship between provider entity type and outlier status:

**Post-2021:**

| Entity Type | Share of All Providers | Share of Outliers | Avg Clipped Excess |
|-------------|----------------------|-------------------|--------------------|
| Individual | 27.1% (26,574) | 28.0% (669) | $60,277 |
| Organization | 72.9% (71,416) | 72.0% (1,721) | $122,966 |

Organizations and individuals are flagged at nearly identical rates (the ratio is 0.99x for organizations, 1.03x for individuals). However, organizations dominate the **top of the excess revenue ranking** due to a pure volume effect: a large health system with 200,000 E&M claims and a modest per-encounter deviation generates more aggregate excess than an individual practitioner with 15,000 claims and a much higher Z-score. Consequently, 29 of the top 30 and 47 of the top 50 outliers by clipped excess are organizations.

This volume-driven ranking effect has a substantive implication that is essential for responsible interpretation. The top of the dollar-ranked list is systematically populated by academic medical centers, children's hospitals, and multi-specialty health systems — exactly the entities expected to treat the most complex patient populations. A children's hospital that bills predominantly Level 4 and 5 visits may be doing so because its patients are, on average, sicker and more diagnostically complex than those seen by a typical community pediatric practice. Without case-mix data, this analysis cannot distinguish that legitimate complexity from billing aggression. The individual-provider outlier list, where case-mix confounding is less systematic, is likely more informative for fraud screening purposes.

### 3.5 Cross-Era Outliers

**678 providers** were flagged as outliers in both the pre-2021 and post-2021 eras. These represent the highest-confidence signals in the analysis: their coding deviation persisted across the fundamental restructuring of E&M documentation requirements. A provider whose high coding was an artifact of the pre-2021 three-pillar documentation system — for example, one who excelled at documenting detailed histories and exams to justify higher codes — would be expected to revert toward the specialty median under the simplified post-2021 framework. Persistence across the structural break suggests a behavioral pattern independent of documentation mechanics.

Of these, **189 are individuals** and **489 are organizations**.

**Top 10 organizational cross-era outliers (by combined excess):**

| Provider | State | Specialty | Pre-2021 Excess | Post-2021 Excess | Combined |
|----------|-------|-----------|-----------------|------------------|----------|
| Valley Children's Medical Group | CA | Pediatrics | $1.51M | $3.78M | $5.29M |
| University Pediatricians | MI | Pediatrics | $1.11M | $3.66M | $4.76M |
| University of California San Francisco | CA | Dermatology | $1.89M | $2.59M | $4.48M |
| The Brooklyn Hospital Center | NY | Clinic/Center | $1.18M | $3.21M | $4.39M |
| UCSF Medical Group | CA | Dermatology | $1.39M | $2.72M | $4.11M |
| LPCH Medical Group (Stanford/Lucile Packard) | CA | Pediatrics | $0.97M | $2.61M | $3.58M |
| Ahura Healthcare Corporation | CA | Pain | $1.75M | $1.38M | $3.14M |
| Arkansas Department of Health | AR | Public Health | $1.26M | $1.77M | $3.03M |
| Children's Specialized Hospital | NJ | Pediatrics | $1.11M | $1.81M | $2.92M |
| Regional Cancer Care Associates | NJ | Clinical Lab | $0.88M | $1.88M | $2.76M |

The organizational list is dominated by tertiary referral centers whose patient populations are inherently more complex — a finding that recurs throughout this analysis and motivates the practice-profile adjustment in Section 5.

**Top 10 individual cross-era outliers (by combined excess):**

| Provider | State | Specialty | Pre-2021 Z | Post-2021 Z | Combined Excess |
|----------|-------|-----------|------------|-------------|-----------------|
| Mahmood Mostoufi | CA | Pediatrics | 5.71 | 8.03 | $1,046,200 |
| Mohammad Abid | CA | Cardiovascular Disease | 3.59 | 4.22 | $944,556 |
| Eric Ritter | MD | Hospitalist | 4.93 | 5.19 | $904,294 |
| Shawn Hamilton | CA | Internal Medicine | 5.68 | 5.16 | $873,386 |
| Eleuterio Go | CA | Specialist | 4.79 | 4.79 | $802,855 |
| Edwin Chapman | DC | Internal Medicine | 3.61 | 3.62 | $736,523 |
| Sasan Yadegar | CA | Neurological Surgery | 4.22 | 3.32 | $645,093 |
| Hitesh Patel | CA | Family Medicine | 2.61 | 2.57 | $597,388 |
| Nirmala Panwar | CT | General Practice | 6.08 | 6.02 | $521,734 |
| Sunil Arora | CA | Specialist | 3.37 | 2.97 | $445,557 |

Individual outliers tend to have **much higher Z-scores** than organizational outliers (Mostoufi at Z=8.0, Panwar at Z=6.0) despite lower aggregate dollar amounts. These represent more extreme statistical deviations within their specialty peer groups. California accounts for 7 of the top 10 individual cross-era outliers — a geographic concentration discussed further in Section 7.3.

**Notable individual patterns:**
- **Mahmood Mostoufi** (CA, Pediatrics): Z-score of 8.0 in post-2021 makes him the most statistically extreme individual cross-era outlier. His Z-score *increased* from 5.71 to 8.03 across eras, suggesting the 2021 code redefinition did not explain his coding pattern — and may have made it more visible as peers shifted their coding distributions.
- **Edwin Chapman** (DC, Internal Medicine): Remarkably stable Z-scores (3.61 → 3.62) across eras, suggesting a fixed coding behavior independent of the documentation framework.
- **Nirmala Panwar** (CT, General Practice): Z > 6.0 in both eras with a relatively low claim volume, indicating extreme per-encounter deviation.

### 3.6 Tiered Confidence Ranking: Cross-Family × Cross-Era Stratification

The 678 cross-era outliers represent providers flagged in at least one code family in each era. But the signal strength varies considerably depending on *how many* of the four possible family×era combinations a provider is flagged in. A provider flagged as an outlier in both new-patient and established-patient codes across both eras represents a qualitatively different signal than one flagged in established-patient codes only — the former suggests a pervasive coding pattern spanning all encounter types and surviving a fundamental regulatory change.

**Tier distribution:**

| Tier | Criteria | Providers | Description |
|------|----------|-----------|-------------|
| **Tier 1** | 4/4 flags | **29** | Outlier in new + established, pre + post-2021 |
| **Tier 2** | 3/4 flags | **67** | Outlier in 3 of 4 family×era combinations |
| **Tier 3** | 2/4 flags | **582** | Outlier in 1 family per era (minimum for cross-era) |

The dominant pattern in Tier 3 is pre-2021 established + post-2021 established (522 of 582), which is expected given that established-patient codes constitute 94% of E&M volume and many providers don't meet the 500-claim threshold for new-patient analysis.

**Tier 1 providers (29, highest confidence):**

These 29 providers are statistical outliers in every possible combination — both code families, both eras. Of these, 8 are individuals and 21 are organizations. A sample:

| Provider | State | Specialty |
|----------|-------|-----------|
| Arkansas Department of Health | AR | Public Health or Welfare |
| Arizona Arthritis & Rheumatology | AZ | Orthopaedic Surgery |
| Baton Rouge General Medical Center | LA | Dermatology |
| Regents of the University of Michigan | MI | Internal Medicine |
| South Bay Foot & Ankle Specialists | CA | Podiatrist |
| Nancy Chase | TN | Pediatric Cardiology |
| Zhanna Rapoport MD | CA | Neurology |
| Gunjan Bhatnagar | CA | Obstetrics & Gynecology |
| Adel Olshansky | CA | Neurology |
| Afsana Qader, DPM P.C. | NY | Foot Surgery |
| *(+ 19 others)* | | |

**Interpretation:** Tier 1 is where the screening signal is strongest. These providers' coding deviation is not explained by the 2021 structural break (persists across eras), not specific to one patient type (persists across new and established), and meets both the Z-score and absolute-deviation thresholds in all four combinations. Tier 1 should be the primary focus for any chart-level review.

**Tier 2 providers (67)** include University of Utah Adult Services (Cardiovascular Disease), Florida Neurology PA (Specialist), and several urgent care organizations — a more heterogeneous mix that includes both plausible complexity-driven outliers and entities in specialties with less inherent case-mix confounding.

### 3.7 Geographic Distribution

**Top 10 states by net excess revenue (post-2021):**

| State | Providers | Outliers | Net Excess Revenue | Avg Excess/Provider |
|-------|-----------|----------|-------------------|---------------------|
| CA | 10,489 | 574 | $97.0M | $9,251 |
| AZ | 1,920 | 96 | $49.1M | $25,573 |
| LA | 2,230 | 46 | $47.2M | $21,158 |
| NC | 3,114 | 71 | $40.3M | $12,946 |
| OH | 2,984 | 59 | $39.0M | $13,077 |
| MA | 2,355 | 26 | $36.2M | $15,351 |
| MD | 2,680 | 98 | $35.2M | $13,136 |
| MN | 980 | 28 | $34.9M | $35,562 |
| WA | 1,148 | 40 | $34.6M | $30,136 |
| CO | 1,738 | 77 | $34.3M | $19,757 |

California leads in absolute terms as the most populous state with the largest Medicaid program (Medi-Cal covers over 15 million enrollees). More notable are the per-provider excess figures: **Minnesota** ($35,562/provider) and **Washington** ($30,136/provider) show the highest per-provider excess, warranting state-specific investigation. Arizona's $25,573/provider is also elevated, consistent with patterns identified in our prior investigation of fast-starter behavioral health clinics in that state.

### 3.8 Provider Type Analysis

Sorted by outlier rate among types with more than 50 qualifying providers (post-2021):

| Provider Type | Providers | Outliers | Outlier Rate |
|---------------|-----------|----------|--------------|
| Podiatric Medicine & Surgery | 2,803 | 141 | 5.0% |
| Residential Treatment Facilities | 101 | 5 | 5.0% |
| Chiropractic Providers | 412 | 16 | 3.9% |
| Other Service Providers | 3,891 | 132 | 3.4% |
| Speech/Language/Hearing Services | 134 | 4 | 3.0% |

The elevated outlier rates among podiatric and chiropractic providers are consistent with broader literature on billing anomalies in these provider types. OIG and state MFCUs have historically identified these categories as higher-risk for E&M coding irregularities, and the finding here — that these provider types show outlier rates approximately double the population average — provides quantitative support for that prior pattern recognition.

### 3.9 Multi-NPI Entity Investigation: UCSF Case Study

The question of whether the methodology produces system-wide false positives for large academic centers can be partially addressed by examining multi-NPI health systems where different clinical departments bill under separate NPIs. UCSF provides an instructive natural experiment, appearing under **6 distinct NPIs** in the post-2021 data, each mapped to a different benchmark specialty:

| NPI | Entity Name | Specialty | Code Family | Claims | Z-Score | Excess Revenue |
|-----|-------------|-----------|-------------|--------|---------|----------------|
| 1477624104 | UCSF Medical Group | Dermatology | est | 125,110 | 3.68 | $2,720,400 |
| 1164512851 | Univ of California SF | Dermatology | est | 100,378 | 4.12 | $2,445,400 |
| 1376614016 | UCSF Pediatrics Assoc | Gastroenterology | est | 28,068 | 3.38 | $784,794 |
| 1922124866 | UCSF Health Medical Fdn | Multi-Specialty | est | 34,281 | -0.40 | -$101,479 |
| 1124439807 | Regents of UC (UCSF) | Cardiovascular Disease | est | 24,126 | -0.65 | -$135,749 |
| 1780727792 | UCSF Health Community Hosp | Clinic/Center | est | 4,420 | -0.47 | -$17,574 |

**This result constitutes a methodological validation finding.** The outlier signal is **not system-wide**. UCSF's Dermatology NPIs (Z = 3.7–4.1) are flagged, but Cardiovascular Disease (Z = -0.65), Multi-Specialty (Z = -0.40), and Community Hospital (Z = -0.47) all code *below* their respective specialty medians. If the methodology were simply a proxy for "large academic institution," all UCSF NPIs would be flagged. Instead, it detects a department-specific pattern in Dermatology while recognizing that other UCSF departments code at or below their peer benchmarks.

This finding provides evidence that the sub-specialty benchmarking framework is discriminating on the intended dimension (coding behavior relative to specialty peers) rather than on a confounded dimension (institutional prestige or patient volume). A UCSF cardiologist is benchmarked against other cardiovascular disease providers (and codes slightly below median), while a UCSF dermatologist is benchmarked against other dermatologists (and codes significantly above).

The two flagged Dermatology NPIs likely represent the faculty practice plan (UCSF Medical Group) and the university hospital outpatient department (Regents of UC), which are distinct billing entities under CMS rules but effectively one clinical practice. Their combined $5.2M in estimated excess reflects one practice's coding pattern counted twice — an illustration of the multi-NPI problem discussed in Section 4.4.

---

## 4. Limitations and Interpretive Caveats

### 4.1 No Case-Mix Adjustment (Primary Limitation)

This is the single most important limitation of the analysis, and the one most likely to produce actionable misinterpretation if ignored. The Medicaid utilization dataset contains no diagnosis codes, patient acuity scores, Hierarchical Condition Category (HCC) risk scores, or chart-level documentation. Without case-mix adjustment, we cannot distinguish between:

- **Legitimate high-complexity providers** who appropriately bill higher-level codes because their patients are sicker, more diagnostically uncertain, or require more complex medical decision-making (academic medical centers, tertiary referral centers, children's hospitals, safety-net providers in high-deprivation areas)
- **Upcoding providers** who bill higher-level codes than their patient complexity warrants, either through deliberate manipulation, inadequate coding education, or systematic misapplication of coding guidelines

The empirical evidence suggests this limitation is not theoretical. The top of our excess revenue ranking is populated almost entirely by entities whose names signal high-complexity patient populations: Valley Children's Medical Group, University Pediatricians, UCSF, Stanford/Lucile Packard, Children's Specialized Hospital. A methodology that systematically ranks tertiary referral centers as top outliers is more likely measuring patient complexity than billing fraud — at least at the top of the distribution.

**Recommendation:** For organizational outliers, this analysis should be used only as a first-pass screen. Any further investigation should incorporate diagnosis-code data (e.g., from T-MSIS or state-level claims) to adjust for case mix. Individual-provider outliers with very high Z-scores (>5.0) in non-referral specialties represent higher-confidence flags for review.

### 4.2 Beneficiary Counts Are Upper Bounds

The `TOTAL_UNIQUE_BENEFICIARIES` field in the source data represents uniqueness within each NPI × HCPCS code × month cell. Summing across months or codes double-counts patients who appear in multiple rows. The beneficiary-to-claim ratio is therefore an approximation, not an exact count. This limitation is inherent to the aggregation structure of the public-use file and cannot be resolved without access to beneficiary-level claims data.

### 4.3 Median-Based Benchmarks and Conservation

Because we benchmark against the specialty **median** rather than the **mean**, estimated excess revenue does not sum to zero across all providers within a specialty. In right-skewed distributions — which E&M coding distributions typically are, with a small number of high coders pulling the mean above the median — the mean exceeds the median, producing systematically positive net excess across the population.

The overall median deviation is $0.00 by construction; the mean deviation ranges from $0.77 to $2.33 per encounter depending on era and code family. Total net (unclipped) excess across all providers in the post-2021 established-patient analysis is $290 million. This figure represents the aggregate asymmetry of the coding distribution — a mathematical property of right-skewed data measured against its median — not "excess billing" at the population level.

The median was chosen over the mean precisely because it is robust to the tail behavior we are trying to detect. Using the mean as a benchmark would mechanically zero out the aggregate excess but would also make the benchmark itself sensitive to the outliers, creating a circularity in which adding a single extreme upcoder to a specialty would raise the benchmark for all peers.

### 4.4 NPI as Unit of Analysis

Large health systems bill under multiple NPIs, and a single clinical practice may be split across organizational NPIs (faculty practice vs. hospital outpatient department) or consolidated under one. This creates two problems:

- **Splitting**: A single practice's volume is divided across NPIs, each independently benchmarked. If the practice's coding pattern is genuinely extreme, both NPIs will be flagged — inflating the entity count and the aggregate excess (as demonstrated in the UCSF case study, Section 3.9).
- **Consolidation**: A health system billing under one NPI combines multiple departments' coding patterns, which may average out to a non-outlier profile even if individual departments have extreme patterns.

An NPI-level entity resolution layer (mapping related NPIs to a single organizational unit) would improve precision. CMS's Provider Enrollment, Chain, and Ownership System (PECOS) data could support this linkage but is beyond the scope of this analysis.

### 4.5 The $5/Encounter Threshold

As documented in Section 2.5, sensitivity analysis confirms the absolute deviation threshold is non-binding at all tested levels from $0 to $8. The outlier set is determined entirely by Z $\geq$ 2.5. The threshold is retained as a structural safeguard — in datasets with different distributional properties, a binding economic-significance floor may be appropriate — but it plays no role in the current results and should not be cited as a meaningful component of the detection methodology.

### 4.6 Paths Toward Case-Mix Adjustment

The absence of case-mix data is this analysis's primary limitation. Three paths exist to address it, in order of feasibility:

**Path 1 — Proxy covariates from existing data (immediate, no new data required):** The current dataset supports several crude complexity proxies that could be used as covariates in a multivariate model predicting PWI:
- *Code diversity*: The number of distinct HCPCS codes (beyond E&M) a provider bills. Providers billing across many procedure types likely treat more complex cases requiring multiple service modalities.
- *E&M-to-total-claims ratio*: What fraction of a provider's total Medicaid billing is E&M office visits. A provider whose billing is almost entirely E&M has a different practice profile than one who also bills surgeries, imaging, and procedures.
- *New-to-established ratio*: The ratio itself is informative — a high new-patient share suggests a referral practice.
- *Total claim volume*: Log-transformed total claims as a scale proxy and precision-inflation control.

These are not real risk adjustment. But regressing PWI on specialty dummies plus these proxies and flagging on *residuals* would at least partially absorb the referral-center effect. This approach is implemented in Section 5.

**Path 2 — Public provider-level data linkage (moderate effort, no data agreements):** Several freely available CMS datasets can be linked by NPI or provider ZIP code:
- *Medicare Provider Utilization and Payment Data*: Procedure-level detail for Medicare. A provider's Medicare coding pattern serves as cross-validation — if a provider codes high in both Medicaid and Medicare, the signal is strengthened; if only in Medicaid, payer-specific factors may be at play.
- *Hospital Compare / Provider of Services files*: For organizational NPIs — teaching status, bed count, case mix index.
- *Area Deprivation Index (ADI) or Social Vulnerability Index (SVI)*: Linked by provider ZIP code. Controls for socioeconomic profile of the catchment area, which correlates with patient complexity and chronic disease burden.

These provide enough covariates to separate "academic tertiary referral center in a high-deprivation area" from "suburban solo practice" — which is where most false-positive concern lives.

**Path 3 — T-MSIS Analytic Files (gold standard, significant access barriers):** CMS's Transformed Medicaid Statistical Information System (T-MSIS) Analytic Files contain diagnosis codes, patient demographics, and eligibility categories at the claim level. With diagnosis data, HCC risk scores or Elixhauser/Charlson comorbidity indices could be computed per provider's patient panel, enabling either risk-score-stratified benchmarks or regression-based residual flagging. Access requires a CMS Data Use Agreement (DUA), typically 3–6 months to negotiate, with associated fees, and is practical primarily for academic or government settings with existing CMS data relationships.

**Recommended sequence:** Path 1 first (nearly free given the existing pipeline), Path 2 as a second layer if results remain referral-center-dominated, Path 3 only if the analysis needs to be audit-grade or publishable in a peer-reviewed clinical journal.

---

## 5. Practice-Profile Adjustment (Path 1 Implementation)

### 5.1 Motivation and Design

The central concern motivating this section is that providers flagged as E&M outliers in Sections 3.1–3.6 may be legitimately complex practices whose high coding is a natural consequence of their service mix. An academic medical center that performs surgeries, advanced imaging, and complex procedures alongside E&M visits has a fundamentally different practice profile than a solo practitioner whose billing is 90% office visits. If this practice-profile variation is correlated with E&M coding intensity — and the results below confirm that it is — then a portion of the "outlier" signal is attributable to practice characteristics rather than coding behavior.

To address this, we fit an OLS regression predicting PWI from specialty dummies plus four practice-profile covariates derived from the full Medicaid dataset:

| Covariate | Rationale |
|-----------|-----------|
| **HCPCS Diversity** (count of distinct codes billed) | Broader code portfolio suggests more complex, multi-service practice |
| **E&M Ratio** (E&M claims / total claims) | Providers billing mostly E&M have different practice profiles than multi-service providers |
| **New-to-Established Ratio** (new-patient E&M / total E&M) | High ratio suggests referral practice |
| **Log Total Volume** (log of total claims) | Controls for precision inflation — very high-volume providers have mechanically tighter confidence intervals |

Residuals from this model represent the component of PWI deviation *not explained by* these practice characteristics. We then Z-score the residuals within specialty and flag on adjusted Z $\geq$ 2.5.

**Important caveat on potential signal absorption:** These covariates may absorb some real fraud signal alongside the complexity confound. A provider who upcodes E&M visits might also bill unnecessary ancillary procedures (increasing code diversity) or generate a practice profile that mimics complexity. For this reason, both raw and adjusted results are reported side by side — the adjustment is designed to *separate* populations (persistent vs. explained outliers), not to replace the raw analysis. A provider who is explained away is not exonerated; they are reclassified as lower-priority for investigation relative to those who persist.

### 5.2 Model Fit

| Era | R² | Interpretation |
|-----|-----|----------------|
| Pre-2021 | 0.211 | Covariates explain 21.1% of PWI variance |
| Post-2021 | 0.183 | Covariates explain 18.3% of PWI variance |

These R² values indicate that practice-profile covariates explain approximately one-fifth of the variation in coding intensity — a meaningful but not dominant fraction. The remaining four-fifths is attributable to specialty fixed effects (absorbed by the dummies), unobserved patient complexity, and provider-level coding behavior. The lower R² in the post-2021 era is consistent with the broader distribution of coding patterns under the simplified framework, which introduces additional variance not captured by these four covariates.

**Covariate effects (standardized betas, post-2021):**

| Covariate | Effect | Interpretation |
|-----------|--------|----------------|
| NEW_EST_RATIO | +3.88 | Strongest predictor — providers with more new patients code higher (mechanically expected: new-patient codes reimburse higher) |
| LOG_VOLUME | +2.80 | Higher-volume providers code higher (consistent with large referral centers) |
| EM_RATIO | +1.51 | E&M-heavy practices code higher (ambiguous — could reflect practice style, limited service scope, or genuine complexity) |
| HCPCS_DIVERSITY | +0.23 | Weakest effect — more codes billed correlates slightly with higher coding |

The strong positive effects of LOG_VOLUME and NEW_EST_RATIO are consistent with the referral-center hypothesis: large-volume providers with high new-patient ratios are referral destinations that see complex, diagnostically uncertain patients. The EM_RATIO effect is more ambiguous — providers with high E&M concentration may be complex primary care practices managing multiple chronic conditions, or they may be narrow-scope practices with few ancillary services.

### 5.3 Movement Analysis

The adjustment classifies each provider into one of four categories based on their raw and adjusted outlier status:

| Category | Definition | Est (Post-2021) | New (Post-2021) |
|----------|-----------|-----------------|-----------------|
| **PERSISTENT** | Outlier in both raw AND adjusted | 890 | 401 |
| **EXPLAINED** | Raw outlier, no longer outlier after adjustment | 1,098 | 1 |
| **UNMASKED** | Not raw outlier, becomes outlier after adjustment | 8 | 1,732 |
| **NORMAL** | Not an outlier in either | 93,860 | — |

**For established-patient codes**, the adjustment is highly discriminating: of 1,988 raw outliers, 890 (45%) persist and 1,098 (55%) are explained away by practice characteristics. The explained group has lower raw Z-scores (median 3.02 vs. 4.63 for persistent) and higher HCPCS diversity (median 9 vs. 6 codes), consistent with multi-service practices whose high coding reflects practice complexity rather than billing aggression.

**For new-patient codes**, a large UNMASKED set (1,732) appears. These are providers whose raw Z-scores were below 2.5 but whose adjusted residuals are extreme. This is a recognized artifact of the model specification: the NEW_EST_RATIO covariate has such a strong positive effect (+3.88) that providers with *low* new-patient ratios who still code high within the new-patient family generate large positive residuals after the model "expects" them to code lower. A more robust approach would fit separate models per code family (eliminating NEW_EST_RATIO from the new-patient model) or use interaction terms. The UNMASKED set should be treated with caution and is not included in the primary outlier counts.

### 5.4 What Happened to the Marquee Academic Centers

The most informative test of the practice-profile adjustment is whether it discriminates between the academic medical centers that dominate the raw outlier list and providers with narrow, E&M-concentrated practices:

| Provider | Raw Z (est) | Adj Z (est) | Movement | HCPCS Diversity | EM Ratio |
|----------|-------------|-------------|----------|-----------------|----------|
| Valley Children's Medical Group (CA) | 3.15 | 1.10 | **EXPLAINED** | 154 codes | 28.2% |
| University Pediatricians (MI) | 3.90 | 1.46 | **EXPLAINED** | 157 codes | 20.8% |
| UCSF Medical Group — Dermatology (CA) | 3.68 | 0.76 | **EXPLAINED** | 173 codes | 38.8% |
| Brooklyn Hospital Center (NY) | 2.71 | 1.00 | **EXPLAINED** | 418 codes | 10.5% |
| LPCH Medical Group / Stanford (CA) | 2.84 | 1.10 | **EXPLAINED** | 187 codes | 36.9% |
| **Ahura Healthcare Corporation (CA)** | **3.19** | **2.68** | **PERSISTENT** | 18 codes | 68.9% |

The academic medical centers — Valley Children's, University Pediatricians, UCSF, Brooklyn Hospital, Stanford — are **all explained away** by the practice-profile adjustment. They bill hundreds of distinct HCPCS codes and have low E&M ratios (10–39%), consistent with large multi-service institutions whose high E&M coding reflects a complex referral practice rather than billing aggression. This is exactly the case-mix confound the covariates were designed to absorb, and their successful absorption provides evidence that the model is performing as intended.

**Ahura Healthcare Corporation** (CA, Pain Medicine) is the notable exception. It **persists** after adjustment with only 18 distinct HCPCS codes and a 68.9% E&M ratio — a narrow, E&M-heavy practice profile that the model cannot explain away. Its coding deviation appears to reflect something beyond observable practice characteristics.

### 5.5 Persistent Outlier Profile (Established-Patient, Post-2021)

The 890 persistent established-patient outliers — those who remain extreme after adjustment — have a distinctive epidemiological profile compared to the 1,098 who were explained:

| Metric | Persistent (890) | Explained (1,098) |
|--------|-------------------|-------------------|
| Median raw Z-score | 4.63 | 3.02 |
| Median adjusted Z-score | 3.22 | 1.82 |
| Median HCPCS diversity | 6 codes | 9 codes |
| Median E&M ratio | 62.1% | 55.3% |

Persistent outliers are more extreme to begin with (higher raw Z), bill fewer distinct code types (narrower practice scope), and derive a larger share of revenue from E&M (more billing-concentrated). This profile is more consistent with coding-focused billing behavior — where E&M code selection is a primary revenue lever — than with complex multi-service referral practices where E&M is one component of a diversified service portfolio. The median HCPCS diversity of 6 codes for persistent outliers (compared to 9 for explained) suggests practices operating within a narrow clinical scope where E&M visits are the dominant billable service.

---

## 6. Cross-Investigation Convergence Analysis

### 6.1 Rationale and Methodology

A fundamental challenge in claims-based fraud detection is that any single methodology — no matter how sophisticated — will produce both false positives (legitimate providers flagged) and false negatives (fraudulent providers missed). The standard response in the program integrity literature is *triangulation*: using multiple independent detection methods and prioritizing providers who appear across several. The logic is Bayesian: each independent signal increases the posterior probability that a provider's behavior is genuinely anomalous rather than a methodological artifact.

We joined the 3,420 unique E&M outlier NPIs (from either era) against signals from five prior investigations conducted on the same Medicaid dataset, categorized by signal type to avoid double-counting correlated methods:

| Signal Type | Source | Description | E&M Outlier Overlap |
|-------------|--------|-------------|---------------------|
| **Billing Anomaly** | Specialty cost-ratio outliers (Inv 3) | Provider cost/bene exceeds 5x specialty median | 13 providers |
| **Billing Anomaly** | Temporal spending spikes (Inv 4) | Month-over-month spending ratio anomaly | 61 providers |
| **Fraud Infrastructure** | Ghost providers (Inv 5) | Billing volume exceeds physical human capacity | 0 providers |
| **Regulatory** | OIG exclusion list (LEIE) | Federally excluded from healthcare programs | 0 providers |
| **Temporal** | Disappearances (Inv 4) | Provider ceased billing after sustained activity | 47 providers |
| **Temporal** | Fast starters (Inv 4) | New entrant with immediate high billing | 0 providers |
| **Internal** | Cross-era E&M outlier | Flagged in both pre- and post-2021 eras | 678 providers |

The categorization by signal type (billing anomaly, fraud infrastructure, regulatory, temporal) rather than raw signal count prevents a provider with two correlated billing-anomaly flags from being ranked equivalently to one with a billing anomaly plus a temporal disappearance — the latter represents convergence across independent detection dimensions.

### 6.2 Convergence Results

Of 3,420 E&M outliers, **772 (22.6%)** have at least one additional signal beyond the E&M flag itself. The dominant additional signal is the cross-era flag (678 providers), which is derived from the E&M analysis itself and represents temporal persistence rather than an independent methodology.

**6 providers** have signals from 2+ independent signal types (beyond E&M itself):

| Provider | State | Specialty | E&M Z-Score | Signals |
|----------|-------|-----------|-------------|---------|
| **Eric Ritter** | MD | Hospitalist | 5.19 | Temporal spike + temporal disappearance + cross-era E&M |
| **Lillian Alt** | NJ | Internal Medicine | 4.50 | Temporal spike + temporal disappearance + cross-era E&M |
| MedStar Franklin Square | MD | General Practice | 2.79 | Temporal spike + temporal disappearance |
| Variety Children's Hospital | FL | General Acute Care | 2.96 | Specialty outlier + temporal disappearance |
| Katie Nicosia | OR | Family Medicine | 2.76 | Specialty outlier + temporal disappearance |
| John P Kelly MD | CA | Orthopaedic Surgery | 2.97 | Specialty outlier + temporal disappearance |

**Eric Ritter** (MD, Hospitalist, Z=5.19) is the highest-convergence individual provider in the entire analysis: flagged as an E&M outlier in both eras, with a temporal spending spike *and* a subsequent billing disappearance. The spike-then-disappearance pattern — a sharp increase in billing intensity followed by a cessation of claims — is a well-documented signature in the program integrity literature, consistent with billing anomalies that attract scrutiny or enforcement action. He also appears in the top 10 individual cross-era outliers from Section 3.5.

**Lillian Alt** (NJ, Internal Medicine, Z=4.50) shows the same spike-then-disappearance pattern with cross-era E&M outlier status, representing independent geographic and methodological convergence with Ritter.

### 6.3 Interpretation

The low overlap between E&M outliers and prior investigation flags (only 22.6% have any additional signal, and only 0.2% have 2+ independent signal types) requires careful interpretation:

1. **The investigations are detecting different fraud typologies.** Ghost providers and fast-starters operate through capacity fraud and entity proliferation — mechanisms entirely unrelated to E&M code selection. The prior investigations largely targeted T1019 personal care services and behavioral health billing, not office visits. The zero overlap with ghost providers and fast-starters is expected: a provider who exploits E&M code subjectivity to upcode office visits is employing a fundamentally different mechanism than one who bills for physically impossible service volumes. Low overlap is the natural consequence of non-overlapping fraud typologies.

2. **The E&M methodology likely has a meaningful false-positive rate.** Many of the 3,420 flagged providers may be legitimately complex, particularly the 55% of established-patient outliers whose signal was explained by practice-profile adjustment. The true-positive rate is likely higher among the 890 persistent outliers, the 29 Tier 1 cross-family/cross-era outliers, and the 6 multi-signal convergence providers.

**The convergence analysis is most useful as a *validation* of the highest-confidence flags**, not as a filtering mechanism. Applying convergence as a hard filter (requiring multiple signals to investigate) would miss the many genuine upcoding cases that happen to not employ other fraud mechanisms. Instead, convergence provides incremental confidence: Eric Ritter, Lillian Alt, and Ahura Healthcare Corporation appear across multiple independent methodologies and represent the strongest candidates for further investigation.

---

## 7. Conclusions and Recommendations

### 7.1 What This Analysis Can and Cannot Do

**Can do:**
- Identify providers whose E&M code-mix deviates statistically from their specialty peers using a price-weighted intensity metric with an interpretable dollar scale
- Quantify the dollar impact of that deviation using national uniform prices, eliminating geographic and contract-level confounds
- Separate the 2021 structural break in E&M coding guidelines from provider-level behavioral variation through independent era-specific analysis
- Partially distinguish practice complexity from coding behavior using observable practice-profile covariates (HCPCS diversity, E&M ratio, new-patient ratio, volume)
- Identify persistent outliers whose coding patterns span both coding eras and survive practice-profile adjustment
- Cross-reference E&M flags against independent fraud signals from prior investigations to validate the highest-confidence flags
- Discriminate within multi-NPI health systems, identifying department-specific coding patterns rather than system-wide artifacts

**Cannot do:**
- Determine whether any individual provider's coding is inappropriate without chart-level review
- Fully distinguish patient complexity from billing aggression in the absence of diagnosis data, HCC risk scores, or Elixhauser/Charlson comorbidity indices
- Account for legitimate variation in local patient populations (e.g., a provider near a methadone clinic treating a disproportionately complex panel)
- Resolve multi-NPI entities into their true organizational units, potentially double-counting some practices
- Distinguish between intentional upcoding (fraud), systematic miscoding (abuse), and accurate coding of a genuinely complex patient population (appropriate billing)

### 7.2 Prioritized Investigation Targets

Based on the layered analysis — raw Z-scores, practice-profile adjustment, cross-era persistence, cross-family stratification, and cross-investigation convergence — we recommend the following prioritization framework for any entity conducting chart-level review or audit referral:

**Tier A: Highest confidence (converging independent signals)**

These providers are flagged by multiple independent methodologies and represent the strongest candidates for targeted chart review:

| Provider | State | Specialty | Key Signals |
|----------|-------|-----------|-------------|
| Eric Ritter | MD | Hospitalist | E&M outlier (Z=5.2), cross-era, temporal spike, temporal disappearance |
| Lillian Alt | NJ | Internal Medicine | E&M outlier (Z=4.5), cross-era, temporal spike, temporal disappearance |
| Ahura Healthcare Corp | CA | Pain Medicine | E&M outlier (Z=3.2), cross-era, **persistent after adjustment** (18 HCPCS codes, 69% E&M ratio) |

**Tier B: Persistent after adjustment, cross-era, individual providers**

These 890 established-patient outliers survived practice-profile adjustment (their coding deviation is not explained by observable practice characteristics), with emphasis on individual practitioners with Z > 5.0:

- Mahmood Mostoufi (CA, Pediatrics, raw Z=8.0) — most statistically extreme individual, cross-era, Z *increased* post-2021
- Nirmala Panwar (CT, General Practice, raw Z=6.0) — stable Z across eras
- Kwasi Nenonene (OH, Family Medicine, adjusted Z=9.9) — highest adjusted Z among persistent outliers

**Tier C: Tier 1 cross-family/cross-era providers (29 total)**

Flagged in all four family×era combinations. Includes both individuals (e.g., Nancy Chase, TN Pediatric Cardiology) and organizations (e.g., Arkansas Department of Health, Baton Rouge General Medical Center).

**De-prioritize:** Valley Children's, UCSF, University Pediatricians, Stanford/LPCH, and Brooklyn Hospital Center. All were **explained away** by practice-profile adjustment — their high coding is attributable to their broad, multi-service practice profiles (150–400+ HCPCS codes, 10–39% E&M ratio) rather than coding behavior.

### 7.3 Remaining Limitations and Next Steps

1. **Path 2 — Area Deprivation Index linkage.** The provider ZIP codes in the NPI address parquet can be joined to the publicly available Area Deprivation Index (University of Wisconsin) or Social Vulnerability Index (CDC/ATSDR) to control for socioeconomic characteristics of each provider's catchment area. Safety-net providers serving high-deprivation communities treat populations with higher chronic disease burden, more social determinants of health complexity, and greater diagnostic uncertainty — all of which legitimately drive higher E&M coding. ADI linkage would help deconfound these providers from the outlier population.

2. **Path 3 — T-MSIS diagnosis data.** For the Tier A and Tier B providers specifically, diagnosis-code-level data from the T-MSIS Analytic Files would definitively resolve whether their coding intensity matches their patient acuity. This is the gold standard for case-mix adjustment but requires a CMS Data Use Agreement. Given the relatively small number of high-priority providers (3 Tier A, ~50 Tier B with Z > 5.0), a targeted chart review or state MFCU referral may be more practical than a full DUA.

3. **New-patient UNMASKED artifact.** The large UNMASKED set in the new-patient family (1,732 providers) is likely a model artifact from the strong NEW_EST_RATIO covariate effect. A more robust approach would fit separate regression models per code family (removing the NEW_EST_RATIO covariate from the new-patient model, where it is tautological) or use family×covariate interaction terms.

4. **California concentration.** Seven of the top 10 individual cross-era outliers are California-based. This geographic concentration warrants state-level investigation and may reflect one or more of: Medi-Cal's reimbursement structure (which differs significantly from other state Medicaid programs), provider density effects (competition or peer-network norms), audit history and compliance culture, or a genuine geographic cluster of outlier billing. A joint analysis with the California MFCU — which accounted for $513 million (53%) of all MFCU criminal recoveries in FY 2024 — would be particularly valuable.

5. **Medicare cross-validation.** For providers who bill both Medicaid and Medicare, comparing E&M coding patterns across payers would provide an additional validation dimension. A provider who codes high in Medicaid but at the specialty median in Medicare may be responding to payer-specific documentation or audit differences. A provider who codes high in both is a stronger signal. The Medicare Provider Utilization and Payment Data is publicly available and linkable by NPI.

---

## Appendix A: Output File Manifest

| File | Description |
|------|-------------|
| `em_upcoding_providers_pre2021.csv` | All qualifying providers, pre-2021, both code families |
| `em_upcoding_providers_post2021.csv` | All qualifying providers, post-2021, both code families |
| `em_upcoding_providers_new_pre2021.csv` | New-patient family only, pre-2021 |
| `em_upcoding_providers_est_pre2021.csv` | Established-patient family only, pre-2021 |
| `em_upcoding_providers_new_post2021.csv` | New-patient family only, post-2021 |
| `em_upcoding_providers_est_post2021.csv` | Established-patient family only, post-2021 |
| `em_upcoding_by_specialty_pre2021.csv` | Specialty aggregates, pre-2021 (sorted by outlier rate) |
| `em_upcoding_by_specialty_post2021.csv` | Specialty aggregates, post-2021 (sorted by outlier rate) |
| `em_upcoding_by_state_pre2021.csv` | State aggregates, pre-2021 |
| `em_upcoding_by_state_post2021.csv` | State aggregates, post-2021 |
| `em_upcoding_by_type_pre2021.csv` | Provider type aggregates, pre-2021 |
| `em_upcoding_by_type_post2021.csv` | Provider type aggregates, post-2021 |
| `em_upcoding_cross_era_summary.csv` | Providers flagged in both eras |
| `em_upcoding_adjusted_pre2021.csv` | Practice-profile-adjusted results, pre-2021 (raw + adjusted Z-scores, movement classification) |
| `em_upcoding_adjusted_post2021.csv` | Practice-profile-adjusted results, post-2021 (raw + adjusted Z-scores, movement classification) |
| `em_upcoding_adjustment_summary.csv` | Adjustment summary by era × code family (persistent/explained/unmasked counts) |
| `em_upcoding_convergence.csv` | All E&M outliers with cross-investigation signal columns |
| `em_upcoding_convergence_flagged.csv` | E&M outliers with 1+ additional signal from prior investigations |

## Appendix B: Provider CSV Column Definitions

| Column | Description |
|--------|-------------|
| `BILLING_PROVIDER_NPI_NUM` | National Provider Identifier |
| `PROVIDER_NAME` | Provider or organization name from NPPES |
| `STATE` | Practice location state |
| `BENCHMARK_SPECIALTY` | NUCC Specialization (or Classification fallback) used for benchmarking |
| `Classification` | NUCC Classification (broader category) |
| `PROVIDER_TYPE` | NUCC Grouping (e.g., "Allopathic & Osteopathic Physicians") |
| `CODE_FAMILY` | "new" (99202–99205) or "est" (99211–99215) |
| `ERA` | "pre2021" or "post2021" |
| `TOTAL_EM_CLAIMS` | Total E&M claims in this family and era |
| `TOTAL_PAID` | Total dollars paid (actual, not price-weighted) |
| `PRICE_WEIGHTED_INDEX` | Average national-price-equivalent per encounter |
| `MEDIAN_PWI` | Specialty median PWI |
| `P95_PWI` | Specialty 95th percentile PWI |
| `STD_PWI` | Specialty standard deviation of PWI |
| `Z_SCORE` | (PWI - median) / STD; 0 if STD = 0 |
| `ABS_DEVIATION` | PWI - median, in dollars per encounter |
| `EST_EXCESS_REVENUE` | Unclipped: ABS_DEVIATION × TOTAL_EM_CLAIMS |
| `EST_EXCESS_REVENUE_CLIPPED` | Floored at $0 |
| `BENE_CLAIM_RATIO` | Total unique beneficiaries / total claims (upper bound) |
| `PEER_COUNT` | Number of providers in the benchmark specialty |
| `IS_OUTLIER` | True if Z ≥ 2.5 AND ABS_DEVIATION ≥ $5.00 |
| `ABOVE_P95` | True if PWI exceeds the specialty 95th percentile |

### Adjusted File Additional Columns

| Column | Description |
|--------|-------------|
| `HCPCS_DIVERSITY` | Distinct HCPCS codes billed by this provider in the era |
| `EM_RATIO` | E&M claims / total claims |
| `NEW_EST_RATIO` | New-patient E&M claims / total E&M claims |
| `LOG_VOLUME` | log(total claims) — controls for precision inflation |
| `ADJ_RESIDUAL` | OLS residual after regressing PWI on specialty dummies + covariates |
| `ADJ_Z_SCORE` | Residual Z-score within specialty (residual - specialty median residual) / specialty residual STD |
| `ADJ_IS_OUTLIER` | True if ADJ_Z_SCORE ≥ 2.5 |
| `MOVEMENT` | PERSISTENT (both outlier), EXPLAINED (raw only), UNMASKED (adjusted only), NORMAL |

### Convergence File Additional Columns

| Column | Description |
|--------|-------------|
| `SIG_SPECIALTY_OUTLIER` | Flagged in Investigation 3 (specialty cost-ratio outlier, >5x median) |
| `SIG_TEMPORAL_SPIKE` | Flagged in Investigation 4 (month-over-month spending spike) |
| `SIG_GHOST_PROVIDER` | Flagged in Investigation 5 (billing volume exceeds physical capacity) |
| `SIG_OIG_EXCLUDED` | Appears on OIG/LEIE exclusion list |
| `SIG_TEMPORAL_DISAPPEARANCE` | Flagged in Investigation 4 (ceased billing after sustained activity) |
| `SIG_FAST_STARTER` | Flagged in Investigation 4 (new entrant with immediate high billing) |
| `SIG_CROSS_ERA` | E&M outlier in both pre- and post-2021 eras |
| `BILLING_ANOMALY_COUNT` | Count of billing anomaly signals (specialty outlier + temporal spike) |
| `FRAUD_INFRA_COUNT` | Count of fraud infrastructure signals (ghost provider) |
| `REGULATORY_COUNT` | Count of regulatory signals (OIG exclusion) |
| `TEMPORAL_COUNT` | Count of temporal signals (disappearance + fast starter) |
| `SIGNAL_TYPE_COUNT` | Distinct signal *types* with at least one signal (0–4) |
| `TOTAL_SIGNAL_COUNT` | Total individual signals across all types (0–7) |

---

## References

Bastani, H., Goh, J., & Bayati, M. (2019). Evidence of upcoding in pay-for-performance programs. *Management Science*, 65(3), 1042–1060.

Bauder, R. A., Khoshgoftaar, T. M., & Seliya, N. (2017). A survey on the state of healthcare upcoding fraud analysis and detection. *Health Services and Outcomes Research Methodology*, 17(1), 31–55.

Geruso, M., & Layton, T. J. (2020). Upcoding: Evidence from Medicare on squishy risk adjustment. *Journal of Political Economy*, 128(6), 2427–2465.

HHS Office of Inspector General. (2014). Improper payments for evaluation and management services cost Medicare billions in 2010. Report OEI-04-10-00181.

HHS Office of Inspector General. (2025). Medicaid Fraud Control Units fiscal year 2024 annual report.

Silverman, E., & Skinner, J. (2004). Medicare upcoding and hospital ownership. *Journal of Health Economics*, 23(2), 369–389.

U.S. Department of Justice. (2024). UCHealth agrees to pay $23 million to resolve allegations of fraudulent billing for emergency department evaluation and management services.
