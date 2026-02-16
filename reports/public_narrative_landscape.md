# Public Narrative Landscape: DOGE Medicaid Provider Spending Dataset

*Compiled February 15, 2026*

This document maps the public discourse surrounding the DOGE-released Medicaid Provider Spending dataset. It serves as a research reference for ensuring our investigations address the questions citizens, journalists, and analysts are actually asking — and that we provide the context needed to avoid contributing to misinformation.

---

## 1. Timeline of Events

| Date | Event |
|------|-------|
| **Mid-2010s** | CMS begins collecting T-MSIS (Transformed Medicaid Statistical Information System) data from states |
| **2024** | CMS reports Medicaid improper payment rate of 5.1% (covering 2022–2024) |
| **Jun 30, 2025** | DOJ announces largest-ever healthcare fraud takedown: 324 defendants, $14.6B in alleged schemes ([DOJ](https://www.justice.gov/opa/pr/national-health-care-fraud-takedown-results-324-defendants-charged-connection-over-146)) |
| **Dec 18, 2025** | U.S. Attorney Joe Thompson tells Minnesota legislature: fraud in 14 MN Medicaid programs "likely exceeds $9 billion" ([Minnesota Reformer](https://minnesotareformer.com/2025/12/18/u-s-attorney-fraud-likely-exceeds-9-billion-in-minnesota-run-medicaid-services/)) |
| **Jan 6, 2026** | CMS Administrator Dr. Mehmet Oz sends letter to Gov. Walz declaring Minnesota Medicaid in "substantial noncompliance"; CMS threatens to withhold >$2B in federal matching funds ([Georgetown CCF](https://ccf.georgetown.edu/2026/01/16/cms-weaponizes-fraud-against-medicaid-in-minnesota/)) |
| **Feb 13, 2026** | **DOGE/HHS releases Medicaid Provider Spending dataset** — 10.32 GB parquet, 227M records, covering Jan 2018–Dec 2024, available at opendata.hhs.gov |
| **Feb 13–14, 2026** | Elon Musk posts on X: "Medicaid data has been open sourced, so the level of fraud is easy to identify. @DOGE is not a department, it's a state of mind." |
| **Feb 14, 2026** | Citizen analysts begin publishing findings within hours of release (Rojas Report, TFTC, others) |

---

## 2. Citizen Analyst Findings

### The "Billion Dollar Club" (Dutch Rojas, *The Rojas Report*)

Source: [The Billion-Dollar Club](https://dutchrojas.substack.com/p/the-billion-dollar-club)

Key findings from analysis of T1019 (Personal Care Services) billing:
- 9,780 organizations nationwide bill Medicaid under T1019
- **Top 1% (98 entities) collected $30.4B — 42.5% of all T1019 spending nationally**
- **Nine entities crossed the billion-dollar threshold individually**
- 17 of the top 20 T1019 billers are in New York State
- 8 of the 9 billion-dollar T1019 billers are in New York or Massachusetts
- **7 billion-dollar entities are in Brooklyn**
- 3x cost-per-claim spread across top 20 billers for the same service code
- Evidence of entity proliferation: same names, same addresses, same authorized officials but different NPIs

### "$90 Billion in Fraud" Claim (TFTC)

Source: [$90 Billion in Medicaid Fraud from 0.16% of Providers](https://www.tftc.io/doge-medicaid-fraud-90-billion-open-source-data/)

- Claims that within hours of dataset release, one analyst identified $90B in "likely fraud" from just 0.16% of providers
- This figure became a widely-circulated talking point in conservative media

**Important caveat**: The $90B figure conflates high billing volume with fraud. High billers include fiscal intermediaries, state agencies, and managed care organizations that legitimately process enormous volumes. Our analysis confirms the top 100 providers by spending are overwhelmingly government agencies and fiscal intermediaries, not rogue practitioners.

### The "$800B Open Secret" (On Healthcare Tech)

Source: [The $800B Open Secret](https://www.onhealthcare.tech/p/the-800b-open-secret-what-the-new)

- Medicaid total program spend approximately $849B (federal + state) in 2023, serving ~90M enrollees
- Frames the dataset release as historically unprecedented: Medicaid data has been "one of the most fragmented, hardest-to-access, least standardized bodies of administrative information in all of American healthcare"
- Previously, accessing T-MSIS research data required CMS Privacy Board approval (process taking up to a year)
- Argues the release has significance beyond fraud detection — for health tech innovation and transparency

---

## 3. Known Fraud Cases

### Minnesota Autism/Behavioral Health Fraud ($9B+)

Sources:
- [Minnesota Reformer: U.S. Attorney says fraud likely exceeds $9 billion](https://minnesotareformer.com/2025/12/18/u-s-attorney-fraud-likely-exceeds-9-billion-in-minnesota-run-medicaid-services/)
- [Georgetown CCF: CMS Weaponizes Fraud Against Medicaid in Minnesota](https://ccf.georgetown.edu/2026/01/16/cms-weaponizes-fraud-against-medicaid-in-minnesota/)

- Assistant U.S. Attorney Joe Thompson stated that fraud in 14 "high risk" MN Medicaid programs could exceed $9 billion
- These 14 programs have cost Minnesota $18 billion since 2018
- Thompson estimated fraud at "half or more" of total spending in those programs
- Federal charges filed against multiple entities, including autism treatment centers
- **Disputed**: State Medicaid Director John Connolly counters that DHS can substantiate fraud totaling "tens of millions," not $9 billion. Governor Walz calls the estimate "sensationalized."

### DOJ National Healthcare Fraud Takedown (Jun 2025)

Source: [DOJ Press Release](https://www.justice.gov/opa/pr/national-health-care-fraud-takedown-results-324-defendants-charged-connection-over-146)

- 324 defendants charged (including 96 doctors, nurse practitioners, pharmacists)
- Cases span 50 federal districts and 12 State AG offices
- $14.6 billion in alleged *intended loss* (not actual paid — an important distinction)
- More than doubled prior record of $6B set in 2020
- 74 defendants charged for illegal diversion of 15M+ prescription pills
- 49 defendants charged for $1.17B in telehealth and genetic testing fraud
- DOJ announced creation of Health Care Fraud Data Fusion Center

### CMS Contract Management Failures

Source: [HHS OIG: CMS Put $11.2 Billion at Risk](https://oig.hhs.gov/reports/all/2025/cms-put-112-billion-at-risk-of-fraud-waste-and-abuse-by-not-properly-closing-contracts/)

- For 50 contracts totaling $11.2B eligible for closeout (Oct 2018–Sep 2023), CMS failed to meet administrative closeout requirements
- This represents internal contract management failure at CMS, not provider fraud

---

## 4. Political Context

### The Central Tension

Fraud claims are being used to justify proposed cuts to Medicaid of up to $1 trillion over 10 years. The key question is whether the scale of actual fraud justifies cuts of this magnitude, or whether "fraud" is being used as a politically palatable framing for ideologically motivated coverage reductions.

### "Improper Payment" ≠ Fraud

Sources:
- [Georgetown CCF: The Truth about Waste and Abuse in Medicaid](https://ccf.georgetown.edu/2025/01/27/the-truth-about-waste-and-abuse-in-medicaid/)
- [CBPP: Republicans' Claims of "Fraud" Are a Pretext](https://www.cbpp.org/blog/republicans-claims-of-fraud-are-a-pretext-for-unpopular-and-drastic-medicaid-cuts)

Critical distinctions:
- Medicaid's improper payment rate was **5.1% in 2024** (cut by two-thirds from prior years)
- This is not a fraud rate. Per Georgetown CCF: **77.17% of improper payments in FY 2025 were due to insufficient documentation**, not fraud or abuse
- Minnesota's payment accuracy was 97.8% proper in FY 2025; 98.7% of fee-for-service payments were proper
- The CMS action to withhold >$2B from Minnesota was described as the first time in Medicaid's 60-year history that CMS launched this "nuclear option"

### Fraud Is Real — But Committed by Providers, Not Beneficiaries

Georgetown CCF documents that fraud is predominantly committed by providers: ambulance services, DME suppliers, diagnostic labs, nursing homes, pain clinics, pharmacies, and physicians. The political narrative, however, often conflates provider fraud with beneficiary eligibility fraud, which is used to justify work requirements and coverage restrictions.

### Competing Narratives by Source Type

| Perspective | Sources | Core argument |
|-------------|---------|---------------|
| **Pro-transparency / fraud emphasis** | Axios, Benzinga, Townhall, TFTC, Rojas Report | Dataset reveals massive fraud; crowdsourcing will expose it |
| **Critical / contextual** | Georgetown CCF, CBPP, Snopes, KFF | Improper payments ≠ fraud; claims used as pretext for cuts |
| **Government oversight** | GAO, HHS OIG, DOJ | Legitimate oversight gaps exist; fraud is prosecuted; management weaknesses need fixing |
| **Data-focused** | On Healthcare Tech, KFF | Unprecedented data access; significant for transparency beyond fraud |

### The "8 Million" Claim

Source: [Snopes Fact Check](https://www.snopes.com/news/2025/05/24/medicaid-doge-rfk-jr/)

- HHS Secretary RFK Jr. claimed 8 million people were on Medicaid due to "fraud, waste and abuse"
- Snopes found no evidence this originated from DOGE's work
- CBO estimated ~7.6–7.7M people would lose Medicaid under the proposed budget bill — a policy outcome, not a fraud finding

### Medicaid Spending Context

Source: [KFF: Medicaid Enrollment & Spending Growth FY 2025–2026](https://www.kff.org/medicaid/medicaid-enrollment-spending-growth-fy-2025-2026/)

- Medicaid enrollment declined 7.6% in FY 2025 (due to end of continuous enrollment)
- Total spending growth: 8.6% in FY 2025, projected 7.9% in FY 2026
- Cost drivers include rate increases, higher acuity among remaining enrollees, and rising long-term care and behavioral health costs
- Nearly two-thirds of states face at least a "50-50" chance of Medicaid budget shortfall in FY 2026

### Directed Payment Growth

Source: [GAO: Medicaid Managed Care Directed Payments](https://www.gao.gov/products/gao-24-106202)

- State directed payments reached $38.5B in 2022, with approved arrangements projected at $110.2B/year as of Aug 2024
- CMS has not established definitions or standards for assessing whether these payments are "reasonable and appropriate"
- This represents a legitimate oversight gap, though GAO frames it as a policy/management issue rather than fraud

---

## 5. Key Questions the Public Is Asking

Based on our review of citizen analysis, media coverage, and social media discourse:

1. **Who are the top billers, and are any of them fraudulent?** The "Billion Dollar Club" analysis identified mega-billers, but most are fiscal intermediaries and government agencies, not solo practitioners committing fraud.

2. **Why is Brooklyn so concentrated for T1019 billing?** 7 of the top 20 T1019 billers nationwide are in Brooklyn. Is this entity proliferation fraud, or a reflection of NY's large home care program and its Consumer Directed Personal Assistance Program (CDPAP)?

3. **Are ghost providers real?** Can we identify providers billing for more services than could physically be delivered? (Our Investigation 5 addresses this directly.)

4. **How much is actual fraud vs. bureaucratic error vs. program design?** The 5.1% improper payment rate is dominated by documentation gaps (77%), not fraud. But the Minnesota case suggests concentrated fraud in specific programs could be enormous.

5. **Which states have the worst oversight?** State-level variation in T-MSIS data quality and payment accuracy is significant. Are some states systematically failing to detect fraud?

6. **Are the DOGE fraud claims being used to justify coverage cuts?** This is the most politically charged question. Our role is to investigate the data honestly, not to advance either side's narrative.

7. **What happened to providers after enforcement actions?** Can we see billing drop-offs that correlate with known indictments and takedowns?

---

## 6. Responsible Reporting Guardrails

### What Context Is Needed to Avoid "Fake News" Traps

1. **High billing ≠ fraud.** Fiscal intermediaries, state agencies, and large managed care organizations process enormous volumes legitimately. Always identify entity type before labeling a top biller suspicious.

2. **"Improper payment" ≠ fraud.** 77% of improper payments are documentation issues. The 5.1% rate is the lowest in years.

3. **"Intended loss" ≠ actual loss.** The DOJ's $14.6B figure represents what defendants allegedly *attempted* to steal, not what Medicaid actually paid.

4. **T-MSIS data has known quality issues.** State reporting to CMS varies significantly. Anomalies may reflect data quality problems, not fraud.

5. **Naming individuals requires evidence, not just outlier statistics.** A provider billing above the median is not evidence of fraud. Always check for alternative explanations (specialty, patient population, geographic cost differences) before flagging individuals.

6. **Our role is investigation, not prosecution.** We identify patterns that warrant further scrutiny. We do not accuse anyone of fraud. Language matters: "anomalous billing pattern" is appropriate; "fraud" requires adjudication.

7. **Consider the human cost.** Medicaid serves 90 million Americans. Fraud claims used to justify cuts affect real people's access to healthcare. Our analysis should be rigorous enough to distinguish genuine fraud from program design issues, so that enforcement targets the right problems.

### Data Caveats

- **Cell suppression**: Rows with fewer than 12 claims are suppressed for privacy, meaning very small providers are invisible in this dataset
- **Aggregation level**: Data is provider × HCPCS code × month — we cannot see individual beneficiary claims
- **Managed care**: Both fee-for-service and managed care claims are included, but managed care payment structures may create apparent anomalies
- **Time period**: 2018–2024 covers both pre-COVID and COVID-era Medicaid expansion, which dramatically changed enrollment and spending patterns

---

## Sources

**Dataset Release Coverage:**
- [Elon Musk says new Medicaid database could help the public find fraud — Axios](https://www.axios.com/2026/02/14/elon-musk-doge-medicaid-fraud-hhs-database)
- [DOGE Open-Sources Largest Medicaid Dataset — Benzinga](https://www.benzinga.com/news/health-care/26/02/50631736/doge-open-sources-largest-medicaid-dataset-in-agency-history-as-elon-musk-touts-transparency-mov)
- [HHS Releases Medicaid Dataset to Crowdsource Fraud Detection — Townhall](https://townhall.com/tipsheet/scott-mcclallen/2026/02/14/health-and-human-services-releases-massive-open-source-data-set-n2671316)

**Citizen Analyst Findings:**
- [The Billion-Dollar Club — The Rojas Report](https://dutchrojas.substack.com/p/the-billion-dollar-club)
- [$90 Billion in Medicaid Fraud from 0.16% of Providers — TFTC](https://www.tftc.io/doge-medicaid-fraud-90-billion-open-source-data/)
- [The $800B Open Secret — On Healthcare Tech](https://www.onhealthcare.tech/p/the-800b-open-secret-what-the-new)

**Minnesota Fraud Case:**
- [U.S. Attorney: Fraud likely exceeds $9 billion — Minnesota Reformer](https://minnesotareformer.com/2025/12/18/u-s-attorney-fraud-likely-exceeds-9-billion-in-minnesota-run-medicaid-services/)
- [CMS Weaponizes Fraud Against Medicaid in Minnesota — Georgetown CCF](https://ccf.georgetown.edu/2026/01/16/cms-weaponizes-fraud-against-medicaid-in-minnesota/)

**Fact Checks and Context:**
- [Clarifying claim that DOGE found 8M people fraudulently on Medicaid — Snopes](https://www.snopes.com/news/2025/05/24/medicaid-doge-rfk-jr/)
- [The Truth about Waste and Abuse in Medicaid — Georgetown CCF](https://ccf.georgetown.edu/2025/01/27/the-truth-about-waste-and-abuse-in-medicaid/)
- [Republicans' Claims of "Fraud" Are a Pretext for Medicaid Cuts — CBPP](https://www.cbpp.org/blog/republicans-claims-of-fraud-are-a-pretext-for-unpopular-and-drastic-medicaid-cuts)

**Government Oversight Reports:**
- [Medicaid Enrollment & Spending Growth FY 2025–2026 — KFF](https://www.kff.org/medicaid/medicaid-enrollment-spending-growth-fy-2025-2026/)
- [Medicaid Managed Care Directed Payments Need Enhanced Oversight — GAO](https://www.gao.gov/products/gao-24-106202)
- [CMS Put $11.2 Billion at Risk — HHS OIG](https://oig.hhs.gov/reports/all/2025/cms-put-112-billion-at-risk-of-fraud-waste-and-abuse-by-not-properly-closing-contracts/)
- [National Health Care Fraud Takedown: 324 Defendants, $14.6B — DOJ](https://www.justice.gov/opa/pr/national-health-care-fraud-takedown-results-324-defendants-charged-connection-over-146)
- ["Wasteful" Medicaid spending: What's been found so far — Newsweek](https://www.newsweek.com/wasteful-medicaid-spending-doge-findings-2035250)
