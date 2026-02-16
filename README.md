# Medicaid Provider Spending — Investigative Analysis Framework

A data-driven framework for detecting potential fraud, waste, and abuse (FWA) in the CMS Medicaid Provider Utilization dataset. Analyzes 227 million claims (2018–2024, ~$1.09 trillion in positive payments) across seven independent investigations, using statistical outlier detection, temporal pattern analysis, and cross-referencing with federal enforcement data.

## Investigations

| # | Script | Focus | Key Finding | Report |
|---|--------|-------|-------------|--------|
| 1 | `investigate_t1019_brooklyn.py` | Brooklyn T1019 personal care clustering | 7 of top 20 national T1019 billers in Brooklyn; 47 shared addresses, 68 shared authorized officials | `comprehensive_findings.md` |
| 2 | `investigate_minnesota.py` | Minnesota behavioral health spending | $2.71B in BH spending; 174 flagged providers; Twin Cities Autism Center only entity with both volume and cost flags | `comprehensive_findings.md` |
| 3 | `investigate_individuals.py` | Individual provider outlier deep dive | 5,949 outliers (>5× specialty median); 14 OIG-matched; highest cost ratio 287× | `comprehensive_findings.md` |
| 4 | `investigate_temporal.py` | Temporal anomaly detection | 6,337 spike providers; 249 fast starters; 8,715 disappearances; 662 near DOJ takedown dates | `comprehensive_findings.md` |
| 5 | `investigate_ghost_providers.py` | Ghost / impossible-volume providers | 37 individuals exceed T1019 physical capacity limits; 2,855 with implausible claims-per-beneficiary | `comprehensive_findings.md` |
| 6 | `investigate_shell_companies.py` | Shell company network detection | Address and authorized-official clustering to identify potential shell networks | `comprehensive_findings.md` |
| 7 | `investigate_em_upcoding.py` | E&M upcoding analysis | Systematic shift toward higher-level E&M codes post-2021 guideline change | `em_upcoding_analysis.md` |

## Project Structure

```
.
├── scripts/
│   ├── lib/
│   │   ├── __init__.py
│   │   └── data.py                  # Shared data-loading utilities
│   ├── download_reference_data.py   # Downloads NPPES NPI registry
│   ├── parse_hcpcs.py               # Parses HCPCS codes from CMS source
│   ├── investigate_t1019_brooklyn.py # Investigation 1
│   ├── investigate_minnesota.py     # Investigation 2
│   ├── investigate_individuals.py   # Investigation 3
│   ├── investigate_temporal.py      # Investigation 4
│   ├── investigate_ghost_providers.py # Investigation 5
│   ├── investigate_shell_companies.py # Investigation 6
│   ├── investigate_em_upcoding.py   # Investigation 7
│   ├── investigate_em_convergence.py # Investigation 7 supplemental
│   ├── investigate_em_adjusted.py   # Investigation 7 supplemental
│   ├── dashboard.py                 # Streamlit dashboard
│   └── ...                          # Additional analysis scripts
├── reports/
│   ├── comprehensive_findings.md    # Investigations 1–6 combined report
│   ├── em_upcoding_analysis.md      # Investigation 7 report
│   ├── identifiability_report.md    # Re-identification risk assessment
│   └── public_narrative_landscape.md # Media and enforcement landscape
├── data/                            # Reference data (see setup below)
├── medicaid-provider-spending/
│   └── data/                        # Primary dataset + taxonomy
├── output/                          # Generated CSVs (gitignored)
└── requirements.txt
```

## Data Sources & Setup

All large data files are excluded from the repository and must be downloaded separately.

### 1. CMS Medicaid Provider Utilization (primary dataset)

- **Source**: [CMS Medicaid Provider Utilization](https://data.cms.gov/summary-statistics-on-use-and-payments/medicare-medicaid-dual-enrollment/medicaid-provider-summary-by-type-of-service)
- **Format**: Parquet (~2.7 GB)
- **Place at**: `medicaid-provider-spending/data/medicaid-provider-spending.parquet`

### 2. NPPES NPI Registry

- **Source**: [CMS NPPES Downloads](https://download.cms.gov/nppes/NPI_Files.html)
- **Setup**: Run `python scripts/download_reference_data.py` to download and extract
- **Derived parquets** (`data/npi_slim.parquet`, `data/npi_address.parquet`) are built automatically on first use by `scripts/lib/data.py`

### 3. HCPCS Codes

- **Source**: [CMS HCPCS Downloads](https://www.cms.gov/medicare/coding-billing/healthcare-common-procedure-system/quarterly-update)
- **Setup**: Download the ZIP, extract to `data/hcpcs/`, then run `python scripts/parse_hcpcs.py`
- Pre-parsed `data/hcpcs_codes.csv` is included in the repository for convenience

### 4. NUCC Taxonomy

- **Source**: [NUCC Health Care Provider Taxonomy](https://nucc.org/index.php/code-sets-mainmenu-41/provider-taxonomy-mainmenu-40)
- **Included** in repo at `medicaid-provider-spending/data/nucc_taxonomy_251.csv`

### 5. OIG LEIE Exclusion List

- **Source**: [OIG LEIE Database](https://oig.hhs.gov/exclusions/exclusions_list.asp)
- **Place at**: `medicaid-provider-spending/data/UPDATED.csv`

## Quick Start

```bash
# 1. Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Download reference data
python scripts/download_reference_data.py   # NPI registry
python scripts/parse_hcpcs.py               # HCPCS codes (after placing ZIP)

# 4. Place primary dataset (see Data Sources above)
#    medicaid-provider-spending/data/medicaid-provider-spending.parquet

# 5. Run an investigation
python scripts/investigate_ghost_providers.py
```

## Processing Pipeline

```
download_reference_data.py  ─→  Raw NPI CSVs
parse_hcpcs.py              ─→  data/hcpcs_codes.csv
                                 ↓
                    (NPI parquets built on first use by lib/data.py)
                                 ↓
                    investigate_*.py scripts  ─→  output/ CSVs + reports/
```

## Disclaimer

This project is a **statistical screening tool** for research and journalistic purposes. Flagged providers represent statistical outliers — not confirmed instances of fraud, waste, or abuse. Any findings require independent verification before drawing conclusions about specific providers.
