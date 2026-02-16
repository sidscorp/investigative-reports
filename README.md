# Investigative Reports

Data-driven investigative analyses using large public datasets. Each investigation applies statistical outlier detection, temporal pattern analysis, and cross-referencing with federal enforcement data to surface anomalies worth further scrutiny.

## Repository Structure

```
.
├── investigations/
│   └── medicaid-provider-spending/     # First investigation
│       ├── scripts/
│       │   ├── lib/
│       │   │   ├── __init__.py
│       │   │   └── data.py            # Shared data-loading utilities
│       │   ├── investigate_*.py       # 7 investigation scripts
│       │   ├── dashboard.py           # Streamlit interactive dashboard
│       │   └── ...                    # Supporting analysis scripts
│       ├── reports/
│       │   ├── comprehensive_findings.md
│       │   ├── em_upcoding_analysis.md
│       │   ├── identifiability_report.md
│       │   └── public_narrative_landscape.md
│       ├── data/                      # Large files gitignored (see setup)
│       └── output/                    # Generated CSVs (gitignored)
├── requirements.txt
└── README.md
```

## Investigations

### 1. Medicaid Provider Spending

Analysis of the CMS Medicaid Provider Utilization dataset — 227 million claims (2018–2024, ~$1.09 trillion in positive payments) — across seven sub-investigations targeting fraud, waste, and abuse (FWA) patterns.

| # | Script | Focus | Key Finding | Report |
|---|--------|-------|-------------|--------|
| 1 | `investigate_t1019_brooklyn.py` | Brooklyn T1019 personal care clustering | 7 of top 20 national T1019 billers in Brooklyn; 47 shared addresses, 68 shared authorized officials | `comprehensive_findings.md` |
| 2 | `investigate_minnesota.py` | Minnesota behavioral health spending | $2.71B in BH spending; 174 flagged providers; Twin Cities Autism Center only entity with both volume and cost flags | `comprehensive_findings.md` |
| 3 | `investigate_individuals.py` | Individual provider outlier deep dive | 5,949 outliers (>5x specialty median); 14 OIG-matched; highest cost ratio 287x | `comprehensive_findings.md` |
| 4 | `investigate_temporal.py` | Temporal anomaly detection | 6,337 spike providers; 249 fast starters; 8,715 disappearances; 662 near DOJ takedown dates | `comprehensive_findings.md` |
| 5 | `investigate_ghost_providers.py` | Ghost / impossible-volume providers | 37 individuals exceed T1019 physical capacity limits; 2,855 with implausible claims-per-beneficiary | `comprehensive_findings.md` |
| 6 | `investigate_shell_companies.py` | Shell company network detection | Address and authorized-official clustering to identify potential shell networks | `comprehensive_findings.md` |
| 7 | `investigate_em_upcoding.py` | E&M upcoding analysis | Systematic shift toward higher-level E&M codes post-2021 guideline change | `em_upcoding_analysis.md` |

#### Data sources

All large data files are excluded from the repository and must be downloaded separately.

| Dataset | Source | Setup |
|---------|--------|-------|
| CMS Medicaid Provider Utilization | [CMS](https://data.cms.gov/summary-statistics-on-use-and-payments/medicare-medicaid-dual-enrollment/medicaid-provider-summary-by-type-of-service) | Download parquet (~2.7 GB) to `investigations/medicaid-provider-spending/data/medicaid-provider-spending.parquet` |
| NPPES NPI Registry | [CMS NPPES](https://download.cms.gov/nppes/NPI_Files.html) | Run `python investigations/medicaid-provider-spending/scripts/download_reference_data.py`; derived parquets built automatically on first use |
| HCPCS Codes | [CMS HCPCS](https://www.cms.gov/medicare/coding-billing/healthcare-common-procedure-system/quarterly-update) | Download ZIP to `investigations/medicaid-provider-spending/data/hcpcs/`, run `python investigations/medicaid-provider-spending/scripts/parse_hcpcs.py` (pre-parsed CSV included in repo) |
| NUCC Taxonomy | [NUCC](https://nucc.org/index.php/code-sets-mainmenu-41/provider-taxonomy-mainmenu-40) | Included at `investigations/medicaid-provider-spending/data/nucc_taxonomy_251.csv` |
| OIG LEIE Exclusion List | [OIG](https://oig.hhs.gov/exclusions/exclusions_list.asp) | Download to `investigations/medicaid-provider-spending/data/UPDATED.csv` |

#### Quick start

```bash
# Setup
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# Download reference data
python investigations/medicaid-provider-spending/scripts/download_reference_data.py
python investigations/medicaid-provider-spending/scripts/parse_hcpcs.py

# Place primary dataset (see data sources above)
# investigations/medicaid-provider-spending/data/medicaid-provider-spending.parquet

# Run an investigation
python investigations/medicaid-provider-spending/scripts/investigate_ghost_providers.py
```

#### Processing pipeline

```
download_reference_data.py  ─→  Raw NPI CSVs in data/
parse_hcpcs.py              ─→  data/hcpcs_codes.csv
                                 ↓
                    (NPI parquets built on first use by lib/data.py)
                                 ↓
                    investigate_*.py scripts  ─→  output/ CSVs + reports/
```

## Disclaimer

These projects are **statistical screening tools** for research and journalistic purposes. Flagged entities represent statistical outliers — not confirmed instances of fraud, waste, or abuse. Any findings require independent verification before drawing conclusions about specific individuals or organizations.
