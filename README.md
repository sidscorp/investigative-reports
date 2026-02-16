# Investigative Reports

Statistical analysis framework for data-driven investigative journalism and public interest research. Each investigation lives in its own directory under `investigations/` with self-contained scripts, data, reports, and output.

## Investigations

| Investigation | Description |
|---------------|-------------|
| [Medicaid Provider Spending](investigations/medicaid-provider-spending/) | Analysis of 227M Medicaid claims (2018-2024) for fraud, waste, and abuse patterns |

## Setup

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

See each investigation's README for data setup and usage instructions.

## Disclaimer

These projects are **statistical screening tools** for research and journalistic purposes. Flagged entities represent statistical outliers, not confirmed wrongdoing. Any findings require independent verification.
