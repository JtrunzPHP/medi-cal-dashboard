# California Medi-Cal Managed Care Dashboard

Live dashboard tracking Medi-Cal managed care enrollment by county, plan, and beneficiary language demographics. All data sourced from the California Department of Health Care Services (DHCS) Open Data Portal.

## Dashboard Tabs

- **Overview** — Statewide enrollment trends, top 10 counties, KPIs
- **County Deep Dive** — Select any CA county; see plan-level breakdowns and trends
- **MOH vs CNC** — Molina Healthcare vs Centene/Health Net head-to-head comparison
- **Language / Immigration Proxy** — Primary language demographics in LA County as an immigration indicator

## Data Sources

| Dataset | Source | Frequency |
|---------|--------|-----------|
| Managed Care Enrollment | [DHCS Open Data](https://data.chhs.ca.gov/dataset/medi-cal-managed-care-enrollment-report) | Monthly |
| Certified Eligibles by Language | [DHCS Open Data](https://data.chhs.ca.gov/dataset/medi-cal-certified-eligibles-with-demographics-by-month) | Monthly |

## How It Works

1. A **GitHub Actions** workflow runs on the 15th of each month
2. It runs `scripts/fetch_data.py` which downloads the latest CSVs from DHCS
3. The script processes them into compact JSON files in `data/`
4. The workflow commits the updated JSON files back to the repo
5. **GitHub Pages** serves `index.html` which reads the local JSON files

No backend server needed. No CORS issues. Fully static.

## Repo Structure

```
├── index.html                         # Dashboard (React + Recharts)
├── data/
│   ├── enrollment.json                # Generated — managed care enrollment
│   └── language.json                  # Generated — primary language demographics
├── scripts/
│   └── fetch_data.py                  # Data fetcher / processor
├── .github/
│   └── workflows/
│       └── update-data.yml            # Monthly auto-refresh cron
└── README.md
```

## Notes

- **MOH** = Molina Healthcare of California (NYSE: MOH)
- **CNC** = Centene Corp (NYSE: CNC), operating as **Health Net** in CA Medi-Cal
- Data starts from January 2023 (3-year window); raw DHCS data goes back to 2007
- Language data comes from the Certified Eligibles dataset (broader than managed care) and serves as a demographic proxy
