# Tuleva Reports

Reporting engine for generating Tuleva board reports from Metabase data.

## Setup

### Prerequisites

- Python 3.10+
- Access to Tuleva AWS VPN (Metabase is behind VPN)
- Access to Tuleva 1Password vault

### 1. Clone the repo

```bash
git clone git@github.com:AskTuleva/tuleva-reports.git
cd tuleva-reports
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

For PDF generation, you also need system libraries for WeasyPrint:

```bash
brew install pango
```

### 3. Set up secrets

Copy the `.env` file from Tuleva 1Password (search for **"Tuleva Reports - Secrets"**) and save it to the project root:

```
tuleva-reports/
  .env          <-- this file from 1Password
  reports/
  common/
  ...
```

The `.env` file contains:
- **METABASE_API_KEY** — API key for metabase.tuleva.ee
- **GCP_SERVICE_ACCOUNT** — Google Cloud service account JSON for Google Sheets access

### 4. Connect to VPN

Connect to the Tuleva AWS VPN before running any data fetches — Metabase is only accessible through VPN.

## Usage

### Monthly board report

The monthly report reads Metabase only through the Tuleva agent gateway (`common/scripts/gateway_client.py`):
no VPN or Metabase API key. The first run opens a browser to log in with your Tuleva Google account.
The gateway answers only cards granted to agents; `fetch_monthly_data.py` stops and lists any card
that is not granted. Ask for the grant with the gateway's `metabase` tool and confirm it in Slack.

```bash
.venv/bin/python3 reports/monthly/fetch_monthly_data.py 2026 9      # -> reports/monthly/data/2026-09.yaml
.venv/bin/python3 reports/monthly/build_monthly_report.py 2026 9 md  # -> reports/monthly/output/2026/monthly_report_2026-09.md
```

Write the comments into the `<!-- comment:... -->` blocks of the MD file. Every build re-renders the MD
from the data and keeps those blocks, so re-fetching and rebuilding is safe; text outside them is overwritten.

```bash
.venv/bin/python3 reports/monthly/build_monthly_report.py 2026 9 html     # check it in the browser
.venv/bin/python3 reports/monthly/build_monthly_report.py 2026 9 publish  # html + docs/latest-monthly-report.html
```

Commit and push `main` to publish on GitHub Pages.

Chapter 7 (financial results, cards 2839 and 2866): on the 1st of the month the warehouse has an estimate
(actual AUM, known costs) and the report shows a note saying so. The accounting figures arrive mid-month;
rebuild then if the report should show them.

## Project structure

```
tuleva-reports/
├── common/
│   ├── config/
│   │   └── metabase.yaml        # Metabase connection config
│   ├── scripts/
│   │   ├── metabase_client.py   # Metabase API client
│   │   ├── fetch_data.py        # Google Sheets data fetcher
│   │   └── generate_charts.py   # Chart generation
│   └── branding/                # Tuleva CSS styles
├── reports/
│   ├── monthly/
│   │   ├── fetch_monthly_data.py   # Fetch monthly KPIs from Metabase
│   │   ├── build_monthly_report.py # Build report from fetched data
│   │   ├── content/
│   │   │   └── report.md           # Jinja2 report template
│   │   ├── data/                   # Fetched YAML data (gitignored)
│   │   └── output/                 # Generated reports (gitignored)
│   └── annual/
│       └── ...
├── .env                         # Secrets (from 1Password, gitignored)
├── requirements.txt
└── README.md
```
