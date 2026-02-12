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

**Fetch data** for a specific month (requires VPN):

```bash
source .env
python reports/monthly/fetch_monthly_data.py 2025 1
```

This saves raw data to `reports/monthly/data/2025-01.yaml`.

**Build the report** from fetched data:

```bash
source .env
python reports/monthly/build_monthly_report.py 2025 1 md
```

Output formats: `md`, `html`, `pdf`. Reports are saved to `reports/monthly/output/`.

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
