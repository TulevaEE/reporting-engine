# Fondide Röntgen — Estonian Pension Fund Comparison

Interactive tool comparing all 24 Estonian II pillar pension funds at stock level.

**Live tool:** https://tulevaee.github.io/reporting-engine/fondide-vordlus/
**Methodology:** [sources page](https://tulevaee.github.io/reporting-engine/fondide-vordlus/sources.html)

## What it does

The pipeline (`export_fund_data.py`) reads pension fund annual reports (PDFs), ETF holdings files (CSVs), and EODHD API data to produce a unified JSON dataset of stock-level holdings for all Estonian II pillar pension funds. The web tool visualizes overlaps, correlations, and geographic/sector allocation differences.

## Running locally

### Prerequisites
- Python 3.10+
- EODHD API key (free tier sufficient, needed for 2 ETFs: EMXU, BNKE)

### Setup
```bash
pip install -r requirements.txt
cp .env.example .env
# Add your EODHD_API_KEY to .env
```

### Download annual reports

Download the latest investment reports from fund managers and place them in `Investeeringute aruanne/`:

- [Tuleva](https://tuleva.ee/fondid/) — Tuleva World Stocks, Tuleva World Bonds
- [Swedbank](https://www.swedbank.ee/private/pensions/pillar2/funds) — K1, K2, K3, K4, K90-99, K100
- [SEB](https://www.seb.ee/pensionifondid) — SEB Energetic, Conservative, Progressive, Balanced
- [Luminor](https://www.luminor.ee/pensionifondid) — Luminor A/B Pluss, Luminor 16-50/51-100
- [LHV](https://www.lhv.ee/pensionifondid) — LHV Pensionifond S, M, L, XL, Indeks, Roheline

### Run
```bash
python export_fund_data.py              # full pipeline (fetches live NAV data)
python export_fund_data.py --month 2026-01  # specific month
python export_fund_data.py --skip-nav   # skip NAV fetch (faster, deterministic)
# Output: web/fund_data.json, web/nav_data.json
```

### Reproducing exact results

The ETF holdings CSVs and monthly config are tracked in the repo as point-in-time snapshots.

- **With `--skip-nav`**: output is fully deterministic from tracked data alone (no network calls for NAV).
  Use this when you need reproducible results or for development/testing.
- **Without `--skip-nav`**: fetches live NAV data from pensionikeskus.ee and yfinance.
  NAV values may differ slightly depending on the date you run the pipeline.
- Expected output: 24 funds, ~6.5 MB `fund_data.json`, ~700 KB `nav_data.json`.

## Data files included

- `data/monthly/2026-01.json` — Monthly fund allocation config (asset class weights, report paths)
- `data/raw/holdings/*.csv` — ETF holdings from iShares, Xtrackers, SPDR (~16 files)
- `data/raw/holdings/*.json` — EODHD API responses for Amundi ETFs
- `data/raw/holdings/blackrock/` — Parsed BlackRock semi-annual report holdings
- `data/raw/robur_custom_*.json` — Swedbank Robur fund holdings

## Data files NOT included (too large or proprietary)

- **Annual reports (PDFs)** — Download from fund manager websites listed above
- **SPPY_raw.xlsx** — Download from [SPDR website](https://www.ssga.com/etfs)
- **Swedbank scraped data** (`swedbank_chunks_*.js`) — Browser dev tools network tab on swedbank.ee fund pages
- **Robur HTML pages** — Download from swedbankrobur.se fund pages

## License

MIT
