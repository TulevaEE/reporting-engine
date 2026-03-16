# Monthly Update Guide — Fondide Võrdlus

Step-by-step workflow for updating the fund comparison tool to a new month.

## Prerequisites

- Python 3 with pdfplumber, pandas, numpy
- EODHD API key in `.env`
- All 24 pension fund PDFs published for the target month

## Steps

### 1. Organize PDFs

Move current month's PDFs into a dated subfolder and create one for the new month:

```
cd fondide-vordlus
mkdir -p "Investeeringute aruanne/YYYY-MM"
mv "Investeeringute aruanne/"*.pdf "Investeeringute aruanne/YYYY-MM-prev/"
```

### 2. Archive published data

Copy the current live data to a dated subfolder so the old month remains accessible:

```
mkdir -p ../docs/fondide-vordlus/YYYY-MM-prev
cp ../docs/fondide-vordlus/*.json ../docs/fondide-vordlus/*.html ../docs/fondide-vordlus/YYYY-MM-prev/
```

### 3. Download all 24 February PDFs

Download into `Investeeringute aruanne/YYYY-MM/`:

**Pensionikeskus funds** (date in URL = last calendar day of month):
- Tuleva: `est_TUK75_raport_YYYYMMDD.pdf`, `est_TUK00_raport_YYYYMMDD.pdf` (last business day)
- LHV: LLK50, LXK75, LXK00, LIK75, LMK25
- SEB: SIK75, SEK50, SEK100, SEK25, SEK00
- Luminor: NPK75, NIK100, NPK50, NPK25, NPK00

URL pattern: `https://www.pensionikeskus.ee/files/raport/{CODE}/est_{CODE}_raport_{YYYYMMDD}.pdf`

**Swedbank** (static URLs, overwritten monthly):
- `https://swedbank.ee/static/investor/funds/{name}_investment_portfolio.pdf`
- Funds: K1960, K1970, K1980, K1990, K2000, Ki, KKONS

**If any PDF is missing, stop.** Never mix months.

### 4. Create monthly config

Copy previous month's config and update:

```
cp data/monthly/YYYY-MM-prev.json data/monthly/YYYY-MM.json
```

Update in the new file:
1. `"month": "YYYY-MM"`
2. All `reports` entries: `pdf` (with `YYYY-MM/` prefix), `date`, `url`
3. `allocations` — extract new weights from PDFs for funds without parsers:
   - **SEB 18+, 60+, 65+**: equity funds (with ISINs), bonds, stocks, RE, PE, bond funds
   - **Luminor Indeks, 50-56, 56+, 61-65**: equity funds, bond funds, RE, PE, deposits
   - **SEB Indeks**: fund names, ISINs, weights (flat list)
   - **SEB 55+**: has a parser but also needs config allocations

**Watch for double-counting:** If you list individual bonds in the `bonds` array, set `direct_bond_pct: 0`. If you use `direct_bond_pct` as an aggregate, leave `bonds: []`.

### 5. Regenerate LLK50_parsed.json

Parse the LHV Ettevõtlik PDF using `parse_lhv_monthly()` and save to `data/raw/holdings/LLK50_parsed.json`. Include `nav_eur` from the PDF's "FONDI VARADE PUHASVÄÄRTUS" line.

### 6. Update sources.html

Change 3 hardcoded month references in `docs/fondide-vordlus/sources.html`:
- Line ~160: `seisuga {month} {year}`
- Line ~183: `({month} {year})`
- Line ~322: `{month_genitive} {year} seisuga`

### 7. Check for new ISINs

Compare new allocations against `ETF_ISIN_TO_CSV` and `TRUE_PROXY_ISINS` in `export_fund_data.py`. Add proxy mappings for any new ISINs (fund share class changes, new ETFs).

### 8. Run pipeline

```bash
python3 export_fund_data.py --month YYYY-MM
```

### 9. Verify

- [ ] `fund_data.json` has `"data_month": "YYYY-MM"`
- [ ] `data_sources.json` — all dates show the target month
- [ ] `sources.html` — month text updated
- [ ] `YYYY-MM-prev/index.html` — still shows previous month
- [ ] No January/previous month data mixed in

### 10. Commit & push

```bash
git add docs/fondide-vordlus/ fondide-vordlus/data/monthly/ fondide-vordlus/data/raw/holdings/LLK50_parsed.json
git commit -m "Update fondide-vordlus to YYYY-MM data"
git push
```

## Fund codes reference

| Fund | Pensionikeskus code | Provider |
|---|---|---|
| Tuleva Maailma Aktsiad | TUK75 | Tuleva |
| Tuleva Võlakirjad | TUK00 | Tuleva |
| SEB Indeks | SIK75 | SEB |
| SEB 55+ | SEK50 | SEB |
| SEB 18+ | SEK100 | SEB |
| SEB 60+ | SEK25 | SEB |
| SEB 65+ | SEK00 | SEB |
| LHV Ettevõtlik | LLK50 | LHV |
| LHV Julge | LXK75 | LHV |
| LHV Rahulik | LXK00 | LHV |
| LHV Indeks | LIK75 | LHV |
| LHV Tasakaalukas | LMK25 | LHV |
| Luminor 16-50 | NPK75 | Luminor |
| Luminor Indeks | NIK100 | Luminor |
| Luminor 50-56 | NPK50 | Luminor |
| Luminor 56+ | NPK25 | Luminor |
| Luminor 61-65 | NPK00 | Luminor |
