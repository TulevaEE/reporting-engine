---
name: fetch-holdings
description: Use when fetching, loading, or matching ETF holdings data for Tuleva portfolio analysis. Covers data sources, ISIN matching, and common pitfalls.
---

# Fetching & Matching ETF Holdings

## Golden rule: use ISIN as the primary stock identifier

ISIN is the only universal, unambiguous equity identifier. Never use `Ticker|Location` as a primary key — tickers differ across providers (e.g. `HDFCB` vs `HDFCBANK`, `BRK/B` vs `BRKB`, `0Y3K|Ireland` vs `EATON|United States`).

## Data sources

### ACWI benchmark — use SPDR, not iShares

**SPDR MSCI ACWI ETF (SPYY GY)** publishes daily holdings WITH ISINs:

```
https://www.ssga.com/uk/en_gb/intermediary/etfs/library-content/products/fund-data/etfs/emea/holdings-daily-emea-en-spyy-gy.xlsx
```

- Skip first 5 rows (metadata), row 6 is header
- Columns: `ISIN, SEDOL, Security Name, Currency, Number of Shares, Percent of Fund, Trade Country Name, Local Price, Sector Classification, Industry Classification, Base Market Value`
- ~2,300 equities, 100% weight coverage, 49 countries
- Filter equities: `df[df['ISIN'].str.match(r'^[A-Z]{2}[A-Z0-9]{9}[0-9]$')]`

Do NOT use iShares SSAC — its CSV omits ISINs (only Ticker, Name, Location).

### TKF model portfolio ETFs

| ETF | Source | Has ISINs? | Notes |
|-----|--------|-----------|-------|
| Xtrackers USA/Canada | Downloaded XLSX | Yes | Skip 3 rows, column C is ISIN |
| Amundi USA | Downloaded XLSX | Yes | `engine='calamine'`, skip 20 rows, column B is ISIN |
| BNP Europe/Japan/Pacific | Downloaded XLSX | Yes | `engine='calamine'`, skip 22 rows, filter `Asset_Class == 'Equity'` |
| Invesco EM | companiesmarketcap.com | Yes | Web scrape, ~93% coverage (missing ~7% inflates active share 3-5pp) |
| iShares SAWD | iShares CSV | **No** | Has Ticker+Location only. Need Ticker→ISIN bridge via SPDR |
| Vanguard ESG NA | Downloaded XLSX | **No** | Has Ticker only. Need Ticker→ISIN bridge via SPDR |

### Building a Ticker→ISIN bridge for iShares/Vanguard

For the 2 ETFs without ISINs, use SPDR ACWI as a bridge:
1. Load SPDR (has ISIN + Name + Country)
2. Build lookup by normalized name or by matching against a known ticker→ISIN mapping
3. Match iShares/Vanguard tickers to SPDR ISINs

## Matching rules

1. **Always join on ISIN first** — this matches 95-100% of holdings for ISIN-bearing ETFs
2. **For residual unmatched**: use normalized name matching (strip LTD/INC/CORP/PLC, currency suffixes like KRW5000, share class suffixes like PFD/CLASS A)
3. **Never match on ticker alone across providers** — same ticker can be different companies (e.g. `CFR` is Cullen/Frost in US, Richemont in Switzerland; `ALV` is Autoliv in US, Allianz in Germany)
4. **Preferred vs common shares**: for active share purposes, treat as the same stock (e.g. Samsung Electronics KR7005931001 pref = KR7005930003 common)

## Common pitfalls

- **OpenFIGI returns wrong exchange tickers**: Maps ISINs to local exchange tickers that don't match iShares conventions. Avoid using OpenFIGI for cross-provider matching.
- **Irish-domiciled US companies**: ISINs starting with `IE` (Eaton, Accenture, Medtronic, Linde) get mapped to Irish tickers by OpenFIGI instead of US tickers.
- **Country mismatches for cross-listed stocks**: Spotify (Sweden vs US), AerCap (Netherlands vs US), Waste Connections (Canada vs US). Same stock, different domicile depending on provider.
- **Korean tickers**: Korean ISINs often missing from OpenFIGI. Samsung, SK Hynix etc. need manual ISIN→ticker mapping if not using ISIN-based matching.
- **pensionikeskus.ee III pillar page** does NOT include TKF — it's a separate fund category.
- **Validate matching quality with decomposition**: Always check weight excluded/added/reduced. If "weight added" is suspiciously large, it signals matching failures, not genuine portfolio differences.

## Name normalization function

```python
def normalize_name(name):
    n = str(name).upper().strip()
    n = re.sub(r'\s*\([A-Z]+\)\s*', ' ', n)
    n = re.sub(r'\s+(KRW|TWD|INR|CNY|HKD|ZAR|SAR|BRL|MXN|THB|IDR|MYR|PHP|PLN|CLP|QAR|AED|KWD|COP|PEN|CZK|EGP|HUF|TRY|USD|NPV)\s*[\d.]*', '', n)
    n = re.sub(r'\s*-?(?:PFD|PREF|NON VOTING PRE|CLASS [A-Z]|CL [A-Z])\s*$', '', n)
    n = re.sub(r'\s*-[A-Z]+\s*$', '', n)
    for _ in range(3):
        n = re.sub(r'\s*(CORPORATION|CORP|LIMITED|LTD|INC|PLC|CO|SA|AG|NV|SE|SPA|AB|ASA|OYJ|TBK|BHD|PCL|BERHAD|SH|ADS)\s*$', '', n)
    n = n.replace(' & ', ' AND ')
    n = re.sub(r'\s+', ' ', n).strip()
    return n
```
