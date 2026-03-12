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
- Has duplicate ISINs (preferred + common shares) — aggregate with `groupby('ISIN')` before using as lookup
- **No ticker column** — only ISIN, SEDOL, Security Name

Do NOT use iShares SSAC — its CSV omits ISINs (only Ticker, Name, Location). SSAC is still useful for iShares-vs-iShares comparisons (e.g. ESG screening analysis) since Ticker|Location is consistent within iShares.

### TKF model portfolio ETFs

| ETF | Source | Has ISINs? | Notes |
|-----|--------|-----------|-------|
| Xtrackers USA/Canada | Downloaded XLSX | Yes | Skip 3 rows, column C is ISIN |
| Amundi USA | Downloaded XLSX | Yes | `engine='calamine'`, skip 20 rows, column B is ISIN |
| BNP Europe/Japan/Pacific | Downloaded XLSX | Yes | `engine='calamine'`, skip 22 rows, filter `Asset_Class == 'Equity'` |
| Invesco EM | companiesmarketcap.com | Yes | Web scrape, ~93% coverage (missing ~7% inflates active share 3-5pp) |
| iShares SAWD | iShares CSV | **No** | Has Ticker+Location only. Need name→ISIN bridge via SPDR |
| Vanguard ESG NA | Downloaded XLSX | **No** | Has Ticker+Name only. Need name→ISIN bridge via SPDR |

### Building a name→ISIN bridge for iShares/Vanguard

For the 2 ETFs without ISINs, use SPDR ACWI as a bridge:
1. Load SPDR (has ISIN + Security Name + Trade Country Name)
2. Build lookup dicts: `{normalized_name: ISIN}` and `{(name_prefix, country): ISIN}`
3. For each iShares/Vanguard stock, normalize its name and look up the ISIN
4. Use **two-pass matching**: first with share class info preserved (`keep_class=True` to distinguish Class A/C), then without
5. Fall back to prefix+country matching (15→10→8→6→4 chars) for abbreviated names

**Bridging results** (as of Mar 2026): ~1800 of 2600 ticker-only stocks bridged, 1.26% weight unmatched (mostly Vanguard small caps genuinely not in ACWI).

## Matching rules

1. **Always join on ISIN first** — this matches 95-100% of holdings for ISIN-bearing ETFs
2. **For residual unmatched**: use normalized name matching with class-aware two-pass bridge
3. **Never match on ticker alone across providers** — same ticker can be different companies (e.g. `CFR` is Cullen/Frost in US, Richemont in Switzerland; `ALV` is Autoliv in US, Allianz in Germany)
4. **Preferred vs common shares**: for active share purposes, treat as the same stock (e.g. Samsung Electronics KR7005931001 pref = KR7005930003 common)
5. **Multi-class shares (Class A/B/C)**: preserve class info when bridging to avoid collapsing different ISINs (e.g. Alphabet Class A US02079K3059 ≠ Class C US02079K1079)

## Common pitfalls

- **OpenFIGI returns wrong exchange tickers**: Maps ISINs to local exchange tickers that don't match iShares conventions. Avoid using OpenFIGI for cross-provider matching.
- **Name normalization must handle cross-provider differences**: iShares uses "INC", SPDR uses "Incorporated"; iShares "AMAZON COM INC", SPDR "Amazon.com Inc."; iShares "MCDONALDS CORP", SPDR "McDonald's Corporation". Strip dots, apostrophes, hyphens early.
- **Abbreviated vs full names**: "LVMH" vs "LVMH Moet Hennessy Louis Vuitton SE", "GSK PLC" vs "GLAXOSMITHKLINE" (rebrand). Short prefixes (4-6 chars) with country constraint catch abbreviations; rebrands need manual mapping.
- **"THE" prefix and "/The" suffix**: Vanguard uses "TJX Cos Inc/The", SPDR uses "The TJX Companies Inc." — strip both leading and trailing "THE".
- **Single-letter sequences from dot stripping**: "U.S. Bancorp" → "U S BANCORP" after dots removed. Collapse `r'\b([A-Z]) ([A-Z])\b'` → joined letters.
- **Country name differences**: iShares uses "Korea (South)", SPDR uses "South Korea". Use prefix+country as a hint, not a hard requirement.
- **BNP Min TE funds use sampling**: BNP doesn't fully replicate — some large stocks (e.g. Toyota Motor) may be missing. This creates genuine underweights, not matching bugs.
- **pensionikeskus.ee III pillar page** does NOT include TKF — it's a separate fund category.
- **Validate matching quality with decomposition**: Always check weight excluded/added/reduced. "Weight added" > 2% signals matching failures. Current notebook achieves 1.54% weight added.

## Name normalization function

```python
def normalize_name(name, keep_class=False):
    """Aggressively normalize stock names for cross-provider matching."""
    n = str(name).upper().strip()
    n = n.replace('.', ' ')    # "Inc." → "Inc "
    n = n.replace("'", '')     # "McDonald's" → "McDonalds"
    n = n.replace('-', ' ')    # "Freeport-McMoRan" → "Freeport McMoRan"
    n = n.replace('/', ' ')    # "TJX Cos Inc/The" → "TJX Cos Inc The"
    n = re.sub(r'^THE\s+', '', n)    # leading "The"
    n = re.sub(r'\s+THE\s*$', '', n)  # trailing "The"
    n = re.sub(r'\s*\([A-Z]+\)\s*', ' ', n)
    n = re.sub(r'\s+(KRW|TWD|INR|CNY|HKD|ZAR|SAR|BRL|MXN|THB|IDR|MYR|PHP|PLN|CLP|QAR|AED|KWD|COP|PEN|CZK|EGP|HUF|TRY|USD|NPV)\s*[\d.]*', '', n)
    # Strip voting designations (but optionally keep CLASS X)
    n = re.sub(r'\s+(?:SUBORDINATE VOTING|NON VOTING|PFD|PREF)\s*', ' ', n)
    if not keep_class:
        n = re.sub(r'\s+(?:CLASS [A-Z]\b|CL [A-Z]\b)\s*', ' ', n)
        n = re.sub(r'\s+CLA?\s*$', '', n)
    # Collapse single-letter sequences: "U S" → "US", "S P A" → "SPA"
    n = re.sub(r'\b([A-Z]) ([A-Z])\b', r'\1\2', n)
    n = re.sub(r'\b([A-Z]) ([A-Z])\b', r'\1\2', n)
    for _ in range(3):
        n = re.sub(r'\s+(AKTIENGESELLSCHAFT|INCORPORATED|CORPORATION|CORP|COMPANIES|COMPANY|COS|LIMITED|LTD|INC|PLC|CO|SA|AG|NV|SE|SPA|AB|ASA|OYJ|TBK|BHD|PCL|BERHAD|SH|ADS|REIT|DIVIDEND RIGHT CERT|PAR|N|SOCIETE ANONYME.*)\s*$', '', n)
    n = re.sub(r'\s+SPA\s*$', '', n)
    n = re.sub(r'\s+A/S\s*$', '', n)
    n = n.replace(' & ', ' AND ')
    n = re.sub(r'\s*(AND|&)\s*$', '', n)
    n = re.sub(r'\s+', ' ', n).strip()
    return n
```

## Two-pass bridge pattern

```python
# Build lookups from SPDR
spdr_name_class = {}   # WITH class → ISIN (precise)
spdr_name_to_isin = {} # WITHOUT class → ISIN (fallback)
spdr_prefix_country = {}  # (prefix, country) → ISIN
for _, r in spdr_acwi.iterrows():
    norm_cls = normalize_name(r['Security Name'], keep_class=True)
    norm = normalize_name(r['Security Name'], keep_class=False)
    country = r['Trade Country Name']
    spdr_name_class[norm_cls] = r['ISIN']
    if norm not in spdr_name_to_isin:
        spdr_name_to_isin[norm] = r['ISIN']
    for plen in [15, 10, 8, 6, 4]:
        key = (norm[:plen], country)
        if len(norm) >= plen and key not in spdr_prefix_country:
            spdr_prefix_country[key] = r['ISIN']

def bridge_to_isin(ticker, name, country):
    norm_cls = normalize_name(name, keep_class=True)
    if norm_cls in spdr_name_class:
        return spdr_name_class[norm_cls]
    norm = normalize_name(name, keep_class=False)
    if norm in spdr_name_to_isin:
        return spdr_name_to_isin[norm]
    if country:
        for plen in [15, 10, 8, 6, 4]:
            if len(norm) >= plen:
                key = (norm[:plen], country)
                if key in spdr_prefix_country:
                    return spdr_prefix_country[key]
    return None
```
