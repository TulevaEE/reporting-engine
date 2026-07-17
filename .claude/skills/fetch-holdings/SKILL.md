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

### Proposed equity model portfolio ETFs

| ETF | ISIN | Source | Has ISINs? | Notes |
|-----|------|--------|-----------|-------|
| Amundi MSCI USA Screened | IE000F60HVH9 | Amundi XLSX | Yes | openpyxl fails on stylesheet — parse via `zipfile` + raw XML (see below) |
| Xtrackers MSCI USA Screened | IE00BJZ2DC62 | DWS XLSX | Yes | Skip 3 rows, header row 4, column C is ISIN, column K is Weighting (0–1 scale, multiply by 100) |
| iShares MSCI USA Screened (SASU) | IE00BFNM3G45 | iShares CSV | **No** | Standard iShares format. Bridge to ISIN via SPDR name matching |
| Amundi MSCI World Ex USA Screened | FR0013209921 | Amundi XLSX | Yes | Same XML parsing as Amundi USA |
| iShares MSCI EM IMI Screened (SAEM) | IE00BFNM3P36 | iShares CSV | **No** | ~2,650 stocks (IMI includes small-caps). Many won't match ACWI — expected |

### TKF100 "Alt B" model portfolio ETFs (locked 2026-07-13)

The chosen redesign: 4 MSCI World Screened + 1 Vanguard global all-cap + Invesco EM. Weights: World funds 19.27% each, Vanguard 12.00%, Invesco EM 10.92% (EM pinned to 12.18% look-through). Benchmark = MSCI ACWI, plus **ACWI IMI** (SPDR SPYI) for the all-cap Vanguard sleeve.

| ETF | ISIN | Source | Has ISINs? | Notes |
|-----|------|--------|-----------|-------|
| iShares MSCI World Screened (SAWD) | IE00BFNM3J75 | iShares CSV | **No** | Standard iShares format. Name→ISIN bridge via SPDR |
| Amundi MSCI World Screened | IE000QWCYQT0 | Amundi XLSX | Yes | `parse_amundi_holdings()` raw-XML method (broken stylesheet). Launched Oct 2025 |
| Xtrackers MSCI World Screened 1C | IE000I9HGDZ3 | DWS XLSX | Yes | Skip 3 rows, header row 4, column C is ISIN, column K is Weighting (×100). German (`de-de`) file parses fine — read by column position |
| BNP Easy MSCI World (ESG Filtered) Min TE | IE000W8HP9L8 | BNP XLSX | Yes | `engine='calamine'`, skip 22 rows, filter `Asset_Class == 'Equity'`. **Sampled** — some large names missing (genuine underweights) |
| Vanguard ESG Global All Cap | IE00BNG8L278 | Vanguard XLSX | **No** | Ticker+Name only → bridge via SPDR. All-cap (incl. small + EM) → small-caps miss standard ACWI; score vs **ACWI IMI**. ~10.5% EM internally |
| Invesco MSCI EM Universal Screened | IE00BMDBMY19 | Invesco XLSX | Yes | Prefer provider XLSX over companiesmarketcap scrape (scrape ~93% coverage inflates AS 3–5pp) |

**Resolved download URLs** (these portals geoblock / JS-render — exact locale matters; the file itself downloads via an in-page button, not a static link):

- **iShares** (SAWD, prod 305419): `https://www.ishares.com/uk/individual/en/products/305419/ishares-msci-world-esg-screened-ucits-etf-usd-acc-fund` → Holdings → Download CSV
- **Amundi**: `https://www.amundietf.nl/en/professional/products/equity/amundi-msci-world-screened-ucits-etf-acc/ie000qwcyqt0` → Documents
- **Xtrackers/DWS**: `https://etf.dws.com/en-no/IE000I9HGDZ3-msci-world-screened-ucits-etf-1c/` → Downloads → Index constituents. (`en-no` = English; `en-ie`/`en-gb` → 404, `en-lu` → 403, `de-de` → German but parses fine)
- **BNP**: `https://www.bnpparibas-am.com/en-lu/fundsheet/equity/bnp-paribas-easy-msci-world-min-te-ucits-etf-c-ie000w8hp9l8/?tab=documents` → Documents (holdings are under **Documents** here, not Portfolio data; use `en-lu`, NO investor-type segment, short slug; fallback slug inserts `-esg-filtered`)
- **Vanguard**: `https://www.vanguard.co.uk/professional/product/etf/equity/9470/esg-global-all-cap-ucits-etf-usd-accumulating` → Portfolio → holdings
- **Invesco EM**: `https://www.invesco.com/lu/en/financial-products/etfs/invesco-msci-emerging-markets-universal-screened-ucits-etf-acc.html` → Holdings XLSX
- **Benchmark ACWI IMI** (SPDR SPYI): same SSGA path as SPYY, swap `spyy`→`spyi` in the filename

### Parsing Amundi XLSX files

Amundi XLSX files have broken XML stylesheets that crash `openpyxl`. Parse via raw XML extraction:

```python
import zipfile, re, html

def parse_amundi_holdings(fpath):
    """Parse Amundi fund holdings XLSX via raw XML (bypasses broken stylesheet)."""
    with zipfile.ZipFile(fpath) as z:
        # Read shared strings
        ss = []
        if 'xl/sharedStrings.xml' in z.namelist():
            for si in re.findall(r'<si>(.*?)</si>',
                    z.read('xl/sharedStrings.xml').decode('utf-8'), re.DOTALL):
                ss.append(''.join(re.findall(r'<t[^>]*>([^<]*)</t>', si)))
        # Read sheet rows
        sheet_xml = z.read('xl/worksheets/sheet1.xml').decode('utf-8')
        rows = []
        for rxml in re.findall(r'<row[^>]*>(.*?)</row>', sheet_xml, re.DOTALL):
            d = {}
            for ref, attrs, val in re.findall(
                    r'<c\s+r="([^"]+)"([^>]*)><v>([^<]*)</v></c>', rxml):
                col = re.match(r'([A-Z]+)', ref).group(1)
                if 't="s"' in attrs:
                    d[col] = ss[int(val)] if int(val) < len(ss) else ''
                else:
                    try: d[col] = float(val)
                    except: d[col] = val
            if d: rows.append(d)
    # Find header row, parse holdings
    hi = next(i for i, r in enumerate(rows) if 'ISIN code' in r.values())
    # Columns: B=ISIN, C=Name, D=Asset class, E=Currency, F=Weight (0-1), G=Sector, H=Country
    holdings = {}
    for r in rows[hi+1:]:
        isin = html.unescape(str(r.get('B', '')))
        if len(isin) == 12 and r.get('D', '') == 'EQUITY':
            holdings[isin] = float(r.get('F', 0)) * 100  # Convert 0.09 → 9%
    return holdings
```

### Prebuilt mapping files

Two mapping files in `reports/adhoc/data/` speed up cross-provider matching:

- **`isin_to_stockid.json`** — 1,698 ISIN → `Ticker|Location` mappings. Built from `isin_ticker_map.json` + matched Amundi/Xtrackers holdings. Use when bridging ISIN-based funds to `Ticker|Location`-based benchmarks (e.g. legacy SSAC).
- **`provider_name_mappings.json`** — 705 fuzzy name mappings per provider (Amundi USA, Xtrackers USA, Amundi World Ex USA). Maps provider-specific names to SSAC `stock_id`. Use as fallback when ISIN matching isn't available.

For ISIN-based active share (preferred), neither file is needed — join directly on ISIN against SPDR ACWI.

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

## Data refresh workflow for active share analysis

When running a new active share analysis, download fresh data on the **same day** to ensure dates match:

### 1. Download ACWI benchmark

Download SPDR MSCI ACWI (SPYY) daily holdings XLSX:
```
https://www.ssga.com/uk/en_gb/intermediary/etfs/library-content/products/fund-data/etfs/emea/holdings-daily-emea-en-spyy-gy.xlsx
```
- Updated daily. Check row 4 for "Holdings As Of:" date.
- Save to working directory (not committed to repo — large file, changes daily).

### 2. Download fund holdings (same day)

| Fund | Where to download |
|------|-------------------|
| **Amundi USA Screened** (IE000F60HVH9) | amundietf.com → product page → Documents → Fund Holdings XLSX |
| **Xtrackers USA Screened** (IE00BJZ2DC62) | etf.dws.com → product page → Documents → Constituent XLSX |
| **iShares USA Screened** (SASU, IE00BFNM3G45) | ishares.com → product page → Holdings → Download CSV |
| **Amundi World Ex USA Screened** (FR0013209921) | amundietf.com → product page → Documents → Fund Holdings XLSX |
| **iShares EM IMI Screened** (SAEM, IE00BFNM3P36) | ishares.com → product page → Holdings → Download CSV |

For the **TKF100 "Alt B" model portfolio** funds (iShares/Amundi/Xtrackers/BNP World Screened + Vanguard ESG Global All Cap + Invesco EM), use the resolved download URLs in the [Alt B data-source table above](#tkf100-alt-b-model-portfolio-etfs-locked-2026-07-13) — plus the ACWI IMI benchmark (SPDR SPYI) for the all-cap Vanguard sleeve. Download all on the **same day** as the benchmark.

### 3. Match and compute

**Preferred: ISIN-based matching** (no mapping files needed)
1. Load SPDR ACWI — `pd.read_excel(..., skiprows=5)`, filter equities by ISIN regex, `groupby('ISIN')` to aggregate
2. Load Amundi XLSX — use `parse_amundi_holdings()` (see above) — returns `{ISIN: weight}`
3. Load Xtrackers XLSX — `pd.read_excel(..., header=3)`, ISIN in column C, weight in column K (×100)
4. Load iShares CSVs — bridge names to ISINs via SPDR (see two-pass bridge above)
5. Build portfolio: for each fund, scale stock weights by allocation percentage, aggregate by ISIN
6. Compute: `Active Share = ½ × Σ|w_portfolio[i] − w_benchmark[i]|` over all ISINs

**Fallback: name-based matching** (when using SSAC benchmark)
- Use `provider_name_mappings.json` for Amundi/Xtrackers → SSAC `stock_id` matching
- Use `isin_to_stockid.json` to bridge remaining ISINs to `Ticker|Location`

### 4. Validate

- Total matched weight should be >99% for ISIN-bearing funds (Amundi, Xtrackers)
- iShares bridging: >98% for USA (SASU), ~40-50% for EM IMI (SAEM) — the rest are small-caps not in ACWI
- Check benchmark date matches fund holdings dates (within 1-2 days)
- "Weight added" (portfolio-only stocks) should be <0.5% — higher signals matching bugs
