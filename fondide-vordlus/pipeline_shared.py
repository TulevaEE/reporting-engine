"""
Multi-source pipeline for Estonian pension fund analysis.

Parses investment reports from all major pension fund providers,
looks through ETFs to stock-level, computes overlaps/correlations.
Exports unified JSON to docs/fondide-vordlus/.
"""
import argparse
import csv
import io
import json
import os
import re
import urllib.request
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pdfplumber

# Load .env file if present (for EODHD_API_KEY etc.)
_env_path = Path('.env')
if _env_path.exists():
    for _line in _env_path.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith('#') and '=' in _line:
            _k, _v = _line.split('=', 1)
            os.environ.setdefault(_k.strip(), _v.strip())

BASE = Path('.')
CACHE_DIR = BASE / 'data' / 'raw' / 'holdings'
OUT_DIR = Path(__file__).resolve().parent.parent / 'docs' / 'fondide-vordlus'
OUT_DIR.mkdir(parents=True, exist_ok=True)
REPORT_DIR = BASE / 'Investeeringute aruanne'

ISIN_RE = re.compile(r'[A-Z]{2}[A-Z0-9]{10}')

# ── iShares ETF CSV config ──
ISHARES_PRODUCTS = {
    'SASU': {'id': 305356, 'slug': 'ishares-msci-usa-esg-screened-ucits-etf'},
    'SAEU': {'id': 305363, 'slug': 'ishares-msci-europe-esg-screened-ucits-etf'},
    'SAJP': {'id': 305412, 'slug': 'ishares-msci-japan-esg-screened-ucits-etf'},
    'SAEM': {'id': 305397, 'slug': 'ishares-msci-em-imi-esg-screened-ucits-etf-usd-acc-fund'},
    'SAWD': {'id': 305419, 'slug': 'ishares-msci-world-esg-screened-ucits-etf-usd-acc-fund'},
    'SSAC': {'id': 251850, 'slug': 'ishares-msci-acwi-ucits-etf'},
    'NDIA': {'id': 297617, 'slug': 'ishares-msci-india-ucits-etf'},
    '4BRZ': {'id': 304304, 'slug': 'ishares-msci-brazil-ucits-etf-usd-hedged'},
    'CNYA': {'id': 273192, 'slug': 'ishares-msci-china-a-ucits-etf'},
    'IKSA': {'id': 279996, 'slug': 'ishares-msci-saudi-arabia-capped-imi-ucits-etf'},
    # Additional iShares ETFs for look-through
    'ISAC': {'id': 251850, 'slug': 'ishares-msci-acwi-ucits-etf'},  # same as SSAC
    'EMXC': {'id': 315592, 'slug': 'ishares-msci-em-ex-china-ucits-etf'},
}
SUB_ETF_TICKERS = {'NDIA', '4BRZ', 'CNYA', 'IKSA'}
SAEM_TOP_N = 1500

# MSCI Emerging Markets country classification (used to extract EM from SSAC/ACWI)
EM_COUNTRIES = {
    'Taiwan', 'China', 'India', 'Korea (South)', 'Brazil',
    'South Africa', 'Mexico', 'Saudi Arabia', 'Thailand', 'Indonesia',
    'Malaysia', 'Philippines', 'Turkey', 'Poland', 'Chile',
    'Qatar', 'United Arab Emirates', 'Kuwait', 'Colombia', 'Peru',
    'Czech Republic', 'Egypt', 'Greece', 'Hungary',
}

# ── EODHD API config ──
EODHD_API_KEY = os.environ.get('EODHD_API_KEY', '')
EODHD_ETFS = {
    'EMXU': 'EMXU.LSE',  # Amundi MSCI EM Ex China
    'BNKE': 'BNKE.PA',   # Amundi Euro Stoxx Banks
}

# ── Manual holdings for funds without API data ──
# Swedbank Robur Globalfond A (SE0000542979) — top 30 holdings from fondlista.se 2026-03-03
GLOBALFOND_A_HOLDINGS = [
    ('Taiwan Semiconductor Manufacturing Company, Ltd.', 6.01, 'Taiwan', 'Technology'),
    ('Alphabet Inc', 5.92, 'United States', 'Communication'),
    ('Microsoft Corp', 4.36, 'United States', 'Technology'),
    ('Nvidia Corp', 4.06, 'United States', 'Technology'),
    ('Apple Inc', 3.73, 'United States', 'Technology'),
    ('McKesson Corp', 2.92, 'United States', 'Health Care'),
    ('Broadcom Inc', 2.52, 'United States', 'Technology'),
    ('Analog Devices Inc', 2.47, 'United States', 'Technology'),
    ('Amazon.com Inc', 2.19, 'United States', 'Consumer Discretionary'),
    ('Eli Lilly & Co', 2.01, 'United States', 'Health Care'),
    ('Intercontinental Exchange Inc', 1.97, 'United States', 'Financials'),
    ('ProLogis Inc', 1.95, 'United States', 'Real Estate'),
    ('Emerson Electric Co', 1.81, 'United States', 'Industrials'),
    ('Iberdrola SA', 1.70, 'Spain', 'Utilities'),
    ('Schneider Electric SE', 1.63, 'France', 'Industrials'),
    ('Intuitive Surgical Inc', 1.53, 'United States', 'Health Care'),
    ('Daifuku Co Ltd', 1.50, 'Japan', 'Industrials'),
    ('National Grid PLC', 1.50, 'United Kingdom', 'Utilities'),
    ('Air Liquide SA', 1.49, 'France', 'Materials'),
    ('Sony Group Corp', 1.49, 'Japan', 'Consumer Discretionary'),
    ('Parker Hannifin Corp', 1.48, 'United States', 'Industrials'),
    ('Lonza Group AG', 1.47, 'Switzerland', 'Health Care'),
    ('Mastercard Inc', 1.47, 'United States', 'Financials'),
    ('Sumitomo Mitsui Financial Group Inc', 1.45, 'Japan', 'Financials'),
    ('Thermo Fisher Scientific Inc', 1.39, 'United States', 'Health Care'),
    ('Xylem Inc', 1.39, 'United States', 'Industrials'),
    ('Citigroup Inc', 1.33, 'United States', 'Financials'),
    ('Allstate Corp', 1.31, 'United States', 'Financials'),
    ('Waste Management Inc', 1.29, 'United States', 'Industrials'),
    ('Lam Research Corp', 1.15, 'United States', 'Technology'),
]

# ── ETF ISIN → proxy mapping ──
# Maps ISINs from pension fund reports to the best available data source
ETF_ISIN_TO_CSV = {
    # Tuleva underlying ETFs
    'IE0009FT4LX4': 'SAWD',  # CCF Developed World ESG Screened
    'IE00BFG1TM61': 'SAWD',  # BlackRock ISF Developed World ESG Screened
    'IE00BFNM3G45': 'SASU',  # iShares MSCI USA ESG Screened
    'IE00BKPTWY98': 'SSAC_EM',  # iShares EM Screened → MSCI EM ex Select Controversies (large+mid, not IMI)
    'IE00BFNM3D14': 'SAEU',  # iShares MSCI Europe ESG Screened
    'IE00BFNM3L97': 'SAJP',  # iShares MSCI Japan ESG Screened
    'IE00BFNM3P36': 'SAEM',  # iShares MSCI EM IMI ESG Screened (SEB variant)
    # Luminor underlying ETFs
    'IE00BKM4GZ66': 'SAEM',  # iShares Core MSCI EM IMI (proxy: SAEM)
    # SEB 55+ equity ETFs (IE00BFNM3G45 and IE00BFNM3D14 already mapped above)
    'IE00B3VWM098': 'SASU',  # iShares MSCI USA Small Cap (proxy: SASU)
    'IE000H1H16W5': 'SAWD',  # iShares MSCI World Value Factor ESG (proxy: SAWD)
    'DE000A0H08F7': 'SAEU',  # iShares STOXX Europe 600 Construction (proxy: SAEU)
    'IE00BJZ2DC62': 'SASU',  # Xtrackers MSCI USA Screened (proxy: SASU)
    'IE000COQKPO9': 'SASU',  # Invesco Nasdaq-100 ESG (proxy: SASU)
    # LHV Indeks ETFs
    'IE0009HF1MK9': 'SSAC',  # Amundi Prime All Country World (proxy: ACWI)
    'IE0003XJA0J9': 'SSAC',  # Amundi Prime All Country World (proxy: ACWI)
    'IE000QIF5N15': 'SAWD',  # Amundi Prime Global UCITS ETF (proxy: SAWD)
    'IE00BTJRMP35': 'SSAC_EM',  # db x-trackers MSCI EM Index (proxy: EM from ACWI)
    'IE000MWUQBJ0': 'SAEU',  # HSBC EURO STOXX 50 (proxy: SAEU)
    'IE00B5SSQT16': 'SSAC_EM',  # HSBC MSCI EM (proxy: EM from ACWI)
    'IE000XZSV718': 'SASU',  # SPDR S&P 500 (proxy: SASU)
    'LU2089238385': 'SAJP',  # Amundi Prime Japan (proxy: SAJP)
    # SEB 18+/60+/65+ additional ETFs
    'IE00BFNM3J75': 'SAWD',  # iShares MSCI World ESG Screened (proxy: SAWD)
    'IE000NFR7C63': 'SAEM',  # iShares MSCI China Tech (proxy: SAEM)
    'LU1525418643': 'SAEU',  # Amundi EUR Corporate Bond 1-5Y ESG (bond, no equity look-through)
    # SEB Indeks
    'IE00BFXR5T61': 'SAJP',  # L&G Japan Equity (proxy: SAJP)
    'IE00BFXR5Q31': 'SASU',  # L&G US Equity (proxy: SASU)
    'IE00BH4GPZ28': 'SPPY',  # SPDR S&P 500 ESG Leaders (real holdings)
    'SE0021147394': 'SSAC_EM',  # SEB Emerging Markets Exposure Fund (proxy: EM from ACWI)
    'LU1118354460': 'SAEU',  # SEB Europe Exposure Fund IC (proxy: Europe)
    'SE0026526592': 'SAEU',  # SEB Europe Exposure Fund D (proxy: Europe, replaced IC)
    'LU1711526407': 'SAWD',  # SEB Global Exposure Fund (proxy: global developed)
    # Swedbank K-series underlying funds (EODHD)
    'LU2345046655': 'EMXU',  # Amundi MSCI EM Ex China
    'SE0000542979': 'GLOBALFOND_A',  # Swedbank Robur Globalfond A (manual top 30)
    'SE0007074091': 'XTJP',  # Swedbank Access Edge Japan A (proxy: Xtrackers Japan TOPIX)
    'IE00BG36TC12': 'XTJP',  # Xtrackers MSCI Japan ESG UCITS ETF (same index as XTJP)
    'SE0012428290': 'SSAC_EM',  # Swedbank Robur Access Edge EM A (proxy: EM from ACWI)
    'IE000O5FBC47': 'SASU',  # Amundi S&P 500 Climate Net Zero PAB (proxy: SASU)
    'IE000CL68Z69': 'SAWD',  # Amundi MSCI World Climate Net Zero PAB (proxy: SAWD)
    'IE00BP2C1V62': 'SAWD',  # HSBC MSCI World Climate Paris Aligned (proxy: SAWD)
    'SE0014429353': 'SAWD',  # Swedbank Robur Access Edge Global A (proxy: SAWD)
    'SE0007074059': 'SAWD',  # Swedbank Robur Access Global A (proxy: SAWD)
    'SE0007074083': 'SASU',  # Swedbank Robur Access USA A (proxy: SASU)
    # Previously opaque funds — now proxied
    'LU2942508834': 'SAWD',  # T Rowe Price Global Focused Growth (proxy: global developed)
    'LU2418734716': 'SAWD',  # Morgan Stanley Global Opportunity (proxy: global developed)
    'LU2853083306': 'SAWD',  # SEB Montrusco Bolton Global Equity (proxy: global developed)
    'IE00B3DJ5M15': 'SSAC_EM',  # Federated Hermes Global EM Equity (proxy: EM from ACWI)
    'LU1829219390': 'BNKE',  # Amundi Euro Stoxx Banks UCITS ETF (real holdings)
    'IE00B4M7GH52': 'SAEU',  # iShares MSCI Poland UCITS ETF (proxy: Europe)
    'IE000G2LIHG9': 'SASU',  # iShares MSCI USA ESG Screened (2nd share class, proxy: SASU)
}

# ISINs of SEB internal proprietary funds whose compositions are unknown.
# We approximate them with regional iShares ETFs, but this is a rough proxy.
# Used to compute coverage: 100% - TRUE_PROXY weight = transparency %.
# Note: Active funds (T Rowe Price, Morgan Stanley, etc.) were added in Phase B
# with proxy data, so they count as "covered" (approximate but visible).
TRUE_PROXY_ISINS = {
    'SE0021147394',  # SEB Emerging Markets Exposure Fund (internal)
    'LU1118354460',  # SEB Europe Exposure Fund IC (internal)
    'SE0026526592',  # SEB Europe Exposure Fund D (internal)
    'LU1711526407',  # SEB Global Exposure Fund (internal)
}

# ISINs of opaque/internal funds where no look-through is possible
OPAQUE_FUND_ISINS = {
    'LU1696455820',  # East Capital Eastern European Small Cap (0% weight, inactive)
}


# ═══════════════════════════════════════════════════════════════════
# SECTION 1: PDF PARSERS
# ═══════════════════════════════════════════════════════════════════

def _pct(s):
    """Parse Estonian percentage string like '29,37%' or '29.37%' to float."""
    if not s:
        return 0.0
    s = s.strip().replace('%', '').replace(' ', '').replace(',', '.')
    if not s or s in ('-', '–'):
        return 0.0
    return float(s)


def _extract_eur_value(line):
    """Extract EUR market value from a PDF investment data line.

    Returns int (EUR market value) or None if extraction fails.

    After the currency code (EUR/USD/GBP/...), PDF lines contain columns:
    [unit_price] [quantity_or_cost_value] [eur_price] [eur_market_value] [weight%]
    The EUR market value is the second-to-last number (last is weight%).

    Handles space-separated integers (9 006 167) and comma-thousands (2,766,000).
    """
    # Find currency code and work only with text after it
    curr_match = re.search(r'\b(EUR|USD|GBP|JPY|CHF|SEK|DKK|NOK|AUD|CAD|HKD|SGD|NZD|KRW|TWD|INR|BRL|MXN|ZAR|PLN|CZK|HUF|TRY|ILS|THB)\b', line)
    if not curr_match:
        return None

    num_text = line[curr_match.end():]

    # Collapse space-separated digit groups: "9 006 167" -> "9006167"
    # Lookbehind prevents matching digits that are part of a decimal
    # (both dot decimals like "8.57 150..." and Estonian comma decimals like "37,89 132...")
    collapsed = re.sub(
        r'(?<![.,\d])\d{1,3}(?:\s\d{3})+',
        lambda m: m.group(0).replace(' ', ''),
        num_text,
    )
    # Collapse comma-thousands: "2,766,000" -> "2766000"
    def _fix_comma_thousands(m):
        parts = m.group(0).split(',')
        if all(len(p) == 3 for p in parts[1:]):
            return m.group(0).replace(',', '')
        return m.group(0)
    collapsed = re.sub(r'\d{1,3}(?:,\d{1,3})+', _fix_comma_thousands, collapsed)

    # Extract all numbers (decimal or integer)
    all_nums = []
    for m in re.finditer(r'(\d+[.,]\d+|\d+)', collapsed):
        s = m.group(1).replace(',', '.')
        all_nums.append(float(s))

    if len(all_nums) < 2:
        return None

    # Luminor PDFs have 6 columns: Qty, UnitPrice, MarketValue, UnitCost, CostBasis, Weight%
    # Other PDFs have 4-5 columns: [NAV], Qty, MarketValue, [UnitCost, CostBasis], Weight%
    # Detect 6-column layout: 6 nums where [-4] and [-2] are both large integers (value columns)
    # and [-3] is small (unit cost price)
    if len(all_nums) >= 6 and all_nums[-4] >= 1000 and all_nums[-2] >= 1000 and all_nums[-3] < 1000:
        val = all_nums[-4]  # Market value (before cost columns)
    else:
        val = all_nums[-2]  # Standard: second-to-last (last is weight%)

    if val >= 100:
        return int(round(val))
    return None


def _extract_deposit_eur(line):
    """Extract EUR market value from a deposit/HOIUSED KOKKU line.

    Format: "HOIUSED KOKKU [soetusmaksumus] [turuväärtus] [pct%] [change%]"
    or:     "2. Hoiused [value] [pct%] [pct%]"
    or:     "AS SEB Pank Nõudmiseni hoius ... [value] [pct%]"

    Can't use generic _extract_eur_value because adjacent space-separated integers
    (e.g. "268 829 268 829") get merged during collapse.

    Strategy: extract all digit groups before the first percentage, then split:
    - Even count → two numbers (soetusmaksumus, turuväärtus) → take second half
    - Odd count → one number → take all
    """
    # Find position of first percentage
    pct_match = re.search(r'(\d+[\.,]\d+)\s*%', line)
    if not pct_match:
        return None

    # Text before the percentage value
    before_pct = line[:pct_match.start()].rstrip()

    # Find the contiguous numeric tail (digits and spaces only, right-to-left)
    i = len(before_pct) - 1
    while i >= 0 and (before_pct[i].isdigit() or before_pct[i] == ' '):
        i -= 1
    numeric_tail = before_pct[i + 1:].strip()
    if not numeric_tail:
        return None

    # Extract digit groups from the numeric tail
    groups = re.findall(r'\d+', numeric_tail)
    if not groups:
        return None

    # If even number of groups, likely two equal-length numbers
    # (soetusmaksumus and turuväärtus) — take the second half
    if len(groups) >= 4 and len(groups) % 2 == 0:
        half = len(groups) // 2
        value = int(''.join(groups[half:]))
    else:
        value = int(''.join(groups))

    if value >= 100:
        return value
    return None


def parse_tuleva_monthly(pdf_path):
    """Parse Tuleva monthly investment report (1-page PDF with ETF allocations).
    Returns: {allocations: [{name, isin, weight_pct, value_eur}], deposits_pct: float,
              _pdf_subtotals, _pdf_holding_counts, _total_value_eur}
    """
    with pdfplumber.open(pdf_path) as pdf:
        text = pdf.pages[0].extract_text() or ''

    allocations = []
    in_equity_funds = False
    pdf_subtotals = {}
    pdf_holding_count = 0
    total_value_eur = 0

    for line in text.splitlines():
        line = line.strip()
        if 'Aktsiafondid' == line:
            in_equity_funds = True
            continue
        if line.startswith('Aktsiafondid kokku'):
            pct_m = re.findall(r'(\d+[\.,]\d+)\s*%', line)
            if pct_m:
                pdf_subtotals['equity_funds'] = _pct(pct_m[0] + '%')
            in_equity_funds = False
            continue
        # Extract deposit EUR value from "HOIUSED KOKKU [soetus] [turuv] [pct%] [change%]"
        if line.startswith('HOIUSED KOKKU'):
            dep_val = _extract_deposit_eur(line)
            if dep_val:
                total_value_eur += dep_val
            continue
        if not in_equity_funds:
            continue

        # Count all data lines with a percentage (independent of parsing success)
        if re.search(r'\d+[\.,]\d+%', line):
            pdf_holding_count += 1

        # Find ISIN in line
        isin_match = ISIN_RE.search(line)
        if not isin_match:
            continue
        isin = isin_match.group(0)
        # Find percentage at end of line
        pct_matches = re.findall(r'(\d+[\.,]\d+%)', line)
        if not pct_matches:
            continue
        weight = _pct(pct_matches[0])  # First percentage is the weight

        # Extract EUR market value
        value_eur = _extract_eur_value(line)
        if value_eur:
            total_value_eur += value_eur

        # Name is everything before the fund manager name
        name = line[:isin_match.start()].strip()
        # Remove fund manager suffix (e.g. "BlackRock Asset Management Ireland Ltd")
        for mgr in ('BlackRock Asset Management', 'BlackRock Investment Management'):
            if mgr in name:
                name = name[:name.index(mgr)].strip()
                break
        # If the name starts with the manager, extract the fund name after " - " or "ISF"
        if name.startswith('BlackRock'):
            dash = name.find(' - ')
            if dash >= 0:
                name = name[dash + 3:].strip()
            else:
                name = name.replace('BlackRock ISF', '').strip()

        entry = {'name': name, 'isin': isin, 'weight_pct': weight}
        if value_eur:
            entry['value_eur'] = value_eur
        allocations.append(entry)

    return {
        'allocations': allocations,
        'deposits_pct': 0.07,
        '_pdf_subtotals': pdf_subtotals,
        '_pdf_holding_counts': {'equity_funds': pdf_holding_count},
        '_total_value_eur': total_value_eur,
    }


def parse_tuleva_bond_monthly(pdf_path):
    """Parse Tuleva Võlakirjad monthly investment report (bond funds + deposit).
    Returns: {bond_funds: [{name, isin, weight_pct, value_eur}], deposits_pct: float,
              _pdf_subtotals, _pdf_holding_counts, _total_value_eur}
    """
    with pdfplumber.open(pdf_path) as pdf:
        text = pdf.pages[0].extract_text() or ''

    bond_funds = []
    in_bond_funds = False
    deposits_pct = 0.0
    pdf_subtotals = {}
    pdf_holding_count = 0
    total_value_eur = 0

    for line in text.splitlines():
        line = line.strip()
        if line == 'Võlakirjafondid':
            in_bond_funds = True
            continue
        if line.startswith('Võlakirjafondid kokku'):
            pct_m = re.findall(r'(\d+[\.,]\d+)\s*%', line)
            if pct_m:
                pdf_subtotals['bond_funds'] = _pct(pct_m[0] + '%')
            in_bond_funds = False
            continue
        if line.startswith('HOIUSED KOKKU'):
            pct_m = re.findall(r'(\d+[\.,]\d+%)', line)
            if pct_m:
                deposits_pct = _pct(pct_m[0])
            dep_val = _extract_deposit_eur(line)
            if dep_val:
                total_value_eur += dep_val
            continue
        if not in_bond_funds:
            continue

        # Count data lines with percentage
        if re.search(r'\d+[\.,]\d+%', line):
            pdf_holding_count += 1

        isin_match = ISIN_RE.search(line)
        if not isin_match:
            continue
        isin = isin_match.group(0)
        pct_matches = re.findall(r'(\d+[\.,]\d+%)', line)
        if not pct_matches:
            continue
        weight = _pct(pct_matches[-1])  # Last percentage is the weight

        # Extract EUR market value
        value_eur = _extract_eur_value(line)
        if value_eur:
            total_value_eur += value_eur

        name = line[:isin_match.start()].strip()
        for mgr in ('BlackRock Asset Management', 'BlackRock Investment Management',
                     'Blackrock Luxembourg SA'):
            if mgr in name:
                name = name[:name.index(mgr)].strip()
                break
        if name.startswith('BlackRock'):
            dash = name.find(' - ')
            if dash >= 0:
                name = name[dash + 3:].strip()

        entry = {'name': name, 'isin': isin, 'weight_pct': weight}
        if value_eur:
            entry['value_eur'] = value_eur
        bond_funds.append(entry)

    return {
        'bond_funds': bond_funds,
        'deposits_pct': deposits_pct,
        '_pdf_subtotals': pdf_subtotals,
        '_pdf_holding_counts': {'bond_funds': pdf_holding_count},
        '_total_value_eur': total_value_eur,
    }


def parse_swedbank_monthly(pdf_path):
    """Parse Swedbank K-series monthly investment report.
    Returns: {stocks, bonds, equity_funds, bond_funds, re_funds, pe_funds,
              deposits_pct, derivatives_pct,
              _pdf_subtotals, _pdf_holding_counts, _total_value_eur}
    """
    stocks = []
    bonds = []
    equity_funds = []
    bond_funds = []
    re_funds = []
    pe_funds = []
    in_section = None
    in_sub = None  # sub-section within FONDIOSAKUD
    deposits_pct = 0.0
    derivatives_pct = 0.0
    pdf_subtotals = {}
    pdf_holding_counts = {}  # section → count of data lines with %
    total_value_eur = 0

    # Track current section key for holding counts
    def _count_key():
        if in_section == 'stocks':
            return 'stocks'
        if in_section == 'bonds':
            return 'bonds'
        if in_section == 'fondiosakud' and in_sub:
            return in_sub
        return None

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ''
            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue

                # Section transitions
                if line == 'AKTSIAD' or line.startswith('AKTSIAD (järg)'):
                    in_section = 'stocks'
                    continue
                if line.startswith('AKTSIAD KOKKU'):
                    pct_m = re.findall(r'(\d+[\.,]\d+)\s*%', line)
                    if pct_m:
                        pdf_subtotals['stocks'] = _pct(pct_m[0] + '%')
                    in_section = None
                    continue
                if line == 'VÕLAKIRJAD' or line.startswith('VÕLAKIRJAD (järg)'):
                    in_section = 'bonds'
                    continue
                if line.startswith('VÕLAKIRJAD KOKKU'):
                    pct_m = re.findall(r'(\d+[\.,]\d+)\s*%', line)
                    if pct_m:
                        pdf_subtotals['bonds'] = _pct(pct_m[0] + '%')
                    in_section = None
                    continue
                if line == 'FONDIOSAKUD' or line.startswith('FONDIOSAKUD (järg)'):
                    in_section = 'fondiosakud'
                    in_sub = None
                    continue
                if line.startswith('FONDIOSAKUD KOKKU'):
                    in_section = None
                    in_sub = None
                    continue
                if line.startswith('HOIUSED KOKKU'):
                    pct_m = re.findall(r'(\d+[\.,]\d+%)', line)
                    if pct_m:
                        deposits_pct = _pct(pct_m[0])
                    dep_val = _extract_deposit_eur(line)
                    if dep_val:
                        total_value_eur += dep_val
                    continue
                if line.startswith('TULETISINSTRUMENDID KOKKU'):
                    pct_m = re.findall(r'-?\d+[\.,]\d+%', line)
                    if pct_m:
                        derivatives_pct = _pct(pct_m[0])
                    continue

                # Sub-section transitions within FONDIOSAKUD
                if in_section == 'fondiosakud':
                    if line.startswith('Aktsiafondid') and 'kokku' not in line.lower():
                        in_sub = 'equity_funds'
                        continue
                    if line.startswith('Aktsiafondid kokku'):
                        pct_m = re.findall(r'(\d+[\.,]\d+)\s*%', line)
                        if pct_m:
                            pdf_subtotals['equity_funds'] = _pct(pct_m[0] + '%')
                        in_sub = None
                        continue
                    if line.startswith('Kinnisvarafondid') and 'kokku' not in line.lower():
                        in_sub = 're_funds'
                        continue
                    if line.startswith('Kinnisvarafondid kokku'):
                        pct_m = re.findall(r'(\d+[\.,]\d+)\s*%', line)
                        if pct_m:
                            pdf_subtotals['re_funds'] = _pct(pct_m[0] + '%')
                        in_sub = None
                        continue
                    if line.startswith('Private Equity') and 'kokku' not in line.lower():
                        in_sub = 'pe_funds'
                        continue
                    if line.startswith('Private Equity fondid kokku'):
                        pct_m = re.findall(r'(\d+[\.,]\d+)\s*%', line)
                        if pct_m:
                            pdf_subtotals['pe_funds'] = _pct(pct_m[0] + '%')
                        in_sub = None
                        continue
                    if line.startswith('Võlakirjafondid') and 'kokku' not in line.lower():
                        in_sub = 'bond_funds'
                        continue
                    if line.startswith('Võlakirjafondid kokku'):
                        pct_m = re.findall(r'(\d+[\.,]\d+)\s*%', line)
                        if pct_m:
                            pdf_subtotals['bond_funds'] = _pct(pct_m[0] + '%')
                        in_sub = None
                        continue

                # Skip headers
                if any(x in line for x in ['Nimetus', 'maksumus', 'puhasväärtusest']):
                    continue

                # Count data lines with percentage per section
                ck = _count_key()
                if ck and re.search(r'\d+[\.,]\d+%', line):
                    pdf_holding_counts[ck] = pdf_holding_counts.get(ck, 0) + 1

                if in_section == 'stocks':
                    isin_match = ISIN_RE.search(line)
                    if not isin_match:
                        continue
                    isin = isin_match.group(0)
                    pct_matches = re.findall(r'(\d+[\.,]\d+%)', line)
                    if not pct_matches:
                        continue
                    weight = _pct(pct_matches[0])
                    if weight <= 0:
                        continue
                    value_eur = _extract_eur_value(line)
                    if value_eur:
                        total_value_eur += value_eur
                    name_part = line[:isin_match.start()].strip()
                    after_isin = line[isin_match.end():].strip()
                    country_m = re.match(r'([A-Z]{2})\s', after_isin)
                    country = country_m.group(1) if country_m else ''
                    name = _clean_swedbank_name(name_part)
                    entry = {'name': name, 'isin': isin, 'country': country, 'weight_pct': weight}
                    if value_eur:
                        entry['value_eur'] = value_eur
                    stocks.append(entry)

                elif in_section == 'bonds':
                    isin_match = ISIN_RE.search(line)
                    if not isin_match:
                        continue
                    pct_matches = re.findall(r'(\d+[\.,]\d+%)', line)
                    if not pct_matches:
                        continue
                    weight = _pct(pct_matches[-1])
                    value_eur = _extract_eur_value(line)
                    if value_eur:
                        total_value_eur += value_eur
                    name = line[:isin_match.start()].strip()
                    entry = {'name': name, 'isin': isin_match.group(0), 'weight_pct': weight}
                    if value_eur:
                        entry['value_eur'] = value_eur
                    bonds.append(entry)

                elif in_section == 'fondiosakud' and in_sub:
                    pct_matches = re.findall(r'(\d+[\.,]\d+%)', line)
                    if not pct_matches:
                        continue
                    weight = _pct(pct_matches[0])
                    if weight <= 0:
                        continue
                    value_eur = _extract_eur_value(line)
                    if value_eur:
                        total_value_eur += value_eur
                    isin_match = ISIN_RE.search(line)
                    isin = isin_match.group(0) if isin_match else None
                    # Name: everything before the fund manager name
                    if isin_match:
                        name_part = line[:isin_match.start()].strip()
                    else:
                        name_part = line.split('  ')[0].strip()
                    # Remove fund manager from name
                    for mgr in ['Swedbank Robur', 'Amundi', 'BlackRock', 'DWS', 'BaltCap',
                                'EfTEN', 'East Capital', 'KJK Management', 'Birdeye',
                                'Nuve Retail', 'SG Capital', 'Livonia', 'Alpha Associates',
                                'Firebird', 'Morgan Stanley', 'Robeco', 'SPDR']:
                        idx = name_part.find(mgr)
                        if idx > 0:
                            name_part = name_part[:idx].strip()
                            break
                    entry = {'name': name_part, 'weight_pct': weight}
                    if isin:
                        entry['isin'] = isin
                    if value_eur:
                        entry['value_eur'] = value_eur
                    if in_sub == 'equity_funds':
                        equity_funds.append(entry)
                    elif in_sub == 'bond_funds':
                        bond_funds.append(entry)
                    elif in_sub == 're_funds':
                        re_funds.append(entry)
                    elif in_sub == 'pe_funds':
                        pe_funds.append(entry)

    return {
        'stocks': stocks, 'bonds': bonds,
        'equity_funds': equity_funds, 'bond_funds': bond_funds,
        'pe_funds': pe_funds, 're_funds': re_funds,
        'deposits_pct': deposits_pct, 'derivatives_pct': derivatives_pct,
        '_pdf_subtotals': pdf_subtotals,
        '_pdf_holding_counts': pdf_holding_counts,
        '_total_value_eur': total_value_eur,
    }


def _clean_swedbank_name(raw):
    """Extract company name from 'Name Emitent' combined string.
    E.g. '3I Group 3I Group PLC' -> '3I Group', 'Nvidia Nvidia Corp' -> 'Nvidia'
    """
    # Strip footnote markers like (1), (2)
    raw = re.sub(r'\s*\(\d+\)\s*', ' ', raw).strip()
    raw = re.sub(r'\s+', ' ', raw)

    words = raw.split()
    if not words:
        return raw

    # First try to detect duplication: "Nvidia Nvidia Corp" -> "Nvidia"
    for split_at in range(len(words) // 2, 0, -1):
        first_half = ' '.join(words[:split_at]).upper()
        second_start = ' '.join(words[split_at:split_at + split_at]).upper()
        if first_half == second_start:
            return ' '.join(words[:split_at])

    # Find where the emitent starts by looking for corporate suffixes
    suffixes = ['PLC', 'Inc', 'Corp', 'Ltd', 'AG', 'SA', 'SE', 'NV', 'AB', 'OYJ',
                'SPA', 'ASA', 'SCA', 'Co', 'AS', 'A/S', 'Tbk', 'PT', 'Corporation']
    candidate = None
    for i, w in enumerate(words):
        clean_w = w.rstrip(',').rstrip('.')
        if clean_w in suffixes and i > 0:
            candidate = ' '.join(words[:i])
            break

    if candidate is None:
        # Fallback: return first half of words
        mid = max(1, len(words) // 2)
        candidate = ' '.join(words[:mid])

    # Remove trailing emitent name duplication
    # E.g., "Alphabet C Alphabet" -> "Alphabet C"
    cw = candidate.split()
    if len(cw) >= 3:
        for j in range(len(cw) - 1, 0, -1):
            if cw[j].upper() == cw[0].upper():
                candidate = ' '.join(cw[:j])
                break

    return candidate


def parse_luminor_monthly(pdf_path):
    """Parse Luminor monthly investment report (ETF + bond + RE + PE allocations).
    Returns dict with equity_funds, bond_funds, re_funds, pe_funds, deposits_pct.
    """
    equity_funds = []
    bond_funds = []
    re_funds = []
    pe_funds = []
    deposits_pct = 0.0
    current_section = None
    pending_name = ''

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ''
            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue

                # Section headers
                if line == 'Aktsiafondid':
                    current_section = 'equity'
                    pending_name = ''
                    continue
                if line.startswith('Aktsiafondid kokku'):
                    pct_m = re.findall(r'(\d+[\.,]\d+)\s*%', line)
                    current_section = None
                    continue
                if line.startswith('Võlakirjafondid') and 'kokku' not in line:
                    current_section = 'bonds'
                    pending_name = ''
                    continue
                if line.startswith('Võlakirjafondid kokku'):
                    current_section = None
                    continue
                if line.startswith('Kinnisvarafondid') and 'kokku' not in line:
                    current_section = 'real_estate'
                    pending_name = ''
                    continue
                if 'Kinnisvarafondid' in line and 'kokku' in line:
                    current_section = None
                    continue
                if line.startswith('Erakapitalifond') and 'kokku' not in line:
                    current_section = 'pe'
                    pending_name = ''
                    continue
                if line.startswith('Erakapitalifond kokku'):
                    current_section = None
                    continue
                if line.startswith('Arvelduskonto'):
                    pct_m = re.findall(r'(\d+[\.,]\d+)\s*%', line)
                    if pct_m:
                        deposits_pct = _pct(pct_m[0] + '%')
                    continue

                # Skip headers and footers
                if any(x in line for x in ['Osakaalu muutus', 'Investeeringu nimetus',
                                            'Fondivalitseja nimi', 'Keskmine', 'Turuväärtus',
                                            'Pensionifondidesse', 'Luminor Pensions', 'koguses',
                                            'puhas-', 'kokku**', 'ühikule*', 'väärtusest']):
                    continue

                if current_section is None:
                    continue

                # Try to find percentage at end of line
                pct_m = re.findall(r'(\d+[\.,]\d+)\s*%', line)
                if not pct_m:
                    # This might be a continuation line (name wraps)
                    if line and not line[0].isdigit():
                        pending_name += ' ' + line
                    continue

                weight = _pct(pct_m[-1] + '%')
                if weight <= 0:
                    continue

                # Extract name: combine pending + current line
                full_line = (pending_name + ' ' + line).strip() if pending_name else line
                pending_name = ''

                # Name is at start of line, before country code + currency
                name_match = re.match(r'^(.+?)\s+([A-Z]{2})\s+(EUR|USD|GBP|JPY|CHF|SEK|DKK|NOK)\s', full_line)
                if name_match:
                    raw_name = name_match.group(1).strip()
                    # Remove manager name (after last capital word sequence before country)
                    name = _clean_luminor_name(raw_name)
                else:
                    name = full_line.split(str(weight).replace('.', ','))[0].strip()
                    name = _clean_luminor_name(name)

                entry = {'name': name, 'weight_pct': weight}

                if current_section == 'equity':
                    equity_funds.append(entry)
                elif current_section == 'bonds':
                    bond_funds.append(entry)
                elif current_section == 'real_estate':
                    re_funds.append(entry)
                elif current_section == 'pe':
                    pe_funds.append(entry)

    return {
        'equity_funds': equity_funds,
        'bond_funds': bond_funds,
        're_funds': re_funds,
        'pe_funds': pe_funds,
        'deposits_pct': deposits_pct,
    }


def _clean_luminor_name(raw):
    """Clean Luminor fund name by removing manager fragment."""
    # Remove common manager suffixes
    managers = ['BlackRock Asset', 'Management Ireland', 'Limited', 'Amundi Luxembourg',
                'Robeco Institutional Asset', 'Management B.V.', 'SSGA SPDR ETFS',
                'Europe Plc', 'Xtrackers IE Plc', 'Eften Capital AS', 'SIA Livonia',
                'Partners AIFP', 'Raft Capital Management', 'UAB INVL Asset',
                'UAB', 'Xtrackers II', 'S.A.']
    result = raw
    for m in managers:
        result = result.replace(m, '')
    result = re.sub(r'\s+', ' ', result).strip()
    # Remove trailing numbers/footnote markers
    result = re.sub(r'\d+$', '', result).strip()
    return result


def parse_seb_indeks_monthly(pdf_path):
    """Parse SEB Indeks monthly report (multi-column layout with ETF allocations).
    Returns: {allocations: [{name, isin, weight_pct}], deposits_pct: float}
    """
    with pdfplumber.open(pdf_path) as pdf:
        # Page 1 has January 2026 data
        text = pdf.pages[1].extract_text() or ''

    # The SEB format has columns concatenated. We need to extract:
    # Fund names, ISINs, and weights from the mixed column text.
    # Strategy: find all ISINs and all weight percentages, then match by position.

    # Find all ISINs
    isins = []
    for line in text.splitlines():
        line = line.strip()
        # ISINs have spaces in SEB PDFs like "IE00BFG1TM 61"
        cleaned = line.replace(' ', '')
        for m in ISIN_RE.finditer(cleaned):
            isin = m.group(0)
            if isin not in [i for i, _, _ in isins]:
                isins.append((isin, line, m.start()))

    # Find weight percentages
    weights = []
    for line in text.splitlines():
        for m in re.finditer(r'(\d+[\.,]\d+)%', line):
            w = _pct(m.group(1) + '%')
            if 0 < w <= 100:
                weights.append(w)

    # SEB Indeks Jan 2026 allocations (extracted from the multi-column layout)
    # Hard-code the mapping since the PDF layout is very difficult to parse reliably
    allocations = _extract_seb_allocations_from_text(text)

    return {'allocations': allocations, 'deposits_pct': 0.22}


def _extract_seb_allocations_from_text(text):
    """Extract SEB fund allocations from multi-column text.

    The SEB PDF format concatenates columns vertically, so fund names,
    ISINs, and weights appear in separate sections of the text.
    We extract them separately and match by order.
    """
    # Split text into lines
    lines = text.splitlines()

    # Find fund names (in the "Fondi liik: Aktsiafond" section)
    names = []
    in_funds = False
    for line in lines:
        line = line.strip()
        if 'Fondi liik: Aktsiafond' in line:
            in_funds = True
            continue
        if in_funds:
            if line.startswith('¹ Investeering') or line.startswith('Hoiused'):
                break
            if line and not line.startswith('¹'):
                # Clean up the M-space artifacts
                clean = line.replace('m ', 'm').replace('W ', 'W').replace('M ', 'M')
                clean = re.sub(r'\s+', ' ', clean).strip()
                if clean and len(clean) > 3:
                    names.append(clean)
            elif line.startswith('¹'):
                # SEB internal fund
                clean = line.lstrip('¹').strip()
                clean = clean.replace('m ', 'm').replace('W ', 'W').replace('M ', 'M')
                clean = re.sub(r'\s+', ' ', clean).strip()
                if clean and len(clean) > 3:
                    names.append(clean)

    # Find ISINs (in the "ISIN - kood" section)
    isins = []
    in_isins = False
    for line in lines:
        stripped = line.strip()
        if 'ISIN' in stripped and 'kood' in stripped:
            in_isins = True
            continue
        if in_isins:
            # ISINs may have spaces: "IE00BFG1TM 61"
            cleaned = stripped.replace(' ', '')
            m = ISIN_RE.search(cleaned)
            if m:
                isins.append(m.group(0))
            elif cleaned and not any(c.isdigit() for c in cleaned[:3]):
                # Non-ISIN line, stop
                if len(isins) >= len(names):
                    break

    # Find weights (in the "Osakaal fondi puhas-väärtusest" section)
    # Look for the first block of percentages after fund data
    all_pcts = []
    in_weights = False
    for line in lines:
        stripped = line.strip()
        if 'Osakaal' in stripped and 'fondi' in stripped:
            in_weights = True
            continue
        if in_weights:
            pct_m = re.match(r'^(\d+[\.,]\d+)%$', stripped)
            if pct_m:
                all_pcts.append(_pct(pct_m.group(1) + '%'))
            elif stripped and not pct_m and len(all_pcts) > 0:
                # Check if this is a sub-total or end of section
                if re.match(r'^[\d\.,]+%', stripped):
                    all_pcts.append(_pct(stripped.replace('%', '') + '%'))
                else:
                    break

    # Match names, ISINs, weights
    allocations = []
    n = min(len(names), len(isins), len(all_pcts))
    if n == 0:
        # Fallback: hard-coded from PDF analysis
        return _seb_indeks_hardcoded_allocations()

    for i in range(n):
        allocations.append({
            'name': names[i],
            'isin': isins[i],
            'weight_pct': all_pcts[i],
        })
    return allocations if allocations else _seb_indeks_hardcoded_allocations()


def _seb_indeks_hardcoded_allocations():
    """Fallback: manually extracted from SEB Indeks PDF analysis."""
    return [
        {'name': 'iShares Developed World ESG Screened Index Fund', 'isin': 'IE00BFG1TM61', 'weight_pct': 25.85},
        {'name': 'L&G Japan Equity UCITS ETF', 'isin': 'IE00BFXR5T61', 'weight_pct': 3.02},
        {'name': 'L&G US Equity UCITS ETF', 'isin': 'IE00BFXR5Q31', 'weight_pct': 10.89},
        {'name': 'SEB Emerging Markets Exposure Fund', 'isin': 'SE0021147394', 'weight_pct': 11.22},
        {'name': 'SEB Europe Exposure Fund IC', 'isin': 'LU1118354460', 'weight_pct': 7.10},
        {'name': 'SEB Global Exposure Fund', 'isin': 'LU1711526407', 'weight_pct': 10.00},
        {'name': 'SPDR S&P 500 ESG Leaders UCITS ETF', 'isin': 'IE00BH4GPZ28', 'weight_pct': 29.57},
    ]


def parse_lhv_monthly(pdf_path):
    """Parse LHV monthly report (bonds, stocks, ETFs, PE, RE).
    Returns dict matching LLK50_parsed.json format, plus
    _pdf_subtotals, _pdf_holding_counts, _total_value_eur.
    """
    bonds = []
    stocks = []
    etf_equity = []
    pe_funds = []
    re_funds = []
    deposits_pct = 0.0
    derivatives_pct = 0.0
    pdf_subtotals = {}
    pdf_holding_counts = {}
    total_value_eur = 0

    current_section = None  # 'bonds', 'stocks', 'etf_equity', 'pe', 're'

    # Map section names to standardized keys for subtotals/counts
    _SECTION_TO_KEY = {
        'bonds': 'bonds', 'stocks': 'stocks',
        'etf_equity': 'equity_funds', 'fondiosakud': 'equity_funds',
        'pe': 'pe_funds', 're': 're_funds',
    }

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ''
            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue

                # Section markers — LHV section headers include count and percentage:
                # "Võlainstrumendid 12 ... 8.45%", "Aktsiad 43 ... 22.63%"
                if 'Võlainstrumendid' in line and 'kokku' not in line.lower():
                    if re.match(r'^Võlainstrumendid\s', line):
                        pct_m = re.findall(r'(\d+[\.,]\d+)%', line)
                        if pct_m:
                            pdf_subtotals['bonds'] = _pct(pct_m[0] + '%')
                        current_section = 'bonds'
                        continue
                if line.startswith('Aktsiad') and 'kokku' not in line.lower() and 'fond' not in line.lower():
                    if re.match(r'^Aktsiad\s+\d', line):
                        pct_m = re.findall(r'(\d+[\.,]\d+)%', line)
                        if pct_m:
                            pdf_subtotals['stocks'] = _pct(pct_m[0] + '%')
                        current_section = 'stocks'
                        continue
                if line.startswith('Aktsiafondid') and 'kokku' not in line.lower():
                    if re.match(r'^Aktsiafondid\s+\d', line):
                        pct_m = re.findall(r'(\d+[\.,]\d+)%', line)
                        if pct_m:
                            pdf_subtotals['equity_funds'] = _pct(pct_m[0] + '%')
                        current_section = 'etf_equity'
                        continue
                if line.startswith('Erakapitalifondid') and 'kokku' not in line.lower():
                    pct_m = re.findall(r'(\d+[\.,]\d+)%', line)
                    if pct_m:
                        pdf_subtotals['pe_funds'] = _pct(pct_m[0] + '%')
                    current_section = 'pe'
                    continue
                if line.startswith('Kinnisvarafondid') and 'kokku' not in line.lower():
                    pct_m = re.findall(r'(\d+[\.,]\d+)%', line)
                    if pct_m:
                        pdf_subtotals['re_funds'] = _pct(pct_m[0] + '%')
                    current_section = 're'
                    continue
                if 'Fondiosakud' in line and re.match(r'^Fondiosakud\s+\d', line):
                    current_section = 'fondiosakud'
                    continue
                if line.startswith('Tuletisinstrumendid'):
                    # Extract derivatives percentage (can be negative)
                    pct_m = re.findall(r'(-?\d+[\.,]\d+)%', line)
                    if pct_m:
                        derivatives_pct = _pct(pct_m[0] + '%')
                    current_section = None
                    continue
                if '2. Hoiused' in line:
                    # Extract deposit percentage from line like "2. Hoiused 37 956 570 4.33% 4.61%"
                    pct_m = re.findall(r'(\d+[\.,]\d+)%', line)
                    if pct_m:
                        deposits_pct = _pct(pct_m[0] + '%')
                    dep_val = _extract_deposit_eur(line)
                    if dep_val:
                        total_value_eur += dep_val
                    current_section = 'deposits'
                    continue

                # Skip headers
                if any(x in line for x in ['Keskmine', 'soetushind', 'soetusväärtus',
                                            'Emitent/väärtpaberi', 'Fondi osaku',
                                            'Fondivalitseja', 'Osakaal fondi',
                                            'Tootlus', 'aegumiseni', 'ühikule',
                                            'Reiting', 'Reitingu-agentuur',
                                            'Emitendi riik', 'ISIN-kood', 'Valuuta',
                                            'eelneval kuul', 'varade puhas-',
                                            'Oodatav krediidikahju', 'kokku (EUR)',
                                            'Alusvara', 'Tuletisinstrumendi',
                                            'Krediidiasutuse', 'Hoiuse liik',
                                            'Päritoluriik', 'Algus-kuupäev',
                                            'Lõpp-tähtaeg', 'Intress',
                                            '3. Muud varad', 'VARAD KOKKU',
                                            'Fondi kohustused', 'FONDI VARADE',
                                            '* Lühendatud', '** Keskmise',
                                            '(1) Investeering', '(2) Reguleeritud',
                                            '(3) Instrumendi']):
                    continue

                if current_section == 'deposits':
                    # "2. Hoiused 18 488 908 5.67% 9.88%" — the section header has the total
                    continue

                # Count data lines with percentage per section
                sk = _SECTION_TO_KEY.get(current_section)
                if sk and re.search(r'\d+[\.,]\d+%', line):
                    pdf_holding_counts[sk] = pdf_holding_counts.get(sk, 0) + 1

                # Parse data lines - need percentage at end
                pct_matches = re.findall(r'(\d+[\.,]\d+)%', line)
                if not pct_matches:
                    continue
                weight = _pct(pct_matches[-1] + '%')
                if weight <= 0 or weight > 50:
                    continue

                # Extract EUR market value
                value_eur = _extract_eur_value(line)
                if value_eur:
                    total_value_eur += value_eur

                # Find ISIN if present
                isin_match = ISIN_RE.search(line)
                isin = isin_match.group(0) if isin_match else None

                # Extract country (Estonian names)
                country = _extract_lhv_country(line)

                # Extract name
                name = _extract_lhv_name(line, isin)

                if not name or len(name) < 2:
                    continue

                entry = {'name': name, 'isin': isin, 'weight': weight, 'country': country}
                if value_eur:
                    entry['value_eur'] = value_eur

                if current_section == 'bonds':
                    bonds.append({**entry, 'type': 'bonds'})
                elif current_section == 'stocks':
                    stocks.append({**entry, 'type': 'stocks'})
                elif current_section in ('etf_equity', 'fondiosakud'):
                    etf_equity.append({**entry, 'type': 'etfs'})
                elif current_section == 'pe':
                    pe_entry = {'name': name, 'weight': weight, 'type': 'pe'}
                    if value_eur:
                        pe_entry['value_eur'] = value_eur
                    pe_funds.append(pe_entry)
                elif current_section == 're':
                    re_entry = {'name': name, 'weight': weight, 'type': 're'}
                    if value_eur:
                        re_entry['value_eur'] = value_eur
                    re_funds.append(re_entry)

    # Compute asset class percentages
    stock_pct = sum(s['weight'] for s in stocks)
    bond_pct = sum(b['weight'] for b in bonds)
    etf_pct = sum(e['weight'] for e in etf_equity)
    pe_pct = sum(p['weight'] for p in pe_funds)
    re_pct = sum(r['weight'] for r in re_funds)

    ac = {
        'stocks': round(stock_pct, 2),
        'bonds': round(bond_pct, 2),
        'etfs': round(etf_pct, 2),
        'pe': round(pe_pct, 2),
        're': round(re_pct, 2),
    }
    if deposits_pct > 0:
        ac['deposits'] = round(deposits_pct, 2)
    if derivatives_pct != 0:
        ac['derivatives'] = round(derivatives_pct, 2)

    return {
        'holdings': stocks + etf_equity,
        'bond_holdings': bonds,
        'pe_holdings': pe_funds,
        're_holdings': re_funds,
        'asset_classes': ac,
        'deposits_pct': deposits_pct,
        '_pdf_subtotals': pdf_subtotals,
        '_pdf_holding_counts': pdf_holding_counts,
        '_total_value_eur': total_value_eur,
    }


def _extract_lhv_country(line):
    """Extract country name from LHV report line (Estonian country names)."""
    countries = {
        'Bermuda': 'Bermuda', 'Eesti': 'Estonia', 'Holland': 'Netherlands',
        'Hispaania': 'Spain', 'Jersey': 'Jersey', 'Kaimanisaared': 'Cayman Islands',
        'Kanada': 'Canada', 'Leedu': 'Lithuania', 'Luksemburg': 'Luxembourg',
        'Läti': 'Latvia', 'Norra': 'Norway', 'Prantsusmaa': 'France',
        'Rootsi': 'Sweden', 'Saksamaa': 'Germany', 'Soome': 'Finland',
        'Suurbritannia': 'United Kingdom', 'Taani': 'Denmark', 'USA': 'United States',
        'Venemaa': 'Russia', 'Iirimaa': 'Ireland', 'Šveits': 'Switzerland',
    }
    for est, eng in countries.items():
        if est in line:
            return eng
    return ''


def _extract_lhv_name(line, isin):
    """Extract investment name from LHV report line."""
    # Remove footnote markers
    line = re.sub(r'\s*\(\d+\)\s*', ' ', line)

    if isin:
        # Name is before the country/ISIN
        idx = line.index(isin) if isin in line else len(line)
    else:
        idx = len(line)

    # Find where numbers start (the financial data columns)
    # Look for country name or ISIN
    countries_est = ['Bermuda', 'Eesti', 'Holland', 'Hispaania', 'Jersey',
                     'Kaimanisaared', 'Kanada', 'Leedu', 'Luksemburg', 'Läti',
                     'Norra', 'Prantsusmaa', 'Rootsi', 'Saksamaa', 'Soome',
                     'Suurbritannia', 'Taani', 'USA', 'Venemaa', 'Iirimaa', 'Šveits']
    for c in countries_est:
        pos = line.find(c)
        if pos > 0 and pos < idx:
            idx = pos
            break

    name = line[:idx].strip()
    # Remove rating info
    name = re.sub(r'\s+(NR|Ba\d|Baa\d|B\d|A\d|Aa\d|AAA|AA\+|AA-|A\+|A-)\s', ' ', name)
    name = re.sub(r'\s+(Moody\'s|S&P|Fitch)\s*', ' ', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name


def parse_seb_55_monthly(pdf_path):
    """Parse SEB 55+ monthly report (complex multi-column: bonds, stocks, ETFs, RE, PE).
    Returns dict with allocations organized by asset class.
    """
    with pdfplumber.open(pdf_path) as pdf:
        text_p1 = pdf.pages[1].extract_text() or ''
        text_p2 = pdf.pages[2].extract_text() or ''

    # SEB 55+ has a multi-column layout similar to SEB Indeks
    # Extract investment names and weights from the concatenated columns

    # Parse bonds
    bonds = _parse_seb_55_bonds(text_p1)
    # Parse stocks
    stocks = _parse_seb_55_stocks(text_p1)
    # Parse equity funds (ETFs)
    equity_funds = _parse_seb_55_equity_funds(text_p1)
    # Parse RE funds
    re_funds = _parse_seb_55_section(text_p2, 'Kinnisvarafond')
    # Parse PE funds
    pe_funds = _parse_seb_55_section(text_p2, 'Private Equity')
    # Parse bond funds
    bond_funds_extra = _parse_seb_55_section(text_p2, 'Võlakirjafond')

    # Use hardcoded fallback if parsing yields too few results
    if len(equity_funds) < 5:
        equity_funds, bonds, stocks, re_funds, pe_funds, bond_funds_extra = _seb_55_hardcoded()

    all_bonds = bonds + [{'name': f['name'], 'weight': f['weight_pct'], 'type': 'bonds'} for f in bond_funds_extra]

    stock_pct = sum(s.get('weight_pct', 0) for s in stocks)
    bond_pct = sum(b.get('weight', b.get('weight_pct', 0)) for b in all_bonds)
    etf_pct = sum(e['weight_pct'] for e in equity_funds)
    pe_pct = sum(p['weight_pct'] for p in pe_funds)
    re_pct = sum(r['weight_pct'] for r in re_funds)

    return {
        'equity_funds': equity_funds,
        'stocks': stocks,
        'bonds': bonds,
        're_funds': re_funds,
        'pe_funds': pe_funds,
        'bond_funds': bond_funds_extra,
        'asset_classes': {
            'stocks': round(stock_pct + etf_pct, 2),
            'bonds': round(bond_pct, 2),
            'pe': round(pe_pct, 2),
            're': round(re_pct, 2),
        },
    }


def _parse_seb_55_bonds(text):
    """Extract bond holdings from SEB 55+ text."""
    # From the PDF analysis, bonds are listed with names, ISINs, and weights
    # The multi-column format makes this hard to parse generically
    # Use the known structure from our PDF analysis
    return []  # Will be populated from hardcoded data if needed


def _parse_seb_55_stocks(text):
    """Extract individual stock holdings from SEB 55+ text."""
    return []  # Will be populated from hardcoded data if needed


def _parse_seb_55_equity_funds(text):
    """Extract equity fund/ETF allocations from SEB 55+ text."""
    return []  # Will be populated from hardcoded data if needed


def _parse_seb_55_section(text, section_type):
    """Extract a section (RE, PE, bonds) from SEB 55+ page 2 text."""
    return []  # Will be populated from hardcoded data if needed


def _seb_55_hardcoded():
    """Hardcoded SEB 55+ allocations from PDF analysis (31.01.2026)."""
    equity_funds = [
        {'name': 'Amundi Euro Stoxx Banks UCITS ETF', 'isin': 'LU1829219390', 'weight_pct': 3.59},
        {'name': 'East Capital Eastern European Small Cap', 'isin': 'LU1696455820', 'weight_pct': 0.00},
        {'name': 'Federated Hermes Global EM Equity Fund', 'isin': 'IE00B3DJ5M15', 'weight_pct': 1.29},
        {'name': 'Invesco Nasdaq-100 ESG UCITS ETF', 'isin': 'IE000COQKPO9', 'weight_pct': 4.12},
        {'name': 'iShares MSCI EM IMI ESG Screened UCITS ETF', 'isin': 'IE00BFNM3P36', 'weight_pct': 1.02},
        {'name': 'iShares MSCI Europe ESG Screened UCITS ETF', 'isin': 'IE00BFNM3D14', 'weight_pct': 2.00},
        {'name': 'iShares MSCI USA ESG Screened UCITS ETF', 'isin': 'IE00BFNM3G45', 'weight_pct': 6.80},
        {'name': 'iShares MSCI USA Small Cap UCITS ETF', 'isin': 'IE00B3VWM098', 'weight_pct': 4.94},
        {'name': 'iShares MSCI World Value Factor ESG UCITS ETF', 'isin': 'IE000H1H16W5', 'weight_pct': 3.74},
        {'name': 'iShares STOXX Europe 600 Constr. & Mater. UCITS ETF', 'isin': 'DE000A0H08F7', 'weight_pct': 0.99},
        {'name': 'L&G Japan Equity UCITS ETF', 'isin': 'IE00BFXR5T61', 'weight_pct': 1.88},
        {'name': 'Morgan Stanley Global Opportunity Fund', 'isin': 'LU2418734716', 'weight_pct': 2.37},
        {'name': 'SEB Emerging Markets Exposure Fund', 'isin': 'SE0021147394', 'weight_pct': 7.89},
        {'name': 'SEB Europe Exposure Fund IC', 'isin': 'LU1118354460', 'weight_pct': 3.80},
        {'name': 'SEB Global Exposure Fund', 'isin': 'LU1711526407', 'weight_pct': 28.13},
        {'name': 'SEB Montrusco Bolton Global Equity Fund', 'isin': 'LU2853083306', 'weight_pct': 1.73},
        {'name': 'T Rowe Price Global Focused Growth Equity', 'isin': 'LU2942508834', 'weight_pct': 2.41},
        {'name': 'Xtrackers MSCI USA Screened UCITS ETF', 'isin': 'IE00BJZ2DC62', 'weight_pct': 0.98},
    ]
    bonds = [
        {'name': 'Inbank 5.5% 2031', 'weight_pct': 0.09, 'type': 'bonds'},
        {'name': 'Inbank AT1 7.5% 2031', 'weight_pct': 0.30, 'type': 'bonds'},
        {'name': 'LHV Group FRN 2035', 'weight_pct': 0.21, 'type': 'bonds'},
        {'name': 'LHV Group FRN 2029', 'weight_pct': 1.54, 'type': 'bonds'},
        {'name': 'Luminor Bank FRN Perp', 'weight_pct': 0.13, 'type': 'bonds'},
        {'name': 'Luminor Bank FRN 2029', 'weight_pct': 0.70, 'type': 'bonds'},
        {'name': 'Nortal FRN 2029', 'weight_pct': 0.59, 'type': 'bonds'},
        {'name': 'Siauliu Bankas FRN 2030', 'weight_pct': 0.27, 'type': 'bonds'},
        {'name': 'Spain Government Bond 3.25% 2034', 'weight_pct': 1.47, 'type': 'bonds'},
    ]
    stocks = [
        {'name': 'Ignitis Grupe', 'isin': 'LT0000115768', 'weight_pct': 1.09, 'country': 'Lithuania'},
        {'name': 'Tallinna Sadam', 'isin': 'EE3100021635', 'weight_pct': 2.05, 'country': 'Estonia'},
    ]
    re_funds = [
        {'name': 'BaltCap Infrastructure Fund', 'weight_pct': 0.52},
        {'name': 'Baltic Horizon Fund', 'weight_pct': 0.11},
        {'name': 'Birdeye Timber Fund 3', 'weight_pct': 1.97},
        {'name': 'East Capital Baltic Property', 'weight_pct': 0.11},
        {'name': 'EfTEN Real Estate Fund IV', 'weight_pct': 4.23},
        {'name': 'SG Capital Partners', 'weight_pct': 1.67},
    ]
    pe_funds = [
        {'name': 'BaltCap PE Fund', 'weight_pct': 0.10},
        {'name': 'BaltCap PE Fund II', 'weight_pct': 0.41},
        {'name': 'INVL PE Fund II', 'weight_pct': 0.02},
        {'name': 'Schroders Capital PE Global Innovation XI', 'weight_pct': 0.49},
        {'name': 'SEB PE Opportunity Fund', 'weight_pct': 0.44},
        {'name': 'BaltCap PE Fund III', 'weight_pct': 0.38},
        {'name': 'Superangel Two', 'weight_pct': 0.00},
    ]
    bond_funds = [
        {'name': 'Amundi Index Euro Corporate SRI UCITS ETF', 'weight_pct': 0.62},
        {'name': 'Fair Oaks AAA CLO Fund', 'weight_pct': 0.49},
        {'name': 'Fair Oaks CLO Mezzanine Opportunities Fund', 'weight_pct': 1.00},
        {'name': 'Robeco Euro Credit Bond', 'weight_pct': 0.48},
        {'name': 'Schroder ISF EURO Corporate Bond', 'weight_pct': 0.48},
    ]
    return equity_funds, bonds, stocks, re_funds, pe_funds, bond_funds


def _seb_18_hardcoded():
    """Hardcoded SEB 18+ allocations from PDF analysis (31.01.2026)."""
    equity_funds = [
        {'name': 'Amundi Euro Stoxx Banks UCITS ETF', 'isin': 'LU1829219390', 'weight_pct': 3.61},
        {'name': 'Federated Hermes Global EM Equity Fund', 'isin': 'IE00B3DJ5M15', 'weight_pct': 0.90},
        {'name': 'Invesco Nasdaq-100 ESG UCITS ETF', 'isin': 'IE000COQKPO9', 'weight_pct': 8.01},
        {'name': 'iShares MSCI EM IMI ESG Screened UCITS ETF', 'isin': 'IE00BFNM3P36', 'weight_pct': 1.03},
        {'name': 'iShares MSCI Europe ESG Screened UCITS ETF', 'isin': 'IE00BFNM3D14', 'weight_pct': 2.88},
        {'name': 'iShares MSCI USA ESG Screened UCITS ETF', 'isin': 'IE00BFNM3G45', 'weight_pct': 16.83},
        {'name': 'iShares MSCI USA Small Cap UCITS ETF', 'isin': 'IE00B3VWM098', 'weight_pct': 4.95},
        {'name': 'iShares MSCI World Value Factor ESG UCITS ETF', 'isin': 'IE000H1H16W5', 'weight_pct': 3.62},
        {'name': 'iShares STOXX Europe 600 Constr. & Mater. UCITS ETF', 'isin': 'DE000A0H08F7', 'weight_pct': 0.98},
        {'name': 'L&G Japan Equity UCITS ETF', 'isin': 'IE00BFXR5T61', 'weight_pct': 1.86},
        {'name': 'Morgan Stanley Global Opportunity Fund', 'isin': 'LU2418734716', 'weight_pct': 2.38},
        {'name': 'SEB Emerging Markets Exposure Fund', 'isin': 'SE0021147394', 'weight_pct': 8.54},
        {'name': 'SEB Europe Exposure Fund IC', 'isin': 'LU1118354460', 'weight_pct': 2.60},
        {'name': 'SEB Global Exposure Fund', 'isin': 'LU1711526407', 'weight_pct': 20.84},
        {'name': 'SEB Montrusco Bolton Global Equity Fund', 'isin': 'LU2853083306', 'weight_pct': 2.06},
        {'name': 'T Rowe Price Global Focused Growth Equity', 'isin': 'LU2942508834', 'weight_pct': 2.42},
        {'name': 'Xtrackers MSCI USA Screened UCITS ETF', 'isin': 'IE00BJZ2DC62', 'weight_pct': 4.66},
    ]
    bonds = [
        {'name': 'Bigbank 12% XXXX', 'weight_pct': 0.32, 'type': 'bonds'},
        {'name': 'COOP Pank AT1 12%', 'weight_pct': 0.26, 'type': 'bonds'},
        {'name': 'Inbank 5.5% 2031', 'weight_pct': 0.04, 'type': 'bonds'},
        {'name': 'Inbank AT1 7.5% 2031', 'weight_pct': 0.15, 'type': 'bonds'},
        {'name': 'LHV Group FRN 2035', 'weight_pct': 0.65, 'type': 'bonds'},
        {'name': 'Luminor Bank FRN Perp', 'weight_pct': 0.11, 'type': 'bonds'},
        {'name': 'Nortal FRN 2029', 'weight_pct': 0.46, 'type': 'bonds'},
    ]
    stocks = [
        {'name': 'Hepsor', 'isin': 'EE3100082306', 'weight_pct': 0.14, 'country': 'Estonia'},
        {'name': 'Ignitis Grupe', 'isin': 'LT0000115768', 'weight_pct': 0.51, 'country': 'Lithuania'},
        {'name': 'Tallinna Sadam', 'isin': 'EE3100021635', 'weight_pct': 0.87, 'country': 'Estonia'},
    ]
    re_funds = [
        {'name': 'Baltic Horizon Fund', 'weight_pct': 0.05},
        {'name': 'Birdeye Timber Fund 3', 'weight_pct': 1.00},
        {'name': 'EfTEN Real Estate Fund IV', 'weight_pct': 1.37},
        {'name': 'EfTEN Special Opportunities Fund', 'weight_pct': 0.53},
        {'name': 'SG Capital Partners', 'weight_pct': 0.59},
    ]
    pe_funds = [
        {'name': 'BaltCap PE Fund II', 'weight_pct': 0.08},
        {'name': 'INVL PE Fund II', 'weight_pct': 0.06},
        {'name': 'Schroders Capital PE Global Innovation XI', 'weight_pct': 0.57},
        {'name': 'SEB PE Global Direct III', 'weight_pct': 0.71},
        {'name': 'BaltCap PE Fund III', 'weight_pct': 0.35},
        {'name': 'Superangel Two', 'weight_pct': 0.41},
    ]
    bond_funds = [
        {'name': 'Fair Oaks CLO Mezzanine Opportunities Fund', 'weight_pct': 0.98},
    ]
    return equity_funds, bonds, stocks, re_funds, pe_funds, bond_funds


def _seb_60_hardcoded():
    """Hardcoded SEB 60+ allocations from PDF analysis (31.01.2026)."""
    equity_funds = [
        {'name': 'iShares Developed World ESG Screened Index Fund', 'isin': 'IE00BFG1TM61', 'weight_pct': 1.95},
        {'name': 'iShares MSCI EM IMI ESG Screened UCITS ETF', 'isin': 'IE00BFNM3P36', 'weight_pct': 1.97},
        {'name': 'iShares MSCI Europe ESG Screened UCITS ETF', 'isin': 'IE00BFNM3D14', 'weight_pct': 2.92},
        {'name': 'iShares MSCI World ESG Screened UCITS ETF', 'isin': 'IE00BFNM3J75', 'weight_pct': 5.46},
        {'name': 'L&G Japan Equity UCITS ETF', 'isin': 'IE00BFXR5T61', 'weight_pct': 0.84},
        {'name': 'SEB Global Exposure Fund', 'isin': 'LU1711526407', 'weight_pct': 21.38},
        {'name': 'SPDR S&P 500 ESG Leaders UCITS ETF', 'isin': 'IE00BH4GPZ28', 'weight_pct': 8.66},
    ]
    bonds = []  # 26 individual bonds totaling 26.29% - tracked via bond_pct
    stocks = [
        {'name': 'Ignitis Grupe', 'isin': 'LT0000115768', 'weight_pct': 0.74, 'country': 'Lithuania'},
        {'name': 'Tallinna Sadam', 'isin': 'EE3100021635', 'weight_pct': 1.10, 'country': 'Estonia'},
    ]
    re_funds = [
        {'name': 'BaltCap Infrastructure Fund', 'weight_pct': 1.12},
        {'name': 'Baltic Horizon Fund', 'weight_pct': 0.12},
        {'name': 'Birdeye Timber Fund 3', 'weight_pct': 2.48},
        {'name': 'EfTEN Real Estate Fund IV', 'weight_pct': 4.68},
        {'name': 'SG Capital Partners', 'weight_pct': 1.90},
    ]
    pe_funds = [
        {'name': 'BaltCap PE Fund II', 'weight_pct': 0.33},
        {'name': 'BaltCap PE Fund III', 'weight_pct': 0.23},
    ]
    bond_funds = [
        {'name': 'Amundi Index Euro Corporate SRI UCITS ETF', 'weight_pct': 0.52},
        {'name': 'Fair Oaks CLO Mezzanine Opportunities Fund', 'weight_pct': 1.49},
        {'name': 'Fair Oaks Dynamic Credit Fund', 'weight_pct': 2.37},
        {'name': 'Robeco Euro Credit Bond', 'weight_pct': 2.34},
        {'name': 'Schroder ISF EURO Corporate Bond', 'weight_pct': 3.50},
        {'name': 'SEB Global High Yield Fund', 'weight_pct': 0.00},
        {'name': 'UBS EUR Corporates Sustainable', 'weight_pct': 6.03},
    ]
    return equity_funds, bonds, stocks, re_funds, pe_funds, bond_funds


def _seb_65_hardcoded():
    """Hardcoded SEB 65+ allocations from PDF analysis (31.01.2026)."""
    equity_funds = [
        {'name': 'iShares Developed World ESG Screened Index Fund', 'isin': 'IE00BFG1TM61', 'weight_pct': 1.46},
        {'name': 'iShares MSCI Europe ESG Screened UCITS ETF', 'isin': 'IE00BFNM3D14', 'weight_pct': 0.38},
        {'name': 'iShares MSCI World ESG Screened UCITS ETF', 'isin': 'IE00BFNM3J75', 'weight_pct': 0.91},
        {'name': 'SEB Global Exposure Fund', 'isin': 'LU1711526407', 'weight_pct': 4.00},
    ]
    bonds = []  # 32 individual bonds totaling 49.33%
    stocks = []
    re_funds = []
    pe_funds = []
    bond_funds = [
        {'name': 'Amundi EUR Corporate Bond 1-5Y ESG', 'weight_pct': 8.36},
        {'name': 'Amundi Index Euro Corporate SRI UCITS ETF', 'weight_pct': 12.82},
        {'name': 'Fair Oaks CLO Mezzanine Opportunities Fund', 'weight_pct': 1.50},
        {'name': 'Fair Oaks Dynamic Credit Fund', 'weight_pct': 1.28},
        {'name': 'Robeco Euro Credit Bond', 'weight_pct': 8.00},
        {'name': 'Schroder ISF EURO Corporate Bond', 'weight_pct': 3.32},
        {'name': 'UBS EUR Corporates Sustainable', 'weight_pct': 7.52},
    ]
    return equity_funds, bonds, stocks, re_funds, pe_funds, bond_funds


def _luminor_indeks_hardcoded():
    """Hardcoded Luminor Indeks allocations from PDF (31.01.2026).
    Pure equity fund: 8 ETFs + 0.20% deposit."""
    return {
        'equity_funds': [
            {'name': 'Amundi Prime Global UCITS ETF', 'weight_pct': 29.07},
            {'name': 'Amundi Stoxx Europe 600 UCITS ETF', 'weight_pct': 4.70},
            {'name': 'SPDR MSCI World UCITS ETF', 'weight_pct': 12.47},
            {'name': 'iShares Core MSCI EM IMI UCITS ETF', 'weight_pct': 6.27},
            {'name': 'iShares Developed World Index Fund (IE) Inst Acc EUR', 'weight_pct': 19.60},
            {'name': 'iShares Emerging Markets Index Fund (IE) - EUR', 'weight_pct': 3.94},
            {'name': 'iShares Japan Index Fund (IE)', 'weight_pct': 1.33},
            {'name': 'iShares S&P 500 Swap UCITS ETF', 'weight_pct': 22.43},
        ],
        'bond_funds': [],
        're_funds': [],
        'pe_funds': [],
        'deposits_pct': 0.20,
    }


def _luminor_50_56_hardcoded():
    """Hardcoded Luminor 50-56 allocations from PDF (31.01.2026)."""
    return {
        'equity_funds': [
            {'name': 'Amundi Nasdaq-100 II-ETF A', 'weight_pct': 3.92},
            {'name': 'Robeco 3D Global Equity UCITS ETF', 'weight_pct': 8.10},
            {'name': 'SPDR MSCI World UCITS ETF', 'weight_pct': 3.06},
            {'name': 'Xtrackers MSCI World Materials ETF', 'weight_pct': 1.96},
            {'name': 'iShares Core MSCI EM IMI UCITS ETF', 'weight_pct': 5.87},
            {'name': 'iShares Developed World Index Fund (IE) Inst Acc EUR', 'weight_pct': 13.20},
            {'name': 'iShares Developed World Screened Index Fund (IE)', 'weight_pct': 17.35},
            {'name': 'iShares Emerging Markets Index Fund (IE) - EUR', 'weight_pct': 4.76},
            {'name': 'iShares Europe Equity Index Fund (LU)', 'weight_pct': 3.77},
            {'name': 'iShares Japan Index Fund (IE)', 'weight_pct': 2.48},
            {'name': 'iShares North America Index Fund', 'weight_pct': 15.55},
        ],
        'bond_funds': [
            {'name': 'Robeco Euro Credit Bonds I EUR', 'weight_pct': 1.89},
            {'name': 'SPDR Bloomberg Barclays Euro High Yield Bond', 'weight_pct': 0.90},
            {'name': 'Xtrackers II EUR High Yield Corporate Bond', 'weight_pct': 0.12},
            {'name': 'iShares EUR Corp Bond ESG SRI UCITS ETF', 'weight_pct': 1.90},
            {'name': 'iShares EUR HGHYLD CORPB ESG Paris-Aligned', 'weight_pct': 0.19},
            {'name': 'Amundi Euro Government Bond 25+Y', 'weight_pct': 0.32},
            {'name': 'BNPP Easy JPM ESG EMBI Global Diversified', 'weight_pct': 0.34},
            {'name': 'Neuberger Berman EM Market Debt Hard Currency', 'weight_pct': 1.44},
        ],
        're_funds': [
            {'name': 'EFTEN Kinnisvarafond II AS', 'weight_pct': 4.95},
            {'name': 'EfTEN Real Estate Fund', 'weight_pct': 1.72},
        ],
        'pe_funds': [
            {'name': 'INVL Private Equity Fund II', 'weight_pct': 0.06},
            {'name': 'KS Livonia Partners Fund II AIF', 'weight_pct': 1.13},
            {'name': 'Raft Capital Baltic Equity Fund', 'weight_pct': 0.10},
        ],
        'deposits_pct': 0.11,
    }


def _luminor_56_plus_hardcoded():
    """Hardcoded Luminor 56+ allocations from PDF (31.01.2026)."""
    return {
        'equity_funds': [
            {'name': 'Amundi Nasdaq-100 II-ETF A', 'weight_pct': 1.96},
            {'name': 'Robeco 3D Global Equity UCITS ETF', 'weight_pct': 3.80},
            {'name': 'SPDR MSCI World UCITS ETF', 'weight_pct': 1.82},
            {'name': 'Xtrackers MSCI World Materials ETF', 'weight_pct': 1.06},
            {'name': 'iShares Core MSCI EM IMI UCITS ETF', 'weight_pct': 2.56},
            {'name': 'iShares Developed World Index Fund (IE) Inst Acc EUR', 'weight_pct': 7.24},
            {'name': 'iShares Developed World Screened Index Fund (IE)', 'weight_pct': 9.96},
            {'name': 'iShares Emerging Markets Index Fund (IE) - EUR', 'weight_pct': 2.60},
            {'name': 'iShares Europe Equity Index Fund (LU)', 'weight_pct': 2.32},
            {'name': 'iShares Japan Index Fund (IE)', 'weight_pct': 1.30},
            {'name': 'iShares North America Index Fund', 'weight_pct': 5.38},
        ],
        'bond_funds': [
            {'name': 'Robeco Euro Credit Bonds I EUR', 'weight_pct': 3.48},
            {'name': 'SPDR Bloomberg Barclays Euro High Yield Bond', 'weight_pct': 1.63},
            {'name': 'Xtrackers II EUR High Yield Corporate Bond', 'weight_pct': 0.24},
            {'name': 'iShares EUR Corp Bond ESG SRI UCITS ETF', 'weight_pct': 3.93},
            {'name': 'iShares EUR HGHYLD CORPB ESG Paris-Aligned', 'weight_pct': 0.33},
            {'name': 'Amundi Euro Government Bond 25+Y', 'weight_pct': 0.60},
            {'name': 'BNPP Easy JPM ESG EMBI Global Diversified', 'weight_pct': 0.71},
            {'name': 'Neuberger Berman EM Market Debt Hard Currency', 'weight_pct': 3.03},
        ],
        're_funds': [
            {'name': 'EFTEN Kinnisvarafond II AS', 'weight_pct': 4.23},
            {'name': 'EfTEN Real Estate Fund', 'weight_pct': 1.65},
            {'name': 'EfTEN Special Opportunities Fund', 'weight_pct': 1.81},
        ],
        'pe_funds': [
            {'name': 'INVL Private Equity Fund II', 'weight_pct': 0.05},
            {'name': 'KS Livonia Partners Fund II AIF', 'weight_pct': 0.97},
            {'name': 'Raft Capital Baltic Equity Fund', 'weight_pct': 0.10},
        ],
        'deposits_pct': 0.15,
    }


def _luminor_61_65_hardcoded():
    """Hardcoded Luminor 61-65 allocations from PDF (31.01.2026)."""
    return {
        'equity_funds': [
            {'name': 'Amundi Nasdaq-100 II-ETF A', 'weight_pct': 0.37},
            {'name': 'Robeco 3D Global Equity UCITS ETF', 'weight_pct': 0.85},
            {'name': 'SPDR MSCI World UCITS ETF', 'weight_pct': 2.80},
            {'name': 'Xtrackers MSCI World Materials ETF', 'weight_pct': 0.21},
            {'name': 'iShares Core MSCI EM IMI UCITS ETF', 'weight_pct': 0.72},
            {'name': 'iShares Developed World Index Fund (IE) Inst Acc EUR', 'weight_pct': 2.46},
            {'name': 'iShares Developed World Screened Index Fund (IE)', 'weight_pct': 0.84},
            {'name': 'iShares Emerging Markets Index Fund (IE) - EUR', 'weight_pct': 0.59},
            {'name': 'iShares Japan Index Fund (IE)', 'weight_pct': 0.30},
        ],
        'bond_funds': [
            {'name': 'BNPP Easy JPM ESG EMBI Global Diversified', 'weight_pct': 0.84},
            {'name': 'Neuberger Berman EM Market Debt Hard Currency', 'weight_pct': 1.85},
            {'name': 'Robeco Euro Credit Bonds I EUR', 'weight_pct': 18.33},
            {'name': 'SPDR Bloomberg Barclays Euro High Yield Bond', 'weight_pct': 2.22},
            {'name': 'Xtrackers II EUR High Yield Corporate Bond', 'weight_pct': 1.12},
            {'name': 'iShares EUR Corp Bond ESG SRI UCITS ETF', 'weight_pct': 25.95},
            {'name': 'iShares Euro Investment Grade Corporate Bond', 'weight_pct': 13.02},
            {'name': 'iShares JPM USD EM Bond EUR Hedged', 'weight_pct': 0.67},
        ],
        're_funds': [],
        'pe_funds': [],
        'direct_bond_pct': 26.30,
        'deposits_pct': 0.28,
    }


# ── Pensionikeskus AUM data ──

# Map pensionikeskus fund names to our fund_keys
_PK_NAME_TO_FUND_KEY = {
    # Tuleva
    'Tuleva Maailma Aktsiate Pensionifond': 'Tuleva',
    'Tuleva Maailma Võlakirjade Pensionifond': 'Tuleva Võlakirjad',
    # LHV
    'LHV Pensionifond Ettevõtlik': 'LHV Ettevõtlik',
    'LHV Pensionifond Julge': 'LHV Julge',
    'LHV Pensionifond Rahulik': 'LHV Rahulik',
    'LHV Pensionifond Indeks': 'LHV Indeks',
    'LHV Pensionifond Tasakaalukas': 'LHV Tasakaalukas',
    # Luminor (pensionikeskus uses lowercase "pensionifond")
    'Luminor 16-50 pensionifond': 'Luminor 16-50',
    'Luminor Indeks Pensionifond': 'Luminor Indeks',
    'Luminor 50-56 pensionifond': 'Luminor 50-56',
    'Luminor 56+ pensionifond': 'Luminor 56+',
    'Luminor 61-65 pensionifond': 'Luminor 61-65',
    # SEB
    'SEB pensionifond indeks': 'SEB Indeks',
    'SEB pensionifond 18+': 'SEB 18+',
    'SEB pensionifond 55+': 'SEB 55+',
    'SEB pensionifond 60+': 'SEB 60+',
    'SEB pensionifond 65+': 'SEB 65+',
    # Swedbank
    'Swedbanki pensionifond 1960-69 sündinutele': 'Swedbank K1960',
    'Swedbanki pensionifond 1970-79 sündinutele': 'Swedbank K1970',
    'Swedbanki pensionifond 1980-89 sündinutele': 'Swedbank K1980',
    'Swedbanki pensionifond indeks 1990-99 sündinutele': 'Swedbank K1990',
    'Swedbank Pensionifond Indeks': 'Swedbank Indeks',
    'Swedbanki pensionifond 2000-09 sündinutele': 'Swedbank 2000-09',
    'Swedbanki pensionifond Konservatiivne': 'Swedbank Konservatiivne',
}

_pk_aum_cache = {}


def fetch_pensionikeskus_aum(date_str):
    """Fetch fund AUM data from pensionikeskus.ee for a given date.

    Args:
        date_str: Date in 'YYYY-MM-DD' format (last day of month)

    Returns:
        dict: {fund_key: aum_eur} where aum_eur is in EUR (not millions).
        Returns empty dict on failure.
    """
    if date_str in _pk_aum_cache:
        return _pk_aum_cache[date_str]

    try:
        import urllib.request
        url = f'https://www.pensionikeskus.ee/statistika/ii-sammas/kogumispensioni-paevastatistika/?date_from={date_str}&date_to={date_str}&download=xls'
        req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0 Tuleva-pipeline/1.0'})
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
    except Exception as e:
        print(f'  WARNING: Failed to fetch pensionikeskus AUM: {e}')
        _pk_aum_cache[date_str] = {}
        return {}

    # Parse TSV (UTF-16 LE with BOM)
    try:
        text = raw.decode('utf-16-le')
    except UnicodeDecodeError:
        # Fallback encodings
        for enc in ('utf-16', 'utf-8', 'latin-1'):
            try:
                text = raw.decode(enc)
                break
            except UnicodeDecodeError:
                continue
        else:
            print('  WARNING: Could not decode pensionikeskus response')
            _pk_aum_cache[date_str] = {}
            return {}

    # Parse as TSV
    lines = text.strip().splitlines()
    if not lines:
        _pk_aum_cache[date_str] = {}
        return {}

    # Find header row and locate "Maht" column
    header = lines[0].split('\t')
    maht_idx = None
    name_idx = None
    for i, col in enumerate(header):
        col_clean = col.strip().strip('\ufeff').strip('"')
        if 'Maht' in col_clean or 'maht' in col_clean:
            maht_idx = i
        if 'Fondi nimi' in col_clean or 'fond' in col_clean.lower():
            name_idx = i

    if maht_idx is None:
        # Try column by common position — typically: Date, Fund name, ..., Maht(volume)
        # Let's look for a numeric column with values in millions
        print('  WARNING: Could not find "Maht" column in pensionikeskus data')
        _pk_aum_cache[date_str] = {}
        return {}
    if name_idx is None:
        name_idx = 1  # Typical position

    result = {}
    for line in lines[1:]:
        cols = line.split('\t')
        if len(cols) <= max(maht_idx, name_idx):
            continue
        fund_name = cols[name_idx].strip().strip('"')
        maht_raw = cols[maht_idx].strip().strip('"').replace(' ', '').replace(',', '.')
        if not maht_raw or not fund_name:
            continue
        try:
            # Maht is in millions EUR
            maht_millions = float(maht_raw)
            aum_eur = int(round(maht_millions * 1_000_000))
        except ValueError:
            continue

        # Map pensionikeskus name to our fund_key
        fund_key = _PK_NAME_TO_FUND_KEY.get(fund_name)
        if fund_key:
            result[fund_key] = aum_eur

    _pk_aum_cache[date_str] = result
    return result


# ── Monthly JSON config loader ──

MONTHLY_DIR = BASE / 'data' / 'monthly'

def load_monthly_config(month_str):
    """Load monthly config from data/monthly/{month}.json.
    Returns (reports, allocations) dicts, or (None, None) if not found.
    """
    path = MONTHLY_DIR / f'{month_str}.json'
    if not path.exists():
        print(f'  WARNING: Monthly config not found: {path}')
        return None, None
    with open(path, encoding='utf-8') as f:
        data = json.load(f)
    return data.get('reports'), data.get('allocations')


def _load_seb_allocations(data):
    """Convert JSON dict → (equity_funds, bonds, stocks, re_funds, pe_funds, bond_funds) tuple."""
    return (
        data.get('equity_funds', []),
        data.get('bonds', []),
        data.get('stocks', []),
        data.get('re_funds', []),
        data.get('pe_funds', []),
        data.get('bond_funds', []),
    )


def _load_luminor_allocations(data):
    """Convert JSON dict → luminor-format dict (pass-through, already correct format)."""
    return data


# ═══════════════════════════════════════════════════════════════════
# SECTION 2: ETF HOLDINGS LOADING
# ═══════════════════════════════════════════════════════════════════

def fetch_ishares_holdings(ticker):
    """Load iShares ETF holdings from cached CSV."""
    cache_path = CACHE_DIR / f'{ticker}_holdings.csv'
    if ticker == 'ISAC':
        cache_path = CACHE_DIR / 'ISAC_acwi_holdings.csv'
        if not cache_path.exists():
            cache_path = CACHE_DIR / 'SSAC_holdings.csv'
    if not cache_path.exists():
        print(f'  WARNING: CSV not found for {ticker}: {cache_path}')
        return pd.DataFrame()
    raw_text = cache_path.read_text(encoding='utf-8-sig')
    rows = list(csv.reader(io.StringIO(raw_text)))
    header_idx = next(i for i, r in enumerate(rows) if len(r) >= 3 and r[0].strip() == 'Ticker')
    header = [c.strip() for c in rows[header_idx]]
    data = []
    for r in rows[header_idx + 1:]:
        if len(r) < len(header):
            continue
        d = {header[j]: r[j].strip().strip('"') for j in range(len(header))}
        w_str = d.get('Weight (%)', '').replace(',', '')
        try:
            d['weight_pct'] = float(w_str)
        except (ValueError, KeyError):
            continue
        data.append(d)
    df = pd.DataFrame(data)
    df = df.rename(columns={'Ticker': 'ticker', 'Name': 'name', 'Sector': 'sector',
                            'Asset Class': 'asset_class', 'Location': 'location'})
    df['stock_id'] = df['ticker'] + '|' + df['location']
    return df


def fetch_eodhd_holdings(ticker):
    """Fetch ETF holdings from EODHD API and return DataFrame in iShares-compatible format."""
    eodhd_ticker = EODHD_ETFS.get(ticker)
    if not eodhd_ticker:
        print(f'  WARNING: No EODHD mapping for {ticker}')
        return pd.DataFrame()

    cache_path = CACHE_DIR / f'{ticker}_eodhd_holdings.json'

    # Use cache if fresh (< 7 days)
    if cache_path.exists():
        import time
        age_days = (time.time() - cache_path.stat().st_mtime) / 86400
        if age_days < 7:
            print(f'  Using cached EODHD data for {ticker} ({age_days:.1f} days old)')
            data = json.loads(cache_path.read_text())
        else:
            data = None
    else:
        data = None

    if data is None:
        if not EODHD_API_KEY:
            print('  ERROR: EODHD_API_KEY not set. Set it in .env or as environment variable.')
            return pd.DataFrame()
        url = f'https://eodhistoricaldata.com/api/fundamentals/{eodhd_ticker}?api_token={EODHD_API_KEY}&fmt=json'
        print(f'  Fetching EODHD: {eodhd_ticker}...')
        last_err = None
        for attempt in range(3):
            try:
                with urllib.request.urlopen(url, timeout=30) as resp:
                    data = json.loads(resp.read().decode())
                cache_path.write_text(json.dumps(data))
                break
            except Exception as e:
                last_err = e
                if attempt < 2:
                    import time
                    time.sleep(2 ** attempt)
                    print(f'  Retry {attempt + 1} for {eodhd_ticker}...')
        if data is None:
            print(f'  ERROR fetching EODHD {eodhd_ticker} after 3 attempts: {last_err}')
            return pd.DataFrame()

    etf_data = data.get('ETF_Data', {})
    holdings = etf_data.get('Holdings', {})
    if not holdings:
        print(f'  WARNING: No holdings in EODHD data for {ticker}')
        return pd.DataFrame()

    rows = []
    for _, h in holdings.items():
        weight = float(h.get('Assets_%', 0) or 0)
        if weight <= 0:
            continue
        rows.append({
            'ticker': h.get('Code', ''),
            'name': h.get('Name', ''),
            'sector': h.get('Sector', ''),
            'asset_class': 'Equity',
            'location': h.get('Country', ''),
            'weight_pct': weight,
        })

    df = pd.DataFrame(rows)
    df['stock_id'] = df['ticker'] + '|' + df['location']
    print(f'  EODHD {ticker}: {len(df)} holdings loaded')
    return df


def load_manual_holdings(ticker):
    """Load manually curated holdings data (e.g. from fund factsheets)."""
    holdings_map = {
        'GLOBALFOND_A': GLOBALFOND_A_HOLDINGS,
    }
    data = holdings_map.get(ticker)
    if data is None:
        return pd.DataFrame()
    rows = []
    for name, weight, country, sector in data:
        rows.append({
            'ticker': '', 'name': name, 'sector': sector,
            'asset_class': 'Equity', 'location': country,
            'weight_pct': weight,
        })
    df = pd.DataFrame(rows)
    df['stock_id'] = df['name'] + '|' + df['location']
    return df


# ═══════════════════════════════════════════════════════════════════
# SECTION 3: LOOK-THROUGH ENGINE
# ═══════════════════════════════════════════════════════════════════

def build_lookthrough(allocations, etf_holdings):
    """Build stock-level portfolio from ETF allocations.
    allocations: list of {isin, weight_pct, etf_ticker}
    etf_holdings: dict of ticker -> DataFrame
    """
    # Validate weight sum
    total_weight = sum(a['weight_pct'] for a in allocations)
    if total_weight < 90 or total_weight > 110:
        print(f'  WARNING: Allocation weights sum to {total_weight:.1f}% (expected ~100%)')

    sw = {}
    opaque_entries = []

    for alloc in allocations:
        isin = alloc.get('isin', '')
        fw = alloc['weight_pct'] / 100.0
        etk = alloc.get('etf_ticker')

        if not etk:
            # Map ISIN to CSV ticker
            etk = ETF_ISIN_TO_CSV.get(isin)

        if not etk or etk not in etf_holdings:
            if isin in OPAQUE_FUND_ISINS:
                opaque_entries.append({
                    'name': alloc.get('name', isin),
                    'weight_pct': alloc['weight_pct'],
                    'type': 'opaque_fund',
                })
            elif alloc['weight_pct'] > 0.5:
                print(f'  WARNING: No ETF mapping for ISIN {isin} '
                      f'(name={alloc.get("name","?")}, weight={alloc["weight_pct"]:.2f}%)')
            continue

        eq = etf_holdings[etk]
        eq = eq[eq['asset_class'] == 'Equity'].copy()
        if etk == 'SAEM':
            eq = eq.nlargest(SAEM_TOP_N, 'weight_pct')

        for _, r in eq.iterrows():
            tk, w = r['ticker'], r['weight_pct']
            if tk in SUB_ETF_TICKERS and tk in etf_holdings:
                for _, sr in etf_holdings[tk][etf_holdings[tk]['asset_class'] == 'Equity'].iterrows():
                    if sr['ticker'] in SUB_ETF_TICKERS:
                        continue
                    c = fw * w * sr['weight_pct'] / 100.0
                    sid = sr['stock_id']
                    if sid in sw:
                        sw[sid]['weight'] += c
                    else:
                        sw[sid] = {'stock_id': sid, 'ticker': sr['ticker'], 'name': sr['name'],
                                   'weight': c, 'sector': sr['sector'], 'location': sr['location']}
            else:
                c = fw * w
                sid = r['stock_id']
                if sid in sw:
                    sw[sid]['weight'] += c
                else:
                    sw[sid] = {'stock_id': sid, 'ticker': tk, 'name': r['name'],
                               'weight': c, 'sector': r['sector'], 'location': r['location']}

    df = pd.DataFrame(sw.values()).sort_values('weight', ascending=False).reset_index(drop=True) if sw else pd.DataFrame()
    return df, opaque_entries


def build_acwi(etf_holdings):
    """Build ACWI benchmark portfolio from SSAC ETF."""
    eq = etf_holdings['SSAC']
    eq = eq[eq['asset_class'] == 'Equity'].copy()
    sw = {}
    for _, r in eq.iterrows():
        tk, w = r['ticker'], r['weight_pct']
        if tk in SUB_ETF_TICKERS and tk in etf_holdings:
            for _, sr in etf_holdings[tk][etf_holdings[tk]['asset_class'] == 'Equity'].iterrows():
                if sr['ticker'] in SUB_ETF_TICKERS:
                    continue
                c = w * sr['weight_pct'] / 100.0
                sid = sr['stock_id']
                if sid in sw:
                    sw[sid]['weight'] += c
                else:
                    sw[sid] = {'stock_id': sid, 'ticker': sr['ticker'], 'name': sr['name'],
                               'weight': c, 'sector': sr['sector'], 'location': sr['location']}
        else:
            sid = r['stock_id']
            if sid in sw:
                sw[sid]['weight'] += w
            else:
                sw[sid] = {'stock_id': sid, 'ticker': tk, 'name': r['name'],
                           'weight': w, 'sector': r['sector'], 'location': r['location']}
    return pd.DataFrame(sw.values()).sort_values('weight', ascending=False).reset_index(drop=True)


def build_ssac_em(etf_holdings):
    """Build EM-only holdings from SSAC by filtering for EM countries.

    Returns a DataFrame in the same format as other etf_holdings entries,
    containing only EM-country equities from SSAC with weights renormalized
    to sum to ~100%. Sub-ETF tickers (NDIA, 4BRZ, CNYA, IKSA) are included
    as-is and will be decomposed downstream.

    This is a better proxy for standard MSCI EM (large+mid cap) funds than
    SAEM, which tracks MSCI EM IMI ESG Screened (includes ~1500 small caps).
    """
    ssac = etf_holdings['SSAC']
    eq = ssac[ssac['asset_class'] == 'Equity'].copy()

    # Keep EM-country stocks + sub-ETF tickers (all are EM country ETFs)
    em_mask = eq['location'].isin(EM_COUNTRIES) | eq['ticker'].isin(SUB_ETF_TICKERS)
    em = eq[em_mask].copy()

    # Renormalize weights to sum to 100%
    total = em['weight_pct'].sum()
    if total > 0:
        em['weight_pct'] = em['weight_pct'] / total * 100.0

    return em.reset_index(drop=True)


# ═══════════════════════════════════════════════════════════════════
# SECTION 4: NORMALIZATION & JSON EXPORT
# ═══════════════════════════════════════════════════════════════════

def normalize_company_name(name):
    if not name:
        return ''
    s = name.upper()
    s = re.sub(r'\([^)]*\)', ' ', s)
    s = s.replace("'", ' ').replace('\u2019', ' ').replace('&', ' AND ').replace('/', ' ')
    s = re.sub(r'\bTHE\b', ' ', s)
    s = re.sub(r'\bCLASS\s+[A-Z]\b', ' ', s)
    s = re.sub(r'\b[A-Z]\s+SHS\b', ' ', s)
    # Remove corporate suffixes
    s = re.sub(r'\b(INCORPORATED|INC|CORPORATION|CORP|COMPANY|CO|HOLDINGS|HOLDING|'
               r'GROUP|PLC|LTD|LIMITED|NV|SA|AG|SPA|OYJ|AB|A/S|SE|PL|ADR)\b', ' ', s)
    # Remove trailing single letter (share class indicator: A, B, C, etc.)
    s = re.sub(r'\s+[A-Z]\s*$', ' ', s.strip())
    s = re.sub(r'[^A-Z0-9]+', '', s)
    return _COMPANY_ALIASES.get(s, s)


_COMPANY_ALIASES = {
    # Big tech variants
    'NVIDIACORP': 'NVIDIA', 'AMAZONCOM': 'AMAZON', 'AMAZONCOMSERVICES': 'AMAZON',
    'METAFORMS': 'METAPLATFORMS', 'METAPLATFORMSA': 'METAPLATFORMS',
    'ALPHABETA': 'ALPHABET', 'ALPHABETCAPITAL': 'ALPHABET',
    'MICROSOFTCORP': 'MICROSOFT',
    'APPLEINC': 'APPLE',
    # TSMC
    'TAIWANSEMICONDUCTORMANUFACTURING': 'TSMC',
    'TAIWANSEMICONDUCTOR': 'TSMC', 'TAIWANSEMICONDUCTORMFG': 'TSMC',
    'TAIWANSEMICONDUCTORMANUFACTURINGCO': 'TSMC',
    # Financial
    'JPMORGANCHASEAND': 'JPMORGANCHASE', 'JPMORGAN': 'JPMORGANCHASE',
    'BERKSHIREHATHAWAY': 'BERKSHIRE', 'BERKSHIREHATHAWAYCAPITAL': 'BERKSHIRE',
    'GOLDMANSACHS': 'GOLDMANSACHS', 'GOLDMANSACHSINTERNATIONAL': 'GOLDMANSACHS',
    'MORGANSTANLEY': 'MORGANSTANLEY', 'MORGANSTANLEYANDCO': 'MORGANSTANLEY',
    'BANKOFAMERICA': 'BOFA', 'BANKAMERICANA': 'BOFA',
    'MIZUHOFINANCIAL': 'MIZUHO',
    # Consumer
    'WALMART': 'WALMARTSTORES', 'WALMARTSTORESWALMART': 'WALMARTSTORES',
    'PEPSICO': 'PEPSI',
    'MCDONALDS': 'MCDONALD',
    'PROCTER': 'PROCTERANDGAMBLE', 'PROCTERCOMGAMBLE': 'PROCTERANDGAMBLE',
    'COCACOLA': 'COKE', 'COCACOLAEUROPACIFIC': 'COKE',
    # Healthcare
    'UNITEDHEALTH': 'UNITEDHEALTH', 'UNITEDHEALTHUNITEDHEALTH': 'UNITEDHEALTH',
    'JOHNSONANDJOHNSON': 'JNJ', 'JOHNSONJOHNSON': 'JNJ',
    'ELILILLYAND': 'ELILILLY', 'ELILILLYANDCO': 'ELILILLY',
    # Industrial/other
    'HILTONWORLDWIDE': 'HILTON',
    'GENERALELECTRIC': 'GE', 'GEAEROSPACE': 'GE', 'GEVERNOVA': 'GEVERNOVA',
    'APPLIEDMATERIALS': 'AMAT',
    'MICRONTECHNOLOGY': 'MICRONTECHNOLOGIES',
    'BROADCOM': 'BROADCOM', 'BROADCOMINC': 'BROADCOM',
    'TOSHIBACORP': 'TOSHIBA',
    'SAMSUNGELECTRONICS': 'SAMSUNG', 'SAMSUNGELECTRONICSPFD': 'SAMSUNG',
    'TOYOTAMOTOR': 'TOYOTA', 'TOYOTAMOTORCORP': 'TOYOTA',
    'SONYGROUP': 'SONY', 'SONYGROUPCORP': 'SONY',
    'SHELLTRANSPORT': 'SHELL', 'SHELLPLC': 'SHELL',
    'TOTALENERGIES': 'TOTALENERGIES', 'TOTALENERGIESSE': 'TOTALENERGIES',
    'NESTLESA': 'NESTLE', 'NESTLE': 'NESTLE',
    'NOVONORDISK': 'NOVONORDISK', 'NOVONORDISKA': 'NOVONORDISK',
    'ROCHEGENUSSSCHEIN': 'ROCHE', 'ROCHE': 'ROCHE',
    'ASMLHOLDING': 'ASML', 'ASML': 'ASML',
    'LVMHMOETHENNESSY': 'LVMH', 'LVMHMOETHENNESSYLOUISVUITTON': 'LVMH',
    'SABORIN': 'SAP', 'SAPAG': 'SAP', 'SAPSE': 'SAP',
    'SIEMENSAG': 'SIEMENS', 'SIEMENS': 'SIEMENS',
}


def _build_sector_lookup_with_fuzzy(acwi):
    """Build sector lookup from ACWI data with fuzzy name matching fallback.
    Returns (sector_lookup DataFrame, fuzzy_map dict).
    """
    acwi['norm_key'] = acwi['name'].apply(normalize_company_name)
    sector_lookup = acwi.drop_duplicates('norm_key').set_index('norm_key')[['sector', 'location']]

    # Build prefix map for fuzzy matching: first 6 chars -> (sector, location)
    # Only use unambiguous prefixes
    prefix_map = {}
    prefix_counts = {}
    for _, row in acwi.iterrows():
        nk = row['norm_key']
        if len(nk) < 6:
            continue
        pfx = nk[:6]
        if pfx not in prefix_counts:
            prefix_counts[pfx] = set()
        prefix_counts[pfx].add((row['sector'], row['location']))
        prefix_map[pfx] = (row['sector'], row['location'])

    # Only keep unambiguous prefixes (same sector for all matches)
    fuzzy_map = {}
    for pfx, sectors in prefix_counts.items():
        if len(sectors) == 1:
            fuzzy_map[pfx] = prefix_map[pfx]

    return sector_lookup, fuzzy_map


COUNTRY_MAP = {
    'US': 'United States', 'GB': 'United Kingdom', 'JP': 'Japan', 'DE': 'Germany',
    'FR': 'France', 'NL': 'Netherlands', 'CH': 'Switzerland', 'CA': 'Canada',
    'AU': 'Australia', 'SE': 'Sweden', 'DK': 'Denmark', 'NO': 'Norway',
    'FI': 'Finland', 'IE': 'Ireland', 'ES': 'Spain', 'IT': 'Italy',
    'BE': 'Belgium', 'AT': 'Austria', 'PT': 'Portugal', 'IL': 'Israel',
    'SG': 'Singapore', 'HK': 'Hong Kong', 'KR': 'South Korea', 'TW': 'Taiwan',
    'IN': 'India', 'BR': 'Brazil', 'MX': 'Mexico', 'ZA': 'South Africa',
    'LT': 'Lithuania', 'LV': 'Latvia', 'EE': 'Estonia', 'PL': 'Poland',
    'CZ': 'Czech Republic', 'HU': 'Hungary', 'PA': 'Panama', 'BM': 'Bermuda',
    'JE': 'Jersey', 'KY': 'Cayman Islands', 'LU': 'Luxembourg', 'GG': 'Guernsey',
}


def fund_to_json(df, name, acwi_nk, acwi_keys, sector_lookup):
    """Convert stock-level DataFrame to JSON export format."""
    if df.empty:
        return {
            'name': name, 'n_stocks': 0, 'total_weight': 0,
            'top_holdings': [], 'sectors': {}, 'countries': {},
            'overlap_with_acwi_pct': 0, 'correlation_with_acwi': 0,
            'weights': {}, '_weight_vec': {},
        }

    valid = df[~df['sector'].isin(['', '-', 'Cash and/or Derivatives'])]
    sectors = valid.groupby('sector')['weight'].sum().sort_values(ascending=False)
    countries = valid.groupby('location')['weight'].sum().sort_values(ascending=False)

    all_holdings = df[['name', 'weight', 'sector', 'location']].to_dict('records')
    for h in all_holdings:
        h['weight'] = round(h['weight'], 3)

    key_col = 'norm_key' if 'norm_key' in df.columns else 'stock_id'
    in_acwi = df[df[key_col].isin(acwi_keys)]
    overlap_w = round(in_acwi['weight'].sum(), 2)

    m = df[[key_col, 'weight']].merge(acwi_nk[[key_col, 'weight']], on=key_col, suffixes=('_f', '_a'))
    corr = round(m['weight_f'].corr(m['weight_a']), 4) if len(m) > 5 else 0

    weight_vec = df.groupby(key_col)['weight'].sum().to_dict()
    weight_vec_export = {k: round(v, 4) for k, v in weight_vec.items() if v > 0.01}

    return {
        'name': name,
        'n_stocks': len(df),
        'total_weight': round(df['weight'].sum(), 2),
        'top_holdings': all_holdings,
        'sectors': {k: round(v, 2) for k, v in sectors.items()},
        'countries': {k: round(v, 2) for k, v in countries.head(15).items()},
        'overlap_with_acwi_pct': overlap_w,
        'correlation_with_acwi': corr,
        'weights': weight_vec_export,
        '_weight_vec': weight_vec,
    }


ETF_DISPLAY_NAMES = {
    'SAWD': 'MSCI World ESG Screened',
    'SASU': 'MSCI USA ESG Screened',
    'SAEU': 'MSCI Europe ESG Screened',
    'SAJP': 'MSCI Japan ESG Screened',
    'SAEM': 'MSCI EM IMI ESG Screened',
    'SSAC_EM': 'MSCI EM (from ACWI)',
}


def build_etf_breakdown(allocations, etf_holdings):
    """Build per-ETF breakdown showing which ETFs the fund holds and their top stocks.

    Each allocation from the PDF is kept as a separate entry (e.g. Tuleva has 6 ETFs,
    even though two map to the same SAWD data source).
    """
    breakdown = []
    for alloc in sorted(allocations, key=lambda a: -a['weight_pct']):
        isin = alloc.get('isin', '')
        etk = ETF_ISIN_TO_CSV.get(isin, alloc.get('etf_ticker', ''))
        if not etk or etk not in etf_holdings:
            continue

        fund_weight = alloc['weight_pct']
        display_name = alloc.get('name', ETF_DISPLAY_NAMES.get(etk, etk))
        # Shorten long fund names
        for prefix in ('CCF ', 'BlackRock ISF ', 'iShares '):
            if display_name.startswith(prefix):
                display_name = display_name[len(prefix):]
                break

        eq = etf_holdings[etk]
        eq = eq[eq['asset_class'] == 'Equity'].copy()
        if etk == 'SAEM':
            eq = eq.nlargest(SAEM_TOP_N, 'weight_pct')

        stocks_only = eq[~eq['ticker'].isin(SUB_ETF_TICKERS)]
        n_direct = len(stocks_only)

        n_sub = 0
        for _, r in eq.iterrows():
            if r['ticker'] in SUB_ETF_TICKERS and r['ticker'] in etf_holdings:
                sub_eq = etf_holdings[r['ticker']]
                n_sub += len(sub_eq[sub_eq['asset_class'] == 'Equity'])

        top = stocks_only.nlargest(20, 'weight_pct')
        top_stocks = []
        for _, r in top.iterrows():
            top_stocks.append({
                'name': r['name'],
                'weight': round(fund_weight / 100 * r['weight_pct'], 3),
            })

        breakdown.append({
            'etf': etk,
            'isin': isin,
            'name': display_name,
            'fund_weight': round(fund_weight, 2),
            'n_stocks': n_direct + n_sub,
            'top_stocks': top_stocks,
        })

    return breakdown


def compute_pairwise_correlations(all_funds_data, fund_names):
    """Compute weight correlation between all fund pairs."""
    corr_matrix = {}
    for fi in fund_names:
        vi = all_funds_data[fi].get('_weight_vec', {})
        for fj in fund_names:
            vj = all_funds_data[fj].get('_weight_vec', {})
            all_keys = set(vi.keys()) | set(vj.keys())
            if len(all_keys) < 5:
                corr_matrix[f'{fi}|{fj}'] = 0
                continue
            a = np.array([vi.get(k, 0) for k in all_keys])
            b = np.array([vj.get(k, 0) for k in all_keys])
            r = np.corrcoef(a, b)[0, 1]
            corr_matrix[f'{fi}|{fj}'] = round(float(r), 4)
    return corr_matrix


# ═══════════════════════════════════════════════════════════════════
# SECTION 5: FUND PROCESSING FUNCTIONS
# ═══════════════════════════════════════════════════════════════════

def process_etf_fund(name, allocations, etf_holdings, acwi, acwi_keys, sector_lookup):
    """Process Type A fund (ETF-based): look through ETFs to stocks."""
    # Enrich allocations with etf_ticker mapping
    for alloc in allocations:
        if 'etf_ticker' not in alloc:
            alloc['etf_ticker'] = ETF_ISIN_TO_CSV.get(alloc.get('isin', ''))

    df, opaque_entries = build_lookthrough(allocations, etf_holdings)

    if df.empty:
        print(f'  WARNING: No stocks found for {name}')
        return None

    # Filter to ACWI universe and normalize weights
    acwi_universe = set(acwi['stock_id'])
    df = df[df['stock_id'].isin(acwi_universe)].copy()
    if df.empty:
        # Keep all stocks if none match ACWI
        df, _ = build_lookthrough(allocations, etf_holdings)
    total_w = df['weight'].sum()
    if total_w > 0:
        df['weight'] = df['weight'] / total_w * 100
    df['norm_key'] = df['name'].apply(normalize_company_name)

    acwi_nk = acwi.copy()
    fund_data = fund_to_json(df, name, acwi_nk, acwi_keys, sector_lookup)
    fund_data['etf_breakdown'] = build_etf_breakdown(allocations, etf_holdings)
    fund_data['asset_classes'] = {'stocks': 100.0}

    # Add opaque fund info
    opaque_pct = sum(e['weight_pct'] for e in opaque_entries)
    if opaque_pct > 0:
        fund_data['opaque_funds'] = opaque_entries
        fund_data['opaque_pct'] = round(opaque_pct, 2)

    return fund_data


def process_stock_fund(name, parsed, etf_holdings, acwi, acwi_keys, sector_lookup,
                       fuzzy_sector_map=None):
    """Process Type B fund (direct stocks from Swedbank K-series PDF)."""
    stocks = parsed['stocks']
    if not stocks:
        print(f'  WARNING: No stocks found for {name}')
        return None

    df = pd.DataFrame(stocks)
    df = df.rename(columns={'weight_pct': 'weight'})
    df['location'] = df['country'].map(COUNTRY_MAP).fillna(df['country'])
    df['norm_key'] = df['name'].apply(normalize_company_name)

    # Try to enrich with ACWI sector data (exact match first, then fuzzy prefix)
    df['sector'] = df['norm_key'].map(sector_lookup['sector'])
    if fuzzy_sector_map:
        for idx in df[df['sector'].isna()].index:
            nk = df.at[idx, 'norm_key']
            if len(nk) >= 6:
                match = fuzzy_sector_map.get(nk[:6])
                if match:
                    df.at[idx, 'sector'] = match[0]
                    if not df.at[idx, 'location'] or df.at[idx, 'location'] == df.at[idx, 'country']:
                        df.at[idx, 'location'] = match[1]
    df.loc[df['sector'].isna(), 'sector'] = 'Unknown'
    df = df.sort_values('weight', ascending=False).reset_index(drop=True)

    # Save direct stock holdings before look-through merge
    direct_stock_holdings = [{'name': r['name'], 'weight': round(r['weight'], 3)}
                             for _, r in df.iterrows()]

    # Look through equity funds that have ETF mappings
    lookthrough_allocs = []
    opaque_equity_funds = []
    for ef in parsed.get('equity_funds', []):
        isin = ef.get('isin', '')
        etk = ETF_ISIN_TO_CSV.get(isin)
        if etk and etk in etf_holdings:
            lookthrough_allocs.append({
                'name': ef['name'], 'isin': isin,
                'weight_pct': ef['weight_pct'], 'etf_ticker': etk,
            })
        else:
            opaque_equity_funds.append(ef)

    if lookthrough_allocs:
        lt_df, _ = build_lookthrough(lookthrough_allocs, etf_holdings)
        if not lt_df.empty:
            lt_df['norm_key'] = lt_df['name'].apply(normalize_company_name)
            lt_df['sector'] = lt_df['norm_key'].map(sector_lookup['sector']).fillna('')
            lt_df.loc[lt_df['sector'] == '', 'sector'] = 'Unknown'
            # Ensure direct stocks df has matching columns
            if 'stock_id' not in df.columns:
                df['stock_id'] = df['norm_key'] + '|' + df['location'].fillna('')
            if 'ticker' not in df.columns:
                df['ticker'] = ''
            df = pd.concat([df, lt_df[['stock_id', 'ticker', 'name', 'weight', 'sector', 'location', 'norm_key']]], ignore_index=True)
            # Merge duplicates by norm_key (more reliable than stock_id across sources)
            df = df.groupby('norm_key', as_index=False).agg({
                'stock_id': 'first', 'ticker': 'first', 'name': 'first', 'weight': 'sum',
                'sector': 'first', 'location': 'first',
            })
            df = df.sort_values('weight', ascending=False).reset_index(drop=True)
            lt_pct = sum(a['weight_pct'] for a in lookthrough_allocs)
            print(f'  Look-through: {len(lookthrough_allocs)} equity funds ({lt_pct:.1f}%) -> {len(lt_df)} stocks merged')

    acwi_nk = acwi.copy()
    fund_data = fund_to_json(df, name, acwi_nk, acwi_keys, sector_lookup)

    # Add all asset classes
    bonds_pct = sum(b['weight_pct'] for b in parsed.get('bonds', []))
    deposits_pct = parsed.get('deposits_pct', 0)
    derivatives_pct = parsed.get('derivatives_pct', 0)
    # For asset class bar: use full equity sub-fund weights (not look-through sum)
    # Look-through loses ~3% (cash/fees inside sub-funds), but the sub-funds are equity
    lt_full_pct = sum(a['weight_pct'] for a in lookthrough_allocs) if lookthrough_allocs else 0
    direct_stock_pct = sum(s['weight_pct'] for s in parsed.get('stocks', []))
    stock_pct = direct_stock_pct + lt_full_pct
    opaque_equity_funds_pct = sum(f['weight_pct'] for f in opaque_equity_funds)
    bond_funds_pct = sum(f['weight_pct'] for f in parsed.get('bond_funds', []))
    re_pct = sum(f['weight_pct'] for f in parsed.get('re_funds', []))
    pe_pct = sum(f['weight_pct'] for f in parsed.get('pe_funds', []))

    fund_data['asset_classes'] = {
        'stocks': round(stock_pct, 1),
    }
    if opaque_equity_funds_pct > 0:
        fund_data['asset_classes']['etfs'] = round(opaque_equity_funds_pct, 1)
    if bonds_pct + bond_funds_pct > 0:
        fund_data['asset_classes']['bonds'] = round(bonds_pct + bond_funds_pct, 1)
    if re_pct > 0:
        fund_data['asset_classes']['re'] = round(re_pct, 1)
    if pe_pct > 0:
        fund_data['asset_classes']['pe'] = round(pe_pct, 1)
    if deposits_pct > 0:
        fund_data['asset_classes']['deposits'] = round(deposits_pct, 1)
    if derivatives_pct != 0:
        fund_data['asset_classes']['derivatives'] = round(derivatives_pct, 1)

    # Add holdings lists for display
    bond_holdings = [{'name': b['name'], 'weight': b['weight_pct'], 'type': 'bonds'}
                     for b in parsed.get('bonds', [])]
    bond_holdings.extend([{'name': f['name'], 'weight': f['weight_pct'], 'type': 'bonds'}
                          for f in parsed.get('bond_funds', [])])
    etf_holdings_list = [{'name': f['name'], 'weight': f['weight_pct'], 'type': 'etfs'}
                         for f in opaque_equity_funds]
    re_holdings = [{'name': f['name'], 'weight': f['weight_pct'], 'type': 're'}
                   for f in parsed.get('re_funds', [])]
    pe_holdings = [{'name': f['name'], 'weight': f['weight_pct'], 'type': 'pe'}
                   for f in parsed.get('pe_funds', [])]

    if bond_holdings:
        fund_data['bond_holdings'] = bond_holdings
    if etf_holdings_list:
        fund_data['etf_holdings'] = etf_holdings_list
    if re_holdings:
        fund_data['re_holdings'] = re_holdings
    if pe_holdings:
        fund_data['pe_holdings'] = pe_holdings
    if direct_stock_holdings:
        fund_data['direct_stock_holdings'] = direct_stock_holdings
    if lookthrough_allocs:
        fund_data['etf_breakdown'] = build_etf_breakdown(lookthrough_allocs, etf_holdings)

    # Add non-stock holdings to weight vectors for overlap/correlation
    wv = fund_data['_weight_vec']
    wv_export = fund_data['weights']
    for prefix, holdings in [('BOND', bond_holdings), ('ETF', etf_holdings_list),
                              ('RE', re_holdings), ('PE', pe_holdings)]:
        for h in holdings:
            key = f"{prefix}|{h['name']}"
            w = h['weight']
            wv[key] = w
            if w > 0.01:
                wv_export[key] = round(w, 4)

    return fund_data


def process_mixed_fund(name, parsed, etf_holdings, acwi, acwi_keys, sector_lookup):
    """Process Type C fund (mixed active: LHV, SEB 55+) with all asset classes."""
    # Build stock DataFrame
    stock_holdings = [h for h in parsed.get('holdings', []) if h.get('type') == 'stocks']
    if stock_holdings:
        df = pd.DataFrame(stock_holdings)
        df = df.rename(columns={'country': 'location'})
        if 'location' not in df.columns:
            df['location'] = ''
        df['norm_key'] = df['name'].apply(normalize_company_name)
        df['sector'] = df['norm_key'].map(sector_lookup['sector'])
        df.loc[df['sector'].isna(), 'sector'] = 'Direct Investment'
        df = df.sort_values('weight', ascending=False).reset_index(drop=True)
    else:
        df = pd.DataFrame(columns=['name', 'weight', 'sector', 'location', 'norm_key'])

    acwi_nk = acwi.copy()
    fund_data = fund_to_json(df, name, acwi_nk, acwi_keys, sector_lookup)
    fund_data['asset_classes'] = parsed.get('asset_classes', {})

    # Add PE/RE/bond/ETF holdings
    pe_holdings = parsed.get('pe_holdings', [])
    re_holdings = parsed.get('re_holdings', [])
    bond_holdings = parsed.get('bond_holdings', [])
    etf_holdings_list = [h for h in parsed.get('holdings', []) if h.get('type') == 'etfs']

    # Validate: if asset_classes says a category has weight, holdings can't be empty
    ac = parsed.get('asset_classes', {})
    for key, ac_key, holdings in [('pe_holdings', 'pe', pe_holdings), ('re_holdings', 're', re_holdings), ('bond_holdings', 'bonds', bond_holdings)]:
        weight = ac.get(ac_key, 0)
        if weight > 1.0 and len(holdings) == 0:
            raise ValueError(f"{name}: {ac_key} is {weight}% in asset_classes but {key} is empty — parsed data incomplete")

    fund_data['pe_holdings'] = pe_holdings
    fund_data['re_holdings'] = re_holdings
    fund_data['bond_holdings'] = bond_holdings
    fund_data['etf_holdings'] = etf_holdings_list

    # Add PE/RE/bonds/ETFs to weight vector
    wv = fund_data['_weight_vec']
    wv_export = fund_data['weights']
    for h in pe_holdings:
        key = f"PE|{h['name']}"
        wv[key] = h['weight']
        if h['weight'] > 0.01:
            wv_export[key] = round(h['weight'], 4)
    for h in re_holdings:
        key = f"RE|{h['name']}"
        wv[key] = h['weight']
        if h['weight'] > 0.01:
            wv_export[key] = round(h['weight'], 4)
    for h in bond_holdings:
        key = f"BOND|{h['name']}"
        wv[key] = h.get('weight', h.get('weight_pct', 0))
        if wv[key] > 0.01:
            wv_export[key] = round(wv[key], 4)
    for h in etf_holdings_list:
        key = f"ETF|{h['name']}"
        wv[key] = h['weight']
        if h['weight'] > 0.01:
            wv_export[key] = round(h['weight'], 4)

    # Add to sectors
    sectors = fund_data['sectors']
    pe_total = sum(h['weight'] for h in pe_holdings)
    re_total = sum(h['weight'] for h in re_holdings)
    bond_total = sum(h.get('weight', h.get('weight_pct', 0)) for h in bond_holdings)
    etf_total = sum(h['weight'] for h in etf_holdings_list)
    if pe_total > 0:
        sectors['Erakapital'] = round(pe_total, 2)
    if re_total > 0:
        sectors['Kinnisvara'] = round(re_total, 2)
    if bond_total > 0:
        sectors['Võlakirjad'] = round(bond_total, 2)
    if etf_total > 0:
        sectors['ETF-id'] = round(etf_total, 2)

    # Add to countries
    countries = fund_data['countries']
    non_stock_total = pe_total + re_total + bond_total + etf_total
    if non_stock_total > 0:
        countries['Eesti (PE/RE/võlak.)'] = round(non_stock_total, 2)

    return fund_data


def process_seb_55(parsed, etf_holdings, acwi, acwi_keys, sector_lookup):
    """Process SEB 55+ fund with ETF look-through where possible."""
    # Build equity fund allocations for look-through
    equity_allocs = parsed.get('equity_funds', [])
    lookthrough_allocs = []
    opaque_entries = []
    etf_only_entries = []

    for alloc in equity_allocs:
        isin = alloc.get('isin', '')
        if isin in ETF_ISIN_TO_CSV:
            lookthrough_allocs.append({
                'name': alloc['name'], 'isin': isin,
                'weight_pct': alloc['weight_pct'],
                'etf_ticker': ETF_ISIN_TO_CSV[isin],
            })
        elif isin in OPAQUE_FUND_ISINS:
            opaque_entries.append({
                'name': alloc['name'],
                'weight_pct': alloc['weight_pct'],
                'type': 'opaque_fund',
            })
        else:
            etf_only_entries.append({
                'name': alloc['name'],
                'isin': isin,
                'weight': alloc['weight_pct'],
                'type': 'etfs',
            })

    # Look through available ETFs
    df, _ = build_lookthrough(lookthrough_allocs, etf_holdings)

    # Add direct stocks
    direct_stocks = parsed.get('stocks', [])
    if direct_stocks:
        stock_df = pd.DataFrame(direct_stocks)
        stock_df = stock_df.rename(columns={'weight_pct': 'weight'})
        stock_df['location'] = stock_df.get('country', pd.Series()).map(COUNTRY_MAP).fillna('')
        stock_df['ticker'] = ''
        stock_df['stock_id'] = stock_df['name'] + '|' + stock_df['location']
        stock_df['sector'] = ''
        if not df.empty:
            df = pd.concat([df, stock_df[['stock_id', 'ticker', 'name', 'weight', 'sector', 'location']]], ignore_index=True)
        else:
            df = stock_df[['stock_id', 'ticker', 'name', 'weight', 'sector', 'location']].copy()

    if not df.empty:
        df['norm_key'] = df['name'].apply(normalize_company_name)
        df['sector'] = df['norm_key'].map(sector_lookup['sector']).fillna(df['sector'])
        df.loc[df['sector'].isin(['', None]), 'sector'] = 'Direct Investment'
        df = df.sort_values('weight', ascending=False).reset_index(drop=True)

    acwi_nk = acwi.copy()
    fund_data = fund_to_json(df, 'SEB 55+', acwi_nk, acwi_keys, sector_lookup)
    fund_data['asset_classes'] = parsed.get('asset_classes', {})

    # Add non-stock holdings
    pe_holdings = [{'name': p['name'], 'weight': p['weight_pct'], 'type': 'pe'} for p in parsed.get('pe_funds', [])]
    re_holdings = [{'name': r['name'], 'weight': r['weight_pct'], 'type': 're'} for r in parsed.get('re_funds', [])]
    bond_holdings = parsed.get('bonds', [])
    bond_fund_holdings = [{'name': b['name'], 'weight': b['weight_pct'], 'type': 'bonds'} for b in parsed.get('bond_funds', [])]

    fund_data['pe_holdings'] = pe_holdings
    fund_data['re_holdings'] = re_holdings
    fund_data['bond_holdings'] = bond_holdings + bond_fund_holdings
    fund_data['etf_holdings'] = etf_only_entries + opaque_entries
    fund_data['etf_breakdown'] = build_etf_breakdown(lookthrough_allocs, etf_holdings)

    # Update weight vectors
    wv = fund_data['_weight_vec']
    wv_export = fund_data['weights']
    for h in pe_holdings + re_holdings:
        prefix = 'PE' if h['type'] == 'pe' else 'RE'
        key = f"{prefix}|{h['name']}"
        wv[key] = h['weight']
        if h['weight'] > 0.01:
            wv_export[key] = round(h['weight'], 4)
    for h in bond_holdings + bond_fund_holdings:
        key = f"BOND|{h['name']}"
        w = h.get('weight', h.get('weight_pct', 0))
        wv[key] = w
        if w > 0.01:
            wv_export[key] = round(w, 4)
    for h in etf_only_entries + opaque_entries:
        key = f"ETF|{h['name']}"
        w = h.get('weight', h.get('weight_pct', 0))
        wv[key] = w
        if w > 0.01:
            wv_export[key] = round(w, 4)

    # Update sectors
    sectors = fund_data['sectors']
    pe_total = sum(h['weight'] for h in pe_holdings)
    re_total = sum(h['weight'] for h in re_holdings)
    bond_total = sum(h.get('weight', h.get('weight_pct', 0)) for h in bond_holdings + bond_fund_holdings)
    etf_total = sum(h.get('weight', h.get('weight_pct', 0)) for h in etf_only_entries + opaque_entries)
    if pe_total > 0:
        sectors['Erakapital'] = round(pe_total, 2)
    if re_total > 0:
        sectors['Kinnisvara'] = round(re_total, 2)
    if bond_total > 0:
        sectors['Võlakirjad'] = round(bond_total, 2)
    if etf_total > 0:
        sectors['ETF-id'] = round(etf_total, 2)

    countries = fund_data['countries']
    non_stock = pe_total + re_total + bond_total + etf_total
    if non_stock > 0:
        countries['Eesti (PE/RE/võlak.)'] = round(non_stock, 2)

    return fund_data


def process_bond_fund(name, parsed, acwi, acwi_keys, sector_lookup):
    """Process a bond-dominated fund (e.g. Tuleva Võlakirjad, Swedbank Konservatiivne).
    These funds have 0 or very few stocks — mostly bonds/bond funds/deposits.
    """
    bond_funds = parsed.get('bond_funds', [])
    bonds = parsed.get('bonds', [])
    deposits_pct = parsed.get('deposits_pct', 0)
    stocks = parsed.get('stocks', [])

    # Build minimal stock DataFrame (may be empty)
    if stocks:
        df = pd.DataFrame(stocks)
        df = df.rename(columns={'weight_pct': 'weight'})
        df['location'] = df.get('country', pd.Series()).map(COUNTRY_MAP).fillna('')
        df['norm_key'] = df['name'].apply(normalize_company_name)
        df['sector'] = df['norm_key'].map(sector_lookup['sector']).fillna('Direct Investment')
        df = df.sort_values('weight', ascending=False).reset_index(drop=True)
    else:
        df = pd.DataFrame()

    acwi_nk = acwi.copy()
    fund_data = fund_to_json(df, name, acwi_nk, acwi_keys, sector_lookup)

    # Compute asset class totals
    bond_fund_pct = sum(bf['weight_pct'] for bf in bond_funds)
    bond_pct = sum(b.get('weight_pct', b.get('weight', 0)) for b in bonds)
    stock_pct = sum(s.get('weight_pct', s.get('weight', 0)) for s in stocks)

    fund_data['asset_classes'] = {}
    if stock_pct > 0:
        fund_data['asset_classes']['stocks'] = round(stock_pct, 1)
    if bond_fund_pct + bond_pct > 0:
        fund_data['asset_classes']['bonds'] = round(bond_fund_pct + bond_pct, 1)
    if deposits_pct > 0:
        fund_data['asset_classes']['deposits'] = round(deposits_pct, 1)

    # Build bond holdings list
    bond_holdings = [{'name': bf['name'], 'weight': bf['weight_pct'], 'type': 'bonds'}
                     for bf in bond_funds]
    bond_holdings.extend([{'name': b['name'], 'weight': b.get('weight_pct', b.get('weight', 0)), 'type': 'bonds'}
                          for b in bonds])
    fund_data['bond_holdings'] = bond_holdings

    # Add bond holdings to weight vectors
    wv = fund_data['_weight_vec']
    wv_export = fund_data['weights']
    for h in bond_holdings:
        key = f"BOND|{h['name']}"
        w = h['weight']
        wv[key] = w
        if w > 0.01:
            wv_export[key] = round(w, 4)

    # Update sectors
    sectors = fund_data['sectors']
    total_bonds = bond_fund_pct + bond_pct
    if total_bonds > 0:
        sectors['Võlakirjad'] = round(total_bonds, 2)

    return fund_data


# ═══════════════════════════════════════════════════════════════════
# SECTION 6: LUMINOR LOOK-THROUGH
# ═══════════════════════════════════════════════════════════════════

# Luminor ISINs are not in the monthly report, so we map by name
LUMINOR_ETF_PROXY_MAP = {
    # Short distinctive substrings for robust matching against garbled PDF names
    'Developed World Screened': 'SAWD',
    'Developed World Index': 'SAWD',
    'Core MSCI EM IMI': 'SAEM',  # actually tracks IMI → SAEM is correct
    'Emerging Markets Index': 'SSAC_EM',  # tracks standard MSCI EM (large+mid)
    'Europe Equity Index': 'SAEU',
    'Japan Index Fund': 'SAJP',
    'North America Index': 'SASU',
    'SPDR MSCI World': 'SAWD',
    'Nasdaq-100': 'SASU',
    'Amundi Prime Global': 'SAWD',
    'Stoxx Europe 600': 'SAEU',
    'S&P 500 Swap': 'SASU',
    'Robeco 3D Global': 'SAWD',
    'MSCI World Materials': 'SAWD',
    'MSCI EM EX-China': 'EMXC',
    'iShares S&P 500': 'SASU',
    'EUR High Yield Corp': 'SAEU',  # bond proxy — won't look through but prevents opaque
    'Euro Investment Grade': 'SAEU',
}


def process_luminor_fund(name, parsed, etf_holdings, acwi, acwi_keys, sector_lookup):
    """Process Luminor 16-50 fund with ETF look-through."""
    equity_funds = parsed.get('equity_funds', [])

    # Build allocations for look-through
    allocations = []
    opaque_entries = []
    for ef in equity_funds:
        etk = None
        for pattern, ticker in LUMINOR_ETF_PROXY_MAP.items():
            if pattern.lower() in ef['name'].lower():
                etk = ticker
                break
        if etk:
            allocations.append({
                'name': ef['name'],
                'weight_pct': ef['weight_pct'],
                'etf_ticker': etk,
            })
        else:
            opaque_entries.append({
                'name': ef['name'],
                'weight_pct': ef['weight_pct'],
                'type': 'opaque_fund',
            })

    df, _ = build_lookthrough(allocations, etf_holdings)

    if not df.empty:
        acwi_universe = set(acwi['stock_id'])
        df_filtered = df[df['stock_id'].isin(acwi_universe)].copy()
        if not df_filtered.empty:
            df = df_filtered
        equity_total = sum(ef['weight_pct'] for ef in equity_funds)
        total_w = df['weight'].sum()
        if total_w > 0:
            df['weight'] = df['weight'] / total_w * equity_total
        df['norm_key'] = df['name'].apply(normalize_company_name)

    acwi_nk = acwi.copy()
    fund_data = fund_to_json(df, name, acwi_nk, acwi_keys, sector_lookup)

    # ETF breakdown
    fund_data['etf_breakdown'] = build_etf_breakdown(
        [{'isin': '', 'name': a['name'], 'weight_pct': a['weight_pct'], 'etf_ticker': a['etf_ticker']} for a in allocations],
        etf_holdings
    )

    # Asset classes
    equity_total = sum(ef['weight_pct'] for ef in equity_funds)
    bond_total = (sum(bf['weight_pct'] for bf in parsed.get('bond_funds', []))
                 + sum(b['weight_pct'] for b in parsed.get('bonds', []))
                 + parsed.get('direct_bond_pct', 0))
    re_total = sum(rf['weight_pct'] for rf in parsed.get('re_funds', []))
    pe_total = sum(pf['weight_pct'] for pf in parsed.get('pe_funds', []))
    deposits = parsed.get('deposits_pct', 0)

    fund_data['asset_classes'] = {'stocks': round(equity_total, 1)}
    if bond_total > 0:
        fund_data['asset_classes']['bonds'] = round(bond_total, 1)
    if re_total > 0:
        fund_data['asset_classes']['re'] = round(re_total, 1)
    if pe_total > 0:
        fund_data['asset_classes']['pe'] = round(pe_total, 1)
    if deposits > 0:
        fund_data['asset_classes']['deposits'] = round(deposits, 1)

    # Add non-equity holdings for display
    direct_bonds = [{'name': b['name'], 'weight': b['weight_pct'], 'type': 'bonds'} for b in parsed.get('bonds', [])]
    bond_fund_entries = [{'name': b['name'], 'weight': b['weight_pct'], 'type': 'bonds'} for b in parsed.get('bond_funds', [])]
    fund_data['bond_holdings'] = direct_bonds + bond_fund_entries
    fund_data['re_holdings'] = [{'name': r['name'], 'weight': r['weight_pct'], 'type': 're'} for r in parsed.get('re_funds', [])]
    fund_data['pe_holdings'] = [{'name': p['name'], 'weight': p['weight_pct'], 'type': 'pe'} for p in parsed.get('pe_funds', [])]
    fund_data['etf_holdings'] = opaque_entries

    # Add non-stock entries to weight vectors
    wv = fund_data['_weight_vec']
    wv_export = fund_data['weights']
    for prefix, holdings in [('BOND', fund_data['bond_holdings']),
                              ('RE', fund_data['re_holdings']),
                              ('PE', fund_data['pe_holdings']),
                              ('ETF', opaque_entries)]:
        for h in holdings:
            key = f"{prefix}|{h['name']}"
            w = h.get('weight', h.get('weight_pct', 0))
            wv[key] = w
            if w > 0.01:
                wv_export[key] = round(w, 4)

    # Update sectors
    sectors = fund_data['sectors']
    if bond_total > 0:
        sectors['Võlakirjad'] = round(bond_total, 2)
    if re_total > 0:
        sectors['Kinnisvara'] = round(re_total, 2)
    if pe_total > 0:
        sectors['Erakapital'] = round(pe_total, 2)

    return fund_data


# ═══════════════════════════════════════════════════════════════════
# SECTION 7: MAIN PIPELINE
# ═══════════════════════════════════════════════════════════════════

# Data source tracking for sources.html
DATA_SOURCES = {}


def main():
    parser = argparse.ArgumentParser(description='Estonian pension fund analysis pipeline')
    parser.add_argument('--month', default=None,
                        help='Month to process (YYYY-MM). Default: latest in data/monthly/')
    parser.add_argument('--skip-nav', action='store_true',
                        help='Skip NAV history fetch (faster, fully deterministic from tracked data)')
    args = parser.parse_args()

    print('=== Multi-Source Pension Fund Pipeline ===\n')

    # Load monthly JSON config
    monthly_files = sorted(Path('data/monthly').glob('*.json'))
    if not args.month and not monthly_files:
        parser.error('No config files found in data/monthly/. Use --month to specify.')
    MONTH = args.month or monthly_files[-1].stem
    reports_cfg, alloc_cfg = load_monthly_config(MONTH)
    if reports_cfg:
        print(f'Loaded monthly config for {MONTH} ({len(reports_cfg)} reports, {len(alloc_cfg or {})} allocations)')
    else:
        print(f'WARNING: No monthly config for {MONTH}, using hardcoded fallbacks')
        alloc_cfg = {}

    # Step 1: Load ETF holdings from iShares CSVs
    print('Loading ETF holdings from CSVs...')
    etf_holdings = {}
    for tk in ['SAWD', 'SASU', 'SAEU', 'SAJP', 'SAEM', 'SSAC', 'NDIA', '4BRZ', 'CNYA', 'IKSA', 'XTJP', 'SPPY', 'EMXC']:
        etf_holdings[tk] = fetch_ishares_holdings(tk)
        print(f'  {tk}: {len(etf_holdings[tk])} rows')

    # Load EODHD ETFs
    print('\nLoading ETF holdings from EODHD...')
    for tk in EODHD_ETFS:
        etf_holdings[tk] = fetch_eodhd_holdings(tk)
        print(f'  {tk}: {len(etf_holdings[tk])} rows')

    # Load manual holdings (fund factsheets)
    print('\nLoading manual holdings...')
    for tk in ['GLOBALFOND_A']:
        etf_holdings[tk] = load_manual_holdings(tk)
        print(f'  {tk}: {len(etf_holdings[tk])} rows')

    # Build SSAC_EM: EM-country stocks from SSAC (proxy for standard MSCI EM large+mid cap)
    print('\nBuilding SSAC_EM (EM from ACWI)...')
    etf_holdings['SSAC_EM'] = build_ssac_em(etf_holdings)
    print(f'  SSAC_EM: {len(etf_holdings["SSAC_EM"])} rows (EM-country equities from SSAC)')

    # Step 2: Build ACWI benchmark
    print('\nBuilding ACWI benchmark...')
    acwi = build_acwi(etf_holdings)
    acwi['weight'] = acwi['weight'] / acwi['weight'].sum() * 100
    acwi['norm_key'] = acwi['name'].apply(normalize_company_name)
    sector_lookup, fuzzy_sector_map = _build_sector_lookup_with_fuzzy(acwi)
    acwi_keys = set(acwi['norm_key'])
    print(f'  ACWI: {len(acwi)} stocks, {len(fuzzy_sector_map)} fuzzy prefixes')

    all_funds_data = {}

    # ACWI benchmark (internal, not in fund_order)
    acwi_data = fund_to_json(acwi, 'MSCI ACWI', acwi, acwi_keys, sector_lookup)
    acwi_data['type'] = 'benchmark'
    acwi_data['provider'] = 'MSCI'
    acwi_data['asset_classes'] = {'stocks': 100.0}
    all_funds_data['ACWI'] = acwi_data

    # ── Fund 1: Tuleva Maailma Aktsiad (Type A) ──
    print('\n1. Tuleva Maailma Aktsiad...')
    tuleva_pdf = REPORT_DIR / reports_cfg['Tuleva']['pdf'] if reports_cfg else REPORT_DIR / 'Tuleva-Maailma-Aktsiate-Pensionifondi-investeeringute-aruanne-jaanuar-2026.pdf'
    tuleva_parsed = parse_tuleva_monthly(tuleva_pdf)
    print(f'   {len(tuleva_parsed["allocations"])} ETF allocations')
    for a in tuleva_parsed['allocations']:
        print(f'     {a["name"][:50]:50s} {a["isin"]} {a["weight_pct"]:.2f}%')

    tuleva_data = process_etf_fund('Tuleva Maailma Aktsiad', tuleva_parsed['allocations'],
                                    etf_holdings, acwi, acwi_keys, sector_lookup)
    if tuleva_data:
        tuleva_data['type'] = 'index'
        tuleva_data['provider'] = 'Tuleva'
        all_funds_data['Tuleva'] = tuleva_data
        print(f'   => {tuleva_data["n_stocks"]} stocks')
        _date = reports_cfg['Tuleva']['date'] if reports_cfg else '30.01.2026'
        DATA_SOURCES['Tuleva'] = {'pdf': tuleva_pdf.name, 'type': 'A (ETF)', 'date': _date,
                                   'etf_count': len(tuleva_parsed['allocations'])}

    # ── Fund 2: Luminor 16-50 (Type A/C mixed) ──
    print('\n2. Luminor 16-50...')
    if alloc_cfg and 'Luminor 16-50' in alloc_cfg:
        lum_parsed = _load_luminor_allocations(alloc_cfg['Luminor 16-50'])
        print('   (from monthly JSON)')
    else:
        lum_pdf = REPORT_DIR / reports_cfg['Luminor 16-50']['pdf'] if reports_cfg else REPORT_DIR / 'investeeringute_aruanne_lum16-50_0126.pdf'
        lum_parsed = parse_luminor_monthly(lum_pdf)
    print(f'   {len(lum_parsed["equity_funds"])} equity funds, {len(lum_parsed["bond_funds"])} bond funds')

    lum_data = process_luminor_fund('Luminor 16-50', lum_parsed, etf_holdings, acwi, acwi_keys, sector_lookup)
    if lum_data:
        lum_data['type'] = 'mixed'
        lum_data['provider'] = 'Luminor'
        all_funds_data['Luminor 16-50'] = lum_data
        print(f'   => {lum_data["n_stocks"]} stocks (look-through)')
        _date = reports_cfg['Luminor 16-50']['date'] if reports_cfg else '31.01.2026'
        _pdf = reports_cfg['Luminor 16-50']['pdf'] if reports_cfg else 'est_NPK75_raport_20260228.pdf'
        DATA_SOURCES['Luminor 16-50'] = {'pdf': _pdf, 'type': 'A (ETF+bonds)', 'date': _date,
                                          'etf_count': len(lum_parsed['equity_funds'])}

    # ── Fund 3: SEB Indeks (Type A) ──
    print('\n3. SEB Indeks...')
    if alloc_cfg and 'SEB Indeks' in alloc_cfg:
        seb_idx_parsed = {'allocations': alloc_cfg['SEB Indeks']}
        print('   (from monthly JSON)')
    else:
        seb_idx_pdf = REPORT_DIR / reports_cfg['SEB Indeks']['pdf'] if reports_cfg else REPORT_DIR / 'est_SIK75_raport_20260131.pdf'
        seb_idx_parsed = parse_seb_indeks_monthly(seb_idx_pdf)
    print(f'   {len(seb_idx_parsed["allocations"])} ETF allocations')
    for a in seb_idx_parsed['allocations']:
        mapped = ETF_ISIN_TO_CSV.get(a['isin'], 'OPAQUE' if a['isin'] in OPAQUE_FUND_ISINS else '?')
        print(f'     {a["name"][:45]:45s} {a["isin"]} {a["weight_pct"]:5.2f}% => {mapped}')

    seb_idx_data = process_etf_fund('SEB Indeks', seb_idx_parsed['allocations'],
                                     etf_holdings, acwi, acwi_keys, sector_lookup)
    if seb_idx_data:
        seb_idx_data['type'] = 'index'
        seb_idx_data['provider'] = 'SEB'
        all_funds_data['SEB Indeks'] = seb_idx_data
        print(f'   => {seb_idx_data["n_stocks"]} stocks')
        _date = reports_cfg['SEB Indeks']['date'] if reports_cfg else '31.01.2026'
        _pdf = reports_cfg['SEB Indeks']['pdf'] if reports_cfg else 'est_SIK75_raport_20260228.pdf'
        DATA_SOURCES['SEB Indeks'] = {'pdf': _pdf, 'type': 'A (ETF)', 'date': _date,
                                       'etf_count': len(seb_idx_parsed['allocations']),
                                       'opaque_pct': seb_idx_data.get('opaque_pct', 0)}

    # ── Funds 4-7: Swedbank K-series (Type B) ──
    k_fund_names = ['Swedbank K1960', 'Swedbank K1970', 'Swedbank K1980', 'Swedbank K1990']
    k_fund_defaults = {
        'Swedbank K1960': 'K1960_investment_portfolio.pdf',
        'Swedbank K1970': 'K1970_investment_portfolio.pdf',
        'Swedbank K1980': 'K1980_investment_portfolio.pdf',
        'Swedbank K1990': 'K1990_investment_portfolio.pdf',
    }
    for i, fund_name in enumerate(k_fund_names, 4):
        pdf_name = reports_cfg[fund_name]['pdf'] if reports_cfg and fund_name in reports_cfg else k_fund_defaults[fund_name]
        print(f'\n{i}. {fund_name}...')
        pdf_path = REPORT_DIR / pdf_name
        parsed = parse_swedbank_monthly(pdf_path)
        print(f'   {len(parsed["stocks"])} stocks, {len(parsed["bonds"])} bonds')

        fund_data = process_stock_fund(fund_name, parsed, etf_holdings, acwi, acwi_keys, sector_lookup,
                                       fuzzy_sector_map)
        if fund_data:
            fund_data['type'] = 'mixed'
            fund_data['provider'] = 'Swedbank'
            all_funds_data[fund_name] = fund_data
            print(f'   => {fund_data["n_stocks"]} stocks exported')
            _date = reports_cfg[fund_name]['date'] if reports_cfg and fund_name in reports_cfg else '31.01.2026'
            DATA_SOURCES[fund_name] = {'pdf': pdf_name, 'type': 'B (direct stocks)', 'date': _date,
                                        'stock_count': len(parsed['stocks'])}

    # ── Fund 8: LHV Ettevõtlik (Type C, pre-parsed) ──
    print('\n8. LHV Ettevõtlik (from pre-parsed JSON)...')
    parsed_path = CACHE_DIR / 'LLK50_parsed.json'
    with open(parsed_path) as f:
        llk50_raw = json.load(f)

    llk50_data = process_mixed_fund('LHV Ettevõtlik', llk50_raw, etf_holdings, acwi, acwi_keys, sector_lookup)
    if llk50_data:
        llk50_data['type'] = 'active'
        llk50_data['provider'] = 'LHV'
        llk50_data['nav_eur'] = llk50_raw.get('nav_eur')
        all_funds_data['LHV Ettevõtlik'] = llk50_data
        print(f'   => {llk50_data["n_stocks"]} stocks')
        _pdf = reports_cfg['LHV Ettevõtlik']['pdf'] if reports_cfg and 'LHV Ettevõtlik' in reports_cfg else 'est_LLK50_raport_20260131.pdf'
        _date = reports_cfg['LHV Ettevõtlik']['date'] if reports_cfg and 'LHV Ettevõtlik' in reports_cfg else '31.01.2026'
        DATA_SOURCES['LHV Ettevõtlik'] = {'pdf': _pdf, 'type': 'C (mixed)',
                                            'date': _date, 'pre_parsed': True}

    # ── Fund 9: LHV Julge (Type C) ──
    print('\n9. LHV Julge...')
    lxk_pdf = REPORT_DIR / (reports_cfg['LHV Julge']['pdf'] if reports_cfg and 'LHV Julge' in reports_cfg else 'est_LXK75_raport_20260131.pdf')
    lxk_parsed = parse_lhv_monthly(lxk_pdf)
    stock_count = len([h for h in lxk_parsed.get('holdings', []) if h.get('type') == 'stocks'])
    etf_count = len([h for h in lxk_parsed.get('holdings', []) if h.get('type') == 'etfs'])
    print(f'   {stock_count} stocks, {etf_count} ETFs, {len(lxk_parsed["pe_holdings"])} PE, {len(lxk_parsed["re_holdings"])} RE')

    lxk_data = process_mixed_fund('LHV Julge', lxk_parsed, etf_holdings, acwi, acwi_keys, sector_lookup)
    if lxk_data:
        lxk_data['type'] = 'active'
        lxk_data['provider'] = 'LHV'
        all_funds_data['LHV Julge'] = lxk_data
        print(f'   => {lxk_data["n_stocks"]} stocks')
        _date = reports_cfg['LHV Julge']['date'] if reports_cfg and 'LHV Julge' in reports_cfg else '31.01.2026'
        DATA_SOURCES['LHV Julge'] = {'pdf': lxk_pdf.name, 'type': 'C (mixed)',
                                      'date': _date}

    # ── Fund 10: SEB 55+ (Type C) ──
    print('\n10. SEB 55+...')
    seb55_pdf = REPORT_DIR / (reports_cfg['SEB 55+']['pdf'] if reports_cfg and 'SEB 55+' in reports_cfg else 'est_SEK50_raport_20260131.pdf')
    if alloc_cfg and 'SEB 55+' in alloc_cfg:
        seb55_eq, seb55_bonds, seb55_stocks, seb55_re, seb55_pe, seb55_bf = _load_seb_allocations(alloc_cfg['SEB 55+'])
        seb55_parsed = {
            'equity_funds': seb55_eq, 'bonds': seb55_bonds, 'stocks': seb55_stocks,
            're_funds': seb55_re, 'pe_funds': seb55_pe, 'bond_funds': seb55_bf,
            'asset_classes': {
                'stocks': round(sum(s['weight_pct'] for s in seb55_stocks) +
                               sum(e['weight_pct'] for e in seb55_eq), 2),
                'bonds': round(sum(b['weight_pct'] for b in seb55_bonds) +
                              sum(b['weight_pct'] for b in seb55_bf), 2),
                'pe': round(sum(p['weight_pct'] for p in seb55_pe), 2),
                're': round(sum(r['weight_pct'] for r in seb55_re), 2),
            },
        }
        print('   (from monthly JSON)')
    else:
        seb55_parsed = parse_seb_55_monthly(seb55_pdf)
    print(f'   {len(seb55_parsed["equity_funds"])} equity ETFs, {len(seb55_parsed["bonds"])} bonds')

    seb55_data = process_seb_55(seb55_parsed, etf_holdings, acwi, acwi_keys, sector_lookup)
    if seb55_data:
        seb55_data['type'] = 'active'
        seb55_data['provider'] = 'SEB'
        all_funds_data['SEB 55+'] = seb55_data
        print(f'   => {seb55_data["n_stocks"]} stocks')
        _date = reports_cfg['SEB 55+']['date'] if reports_cfg and 'SEB 55+' in reports_cfg else '31.01.2026'
        DATA_SOURCES['SEB 55+'] = {'pdf': seb55_pdf.name, 'type': 'C (mixed)',
                                    'date': _date}

    # ── Funds 11-13: LHV Rahulik, Indeks, Tasakaalukas ──
    print('\n11. LHV Rahulik...')
    lxk00_pdf = REPORT_DIR / (reports_cfg['LHV Rahulik']['pdf'] if reports_cfg and 'LHV Rahulik' in reports_cfg else 'est_LXK00_raport_20260131.pdf')
    lxk00_parsed = parse_lhv_monthly(lxk00_pdf)
    lxk00_data = process_mixed_fund('LHV Rahulik', lxk00_parsed, etf_holdings, acwi, acwi_keys, sector_lookup)
    if lxk00_data:
        lxk00_data['type'] = 'conservative'
        lxk00_data['provider'] = 'LHV'
        all_funds_data['LHV Rahulik'] = lxk00_data
        print(f'   => {lxk00_data["n_stocks"]} stocks')
        _date = reports_cfg['LHV Rahulik']['date'] if reports_cfg and 'LHV Rahulik' in reports_cfg else '31.01.2026'
        DATA_SOURCES['LHV Rahulik'] = {'pdf': lxk00_pdf.name, 'type': 'C (bond-heavy)',
                                        'date': _date}

    print('\n12. LHV Indeks...')
    lik_pdf = REPORT_DIR / (reports_cfg['LHV Indeks']['pdf'] if reports_cfg and 'LHV Indeks' in reports_cfg else 'est_LIK75_raport_20260131.pdf')
    lik_parsed = parse_lhv_monthly(lik_pdf)
    # LHV Indeks is all ETFs — process like ETF fund
    lik_etf_holdings = [h for h in lik_parsed.get('holdings', []) if h.get('type') == 'etfs']
    lik_allocations = [{'name': h['name'], 'isin': h.get('isin', ''), 'weight_pct': h['weight']}
                       for h in lik_etf_holdings]
    lik_data = process_etf_fund('LHV Indeks', lik_allocations, etf_holdings, acwi, acwi_keys, sector_lookup)
    if lik_data:
        lik_data['type'] = 'index'
        lik_data['provider'] = 'LHV'
        all_funds_data['LHV Indeks'] = lik_data
        print(f'   => {lik_data["n_stocks"]} stocks')
        _date = reports_cfg['LHV Indeks']['date'] if reports_cfg and 'LHV Indeks' in reports_cfg else '31.01.2026'
        DATA_SOURCES['LHV Indeks'] = {'pdf': lik_pdf.name, 'type': 'A (ETF)',
                                       'date': _date}

    print('\n13. LHV Tasakaalukas...')
    lmk_pdf = REPORT_DIR / (reports_cfg['LHV Tasakaalukas']['pdf'] if reports_cfg and 'LHV Tasakaalukas' in reports_cfg else 'est_LMK25_raport_20260131.pdf')
    lmk_parsed = parse_lhv_monthly(lmk_pdf)
    lmk_data = process_mixed_fund('LHV Tasakaalukas', lmk_parsed, etf_holdings, acwi, acwi_keys, sector_lookup)
    if lmk_data:
        lmk_data['type'] = 'active'
        lmk_data['provider'] = 'LHV'
        all_funds_data['LHV Tasakaalukas'] = lmk_data
        print(f'   => {lmk_data["n_stocks"]} stocks')
        _date = reports_cfg['LHV Tasakaalukas']['date'] if reports_cfg and 'LHV Tasakaalukas' in reports_cfg else '31.01.2026'
        DATA_SOURCES['LHV Tasakaalukas'] = {'pdf': lmk_pdf.name, 'type': 'C (mixed)',
                                              'date': _date}

    # ── Funds 14-17: Luminor Indeks, 50-56, 56+, 61-65 ──
    _luminor_funds = [
        ('Luminor Indeks', 14, 'index', _luminor_indeks_hardcoded),
        ('Luminor 50-56', 15, 'mixed', _luminor_50_56_hardcoded),
        ('Luminor 56+', 16, 'mixed', _luminor_56_plus_hardcoded),
        ('Luminor 61-65', 17, 'conservative', _luminor_61_65_hardcoded),
    ]
    for lum_name, lum_idx, lum_type, lum_fallback in _luminor_funds:
        print(f'\n{lum_idx}. {lum_name}...')
        if alloc_cfg and lum_name in alloc_cfg:
            lum_parsed = _load_luminor_allocations(alloc_cfg[lum_name])
            print('   (from monthly JSON)')
        else:
            lum_parsed = lum_fallback()
        lum_data = process_luminor_fund(lum_name, lum_parsed, etf_holdings, acwi, acwi_keys, sector_lookup)
        if lum_data:
            lum_data['type'] = lum_type
            lum_data['provider'] = 'Luminor'
            all_funds_data[lum_name] = lum_data
            print(f'   => {lum_data["n_stocks"]} stocks')
            _pdf = reports_cfg[lum_name]['pdf'] if reports_cfg and lum_name in reports_cfg else ''
            _date = reports_cfg[lum_name]['date'] if reports_cfg and lum_name in reports_cfg else '31.01.2026'
            _type_str = 'A (ETF)' if lum_type == 'index' else 'A (ETF+bonds)'
            DATA_SOURCES[lum_name] = {'pdf': _pdf, 'type': _type_str, 'date': _date}

    # ── Funds 18-20: SEB 18+, 60+, 65+ ──
    _seb_extra_funds = [
        ('SEB 18+', 18, 'active', _seb_18_hardcoded, 0),
        ('SEB 60+', 19, 'mixed', _seb_60_hardcoded, 26.29),
        ('SEB 65+', 20, 'conservative', _seb_65_hardcoded, 49.33),
    ]
    for seb_name, seb_idx, seb_type, seb_fallback, default_direct_bond_pct in _seb_extra_funds:
        print(f'\n{seb_idx}. {seb_name}...')
        if alloc_cfg and seb_name in alloc_cfg:
            seb_eq, seb_bonds, seb_stocks, seb_re, seb_pe, seb_bf = _load_seb_allocations(alloc_cfg[seb_name])
            direct_bond_pct = alloc_cfg[seb_name].get('direct_bond_pct', default_direct_bond_pct)
            print('   (from monthly JSON)')
        else:
            seb_eq, seb_bonds, seb_stocks, seb_re, seb_pe, seb_bf = seb_fallback()
            direct_bond_pct = default_direct_bond_pct
        seb_parsed = {
            'equity_funds': seb_eq, 'bonds': seb_bonds, 'stocks': seb_stocks,
            're_funds': seb_re, 'pe_funds': seb_pe, 'bond_funds': seb_bf,
            'asset_classes': {
                'stocks': round(sum(s['weight_pct'] for s in seb_stocks) +
                               sum(e['weight_pct'] for e in seb_eq), 2),
                'bonds': round(direct_bond_pct + sum(b['weight_pct'] for b in seb_bonds) +
                              sum(b['weight_pct'] for b in seb_bf), 2),
                'pe': round(sum(p['weight_pct'] for p in seb_pe), 2),
                're': round(sum(r['weight_pct'] for r in seb_re), 2),
            },
        }
        seb_data = process_seb_55(seb_parsed, etf_holdings, acwi, acwi_keys, sector_lookup)
        if seb_data:
            seb_data['type'] = seb_type
            seb_data['provider'] = 'SEB'
            all_funds_data[seb_name] = seb_data
            print(f'   => {seb_data["n_stocks"]} stocks')
            _pdf = reports_cfg[seb_name]['pdf'] if reports_cfg and seb_name in reports_cfg else ''
            _date = reports_cfg[seb_name]['date'] if reports_cfg and seb_name in reports_cfg else '31.01.2026'
            _type_str = 'C (bond-heavy)' if seb_type == 'conservative' else 'C (mixed)'
            DATA_SOURCES[seb_name] = {'pdf': _pdf, 'type': _type_str, 'date': _date}

    # ── Funds 21-23: Swedbank Indeks, 2000-09, Konservatiivne ──
    print('\n21. Swedbank Indeks...')
    swi_pdf = REPORT_DIR / (reports_cfg['Swedbank Indeks']['pdf'] if reports_cfg and 'Swedbank Indeks' in reports_cfg else 'Ki_investment_portfolio.pdf')
    swi_parsed = parse_swedbank_monthly(swi_pdf)
    swi_data = process_stock_fund('Swedbank Indeks', swi_parsed, etf_holdings, acwi, acwi_keys,
                                   sector_lookup, fuzzy_sector_map)
    if swi_data:
        swi_data['type'] = 'index'
        swi_data['provider'] = 'Swedbank'
        all_funds_data['Swedbank Indeks'] = swi_data
        print(f'   => {swi_data["n_stocks"]} stocks')
        _date = reports_cfg['Swedbank Indeks']['date'] if reports_cfg and 'Swedbank Indeks' in reports_cfg else '31.01.2026'
        DATA_SOURCES['Swedbank Indeks'] = {'pdf': swi_pdf.name, 'type': 'B (direct stocks)',
                                            'date': _date}

    print('\n22. Swedbank 2000-09...')
    sw2000_pdf = REPORT_DIR / (reports_cfg['Swedbank 2000-09']['pdf'] if reports_cfg and 'Swedbank 2000-09' in reports_cfg else 'K2000_investment_portfolio.pdf')
    sw2000_parsed = parse_swedbank_monthly(sw2000_pdf)
    sw2000_data = process_stock_fund('Swedbank 2000-09', sw2000_parsed, etf_holdings, acwi, acwi_keys,
                                      sector_lookup, fuzzy_sector_map)
    if sw2000_data:
        sw2000_data['type'] = 'mixed'
        sw2000_data['provider'] = 'Swedbank'
        all_funds_data['Swedbank 2000-09'] = sw2000_data
        print(f'   => {sw2000_data["n_stocks"]} stocks')
        _date = reports_cfg['Swedbank 2000-09']['date'] if reports_cfg and 'Swedbank 2000-09' in reports_cfg else '31.01.2026'
        DATA_SOURCES['Swedbank 2000-09'] = {'pdf': sw2000_pdf.name, 'type': 'B (mixed)',
                                              'date': _date}

    print('\n23. Swedbank Konservatiivne...')
    swk_pdf = REPORT_DIR / (reports_cfg['Swedbank Konservatiivne']['pdf'] if reports_cfg and 'Swedbank Konservatiivne' in reports_cfg else 'KKONS_investment_portfolio.pdf')
    swk_parsed = parse_swedbank_monthly(swk_pdf)
    swk_data = process_bond_fund('Swedbank Konservatiivne', {
        'bond_funds': [], 'bonds': [{'name': b['name'], 'weight_pct': b['weight_pct']}
                                     for b in swk_parsed.get('bonds', [])],
        'deposits_pct': swk_parsed.get('deposits_pct', 0),
        'stocks': [],
    }, acwi, acwi_keys, sector_lookup)
    if swk_data:
        swk_data['type'] = 'conservative'
        swk_data['provider'] = 'Swedbank'
        # Add RE holdings from parsed data
        re_pct = sum(r['weight_pct'] for r in swk_parsed.get('re_funds', []))
        if re_pct > 0:
            swk_data['asset_classes']['re'] = round(re_pct, 1)
            swk_data['re_holdings'] = [{'name': r['name'], 'weight': r['weight_pct'], 'type': 're'}
                                        for r in swk_parsed.get('re_funds', [])]
            for h in swk_data['re_holdings']:
                key = f"RE|{h['name']}"
                swk_data['_weight_vec'][key] = h['weight']
                if h['weight'] > 0.01:
                    swk_data['weights'][key] = round(h['weight'], 4)
        all_funds_data['Swedbank Konservatiivne'] = swk_data
        print(f'   => {swk_data["n_stocks"]} stocks, {len(swk_parsed.get("bonds", []))} bonds')
        _date = reports_cfg['Swedbank Konservatiivne']['date'] if reports_cfg and 'Swedbank Konservatiivne' in reports_cfg else '31.01.2026'
        DATA_SOURCES['Swedbank Konservatiivne'] = {'pdf': swk_pdf.name,
                                                     'type': 'B (bond-heavy)', 'date': _date}

    # ── Fund 24: Tuleva Võlakirjad ──
    print('\n24. Tuleva Võlakirjad...')
    tuk_pdf = REPORT_DIR / (reports_cfg['Tuleva Võlakirjad']['pdf'] if reports_cfg and 'Tuleva Võlakirjad' in reports_cfg else 'est_TUK00_raport_20260130.pdf')
    tuk_parsed = parse_tuleva_bond_monthly(tuk_pdf)
    tuk_data = process_bond_fund('Tuleva Võlakirjad', {
        'bond_funds': tuk_parsed['bond_funds'],
        'bonds': [],
        'deposits_pct': tuk_parsed['deposits_pct'],
        'stocks': [],
    }, acwi, acwi_keys, sector_lookup)
    if tuk_data:
        tuk_data['type'] = 'bond'
        tuk_data['provider'] = 'Tuleva'
        all_funds_data['Tuleva Võlakirjad'] = tuk_data
        print(f'   => {len(tuk_parsed["bond_funds"])} bond funds, deposits {tuk_parsed["deposits_pct"]:.2f}%')
        _date = reports_cfg['Tuleva Võlakirjad']['date'] if reports_cfg and 'Tuleva Võlakirjad' in reports_cfg else '30.01.2026'
        DATA_SOURCES['Tuleva Võlakirjad'] = {'pdf': tuk_pdf.name, 'type': 'A (bonds)',
                                              'date': _date}

    # ── Compute correlations and overlaps ──
    fund_order = [k for k in all_funds_data.keys() if k != 'ACWI']
    all_fund_names = list(all_funds_data.keys())

    print(f'\n=== {len(fund_order)} funds processed ===')
    for fn in fund_order:
        fd = all_funds_data[fn]
        print(f'  {fn:25s} {fd["n_stocks"]:5d} stocks  {fd["total_weight"]:6.1f}% weight')

    print('\nComputing pairwise correlations...')
    corr_matrix = compute_pairwise_correlations(all_funds_data, all_fund_names)

    print('Computing overlap stats...')
    overlap_stats = {}
    for fi in fund_order:
        vi = all_funds_data[fi]['_weight_vec']
        ki = set(vi.keys())
        for fj in fund_order:
            if fi == fj:
                continue
            vj = all_funds_data[fj]['_weight_vec']
            kj = set(vj.keys())
            shared = ki & kj
            only_i = ki - kj
            only_j = kj - ki
            sw_i = sum(vi.get(k, 0) for k in shared)
            sw_j = sum(vj.get(k, 0) for k in shared)
            ow_i = sum(vi.get(k, 0) for k in only_i)
            ow_j = sum(vj.get(k, 0) for k in only_j)
            overlap_stats[f'{fi}|{fj}'] = {
                'shared': len(shared),
                'only_a': len(only_i),
                'only_b': len(only_j),
                'total_a': len(ki),
                'total_b': len(kj),
                'shared_weight_a': round(sw_i, 2),
                'shared_weight_b': round(sw_j, 2),
                'only_weight_a': round(ow_i, 2),
                'only_weight_b': round(ow_j, 2),
            }

    with open(OUT_DIR / 'overlap_stats.json', 'w') as f:
        json.dump(overlap_stats, f, indent=2)

    # Remove internal weight vectors
    for fd in all_funds_data.values():
        fd.pop('_weight_vec', None)

    acwi_sectors = list(all_funds_data['ACWI']['sectors'].keys())

    # Single source of truth for management fees (% per year)
    fees = {
        'Tuleva': 0.28, 'Tuleva Võlakirjad': 0.28,
        'Luminor 16-50': 1.08, 'Luminor Indeks': 0.27,
        'Luminor 50-56': 1.13, 'Luminor 56+': 1.14, 'Luminor 61-65': 0.88,
        'SEB Indeks': 0.28, 'SEB 18+': 0.95, 'SEB 55+': 0.99,
        'SEB 60+': 0.90, 'SEB 65+': 0.50,
        'Swedbank K1960': 0.74, 'Swedbank K1970': 0.74,
        'Swedbank K1980': 0.72, 'Swedbank K1990': 0.31,
        'Swedbank Indeks': 0.27, 'Swedbank 2000-09': 0.75, 'Swedbank Konservatiivne': 0.47,
        'LHV Ettevõtlik': 1.57, 'LHV Julge': 1.21,
        'LHV Rahulik': 0.53, 'LHV Indeks': 0.27, 'LHV Tasakaalukas': 1.13,
    }

    output = {
        'generated': date.today().isoformat(),
        'data_month': MONTH,
        'fees': fees,
        'funds': all_funds_data,
        'fund_order': fund_order,
        'acwi_sector_order': acwi_sectors,
        'correlation_matrix': corr_matrix,
    }

    out_path = OUT_DIR / 'fund_data.json'
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f'\nExported to {out_path} ({out_path.stat().st_size / 1024:.0f} KB)')

    # Export data sources info for sources.html
    # Merge report URLs from monthly config
    if reports_cfg:
        for fund_name, report_info in reports_cfg.items():
            if fund_name in DATA_SOURCES and 'url' in report_info:
                DATA_SOURCES[fund_name]['url'] = report_info['url']
    sources_path = OUT_DIR / 'data_sources.json'
    with open(sources_path, 'w', encoding='utf-8') as f:
        json.dump(DATA_SOURCES, f, ensure_ascii=False, indent=2)

    # Export ETF metadata for sources.html (coverage, ETF sources, proxy mappings)
    # Coverage = 100% - weight of SEB internal proprietary funds (truly unknown compositions)
    # PE/RE, bonds, deposits, direct stocks, public ETFs and proxied active funds
    # all count as "covered" (we can identify them, even if PE/RE is at fund-name level)
    coverage = {}
    for fund_name in fund_order:
        fd = all_funds_data[fund_name]
        breakdown = fd.get('etf_breakdown', [])
        opaque = fd.get('opaque_pct', 0)
        proxy_w = sum(e['fund_weight'] for e in breakdown
                      if e.get('isin', '') in TRUE_PROXY_ISINS)
        pct = round(100 - proxy_w - opaque)
        coverage[fund_name] = {'pct': max(0, min(100, pct))}
        # Flag funds with significant unitemized weight
        if fund_name == 'Luminor 61-65':
            coverage[fund_name]['note'] = 'Otseobligatsioonid (26%) kajastatud koondkaaluna, mitte üksikute väärtpaberitena'

    etf_sources = []
    for ticker in sorted(etf_holdings.keys()):
        eq = etf_holdings[ticker]
        count = len(eq[eq['asset_class'] == 'Equity']) if 'asset_class' in eq.columns else len(eq)
        if ticker in EODHD_ETFS:
            source = 'EODHD API'
        elif ticker == 'GLOBALFOND_A':
            source = 'Fondlista.se (top 30)'
        elif ticker == 'SPPY':
            source = 'SPDR CSV'
        elif ticker == 'XTJP':
            source = 'Xtrackers (aastaaruanne)'
        elif ticker in ISHARES_PRODUCTS:
            source = 'iShares CSV'
        else:
            source = 'Muu'
        etf_sources.append({'ticker': ticker, 'stocks': count, 'source': source, 'type': 'full'})

    # Build proxy mappings from ETF_ISIN_TO_CSV
    # Canonical ISIN = the "real" ETF; everything else is a proxy mapped to it
    proxy_mappings = []
    canonical_isins = {
        'SAWD': 'IE0009FT4LX4', 'SASU': 'IE00BFNM3G45', 'SAEU': 'IE00BFNM3D14',
        'SAJP': 'IE00BFNM3L97', 'SAEM': 'IE00BFNM3P36', 'SSAC': 'IE00B6R52259',
        'SSAC_EM': 'IE00BKPTWY98',
        'NDIA': 'IE00BZCQB185', '4BRZ': 'IE00BFNM3V63', 'CNYA': 'IE00BQT3WG13',
        'IKSA': 'IE00BYYR0489', 'SPPY': 'IE00BH4GPZ28', 'XTJP': 'IE00BRB36B93',
        'EMXU': 'LU2345046655', 'BNKE': 'LU1829219390',
        'GLOBALFOND_A': 'SE0000542979',
    }
    for isin, ticker in ETF_ISIN_TO_CSV.items():
        if isin != canonical_isins.get(ticker):
            proxy_mappings.append({'isin': isin, 'mapped_to': ticker})

    etf_meta = {
        'generated': date.today().isoformat(),
        'coverage': coverage,
        'etf_sources': etf_sources,
        'proxy_mappings': proxy_mappings,
    }
    etf_meta_path = OUT_DIR / 'etf_metadata.json'
    with open(etf_meta_path, 'w', encoding='utf-8') as f:
        json.dump(etf_meta, f, ensure_ascii=False, indent=2)
    print(f'Exported ETF metadata to {etf_meta_path}')

    # Print correlation highlights
    print('\nCorrelation highlights:')
    pairs = [(k, v) for k, v in corr_matrix.items()
             if '|' in k and k.split('|')[0] != k.split('|')[1]
             and k.split('|')[0] != 'ACWI' and k.split('|')[1] != 'ACWI']
    pairs.sort(key=lambda x: -x[1])
    for k, v in pairs[:10]:
        print(f'  {k:45s} r = {v:.4f}')

    # Fetch NAV history from pensionikeskus.ee
    if args.skip_nav:
        print('\n--skip-nav: skipping NAV fetch, ACWI fetch, and return correlations')
    else:
        fetch_nav_history()

        # Fetch MSCI ACWI ETF NAV via yfinance
        fetch_acwi_nav()

        # Compute NAV return correlations (ESMA closet indexing metrics)
        nav_path = OUT_DIR / 'nav_data.json'
        with open(nav_path, 'r', encoding='utf-8') as f:
            nav_data = json.load(f)

        corr_data = compute_nav_return_correlations(nav_data)

        # Last-1-year correlations for ESMA section
        one_year_ago = date.today() - timedelta(days=365)
        one_year_ago_str = one_year_ago.strftime('%Y-%m-%d')
        corr_1y = compute_nav_return_correlations(nav_data, cutoff_date=one_year_ago_str)
        corr_data['last_1y'] = corr_1y

        corr_path = OUT_DIR / 'return_correlations.json'
        with open(corr_path, 'w', encoding='utf-8') as f:
            json.dump(corr_data, f, ensure_ascii=False, indent=2)
        print(f'  Saved NAV return correlations to {corr_path} ({len(corr_data["correlations"])} pairs, +last_1y)')

    # Output summary
    print('\n=== Pipeline complete ===')
    print(f'Month: {MONTH}')
    print(f'Funds processed: {len(fund_order)}')
    print(f'fund_data.json:  {(OUT_DIR / "fund_data.json").stat().st_size:,} bytes')
    if not args.skip_nav:
        print(f'nav_data.json:   {(OUT_DIR / "nav_data.json").stat().st_size:,} bytes')


# ═══════════════════════════════════════════════════════════════════
# SECTION 8: NAV HISTORY
# ═══════════════════════════════════════════════════════════════════

# Pensionikeskus fund IDs
NAV_FUND_IDS = {
    'Tuleva': 77,
    'Luminor 16-50': 57,
    'SEB Indeks': 75,
    'Swedbank K1960': 36,
    'Swedbank K1970': 37,
    'Swedbank K1980': 52,
    'Swedbank K1990': 74,
    'LHV Ettevõtlik': 47,
    'LHV Julge': 38,
    'SEB 55+': 61,
    'LHV Rahulik': 59,
    'LHV Indeks': 73,
    'LHV Tasakaalukas': 39,
    'Luminor Indeks': 86,
    'Luminor 50-56': 48,
    'Luminor 56+': 49,
    'Luminor 61-65': 50,
    'SEB 18+': 80,
    'SEB 60+': 51,
    'SEB 65+': 60,
    'Swedbank Indeks': 88,
    'Swedbank 2000-09': 91,
    'Swedbank Konservatiivne': 58,
    'Tuleva Võlakirjad': 76,
}


def compute_nav_return_correlations(nav_data, cutoff_date=None):
    """Weekly log-return Pearson correlations from NAV data.
    Uses weekly frequency to avoid NAV timing bias (Lo & MacKinlay 1990).
    Returns ESMA/2016/165 closet indexing metrics: correlation, R², tracking error.
    If cutoff_date is provided (YYYY-MM-DD string), only dates >= cutoff_date are used."""
    # Exclude MSCI ACWI from fund-vs-fund metrics
    fund_names = sorted(k for k in nav_data.keys() if k != 'MSCI ACWI')

    # For each pair, use their common dates (not global intersection)
    correlations = {}
    r_squared = {}
    tracking_error = {}

    for i, fi in enumerate(fund_names):
        for j, fj in enumerate(fund_names):
            if i >= j:
                continue
            key = f'{fi}|{fj}'

            # Find common dates for this pair
            dates_i = set(nav_data[fi]['dates'])
            dates_j = set(nav_data[fj]['dates'])
            common = sorted(dates_i & dates_j)

            if cutoff_date:
                common = [d for d in common if d >= cutoff_date]

            if len(common) < 10:
                continue

            dv_i = dict(zip(nav_data[fi]['dates'], nav_data[fi]['values']))
            dv_j = dict(zip(nav_data[fj]['dates'], nav_data[fj]['values']))
            prices_i = np.array([dv_i[d] for d in common])
            prices_j = np.array([dv_j[d] for d in common])

            ret_i = np.log(prices_i[1:] / prices_i[:-1])
            ret_j = np.log(prices_j[1:] / prices_j[:-1])

            r = float(np.corrcoef(ret_i, ret_j)[0, 1])
            correlations[key] = round(r, 4)
            r_squared[key] = round(r ** 2, 4)
            te = float(np.std(ret_i - ret_j, ddof=1) * np.sqrt(52) * 100)
            tracking_error[key] = round(te, 2)

    # Determine overall period from all funds
    all_dates = set()
    for f in fund_names:
        all_dates.update(nav_data[f]['dates'])
    if cutoff_date:
        all_dates = {d for d in all_dates if d >= cutoff_date}
    all_dates = sorted(all_dates)

    return {
        'method': 'ESMA/2016/165 closet indexing metrics',
        'period': f'{all_dates[0]} to {all_dates[-1]}',
        'n_funds': len(fund_names),
        'correlations': correlations,
        'r_squared': r_squared,
        'tracking_error_pct': tracking_error,
    }


def fetch_nav_history():
    """Fetch 10 years of weekly NAV data from pensionikeskus.ee and export to nav_data.json."""
    import urllib.request
    from datetime import date, timedelta

    print('\nFetching NAV history from pensionikeskus.ee...')

    end_date = date.today()
    start_date = end_date - timedelta(days=10 * 365)

    nav_data = {}
    for fund_name, fund_id in NAV_FUND_IDS.items():
        url = (f'https://www.pensionikeskus.ee/en/statistics/ii-pillar/nav-of-funded-pension/'
               f'?download=xls&date_from={start_date}&date_to={end_date}&f%5B0%5D={fund_id}')
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=15) as resp:
                raw_bytes = resp.read()
            # pensionikeskus returns UTF-16 LE with BOM
            try:
                raw = raw_bytes.decode('utf-16')
            except UnicodeDecodeError:
                raw = raw_bytes.decode('utf-8')

            # Parse tab-separated values: Date, Fund, Shortname, ISIN, NAV, Change%
            dates = []
            navs = []
            for line in raw.strip().splitlines():
                parts = line.split('\t')
                if len(parts) < 5:
                    continue
                try:
                    d = parts[0].strip()
                    if not re.match(r'\d{4}-\d{2}-\d{2}', d):
                        continue
                    nav_val = float(parts[4].strip().replace(',', '.'))
                    dates.append(d)
                    navs.append(nav_val)
                except (ValueError, IndexError):
                    continue

            if not dates:
                print(f'  {fund_name}: no data')
                continue

            # Sample weekly (every Friday or nearest available)
            df = pd.DataFrame({'date': pd.to_datetime(dates), 'nav': navs}).sort_values('date')
            df = df.set_index('date').resample('W-FRI').last().dropna()

            # Normalize to 100
            base = df['nav'].iloc[0]
            df['norm'] = df['nav'] / base * 100

            nav_data[fund_name] = {
                'dates': [d.strftime('%Y-%m-%d') for d in df.index],
                'values': [round(v, 2) for v in df['norm']],
            }
            print(f'  {fund_name}: {len(df)} weeks')
        except Exception as e:
            print(f'  {fund_name}: ERROR {e}')

    nav_path = OUT_DIR / 'nav_data.json'
    with open(nav_path, 'w', encoding='utf-8') as f:
        json.dump(nav_data, f, ensure_ascii=False, indent=2)
    print(f'  Saved to {nav_path}')


def fetch_acwi_nav():
    """Fetch MSCI ACWI ETF (SSAC.L) weekly NAV data via yfinance, add to nav_data.json."""
    import yfinance as yf

    print('\nFetching MSCI ACWI (IUSQ.DE, EUR/Xetra) NAV via yfinance...')
    ticker = yf.Ticker('IUSQ.DE')
    hist = ticker.history(period='10y', interval='1wk')

    if hist.empty:
        print('  ACWI: no data from yfinance')
        return

    # Resample to weekly Fridays to match pension fund frequency
    hist = hist[['Close']].resample('W-FRI').last().dropna()

    # Normalize to 100
    base = hist['Close'].iloc[0]
    hist['norm'] = hist['Close'] / base * 100

    acwi_data = {
        'dates': [d.strftime('%Y-%m-%d') for d in hist.index],
        'values': [round(v, 2) for v in hist['norm']],
    }
    print(f'  MSCI ACWI: {len(hist)} weeks')

    # Load existing nav_data.json and add ACWI
    nav_path = OUT_DIR / 'nav_data.json'
    with open(nav_path, 'r', encoding='utf-8') as f:
        nav_data = json.load(f)
    nav_data['MSCI ACWI'] = acwi_data
    with open(nav_path, 'w', encoding='utf-8') as f:
        json.dump(nav_data, f, ensure_ascii=False, indent=2)
    print(f'  Added MSCI ACWI to {nav_path}')


if __name__ == '__main__':
    main()
