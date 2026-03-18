"""
V2 pipeline: Standardized parsing → validation → universal processing.

Every fund goes through the same three steps:
  1. parse_fund() → standardized dict (same schema for all 24 funds)
  2. validate_parsed_fund() → catches missing keys, weight mismatches, empty arrays
  3. process_fund() → one universal function for ETF lookthrough, stock merging, JSON output

Imports heavy lifting (ETF loading, lookthrough engine, normalization) from v1.
"""
import argparse
import json
import re
from datetime import date
from pathlib import Path

import pandas as pd

# Import shared infrastructure (constants, ETF loading, lookthrough engine, etc.)
from pipeline_shared import (
    _pct, _extract_eur_value, _extract_deposit_eur,
    ISIN_RE, REPORT_DIR, OUT_DIR, COUNTRY_MAP,
    ETF_ISIN_TO_CSV, OPAQUE_FUND_ISINS, TRUE_PROXY_ISINS,
    LUMINOR_ETF_PROXY_MAP, ISHARES_PRODUCTS, EODHD_ETFS, fetch_ishares_holdings, fetch_eodhd_holdings, load_manual_holdings,
    build_lookthrough, build_acwi, build_ssac_em,
    normalize_company_name, _build_sector_lookup_with_fuzzy,
    fund_to_json, build_etf_breakdown,
    compute_pairwise_correlations,
    load_monthly_config,
    fetch_pensionikeskus_aum,
    # Existing parsers (wrapped by v2 parsers)
    parse_tuleva_monthly, parse_tuleva_bond_monthly,
    parse_swedbank_monthly, parse_seb_indeks_monthly, parse_seb_pdf, parse_lhv_monthly,
    compute_nav_return_correlations, fetch_nav_history, fetch_acwi_nav,
)

BASE = Path('.')
PARSED_DIR = BASE / 'data' / 'parsed'

# ═══════════════════════════════════════════════════════════════════
# FUND REGISTRY: maps fund_key → parsing + metadata config
# ═══════════════════════════════════════════════════════════════════

FUND_REGISTRY = [
    # (fund_key, display_name, provider, fund_type, report_config_key, pdf_code)
    ('Tuleva', 'Tuleva Maailma Aktsiad', 'Tuleva', 'index', 'Tuleva', 'TUK75'),
    ('Luminor 16-50', 'Luminor 16-50', 'Luminor', 'mixed', 'Luminor 16-50', 'NPK75'),
    ('SEB Indeks', 'SEB Indeks', 'SEB', 'index', 'SEB Indeks', 'SIK75'),
    ('Swedbank K1960', 'Swedbank K1960', 'Swedbank', 'mixed', 'Swedbank K1960', None),
    ('Swedbank K1970', 'Swedbank K1970', 'Swedbank', 'mixed', 'Swedbank K1970', None),
    ('Swedbank K1980', 'Swedbank K1980', 'Swedbank', 'mixed', 'Swedbank K1980', None),
    ('Swedbank K1990', 'Swedbank K1990', 'Swedbank', 'mixed', 'Swedbank K1990', None),
    ('LHV Ettevõtlik', 'LHV Ettevõtlik', 'LHV', 'active', 'LHV Ettevõtlik', 'LLK50'),
    ('LHV Julge', 'LHV Julge', 'LHV', 'active', 'LHV Julge', 'LXK75'),
    ('SEB 55+', 'SEB 55+', 'SEB', 'active', 'SEB 55+', 'SEK50'),
    ('LHV Rahulik', 'LHV Rahulik', 'LHV', 'conservative', 'LHV Rahulik', 'LXK00'),
    ('LHV Indeks', 'LHV Indeks', 'LHV', 'index', 'LHV Indeks', 'LIK75'),
    ('LHV Tasakaalukas', 'LHV Tasakaalukas', 'LHV', 'active', 'LHV Tasakaalukas', 'LMK25'),
    ('Luminor Indeks', 'Luminor Indeks', 'Luminor', 'index', 'Luminor Indeks', 'NIK100'),
    ('Luminor 50-56', 'Luminor 50-56', 'Luminor', 'mixed', 'Luminor 50-56', 'NPK50'),
    ('Luminor 56+', 'Luminor 56+', 'Luminor', 'mixed', 'Luminor 56+', 'NPK25'),
    ('Luminor 61-65', 'Luminor 61-65', 'Luminor', 'conservative', 'Luminor 61-65', 'NPK00'),
    ('SEB 18+', 'SEB 18+', 'SEB', 'active', 'SEB 18+', 'SEK100'),
    ('SEB 60+', 'SEB 60+', 'SEB', 'mixed', 'SEB 60+', 'SEK25'),
    ('SEB 65+', 'SEB 65+', 'SEB', 'conservative', 'SEB 65+', 'SEK00'),
    ('Swedbank Indeks', 'Swedbank Indeks', 'Swedbank', 'index', 'Swedbank Indeks', None),
    ('Swedbank 2000-09', 'Swedbank 2000-09', 'Swedbank', 'mixed', 'Swedbank 2000-09', None),
    ('Swedbank Konservatiivne', 'Swedbank Konservatiivne', 'Swedbank', 'conservative', 'Swedbank Konservatiivne', None),
    ('Tuleva Võlakirjad', 'Tuleva Võlakirjad', 'Tuleva', 'bond', 'Tuleva Võlakirjad', 'TUK00'),
]

# Swedbank PDF filename mapping (not pensionikeskus format)
SWEDBANK_PDF_NAMES = {
    'Swedbank K1960': 'K1960_investment_portfolio.pdf',
    'Swedbank K1970': 'K1970_investment_portfolio.pdf',
    'Swedbank K1980': 'K1980_investment_portfolio.pdf',
    'Swedbank K1990': 'K1990_investment_portfolio.pdf',
    'Swedbank Indeks': 'Ki_investment_portfolio.pdf',
    'Swedbank 2000-09': 'K2000_investment_portfolio.pdf',
    'Swedbank Konservatiivne': 'KKONS_investment_portfolio.pdf',
}

# Management fees (% per year) — single source of truth
FEES = {
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


# ═══════════════════════════════════════════════════════════════════
# STEP 2: STANDARDIZED PARSERS
# ═══════════════════════════════════════════════════════════════════

def _empty_parsed(fund_key, provider, fund_type, month, source=''):
    """Create empty standardized parsed dict."""
    return {
        'fund_name': fund_key,
        'fund_key': fund_key,
        'provider': provider,
        'fund_type': fund_type,
        'month': month,
        'source': source,
        'equity_funds': [],
        'stocks': [],
        'bonds': [],
        'bond_funds': [],
        'pe_funds': [],
        're_funds': [],
        'deposits_pct': 0.0,
        'derivatives_pct': 0.0,
    }


def parse_fund(fund_key, provider, fund_type, month, pdf_path, alloc_cfg_entry=None):
    """Parse any fund into the standardized format.

    If alloc_cfg_entry is provided (from monthly JSON), use that instead of parsing PDF.
    Otherwise, dispatch to the appropriate PDF parser.
    """
    source = pdf_path.name if pdf_path else ''
    parsed = _empty_parsed(fund_key, provider, fund_type, month, source)

    # If monthly JSON has allocations for this fund, use them
    if alloc_cfg_entry is not None:
        return _from_monthly_json(parsed, alloc_cfg_entry)

    # Dispatch by provider
    if provider == 'Tuleva':
        if fund_type == 'bond':
            return _parse_tuleva_bond(parsed, pdf_path)
        return _parse_tuleva(parsed, pdf_path)
    elif provider == 'Swedbank':
        return _parse_swedbank(parsed, pdf_path)
    elif provider == 'LHV':
        return _parse_lhv(parsed, pdf_path)
    elif provider == 'SEB':
        if fund_key == 'SEB Indeks':
            return _parse_seb_indeks(parsed, pdf_path)
        return _parse_seb(parsed, pdf_path)
    elif provider == 'Luminor':
        return _parse_luminor(parsed, pdf_path)
    else:
        raise ValueError(f"Unknown provider: {provider}")


def _from_monthly_json(parsed, alloc):
    """Convert monthly JSON allocation entry to standardized format.

    Some funds (SEB Indeks) store allocations as a plain list of equity funds.
    Others store a dict with equity_funds, stocks, bonds, etc.
    """
    # Handle plain list format (e.g. SEB Indeks: just a list of ETF allocations)
    if isinstance(alloc, list):
        parsed['equity_funds'] = alloc
        parsed['source'] = 'monthly JSON'
        return parsed

    parsed['equity_funds'] = alloc.get('equity_funds', [])
    parsed['stocks'] = alloc.get('stocks', [])
    parsed['bonds'] = alloc.get('bonds', [])
    parsed['bond_funds'] = alloc.get('bond_funds', [])
    parsed['pe_funds'] = alloc.get('pe_funds', [])
    parsed['re_funds'] = alloc.get('re_funds', [])
    parsed['deposits_pct'] = alloc.get('deposits_pct', 0.0)
    parsed['derivatives_pct'] = alloc.get('derivatives_pct', 0.0)
    # Handle direct_bond_pct (SEB funds with unitemized bonds)
    direct_bond_pct = alloc.get('direct_bond_pct', 0)
    if direct_bond_pct > 0:
        parsed['bonds'].append({
            'name': f"Otseobligatsioonid ({direct_bond_pct}%)",
            'weight_pct': direct_bond_pct,
        })
    parsed['source'] = 'monthly JSON'
    return parsed


def _parse_tuleva(parsed, pdf_path):
    """Wrap existing Tuleva parser into standardized format."""
    raw = parse_tuleva_monthly(pdf_path)
    parsed['equity_funds'] = raw['allocations']
    parsed['deposits_pct'] = raw.get('deposits_pct', 0.07)
    parsed['_pdf_subtotals'] = raw.get('_pdf_subtotals', {})
    parsed['_pdf_holding_counts'] = raw.get('_pdf_holding_counts', {})
    parsed['_total_value_eur'] = raw.get('_total_value_eur', 0)
    return parsed


def _parse_tuleva_bond(parsed, pdf_path):
    """Wrap existing Tuleva bond parser into standardized format."""
    raw = parse_tuleva_bond_monthly(pdf_path)
    parsed['bond_funds'] = raw['bond_funds']
    parsed['deposits_pct'] = raw.get('deposits_pct', 0)
    parsed['_pdf_subtotals'] = raw.get('_pdf_subtotals', {})
    parsed['_pdf_holding_counts'] = raw.get('_pdf_holding_counts', {})
    parsed['_total_value_eur'] = raw.get('_total_value_eur', 0)
    return parsed


def _parse_swedbank(parsed, pdf_path):
    """Wrap existing Swedbank parser into standardized format."""
    raw = parse_swedbank_monthly(pdf_path)
    parsed['stocks'] = raw.get('stocks', [])
    parsed['bonds'] = raw.get('bonds', [])
    parsed['equity_funds'] = raw.get('equity_funds', [])
    parsed['bond_funds'] = raw.get('bond_funds', [])
    parsed['pe_funds'] = raw.get('pe_funds', [])
    parsed['re_funds'] = raw.get('re_funds', [])
    parsed['deposits_pct'] = raw.get('deposits_pct', 0)
    parsed['derivatives_pct'] = raw.get('derivatives_pct', 0)
    parsed['_pdf_subtotals'] = raw.get('_pdf_subtotals', {})
    parsed['_pdf_holding_counts'] = raw.get('_pdf_holding_counts', {})
    parsed['_total_value_eur'] = raw.get('_total_value_eur', 0)
    return parsed


def _parse_lhv(parsed, pdf_path):
    """Wrap existing LHV parser into standardized format.

    LHV parser returns a different format (holdings list with type field).
    We split it into the standardized arrays.
    """
    raw = parse_lhv_monthly(pdf_path)

    # Split holdings into stocks and equity_funds
    for h in raw.get('holdings', []):
        if h.get('type') == 'stocks':
            entry = {
                'name': h['name'],
                'isin': h.get('isin'),
                'weight_pct': h['weight'],
                'country': h.get('country', ''),
            }
            if h.get('value_eur'):
                entry['value_eur'] = h['value_eur']
            parsed['stocks'].append(entry)
        elif h.get('type') == 'etfs':
            entry = {
                'name': h['name'],
                'isin': h.get('isin', ''),
                'weight_pct': h['weight'],
            }
            if h.get('value_eur'):
                entry['value_eur'] = h['value_eur']
            parsed['equity_funds'].append(entry)

    # Bonds
    for h in raw.get('bond_holdings', []):
        entry = {
            'name': h['name'],
            'isin': h.get('isin'),
            'weight_pct': h['weight'],
        }
        if h.get('value_eur'):
            entry['value_eur'] = h['value_eur']
        parsed['bonds'].append(entry)

    # PE funds
    for h in raw.get('pe_holdings', []):
        entry = {
            'name': h['name'],
            'weight_pct': h['weight'],
        }
        if h.get('value_eur'):
            entry['value_eur'] = h['value_eur']
        parsed['pe_funds'].append(entry)

    # RE funds
    for h in raw.get('re_holdings', []):
        entry = {
            'name': h['name'],
            'weight_pct': h['weight'],
        }
        if h.get('value_eur'):
            entry['value_eur'] = h['value_eur']
        parsed['re_funds'].append(entry)

    parsed['deposits_pct'] = raw.get('deposits_pct', 0)
    parsed['_pdf_subtotals'] = raw.get('_pdf_subtotals', {})
    parsed['_pdf_holding_counts'] = raw.get('_pdf_holding_counts', {})
    parsed['_total_value_eur'] = raw.get('_total_value_eur', 0)
    return parsed


def _parse_seb_indeks(parsed, pdf_path):
    """Parse SEB Indeks from PDF using word-level extraction."""
    return _parse_seb(parsed, pdf_path)


def _parse_seb(parsed, pdf_path):
    """Parse any SEB fund PDF using word-level coordinate extraction.

    Uses parse_seb_pdf() which extracts names, ISINs, percentages, and EUR
    values by matching words at the same y-coordinate across columns.
    Works for both inline (SEB 18+, 65+) and multi-column (Indeks, 55+, 60+) layouts.
    """
    raw = parse_seb_pdf(pdf_path)

    if not raw or not raw.get('equity_funds'):
        print('  SEB PDF: no equity funds parsed from PDF')

    total_value_eur = 0
    for section_key in ['equity_funds', 'stocks', 'bonds', 'bond_funds', 'pe_funds', 're_funds']:
        for entry in raw.get(section_key, []):
            if entry.get('value_eur'):
                total_value_eur += entry['value_eur']

    parsed['equity_funds'] = raw.get('equity_funds', [])
    parsed['stocks'] = raw.get('stocks', [])
    parsed['bonds'] = raw.get('bonds', [])
    parsed['bond_funds'] = raw.get('bond_funds', [])
    parsed['pe_funds'] = raw.get('pe_funds', [])
    parsed['re_funds'] = raw.get('re_funds', [])
    parsed['deposits_pct'] = raw.get('deposits_pct', 0.0)
    parsed['derivatives_pct'] = raw.get('derivatives_pct', 0.0)
    parsed['_total_value_eur'] = total_value_eur
    return parsed


def _parse_luminor(parsed, pdf_path):
    """Parse all Luminor funds from PDF into standardized format.

    Handles:
    - Multi-line fund names (fund name, manager name, and country on separate lines)
    - Fund sections: Aktsiafondid, Võlakirjafondid, Kinnisvarafondid, Erakapitalifond
    - Direct bonds section (Võlakirjad — different column layout with ISIN)
    - Deposits from Arvelduskonto line
    """
    import pdfplumber

    equity_funds = []
    bond_funds = []
    bonds = []
    re_funds = []
    pe_funds = []
    deposits_pct = 0.0
    pdf_subtotals = {}
    pdf_holding_counts = {}
    total_value_eur = 0

    # Section types for fund-style sections (no ISIN column)
    FUND_SECTIONS = {'equity', 'bond_fund', 'real_estate', 'pe'}
    current_section = None  # None, or one of FUND_SECTIONS, or 'bonds'
    pending_lines = []  # accumulate multi-line entries

    # Map Luminor section names to standardized keys
    _LUM_SECTION_KEY = {
        'equity': 'equity_funds', 'bond_fund': 'bond_funds',
        'real_estate': 're_funds', 'pe': 'pe_funds', 'bonds': 'bonds',
    }

    # Known manager fragments to strip from fund names
    MANAGERS = [
        'BlackRock Asset Management Ireland Limited',
        'BlackRock Asset Management',
        'BlackRock (Luxembourg) S.A.',
        'Blackrock Luxembourg SA',
        'Management Ireland Limited',
        'Ireland Limited',
        'Amundi Asset Management S.A.S.',
        'Amundi Asset Management',
        'Amundi Ireland Limited',
        'Robeco Institutional Asset Management B.V.',
        'Robeco Luxembourg SA',
        'SSGA SPDR ETFS Europe Plc',
        'BNP Paribas Asset Management',
        'Neuberger Berman Asset Management Ireland Ltd',
        'Xtrackers IE Plc',
        'Xtrackers II',
        'Eften Capital AS',
        'SIA Livonia Partners AIFP',
        'Raft Capital Management UAB',
        'UAB INVL Asset Management',
        'Van Eck Associates',
        'Global X',
    ]

    def _clean_name(raw):
        """Clean fund name by removing manager fragments and artifacts."""
        result = raw
        for m in MANAGERS:
            result = result.replace(m, '')
        # Remove footnote markers
        result = re.sub(r'\d+$', '', result)
        result = re.sub(r'^1\s+', '', result)
        # Clean whitespace
        result = re.sub(r'\s+', ' ', result).strip()
        # Remove leading/trailing punctuation
        result = result.strip('- ,.')
        return result

    def _flush_fund_entry(lines_block, section):
        """Parse a multi-line fund entry into a name + weight_pct + value_eur dict."""
        if not lines_block:
            return None

        # Join all lines
        full_text = ' '.join(lines_block)

        # Find the last percentage
        pct_m = re.findall(r'(\d+[\.,]\d+)\s*%', full_text)
        if not pct_m:
            return None
        weight = _pct(pct_m[-1] + '%')
        if weight <= 0:
            return None

        # Extract EUR market value from the full text
        value_eur = _extract_eur_value(full_text)

        # Remove everything from the country code + currency onwards
        # Pattern: 2-letter country + EUR/USD/GBP/etc + numbers
        name_match = re.match(
            r'^(.+?)\s+(?:[A-Z]{2})\s+(?:EUR|USD|GBP|JPY|CHF|SEK|DKK|NOK)\s',
            full_text
        )
        if name_match:
            raw_name = name_match.group(1).strip()
        else:
            # Fallback: take everything before the weight percentage
            raw_name = full_text.split(str(pct_m[-1]).replace('.', ','))[0]
            raw_name = re.sub(r'[\d,.]+ [\d,.]+ [\d,.]+.*$', '', raw_name)

        name = _clean_name(raw_name)
        if not name or len(name) < 3:
            return None

        entry = {'name': name, 'weight_pct': weight}
        if value_eur:
            entry['value_eur'] = value_eur
        return entry

    def _flush_bond_entry(lines_block):
        """Parse a multi-line direct bond entry.

        Bond entries span 2-3 lines:
        - Name line: "AB Artea bankas 10.75 Ba1" or "ESTONIA 3.25"
        - Data line: "ISIN Country Currency Amount Price Value ... YTM% Weight%"
        - Optional tail: date or rating continuation

        The data line has TWO percentages; the last is the fund weight.
        """
        if not lines_block:
            return None

        # Find the data line — it contains the ISIN and/or country+currency pattern
        # and has at least two percentage values
        data_line = None
        name_parts = []
        isin = None

        for line in lines_block:
            # Check if line has ISIN
            isin_match = ISIN_RE.search(line)
            # Check for percentage values
            pcts = re.findall(r'(\d+[\.,]\d+)\s*%', line)

            if len(pcts) >= 2:
                # This is the data line (has YTM% and Weight%)
                data_line = line
                if isin_match:
                    isin = isin_match.group(0)
                break
            elif isin_match and len(pcts) >= 1:
                # Single-line entry with ISIN (some bonds fit on one line)
                data_line = line
                isin = isin_match.group(0)
                break
            else:
                # Name/rating continuation line
                name_parts.append(line)

        if data_line is None:
            return None

        # Extract fund weight (last percentage on data line)
        pcts = re.findall(r'(\d+[\.,]\d+)\s*%', data_line)
        if not pcts:
            return None
        weight = _pct(pcts[-1] + '%')
        if weight <= 0 or weight > 50:
            return None

        # Extract bond name
        if isin and isin in data_line:
            # Name from the data line: text between ISIN and country code
            after_isin = data_line[data_line.index(isin) + len(isin):].strip()
            # Name is before "XX EUR" pattern (country + currency)
            nm = re.match(r'^(.+?)\s+(?:[A-Z]{2})\s+(?:EUR|USD|GBP)', after_isin)
            if nm:
                data_name = nm.group(1).strip()
            else:
                data_name = ''
        else:
            data_name = ''

        # Combine name parts (from lines before data line) with any name from data line
        if name_parts:
            name = ' '.join(name_parts).strip()
            if data_name:
                name = name + ' ' + data_name
        elif data_name:
            name = data_name
        else:
            name = 'Unknown bond'

        # Clean up name
        name = re.sub(r'\s+(Ba\d|Baa\d|B\d|A\d|Aa\d|AAA|AA\+|AA-|A\+|A-|BB\+|BBB\+|BBB-)\b.*', '', name)
        name = re.sub(r'\s+\((Moody\'s|Fitch|S&P)\).*', '', name)
        name = re.sub(r'\s+', ' ', name).strip()
        if not name or len(name) < 2:
            return None

        # Extract EUR market value from the data line
        value_eur = _extract_eur_value(data_line) if data_line else None

        entry = {'name': name, 'weight_pct': weight}
        if isin:
            entry['isin'] = isin
        if value_eur:
            entry['value_eur'] = value_eur
        return entry

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text() or ''
            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue

                # Section headers
                if line == 'Aktsiafondid':
                    # Flush any pending entry from previous section
                    if pending_lines and current_section in FUND_SECTIONS:
                        entry = _flush_fund_entry(pending_lines, current_section)
                        if entry:
                            if current_section == 'equity':
                                equity_funds.append(entry)
                            elif current_section == 'bond_fund':
                                bond_funds.append(entry)
                    pending_lines = []
                    current_section = 'equity'
                    continue
                if line.startswith('Aktsiafondid kokku'):
                    if pending_lines:
                        entry = _flush_fund_entry(pending_lines, current_section)
                        if entry:
                            equity_funds.append(entry)
                    pct_m = re.findall(r'(\d+[\.,]\d+)\s*%', line)
                    if pct_m:
                        pdf_subtotals['equity_funds'] = _pct(pct_m[0] + '%')
                    pending_lines = []
                    current_section = None
                    continue
                if line.startswith('Võlakirjafondid') and 'kokku' not in line:
                    if pending_lines and current_section in FUND_SECTIONS:
                        entry = _flush_fund_entry(pending_lines, current_section)
                        if entry:
                            if current_section == 'equity':
                                equity_funds.append(entry)
                    pending_lines = []
                    current_section = 'bond_fund'
                    continue
                if line.startswith('Võlakirjafondid kokku'):
                    if pending_lines:
                        entry = _flush_fund_entry(pending_lines, current_section)
                        if entry:
                            bond_funds.append(entry)
                    pct_m = re.findall(r'(\d+[\.,]\d+)\s*%', line)
                    if pct_m:
                        pdf_subtotals['bond_funds'] = _pct(pct_m[0] + '%')
                    pending_lines = []
                    current_section = None
                    continue
                if line.startswith('Kinnisvarafondid') and 'kokku' not in line:
                    if pending_lines and current_section in FUND_SECTIONS:
                        entry = _flush_fund_entry(pending_lines, current_section)
                        if entry:
                            if current_section == 'bond_fund':
                                bond_funds.append(entry)
                    pending_lines = []
                    current_section = 'real_estate'
                    continue
                if 'Kinnisvarafondid' in line and 'kokku' in line:
                    if pending_lines:
                        entry = _flush_fund_entry(pending_lines, current_section)
                        if entry:
                            re_funds.append(entry)
                    pct_m = re.findall(r'(\d+[\.,]\d+)\s*%', line)
                    if pct_m:
                        pdf_subtotals['re_funds'] = _pct(pct_m[0] + '%')
                    pending_lines = []
                    current_section = None
                    continue
                if line.startswith('Erakapitalifond') and 'kokku' not in line:
                    if pending_lines and current_section in FUND_SECTIONS:
                        entry = _flush_fund_entry(pending_lines, current_section)
                        if entry:
                            if current_section == 'real_estate':
                                re_funds.append(entry)
                    pending_lines = []
                    current_section = 'pe'
                    continue
                if line.startswith('Erakapitalifond') and 'kokku' in line:
                    if pending_lines:
                        entry = _flush_fund_entry(pending_lines, current_section)
                        if entry:
                            pe_funds.append(entry)
                    pct_m = re.findall(r'(\d+[\.,]\d+)\s*%', line)
                    if pct_m:
                        pdf_subtotals['pe_funds'] = _pct(pct_m[0] + '%')
                    pending_lines = []
                    current_section = None
                    continue

                # Direct bonds section (different from bond funds!)
                if line == 'Võlakirjad':
                    if pending_lines and current_section in FUND_SECTIONS:
                        entry = _flush_fund_entry(pending_lines, current_section)
                        if entry:
                            if current_section == 'bond_fund':
                                bond_funds.append(entry)
                    pending_lines = []
                    current_section = 'bonds'
                    continue
                if line.startswith('Võlakirjad kokku'):
                    if pending_lines:
                        entry = _flush_bond_entry(pending_lines)
                        if entry:
                            bonds.append(entry)
                    pct_m = re.findall(r'(\d+[\.,]\d+)\s*%', line)
                    if pct_m:
                        pdf_subtotals['bonds'] = _pct(pct_m[0] + '%')
                    pending_lines = []
                    current_section = None
                    continue

                # Deposits
                if line.startswith('Arvelduskonto'):
                    pct_m = re.findall(r'(\d+[\.,]\d+)\s*%', line)
                    if pct_m:
                        deposits_pct = _pct(pct_m[-1] + '%')
                    # Extract deposit EUR value
                    dep_val = _extract_eur_value(line)
                    if dep_val:
                        total_value_eur += dep_val
                    continue

                # Skip non-data lines
                if any(x in line for x in ['Osakaalu muutus', 'Investeeringu nimetus',
                                            'Fondivalitseja nimi', 'Keskmine', 'Turuväärtus',
                                            'Pensionifondidesse', 'Luminor Pensions', 'koguses',
                                            'puhas-', 'kokku**', 'ühikule', 'väärtusest',
                                            'ISIN', 'Reiting', 'aegumiseni', 'Tootlus',
                                            'Investeeringute aruanne', 'Osakaal',
                                            '* -', '** -', 'Muud nõuded', 'Laekumata',
                                            'Varade turuväärtus', 'Fondi kohustused',
                                            'Fondi puhasväärtus', '¹ Investeering',
                                            'Hoiuse algus', 'Hoiuse lõpp', 'Hoiustatud',
                                            'Krediidiasutuse', 'Intressimäär']):
                    continue

                if current_section is None:
                    continue

                # In fund sections: check if this line has a percentage (= end of entry)
                if current_section in FUND_SECTIONS:
                    pct_m = re.findall(r'(\d+[\.,]\d+)\s*%', line)
                    if pct_m:
                        # This line completes an entry — count it
                        sk = _LUM_SECTION_KEY.get(current_section)
                        if sk:
                            pdf_holding_counts[sk] = pdf_holding_counts.get(sk, 0) + 1
                        pending_lines.append(line)
                        entry = _flush_fund_entry(pending_lines, current_section)
                        if entry:
                            if entry.get('value_eur'):
                                total_value_eur += entry['value_eur']
                            if current_section == 'equity':
                                equity_funds.append(entry)
                            elif current_section == 'bond_fund':
                                bond_funds.append(entry)
                            elif current_section == 'real_estate':
                                re_funds.append(entry)
                            elif current_section == 'pe':
                                pe_funds.append(entry)
                        pending_lines = []
                    else:
                        # Continuation line (name wraps)
                        pending_lines.append(line)

                elif current_section == 'bonds':
                    pct_m = re.findall(r'(\d+[\.,]\d+)\s*%', line)
                    # Data lines have 2+ percentages (YTM% and Weight%)
                    # Name lines may have 1 percentage (coupon like "4.853%")
                    if len(pct_m) >= 2:
                        pdf_holding_counts['bonds'] = pdf_holding_counts.get('bonds', 0) + 1
                        pending_lines.append(line)
                        entry = _flush_bond_entry(pending_lines)
                        if entry:
                            if entry.get('value_eur'):
                                total_value_eur += entry['value_eur']
                            bonds.append(entry)
                        pending_lines = []
                    else:
                        pending_lines.append(line)

    parsed['equity_funds'] = equity_funds
    parsed['bonds'] = bonds
    parsed['bond_funds'] = bond_funds
    parsed['pe_funds'] = pe_funds
    parsed['re_funds'] = re_funds
    parsed['deposits_pct'] = deposits_pct
    parsed['_pdf_subtotals'] = pdf_subtotals
    parsed['_pdf_holding_counts'] = pdf_holding_counts
    parsed['_total_value_eur'] = total_value_eur
    return parsed


# ═══════════════════════════════════════════════════════════════════
# STEP 3: VALIDATION
# ═══════════════════════════════════════════════════════════════════

def validate_parsed_fund(parsed, prev_parsed=None, pk_aum=None):
    """Validate a standardized parsed fund dict. Raises ValueError on errors.

    Checks:
    - All 6 arrays present as keys (not relying on .get())
    - Each holding has name (str) and weight_pct (float > 0)
    - stocks entries must have country
    - equity_funds entries should have isin (warning if missing, not error — Luminor doesn't have them)
    - Total weight ~100% (warning if off by >3pp, error if >5pp)
    - CHECK 1: PDF section subtotals vs parsed sums
    - CHECK 2: PDF holding counts vs parsed counts
    - CHECK 3: Sum of parsed EUR values vs pensionikeskus AUM
    - CHECK 4: Enhanced cross-month consistency
    """
    fund_key = parsed.get('fund_key', '?')
    errors = []
    warnings = []

    # Schema: all 6 arrays must be present as keys
    required_arrays = ['equity_funds', 'stocks', 'bonds', 'bond_funds', 'pe_funds', 're_funds']
    for key in required_arrays:
        if key not in parsed:
            errors.append(f"Missing key: {key}")
        elif not isinstance(parsed[key], list):
            errors.append(f"{key} must be a list, got {type(parsed[key])}")

    # Required scalars
    for key in ['deposits_pct', 'derivatives_pct']:
        if key not in parsed:
            errors.append(f"Missing key: {key}")

    if errors:
        raise ValueError(f"[{fund_key}] Schema errors: {'; '.join(errors)}")

    # Validate individual holdings
    for arr_key in required_arrays:
        for i, h in enumerate(parsed[arr_key]):
            if not isinstance(h, dict):
                errors.append(f"{arr_key}[{i}]: not a dict")
                continue
            if 'name' not in h or not isinstance(h.get('name'), str):
                errors.append(f"{arr_key}[{i}]: missing or invalid 'name'")
            if 'weight_pct' not in h:
                errors.append(f"{arr_key}[{i}] ({h.get('name', '?')}): missing 'weight_pct'")
            elif not isinstance(h['weight_pct'], (int, float)):
                errors.append(f"{arr_key}[{i}] ({h.get('name', '?')}): weight_pct must be numeric")

    # stocks must have country
    for i, s in enumerate(parsed['stocks']):
        if 'country' not in s:
            warnings.append(f"stocks[{i}] ({s.get('name', '?')}): missing 'country'")

    # equity_funds should have isin (but Luminor doesn't — just warn)
    provider = parsed.get('provider', '')
    if provider != 'Luminor':
        for i, ef in enumerate(parsed['equity_funds']):
            if not ef.get('isin'):
                warnings.append(f"equity_funds[{i}] ({ef.get('name', '?')}): missing 'isin'")

    if errors:
        raise ValueError(f"[{fund_key}] Holding errors: {'; '.join(errors)}")

    # Compute per-class weight sums
    class_weights = {}
    for arr_key in required_arrays:
        class_weights[arr_key] = sum(h['weight_pct'] for h in parsed[arr_key])
    equity_funds_pct = class_weights['equity_funds']
    stocks_pct = class_weights['stocks']
    bonds_pct = class_weights['bonds']
    bond_funds_pct = class_weights['bond_funds']
    pe_pct = class_weights['pe_funds']
    re_pct = class_weights['re_funds']
    deposits = parsed.get('deposits_pct', 0)
    derivatives = abs(parsed.get('derivatives_pct', 0))
    total = equity_funds_pct + stocks_pct + bonds_pct + bond_funds_pct + pe_pct + re_pct + deposits + derivatives

    if abs(total - 100) > 5:
        warnings.append(
            f"Total weight {total:.1f}% (expected ~100%, >5pp off). "
            f"equity_funds={equity_funds_pct:.1f}, stocks={stocks_pct:.1f}, "
            f"bonds={bonds_pct:.1f}, bond_funds={bond_funds_pct:.1f}, "
            f"pe={pe_pct:.1f}, re={re_pct:.1f}, deposits={deposits:.1f}, derivatives={derivatives:.1f}"
        )
    elif abs(total - 100) > 3:
        warnings.append(
            f"Total weight {total:.1f}% deviates from 100%. "
            f"equity_funds={equity_funds_pct:.1f}, stocks={stocks_pct:.1f}, "
            f"bonds={bonds_pct:.1f}+{bond_funds_pct:.1f}, pe={pe_pct:.1f}, re={re_pct:.1f}"
        )

    # ── CHECK 1: PDF section subtotals vs parsed sums ──
    pdf_subtotals = parsed.get('_pdf_subtotals', {})
    if pdf_subtotals:
        for section_key, pdf_pct in pdf_subtotals.items():
            parsed_pct = class_weights.get(section_key, 0)
            diff = abs(parsed_pct - pdf_pct)
            if diff > 0.5:
                warnings.append(
                    f"Subtotal mismatch: {section_key} parsed={parsed_pct:.2f}% vs PDF kokku={pdf_pct:.2f}% (diff={diff:.2f}pp)"
                )

    # ── CHECK 2: PDF holding counts vs parsed counts ──
    pdf_holding_counts = parsed.get('_pdf_holding_counts', {})
    if pdf_holding_counts:
        for section_key, pdf_count in pdf_holding_counts.items():
            parsed_count = len(parsed.get(section_key, []))
            if parsed_count != pdf_count:
                # Use warning (not error) when parsed < pdf (some lines may be intentionally skipped)
                # Use error only when parsed > pdf (double-counting)
                if parsed_count > pdf_count:
                    errors.append(
                        f"Holding count: {section_key} parsed={parsed_count} > PDF lines={pdf_count} (double-counted?)"
                    )
                else:
                    warnings.append(
                        f"Holding count: {section_key} parsed={parsed_count} < PDF lines={pdf_count} (skipped {pdf_count - parsed_count})"
                    )

    # ── CHECK 3: Sum of parsed EUR values vs pensionikeskus AUM ──
    total_value_eur = parsed.get('_total_value_eur', 0)
    if total_value_eur > 0 and pk_aum:
        pk_value = pk_aum.get(fund_key)
        if pk_value and pk_value > 0:
            pct_diff = abs(total_value_eur - pk_value) / pk_value
            if pct_diff > 0.10:
                errors.append(
                    f"AUM mismatch >10%: parsed EUR total={total_value_eur:,.0f} vs pensionikeskus={pk_value:,.0f} "
                    f"(diff={pct_diff:.1%})"
                )
            elif pct_diff > 0.03:
                warnings.append(
                    f"AUM mismatch >3%: parsed EUR total={total_value_eur:,.0f} vs pensionikeskus={pk_value:,.0f} "
                    f"(diff={pct_diff:.1%})"
                )

    # ── CHECK 4: Enhanced cross-month consistency ──
    if prev_parsed:
        checks = [
            ('pe_funds', 'PE'),
            ('re_funds', 'RE'),
            ('bonds', 'bonds'),
            ('bond_funds', 'bond funds'),
            ('equity_funds', 'equity funds'),
            ('stocks', 'stocks'),
        ]
        for key, label in checks:
            prev_count = len(prev_parsed.get(key, []))
            curr_count = len(parsed[key])

            # Original check: complete disappearance
            if prev_count > 5 and curr_count == 0:
                warnings.append(
                    f"{label}: had {prev_count} entries last month, now 0 — likely parsing failure"
                )
            # Enhanced: large count change (>20%)
            elif prev_count > 3 and curr_count > 0:
                count_change = abs(curr_count - prev_count) / prev_count
                if count_change > 0.20:
                    warnings.append(
                        f"{label}: count changed from {prev_count} to {curr_count} "
                        f"({count_change:.0%} change)"
                    )

        # Asset class weight stability (>5pp change)
        prev_class_weights = {}
        for arr_key in required_arrays:
            prev_class_weights[arr_key] = sum(
                h.get('weight_pct', 0) for h in prev_parsed.get(arr_key, [])
            )
        for arr_key in required_arrays:
            curr_w = class_weights[arr_key]
            prev_w = prev_class_weights[arr_key]
            if prev_w > 1 or curr_w > 1:  # Only check non-trivial classes
                diff = abs(curr_w - prev_w)
                if diff > 5:
                    label = arr_key.replace('_', ' ')
                    warnings.append(
                        f"{label} weight changed by {diff:.1f}pp: {prev_w:.1f}% → {curr_w:.1f}%"
                    )

        # Total weight stability (>3pp change)
        prev_total = (
            sum(prev_class_weights.values())
            + prev_parsed.get('deposits_pct', 0)
            + abs(prev_parsed.get('derivatives_pct', 0))
        )
        if prev_total > 0:
            total_diff = abs(total - prev_total)
            if total_diff > 3:
                warnings.append(
                    f"Total weight changed by {total_diff:.1f}pp: {prev_total:.1f}% → {total:.1f}%"
                )

    # Print results
    for w in warnings:
        print(f"  WARNING [{fund_key}]: {w}")
    if errors:
        raise ValueError(f"[{fund_key}] Validation errors: {'; '.join(errors)}")

    return True


# ═══════════════════════════════════════════════════════════════════
# STEP 4: UNIVERSAL process_fund()
# ═══════════════════════════════════════════════════════════════════

def process_fund(parsed, etf_holdings, acwi, acwi_keys, sector_lookup, fuzzy_sector_map=None):
    """Universal fund processor. Works for all 24 funds.

    Steps:
    1. ETF lookthrough for equity_funds with ISIN/name mappings
    2. Build direct stock DataFrame
    3. Merge lookthrough stocks with direct stocks
    4. Enrich with sectors from ACWI
    5. Build fund_to_json output
    6. Add non-equity holdings (bonds, PE, RE)
    7. Build weight vectors for correlation
    8. Build etf_breakdown
    """
    provider = parsed['provider']

    # ── 1. ETF lookthrough ──
    equity_funds = parsed['equity_funds']
    lookthrough_allocs = []
    opaque_entries = []
    unmapped_etf_entries = []

    for ef in equity_funds:
        isin = ef.get('isin', '')
        etk = ETF_ISIN_TO_CSV.get(isin) if isin else None

        # Luminor funds don't have ISINs — match by name
        if not etk and provider == 'Luminor':
            for pattern, ticker in LUMINOR_ETF_PROXY_MAP.items():
                if pattern.lower() in ef['name'].lower():
                    etk = ticker
                    break

        if etk and etk in etf_holdings:
            lookthrough_allocs.append({
                'name': ef['name'],
                'isin': isin,
                'weight_pct': ef['weight_pct'],
                'etf_ticker': etk,
            })
        elif isin and isin in OPAQUE_FUND_ISINS:
            opaque_entries.append({
                'name': ef.get('name', isin),
                'weight_pct': ef['weight_pct'],
                'type': 'opaque_fund',
            })
        else:
            unmapped_etf_entries.append({
                'name': ef['name'],
                'isin': isin,
                'weight': ef['weight_pct'],
                'weight_pct': ef['weight_pct'],
                'type': 'etfs',
            })

    lt_df = pd.DataFrame()
    if lookthrough_allocs:
        lt_df, lt_opaque = build_lookthrough(lookthrough_allocs, etf_holdings)
        opaque_entries.extend(lt_opaque)

    # ── 2. Direct stocks DataFrame ──
    stocks = parsed['stocks']
    if stocks:
        stock_df = pd.DataFrame(stocks)
        stock_df = stock_df.rename(columns={'weight_pct': 'weight'})
        if 'country' in stock_df.columns:
            stock_df['location'] = stock_df['country'].map(COUNTRY_MAP).fillna(stock_df['country'])
        else:
            stock_df['location'] = ''
        stock_df['norm_key'] = stock_df['name'].apply(normalize_company_name)

        # Enrich with ACWI sector data
        stock_df['sector'] = stock_df['norm_key'].map(sector_lookup['sector'])
        if fuzzy_sector_map:
            for idx in stock_df[stock_df['sector'].isna()].index:
                nk = stock_df.at[idx, 'norm_key']
                if len(nk) >= 6:
                    match = fuzzy_sector_map.get(nk[:6])
                    if match:
                        stock_df.at[idx, 'sector'] = match[0]
                        if not stock_df.at[idx, 'location'] or stock_df.at[idx, 'location'] == stock_df.at[idx, 'country']:
                            stock_df.at[idx, 'location'] = match[1]
        stock_df.loc[stock_df['sector'].isna(), 'sector'] = 'Direct Investment'

        if 'stock_id' not in stock_df.columns:
            stock_df['stock_id'] = stock_df['norm_key'] + '|' + stock_df['location'].fillna('')
        if 'ticker' not in stock_df.columns:
            stock_df['ticker'] = ''
    else:
        stock_df = pd.DataFrame()

    # ── 3. Merge lookthrough + direct stocks ──
    if not lt_df.empty and not stock_df.empty:
        lt_df['norm_key'] = lt_df['name'].apply(normalize_company_name)
        lt_df['sector'] = lt_df['norm_key'].map(sector_lookup['sector']).fillna('')
        lt_df.loc[lt_df['sector'] == '', 'sector'] = 'Unknown'

        df = pd.concat([
            stock_df[['stock_id', 'ticker', 'name', 'weight', 'sector', 'location', 'norm_key']],
            lt_df[['stock_id', 'ticker', 'name', 'weight', 'sector', 'location', 'norm_key']],
        ], ignore_index=True)
        df = df.groupby('norm_key', as_index=False).agg({
            'stock_id': 'first', 'ticker': 'first', 'name': 'first', 'weight': 'sum',
            'sector': 'first', 'location': 'first',
        })
    elif not lt_df.empty:
        df = lt_df.copy()
        if 'norm_key' not in df.columns:
            df['norm_key'] = df['name'].apply(normalize_company_name)
        # Filter to ACWI universe for ETF lookthrough funds (no direct stocks)
        # In v1: process_etf_fund and process_luminor_fund both filter; process_seb_55 does not
        is_pure_etf = (len(stocks) == 0
                       and len(parsed['pe_funds']) == 0
                       and len(parsed['re_funds']) == 0
                       and len(parsed['bonds']) == 0
                       and len(parsed['bond_funds']) == 0)
        should_filter_acwi = len(stocks) == 0 and provider not in ('SEB',)
        if should_filter_acwi:
            acwi_universe = set(acwi['stock_id'])
            df_filtered = df[df['stock_id'].isin(acwi_universe)].copy()
            if not df_filtered.empty:
                df = df_filtered
        # Normalize weights
        equity_total = sum(ef['weight_pct'] for ef in equity_funds)
        total_w = df['weight'].sum()
        if total_w > 0:
            if is_pure_etf:
                # Pure ETF funds: normalize to 100% (like v1 process_etf_fund)
                df['weight'] = df['weight'] / total_w * 100
            else:
                # Mixed funds: normalize to equity_total (preserves fund intent)
                df['weight'] = df['weight'] / total_w * equity_total
        df['sector'] = df['norm_key'].map(sector_lookup['sector']).fillna(df.get('sector', ''))
        df.loc[df['sector'].isin(['', None]), 'sector'] = 'Unknown'
    elif not stock_df.empty:
        df = stock_df[['stock_id', 'ticker', 'name', 'weight', 'sector', 'location', 'norm_key']].copy()
    else:
        df = pd.DataFrame(columns=['stock_id', 'ticker', 'name', 'weight', 'sector', 'location', 'norm_key'])

    df = df.sort_values('weight', ascending=False).reset_index(drop=True) if not df.empty else df

    # ── 4. Build JSON output ──
    acwi_nk = acwi.copy()
    fund_data = fund_to_json(df, parsed['fund_name'], acwi_nk, acwi_keys, sector_lookup)

    # ── 5. Asset classes ──
    equity_funds_pct = sum(ef['weight_pct'] for ef in equity_funds)
    stocks_pct = sum(s['weight_pct'] for s in stocks)
    bonds_pct = sum(b['weight_pct'] for b in parsed['bonds'])
    bond_funds_pct = sum(bf['weight_pct'] for bf in parsed['bond_funds'])
    pe_pct = sum(p['weight_pct'] for p in parsed['pe_funds'])
    re_pct = sum(r['weight_pct'] for r in parsed['re_funds'])
    deposits = parsed.get('deposits_pct', 0)
    derivatives = parsed.get('derivatives_pct', 0)
    opaque_pct = sum(e['weight_pct'] for e in opaque_entries)
    unmapped_pct = sum(e['weight_pct'] for e in unmapped_etf_entries)

    # "stocks" asset class = direct stocks + lookthrough equity funds
    total_equity = stocks_pct + equity_funds_pct
    fund_data['asset_classes'] = {}
    if total_equity > 0:
        fund_data['asset_classes']['stocks'] = round(total_equity, 1)
    if unmapped_pct > 0:
        fund_data['asset_classes']['etfs'] = round(unmapped_pct, 1)
    if bonds_pct + bond_funds_pct > 0:
        fund_data['asset_classes']['bonds'] = round(bonds_pct + bond_funds_pct, 1)
    if re_pct > 0:
        fund_data['asset_classes']['re'] = round(re_pct, 1)
    if pe_pct > 0:
        fund_data['asset_classes']['pe'] = round(pe_pct, 1)
    if deposits > 0:
        fund_data['asset_classes']['deposits'] = round(deposits, 1)
    if derivatives != 0:
        fund_data['asset_classes']['derivatives'] = round(derivatives, 1)

    # ── 6. Non-equity holdings for display ──
    bond_holdings = [{'name': b['name'], 'weight': b['weight_pct'], 'type': 'bonds'}
                     for b in parsed['bonds']]
    bond_holdings.extend([{'name': bf['name'], 'weight': bf['weight_pct'], 'type': 'bonds'}
                          for bf in parsed['bond_funds']])
    pe_holdings = [{'name': p['name'], 'weight': p['weight_pct'], 'type': 'pe'}
                   for p in parsed['pe_funds']]
    re_holdings = [{'name': r['name'], 'weight': r['weight_pct'], 'type': 're'}
                   for r in parsed['re_funds']]
    etf_holdings_list = unmapped_etf_entries + opaque_entries

    if bond_holdings:
        fund_data['bond_holdings'] = bond_holdings
    if pe_holdings:
        fund_data['pe_holdings'] = pe_holdings
    if re_holdings:
        fund_data['re_holdings'] = re_holdings
    if etf_holdings_list:
        fund_data['etf_holdings'] = etf_holdings_list

    # Direct stock holdings (for Swedbank-style funds with both direct stocks and ETFs)
    if stocks and lookthrough_allocs:
        fund_data['direct_stock_holdings'] = [
            {'name': s['name'], 'weight': round(s['weight_pct'], 3)} for s in stocks
        ]

    # ── 7. ETF breakdown ──
    if lookthrough_allocs:
        fund_data['etf_breakdown'] = build_etf_breakdown(lookthrough_allocs, etf_holdings)

    # Opaque fund info
    if opaque_pct > 0:
        fund_data['opaque_funds'] = opaque_entries
        fund_data['opaque_pct'] = round(opaque_pct, 2)

    # ── 8. Weight vectors (for pairwise correlation) ──
    wv = fund_data['_weight_vec']
    wv_export = fund_data['weights']
    for prefix, holdings in [('BOND', bond_holdings), ('ETF', etf_holdings_list),
                              ('RE', re_holdings), ('PE', pe_holdings)]:
        for h in holdings:
            key = f"{prefix}|{h['name']}"
            w = h.get('weight', h.get('weight_pct', 0))
            wv[key] = w
            if w > 0.01:
                wv_export[key] = round(w, 4)

    # ── 9. Sectors/countries for non-stock holdings ──
    sectors = fund_data['sectors']
    countries = fund_data['countries']
    pe_total = sum(h['weight'] for h in pe_holdings)
    re_total = sum(h['weight'] for h in re_holdings)
    bond_total = sum(h['weight'] for h in bond_holdings)
    etf_total = sum(h.get('weight', h.get('weight_pct', 0)) for h in etf_holdings_list)

    if pe_total > 0:
        sectors['Erakapital'] = round(pe_total, 2)
    if re_total > 0:
        sectors['Kinnisvara'] = round(re_total, 2)
    if bond_total > 0:
        sectors['Võlakirjad'] = round(bond_total, 2)
    if etf_total > 0:
        sectors['ETF-id'] = round(etf_total, 2)

    non_stock_total = pe_total + re_total + bond_total + etf_total
    if non_stock_total > 0:
        countries['Eesti (PE/RE/võlak.)'] = round(non_stock_total, 2)

    # ── 10. Metadata ──
    fund_data['type'] = parsed['fund_type']
    fund_data['provider'] = parsed['provider']

    return fund_data


# ═══════════════════════════════════════════════════════════════════
# STEP 4.5: TOP CHANGES (month-over-month)
# ═══════════════════════════════════════════════════════════════════

def _ensure_eur_values(parsed, pk_aum):
    """For monthly-JSON funds without value_eur, compute from weight_pct × pk_aum.

    Modifies parsed in-place: sets value_eur on each holding and _total_value_eur.
    """
    if parsed.get('_total_value_eur'):
        return  # Already has EUR values from PDF parsing
    fund_key = parsed.get('fund_key', '')
    aum = pk_aum.get(fund_key, 0)
    if not aum:
        return
    parsed['_total_value_eur'] = aum
    for arr_key in ['equity_funds', 'stocks', 'bonds', 'bond_funds', 'pe_funds', 're_funds']:
        for h in parsed.get(arr_key, []):
            if 'value_eur' not in h and h.get('weight_pct', 0) > 0:
                h['value_eur'] = round(h['weight_pct'] / 100 * aum)


def _normalize_holding_name(name):
    """Normalize a holding name for cross-month matching.

    Luminor monthly JSON names vary between months (e.g. 'ETF1 iShares...' vs
    'iShares ... Fund (IE) Inst Acc EUR'). We strip all variable parts to get
    a stable core name.
    """
    n = name.upper()
    # Strip common prefixes that vary between months
    n = re.sub(r'^ETF1?\s+', '', n)
    n = re.sub(r'^FUND\s+\([A-Z]{2}\)[\s\-]+(INST\s+ACC\s+EUR|[A-Z]{3})\s+', '', n)
    n = re.sub(r'^INDEX\s+FUND\s+\([A-Z]{2}\)\s+', '', n)
    n = re.sub(r'^\([A-Z]{2}\)\s+', '', n)
    # Remove domicile markers
    # Remove manager names that sometimes appear
    n = re.sub(r'\s+BLACKROCK\b.*$', '', n)
    # Remove domicile markers and everything after
    n = re.sub(r'\s*\(IE\).*$', '', n)
    n = re.sub(r'\s*\(LU\).*$', '', n)
    n = re.sub(r'\s*\(LUXEMBOURG\).*$', '', n)
    # Remove trailing fund type / share class descriptors
    # Apply repeatedly since multiple suffixes may stack
    for _ in range(3):
        n = re.sub(r'\s+(EQUITY\s+)?INDEX(\s+FUND)?$', '', n)
        n = re.sub(r'\s+FUND$', '', n)
        n = re.sub(r'\s+ETF(\s+ACC)?$', '', n)
        n = re.sub(r'\s+UCITS(\s+ETF(\s+ACC)?)?$', '', n)
        n = re.sub(r'\s+II-ETF\s+A\s+SA$', ' II', n)
        n = re.sub(r'\s+II\s+UCITS.*$', ' II', n)
    n = re.sub(r'\s+', ' ', n).strip()
    return n


def _collect_parsed_holdings(parsed):
    """Collect all holdings from a parsed fund dict, keyed by normalized name.

    Returns dict: norm_name → {name, isin, value_eur}
    Uses ISIN as primary key if available, falls back to normalized name.
    """
    holdings = {}  # key → {name, isin, value_eur}
    for arr_key in ['equity_funds', 'stocks', 'bonds', 'bond_funds', 'pe_funds', 're_funds']:
        for h in parsed.get(arr_key, []):
            name = h.get('name', '')
            if not name:
                continue
            val = h.get('value_eur', 0) or 0
            isin = h.get('isin', '')
            # Use ISIN as key if available, else normalized name
            key = isin if isin else _normalize_holding_name(name)
            if key in holdings:
                holdings[key]['value_eur'] += val
            else:
                holdings[key] = {'name': name, 'isin': isin, 'value_eur': val}
    return holdings


def compute_top_changes(curr_parsed, prev_parsed, curr_fund_data, prev_fund_data,
                        curr_total_eur, prev_total_eur):
    """Compute top 10 biggest month-over-month changes for a fund.

    Returns {"prev_month": ..., "parsed": [...], "lookthrough": [...]} or None.
    """
    if not prev_parsed:
        return None

    prev_month = prev_parsed.get('month', '')
    result = {'prev_month': prev_month, 'parsed': [], 'lookthrough': []}

    # ── Parsed view: EUR changes at holding level ──
    curr_h = _collect_parsed_holdings(curr_parsed)
    prev_h = _collect_parsed_holdings(prev_parsed)
    all_keys = set(curr_h.keys()) | set(prev_h.keys())

    parsed_changes = []
    for key in all_keys:
        curr_entry = curr_h.get(key, {})
        prev_entry = prev_h.get(key, {})
        curr_eur = curr_entry.get('value_eur', 0) or 0
        prev_eur = prev_entry.get('value_eur', 0) or 0
        if curr_eur == 0 and prev_eur == 0:
            continue
        change = curr_eur - prev_eur
        if change == 0:
            continue
        # Use current month name if available, else previous
        display_name = curr_entry.get('name') or prev_entry.get('name', key)
        parsed_changes.append({
            'name': display_name,
            'prev_eur': prev_eur,
            'curr_eur': curr_eur,
            'change_eur': change,
        })

    parsed_changes.sort(key=lambda x: abs(x['change_eur']), reverse=True)
    result['parsed'] = parsed_changes[:10]

    # ── Lookthrough view: EUR changes at stock level ──
    if curr_fund_data and prev_fund_data and curr_total_eur and prev_total_eur:
        curr_stocks = {h['name']: h['weight'] for h in curr_fund_data.get('top_holdings', [])}
        prev_stocks = {h['name']: h['weight'] for h in prev_fund_data.get('top_holdings', [])}
        all_stock_names = set(curr_stocks.keys()) | set(prev_stocks.keys())

        lt_changes = []
        for name in all_stock_names:
            cw = curr_stocks.get(name, 0)
            pw = prev_stocks.get(name, 0)
            c_eur = round(cw / 100 * curr_total_eur)
            p_eur = round(pw / 100 * prev_total_eur)
            change = c_eur - p_eur
            if change == 0:
                continue
            lt_changes.append({
                'name': name,
                'prev_eur': p_eur,
                'curr_eur': c_eur,
                'change_eur': change,
            })

        lt_changes.sort(key=lambda x: abs(x['change_eur']), reverse=True)
        result['lookthrough'] = lt_changes[:10]

    return result


# ═══════════════════════════════════════════════════════════════════
# STEP 5: MAIN
# ═══════════════════════════════════════════════════════════════════

def _resolve_pdf_path(fund_key, provider, pdf_code, month, reports_cfg):
    """Find the PDF path for a fund."""
    if reports_cfg and fund_key in reports_cfg:
        return REPORT_DIR / reports_cfg[fund_key]['pdf']

    # Swedbank uses custom PDF names
    if fund_key in SWEDBANK_PDF_NAMES:
        return REPORT_DIR / f"{month}/{SWEDBANK_PDF_NAMES[fund_key]}"

    # Pensionikeskus format: est_{CODE}_raport_YYYYMMDD.pdf
    if pdf_code:
        # Compute last day of month
        year, mo = month.split('-')
        if int(mo) == 12:
            next_month = date(int(year) + 1, 1, 1)
        else:
            next_month = date(int(year), int(mo) + 1, 1)
        from datetime import timedelta
        last_day = next_month - timedelta(days=1)
        return REPORT_DIR / f"{month}/est_{pdf_code}_raport_{last_day.strftime('%Y%m%d')}.pdf"

    return None


def save_parsed(parsed, month):
    """Save parsed fund data to data/parsed/YYYY-MM/."""
    month_dir = PARSED_DIR / month
    month_dir.mkdir(parents=True, exist_ok=True)
    fund_key = parsed['fund_key'].replace(' ', '_').replace('+', 'plus')
    path = month_dir / f"{fund_key}.json"
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(parsed, f, ensure_ascii=False, indent=2)
    return path


def load_prev_parsed(fund_key, month):
    """Load previous month's parsed data for cross-month validation."""
    year, mo = month.split('-')
    if int(mo) == 1:
        prev_month = f"{int(year) - 1}-12"
    else:
        prev_month = f"{year}-{int(mo) - 1:02d}"
    fk_safe = fund_key.replace(' ', '_').replace('+', 'plus')
    path = PARSED_DIR / prev_month / f"{fk_safe}.json"
    if path.exists():
        with open(path, encoding='utf-8') as f:
            return json.load(f)
    return None


def main():
    parser = argparse.ArgumentParser(description='V2 pension fund pipeline')
    parser.add_argument('--month', default=None,
                        help='Month to process (YYYY-MM). Default: latest in data/monthly/')
    parser.add_argument('--skip-nav', action='store_true',
                        help='Skip NAV history fetch')
    parser.add_argument('--output', default=None,
                        help='Output path for fund_data.json (default: docs/fondide-vordlus/)')
    parser.add_argument('--offline', action='store_true',
                        help='Skip external data fetches (pensionikeskus AUM check)')
    args = parser.parse_args()

    print('=== V2 Multi-Source Pension Fund Pipeline ===\n')

    # Load monthly config
    monthly_files = sorted(Path('data/monthly').glob('*.json'))
    if not args.month and not monthly_files:
        parser.error('No config files found in data/monthly/. Use --month to specify.')
    MONTH = args.month or monthly_files[-1].stem
    reports_cfg, alloc_cfg = load_monthly_config(MONTH)
    alloc_cfg = alloc_cfg or {}
    print(f'Month: {MONTH} ({len(reports_cfg or {})} reports, {len(alloc_cfg)} allocations)\n')

    # Fetch pensionikeskus AUM for validation check 3
    pk_aum = {}
    if not args.offline:
        # Compute last day of month for pensionikeskus query
        year, mo = MONTH.split('-')
        if int(mo) == 12:
            next_month = date(int(year) + 1, 1, 1)
        else:
            next_month = date(int(year), int(mo) + 1, 1)
        from datetime import timedelta
        last_day = next_month - timedelta(days=1)
        pk_date = last_day.strftime('%Y-%m-%d')
        print(f'Fetching pensionikeskus AUM for {pk_date}...')
        pk_aum = fetch_pensionikeskus_aum(pk_date)
        if pk_aum:
            print(f'  Got AUM data for {len(pk_aum)} funds')
        else:
            print('  No AUM data received (will skip AUM validation)')
        print()

    # Fetch previous month pensionikeskus AUM (for top_changes lookthrough EUR)
    prev_pk_aum = {}
    prev_fund_data_all = {}
    if not args.offline:
        year, mo = MONTH.split('-')
        if int(mo) == 1:
            prev_month_str = f"{int(year) - 1}-12"
        else:
            prev_month_str = f"{year}-{int(mo) - 1:02d}"
        py, pm = prev_month_str.split('-')
        if int(pm) == 12:
            prev_next = date(int(py) + 1, 1, 1)
        else:
            prev_next = date(int(py), int(pm) + 1, 1)
        from datetime import timedelta
        prev_last_day = prev_next - timedelta(days=1)
        prev_pk_date = prev_last_day.strftime('%Y-%m-%d')
        print(f'Fetching prev month pensionikeskus AUM for {prev_pk_date}...')
        prev_pk_aum = fetch_pensionikeskus_aum(prev_pk_date)
        if prev_pk_aum:
            print(f'  Got prev AUM data for {len(prev_pk_aum)} funds')
        else:
            print('  No prev AUM data received')

        # Load previous month's fund_data.json (for lookthrough comparison)
        prev_fd_path = OUT_DIR / prev_month_str / 'fund_data.json'
        if prev_fd_path.exists():
            with open(prev_fd_path, encoding='utf-8') as f:
                prev_fd_raw = json.load(f)
            prev_fund_data_all = prev_fd_raw.get('funds', {})
            print(f'  Loaded prev fund_data.json ({len(prev_fund_data_all)} funds)')
        else:
            print(f'  No prev fund_data.json at {prev_fd_path}')
        print()

    # Output directory
    out_dir = Path(args.output) if args.output else OUT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    # ── Load ETF holdings ──
    print('Loading ETF holdings...')
    etf_holdings = {}
    for tk in ['SAWD', 'SASU', 'SAEU', 'SAJP', 'SAEM', 'SSAC', 'NDIA', '4BRZ', 'CNYA', 'IKSA', 'XTJP', 'SPPY', 'EMXC']:
        etf_holdings[tk] = fetch_ishares_holdings(tk)
    for tk in EODHD_ETFS:
        etf_holdings[tk] = fetch_eodhd_holdings(tk)
    for tk in ['GLOBALFOND_A']:
        etf_holdings[tk] = load_manual_holdings(tk)
    etf_holdings['SSAC_EM'] = build_ssac_em(etf_holdings)
    print(f'  Loaded {len(etf_holdings)} ETF data sources')

    # ── Build ACWI benchmark ──
    print('Building ACWI benchmark...')
    acwi = build_acwi(etf_holdings)
    acwi['weight'] = acwi['weight'] / acwi['weight'].sum() * 100
    acwi['norm_key'] = acwi['name'].apply(normalize_company_name)
    sector_lookup, fuzzy_sector_map = _build_sector_lookup_with_fuzzy(acwi)
    acwi_keys = set(acwi['norm_key'])
    print(f'  ACWI: {len(acwi)} stocks\n')

    all_funds_data = {}

    # ACWI benchmark (internal, not in fund_order)
    acwi_data = fund_to_json(acwi, 'MSCI ACWI', acwi, acwi_keys, sector_lookup)
    acwi_data['type'] = 'benchmark'
    acwi_data['provider'] = 'MSCI'
    acwi_data['asset_classes'] = {'stocks': 100.0}
    all_funds_data['ACWI'] = acwi_data

    # ── Process all funds ──
    data_sources = {}
    for i, (fund_key, display_name, provider, fund_type, report_key, pdf_code) in enumerate(FUND_REGISTRY, 1):
        print(f'{i:2d}. {fund_key}...')

        # Resolve PDF path
        pdf_path = _resolve_pdf_path(fund_key, provider, pdf_code, MONTH, reports_cfg)

        # Get allocation from monthly JSON if available
        alloc_entry = alloc_cfg.get(fund_key)

        # Parse
        try:
            parsed = parse_fund(fund_key, provider, fund_type, MONTH, pdf_path, alloc_entry)
        except Exception as e:
            print(f'   ERROR parsing: {e}')
            continue

        # Validate
        prev_parsed = load_prev_parsed(fund_key, MONTH)
        try:
            validate_parsed_fund(parsed, prev_parsed, pk_aum=pk_aum)
        except ValueError as e:
            print(f'   VALIDATION ERROR: {e}')
            continue

        # Save parsed
        save_parsed(parsed, MONTH)

        # Process
        try:
            fund_data = process_fund(parsed, etf_holdings, acwi, acwi_keys, sector_lookup, fuzzy_sector_map)
        except Exception as e:
            print(f'   ERROR processing: {e}')
            import traceback
            traceback.print_exc()
            continue

        if fund_data:
            all_funds_data[fund_key] = fund_data
            n = fund_data['n_stocks']
            src = 'JSON' if alloc_entry else (pdf_path.name if pdf_path else '?')
            print(f'   => {n} stocks (from {src})')

            # Compute top changes (month-over-month)
            _ensure_eur_values(parsed, pk_aum)
            if prev_parsed:
                _ensure_eur_values(prev_parsed, prev_pk_aum)
            curr_total = parsed.get('_total_value_eur', 0) or (pk_aum.get(fund_key, 0))
            prev_total = (prev_parsed.get('_total_value_eur', 0) if prev_parsed else 0) or prev_pk_aum.get(fund_key, 0)
            prev_fd = prev_fund_data_all.get(fund_key)
            tc = compute_top_changes(parsed, prev_parsed, fund_data, prev_fd,
                                     curr_total, prev_total)
            if tc:
                fund_data['top_changes'] = tc

            # Track data sources
            _date = reports_cfg[report_key]['date'] if reports_cfg and report_key in reports_cfg else ''
            _pdf = reports_cfg[report_key]['pdf'] if reports_cfg and report_key in reports_cfg else ''
            _url = reports_cfg[report_key].get('url', '') if reports_cfg and report_key in reports_cfg else ''
            data_sources[fund_key] = {
                'pdf': _pdf, 'date': _date,
                'type': f'{provider} ({fund_type})',
            }
            if _url:
                data_sources[fund_key]['url'] = _url

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
                'shared': len(shared), 'only_a': len(only_i), 'only_b': len(only_j),
                'total_a': len(ki), 'total_b': len(kj),
                'shared_weight_a': round(sw_i, 2), 'shared_weight_b': round(sw_j, 2),
                'only_weight_a': round(ow_i, 2), 'only_weight_b': round(ow_j, 2),
            }

    with open(out_dir / 'overlap_stats.json', 'w') as f:
        json.dump(overlap_stats, f, indent=2)

    # Remove internal weight vectors
    for fd in all_funds_data.values():
        fd.pop('_weight_vec', None)

    acwi_sectors = list(all_funds_data['ACWI']['sectors'].keys())

    output = {
        'generated': date.today().isoformat(),
        'data_month': MONTH,
        'fees': FEES,
        'funds': all_funds_data,
        'fund_order': fund_order,
        'acwi_sector_order': acwi_sectors,
        'correlation_matrix': corr_matrix,
    }

    out_path = out_dir / 'fund_data.json'
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f'\nExported to {out_path} ({out_path.stat().st_size / 1024:.0f} KB)')

    # Export data sources
    with open(out_dir / 'data_sources.json', 'w', encoding='utf-8') as f:
        json.dump(data_sources, f, ensure_ascii=False, indent=2)

    # Export ETF metadata
    coverage = {}
    for fund_name in fund_order:
        fd = all_funds_data[fund_name]
        breakdown = fd.get('etf_breakdown', [])
        opaque = fd.get('opaque_pct', 0)
        proxy_w = sum(e['fund_weight'] for e in breakdown
                      if e.get('isin', '') in TRUE_PROXY_ISINS)
        pct = round(100 - proxy_w - opaque)
        coverage[fund_name] = {'pct': max(0, min(100, pct))}
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

    from pipeline_shared import ETF_ISIN_TO_CSV as _etf_map
    canonical_isins = {
        'SAWD': 'IE0009FT4LX4', 'SASU': 'IE00BFNM3G45', 'SAEU': 'IE00BFNM3D14',
        'SAJP': 'IE00BFNM3L97', 'SAEM': 'IE00BFNM3P36', 'SSAC': 'IE00B6R52259',
        'SSAC_EM': 'IE00BKPTWY98',
        'NDIA': 'IE00BZCQB185', '4BRZ': 'IE00BFNM3V63', 'CNYA': 'IE00BQT3WG13',
        'IKSA': 'IE00BYYR0489', 'SPPY': 'IE00BH4GPZ28', 'XTJP': 'IE00BRB36B93',
        'EMXU': 'LU2345046655', 'BNKE': 'LU1829219390',
        'GLOBALFOND_A': 'SE0000542979',
    }
    proxy_mappings = []
    for isin, ticker in _etf_map.items():
        if isin != canonical_isins.get(ticker):
            proxy_mappings.append({'isin': isin, 'mapped_to': ticker})

    etf_meta = {
        'generated': date.today().isoformat(),
        'coverage': coverage,
        'etf_sources': etf_sources,
        'proxy_mappings': proxy_mappings,
    }
    with open(out_dir / 'etf_metadata.json', 'w', encoding='utf-8') as f:
        json.dump(etf_meta, f, ensure_ascii=False, indent=2)

    # NAV history
    if args.skip_nav:
        print('\n--skip-nav: skipping NAV fetch')
    else:
        fetch_nav_history()
        fetch_acwi_nav()
        nav_path = out_dir / 'nav_data.json'
        with open(nav_path, 'r', encoding='utf-8') as f:
            nav_data = json.load(f)
        corr_data = compute_nav_return_correlations(nav_data)
        from datetime import timedelta
        one_year_ago = date.today() - timedelta(days=365)
        corr_1y = compute_nav_return_correlations(nav_data, cutoff_date=one_year_ago.strftime('%Y-%m-%d'))
        corr_data['last_1y'] = corr_1y
        corr_path = out_dir / 'return_correlations.json'
        with open(corr_path, 'w', encoding='utf-8') as f:
            json.dump(corr_data, f, ensure_ascii=False, indent=2)

    print('\n=== V2 Pipeline complete ===')
    print(f'Month: {MONTH}')
    print(f'Funds processed: {len(fund_order)}')
    print(f'fund_data.json:  {(out_dir / "fund_data.json").stat().st_size:,} bytes')


if __name__ == '__main__':
    main()
