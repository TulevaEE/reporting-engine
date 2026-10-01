"""Osakuhinna võrdluse seeriad otse avalikest allikatest.

Asendab Metabase'i kaardi 2245 ("Osakuhinna võrdlus"), mis luges tabelit
``public.index_values``. Selle tabeli täidab onboarding-service samadest
avalikest allikatest (``comparisons/fundvalue/retrieval/``), seega võtame
numbrid otse:

  EE3600109435  Tuleva Maailma Aktsiate PF NAV  pensionikeskus.ee, fondide NAV (fond 77)
  EPI           II samba EPI-indeks             pensionikeskus.ee, EPI graafikud
  MSCI_ACWI     MSCI ACWI net, EUR              MSCI indeks 892400 (nagu MsciIndexRetriever)
  CPI           Eesti HICP, 2025 = 100          Eurostat prc_hicp_minr (nagu CpiValueRetriever)

Kaardil 2245 lõppesid kõik seeriad 2025-12-31 ja CPI tuli vanalt võtmelt,
mida onboarding-service alates 27.04.2026 enam ei uuenda (uus on CPI_ECOICOP2).

Read on samas kujus nagu kaardil (``Key``, ``Date``, ``Value``, ``Provider``),
et graafikud jääksid samaks. Aken lõpeb aruandekuu viimase päevaga, nii et
vana kuu uuesti ehitamine annab sama graafiku.
"""
import calendar
import gzip
from datetime import date, datetime

import requests

TULEVA_ISIN = 'EE3600109435'
PENSIONIKESKUS_FUND_ID = 77

NAV_URL = 'https://www.pensionikeskus.ee/statistika/ii-sammas/kogumispensioni-fondide-nav/'
EPI_URL = 'https://www.pensionikeskus.ee/en/statistics/ii-pillar/epi-charts/'
MSCI_URL = 'https://app2.msci.com/products/service/index/indexmaster/getLevelDataForGraph'
MSCI_ACWI_CODE = '892400'
EUROSTAT_CPI_URL = ('https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/'
                    'prc_hicp_minr/M.I25.TOTAL.EE/?format=TSV&compressed=true')

YEARS_BACK = 5
TIMEOUT = 60
HEADERS = {'User-Agent': 'tuleva-reports (monthly report)'}


def _get(url, params=None):
    response = requests.get(url, params=params, headers=HEADERS, timeout=TIMEOUT)
    response.raise_for_status()
    return response.content


def _pensionikeskus_rows(content):
    """Pensionikeskuse 'xls' on tegelikult UTF-16 TSV, esimene rida päis."""
    lines = content.decode('utf-16').splitlines()
    header = lines[0].split('\t')
    return [dict(zip(header, line.split('\t'))) for line in lines[1:] if line.strip()]


def _number(text):
    return float(text.strip().replace(',', '.'))


def _row(key, day, value, provider):
    return {'Key': key, 'Date': day.isoformat(), 'Value': value, 'Provider': provider}


def tuleva_nav(start, end):
    content = _get(NAV_URL, {
        'f[]': PENSIONIKESKUS_FUND_ID,
        'date_from': start.strftime('%d.%m.%Y'),
        'date_to': end.strftime('%d.%m.%Y'),
        'download': 'xls',
    })
    rows = []
    for r in _pensionikeskus_rows(content):
        if r.get('ISIN') != TULEVA_ISIN:
            raise ValueError(f'Pensionikeskuse fond {PENSIONIKESKUS_FUND_ID} ei ole enam '
                             f'{TULEVA_ISIN}: {r.get("ISIN")} {r.get("Fond")}')
        day = datetime.strptime(r['Kuupäev'], '%d.%m.%Y').date()
        rows.append(_row(TULEVA_ISIN, day, _number(r['NAV']), 'PENSIONIKESKUS'))
    return rows


def epi(start, end):
    content = _get(EPI_URL, {
        'date_from': start.strftime('%d.%m.%Y'),
        'date_to': end.strftime('%d.%m.%Y'),
        'download': 'xls',
    })
    return [_row('EPI', date.fromisoformat(r['Date']), _number(r['Value']), 'PENSIONIKESKUS')
            for r in _pensionikeskus_rows(content) if r['Index'] == 'EPI-II']


def msci_acwi(start, end):
    response = requests.get(MSCI_URL, params={
        'output': 'INDEX_LEVELS',
        'currency_symbol': 'EUR',
        'index_variant': 'NETR',
        'start_date': start.strftime('%Y%m%d'),
        'end_date': end.strftime('%Y%m%d'),
        'data_frequency': 'DAILY',
        'baseValue': 'false',
        'index_codes': MSCI_ACWI_CODE,
    }, headers=HEADERS, timeout=TIMEOUT)
    response.raise_for_status()
    if response.text.lstrip().startswith('<'):
        raise ValueError('MSCI vastas HTML-lehega, mitte JSON-iga')
    levels = response.json()['indexes']['INDEX_LEVELS']
    rows = []
    for level in levels:
        day = datetime.strptime(str(level['calc_date']), '%Y%m%d').date()
        if start <= day <= end:
            rows.append(_row('MSCI_ACWI', day, float(level['level_eod']), 'MSCI'))
    return rows


def cpi(start, end):
    lines = gzip.decompress(_get(EUROSTAT_CPI_URL)).decode().splitlines()
    header = next(l for l in lines if l.startswith('freq,unit,coicop18,geo')).split('\t')
    data = next(l for l in lines if l.startswith('M,I25,TOTAL,EE')).split('\t')
    rows = []
    for period, raw in zip(header[1:], data[1:]):
        raw = raw.strip()
        if not raw or raw.startswith(':'):
            continue
        day = date.fromisoformat(period.strip() + '-01')
        if start <= day <= end:
            rows.append(_row('CPI', day, float(raw.split()[0]), 'EUROSTAT'))
    return rows


def window(year, month):
    end = date(year, month, calendar.monthrange(year, month)[1])
    start = date(end.year - YEARS_BACK, 1, 1)
    return start, end


def fetch_unit_prices(year, month):
    """Kõik neli seeriat aruandekuu lõpuni, YEARS_BACK aastat tagasi aasta algusest."""
    start, end = window(year, month)
    rows = []
    for name, fetch in [('Tuleva NAV', tuleva_nav), ('EPI', epi),
                        ('MSCI ACWI', msci_acwi), ('CPI', cpi)]:
        series = fetch(start, end)
        if not series:
            raise ValueError(f'{name}: {start}–{end} kohta ei tulnud ühtegi väärtust')
        print(f'  {name}: {len(series)} väärtust, {series[0]["Date"]} … {series[-1]["Date"]}')
        rows.extend(series)
    return rows
