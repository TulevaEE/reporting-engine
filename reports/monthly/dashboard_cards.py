"""Dashboardi 74 uued kaardid vanade survivor-kaartide kujul.

Gateway annab välja ainult dashboardide 74 ja 12 kaarte. Dashboard 74 ehitati
21.–23.09.2026 natiivse SQL-i peale ümber (tuleva repo
``work/kpi/monthly-kpis/README.md``) ja vanad kaardid, mida aruanne luges
(1518, 1519, 1520, 418, 1534, 1535, 1657, 1911, 1912, 389, 392, 334, 1516),
ei ole enam dashboardil. Siin loetakse uued kaardid ja teisendatakse need
vanade kaartide ridade kujule, et build_monthly_report.py, generate_monthly_charts.py
ja mission_kpis.py jääksid samaks. Allavoolu nimi jääb vana kaardi nimeks.

Sisulised erinevused vanadest kaartidest:
  - uued kogujad (2570, 2716, 2718, 2710, 2717, 2719) loevad ka kogumisfondi (TKF)
    liitumised, nagu dashboard; vanad kaardid ei lugenud;
  - vahetusavaldused fondi järgi (2722, 2723) loevad inimesi, mitte avaldusi;
  - kasvuallikad (2737, 2739) ja AUM (2742) on eurodes, siin teisendatakse M EUR-ks.
"""
from datetime import date


def _data_month(reporting_date) -> str:
    """Snapshot-vaate reporting_date on andmekuule järgneva kuu 1. päev."""
    y, m = int(str(reporting_date)[:4]), int(str(reporting_date)[5:7])
    y, m = (y - 1, 12) if m == 1 else (y, m - 1)
    return f'{y}-{m:02d}-01'


def new_savers_monthly(rows):
    """2570 -> 1518: 'kuu: Month', 'uute koguate arv', 'YoY, %'."""
    return [{'kuu: Month': r['kuu: Month'], 'uute koguate arv': r['uute kogujate arv'],
             'YoY, %': r['YoY %']} for r in rows]


def ii_joiners_monthly(rows):
    """2716 -> 1519: '2', '2+3', '3>2' (uues kaardis koos TKF-iga), 'YoY, %'."""
    return [{'kuu: Month': r['kuu'], '3>2': r['3/tkf > 2'], '2+3': r['2 + 3/tkf'],
             '2': r['2'], 'YoY, %': r['YoY %']} for r in rows]


def iii_joiners_monthly(rows):
    """2718 -> 1520: '3', '2+3', '2>3' (uues kaardis koos TKF-iga), 'YoY, %'."""
    return [{'kuu: Month': r['kuu'], '2>3': r['2/tkf > 3'], '2+3': r['3 + 2/tkf'],
             '3': r['3'], 'YoY, %': r['YoY %']} for r in rows]


def ytd(value_column, new_column):
    """2710/2717/2719 -> 418/1534/1535: üks rida aasta kohta, kõik aastad sama kuuni."""
    def adapt(rows):
        return [{'reporting_year': r['Aasta'], value_column: r[new_column]} for r in rows]
    return adapt


def iii_contributors_ytd(rows, year):
    """2613 -> 1657: üks rida, aruandeaasta inimeste arv."""
    this_year = [r for r in rows if str(r['Aasta']).startswith(str(year))]
    return [{'Distinct values of Personal ID': this_year[0]['Sissemakse tegijaid YTD']}] \
        if this_year else []


def switching_by_fund(fund_column):
    """2723/2722 -> 1911/1912: fond ja inimeste arv."""
    def adapt(rows):
        return [{fund_column: r['fond'], 'Distinct values of Code': r['inimesi']} for r in rows]
    return adapt


def growth_sources(rows):
    """2737/2739 -> 389/392: EUR -> M EUR, üks koht pärast koma nagu vanal kaardil."""
    return [{'kasvuallikas': r['kasvuallikas'], 'väärtus': round(r['väärtus'] / 1e6, 1)}
            for r in rows]


def aum(rows):
    """2742 -> 334: ainult tegelikud kuud, M EUR täisarvuna, kasv protsentides."""
    out = []
    for r in rows:
        if r['kuu lõpu AUM'] is None:
            continue
        out.append({
            'month': r['month'],
            'kuu lõpu AUM (M EUR)': round(r['kuu lõpu AUM'] / 1e6),
            'AUM 12 kuu kasv %': round(r['AUM 12 kuu kasv'] * 100)
            if r['AUM 12 kuu kasv'] is not None else None,
            'AUM 12 kuu kasv sissemaksetest ja -vahetustest %':
                round(r['AUM 12 kuu kasv sissemaksetest ja -vahetustest'] * 100)
                if r['AUM 12 kuu kasv sissemaksetest ja -vahetustest'] is not None else None,
        })
    return out


def conversions_history(rows):
    """2748 (mv_monthly_conversions_with_tkf) -> 1516: kogu ajalugu, andmekuu veerus 'kuu'."""
    return [{'kuu': _data_month(r['reporting_date']), **r} for r in rows]


def tkf_payments(rows):
    """2747 (v_tkf_kpi) -> 2305: TKF-i sissemaksed ja maksjad andmekuu kaupa."""
    return [{'Created At: Month': _data_month(r['reporting_date']) + 'T00:00:00Z',
             'Sum of Amount': r['tkf_contributions_eur'],
             'Distinct values of Remitter ID Code': r['tkf_contributors']}
            for r in rows if r.get('tkf_contributions_eur') is not None]


# Uus kaart -> (vana kaardi nimi allavoolu, vana kaardi id, teisendus).
# Teisendus võtab read ja aruandeaasta.
CARDS = {
    2570: ('uute kogujate arv kuus', 1518, lambda rows, y: new_savers_monthly(rows)),
    2710: ('uute kogujate arv YTD', 418,
           lambda rows, y: ytd('uute kogujate arv', 'Uusi kogujaid YTD')(rows)),
    2716: ('II sambaga liitujate arv kuus', 1519, lambda rows, y: ii_joiners_monthly(rows)),
    2718: ('III sambaga liitujate arv kuus', 1520, lambda rows, y: iii_joiners_monthly(rows)),
    2717: ('uute II samba kogujate arv YTD', 1534,
           lambda rows, y: ytd('uute II samba kogujate arv', 'uute II samba kogujate arv')(rows)),
    2719: ('uute III samba kogujate arv YTD', 1535,
           lambda rows, y: ytd('uute III samba kogujate arv', 'uute III samba kogujate arv')(rows)),
    2613: ('III s sissemakse tegijate arv YTD', 1657, iii_contributors_ytd),
    2723: ('II samba vahetusavalduste arv pangafondidesse sel vahetusperioodil', 1911,
           lambda rows, y: switching_by_fund('Fund - Security To → Name Estonian')(rows)),
    2722: ('II samba vahetusavalduste arv lähtefondi järgi sel vahetusperioodil', 1912,
           lambda rows, y: switching_by_fund('Fund - Security From → Name Estonian')(rows)),
    2737: ('Kasvuallikad eelmisel kuul (tegelik), M EUR', 389, lambda rows, y: growth_sources(rows)),
    2739: ('Kasvuallikad YTD (tegelik), M EUR', 392, lambda rows, y: growth_sources(rows)),
    2742: ('AUM (koos ootel vahetuste ja väljumistega)', 334, lambda rows, y: aum(rows)),
    2748: ('uute kogujate arv kuus, kogu ajalugu', 1516, lambda rows, y: conversions_history(rows)),
    2747: ('Täiendavasse Kogumisfondi tehtud maksed', 2305, lambda rows, y: tkf_payments(rows)),
}
