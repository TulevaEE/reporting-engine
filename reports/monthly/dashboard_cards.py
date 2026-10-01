"""Dashboardi 74 uued kaardid vanade survivor-kaartide kujul.

Gateway annab välja ainult dashboardide 74 ja 12 kaarte. Dashboard 74 ehitati
21.–23.09.2026 natiivse SQL-i peale ümber (tuleva repo
``work/kpi/monthly-kpis/README.md``) ja vanad kaardid, mida aruanne luges
(1518, 1519, 1520, 418, 1534, 1535, 1657, 1911, 1912, 389, 392, 334, 1516),
ei ole enam dashboardil. Siin loetakse uued kaardid ja teisendatakse need
vanade kaartide ridade kujule, et build_monthly_report.py, generate_monthly_charts.py
ja mission_kpis.py jääksid samaks. Allavoolu nimi jääb vana kaardi nimeks.

Uued kogujad (vanad 1518, 1519, 1520, 418, 1534, 1535) arvutatakse tabelist 2748
(mv_monthly_conversions_with_tkf), mitte dashboardi kaartidelt 2570/2716/2718/2710/
2717/2719: aruanne loeb ainult II ja III samba kogujaid (Tõnu otsus 01.10.2026),
dashboard loeb ka ainult kogumisfondiga liitujad. Vt ``pillar_new_savers``.

Sisulised erinevused vanadest kaartidest:
  - vahetusavaldused fondi järgi (2722, 2723) loevad inimesi, mitte avaldusi;
  - kasvuallikad (2737, 2739) ja AUM (2742) on eurodes, siin teisendatakse M EUR-ks;
  - AUM-i kasvule sissemaksetest ja -vahetustest lisatakse kogumisfondi sissemaksed
    (``add_tkf_to_organic``), 2742 neid ei loe.
"""

import kpi_2578 as k


def _data_month(reporting_date) -> str:
    """Snapshot-vaate reporting_date on andmekuule järgneva kuu 1. päev."""
    y, m = int(str(reporting_date)[:4]), int(str(reporting_date)[5:7])
    y, m = (y - 1, 12) if m == 1 else (y, m - 1)
    return f'{y}-{m:02d}-01'










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
    return [{'kasvuallikas': r['kasvuallikas'], 'väärtus': round(r['väärtus'] / 1e6, 1),
             '_eur': r['väärtus']} for r in rows]


def split_tkf_contributions(rows, tkf_eur):
    """Tõstab kogumisfondi sissemaksed reast „sissemaksed" eraldi tulbaks.

    2737/2739 liidavad II ja III samba sissemaksetele kogumisfondi omad (v_tkf_kpi)
    juurde; aruanne näitab neid eraldi. Kogumisfondi väljamaksed on juba
    „väljavõetud vara" sees ja turu mõju „turu mõju" sees.
    """
    out = []
    for r in rows:
        if r['kasvuallikas'] != 'sissemaksed':
            out.append(r)
            continue
        pillars = r['_eur'] - tkf_eur
        out.append({'kasvuallikas': 'sissemaksed II ja III sambasse',
                    'väärtus': round(pillars / 1e6, 1), '_eur': pillars})
        out.append({'kasvuallikas': 'sissemaksed TKF-i',
                    'väärtus': round(tkf_eur / 1e6, 1), '_eur': tkf_eur})
    return out


def aum(rows):
    """2742 -> 334: ainult tegelikud kuud, M EUR täisarvuna, kasv protsentides.

    ``_aum_eur``, ``_growth`` ja ``_organic`` hoiavad täpseid väärtusi, et
    ``add_tkf_to_organic`` saaks orgaanilisele kasvule kogumisfondi sissemaksed lisada.
    """
    out = []
    for r in rows:
        if r['kuu lõpu AUM'] is None:
            continue
        growth = r['AUM 12 kuu kasv']
        organic = r['AUM 12 kuu kasv sissemaksetest ja -vahetustest']
        out.append({
            'month': r['month'],
            'kuu lõpu AUM (M EUR)': round(r['kuu lõpu AUM'] / 1e6),
            'AUM 12 kuu kasv %': round(growth * 100) if growth is not None else None,
            'AUM 12 kuu kasv sissemaksetest ja -vahetustest %':
                round(organic * 100) if organic is not None else None,
            '_aum_eur': r['kuu lõpu AUM'], '_growth': growth, '_organic': organic,
        })
    return out


def add_tkf_to_organic(aum_rows, tkf_rows):
    """Lisab orgaanilisele kasvule viimase 12 kuu kogumisfondi sissemaksed.

    2742 (v_aum_12m_growth_with_prognosis) arvutab kasvu sissemaksetest ja
    -vahetustest ainult II ja III samba pealt, kuigi AUM sisaldab alates 2026-02
    kogumisfondi. Nimetaja on sama mis 2742-l, AUM 12 kuud tagasi:
    AUM / (1 + 12 kuu kasv).
    """
    tkf = {str(r['Created At: Month'])[:7]: r['Sum of Amount'] or 0 for r in tkf_rows}
    for r in aum_rows:
        if r['_organic'] is None or r['_growth'] is None:
            continue
        y, m = k.parse_label(r['month'])
        months = [divmod(y * 12 + m - 1 - j, 12) for j in range(12)]
        tkf12 = sum(tkf.get(f'{yy}-{mm + 1:02d}', 0) for yy, mm in months)
        base = r['_aum_eur'] / (1 + r['_growth'])
        r['_organic'] += tkf12 / base
        r['AUM 12 kuu kasv sissemaksetest ja -vahetustest %'] = round(r['_organic'] * 100)
    return aum_rows


def conversions_history(rows):
    """2748 (mv_monthly_conversions_with_tkf) -> 1516: kogu ajalugu, andmekuu veerus 'kuu'."""
    return [{'kuu': _data_month(r['reporting_date']), **r} for r in rows]


def tkf_payments(rows):
    """2747 (v_tkf_kpi) -> 2305: TKF-i sissemaksed ja maksjad andmekuu kaupa."""
    return [{'Created At: Month': _data_month(r['reporting_date']) + 'T00:00:00Z',
             'Sum of Amount': r['tkf_contributions_eur'],
             'Distinct values of Remitter ID Code': r['tkf_contributors']}
            for r in rows if r.get('tkf_contributions_eur') is not None]


# Vana veerg -> tabeli 2748 veerud. Kes sai II või III samba kogujaks, loetakse sõltumata
# sellest, kas ta liitus samal ajal kogumisfondiga või oli enne kogumisfondi klient;
# ainult kogumisfondiga liitujad (`tkf`, `tkf oy`) ja kogumisfondi lisamised (`2>tkf` jms)
# jäävad välja. 2025. aastal, enne kogumisfondi, annab see täpselt vanad kaardid.
PILLAR_COLUMNS = {
    '2': ['2', '2+tkf', 'tkf>2'],
    '3': ['3', '3+tkf', 'tkf>3'],
    '2+3': ['2+3', '2+3+tkf', 'tkf>2+3'],
    '3>2': ['3>2', '3>2+tkf', '3+tkf>2'],
    '2>3': ['2>3', '2>3+tkf', '2+tkf>3'],
}
NEW_SAVERS = ['2', '3', '2+3']
NEW_II = ['2', '2+3', '3>2']
NEW_III = ['3', '2+3', '2>3']
MONTHS_SHOWN = 13


def _grouped(row):
    return {old: sum(row.get(c) or 0 for c in cols) for old, cols in PILLAR_COLUMNS.items()}


def _yoy(cur, prev):
    return (cur - prev) / prev if prev else None


def pillar_new_savers(conversions, year, month):
    """Tabeli 2748 read -> vanade kaartide 1518, 1519, 1520, 418, 1534, 1535 read."""
    by_month = {(int(r['kuu'][:4]), int(r['kuu'][5:7])): _grouped(r) for r in conversions}

    def total(key, cols):
        g = by_month.get(key)
        return sum(g[c] for c in cols) if g else None

    def ytd(y, cols):
        return sum(total((y, m), cols) or 0 for m in range(1, month + 1))

    keys = [divmod(year * 12 + month - 1 - i, 12) for i in reversed(range(MONTHS_SHOWN))]
    keys = [(y, m + 1) for y, m in keys if (y, m + 1) in by_month]
    label = lambda y, m: f'{y}-{m:02d}-01'
    cards = {
        'uute kogujate arv kuus': [
            {'kuu: Month': label(*k), 'uute koguate arv': total(k, NEW_SAVERS),
             'YoY, %': _yoy(total(k, NEW_SAVERS), total((k[0] - 1, k[1]), NEW_SAVERS) or 0)}
            for k in keys],
        'II sambaga liitujate arv kuus': [
            {'kuu: Month': label(*k), **{c: by_month[k][c] for c in ['3>2', '2+3', '2']},
             'YoY, %': _yoy(total(k, NEW_II), total((k[0] - 1, k[1]), NEW_II) or 0)}
            for k in keys],
        'III sambaga liitujate arv kuus': [
            {'kuu: Month': label(*k), **{c: by_month[k][c] for c in ['2>3', '2+3', '3']},
             'YoY, %': _yoy(total(k, NEW_III), total((k[0] - 1, k[1]), NEW_III) or 0)}
            for k in keys],
    }
    for name, column, cols in [('uute kogujate arv YTD', 'uute kogujate arv', NEW_SAVERS),
                               ('uute II samba kogujate arv YTD', 'uute II samba kogujate arv', NEW_II),
                               ('uute III samba kogujate arv YTD', 'uute III samba kogujate arv', NEW_III)]:
        cards[name] = [{'reporting_year': f'{y}-01-01', column: ytd(y, cols)}
                       for y in (year - 1, year)]
    return cards


# Uus kaart -> (vana kaardi nimi allavoolu, vana kaardi id, teisendus).
# Teisendus võtab read ja aruandeaasta.
CARDS = {
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
    2612: ('kogumisfondi sissemakse tegijate arv YTD', None, lambda rows, y: rows),
}
