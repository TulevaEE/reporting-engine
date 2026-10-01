"""Kas koondtabelid annavad samad numbrid nagu kaardid, mida nad asendama hakkavad?

    python3 check_tables.py 2026 9

Loeb ``data/YYYY-MM.yaml`` plokid ``tables`` (kaardid 2747-2749, vt
fetch_monthly_data.TABLE_CARDS) ja ``cards`` ning võrdleb kuu kaupa:

  mv_monthly_conversions_with_tkf  vs  1518, 1519, 1520 (kuus), 418, 1534, 1535 (YTD)
  v_tkf_kpi                        vs  2305 (TKF-i sissemaksed ja maksjad kuus)
  v_aum_12m_growth_with_prognosis  vs  334 (AUM ja 12 kuu kasv)

Kuu on andmekuu: snapshot-vaadetes on ``reporting_date`` andmekuule järgneva
kuu 1. päev (tuleva repo ``work/kpi/monthly-kpis/README.md``, „Kuu nihe").
Veerunimed on vaadete omad (natiivpäring ``SELECT *`` ei muuda neid); kui
mõnda ei leita, trükitakse tabeli veerud, et nime saaks parandada.

Skript ainult võrdleb ja trükib. Aruanne kasutab vanu kaarte seni, kuni
erinevused on läbi vaadatud.
"""
import sys
from datetime import date
from pathlib import Path

import yaml

# Uued kogujad: nimes ei ole ">" (tuleva repo uued-kogujad-ytd.sql päis).
NEW_SAVER_COLUMNS = ['2', '3', '2+3', 'tkf', '2+tkf', '3+tkf', '2+3+tkf', 'tkf oy']
# Uued II / III samba kogujad (ii-samba-liitujad-ytd.sql, iii-samba-liitujad-ytd.sql).
NEW_II_COLUMNS = ['2', '2+3', '2+tkf', '2+3+tkf', '3>2', '3>2+tkf', '3+tkf>2', 'tkf>2', 'tkf>2+3']
NEW_III_COLUMNS = ['3', '2+3', '3+tkf', '2+3+tkf', '2>3', '2>3+tkf', '2+tkf>3', 'tkf>3', 'tkf>2+3']


def data_month(reporting_date) -> date:
    y, m = int(str(reporting_date)[:4]), int(str(reporting_date)[5:7])
    return date(y - 1, 12, 1) if m == 1 else date(y, m - 1, 1)


def card_month(text) -> date:
    return date(int(str(text)[:4]), int(str(text)[5:7]), 1)


def by_month(rows, missing):
    out = {}
    for r in rows:
        if 'reporting_date' not in r:
            missing.add(('reporting_date', tuple(r)))
            return {}
        out[data_month(r['reporting_date'])] = r
    return out


def num(row, column, missing):
    if column not in row:
        missing.add((column, tuple(row)))
        return None
    return row[column] or 0


def compare(title, pairs):
    """pairs: [(silt, kaardilt, tabelist)]"""
    shown = [(label, a, b) for label, a, b in pairs if a is not None and b is not None]
    same = sum(1 for _, a, b in shown if a == b)
    print(f'\n{title}: {same}/{len(shown)} klapivad')
    for label, a, b in shown:
        if a != b:
            print(f'  {label}: kaart {a}  tabel {b}  vahe {b - a:+}')


def main(year, month, path=None):
    path = Path(path) if path else Path(__file__).parent / 'data' / f'{year}-{month:02d}.yaml'
    data = yaml.safe_load(path.read_text())
    tables = {name: t.get('data') or [] for name, t in (data.get('tables') or {}).items()}
    cards = {name: c.get('data') or [] for name, c in (data.get('cards') or {}).items()}
    missing = set()

    conv = by_month(tables.get('mv_monthly_conversions_with_tkf', []), missing)
    if conv:
        def new_savers(r):
            return sum(num(r, c, missing) or 0 for c in NEW_SAVER_COLUMNS)

        compare('Uued kogujad kuus (1518 vs conversions, 8 veergu)', [
            (r['kuu: Month'][:7], r['uute koguate arv'],
             new_savers(conv[card_month(r['kuu: Month'])]) if card_month(r['kuu: Month']) in conv else None)
            for r in cards.get('uute kogujate arv kuus', [])])
        for card, columns in [('II sambaga liitujate arv kuus', ['3>2', '2+3', '2']),
                              ('III sambaga liitujate arv kuus', ['2>3', '2+3', '3'])]:
            for column in columns:
                compare(f'{card}, veerg {column}', [
                    (r['kuu: Month'][:7], r.get(column),
                     num(conv[card_month(r['kuu: Month'])], column, missing)
                     if card_month(r['kuu: Month']) in conv else None)
                    for r in cards.get(card, [])])
        for card, value_column, columns in [
                ('uute kogujate arv YTD', 'uute kogujate arv', NEW_SAVER_COLUMNS),
                ('uute II samba kogujate arv YTD', 'uute II samba kogujate arv', NEW_II_COLUMNS),
                ('uute III samba kogujate arv YTD', 'uute III samba kogujate arv', NEW_III_COLUMNS)]:
            pairs = []
            for r in cards.get(card, []):
                y = int(str(r['reporting_year'])[:4])
                months = [m for m in conv if m.year == y and m.month <= month]
                pairs.append((str(y), r[value_column],
                              sum(sum(num(conv[m], c, missing) or 0 for c in columns)
                                  for m in months)))
            compare(f'{card} (jaan kuni {month:02d})', pairs)

    tkf = by_month(tables.get('v_tkf_kpi', []), missing)
    if tkf:
        rows = cards.get('Täiendavasse Kogumisfondi tehtud maksed', [])
        compare('TKF sissemaksed kuus, EUR (2305 vs v_tkf_kpi)', [
            (str(r['Created At: Month'])[:7], round(r['Sum of Amount'], 2),
             round(num(tkf[card_month(r['Created At: Month'])], 'tkf_contributions_eur', missing), 2)
             if card_month(r['Created At: Month']) in tkf else None)
            for r in rows])
        compare('TKF sissemakse tegijad kuus (2305 vs v_tkf_kpi)', [
            (str(r['Created At: Month'])[:7], r['Distinct values of Remitter ID Code'],
             num(tkf[card_month(r['Created At: Month'])], 'tkf_contributors', missing)
             if card_month(r['Created At: Month']) in tkf else None)
            for r in rows])

    aum = by_month(tables.get('v_aum_12m_growth_with_prognosis', []), missing)
    if aum:
        import kpi_2578 as k
        pairs_aum, pairs_growth, pairs_organic = [], [], []
        for r in cards.get('AUM (koos ootel vahetuste ja väljumistega)', []):
            if str(r['month']).startswith('prog:'):
                continue
            y, m = k.parse_label(r['month'])
            t = aum.get(date(y, m, 1))
            if t is None:
                continue
            cur = num(t, 'current_aum_pending', missing)
            prev = num(t, 'prev_aum_pending', missing)
            wo = num(t, 'aum_12m_growth_wo_market_impact_and_compensation_eur', missing)
            pairs_aum.append((r['month'], r['kuu lõpu AUM (M EUR)'], round(cur / 1e6) if cur else None))
            pairs_growth.append((r['month'], r['AUM 12 kuu kasv %'],
                                 round((cur - prev) / prev * 100) if cur and prev else None))
            pairs_organic.append((r['month'], r['AUM 12 kuu kasv sissemaksetest ja -vahetustest %'],
                                  round(wo / prev * 100) if wo is not None and prev else None))
        compare('AUM kuu lõpus, M EUR (334 vs v_aum_12m_growth)', pairs_aum)
        compare('AUM 12 kuu kasv % (334 vs v_aum_12m_growth)', pairs_growth)
        compare('Orgaaniline 12 kuu kasv % (334 vs v_aum_12m_growth)', pairs_organic)

    for name in ['mv_monthly_conversions_with_tkf', 'v_tkf_kpi', 'v_aum_12m_growth_with_prognosis']:
        if name not in tables:
            print(f'\n{name}: andmefailis ei ole (tõmba uuesti fetch_monthly_data.py-ga)')
    for column, columns in sorted(missing):
        print(f'\nVeergu "{column}" ei ole. Tabeli veerud: {list(columns)}')


if __name__ == '__main__':
    main(int(sys.argv[1]), int(sys.argv[2]), *sys.argv[3:4])
