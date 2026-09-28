"""Tuleva missiooni tabloo: viis KPI-d ja north star, üks tõeallikas.

North star on ainus eesmärk: 100 000 sihikindlat kogujat. KPI-d ei ole eesmärgid,
nad mõõdavad tempot selle suunas; lävendid on siin andmena, mitte kommentaarina.

Andmed tulevad ``data/YYYY-MM.yaml`` failist:

    kpi_2578                          2.1 (maht tasuastmete juures), 2.2, 2.5
    cards / uute kogujate arv, kogu ajalugu (1516)      2.3
    cards / sihikindluse trepp (2741)                   2.4 ja north star

Sõnaline osa (miks me seda mõõdame) tuleb monorepo failist
``work/tonu/noukogu/mission-scoreboard/kpi-tekstid.yaml``: see on narratiiv, mitte
kood, ja see repo on avalik. Tee: TULEVA_MONOREPO env või ~/Desktop/tuleva.

Käivita ilma aruannet ehitamata: python3 mission_kpis.py 2026 8
"""
import os
import sys
import unicodedata
from datetime import date
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).parent))
import kpi_2578 as k2578

NORTH_STAR = 100_000
KPI_ORDER = ['2.1', '2.2', '2.3', '2.4', '2.5', '2.6']
MIN_KUUD = 36                  # iga KPI on graafik vähemalt kolm aastat tagasi
ALGUS = (2023, 1)              # seeriate algus (44 kuud 2026-08 seisuga)

CARD_1516 = 'uute kogujate arv kuus, kogu ajalugu'
CARD_2741 = 'sihikindluse trepp'

# KPI 2.1: jooksva tasu astmed (Tuleva Maailma Aktsiate Pensionifond, blogist).
# Maht iga astme juures loetakse kaardilt 2578, mitte ei interpoleerita.
# TODO plaan samm 1: kaart 2465 (monthly-ocf) asendab selle listi.
TASU_ASTMED = [((2019, 1), 0.50), ((2020, 1), 0.45), ((2021, 4), 0.39), ((2021, 12), 0.37),
               ((2023, 10), 0.35), ((2024, 11), 0.32), ((2025, 12), 0.29), ((2026, 3), 0.28)]

LAVENDID = {
    '2.1': {'siht': 0.22, 'siht_maht': 3000.0},             # %, M€: nõukogu tasumudel
    '2.2': {},                                              # kokku leppimata (2026-09)
    '2.3': {'siht': 1000, 'roheline': 700, 'kollane': 500},  # inimest kuus, 12k keskm
    '2.5': {'kollane': 2.0, 'punane': 3.0},                 # % varadest aastas
}

# 2.2 ja 2.5 komponendid kaardil 2578
SISSE = ['Second Pillar Contributions Eur', 'Third Pillar Contributions Eur',
         'New Monthly Mandates Eur', 'New Monthly Mandates Third Pillar Eur']
SISSE_T = ['New Cancelled Mandates Eur']         # tühistatud vahetused, bruto korrigeerimiseks
LAHK = ['New Monthly Leavers Eur', 'New Monthly Leavers Third Pillar Eur']
LAHK_T = ['New Cancelled Leavers Eur']

TREPP = ['sihikindel', 'poole teel: II tõstetud', 'poole teel: III üle 1200',
         'muud', 'kogujaid kokku']


def monorepo() -> Path:
    return Path(os.environ.get('TULEVA_MONOREPO', Path.home() / 'Desktop' / 'tuleva'))


def load_texts() -> dict:
    """kpi-tekstid.yaml. Puuduv fail või tekst annab vea, mitte tühja koha."""
    p = monorepo() / 'work' / 'tonu' / 'noukogu' / 'mission-scoreboard' / 'kpi-tekstid.yaml'
    if not p.exists():
        raise SystemExit(f'kpi-tekstid.yaml puudub: {p} (sea TULEVA_MONOREPO)')
    d = yaml.safe_load(p.read_text(encoding='utf-8'))
    for nr in KPI_ORDER:
        for v in ('nimi', 'miks'):
            if not (d['kpi'].get(nr) or {}).get(v):
                raise SystemExit(f'kpi-tekstid.yaml: KPI {nr} väli "{v}" puudub')
    return d


def _nfc(t):
    return unicodedata.normalize('NFC', str(t or '')).strip()


def _ym(v):
    s = str(v)[:7]
    return int(s[:4]), int(s[5:7])


def _i(y, m):
    return y * 12 + m - 1


def _card(data, name):
    c = (data.get('cards') or {}).get(name) or {}
    if c.get('error') or not c.get('data'):
        raise SystemExit(f'kaart "{name}" puudub andmefailist ({c.get("error", "tühi")}); '
                         f'jooksuta fetch_monthly_data.py')
    return c['data']


def _hoiatus(nr, seeria, allikas):
    if len(seeria) < MIN_KUUD:
        print(f'  HOIATUS: KPI {nr} seeria on {len(seeria)} punkti (< {MIN_KUUD} kuud), '
              f'allikas {allikas}')


def _seis_23(v):
    lv = LAVENDID['2.3']
    return 'roheline' if v >= lv['roheline'] else 'kollane' if v >= lv['kollane'] else 'punane'


def _seis_25(v):
    lv = LAVENDID['2.5']
    return 'punane' if v >= lv['punane'] else 'kollane' if v >= lv['kollane'] else 'roheline'


def compute(data: dict, year: int, month: int) -> dict:
    """Tagastab {'north_star': {...}, 'kpi': {nr: {...}}}. Seeriad: [((a, k), v), ...]."""
    idx = k2578.index_series(data['kpi_2578']['data'])
    g = lambda r, cols: sum(float(r.get(c) or 0) for c in cols)
    lopp = _i(year, month)

    def summa12(i, cols):
        return sum(g(idx[(j // 12, j % 12 + 1)], cols) for j in range(i - 11, i + 1))

    def aum(i):
        r = idx.get((i // 12, i % 12 + 1))
        return float(r['Current Aum']) / 1e6 if r and r.get('Current Aum') else None

    # 2.2 ja 2.5
    sisse, muutus, lahk = [], [], []
    for i in range(_i(*ALGUS), lopp + 1):
        k = (i // 12, i % 12 + 1)
        s12 = summa12(i, SISSE) - summa12(i, SISSE_T)
        s12_ea = summa12(i - 12, SISSE) - summa12(i - 12, SISSE_T)
        sisse.append((k, round(s12 / 1e6, 1)))
        muutus.append((k, round((s12 / s12_ea - 1) * 100, 1)))
        lahk.append((k, round((summa12(i, LAHK) - summa12(i, LAHK_T)) / (aum(i - 12) * 1e6) * 100, 2)))

    # 2.3: 12 kuu libisev keskmine kaardilt 1516 (kaardi 1518 allikas ilma 13 kuu filtrita)
    uued = {_ym(r['kuu']): (r.get('2') or 0) + (r.get('3') or 0) + (r.get('2+3') or 0)
            for r in _card(data, CARD_1516)}
    k1518 = {_ym(r['kuu: Month']): r['uute koguate arv']
             for r in _card(data, 'uute kogujate arv kuus')}
    lahku = {k: (v, uued.get(k)) for k, v in k1518.items() if uued.get(k) != v}
    if lahku:
        raise SystemExit(f'kaart 1516 ja kuuaruande kaart 1518 ei klapi: {lahku}')
    uued12 = []
    for i in range(_i(*ALGUS), lopp + 1):
        aken = [(j // 12, j % 12 + 1) for j in range(i - 11, i + 1)]
        if any(a not in uued for a in aken):
            raise SystemExit(f'kaart 1516: aknas {aken[0]}..{aken[-1]} on kuid puudu')
        uued12.append(((i // 12, i % 12 + 1), round(sum(uued[a] for a in aken) / 12)))

    # 2.4: sihikindluse trepp, riiklik maksemäär (unit_owner.p2_rate)
    trepp_read = [{_nfc(k): v for k, v in r.items()} for r in _card(data, CARD_2741)]
    puudu = [v for v in TREPP if v not in trepp_read[0]]
    if puudu:
        raise SystemExit(f'kaart 2741: veerud puuduvad {puudu}; olemas {list(trepp_read[0])}')
    trepp_read = [r for r in trepp_read if _i(*_ym(r['kuu'])) <= lopp]
    trepp = {v: [(_ym(r['kuu']), int(r[v])) for r in trepp_read] for v in TREPP}
    sihikindlad = trepp['sihikindel'][-1][1]

    # 2.1: tasuastmed varade mahu teljel
    tasu = [(round(aum(_i(*k)), 1), t) for k, t in TASU_ASTMED if aum(_i(*k))]
    tasu.append((round(aum(lopp), 1), TASU_ASTMED[-1][1]))

    for nr, s, allikas in (('2.2', sisse, '2578'), ('2.3', uued12, '1516'), ('2.5', lahk, '2578')):
        _hoiatus(nr, s, allikas)
    kuid_24 = lopp - _i(*trepp['sihikindel'][0][0]) + 1
    if kuid_24 < MIN_KUUD:
        print(f'  HOIATUS: KPI 2.4 seeria ulatub {kuid_24} kuud (< {MIN_KUUD}), '
              f'snapshot-ajalugu algab 2025-04 (kaart 2741)')

    return {
        'north_star': {'vaartus': sihikindlad, 'siht': NORTH_STAR,
                       'kuu': trepp['sihikindel'][-1][0]},
        'kpi': {
            '2.1': {'vaartus': TASU_ASTMED[-1][1], 'seeria': tasu, 'yhik': '%',
                    'lavend': LAVENDID['2.1'],
                    'markus': 'X-telg on varade maht, mitte aeg: kulud ei lange ajas, vaid peavad '
                              'langema mahu kasvuga. Iga aste on üks tasulangetus, maht selle kuu '
                              'seisuga (kaart 2578).'},
            '2.2': {'vaartus': sisse[-1][1], 'muutus': muutus[-1][1], 'seeria': sisse,
                    'seeria_muutus': muutus, 'yhik': 'M€', 'lavend': LAVENDID['2.2'],
                    'markus': 'Viimase 12 kuu II ja III samba sissemaksed ja ületoomised eurodes, '
                              'tühistatud vahetused maha arvatud. Iga tulp on 12 kuu summa selle '
                              'kuu seisuga, joon sama summa muutus aasta varasemaga. Kaart 2578.'},
            '2.3': {'vaartus': uued12[-1][1], 'seeria': uued12, 'seis': _seis_23(uued12[-1][1]),
                    'yhik': 'inimest kuus', 'lavend': LAVENDID['2.3'],
                    'markus': '12 kuu libisev keskmine: iga punkt on viimase 12 kuu uute kogujate '
                              'arv jagatud 12-ga. Kaart 1516 (kuuaruande kaardi 1518 allikas, mis '
                              'näitab ainult 13 kuud).'},
            '2.4': {'vaartus': sihikindlad, 'seeria': trepp, 'yhik': 'inimest',
                    'markus': 'Kaart 2741, poolaastad ja viimane kuu; snapshot-ajalugu algab '
                              '2025-04. II samba maksemäär on riiklik, seega teada ka siis, kui '
                              'inimese II sammas on teises fondis: iga koguja on ühel trepiastmel. '
                              f'Jääk "muud" ({trepp["muud"][-1][1]:,}) ei ole graafikul.'
                              .replace(',', ' ')},
            '2.5': {'vaartus': lahk[-1][1], 'seeria': lahk, 'seis': _seis_25(lahk[-1][1]),
                    'yhik': '% varadest aastas', 'lavend': LAVENDID['2.5'],
                    'markus': 'Ainult teise fondi lahkujad, tühistatud avaldused maha arvatud; '
                              'väljujad on eraldi rida. Nimetaja on varade maht 12 kuud tagasi. '
                              'Kaart 2578.'},
            '2.6': {'vaartus': None, 'seeria': [], 'staatus': 'töös'},
        },
    }


def et(v, dec=0):
    return f'{v:,.{dec}f}'.replace(',', ' ').replace('.', ',')


def scoreboard(data: dict, year: int, month: int) -> dict:
    """Mallile: arvud + tekstid ühes kohas."""
    k = compute(data, year, month)
    t = load_texts()
    kpi = k['kpi']
    silt = {
        '2.1': (f"{et(kpi['2.1']['vaartus'], 2)}%",
                f"täna, siht {et(LAVENDID['2.1']['siht'], 2)}% "
                f"{et(LAVENDID['2.1']['siht_maht'] / 1000)} mld € varade juures"),
        '2.2': (f"{et(kpi['2.2']['vaartus'])} M€",
                f"viimase 12 kuu kohta, {'+' if kpi['2.2']['muutus'] >= 0 else '−'}"
                f"{et(abs(kpi['2.2']['muutus']), 1)}% aasta varasemaga"),
        '2.3': (et(kpi['2.3']['vaartus']), 'inimest kuus, 12 kuu keskmine'),
        '2.4': (et(kpi['2.4']['vaartus']), 'sihikindlat kogujat'),
        '2.5': (f"{et(kpi['2.5']['vaartus'], 1)}%", 'varade mahust aastas'),
        '2.6': ('', 'mõõdik on ettevalmistamisel'),
    }
    ns = k['north_star']
    return {
        'eesmark': t['raamistik']['eesmark'],
        'eesmark_selgitus': ' '.join(t['raamistik']['eesmark_selgitus'].split()),
        'kpi_ei_ole_eesmark': ' '.join(t['raamistik']['kpi_ei_ole_eesmark'].split()),
        'north_star': {**ns, 'vaartus_et': et(ns['vaartus']),
                       'kuu_et': f"{ns['kuu'][1]:02d}.{ns['kuu'][0]}"},
        'kpid': [{'nr': nr, 'kood': nr.replace('.', ''),
                  'nimi': t['kpi'][nr]['nimi'],
                  'miks': ' '.join(t['kpi'][nr]['miks'].split()),
                  'vaartus': silt[nr][0], 'alt': silt[nr][1],
                  'markus': kpi[nr].get('markus', ''),
                  'staatus': kpi[nr].get('staatus')} for nr in KPI_ORDER],
        '_kpi': k,
    }


if __name__ == '__main__':
    y, m = (int(sys.argv[1]), int(sys.argv[2])) if len(sys.argv) > 2 else (date.today().year, date.today().month - 1)
    d = yaml.safe_load((Path(__file__).parent / 'data' / f'{y}-{m:02d}.yaml').read_text())
    s = scoreboard(d, y, m)
    ns = s['north_star']
    print(f"north star: {ns['vaartus_et']} / {et(ns['siht'])} ({ns['kuu_et']})")
    for r in s['kpid']:
        seis = s['_kpi']['kpi'][r['nr']].get('seis') or ''
        print(f"  {r['nr']} {r['nimi'][:48]:48s} {r['vaartus']:>9s}  {seis:9s} {r['alt']}")
