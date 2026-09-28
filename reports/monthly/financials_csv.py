"""Finantstulemuste read prognoositabeli CSV-st.

AJUTINE. Metabase kaart 636 ("Tuleva finantstulemused") ei uuene: augusti
tõmmises seisis „Kuu Tulemus" veerus veebruari number. Kuni kaart on parandatud,
võtame aruande 7. peatüki read otse prognoositabelist, mis eksporditakse
käsitsi ``reports/monthly/downloads/prognoos-kuu.csv`` faili (kaust on
gitignore'itud, repo on avalik — toorik sinna ei kuulu).

CSV kuju: esimene veerg on rea nimi, edasi iga kuu kohta neli veergu
``prognoos MM-AAAA``, ``tegelik MM-AAAA``, ``vahe %``, ``vahe eur`` ning
vabatekstiline kommentaar. Loeme ainult prognoosi ja tegeliku; YoY arvutame
sama kuu eelmise aasta tegelikust. YTD on aasta algusest kuu lõpuni tegelike
summa ja selle YoY sama perioodi eelmise aasta summa vastu; kui mõni kuu
veergudest puudub, jääb YTD tühjaks (mitte osaliseks summaks).

``load()`` tagastab read samas kujus, nagu need tulid kaardilt 636, et
aruande mall ja ``build_monthly_report.py`` valik muutumatuks jääks.
"""
import csv
import re
from pathlib import Path

DEFAULT_CSV = Path(__file__).parent / 'downloads' / 'prognoos-kuu.csv'


def _norm(name: str) -> str:
    """Rea nimed erinevad kaardil ja tabelis tühikute ja koolonite poolest.

    „EBITDA/ ärikasum" vs „EBITDA/ärikasum", „tööjõukulud:" vs „tööjõukulud",
    „bruto marginaal" vs „brutomarginaal" — kõik sama rida.
    """
    return re.sub(r'\s+', '', (name or '').strip().rstrip(':').lower())


def _num(cell):
    """Lahtrist arv. Tühi lahter ja mitte-arv annavad None."""
    s = (cell or '').strip().replace(' ', '').replace(' ', '').replace(',', '')
    if not s or s in ('-', '–'):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _change(now, before):
    """Muutus protsendina, kaardi 636 kombel absoluutväärtustelt.

    Kuluread on tabelis miinusega. Kui võtta muutus märgiga, siis kulu kasv
    −90 604 -> −138 803 tuleks välja miinusena ja loeks aruandes nagu kulu
    oleks kahanenud. Absoluutväärtustelt tuleb +53%, nagu kaardil oli.

    Märgivahetus (mullu kahjum, tänavu kasum) annab None: selle kohta ei ole
    ausat protsenti, aruanne näitab seal kriipsu.
    """
    if not before or now is None:
        return None
    if (now < 0) != (before < 0):
        return None
    return round((abs(now) - abs(before)) / abs(before), 2)


def _col(header, prefix, year, month):
    """Veeru indeks, nt ``tegelik 08-2026``."""
    want = f'{prefix} {month:02d}-{year}'
    for i, h in enumerate(header):
        if (h or '').strip().lower() == want:
            return i
    return None


def load(year: int, month: int, names: list, path: Path = None) -> list:
    """Read ``names`` järjekorras. Tagastab [] kui faili või kuu veergu ei ole.

    ``names`` on kaardi 636 nimekujud; need jäävad ka tagastatud ridade
    ``Eur`` väljale, et mall neid edasi ära tunneks.
    """
    path = Path(path) if path else DEFAULT_CSV
    if not path.exists():
        print(f"  Hoiatus: finantstulemuste CSV puudub: {path}")
        return []

    with open(path, newline='', encoding='utf-8-sig') as f:
        rows = list(csv.reader(f))
    if not rows:
        return []

    header = rows[0]
    i_act = _col(header, 'tegelik', year, month)
    i_fc = _col(header, 'prognoos', year, month)
    if i_act is None:
        print(f"  Hoiatus: CSV-s ei ole veergu 'tegelik {month:02d}-{year}' "
              f"({path.name}) — finantstulemuste peatükk jääb välja")
        return []
    i_prev = _col(header, 'tegelik', year - 1, month)
    i_ytd = [_col(header, 'tegelik', year, m) for m in range(1, month + 1)]
    i_ytd_prev = [_col(header, 'tegelik', year - 1, m) for m in range(1, month + 1)]

    by_name = {}
    for r in rows[1:]:
        if not r or not (r[0] or '').strip():
            continue
        by_name.setdefault(_norm(r[0]), r)

    out = []
    for name in names:
        r = by_name.get(_norm(name))
        if r is None:
            continue
        def cell(i):
            return _num(r[i]) if i is not None and i < len(r) else None
        act, fc, prev = cell(i_act), cell(i_fc), cell(i_prev)
        if act is None:
            continue
        def ytd(cols):
            v = [cell(i) for i in cols]
            return None if any(x is None for x in v) else sum(v)
        ytd_now, ytd_prev = ytd(i_ytd), ytd(i_ytd_prev)
        out.append({
            'Eur': name,
            'Prognoos': fc,
            'Kuu Tulemus': act,
            'Tulemus %': _change(act, fc),
            'Eelmise Aasta Tulemus': prev,
            'YoY %': _change(act, prev),
            'YTD Tulemus': ytd_now,
            'Eelmise Aasta YTD': ytd_prev,
            'YTD YoY %': _change(ytd_now, ytd_prev),
        })
    return out
