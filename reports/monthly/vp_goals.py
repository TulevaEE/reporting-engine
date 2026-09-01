"""Vahetusperioodi (VP) eesmärgid kuuaruande algusesse.

Neli eesmärki, igaüks oma Metabase kaardil (nädalane seeria):

    2631  goal_1  30% III samba avaldajatest toob kaasa ka II samba
    2632  goal_2  500 sissemakse teinud OÜd
    2633  goal_3  400 last kogub püsimaksega
    2634  goal_4  1350 kõrge palgaga kogujat tõstab II samba maksemäära

Kaardid 2632 ja 2633 kannavad kaasa ka ``sihtjoon`` veeru — lineaarse
tempojoone sihini VP lõpuks (30.11). Kaartidel 2631 ja 2634 sihtjoont ei ole,
seega neil näitame ainult seisu ja sihi, mitte vahet tempost.

Andmed loetakse ``data/YYYY-MM.yaml`` failist ``vp_goals`` ploki alt, mille
``fetch_monthly_data.py`` sinna kirjutab.
"""
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

GOAL_ORDER = ['goal_1', 'goal_2', 'goal_3', 'goal_4']

# Lühinimi koondtabelisse (kaardi enda nimi on aruande jaoks liiga pikk).
SHORT_TITLES = {
    'goal_1': '1. III samba avaldusega tuleb kaasa ka II sammas',
    'goal_2': '2. Sissemakse teinud OÜd',
    'goal_3': '3. Lapsed püsimaksega',
    'goal_4': '4. Kõrge palgaga kogujad tõstavad maksemäära',
}

MINUS = '−'  # U+2212, sama mis aruande ülejäänud märgiga arvudel


def _as_date(v):
    """Kaardi ``nadal`` väli tuleb kas date, datetime või ISO-stringina."""
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return datetime.fromisoformat(str(v)[:10]).date()


def _rows(vp, key):
    g = vp.get(key) or {}
    return g, [r for r in (g.get('data') or [])]


def _last_row_with(rows, col, month_end):
    """Viimane rida, kus ``col`` on täidetud ja nädal ei ületa kuu lõppu."""
    hits = [r for r in rows
            if r.get(col) is not None and _as_date(r['nadal']) <= month_end]
    return hits[-1] if hits else None


def _month_end(year, month):
    return date(year + (month == 12), month % 12 + 1, 1) - timedelta(days=1)


def summarise(vp_data: dict, year: int, month: int) -> list:
    """Koondrida iga eesmärgi kohta: siht, kuu lõpu seis, sihtjoon, vahe.

    Tagastab listi dictidest võtmetega ``key``, ``title``, ``target_label``,
    ``current_label``, ``pace_label``, ``gap_label``, ``on_track``.
    """
    if not vp_data:
        return []
    me = _month_end(year, month)
    out = []

    # --- Eesmärk 1: osakaal, kuu nädalate summa (mitte nädalate keskmine) ---
    g, rows = _rows(vp_data, 'goal_1')
    if rows:
        m = [r for r in rows
             if _as_date(r['nadal']).year == year and _as_date(r['nadal']).month == month]
        base = sum((r.get('sai_tuua_ii') or 0) for r in m)
        koos = sum((r.get('toi_koos') or 0) for r in m)
        hiljem = sum((r.get('toi_hiljem') or 0) for r in m)
        pct = koos / base * 100 if base else None
        pct_all = (koos + hiljem) / base * 100 if base else None
        # hiljem_taielik=False tähendab, et "tuli hiljem" aken pole veel sulgunud
        provisional = any(r.get('hiljem_taielik') is False for r in m)
        out.append({
            'key': 'goal_1',
            'title': SHORT_TITLES['goal_1'],
            'target_label': '30%',
            'current_label': (f'{pct:.1f}%'.replace('.', ',') if pct is not None else '–'),
            'current_note': (
                f'koos {koos}/{base}; hiljem lisandus {hiljem}'
                + (' (aken lahti)' if provisional else '')
            ),
            'pace_label': '–',
            'gap_label': (f'{MINUS}{30 - pct:.1f} pp'.replace('.', ',')
                          if pct is not None and pct < 30 else
                          (f'+{pct - 30:.1f} pp'.replace('.', ',') if pct is not None else '–')),
            'on_track': None if pct is None else pct >= 30,
            'extra': {'pct_all': pct_all, 'provisional': provisional},
        })

    # --- Eesmärgid 2 ja 3: kumulatiivne arv + sihtjoon ---
    for key, col, target in (('goal_2', 'oud_kokku', 500),
                             ('goal_3', 'pusimakse_kokku', 400)):
        g, rows = _rows(vp_data, key)
        r = _last_row_with(rows, col, me)
        if not r:
            continue
        cur = r[col]
        pace = r.get('sihtjoon')
        gap = (cur - pace) if pace is not None else None
        out.append({
            'key': key,
            'title': SHORT_TITLES[key],
            'target_label': str(target),
            'current_label': f'{cur:,}'.replace(',', ' '),
            'current_note': f"seis {_as_date(r['nadal']).strftime('%d.%m')}",
            'pace_label': (f'{pace:,}'.replace(',', ' ') if pace is not None else '–'),
            'gap_label': ('–' if gap is None else
                          (f'+{gap}' if gap >= 0 else f'{MINUS}{abs(gap)}')),
            'on_track': None if gap is None else gap >= 0,
            'extra': {},
        })

    # --- Eesmärk 4: kumulatiivne arv, sihtjoont kaardil ei ole ---
    g, rows = _rows(vp_data, 'goal_4')
    r = _last_row_with(rows, 'tostnud', me)
    if r:
        cur, cohort = r['tostnud'], r.get('kohort')
        out.append({
            'key': 'goal_4',
            'title': SHORT_TITLES['goal_4'],
            'target_label': '1350',
            'current_label': f'{cur:,}'.replace(',', ' '),
            'current_note': (f'kohordist {cohort:,}'.replace(',', ' ')
                             + f'; seis {_as_date(r["nadal"]).strftime("%d.%m")}'),
            'pace_label': '–',
            'gap_label': f'{MINUS}{1350 - cur:,}'.replace(',', ' '),
            'on_track': None,
            'extra': {},
        })

    return sorted(out, key=lambda d: GOAL_ORDER.index(d['key']))


def summary_md(rows: list) -> str:
    """Koondtabel markdownina."""
    if not rows:
        return ''
    lines = [
        '| Eesmärk | Siht | Seis | Sihtjoon | Vahe |',
        '|---|:---:|:---:|:---:|:---:|',
    ]
    for r in rows:
        lines.append(
            f"| {r['title']} | {r['target_label']} "
            f"| **{r['current_label']}** | {r['pace_label']} | {r['gap_label']} |"
        )
    return '\n'.join(lines)


# --------------------------------------------------------------------------
# Joonised
# --------------------------------------------------------------------------

def _style():
    base = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(base / 'common' / 'scripts'))
    import matplotlib
    matplotlib.use('Agg')
    from generate_charts import (setup_plot_style, TULEVA_BLUE, TULEVA_NAVY,
                                 TULEVA_MID_BLUE)
    setup_plot_style()
    return TULEVA_BLUE, TULEVA_NAVY, TULEVA_MID_BLUE


def _finish(ax, fig, title, out, ticks=None):
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from generate_charts import TULEVA_NAVY
    ax.set_title(title, fontweight='bold', color=TULEVA_NAVY, pad=12)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))
    if ticks:
        # Nädalaseeria: näita iga nädalat, aga hoia sildid loetavana
        step = max(1, len(ticks) // 12 + 1)
        ax.set_xticks(ticks[::step])
    for lbl in ax.get_xticklabels():
        lbl.set_rotation(45)
        lbl.set_ha('right')
    plt.tight_layout()
    out.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(out, dpi=130, bbox_inches='tight')
    plt.close(fig)


def _chart_goal_1(rows, out):
    import matplotlib.pyplot as plt
    BLUE, NAVY, MID = _style()
    x = [_as_date(r['nadal']) for r in rows]
    koos = [r.get('koos_pct') or 0 for r in rows]
    hiljem = [(r.get('kokku_pct') or 0) - (r.get('koos_pct') or 0) for r in rows]
    fig, ax = plt.subplots(figsize=(9, 4))
    ax.bar(x, koos, width=5, color=NAVY, label='tõi kohe koos', zorder=3)
    ax.bar(x, hiljem, width=5, bottom=koos, color=BLUE, label='tõi hiljem', zorder=3)
    ax.axhline(30, color='#FF4800', linestyle='--', linewidth=1.5, zorder=4)
    ax.text(x[0], 31, 'siht 30%', color='#FF4800', fontsize=9, fontweight='bold')
    ax.set_ylabel('% neist, kes said II samba tuua')
    ax.set_ylim(0, max(40, max([a + b for a, b in zip(koos, hiljem)] or [0]) * 1.2))
    ax.legend(frameon=False, fontsize=8.5, loc='upper left')
    _finish(ax, fig, 'Eesmärk 1: III samba avaldusega tuleb kaasa ka II sammas',
            out, ticks=x)


def _chart_cumulative(rows, col, pace_col, target, title, ylabel, out,
                      context=None):
    import matplotlib.pyplot as plt
    BLUE, NAVY, MID = _style()
    fig, ax = plt.subplots(figsize=(9, 4))

    pace = [(_as_date(r['nadal']), r.get(pace_col)) for r in rows
            if r.get(pace_col) is not None]
    if pace:
        ax.plot([p[0] for p in pace], [p[1] for p in pace], linestyle='--',
                color='#FF4800', linewidth=1.5, label='sihtjoon', zorder=3)

    if context:
        ccol, clabel = context
        c = [(_as_date(r['nadal']), r.get(ccol)) for r in rows
             if r.get(ccol) is not None]
        if c:
            ax.plot([p[0] for p in c], [p[1] for p in c], color=BLUE,
                    linewidth=1.5, alpha=0.8, label=clabel, zorder=3)

    act = [(_as_date(r['nadal']), r.get(col)) for r in rows if r.get(col) is not None]
    ax.plot([p[0] for p in act], [p[1] for p in act], color=NAVY, linewidth=2.5,
            marker='o', markersize=4, label='tegelik', zorder=5)

    ax.axhline(target, color=NAVY, linestyle=':', linewidth=1, alpha=0.6)
    ax.set_ylabel(ylabel)
    ax.set_ylim(0, target * 1.1)
    ax.legend(frameon=False, fontsize=8.5, loc='upper left')
    _finish(ax, fig, title, out)


def _chart_goal_4(rows, out):
    """Nädalane juurdekasv + kumulatiiv.

    Siht 1350 ei mahu siia teljele (august annab kümneid, mitte sadu) ja joon
    1350 juures muudaks tegeliku seeria nähtamatuks. Maksemäära avaldusi saab
    esitada 30.11-ni ja need laekuvad kuhjaga lõpu poole, seega on praegu
    loetav suurus tempo, mitte kaugus sihist. Kaugus on kirjas allkirjas.
    """
    import matplotlib.pyplot as plt
    BLUE, NAVY, MID = _style()
    x = [_as_date(r['nadal']) for r in rows]
    cum = [r.get('tostnud') or 0 for r in rows]
    add = [r.get('lisandunud') or 0 for r in rows]
    fig, ax = plt.subplots(figsize=(9, 4))
    ax.bar(x, add, width=2.5, color=BLUE, label='lisandus perioodil', zorder=3)
    ax.plot(x, cum, color=NAVY, linewidth=2.5, marker='o', markersize=4,
            label='kokku tõstnud', zorder=5)
    ax.set_ylabel('kogujat')
    ax.set_ylim(0, max(max(cum), max(add)) * 1.35 or 1)
    share = cum[-1] / 1350 * 100
    ax.text(0.98, 0.94,
            f'siht 1350 avaldust 30.11-ks\nseis {cum[-1]} ehk {share:.1f}% sihist'.replace('.', ','),
            transform=ax.transAxes, ha='right', va='top', fontsize=9,
            color='#FF4800', fontweight='bold')
    ax.legend(frameon=False, fontsize=8.5, loc='upper left')
    _finish(ax, fig,
            'Eesmärk 4: kõrge palgaga kogujad tõstavad II samba maksemäära',
            out, ticks=x)


def generate_charts(vp_data: dict, charts_dir: Path) -> dict:
    """Joonistab neli eesmärgigraafikut. Tagastab {key: suhteline tee}."""
    if not vp_data:
        return {}
    paths = {}

    _, rows = _rows(vp_data, 'goal_1')
    if rows:
        _chart_goal_1(rows, charts_dir / 'vp_goal_1.png')
        paths['vp_goal_1'] = 'charts/vp_goal_1.png'

    _, rows = _rows(vp_data, 'goal_2')
    if rows:
        _chart_cumulative(
            rows, 'oud_kokku', 'sihtjoon', 500,
            'Eesmärk 2: sissemakse teinud OÜd', 'OÜd kokku',
            charts_dir / 'vp_goal_2.png')
        paths['vp_goal_2'] = 'charts/vp_goal_2.png'

    _, rows = _rows(vp_data, 'goal_3')
    if rows:
        _chart_cumulative(
            rows, 'pusimakse_kokku', 'sihtjoon', 400,
            'Eesmärk 3: lapsed, kes koguvad püsimaksega', 'lapsi',
            charts_dir / 'vp_goal_3.png',
            context=('maksnud_kokku', 'sissemakse teinud (sh ühekordne)'))
        paths['vp_goal_3'] = 'charts/vp_goal_3.png'

    _, rows = _rows(vp_data, 'goal_4')
    if rows:
        _chart_goal_4(rows, charts_dir / 'vp_goal_4.png')
        paths['vp_goal_4'] = 'charts/vp_goal_4.png'

    return paths
