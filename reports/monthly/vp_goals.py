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

# Lühinimi paneeli pealkirjaks (kaardi enda nimi on joonise jaoks liiga pikk).
SHORT_TITLES = {
    'goal_1': 'III samba avaldusega tuleb kaasa ka II sammas',
    'goal_2': 'Sissemakse teinud OÜd',
    'goal_3': 'Lapsed püsimaksega',
    'goal_4': 'Kõrge palgaga kogujad tõstavad maksemäära',
}

MINUS = '−'  # U+2212, sama mis aruande ülejäänud märgiga arvudel


def _as_date(v):
    """Kaardi ``nadal`` väli tuleb kas date, datetime või ISO-stringina."""
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return datetime.fromisoformat(str(v)[:10]).date()


def _day(r):
    """Rea kuupäev: nädalakaartidel ``nadal``, kuisel kaardil (2631 alates 2026-09) ``kuu``."""
    return _as_date(r['nadal'] if 'nadal' in r else r['kuu'])


def _rows(vp, key):
    g = vp.get(key) or {}
    return g, [r for r in (g.get('data') or [])]


def _last_row_with(rows, col, month_end):
    """Viimane rida, kus ``col`` on täidetud ja nädal ei ületa kuu lõppu."""
    hits = [r for r in rows
            if r.get(col) is not None and _day(r) <= month_end]
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
             if _day(r).year == year and _day(r).month == month]
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
            'current_note': f"seis {_day(r).strftime('%d.%m')}",
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
                             + f'; seis {_day(r).strftime("%d.%m")}'),
            'pace_label': '–',
            'gap_label': f'{MINUS}{1350 - cur:,}'.replace(',', ' '),
            'on_track': None,
            'extra': {},
        })

    return sorted(out, key=lambda d: GOAL_ORDER.index(d['key']))


# --------------------------------------------------------------------------
# Joonised
#
# Iga eesmärk on eraldi pilt kuuaruande mõõdus (14 x 5,5), aruandes üksteise
# all nagu missiooni tabloo KPI-d. Pealkiri ei ole pildil vaid aruande
# alampealkirjas. Iga paneeli joonistab oma funktsioon etteantud teljestikule.



def _style():
    base = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(base / 'common' / 'scripts'))
    import matplotlib
    matplotlib.use('Agg')
    from generate_charts import (setup_plot_style, TULEVA_BLUE, TULEVA_NAVY,
                                 TULEVA_MID_BLUE)
    setup_plot_style()
    return TULEVA_BLUE, TULEVA_NAVY, TULEVA_MID_BLUE


def _finish(ax, title=None, ticks=None):
    """Paneeli viimistlus. Pealkiri on aruandes, mitte pildil."""
    import matplotlib.dates as mdates
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d.%m'))
    if ticks:
        # Nädalaseeria: täislaiusel paneelil mahub ~12 silti
        step = max(1, len(ticks) // 12 + 1)
        ax.set_xticks(ticks[::step])
    else:
        ax.xaxis.set_major_locator(mdates.AutoDateLocator(maxticks=12))
    ax.tick_params(labelsize=11)
    ax.yaxis.label.set_size(11)
    ax.grid(axis='y', color='#e3e7ec', linewidth=1)
    ax.set_axisbelow(True)
    for lbl in ax.get_xticklabels():
        lbl.set_rotation(45)
        lbl.set_ha('right')


KUUD_LYHI = ['jaan', 'veebr', 'märts', 'apr', 'mai', 'juuni',
             'juuli', 'aug', 'sept', 'okt', 'nov', 'dets']


def _panel_goal_1(rows, ax):
    """Kuine: tulp kuu kohta (tõi kohe koos + tõi hiljem), punkt aasta varasema kuu kohta.

    Kaart 2631 on alates 2026-09 kuine. Kuu, mille „tõi hiljem" aken on veel lahti
    (``hiljem_taielik`` false), on heledam: selle osakaal võib veel kasvada.
    """
    BLUE, NAVY, MID = _style()
    days = [_day(r) for r in rows]
    x = list(range(len(rows)))
    koos = [r.get('koos_pct') or 0 for r in rows]
    hiljem = [(r.get('kokku_pct') or 0) - (r.get('koos_pct') or 0) for r in rows]
    lahti = [r.get('hiljem_taielik') is False for r in rows]
    alpha = [0.45 if o else 1.0 for o in lahti]
    for i in x:
        ax.bar(i, koos[i], width=0.6, color=NAVY, alpha=alpha[i], zorder=3,
               label='tõi kohe koos' if i == 0 else None)
        ax.bar(i, hiljem[i], width=0.6, bottom=koos[i], color=BLUE, alpha=alpha[i], zorder=3,
               label='tõi hiljem' if i == 0 else None)
        ax.text(i, koos[i] + hiljem[i] - 0.6, f'{koos[i] + hiljem[i]:.0f}%',
                ha='center', va='top', fontsize=10, color='white', fontweight='bold', zorder=5)
    eelmine = [(i, r.get('koos_pct_yoy')) for i, r in zip(x, rows) if r.get('koos_pct_yoy') is not None]
    if eelmine:
        import matplotlib.patheffects as pe
        ax.scatter([i for i, _ in eelmine], [v for _, v in eelmine], marker='_', s=420,
                   linewidths=2.5, color='#1a1a1a', zorder=4, label='aasta varem (tõi kohe koos)',
                   path_effects=[pe.Stroke(linewidth=5, foreground='white'), pe.Normal()])
    ax.axhline(30, color='#FF4800', linestyle='--', linewidth=1.5, zorder=2)
    ax.text(-0.4, 31, 'siht 30%', color='#FF4800', fontsize=11, fontweight='bold')
    ax.set_ylabel('% neist, kes said II samba tuua')
    ax.set_ylim(0, max(40, max([a + b for a, b in zip(koos, hiljem)] or [0]) * 1.2))
    ax.legend(frameon=False, fontsize=11, loc='upper left', ncol=3)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.set_xticks(x)
    ax.set_xticklabels([f'{KUUD_LYHI[d.month - 1]} {d.year % 100:02d}' for d in days])
    ax.tick_params(labelsize=11)
    ax.yaxis.label.set_size(11)
    ax.grid(axis='y', color='#e3e7ec', linewidth=1)
    ax.set_axisbelow(True)
    if any(lahti):
        ax.annotate('„tõi hiljem" aken lahti', (x[lahti.index(True)], 1), ha='center',
                    fontsize=9, color=MID, xytext=(0, -38), textcoords='offset points',
                    annotation_clip=False)


def _panel_cumulative(rows, col, pace_col, target, title, ylabel, ax,
                      context=None):
    BLUE, NAVY, MID = _style()

    pace = [(_day(r), r.get(pace_col)) for r in rows
            if r.get(pace_col) is not None]
    if pace:
        ax.plot([p[0] for p in pace], [p[1] for p in pace], linestyle='--',
                color='#FF4800', linewidth=1.5, label='sihtjoon', zorder=3)

    if context:
        ccol, clabel = context
        c = [(_day(r), r.get(ccol)) for r in rows
             if r.get(ccol) is not None]
        if c:
            ax.plot([p[0] for p in c], [p[1] for p in c], color=BLUE,
                    linewidth=1.5, alpha=0.8, label=clabel, zorder=3)

    act = [(_day(r), r.get(col)) for r in rows if r.get(col) is not None]
    ax.plot([p[0] for p in act], [p[1] for p in act], color=NAVY, linewidth=2.5,
            marker='o', markersize=3, label='tegelik', zorder=5)

    ax.axhline(target, color=NAVY, linestyle=':', linewidth=1, alpha=0.6)
    ax.set_ylabel(ylabel)
    ax.set_ylim(0, target * 1.1)
    ax.legend(frameon=False, fontsize=11, loc='upper left')
    _finish(ax, title)


def _panel_goal_4(rows, ax):
    """Nädalane juurdekasv + kumulatiiv.

    Siht 1350 ei mahu siia teljele (august annab kümneid, mitte sadu) ja joon
    1350 juures muudaks tegeliku seeria nähtamatuks. Maksemäära avaldusi saab
    esitada 30.11-ni ja need laekuvad kuhjaga lõpu poole, seega on praegu
    loetav suurus tempo, mitte kaugus sihist. Kaugus on kirjas paneeli nurgas.
    """
    BLUE, NAVY, MID = _style()
    x = [_day(r) for r in rows]
    cum = [r.get('tostnud') or 0 for r in rows]
    add = [r.get('lisandunud') or 0 for r in rows]
    ax.bar(x, add, width=2.5, color=BLUE, label='lisandus perioodil', zorder=3)
    ax.plot(x, cum, color=NAVY, linewidth=2.5, marker='o', markersize=3,
            label='kokku tõstnud', zorder=5)
    ax.set_ylabel('kogujat')
    ax.set_ylim(0, max(max(cum), max(add)) * 1.35 or 1)
    share = cum[-1] / 1350 * 100
    ax.text(0.98, 0.94,
            f'siht 1350 avaldust 30.11-ks\nseis {cum[-1]} ehk '
            + f'{share:.1f}'.replace('.', ',') + '% sihist',
            transform=ax.transAxes, ha='right', va='top', fontsize=11,
            color='#FF4800', fontweight='bold')
    ax.legend(frameon=False, fontsize=11, loc='upper left')
    _finish(ax, SHORT_TITLES['goal_4'], ticks=x)


def generate_charts(vp_data: dict, charts_dir: Path, year: int = None,
                    month: int = None) -> dict:
    """Joonistab iga eesmärgi eraldi pildile. Tagastab {'vp_goal_1': tee, ...}.

    ``year``/``month`` korral lõigatakse seeria kuu lõpus, et pilt ja
    ``summarise()`` näitaksid sama seisu (kaardid annavad ka järgmise kuu nädalaid).
    """
    me = _month_end(year, month) if year and month else None
    if not vp_data:
        return {}
    import matplotlib.pyplot as plt
    _style()
    charts_dir.mkdir(parents=True, exist_ok=True)
    out = {}
    for key in GOAL_ORDER:
        _, rows = _rows(vp_data, key)
        if me:
            rows = [r for r in rows if _day(r) <= me]
        if not rows:
            continue
        fig, ax = plt.subplots(figsize=(14, 5.5))
        if key == 'goal_1':
            _panel_goal_1(rows, ax)
        elif key == 'goal_2':
            _panel_cumulative(rows, 'oud_kokku', 'sihtjoon', 500,
                              SHORT_TITLES['goal_2'], 'OÜd kokku', ax)
        elif key == 'goal_3':
            _panel_cumulative(rows, 'pusimakse_kokku', 'sihtjoon', 400,
                              SHORT_TITLES['goal_3'], 'lapsi', ax,
                              context=('maksnud_kokku', 'sissemakse teinud (sh ühekordne)'))
        elif key == 'goal_4':
            _panel_goal_4(rows, ax)
        fig.tight_layout()
        nimi = key.replace('goal_', 'vp_goal_')
        plt.savefig(charts_dir / f'{nimi}.png', dpi=150, bbox_inches='tight')
        plt.close(fig)
        out[nimi] = f'charts/{nimi}.png'
    return out
