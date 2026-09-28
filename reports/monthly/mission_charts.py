"""Missiooni tabloo graafikud kuuaruandele: north star -riba ja viis KPI paneeli.

Iga paneel on eraldi pilt kuuaruande mõõdus (14 x 5,5, nagu generate_monthly_charts.py),
aruandes üksteise all. Lävendid on graafikul vööndina (2.3, 2.5) või sihtjoonena
(2.1, 2.3). Andmed: mission_kpis.compute().
"""
import sys
from datetime import date
from pathlib import Path

GOOD, WARN, CRIT = '#0ca30c', '#fab219', '#d03b3b'
ORANZ = '#FF4800'
TREPI_VARVID = ['#002F63', '#2f7fbf', '#7fbde0']
FIGSIZE = (14, 5.5)


def _style():
    base = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(base / 'common' / 'scripts'))
    import matplotlib
    matplotlib.use('Agg')
    from generate_charts import setup_plot_style, TULEVA_BLUE, TULEVA_NAVY
    setup_plot_style()
    return TULEVA_BLUE, TULEVA_NAVY


def _d(k):
    return date(k[0], k[1], 1)


def _et(v, dec=0):
    return f'{v:,.{dec}f}'.replace(',', ' ').replace('.', ',')


def _fmt(dec=0, yhik=''):
    import matplotlib.ticker as mt
    return mt.FuncFormatter(lambda v, _: f'{_et(v, dec)}{yhik}')


def _aastad(ax):
    import matplotlib.dates as md
    ax.xaxis.set_major_locator(md.YearLocator())
    ax.xaxis.set_major_formatter(md.DateFormatter('%Y'))
    ax.grid(axis='x', color='#e3e7ec', linewidth=1)


def _base(ax):
    ax.grid(axis='y', color='#e3e7ec', linewidth=1)
    ax.set_axisbelow(True)
    ax.tick_params(labelsize=11, colors='#4a5766')
    for s in ('left', 'bottom'):
        ax.spines[s].set_color('#c9d0d8')


def _vyondid(ax, vyondid):
    for a, b, c in vyondid:
        ax.axhspan(a, b, color=c, alpha=.09, linewidth=0, zorder=0)


def _ots(ax, x, y, color):
    ax.plot([x], [y], 'o', ms=9, color='white', zorder=5)
    ax.plot([x], [y], 'o', ms=6, color=color, zorder=6)


def _save(fig, out):
    import matplotlib.pyplot as plt
    fig.tight_layout()
    fig.savefig(out)
    plt.close(fig)


def north_star(ns, out):
    import matplotlib.pyplot as plt
    BLUE, NAVY = _style()
    fig, ax = plt.subplots(figsize=(14, 1.1))
    ax.barh([0], [ns['siht']], color='#cfe9f8', height=.6)
    ax.barh([0], [ns['vaartus']], color=BLUE, height=.6)
    ax.set_xlim(0, ns['siht'])
    ax.set_yticks([])
    ax.xaxis.set_major_formatter(_fmt())
    ax.tick_params(labelsize=10, colors='#4a5766')
    for s in ('left', 'top', 'right'):
        ax.spines[s].set_visible(False)
    ax.text(ns['vaartus'] + 800, 0, f"täna {_et(ns['vaartus'])}", va='center',
            fontsize=12, fontweight='bold', color=NAVY)
    _save(fig, out)


def kpi_21(k, out):
    import matplotlib.pyplot as plt
    BLUE, NAVY = _style()
    lv = k['lavend']
    xs, ys = zip(*k['seeria'])
    fig, ax = plt.subplots(figsize=FIGSIZE)
    _base(ax)
    ax.step(xs, ys, where='post', color=NAVY, linewidth=2.5)
    ax.plot([xs[-1], lv['siht_maht']], [ys[-1], lv['siht']], '--', color=NAVY,
            linewidth=1.8, alpha=.5)
    ax.plot([lv['siht_maht']], [lv['siht']], 'o', ms=8, mfc='white', mec=NAVY, mew=2)
    ax.annotate(f"siht {_et(lv['siht'], 2)}% @ 3 mld", (lv['siht_maht'], lv['siht']),
                xytext=(0, -20), textcoords='offset points', ha='right', fontsize=11,
                color='#4a5766')
    _ots(ax, xs[-1], ys[-1], NAVY)
    ax.set_xlim(0, lv['siht_maht'] * 1.01)
    ax.set_ylim(.20, .52)
    ax.set_xticks([0, 1000, 2000, 3000], ['0', '1 mld €', '2 mld €', '3 mld €'])
    ax.yaxis.set_major_formatter(_fmt(2, '%'))
    ax.grid(axis='x', color='#e3e7ec', linewidth=1)
    ax.set_xlabel('varade maht', fontsize=11, color='#4a5766')
    _save(fig, out)


def kpi_22(k, out):
    import matplotlib.pyplot as plt
    BLUE, NAVY = _style()
    x = [_d(a) for a, _ in k['seeria']]
    fig, ax = plt.subplots(figsize=FIGSIZE)
    _base(ax)
    ax.bar(x, [v for _, v in k['seeria']], width=22, color='#7fbde0', zorder=2,
           label='12 kuu summa, M€ (vasak telg)')
    ax.set_ylim(0, 300)
    ax.yaxis.set_major_formatter(_fmt(0, ' M€'))
    ax2 = ax.twinx()
    y2 = [v for _, v in k['seeria_muutus']]
    ax2.plot(x, y2, color=ORANZ, linewidth=2.5, zorder=4,
             label='muutus aasta varasemaga, % (parem telg)')
    _ots(ax2, x[-1], y2[-1], ORANZ)
    ax2.set_ylim(0, 50)                      # joon läheb 2024 korra pildilt välja
    ax2.yaxis.set_major_formatter(_fmt(0, '%'))
    ax2.tick_params(labelsize=11, colors=ORANZ)
    ax2.spines['right'].set_visible(True)
    ax2.spines['right'].set_color('#c9d0d8')
    ax2.spines['top'].set_visible(False)
    _aastad(ax)
    h1, l1 = ax.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax.legend(h1 + h2, l1 + l2, loc='upper left', frameon=False, fontsize=11)
    _save(fig, out)


def kpi_23(k, out):
    import matplotlib.pyplot as plt
    BLUE, NAVY = _style()
    lv = k['lavend']
    x = [_d(a) for a, _ in k['seeria']]
    y = [v for _, v in k['seeria']]
    fig, ax = plt.subplots(figsize=FIGSIZE)
    _base(ax)
    _vyondid(ax, [(0, lv['kollane'], CRIT), (lv['kollane'], lv['roheline'], WARN),
                  (lv['roheline'], 1200, GOOD)])
    ax.axhline(lv['siht'], color='#14202e', linewidth=1.5, alpha=.8, zorder=3)
    ax.text(x[0], lv['siht'] + 18, f"siht {_et(lv['siht'])} kuus", fontsize=12,
            fontweight='bold', color='#14202e')
    ax.plot(x, y, color=NAVY, linewidth=2.5, zorder=4)
    _ots(ax, x[-1], y[-1], NAVY)
    ax.set_ylim(0, 1200)
    ax.yaxis.set_major_formatter(_fmt())
    _aastad(ax)
    _save(fig, out)


def kpi_24(k, out):
    import matplotlib.pyplot as plt
    import matplotlib.dates as md
    BLUE, NAVY = _style()
    s = k['seeria']
    x = [_d(a) for a, _ in s['sihikindel']]
    kihid = ['sihikindel', 'poole teel: II tõstetud', 'poole teel: III üle 1200']
    nimed = ['Sihikindel', 'Poole teel: II tõstetud', 'Poole teel: III üle 1200']
    fig, ax = plt.subplots(figsize=FIGSIZE)
    _base(ax)
    ax.stackplot(x, *[[v for _, v in s[kk]] for kk in kihid], colors=TREPI_VARVID,
                 labels=nimed, zorder=2)
    ax.set_xlim(x[0], x[-1])
    ax.set_ylim(0, 50000)
    ax.yaxis.set_major_formatter(_fmt())
    ax.xaxis.set_major_locator(md.MonthLocator(bymonth=[1, 7]))
    ax.xaxis.set_major_formatter(md.DateFormatter('%m.%Y'))
    ax.grid(axis='x', color='#e3e7ec', linewidth=1)
    h, l = ax.get_legend_handles_labels()
    ax.legend(h[::-1], l[::-1], loc='upper left', frameon=False, fontsize=11)
    _save(fig, out)


def kpi_25(k, out):
    import matplotlib.pyplot as plt
    BLUE, NAVY = _style()
    lv = k['lavend']
    x = [_d(a) for a, _ in k['seeria']]
    y = [v for _, v in k['seeria']]
    fig, ax = plt.subplots(figsize=FIGSIZE)
    _base(ax)
    _vyondid(ax, [(0, lv['kollane'], GOOD), (lv['kollane'], lv['punane'], WARN),
                  (lv['punane'], 4, CRIT)])
    for v, t in ((lv['punane'], '3% punane piir'), (lv['kollane'], '2% kollane piir')):
        ax.axhline(v, color='#78838f', linewidth=1, linestyle='--', alpha=.6, zorder=3)
        ax.text(x[-1], v + .05, t, ha='right', fontsize=11, color='#78838f')
    ax.plot(x, y, color=NAVY, linewidth=2.5, zorder=4)
    _ots(ax, x[-1], y[-1], NAVY)
    ax.set_ylim(0, 4)
    ax.yaxis.set_major_formatter(_fmt(1, '%'))
    _aastad(ax)
    _save(fig, out)


def generate_charts(k: dict, charts_dir: Path) -> dict:
    """k = mission_kpis.compute(). Tagastab {nimi: suhteline tee}."""
    charts_dir.mkdir(parents=True, exist_ok=True)
    out = {}
    joonised = [('mission_north_star', north_star, k['north_star'])] + [
        (f"mission_{nr.replace('.', '')}", f, k['kpi'][nr])
        for nr, f in (('2.1', kpi_21), ('2.2', kpi_22), ('2.3', kpi_23),
                      ('2.4', kpi_24), ('2.5', kpi_25))]
    for nimi, f, arg in joonised:
        f(arg, charts_dir / f'{nimi}.png')
        out[nimi] = f'charts/{nimi}.png'
    return out
