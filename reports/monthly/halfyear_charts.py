"""Charts for the half-year (poolaasta) blog report: 1H(year) vs 1H(year-1)."""
import sys
from pathlib import Path

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'common' / 'scripts'))
from generate_charts import setup_plot_style, TULEVA_BLUE, TULEVA_NAVY, TULEVA_MID_BLUE  # noqa: E402

PREV_COLOR = '#B0D4F1'   # light blue for the prior June
YE_COLOR = '#5AA9DE'     # mid blue for the prior year-end
POSITIVE_COLOR = '#51c26c'
NEGATIVE_COLOR = '#FF4800'


def _grouped_bars(ax, categories, cur_vals, prev_vals, cur_label, prev_label):
    import numpy as np
    x = np.arange(len(categories))
    w = 0.38
    b1 = ax.bar(x - w / 2, prev_vals, w, label=prev_label, color=PREV_COLOR, zorder=3)
    b2 = ax.bar(x + w / 2, cur_vals, w, label=cur_label, color=TULEVA_BLUE, zorder=3)
    ax.set_xticks(x)
    ax.set_xticklabels(categories)
    ax.legend(loc='upper left', fontsize=9)
    ax.grid(axis='y', alpha=0.3, zorder=0)
    return b1, b2


def _grouped_bars3(ax, categories, prev_vals, ye_vals, cur_vals, prev_label, ye_label, cur_label):
    """Three bars per category: prior June, prior year-end, current June."""
    import numpy as np
    x = np.arange(len(categories))
    w = 0.27
    b1 = ax.bar(x - w, prev_vals, w, label=prev_label, color=PREV_COLOR, zorder=3)
    b2 = ax.bar(x, ye_vals, w, label=ye_label, color=YE_COLOR, zorder=3)
    b3 = ax.bar(x + w, cur_vals, w, label=cur_label, color=TULEVA_BLUE, zorder=3)
    ax.set_xticks(x)
    ax.set_xticklabels(categories)
    ax.legend(loc='upper left', fontsize=9)
    ax.grid(axis='y', alpha=0.3, zorder=0)
    return b1, b2, b3


def _label_bars(ax, bars, fmt):
    for bar in bars:
        h = bar.get_height()
        va = 'bottom' if h >= 0 else 'top'
        off = 3 if h >= 0 else -3
        ax.annotate(fmt(h), (bar.get_x() + bar.get_width() / 2, h),
                    textcoords='offset points', xytext=(0, off),
                    ha='center', va=va, fontsize=8, color=TULEVA_NAVY)


def chart_aum(m, year, prev, out):
    fig, ax = plt.subplots(figsize=(9, 4.5))
    cats = ['Kokku', 'II sammas', 'III sammas']
    cur = [m['aum']['total'][0], m['aum']['ii'][0], m['aum']['iii'][0]]
    ye = [m['aum']['total'][1], m['aum']['ii'][1], m['aum']['iii'][1]]
    pre = [m['aum']['total'][2], m['aum']['ii'][2], m['aum']['iii'][2]]
    fmt = lambda v: f'{v:,.0f}'.replace(',', ' ')
    b1, b2, b3 = _grouped_bars3(ax, cats, pre, ye, cur,
                                f'30.06.{prev}', f'31.12.{prev}', f'30.06.{year}')
    for b in (b1, b2, b3):
        _label_bars(ax, b, fmt)
    ax.set_ylabel('M EUR')
    ax.set_title('Varade maht', fontweight='bold', color=TULEVA_NAVY)
    ax.set_ylim(top=ax.get_ylim()[1] * 1.12)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: fmt(v)))
    plt.tight_layout()
    plt.savefig(out / 'aum.png', dpi=130)
    plt.close()


def chart_savers(m, year, prev, out):
    fig, ax = plt.subplots(figsize=(9, 4.5))
    cats = ['Kokku', 'ainult II', 'ainult III', 'II ja III']
    cur = [m['savers']['total'][0], m['savers']['ii_only'][0],
           m['savers']['iii_only'][0], m['savers']['both'][0]]
    ye = [m['savers']['total'][1], m['savers']['ii_only'][1],
          m['savers']['iii_only'][1], m['savers']['both'][1]]
    pre = [m['savers']['total'][2], m['savers']['ii_only'][2],
           m['savers']['iii_only'][2], m['savers']['both'][2]]
    fmt = lambda v: f'{v:,.0f}'.replace(',', ' ')
    b1, b2, b3 = _grouped_bars3(ax, cats, pre, ye, cur,
                                f'30.06.{prev}', f'31.12.{prev}', f'30.06.{year}')
    for b in (b1, b2, b3):
        _label_bars(ax, b, fmt)
    ax.legend(loc='upper right', fontsize=9)  # left group is tallest here
    ax.set_ylabel('Kogujate arv')
    ax.set_title('Kogujate arv', fontweight='bold', color=TULEVA_NAVY)
    ax.set_ylim(top=ax.get_ylim()[1] * 1.12)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: fmt(v)))
    plt.tight_layout()
    plt.savefig(out / 'savers.png', dpi=130)
    plt.close()


def chart_contributions(m, year, prev, out):
    fig, ax = plt.subplots(figsize=(8, 4.5))
    cats = ['II sammas', 'III sammas', 'Kokku']
    cur = [m['contributions']['ii'][0] / 1e6, m['contributions']['iii'][0] / 1e6,
           m['contributions']['total'][0] / 1e6]
    pre = [m['contributions']['ii'][1] / 1e6, m['contributions']['iii'][1] / 1e6,
           m['contributions']['total'][1] / 1e6]
    b1, b2 = _grouped_bars(ax, cats, cur, pre, f'1. pa {year}', f'1. pa {prev}')
    _label_bars(ax, b1, lambda v: f'{v:.1f}')
    _label_bars(ax, b2, lambda v: f'{v:.1f}')
    ax.set_ylabel('M EUR')
    ax.set_title('Sissemaksed poolaastaga', fontweight='bold', color=TULEVA_NAVY)
    plt.tight_layout()
    plt.savefig(out / 'contributions.png', dpi=130)
    plt.close()


def chart_growth_waterfall(m, year, out):
    """1H growth-sources waterfall for the report year."""
    g = m['growth']
    steps = [
        ('Sissemaksed', g['sissemaksed'][0] / 1e6),
        ('Ületoodav\nvara', g['yletoodav'][0] / 1e6),
        ('Lahkujate\nvara', g['lahkujad'][0] / 1e6),
        ('Väljavõetud\nvara', g['valjavoetud'][0] / 1e6),
        ('Turu mõju', g['turg'][0] / 1e6),
    ]
    fig, ax = plt.subplots(figsize=(9, 4.5))
    cum = 0.0
    for label, val in steps:
        color = POSITIVE_COLOR if val >= 0 else NEGATIVE_COLOR
        bottom = cum if val >= 0 else cum + val
        ax.bar(label, abs(val), bottom=bottom, color=color, zorder=3)
        ytxt = cum + val + (2 if val >= 0 else -2)
        ax.annotate(f'{val:+.1f}', (label, ytxt), ha='center',
                    va='bottom' if val >= 0 else 'top', fontsize=8, color=TULEVA_NAVY)
        cum += val
    ax.axhline(0, color='gray', linewidth=0.6)
    ax.set_ylabel('M EUR')
    ax.set_title(f'Varade kasvu allikad 1. poolaastal {year}',
                 fontweight='bold', color=TULEVA_NAVY)
    ax.grid(axis='y', alpha=0.3, zorder=0)
    plt.tight_layout()
    plt.savefig(out / 'growth_waterfall.png', dpi=130)
    plt.close()


def generate_all(m, year, out_dir: Path):
    setup_plot_style()
    out_dir.mkdir(parents=True, exist_ok=True)
    prev = year - 1
    chart_aum(m, year, prev, out_dir)
    chart_savers(m, year, prev, out_dir)
    chart_contributions(m, year, prev, out_dir)
    chart_growth_waterfall(m, year, out_dir)
    print(f"  Charts saved to {out_dir}")
