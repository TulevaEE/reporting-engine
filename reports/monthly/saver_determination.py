"""Saver determination segmentation from per-saver card 2324.

Shared by the monthly and half-year reports. Segments savers by II-pillar
payment rate x III-pillar last-12-month contributions:

    Sihikindel     : II rate in {4,6} AND III last-12m >= 1200 EUR
    Poole teel (A) : II rate in {4,6} AND III last-12m <  1200 EUR
    Poole teel (B) : II rate == 2     AND III last-12m >= 1200 EUR
    Muud           : everything else

Card 2324 is a *current snapshot* (no history). To build a history over time,
``load_card_2324`` writes a date-stamped snapshot on every live fetch, so future
period-over-period comparisons become possible.

Per-person data is cached OUTSIDE the repo (~/.cache/tuleva-reports/).
Committed report outputs contain only the aggregate counts produced here.
"""
import os
import sys
from datetime import date
from pathlib import Path

DETERMINED_III_MIN = 1200  # EUR, III pillar last-12m contributions threshold

# Annual-report baseline (2025 aastaaruanne, as of 31 Dec 2025). Card 2324 has no
# history, so this hardcoded snapshot is the YTD baseline for over-time comparison.
# halfway_a here is the sum of the two annual-report sub-bands with II 4/6% and
# III < 1200 EUR: "III sambasse aktiivselt ei kogu" (5 463) + "kogub mõõdukalt" (4 318).
BASELINE_2025 = {
    'label': '31.12.2025',
    'total': 83378,
    'determined': 12796,   # II 4/6% + III kogub aktiivselt (>1200 €)
    'halfway_a': 9781,     # II 4/6% + III < 1200 € (ei kogu 5 463 + mõõdukalt 4 318)
    'halfway_b': 5777,     # II 2% + III kogub aktiivselt (>1200 €)
    'halfway': 15558,      # halfway_a + halfway_b
    'other': 55024,        # Ülejäänud
}

# (label, dict key, indent flag) for the determination comparison table rows.
_SEG_ROWS = [
    ('**Sihikindlad** (II 4/6% ja III ≥ 1200 €)', 'determined'),
    ('**Sihikindla poole teel**', 'halfway'),
    ('&nbsp;&nbsp;– II 4/6%, aga III < 1200 €', 'halfway_a'),
    ('&nbsp;&nbsp;– II 2%, aga III ≥ 1200 €', 'halfway_b'),
    ('Muud', 'other'),
]

_CACHE = Path(os.environ.get('TULEVA_CACHE_DIR', Path.home() / '.cache' / 'tuleva-reports'))


def _snapshot_path(day: str) -> Path:
    return _CACHE / f'card_2324_savers_{day}.pkl'


def load_card_2324(refresh: bool = False):
    """Return per-saver card 2324 as a DataFrame, or None if unavailable.

    Uses the working cache ``card_2324_savers.pkl``. When it must fetch live
    (missing cache or ``refresh=True``), it also writes a dated snapshot
    ``card_2324_savers_YYYY-MM-DD.pkl`` to build history over time.
    """
    import pandas as pd
    pk = _CACHE / 'card_2324_savers.pkl'
    if pk.exists() and not refresh:
        return pd.read_pickle(pk)
    try:
        base = Path(__file__).parent.parent.parent
        sys.path.insert(0, str(base / 'common' / 'scripts'))
        from dotenv import load_dotenv
        from metabase_client import MetabaseClient
        load_dotenv(base / '.env')
        df = pd.DataFrame(MetabaseClient().execute_card(2324))
        _CACHE.mkdir(parents=True, exist_ok=True)
        df.to_pickle(pk)
        df.to_pickle(_snapshot_path(date.today().isoformat()))
        return df
    except Exception as e:  # noqa: BLE001 - graceful degradation
        print(f"  Warning: could not load card 2324 ({e}); skipping determination section.")
        return None


def save_snapshot(df, day: str = None) -> Path:
    """Persist a date-stamped snapshot of the saver base for future comparisons."""
    _CACHE.mkdir(parents=True, exist_ok=True)
    day = day or date.today().isoformat()
    path = _snapshot_path(day)
    df.to_pickle(path)
    return path


def list_snapshots() -> list:
    """Available dated snapshots, oldest first: list of (date_str, Path)."""
    out = []
    for p in sorted(_CACHE.glob('card_2324_savers_*.pkl')):
        day = p.stem.replace('card_2324_savers_', '')
        if len(day) == 10 and day[4] == '-':  # YYYY-MM-DD
            out.append((day, p))
    return out


# Positive AUM in any pillar => an "active" saver, matching the population that
# card 2578 counts as an active investor. Card 2324 also carries non-active people
# (exited / fully redeemed), which is why its raw row count is ~13k higher.
_AUM_COLS = ['Tuk75 Current Aum', 'Tuk00 Current Aum', 'Third Pillar Current Aum']


def compute_determination(df, active_total: int = None) -> dict:
    """Return aggregate counts for each determination segment.

    Segments are computed over *active* savers only (positive AUM in at least one
    pillar), so the base matches the official active-investor count from card 2578
    rather than card 2324's broader row count. Pass ``active_total`` (that official
    count) to anchor the base exactly; ``other`` (Muud) is then the residual. Left
    unset, the base is card 2324's own active subset.
    """
    active = (df[_AUM_COLS].fillna(0) > 0).any(axis=1)
    a = df[active]
    rate = a['Current Rate'].fillna(0)
    iii = a['Third Pillar Last 12m Contributions Sum'].fillna(0)
    hi = rate.isin([4, 6])
    determined = int((hi & (iii >= DETERMINED_III_MIN)).sum())
    halfway_a = int((hi & (iii < DETERMINED_III_MIN)).sum())
    halfway_b = int(((rate == 2) & (iii >= DETERMINED_III_MIN)).sum())
    total = int(active_total) if active_total else int(active.sum())
    halfway = halfway_a + halfway_b
    return {
        'total': total,
        'determined': determined,
        'halfway_a': halfway_a,
        'halfway_b': halfway_b,
        'halfway': halfway,
        'other': total - determined - halfway,
    }


def determination_comparison_md(cur: dict, cur_label: str,
                                base: dict = BASELINE_2025, sep: str = ',') -> str:
    """Markdown YTD comparison table: baseline snapshot vs current determination.

    Each segment shows count (share) at both dates plus the change in count and in
    share (percentage points). ``sep`` is the thousands separator so each report
    keeps its own number style (',' for the monthly, ' ' for the half-year report).
    """
    minus = '−'  # U+2212, matches the reports' other signed figures

    def num(v):
        return f'{v:,}'.replace(',', sep)

    def signed(v):
        return f'{v:+,}'.replace(',', sep).replace('-', minus)

    def share(v, total):
        return f'{v / total:.1%}'.replace('.', ',')

    def dpp(seg):
        d = (cur[seg] / cur['total'] - base[seg] / base['total']) * 100
        return f'{d:+.1f}'.replace('.', ',').replace('-', minus) + ' pp'

    lines = [
        f'| Grupp | {base["label"]} | {cur_label} | Muutus |',
        '|---|:---:|:---:|:---:|',
    ]
    for label, seg in _SEG_ROWS:
        lines.append(
            f'| {label} '
            f'| {num(base[seg])} ({share(base[seg], base["total"])}) '
            f'| {num(cur[seg])} ({share(cur[seg], cur["total"])}) '
            f'| {signed(cur[seg] - base[seg])} ({dpp(seg)}) |'
        )
    lines.append(
        f'| **Kogujaid kokku** | **{num(base["total"])}** | **{num(cur["total"])}** '
        f'| **{signed(cur["total"] - base["total"])}** |'
    )
    return '\n'.join(lines)


def generate_determination_chart(det: dict, output_file: Path):
    """Horizontal 100% stacked bar of saver determination (current snapshot)."""
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.patches import Patch

    base = Path(__file__).parent.parent.parent
    sys.path.insert(0, str(base / 'common' / 'scripts'))
    from generate_charts import setup_plot_style, TULEVA_BLUE, TULEVA_NAVY, TULEVA_MID_BLUE
    setup_plot_style()

    n = det['total']
    segs = [
        ('Sihikindlad', det['determined'], TULEVA_NAVY),
        ('Sihikindla poole teel (II 2%, III ≥1200)', det['halfway_b'], TULEVA_MID_BLUE),
        ('Sihikindla poole teel (II 4/6%, III <1200)', det['halfway_a'], TULEVA_BLUE),
        ('Muud', det['other'], '#D0D0D0'),
    ]
    fig, ax = plt.subplots(figsize=(10, 2.9))
    left = 0.0
    for label, val, color in segs:
        share = val / n * 100
        ax.barh(0, share, left=left, color=color, edgecolor='white', height=0.6, zorder=3)
        if share > 4:
            txt_color = 'white' if color in (TULEVA_NAVY, TULEVA_MID_BLUE, TULEVA_BLUE) else TULEVA_NAVY
            ax.text(left + share / 2, 0, f'{val:,}'.replace(',', ' ') + f'\n{share:.0f}%',
                    ha='center', va='center', fontsize=9, color=txt_color, fontweight='bold')
        left += share
    ax.set_xlim(0, 100)
    ax.set_ylim(-0.5, 0.5)
    ax.axis('off')
    handles = [Patch(facecolor=c, label=lbl) for lbl, _, c in segs]
    ax.legend(handles=handles, loc='upper center', bbox_to_anchor=(0.5, -0.05),
              ncol=2, frameon=False, fontsize=8.5)
    ax.set_title('Kogujate sihikindlus (osakaal kõigist kogujatest)',
                 fontweight='bold', color=TULEVA_NAVY, pad=12)
    plt.tight_layout()
    plt.savefig(output_file, dpi=130, bbox_inches='tight')
    plt.close()
