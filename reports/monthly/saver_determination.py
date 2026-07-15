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


def compute_determination(df) -> dict:
    """Return aggregate counts for each determination segment."""
    rate = df['Current Rate'].fillna(0)
    iii = df['Third Pillar Last 12m Contributions Sum'].fillna(0)
    hi = rate.isin([4, 6])
    determined = hi & (iii >= DETERMINED_III_MIN)
    a = hi & (iii < DETERMINED_III_MIN)
    b = (rate == 2) & (iii >= DETERMINED_III_MIN)
    n = len(df)
    return {
        'total': n,
        'determined': int(determined.sum()),
        'halfway_a': int(a.sum()),
        'halfway_b': int(b.sum()),
        'halfway': int((a | b).sum()),
        'other': int((~(determined | a | b)).sum()),
    }


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
