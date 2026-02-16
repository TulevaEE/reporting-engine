"""
Generate charts for the monthly board report from YAML data.
"""
import yaml
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
from pathlib import Path

# Import shared style setup
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'common' / 'scripts'))
from generate_charts import setup_plot_style, TULEVA_BLUE, TULEVA_NAVY, TULEVA_MID_BLUE

# Chart colors
POSITIVE_COLOR = '#51c26c'
NEGATIVE_COLOR = '#FF4800'
TOTAL_COLOR = TULEVA_NAVY
FORECAST_COLOR = '#B0D4F1'
ACTUAL_COLOR = TULEVA_BLUE

ESTONIAN_MONTHS = {
    1: 'jaanuar', 2: 'veebruar', 3: 'märts', 4: 'aprill',
    5: 'mai', 6: 'juuni', 7: 'juuli', 8: 'august',
    9: 'september', 10: 'oktoober', 11: 'november', 12: 'detsember',
}


def generate_aum_chart(aum_data, report_year, report_month, output_dir: Path):
    """Generate AUM bar chart with dual-axis growth lines (card 334).

    Left Y-axis: AUM columns (dark blue = actual, light blue = forecast).
    Right Y-axis: 12-month growth % and organic growth % as lines.
    """
    print("Generating AUM chart...")

    months = []
    aum_values = []
    growth_pct = []
    organic_pct = []
    is_forecast = []

    for row in aum_data:
        month_str = row['month']
        forecast = month_str.startswith('prog:')
        clean = month_str.replace('prog:', '')
        months.append(clean)
        aum_values.append(row['kuu lõpu AUM (M EUR)'])
        growth_pct.append(row['AUM 12 kuu kasv %'])
        organic_pct.append(row['AUM 12 kuu kasv sissemaksetest ja -vahetustest %'])
        is_forecast.append(forecast)

    fig, ax_bar = plt.subplots(figsize=(12, 5.5))
    ax_line = ax_bar.twinx()

    x = np.arange(len(months))
    bar_colors = [ACTUAL_COLOR if not f else FORECAST_COLOR for f in is_forecast]

    # AUM columns
    ax_bar.bar(x, aum_values, color=bar_colors, width=0.7, zorder=2)

    # Mark the report month
    report_month_abbr = (
        f"{'Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec'.split()[report_month-1]}"
        f"-{report_year % 100:02d}"
    )
    if report_month_abbr in months:
        idx = months.index(report_month_abbr)
        ax_bar.annotate(
            f'{aum_values[idx]:,.0f} M',
            (idx, aum_values[idx]),
            textcoords="offset points", xytext=(0, 8),
            ha='center', fontweight='bold', fontsize=9, color=TULEVA_NAVY,
        )

    # Growth lines on secondary axis
    actual_idx = [i for i, f in enumerate(is_forecast) if not f]
    forecast_idx = [i for i, f in enumerate(is_forecast) if f]

    # 12-month growth
    if actual_idx:
        ax_line.plot(
            x[actual_idx], [growth_pct[i] for i in actual_idx],
            color='#FF4800', linewidth=2, marker='o', markersize=3,
            label='AUM 12 kuu kasv %', zorder=4,
        )
    if forecast_idx and actual_idx:
        bridge = [actual_idx[-1]] + forecast_idx
        ax_line.plot(
            x[bridge], [growth_pct[i] for i in bridge],
            color='#FF4800', linewidth=1.5, linestyle='--', marker='o',
            markersize=2, zorder=3,
        )

    # Organic growth
    if actual_idx:
        ax_line.plot(
            x[actual_idx], [organic_pct[i] for i in actual_idx],
            color='#51c26c', linewidth=2, marker='s', markersize=3,
            label='sh orgaaniline kasv %', zorder=4,
        )
    if forecast_idx and actual_idx:
        bridge = [actual_idx[-1]] + forecast_idx
        ax_line.plot(
            x[bridge], [organic_pct[i] for i in bridge],
            color='#51c26c', linewidth=1.5, linestyle='--', marker='s',
            markersize=2, zorder=3,
        )

    # Axes formatting
    ax_bar.set_ylabel('AUM (M EUR)')
    ax_bar.set_ylim(bottom=0)
    ax_bar.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f'{v:,.0f}'))

    ax_line.set_ylabel('12 kuu kasv (%)')
    ax_line.set_ylim(bottom=0)
    ax_line.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f'{v:.0f}%'))
    ax_line.tick_params(axis='y', length=0, pad=8)

    ax_bar.set_title('Varade maht (AUM)', fontweight='bold', color=TULEVA_NAVY)
    ax_bar.set_xticks(x[::2])
    ax_bar.set_xticklabels(
        [months[i] for i in range(0, len(months), 2)],
        rotation=45, ha='right', fontsize=8,
    )
    ax_bar.grid(axis='y', alpha=0.3, zorder=0)

    # Combined legend
    from matplotlib.patches import Patch
    from matplotlib.lines import Line2D
    legend_elements = [
        Patch(facecolor=ACTUAL_COLOR, label='AUM (tegelik)'),
        Patch(facecolor=FORECAST_COLOR, label='AUM (prognoos)'),
        Line2D([0], [0], color='#FF4800', linewidth=2, label='AUM 12 kuu kasv %'),
        Line2D([0], [0], color='#51c26c', linewidth=2, label='sh orgaaniline kasv %'),
    ]
    ax_bar.legend(handles=legend_elements, loc='upper left', fontsize=8)

    plt.tight_layout()
    output_file = output_dir / 'aum.png'
    plt.savefig(output_file)
    plt.close()
    print(f"  Saved: {output_file}")


def generate_savers_chart(savers_data, report_year, report_month, output_dir: Path):
    """Generate savers stacked bar chart with YoY growth line (card 1515).

    Left Y-axis: stacked columns (ainult II, ainult III, II ja III).
    Right Y-axis: YoY growth % line.
    """
    print("Generating savers chart...")

    months = []
    only_ii = []
    only_iii = []
    both = []
    yoy = []

    for row in savers_data:
        date_str = row['kuu: Month']
        # Convert '2025-01-01' to 'Jan-25'
        parts = date_str.split('-')
        yr, mo = int(parts[0]), int(parts[1])
        abbr = 'Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec'.split()[mo - 1]
        months.append(f'{abbr}-{yr % 100:02d}')
        only_ii.append(row['ainult II sammas'])
        only_iii.append(row['ainult III sammas'])
        both.append(row['II ja III sammas'])
        yoy.append(row['YoY, %'] * 100)

    fig, ax_bar = plt.subplots(figsize=(10, 5.5))
    ax_line = ax_bar.twinx()

    x = np.arange(len(months))
    width = 0.7

    # Stacked bars
    bars_ii = ax_bar.bar(x, only_ii, width, label='Ainult II sammas',
                         color=TULEVA_NAVY, zorder=2)
    bars_both = ax_bar.bar(x, both, width, bottom=only_ii,
                           label='II ja III sammas', color=TULEVA_MID_BLUE, zorder=2)
    bottom_iii = [a + b for a, b in zip(only_ii, both)]
    bars_iii = ax_bar.bar(x, only_iii, width, bottom=bottom_iii,
                          label='Ainult III sammas', color=FORECAST_COLOR, zorder=2)

    # Mark the report month
    report_month_abbr = (
        f"{'Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec'.split()[report_month - 1]}"
        f"-{report_year % 100:02d}"
    )
    if report_month_abbr in months:
        idx = months.index(report_month_abbr)
        total = only_ii[idx] + only_iii[idx] + both[idx]
        ax_bar.annotate(
            f'{total:,}',
            (idx, total),
            textcoords="offset points", xytext=(0, 8),
            ha='center', fontweight='bold', fontsize=9, color=TULEVA_NAVY,
        )

    # YoY growth line on secondary axis
    ax_line.plot(x, yoy, color='#FF4800', linewidth=2, marker='o', markersize=3,
                 label='YoY kasv %', zorder=4)

    # Axes formatting
    ax_bar.set_ylabel('Kogujate arv')
    ax_bar.set_ylim(bottom=0)
    ax_bar.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f'{v:,.0f}'))

    ax_line.set_ylabel('YoY kasv (%)')
    ax_line.set_ylim(bottom=0)
    ax_line.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f'{v:.0f}%'))
    ax_line.tick_params(axis='y', length=0, pad=8)

    ax_bar.set_title('Kogujate arv', fontweight='bold', color=TULEVA_NAVY)
    ax_bar.set_xticks(x)
    ax_bar.set_xticklabels(months, rotation=45, ha='right', fontsize=8)
    ax_bar.grid(axis='y', alpha=0.3, zorder=0)

    # Combined legend
    from matplotlib.patches import Patch
    from matplotlib.lines import Line2D
    legend_elements = [
        Patch(facecolor=TULEVA_NAVY, label='Ainult II sammas'),
        Patch(facecolor=TULEVA_MID_BLUE, label='II ja III sammas'),
        Patch(facecolor=FORECAST_COLOR, label='Ainult III sammas'),
        Line2D([0], [0], color='#FF4800', linewidth=2, label='YoY kasv %'),
    ]
    ax_bar.legend(handles=legend_elements, loc='upper left', fontsize=8)

    plt.tight_layout()
    output_file = output_dir / 'savers.png'
    plt.savefig(output_file)
    plt.close()
    print(f"  Saved: {output_file}")


def _parse_month_label(date_str):
    """Convert '2025-01-01' to 'Jan-25'."""
    parts = date_str.split('-')
    yr, mo = int(parts[0]), int(parts[1])
    abbr = 'Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec'.split()[mo - 1]
    return f'{abbr}-{yr % 100:02d}'


def generate_new_savers_by_pillar_chart(ii_data, iii_data, report_year, report_month, output_dir: Path):
    """Generate new savers stacked bar chart by pillar (cards 1519 + 1520).

    Stacked columns: only II (1519['2']), only III (1520['3']),
    both II+III (1519['2+3']).
    """
    print("Generating new savers by pillar chart...")

    # Build lookup for III-only from card 1520 keyed by month
    iii_by_month = {}
    for row in iii_data:
        iii_by_month[row['kuu: Month']] = row['3']

    months = []
    only_ii = []
    only_iii = []
    both = []

    for row in ii_data:
        date_str = row['kuu: Month']
        months.append(_parse_month_label(date_str))
        only_ii.append(row['2'])
        both.append(row['2+3'])
        only_iii.append(iii_by_month.get(date_str, 0))

    fig, ax = plt.subplots(figsize=(10, 5.5))

    x = np.arange(len(months))
    width = 0.7

    ax.bar(x, only_ii, width, label='Ainult II sammas',
           color=TULEVA_NAVY, zorder=2)
    ax.bar(x, both, width, bottom=only_ii,
           label='II ja III sammas', color=TULEVA_MID_BLUE, zorder=2)
    bottom_iii = [a + b for a, b in zip(only_ii, both)]
    ax.bar(x, only_iii, width, bottom=bottom_iii,
           label='Ainult III sammas', color=FORECAST_COLOR, zorder=2)

    # Annotate report month total
    report_month_abbr = (
        f"{'Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec'.split()[report_month - 1]}"
        f"-{report_year % 100:02d}"
    )
    if report_month_abbr in months:
        idx = months.index(report_month_abbr)
        total = only_ii[idx] + both[idx] + only_iii[idx]
        ax.annotate(
            f'{total:,}',
            (idx, total),
            textcoords="offset points", xytext=(0, 8),
            ha='center', fontweight='bold', fontsize=9, color=TULEVA_NAVY,
        )

    ax.set_ylabel('Uute kogujate arv')
    ax.set_ylim(bottom=0)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f'{v:,.0f}'))
    ax.set_title('Uued kogujad samba järgi', fontweight='bold', color=TULEVA_NAVY)
    ax.set_xticks(x)
    ax.set_xticklabels(months, rotation=45, ha='right', fontsize=8)
    ax.grid(axis='y', alpha=0.3, zorder=0)
    ax.legend(loc='upper left', fontsize=8)

    plt.tight_layout()
    output_file = output_dir / 'new_savers_pillar.png'
    plt.savefig(output_file)
    plt.close()
    print(f"  Saved: {output_file}")


def generate_new_ii_savers_by_source_chart(ii_data, report_year, report_month, output_dir: Path):
    """Generate new II pillar savers stacked bar chart by source (card 1519).

    Stacked columns: '2' (new II-only), '2+3' (opened both), '3>2' (had III, added II).
    """
    print("Generating new II savers by source chart...")

    months = []
    new_ii_only = []
    new_both = []
    from_iii = []

    for row in ii_data:
        months.append(_parse_month_label(row['kuu: Month']))
        new_ii_only.append(row['2'])
        new_both.append(row['2+3'])
        from_iii.append(row['3>2'])

    fig, ax = plt.subplots(figsize=(10, 5.5))

    x = np.arange(len(months))
    width = 0.7

    ax.bar(x, new_ii_only, width, label='Uus II samba koguja',
           color=TULEVA_NAVY, zorder=2)
    ax.bar(x, new_both, width, bottom=new_ii_only,
           label='Avas II ja III samba', color=TULEVA_MID_BLUE, zorder=2)
    bottom_from_iii = [a + b for a, b in zip(new_ii_only, new_both)]
    ax.bar(x, from_iii, width, bottom=bottom_from_iii,
           label='III samba koguja tõi II samba üle', color=FORECAST_COLOR, zorder=2)

    # Annotate report month total
    report_month_abbr = (
        f"{'Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec'.split()[report_month - 1]}"
        f"-{report_year % 100:02d}"
    )
    if report_month_abbr in months:
        idx = months.index(report_month_abbr)
        total = new_ii_only[idx] + new_both[idx] + from_iii[idx]
        ax.annotate(
            f'{total:,}',
            (idx, total),
            textcoords="offset points", xytext=(0, 8),
            ha='center', fontweight='bold', fontsize=9, color=TULEVA_NAVY,
        )

    ax.set_ylabel('II sambaga liitujate arv')
    ax.set_ylim(bottom=0)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f'{v:,.0f}'))
    ax.set_title('II sambaga liitujad allika järgi', fontweight='bold', color=TULEVA_NAVY)
    ax.set_xticks(x)
    ax.set_xticklabels(months, rotation=45, ha='right', fontsize=8)
    ax.grid(axis='y', alpha=0.3, zorder=0)
    ax.legend(loc='upper left', fontsize=8)

    plt.tight_layout()
    output_file = output_dir / 'new_ii_savers_source.png'
    plt.savefig(output_file)
    plt.close()
    print(f"  Saved: {output_file}")


def generate_contributions_chart(ii_data, iii_data, report_year, report_month, output_dir: Path):
    """Generate contributions stacked bar chart with Y-axis break (cards 1513 + 1512).

    Stacked columns: II pillar (dark) + III pillar (mid-blue) in M EUR.
    Uses a broken Y-axis when outlier bars exceed 2x the median.
    """
    print("Generating contributions chart...")

    # Build lookup for III pillar by month
    iii_by_month = {}
    for row in iii_data:
        iii_by_month[row['kuu: Month']] = row

    months = []
    ii_vals = []
    iii_vals = []

    for row in ii_data:
        date_str = row['kuu: Month']
        months.append(_parse_month_label(date_str))
        ii_eur = row['II samba sissemaksed, M EUR']
        iii_row = iii_by_month.get(date_str, {})
        iii_eur = iii_row.get('III samba sissemaksed, M EUR', 0)
        ii_vals.append(ii_eur / 1_000_000)
        iii_vals.append(iii_eur / 1_000_000)

    totals = [a + b for a, b in zip(ii_vals, iii_vals)]
    x = np.arange(len(months))
    width = 0.7

    # Determine if we need a broken axis
    sorted_totals = sorted(totals)
    median_val = sorted_totals[len(sorted_totals) // 2]
    max_val = max(totals)
    needs_break = max_val > 2 * median_val

    if needs_break:
        # Find the break range: gap between the "normal" cluster and outliers
        normal_max = sorted_totals[-2]  # second-highest value
        break_bottom = normal_max * 1.15
        break_top = max_val * 0.75

        fig, (ax_top, ax_bot) = plt.subplots(
            2, 1, sharex=True, figsize=(10, 5.5),
            gridspec_kw={'height_ratios': [1, 3], 'hspace': 0.06},
        )

        for ax in (ax_top, ax_bot):
            ax.bar(x, ii_vals, width, color=TULEVA_NAVY, zorder=2)
            ax.bar(x, iii_vals, width, bottom=ii_vals, color=TULEVA_MID_BLUE, zorder=2)
            ax.grid(axis='y', alpha=0.3, zorder=0)

        ax_top.set_ylim(break_top, max_val * 1.12)
        ax_bot.set_ylim(0, break_bottom)

        # Hide spines at the break
        ax_top.spines['bottom'].set_visible(False)
        ax_bot.spines['top'].set_visible(False)
        ax_top.tick_params(bottom=False)

        # Draw diagonal break marks
        d = 0.012
        kwargs = dict(transform=ax_top.transAxes, color='gray', clip_on=False, linewidth=1)
        ax_top.plot((-d, +d), (-d*3, +d*3), **kwargs)
        ax_top.plot((1 - d, 1 + d), (-d*3, +d*3), **kwargs)
        kwargs['transform'] = ax_bot.transAxes
        ax_bot.plot((-d, +d), (1 - d*3, 1 + d*3), **kwargs)
        ax_bot.plot((1 - d, 1 + d), (1 - d*3, 1 + d*3), **kwargs)

        # Formatting
        ax_top.set_title('Sissemaksed', fontweight='bold', color=TULEVA_NAVY)
        for ax in (ax_top, ax_bot):
            ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f'{v:,.0f}'))

        # Shared Y label
        fig.text(0.01, 0.5, 'Sissemaksed (M EUR)', va='center', rotation='vertical',
                 fontsize=10)

        ax_bot.set_xticks(x)
        ax_bot.set_xticklabels(months, rotation=45, ha='right', fontsize=8)

        # Annotate report month
        report_month_abbr = (
            f"{'Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec'.split()[report_month - 1]}"
            f"-{report_year % 100:02d}"
        )
        if report_month_abbr in months:
            idx = months.index(report_month_abbr)
            total = totals[idx]
            target_ax = ax_top if total > break_top else ax_bot
            target_ax.annotate(
                f'{total:,.1f} M',
                (idx, total),
                textcoords="offset points", xytext=(0, 8),
                ha='center', fontweight='bold', fontsize=9, color=TULEVA_NAVY,
            )

        # Annotate outlier bars in top panel
        for i, t in enumerate(totals):
            if t > break_top and not (report_month_abbr in months and i == months.index(report_month_abbr)):
                ax_top.annotate(
                    f'{t:,.1f} M',
                    (i, t),
                    textcoords="offset points", xytext=(0, 8),
                    ha='center', fontsize=8, color=TULEVA_NAVY,
                )

        # Legend on bottom axes
        from matplotlib.patches import Patch
        legend_elements = [
            Patch(facecolor=TULEVA_NAVY, label='II sammas'),
            Patch(facecolor=TULEVA_MID_BLUE, label='III sammas'),
        ]
        ax_bot.legend(handles=legend_elements, loc='upper left', fontsize=8)

    else:
        fig, ax = plt.subplots(figsize=(10, 5.5))

        ax.bar(x, ii_vals, width, label='II sammas',
               color=TULEVA_NAVY, zorder=2)
        ax.bar(x, iii_vals, width, bottom=ii_vals,
               label='III sammas', color=TULEVA_MID_BLUE, zorder=2)

        # Annotate report month
        report_month_abbr = (
            f"{'Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec'.split()[report_month - 1]}"
            f"-{report_year % 100:02d}"
        )
        if report_month_abbr in months:
            idx = months.index(report_month_abbr)
            total = totals[idx]
            ax.annotate(
                f'{total:,.1f} M',
                (idx, total),
                textcoords="offset points", xytext=(0, 8),
                ha='center', fontweight='bold', fontsize=9, color=TULEVA_NAVY,
            )

        ax.set_ylabel('Sissemaksed (M EUR)')
        ax.set_ylim(bottom=0)
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f'{v:,.0f}'))
        ax.set_title('Sissemaksed', fontweight='bold', color=TULEVA_NAVY)
        ax.set_xticks(x)
        ax.set_xticklabels(months, rotation=45, ha='right', fontsize=8)
        ax.grid(axis='y', alpha=0.3, zorder=0)
        ax.legend(loc='upper left', fontsize=8)

    if not needs_break:
        plt.tight_layout()
    else:
        fig.subplots_adjust(left=0.1, right=0.97, bottom=0.18, top=0.92)
    output_file = output_dir / 'contributions.png'
    plt.savefig(output_file)
    plt.close()
    print(f"  Saved: {output_file}")


def generate_iii_contributors_chart(contributors_data, report_year, report_month, output_dir: Path):
    """Generate III pillar contributors bar chart with YoY line (card 1532).

    Left Y-axis: contributor count bars.
    Right Y-axis: YoY growth % line.
    """
    print("Generating III pillar contributors chart...")

    months = []
    counts = []
    yoy = []

    for row in contributors_data:
        months.append(_parse_month_label(row['kuu: Month']))
        counts.append(row['III samba sissemakse tegijate arv'])
        yoy.append(row['YoY, %'] * 100)

    fig, ax_bar = plt.subplots(figsize=(10, 5.5))
    ax_line = ax_bar.twinx()

    x = np.arange(len(months))
    width = 0.7

    ax_bar.bar(x, counts, width, color=TULEVA_NAVY, zorder=2)

    # YoY growth line
    ax_line.plot(x, yoy, color='#FF4800', linewidth=2, marker='o',
                 markersize=3, label='YoY kasv %', zorder=4)

    # Annotate report month
    report_month_abbr = (
        f"{'Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec'.split()[report_month - 1]}"
        f"-{report_year % 100:02d}"
    )
    if report_month_abbr in months:
        idx = months.index(report_month_abbr)
        ax_bar.annotate(
            f'{counts[idx]:,}',
            (idx, counts[idx]),
            textcoords="offset points", xytext=(0, 8),
            ha='center', fontweight='bold', fontsize=9, color=TULEVA_NAVY,
        )

    # Axes formatting
    ax_bar.set_ylabel('Sissemakse tegijate arv')
    ax_bar.set_ylim(bottom=0)
    ax_bar.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f'{v:,.0f}'))

    ax_line.set_ylabel('YoY kasv (%)')
    ax_line.set_ylim(bottom=0)
    ax_line.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f'{v:.0f}%'))
    ax_line.tick_params(axis='y', length=0, pad=8)

    ax_bar.set_title('III samba sissemakse tegijad', fontweight='bold', color=TULEVA_NAVY)
    ax_bar.set_xticks(x)
    ax_bar.set_xticklabels(months, rotation=45, ha='right', fontsize=8)
    ax_bar.grid(axis='y', alpha=0.3, zorder=0)

    # Combined legend
    from matplotlib.patches import Patch
    from matplotlib.lines import Line2D
    legend_elements = [
        Patch(facecolor=TULEVA_NAVY, label='Sissemakse tegijad'),
        Line2D([0], [0], color='#FF4800', linewidth=2, label='YoY kasv %'),
    ]
    ax_bar.legend(handles=legend_elements, loc='upper left', fontsize=8)

    plt.tight_layout()
    output_file = output_dir / 'iii_contributors.png'
    plt.savefig(output_file)
    plt.close()
    print(f"  Saved: {output_file}")


def generate_switching_volume_chart(switching_data, report_year, report_month, output_dir: Path):
    """Generate II pillar switching volume bar chart with YoY line (card 1508).

    Left Y-axis: switching volume bars (M EUR).
    Right Y-axis: YoY growth % line.
    """
    print("Generating switching volume chart...")

    months = []
    volumes = []
    yoy = []

    for row in switching_data:
        months.append(_parse_month_label(row['kuu: Month']))
        volumes.append(row['vahetajate ületoodud varade maht, M EUR'] / 1_000_000)
        yoy.append(row['YoY, %'] * 100)

    fig, ax_bar = plt.subplots(figsize=(10, 5.5))
    ax_line = ax_bar.twinx()

    x = np.arange(len(months))
    width = 0.7

    ax_bar.bar(x, volumes, width, color=TULEVA_NAVY, zorder=2)

    # YoY growth line
    ax_line.plot(x, yoy, color='#FF4800', linewidth=2, marker='o',
                 markersize=3, label='YoY kasv %', zorder=4)
    ax_line.axhline(y=0, color='gray', linewidth=0.5, zorder=1)

    # Annotate report month
    report_month_abbr = (
        f"{'Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec'.split()[report_month - 1]}"
        f"-{report_year % 100:02d}"
    )
    if report_month_abbr in months:
        idx = months.index(report_month_abbr)
        ax_bar.annotate(
            f'{volumes[idx]:,.1f} M',
            (idx, volumes[idx]),
            textcoords="offset points", xytext=(0, 8),
            ha='center', fontweight='bold', fontsize=9, color=TULEVA_NAVY,
        )

    # Axes formatting
    ax_bar.set_ylabel('Ületoodud vara (M EUR)')
    ax_bar.set_ylim(bottom=0)
    ax_bar.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f'{v:,.1f}'))

    ax_line.set_ylabel('YoY kasv (%)')
    ax_line.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f'{v:.0f}%'))
    ax_line.tick_params(axis='y', length=0, pad=8)

    ax_bar.set_title('II samba vahetuste maht', fontweight='bold', color=TULEVA_NAVY)
    ax_bar.set_xticks(x)
    ax_bar.set_xticklabels(months, rotation=45, ha='right', fontsize=8)
    ax_bar.grid(axis='y', alpha=0.3, zorder=0)

    # Combined legend
    from matplotlib.patches import Patch
    from matplotlib.lines import Line2D
    legend_elements = [
        Patch(facecolor=TULEVA_NAVY, label='Ületoodud vara'),
        Line2D([0], [0], color='#FF4800', linewidth=2, label='YoY kasv %'),
    ]
    ax_bar.legend(handles=legend_elements, loc='upper left', fontsize=8)

    plt.tight_layout()
    output_file = output_dir / 'switching_volume.png'
    plt.savefig(output_file)
    plt.close()
    print(f"  Saved: {output_file}")


def generate_switching_sources_chart(from_data, report_year, report_month, output_dir: Path):
    """Generate horizontal bar chart of switching sources (card 1456).

    Top 10 source funds by application count.
    """
    print("Generating switching sources chart...")

    # Take top 10
    top = from_data[:10]
    # Reverse so largest is at top
    names = [row['Fund - Security From → Name Estonian'] for row in reversed(top)]
    counts = [row['Distinct values of Code'] for row in reversed(top)]

    fig, ax = plt.subplots(figsize=(10, 5.5))

    y = np.arange(len(names))
    bars = ax.barh(y, counts, height=0.6, color=TULEVA_NAVY, zorder=2)

    # Value labels
    for bar, count in zip(bars, counts):
        ax.text(bar.get_width() + max(counts) * 0.01, bar.get_y() + bar.get_height() / 2,
                str(count), va='center', fontsize=9, color=TULEVA_NAVY, fontweight='bold')

    ax.set_yticks(y)
    ax.set_yticklabels(names, fontsize=8)
    ax.set_xlabel('Avalduste arv')
    ax.set_title('Millistest fondidest vahetatakse Tulevasse (top 10)',
                 fontweight='bold', color=TULEVA_NAVY)
    ax.grid(axis='x', alpha=0.3, zorder=0)

    # Add some padding on the right for labels
    ax.set_xlim(right=max(counts) * 1.12)

    plt.tight_layout()
    output_file = output_dir / 'switching_sources.png'
    plt.savefig(output_file)
    plt.close()
    print(f"  Saved: {output_file}")


def generate_leavers_chart(leavers_data, report_year, report_month, output_dir: Path):
    """Generate II pillar leavers bar chart with YoY line (card 1522).

    Left Y-axis: leavers asset volume bars (M EUR).
    Right Y-axis: YoY growth % line.
    """
    print("Generating II pillar leavers chart...")

    months = []
    volumes = []
    yoy = []

    for row in leavers_data:
        months.append(_parse_month_label(row['kuu: Month']))
        volumes.append(row['lahkujate varade maht, M EUR'] / 1_000_000)
        yoy.append(row['YoY, %'] * 100)

    fig, ax_bar = plt.subplots(figsize=(10, 5.5))
    ax_line = ax_bar.twinx()

    x = np.arange(len(months))
    width = 0.7

    ax_bar.bar(x, volumes, width, color=TULEVA_NAVY, zorder=2)

    # YoY growth line
    ax_line.plot(x, yoy, color='#FF4800', linewidth=2, marker='o',
                 markersize=3, label='YoY kasv %', zorder=4)
    ax_line.axhline(y=0, color='gray', linewidth=0.5, zorder=1)

    # Annotate report month
    report_month_abbr = (
        f"{'Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec'.split()[report_month - 1]}"
        f"-{report_year % 100:02d}"
    )
    if report_month_abbr in months:
        idx = months.index(report_month_abbr)
        ax_bar.annotate(
            f'{volumes[idx]:,.2f} M',
            (idx, volumes[idx]),
            textcoords="offset points", xytext=(0, 8),
            ha='center', fontweight='bold', fontsize=9, color=TULEVA_NAVY,
        )

    # Axes formatting
    ax_bar.set_ylabel('Lahkujate vara (M EUR)')
    ax_bar.set_ylim(bottom=0)
    ax_bar.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f'{v:,.1f}'))

    ax_line.set_ylabel('YoY kasv (%)')
    ax_line.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f'{v:.0f}%'))
    ax_line.tick_params(axis='y', length=0, pad=8)

    ax_bar.set_title('II samba lahkujate vara', fontweight='bold', color=TULEVA_NAVY)
    ax_bar.set_xticks(x)
    ax_bar.set_xticklabels(months, rotation=45, ha='right', fontsize=8)
    ax_bar.grid(axis='y', alpha=0.3, zorder=0)

    # Combined legend
    from matplotlib.patches import Patch
    from matplotlib.lines import Line2D
    legend_elements = [
        Patch(facecolor=TULEVA_NAVY, label='Lahkujate vara'),
        Line2D([0], [0], color='#FF4800', linewidth=2, label='YoY kasv %'),
    ]
    ax_bar.legend(handles=legend_elements, loc='upper left', fontsize=8)

    plt.tight_layout()
    output_file = output_dir / 'leavers.png'
    plt.savefig(output_file)
    plt.close()
    print(f"  Saved: {output_file}")


def generate_drawdowns_chart(ii_data, iii_data, report_year, report_month, output_dir: Path):
    """Generate stacked drawdowns bar chart (cards 1523 + 1524).

    Stacked columns: II pillar exiters + III pillar withdrawals in M EUR.
    """
    print("Generating drawdowns chart...")

    # Build lookup for III pillar by month
    iii_by_month = {}
    for row in iii_data:
        iii_by_month[row['kuu: Month']] = row

    months = []
    ii_vals = []
    iii_vals = []

    for row in ii_data:
        date_str = row['kuu: Month']
        months.append(_parse_month_label(date_str))
        ii_vals.append(row['väljujate varade maht, M EUR'] / 1_000_000)
        iii_row = iii_by_month.get(date_str, {})
        iii_vals.append(iii_row.get('III sambast väljavõetud varade maht, M EUR', 0) / 1_000_000)

    fig, ax = plt.subplots(figsize=(10, 5.5))

    x = np.arange(len(months))
    width = 0.7

    ax.bar(x, ii_vals, width, label='II sammas',
           color=TULEVA_NAVY, zorder=2)
    ax.bar(x, iii_vals, width, bottom=ii_vals,
           label='III sammas', color=TULEVA_MID_BLUE, zorder=2)

    # Annotate report month total
    report_month_abbr = (
        f"{'Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec'.split()[report_month - 1]}"
        f"-{report_year % 100:02d}"
    )
    if report_month_abbr in months:
        idx = months.index(report_month_abbr)
        total = ii_vals[idx] + iii_vals[idx]
        ax.annotate(
            f'{total:,.2f} M',
            (idx, total),
            textcoords="offset points", xytext=(0, 8),
            ha='center', fontweight='bold', fontsize=9, color=TULEVA_NAVY,
        )

    ax.set_ylabel('Väljavõetud vara (M EUR)')
    ax.set_ylim(bottom=0)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f'{v:,.1f}'))
    ax.set_title('Pensionifondidest väljavõetud vara', fontweight='bold', color=TULEVA_NAVY)
    ax.set_xticks(x)
    ax.set_xticklabels(months, rotation=45, ha='right', fontsize=8)
    ax.grid(axis='y', alpha=0.3, zorder=0)
    ax.legend(loc='upper left', fontsize=8)

    plt.tight_layout()
    output_file = output_dir / 'drawdowns.png'
    plt.savefig(output_file)
    plt.close()
    print(f"  Saved: {output_file}")


def generate_unit_price_chart(price_data, output_dir: Path):
    """Generate rebased line chart comparing Tuleva, EPI, MSCI ACWI, and CPI (card 2245).

    All series are rebased to 1.0 at the earliest common date.
    """
    from datetime import datetime

    print("Generating unit price comparison chart...")

    SERIES_CONFIG = {
        'EE3600109435': {'label': 'Tuleva', 'color': TULEVA_BLUE, 'linewidth': 2.5, 'zorder': 5},
        'EPI':          {'label': 'EPI', 'color': '#51c26c', 'linewidth': 1.5, 'zorder': 4},
        'MSCI_ACWI':    {'label': 'MSCI ACWI', 'color': '#FF4800', 'linewidth': 1.5, 'zorder': 3},
        'CPI':          {'label': 'Inflatsioon', 'color': '#303030', 'linewidth': 1.5, 'zorder': 2},
    }

    # Group data by key, sorted by date
    series = {}
    for row in price_data:
        key = row['Key']
        if key not in SERIES_CONFIG:
            continue
        if key not in series:
            series[key] = []
        series[key].append((row['Date'], row['Value']))

    for key in series:
        series[key].sort(key=lambda x: x[0])

    # Rebase each series: first value = 1.0
    fig, ax = plt.subplots(figsize=(10, 5.5))

    for key, cfg in SERIES_CONFIG.items():
        if key not in series or not series[key]:
            continue
        dates = [datetime.strptime(d, '%Y-%m-%d') for d, _ in series[key]]
        values = [v for _, v in series[key]]
        base = values[0]
        rebased = [v / base for v in values]
        ax.plot(dates, rebased, label=cfg['label'], color=cfg['color'],
                linewidth=cfg['linewidth'], zorder=cfg['zorder'])

    ax.set_ylabel('Väärtus (baas = 1.0)')
    ax.set_title('Osakuhinna muutus võrrelduna maailmaturu, EPI ja inflatsiooniga',
                 fontweight='bold', color=TULEVA_NAVY, fontsize=11)
    ax.axhline(y=1.0, color='gray', linewidth=0.5, linestyle='--', zorder=1)
    ax.grid(axis='both', alpha=0.3, zorder=0)
    ax.legend(loc='upper left', fontsize=9)

    import matplotlib.dates as mdates
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_minor_locator(mdates.MonthLocator(bymonth=[4, 7, 10]))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))

    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f'{v:.2f}'))

    plt.tight_layout()
    output_file = output_dir / 'unit_price.png'
    plt.savefig(output_file)
    plt.close()
    print(f"  Saved: {output_file}")


def generate_cumulative_returns_chart(price_data, output_dir: Path):
    """Generate grouped bar chart of cumulative returns over 1, 2, 3, 5 years (card 2245)."""
    from datetime import datetime, timedelta
    from dateutil.relativedelta import relativedelta

    print("Generating cumulative returns chart...")

    SERIES_CONFIG = {
        'EE3600109435': {'label': 'Tuleva', 'color': TULEVA_BLUE},
        'MSCI_ACWI':    {'label': 'MSCI ACWI', 'color': '#FF4800'},
        'EPI':          {'label': 'EPI', 'color': '#51c26c'},
        'CPI':          {'label': 'Inflatsioon', 'color': '#303030'},
    }
    PERIODS = [1, 2, 3, 5]

    # Group by key, sorted by date
    series = {}
    for row in price_data:
        key = row['Key']
        if key not in SERIES_CONFIG:
            continue
        if key not in series:
            series[key] = []
        series[key].append((datetime.strptime(row['Date'], '%Y-%m-%d'), row['Value']))

    for key in series:
        series[key].sort(key=lambda x: x[0])

    def find_nearest_value(data, target_date):
        """Find value at the date closest to target_date."""
        best = None
        best_dist = None
        for d, v in data:
            dist = abs((d - target_date).days)
            if best_dist is None or dist < best_dist:
                best_dist = dist
                best = v
        return best

    # Calculate cumulative returns
    returns = {}  # key -> [1y_return, 2y_return, ...]
    for key, data in series.items():
        end_date = data[-1][0]
        end_val = data[-1][1]
        returns[key] = []
        for years in PERIODS:
            start_date = end_date - relativedelta(years=years)
            start_val = find_nearest_value(data, start_date)
            if start_val and start_val > 0:
                returns[key].append((end_val / start_val - 1) * 100)
            else:
                returns[key].append(None)

    # Plot grouped bars
    fig, ax = plt.subplots(figsize=(10, 5.5))

    period_labels = [f'{y}a' for y in PERIODS]
    x = np.arange(len(PERIODS))
    n_series = len(SERIES_CONFIG)
    bar_width = 0.7 / n_series

    for i, (key, cfg) in enumerate(SERIES_CONFIG.items()):
        if key not in returns:
            continue
        vals = returns[key]
        offsets = x + (i - n_series / 2 + 0.5) * bar_width
        bars = ax.bar(offsets, vals, bar_width, label=cfg['label'],
                      color=cfg['color'], zorder=2)
        # Value labels on bars
        for bar, val in zip(bars, vals):
            if val is not None:
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                        f'{val:.0f}%', ha='center', va='bottom', fontsize=8,
                        fontweight='bold', color=cfg['color'])

    ax.set_ylabel('Kumulatiivne tootlus (%)')
    ax.set_title('Kumulatiivne tootlus perioodide lõikes', fontweight='bold', color=TULEVA_NAVY)
    ax.set_xticks(x)
    ax.set_xticklabels(period_labels, fontsize=10)
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda v, _: f'{v:.0f}%'))
    ax.grid(axis='y', alpha=0.3, zorder=0)
    ax.legend(loc='upper left', fontsize=9)

    plt.tight_layout()
    output_file = output_dir / 'cumulative_returns.png'
    plt.savefig(output_file)
    plt.close()
    print(f"  Saved: {output_file}")


def generate_waterfall_chart(growth_data, title, output_file: Path):
    """Generate a waterfall bar chart for growth sources."""
    print(f"Generating waterfall: {title}...")

    labels = [row['kasvuallikas'] for row in growth_data]
    values = [row['väärtus'] for row in growth_data]

    # Calculate running total for the waterfall
    cumulative = 0
    bottoms = []
    bar_colors = []
    for v in values:
        if v >= 0:
            bottoms.append(cumulative)
            bar_colors.append(POSITIVE_COLOR)
        else:
            bottoms.append(cumulative + v)
            bar_colors.append(NEGATIVE_COLOR)
        cumulative += v

    # Add total bar
    labels.append('KOKKU')
    values.append(cumulative)
    bottoms.append(0)
    bar_colors.append(TOTAL_COLOR)

    fig, ax = plt.subplots(figsize=(8, 5))

    x = np.arange(len(labels))
    bars = ax.bar(x, [abs(v) for v in values], bottom=bottoms, color=bar_colors,
                  width=0.6, edgecolor='white', linewidth=0.5)

    # Add value labels on bars
    for i, (bar, val) in enumerate(zip(bars, values)):
        y_pos = bottoms[i] + abs(val) / 2
        label = f'{val:+.1f}' if i < len(values) - 1 else f'{val:.1f}'
        ax.text(bar.get_x() + bar.get_width() / 2, y_pos,
                label, ha='center', va='center', fontsize=9,
                fontweight='bold', color='white')

    # Add connector lines between bars (not to total)
    for i in range(len(values) - 2):
        top = bottoms[i] + abs(values[i]) if values[i] >= 0 else bottoms[i]
        ax.plot([x[i] + 0.3, x[i+1] - 0.3],
                [bottoms[i] + (values[i] if values[i] >= 0 else 0)] * 2,
                color='gray', linewidth=0.5, linestyle='-')

    ax.set_title(title, fontweight='bold', color=TULEVA_NAVY)
    ax.set_ylabel('M EUR')
    ax.set_xticks(x)
    ax.set_xticklabels(labels, fontsize=9)
    ax.axhline(y=0, color='gray', linewidth=0.5)
    ax.grid(axis='y', alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_file)
    plt.close()
    print(f"  Saved: {output_file}")


def generate_monthly_charts(year: int, month: int) -> Path:
    """Generate all section-1 charts from the monthly YAML data file.

    Returns the chart output directory path.
    """
    setup_plot_style()

    report_dir = Path(__file__).parent
    data_file = report_dir / 'data' / f'{year}-{month:02d}.yaml'
    output_dir = report_dir / 'output' / str(year) / 'charts'
    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Loading data from: {data_file}")
    with open(data_file, 'r') as f:
        data = yaml.safe_load(f)

    cards = data.get('cards', {})

    # AUM line chart (card 334)
    aum_card = cards.get('AUM (koos ootel vahetuste ja väljumistega)', {})
    aum_data = aum_card.get('data', [])
    if aum_data:
        generate_aum_chart(aum_data, year, month, output_dir)

    # Savers stacked bar chart (card 1515)
    savers_card = cards.get('kogujate arv kuus', {})
    savers_data = savers_card.get('data', [])
    if savers_data:
        generate_savers_chart(savers_data, year, month, output_dir)

    # New savers by pillar (cards 1519 + 1520)
    ii_joiners = cards.get('II sambaga liitujate arv kuus', {}).get('data', [])
    iii_joiners = cards.get('III sambaga liitujate arv kuus', {}).get('data', [])
    if ii_joiners and iii_joiners:
        generate_new_savers_by_pillar_chart(ii_joiners, iii_joiners, year, month, output_dir)
    if ii_joiners:
        generate_new_ii_savers_by_source_chart(ii_joiners, year, month, output_dir)

    # Contributions stacked bar chart (cards 1513 + 1512)
    ii_contributions = cards.get('II samba sissemaksete summa kuus, M EUR', {}).get('data', [])
    iii_contributions = cards.get('III samba sissemaksete summa kuus, M EUR', {}).get('data', [])
    if ii_contributions and iii_contributions:
        generate_contributions_chart(ii_contributions, iii_contributions, year, month, output_dir)

    # III pillar contributors chart (card 1532)
    iii_contributors = cards.get('III samba sissemakse tegijate arv kuus', {}).get('data', [])
    if iii_contributors:
        generate_iii_contributors_chart(iii_contributors, year, month, output_dir)

    # Switching volume chart (card 1508)
    switching_vol = cards.get('II samba vahetajate ületoodava vara maht kuus, M EUR', {}).get('data', [])
    if switching_vol:
        generate_switching_volume_chart(switching_vol, year, month, output_dir)

    # Switching sources chart (card 1456)
    switching_from = cards.get('II samba vahetusavalduste arv lähtefondi järgi sel vahetusperioodil', {}).get('data', [])
    if switching_from:
        generate_switching_sources_chart(switching_from, year, month, output_dir)

    # II pillar leavers chart (card 1522)
    leavers = cards.get('II samba lahkujate varade maht kuus, M EUR', {}).get('data', [])
    if leavers:
        generate_leavers_chart(leavers, year, month, output_dir)

    # Drawdowns stacked chart (cards 1523 + 1524)
    ii_exiters = cards.get('II samba väljujate varade maht kuus, M EUR', {}).get('data', [])
    iii_withdrawals = cards.get('III sambast välja võetud varade maht kuus, M EUR', {}).get('data', [])
    if ii_exiters and iii_withdrawals:
        generate_drawdowns_chart(ii_exiters, iii_withdrawals, year, month, output_dir)

    # Unit price comparison chart (card 2245)
    unit_price = cards.get('Osakuhinna võrdlus', {}).get('data', [])
    if unit_price:
        generate_unit_price_chart(unit_price, output_dir)
        generate_cumulative_returns_chart(unit_price, output_dir)

    # Growth sources waterfall — month (card 389)
    growth_month = cards.get('Kasvuallikad eelmisel kuul (tegelik), M EUR', {}).get('data', [])
    month_name = ESTONIAN_MONTHS.get(month, str(month))
    if growth_month:
        generate_waterfall_chart(
            growth_month,
            f'Kasvuallikad — {month_name}',
            output_dir / 'growth_month.png',
        )

    # Growth sources waterfall — YTD (card 392)
    growth_ytd = cards.get('Kasvuallikad YTD (tegelik), M EUR', {}).get('data', [])
    if growth_ytd:
        generate_waterfall_chart(
            growth_ytd,
            'Kasvuallikad — aasta algusest (YTD)',
            output_dir / 'growth_ytd.png',
        )

    print(f"\nAll monthly charts saved to: {output_dir}")
    return output_dir


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python generate_monthly_charts.py <year> <month>")
        print("Example: python generate_monthly_charts.py 2025 1")
        sys.exit(1)

    year = int(sys.argv[1])
    month = int(sys.argv[2])
    generate_monthly_charts(year, month)
