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
