import os
import json
import gspread
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from pathlib import Path

# Tuleva brand colors
TULEVA_BLUE = '#00AEEA'
TULEVA_NAVY = '#002F63'
TULEVA_MID_BLUE = '#0081EE'
COLORS = [TULEVA_BLUE, TULEVA_NAVY, TULEVA_MID_BLUE, '#51c26c', '#FF4800', '#FCE228', '#303030']
COMPETITOR_COLORS = {
    'Tuleva': TULEVA_BLUE,
    'LHV': '#FF4800',
    'Swedbank': '#FCE228',
    'SEB': '#51c26c',
    'Luminor': '#303030',
}

# Legacy aliases for backwards compatibility within this file
TULEVA_GREEN = TULEVA_BLUE
TULEVA_LIGHT_GREEN = TULEVA_MID_BLUE

# Chart data sheet
CHART_SHEET_ID = "1gtER8AHI7Nf9r-nFJKs3CXlhIKua1xvtwrLfzzxc2sQ"


def get_gspread_client():
    """Authenticate and return a gspread client."""
    service_account_info = json.loads(os.environ['GCP_SERVICE_ACCOUNT'])
    return gspread.service_account_from_dict(service_account_info)


def setup_plot_style():
    """Configure matplotlib for Tuleva branding."""
    plt.rcParams['font.family'] = 'sans-serif'
    plt.rcParams['font.sans-serif'] = ['Roboto', 'DejaVu Sans', 'Arial']
    plt.rcParams['font.size'] = 10
    plt.rcParams['axes.titlesize'] = 14
    plt.rcParams['axes.labelsize'] = 11
    plt.rcParams['axes.spines.top'] = False
    plt.rcParams['axes.spines.right'] = False
    plt.rcParams['figure.facecolor'] = 'white'
    plt.rcParams['axes.facecolor'] = 'white'
    plt.rcParams['savefig.facecolor'] = 'white'
    plt.rcParams['savefig.bbox'] = 'tight'
    plt.rcParams['savefig.dpi'] = 150


def generate_chart_3_determined_savers(sh, output_dir: Path):
    """Image 3: Determined savers breakdown."""
    print("Generating Chart 3: Determined savers...")

    ws = sh.worksheet("sihikindlate arv")
    data = ws.get_all_values()

    # Parse data (skip header)
    segments = []
    counts = []
    for row in data[1:]:
        if row and row[0] and row[1]:
            segments.append(row[0])
            counts.append(int(row[1].replace(',', '')))

    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.barh(segments, counts, color=COLORS[:len(segments)])
    ax.set_xlabel('Kogujate arv')
    ax.set_title('Tuleva kogujate jaotus', fontweight='bold', color=TULEVA_GREEN)

    # Add value labels
    for bar, count in zip(bars, counts):
        ax.text(bar.get_width() + 500, bar.get_y() + bar.get_height()/2,
                f'{count:,}', va='center', fontsize=9)

    plt.tight_layout()
    output_file = output_dir / 'chart_3_determined_savers.png'
    plt.savefig(output_file)
    plt.close()
    print(f"  Saved: {output_file}")


def generate_chart_4_contribution_increase(sh, output_dir: Path):
    """Image 4: II pillar contribution increase comparison."""
    print("Generating Chart 4: II pillar contribution increase...")

    ws = sh.worksheet("246")
    data = ws.get_all_values()

    # Aggregate by fund manager and rate
    manager_data = {}
    manager_totals = {}

    for row in data[1:]:
        if len(row) >= 4 and row[1] and row[2] and row[3]:
            manager = row[1]
            rate = row[2]
            try:
                quantity = int(row[3])
            except ValueError:
                continue

            if manager not in manager_data:
                manager_data[manager] = {'2': 0, '4': 0, '6': 0}
                manager_totals[manager] = 0

            if rate in ['2', '4', '6']:
                manager_data[manager][rate] += quantity
                if rate in ['4', '6']:  # Count those who increased
                    manager_totals[manager] += quantity

    # Get total savers from column F/G
    for row in data[1:10]:
        if len(row) >= 7 and row[5] and row[6]:
            try:
                manager = row[5]
                total = int(row[6].replace(',', ''))
                if manager in manager_totals:
                    manager_totals[manager] = (manager_totals.get(manager, 0), total)
            except (ValueError, IndexError):
                pass

    # Calculate percentages for Tuleva vs others
    managers = ['Tuleva', 'LHV', 'Swedbank', 'SEB', 'Luminor']
    increased_pct = []

    for m in managers:
        if m in manager_data:
            increased = manager_data[m]['4'] + manager_data[m]['6']
            total = sum(manager_data[m].values())
            if total > 0:
                increased_pct.append(increased / total * 100)
            else:
                increased_pct.append(0)
        else:
            increased_pct.append(0)

    fig, ax = plt.subplots(figsize=(8, 5))
    colors = [COMPETITOR_COLORS.get(m, '#888888') for m in managers]
    bars = ax.bar(managers, increased_pct, color=colors)

    ax.set_ylabel('Makset tõstnud kogujate %')
    ax.set_title('II samba makse tõstjate osakaal', fontweight='bold', color=TULEVA_GREEN)
    ax.set_ylim(0, 100)

    # Add value labels
    for bar, pct in zip(bars, increased_pct):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 2,
                f'{pct:.0f}%', ha='center', fontsize=10, fontweight='bold')

    plt.tight_layout()
    output_file = output_dir / 'chart_4_contribution_increase.png'
    plt.savefig(output_file)
    plt.close()
    print(f"  Saved: {output_file}")


def generate_chart_5_contributions(sh, output_dir: Path):
    """Image 5: Contributions to II and III pillar."""
    print("Generating Chart 5: Contributions...")

    ws = sh.worksheet("sissemaksed")
    data = ws.get_all_values()

    years = []
    pillar_2 = []
    pillar_3 = []

    for row in data[1:]:
        if row and row[0] and row[0].isdigit() and len(row) >= 3:
            years.append(row[0])
            pillar_2.append(float(row[1]) / 1_000_000)  # Convert to millions
            pillar_3.append(float(row[2]) / 1_000_000)

    fig, ax = plt.subplots(figsize=(8, 5))

    x = range(len(years))
    width = 0.35

    bars1 = ax.bar([i - width/2 for i in x], pillar_2, width, label='II sammas', color=TULEVA_GREEN)
    bars2 = ax.bar([i + width/2 for i in x], pillar_3, width, label='III sammas', color=TULEVA_LIGHT_GREEN)

    ax.set_xlabel('Aasta')
    ax.set_ylabel('Sissemaksed (M EUR)')
    ax.set_title('Sissemaksed Tuleva pensionifondidesse', fontweight='bold', color=TULEVA_GREEN)
    ax.set_xticks(x)
    ax.set_xticklabels(years)
    ax.legend()

    # Add value labels
    for bar in bars1:
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                f'{bar.get_height():.0f}M', ha='center', fontsize=9)
    for bar in bars2:
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1,
                f'{bar.get_height():.0f}M', ha='center', fontsize=9)

    plt.tight_layout()
    output_file = output_dir / 'chart_5_contributions.png'
    plt.savefig(output_file)
    plt.close()
    print(f"  Saved: {output_file}")


def generate_chart_6_returns(sh, output_dir: Path):
    """Image 6: Fund returns comparison."""
    print("Generating Chart 6: Returns...")

    ws = sh.worksheet("tootlus")
    data = ws.get_all_values()

    # Find the data rows (starting after headers)
    funds = []
    returns_2y = []
    returns_3y = []
    returns_5y = []

    for row in data:
        if len(row) >= 6 and row[0] and '31.12' in row[0]:
            name = row[2].strip() if row[2] else row[1]
            if name:
                funds.append(name[:30])  # Truncate long names
                returns_2y.append(float(row[3].replace('%', '').replace(',', '.')))
                returns_3y.append(float(row[4].replace('%', '').replace(',', '.')))
                returns_5y.append(float(row[5].replace('%', '').replace(',', '.')))

    if not funds:
        print("  Warning: No returns data found")
        return

    fig, ax = plt.subplots(figsize=(10, 5))

    x = range(len(funds))
    width = 0.25

    bars1 = ax.bar([i - width for i in x], returns_2y, width, label='2 aastat', color=COLORS[0])
    bars2 = ax.bar(x, returns_3y, width, label='3 aastat', color=COLORS[2])
    bars3 = ax.bar([i + width for i in x], returns_5y, width, label='5 aastat', color=COLORS[4])

    ax.set_ylabel('Tootlus (%)')
    ax.set_title('Pensionifondide tootlus', fontweight='bold', color=TULEVA_GREEN)
    ax.set_xticks(x)
    ax.set_xticklabels(funds, rotation=15, ha='right', fontsize=8)
    ax.legend()
    ax.axhline(y=0, color='gray', linestyle='-', linewidth=0.5)

    plt.tight_layout()
    output_file = output_dir / 'chart_6_returns.png'
    plt.savefig(output_file)
    plt.close()
    print(f"  Saved: {output_file}")


def generate_chart_7_aum_growth(sh, output_dir: Path):
    """Image 7: Fund AUM growth and sources."""
    print("Generating Chart 7: AUM growth...")

    # Get growth sources
    ws = sh.worksheet("kasvuallikad")
    data = ws.get_all_values()

    sources = []
    values = []

    for row in data[1:]:
        if row and row[0] and row[1]:
            sources.append(row[0])
            try:
                values.append(float(row[1]))
            except ValueError:
                continue

    fig, ax = plt.subplots(figsize=(8, 5))

    colors = [TULEVA_GREEN if v > 0 else '#c62828' for v in values]
    bars = ax.barh(sources, values, color=colors)

    ax.set_xlabel('Väärtus (M EUR)')
    ax.set_title('Tuleva fondide kasvu allikad', fontweight='bold', color=TULEVA_GREEN)
    ax.axvline(x=0, color='gray', linestyle='-', linewidth=0.5)

    # Add value labels
    for bar, val in zip(bars, values):
        offset = 5 if val > 0 else -15
        ax.text(bar.get_width() + offset, bar.get_y() + bar.get_height()/2,
                f'{val:+.0f}M', va='center', fontsize=9)

    plt.tight_layout()
    output_file = output_dir / 'chart_7_aum_growth.png'
    plt.savefig(output_file)
    plt.close()
    print(f"  Saved: {output_file}")


def generate_chart_8_outflows(sh, output_dir: Path):
    """Image 8: Fund switching outflows comparison."""
    print("Generating Chart 8: Outflows...")

    ws = sh.worksheet("mujale vahetamised")
    data = ws.get_all_values()

    # Get average outflow for each manager (latest periods)
    managers = []
    avg_outflows = []

    for row in data[1:]:
        if row and row[0] and row[0] in COMPETITOR_COLORS:
            manager = row[0]
            managers.append(manager)
            # Get the latest 3 periods and average
            outflows = []
            for val in row[4:7]:  # 2024 VP I, II, III
                if val:
                    try:
                        outflows.append(abs(float(val.replace('%', '').replace(',', '.'))))
                    except ValueError:
                        pass
            avg_outflows.append(sum(outflows) / len(outflows) if outflows else 0)

    fig, ax = plt.subplots(figsize=(8, 5))

    colors = [COMPETITOR_COLORS.get(m, '#888888') for m in managers]
    bars = ax.bar(managers, avg_outflows, color=colors)

    ax.set_ylabel('Vahetustehingutega lahkunud vara (%)')
    ax.set_title('Fondidest lahkumine vahetusperioodil', fontweight='bold', color=TULEVA_GREEN)

    # Add value labels
    for bar, val in zip(bars, avg_outflows):
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1,
                f'{val:.1f}%', ha='center', fontsize=10, fontweight='bold')

    plt.tight_layout()
    output_file = output_dir / 'chart_8_outflows.png'
    plt.savefig(output_file)
    plt.close()
    print(f"  Saved: {output_file}")


def generate_chart_9_market_share(sh, output_dir: Path):
    """Image 9: Market share over time."""
    print("Generating Chart 9: Market share...")

    ws = sh.worksheet("fondide AUM-id")
    data = ws.get_all_values()

    # Get headers (dates)
    headers = data[0][2:6]  # Dates columns

    # Aggregate by fund manager
    manager_aum = {m: [0, 0, 0, 0] for m in ['LHV', 'Swedbank', 'SEB', 'Luminor', 'Tuleva']}

    for row in data[1:]:
        if len(row) >= 6 and row[1]:
            manager = row[1]
            if manager in manager_aum:
                for i, val in enumerate(row[2:6]):
                    try:
                        manager_aum[manager][i] += float(val.replace(',', ''))
                    except (ValueError, IndexError):
                        pass

    # Calculate market share
    years = ['2022', '2023', '2024', '2025']
    tuleva_share = []

    for i in range(4):
        total = sum(manager_aum[m][i] for m in manager_aum)
        if total > 0:
            tuleva_share.append(manager_aum['Tuleva'][i] / total * 100)
        else:
            tuleva_share.append(0)

    fig, ax = plt.subplots(figsize=(8, 5))

    ax.plot(years, tuleva_share, marker='o', linewidth=2, markersize=8, color=TULEVA_GREEN)
    ax.fill_between(years, tuleva_share, alpha=0.3, color=TULEVA_GREEN)

    ax.set_xlabel('Aasta')
    ax.set_ylabel('Turuosa (%)')
    ax.set_title('Tuleva turuosa Eesti pensionifondide seas', fontweight='bold', color=TULEVA_GREEN)

    # Add value labels
    for i, (year, share) in enumerate(zip(years, tuleva_share)):
        ax.annotate(f'{share:.1f}%', (year, share), textcoords="offset points",
                   xytext=(0, 10), ha='center', fontweight='bold')

    plt.tight_layout()
    output_file = output_dir / 'chart_9_market_share.png'
    plt.savefig(output_file)
    plt.close()
    print(f"  Saved: {output_file}")


def generate_all_charts(year: int = 2025):
    """Generate all data-based charts."""
    print(f"Generating charts for {year} report...\n")

    setup_plot_style()

    # Output directory
    base_dir = Path(__file__).parent.parent.parent
    output_dir = base_dir / 'reports' / 'annual' / str(year) / 'charts'
    output_dir.mkdir(parents=True, exist_ok=True)

    # Connect to Google Sheet
    gc = get_gspread_client()
    sh = gc.open_by_key(CHART_SHEET_ID)
    print(f"Connected to: {sh.title}\n")

    # Generate each chart
    generate_chart_3_determined_savers(sh, output_dir)
    generate_chart_4_contribution_increase(sh, output_dir)
    generate_chart_5_contributions(sh, output_dir)
    generate_chart_6_returns(sh, output_dir)
    generate_chart_7_aum_growth(sh, output_dir)
    generate_chart_8_outflows(sh, output_dir)
    generate_chart_9_market_share(sh, output_dir)

    print(f"\nAll charts saved to: {output_dir}")


if __name__ == "__main__":
    import sys
    year = int(sys.argv[1]) if len(sys.argv) > 1 else 2025
    generate_all_charts(year)
