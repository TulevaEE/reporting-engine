"""Regenerate the three charts with Tuleva brand styling.

Output goes to charts/ next to this script.
"""

from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import pandas as pd

HERE = Path(__file__).parent
CHARTS = HERE / "charts"
CHARTS.mkdir(exist_ok=True)

# Tuleva brand colours (from knowledge/stiil/brand.md)
NAVY = "#002F63"            # Tuleva fund / primary emphasis
BRIGHT_BLUE = "#00AEEA"     # Tuleva accent / secondary
ACTION_BLUE = "#006CE6"     # Action / II sammas
NAVY_SECONDARY = "#5C738C"
TEXT_PRIMARY = "#293036"
TEXT_SECONDARY = "#6B7074"
BG = "#f3fafe"              # User-specified chart background

# Bank-specific colours (matching Tõnu's original design intent — each bank
# carries its own visual identity instead of one neutral grey).
BANK_COLORS = {
    "Swedbank": "#FF8200",  # Swedbank orange
    "SEB":      "#21A038",  # SEB green
    "LHV":      "#404040",  # LHV charcoal
    "Luminor":  "#8B2D4F",  # Luminor maroon
}

FIGSIZE = (14, 6.5)
DPI = 200


def setup_style():
    # Register user-installed Roboto + Merriweather if present.
    fonts_dir = Path.home() / "Library" / "Fonts"
    candidates = [
        "Roboto-Regular.ttf",
        "Roboto-Medium.ttf",
        "Roboto-Bold.ttf",
        "Merriweather-Variable.ttf",
        "Merriweather[opsz,wdth,wght].ttf",
    ]
    for ttf in candidates:
        path = fonts_dir / ttf
        if path.exists():
            try:
                fm.fontManager.addfont(str(path))
            except Exception:
                pass

    plt.rcParams["font.family"] = "sans-serif"
    plt.rcParams["font.sans-serif"] = [
        "Roboto",
        "Roboto Flex",
        "Helvetica Neue",
        "Arial",
        "DejaVu Sans",
    ]
    plt.rcParams["font.size"] = 14
    plt.rcParams["axes.titlesize"] = 18
    plt.rcParams["axes.labelsize"] = 15
    plt.rcParams["axes.spines.top"] = False
    plt.rcParams["axes.spines.right"] = False
    plt.rcParams["axes.edgecolor"] = TEXT_SECONDARY
    plt.rcParams["axes.labelcolor"] = TEXT_PRIMARY
    plt.rcParams["xtick.color"] = TEXT_PRIMARY
    plt.rcParams["ytick.color"] = TEXT_PRIMARY
    plt.rcParams["xtick.labelsize"] = 16
    plt.rcParams["ytick.labelsize"] = 14
    plt.rcParams["figure.facecolor"] = BG
    plt.rcParams["axes.facecolor"] = BG
    plt.rcParams["savefig.facecolor"] = BG
    plt.rcParams["savefig.dpi"] = DPI
    # Keep text as real <text> elements in SVG (browsers render natively;
    # avoids matplotlib's glyph-as-path quirks that broke our previous SVG).
    plt.rcParams["svg.fonttype"] = "none"


def save(fig, slug):
    for ext in ("png", "svg"):
        fig.savefig(CHARTS / f"{slug}.{ext}", facecolor=BG)
    plt.close(fig)


def chart_a():
    """II samba väljavahetamiste maht fondide kogumahust 2025. a."""
    funds = ["Tuleva", "Swedbank", "SEB", "LHV", "Luminor"]
    churn = [1.8, 5.8, 7.8, 16.6, 19.3]
    colors = [NAVY] + [BANK_COLORS[name] for name in funds[1:]]

    fig, ax = plt.subplots(figsize=FIGSIZE)
    bars = ax.bar(funds, churn, color=colors, width=0.6)

    # Value labels above each bar
    for bar, val in zip(bars, churn):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.5,
            f"{val:.1f}%".replace(".", ","),
            ha="center",
            va="bottom",
            fontsize=18,
            fontweight="bold",
            color=TEXT_PRIMARY,
        )

    ax.set_ylim(0, 23)
    ax.set_yticks([0, 5, 10, 15, 20])
    ax.set_yticklabels(["0%", "5%", "10%", "15%", "20%"])
    ax.grid(axis="y", color="#D3E8F5", linewidth=1)
    ax.set_axisbelow(True)
    ax.spines["bottom"].set_color(TEXT_SECONDARY)
    ax.spines["left"].set_visible(False)
    ax.tick_params(left=False)

    # Make x-axis labels bold for fund names
    for label in ax.get_xticklabels():
        label.set_fontweight("bold")
        label.set_fontsize(17)

    fig.subplots_adjust(top=0.96, bottom=0.10, left=0.06, right=0.97)
    save(fig, "chart-a-ii-samba-churn-2025")


def chart_b():
    """Pankade pensionifondide teenustasu jagunemine 2025."""
    sizes = [82, 18]
    labels = [
        "Müük, turundus,\nemaettevõtte vahendustasud\nja pankade kasum",
        "Fondi tegelik haldamine\n(investeerimistiim, IT,\ndepootasud)",
    ]
    amounts = ["42 mln €", "9 mln €"]
    colors = [NAVY, BRIGHT_BLUE]

    fig, ax = plt.subplots(figsize=FIGSIZE)

    wedges, _ = ax.pie(
        sizes,
        startangle=90,
        colors=colors,
        wedgeprops=dict(edgecolor=BG, linewidth=3),
        radius=1.0,
    )
    ax.set_aspect("equal")
    ax.set_xlim(-2.4, 2.4)
    ax.set_ylim(-1.5, 1.5)

    # Annotate wedges outside the pie with both percentage and EUR amount.
    for wedge, pct, label, amt in zip(wedges, sizes, labels, amounts):
        ang = (wedge.theta2 + wedge.theta1) / 2
        import math
        x = math.cos(math.radians(ang))
        y = math.sin(math.radians(ang))
        ha = "left" if x >= 0 else "right"
        ax.annotate(
            f"{pct}%\n{label}\n{amt}",
            xy=(x * 1.0, y * 1.0),
            xytext=(x * 1.55, y * 1.15),
            ha=ha,
            va="center",
            fontsize=16,
            fontweight="bold",
            color=TEXT_PRIMARY,
        )

    fig.subplots_adjust(top=0.96, bottom=0.04)
    save(fig, "chart-b-teenustasu-pie-2025")


def chart_c():
    """Indeksifondide osakaal II ja III sambas 2019–2025."""
    csv_path = HERE / "data" / "indeksifondide_osakaal_2020_2025.csv"
    df = pd.read_csv(csv_path)
    df = df[df["Year"] >= 2020].copy()  # post.md räägib 2020-2025 perioodist

    ii = df[df["pillar"] == "II"].sort_values("Year")
    iii = df[df["pillar"] == "III"].sort_values("Year")

    fig, ax = plt.subplots(figsize=FIGSIZE)

    ax.plot(
        ii["Year"], ii["indeks_%"],
        color=BRIGHT_BLUE, linewidth=4, marker="o", markersize=10,
        label="II sammas",
    )
    ax.plot(
        iii["Year"], iii["indeks_%"],
        color=NAVY, linewidth=4, marker="o", markersize=10,
        label="III sammas",
    )

    # Endpoint labels
    for row, color, anchor in [
        (ii.iloc[0], BRIGHT_BLUE, "bottom"),
        (ii.iloc[-1], BRIGHT_BLUE, "bottom"),
        (iii.iloc[0], NAVY, "bottom"),
        (iii.iloc[-1], NAVY, "bottom"),
    ]:
        ax.annotate(
            f"{row['indeks_%']:.0f}%",
            xy=(row["Year"], row["indeks_%"]),
            xytext=(0, 12),
            textcoords="offset points",
            ha="center",
            va=anchor,
            fontsize=16,
            fontweight="bold",
            color=color,
        )

    ax.set_ylim(0, 70)
    ax.set_yticks([0, 20, 40, 60])
    ax.set_yticklabels(["0%", "20%", "40%", "60%"])
    ax.set_ylabel("Osakaal kogu varast", fontsize=15, color=TEXT_PRIMARY, labelpad=10)
    ax.set_xticks(range(2020, 2026))
    ax.grid(axis="y", color="#D3E8F5", linewidth=1)
    ax.set_axisbelow(True)
    ax.spines["bottom"].set_color(TEXT_SECONDARY)
    ax.spines["left"].set_visible(False)
    ax.tick_params(left=False)

    for label in ax.get_xticklabels():
        label.set_fontsize(16)

    ax.legend(
        loc="upper left",
        frameon=False,
        fontsize=16,
        labelcolor=TEXT_PRIMARY,
    )

    fig.subplots_adjust(top=0.96, bottom=0.10, left=0.06, right=0.97)
    save(fig, "chart-c-indeksifondide-osakaal-2020-2025")


if __name__ == "__main__":
    setup_style()
    chart_a()
    chart_b()
    chart_c()
    print(f"Wrote charts to {CHARTS}/")
