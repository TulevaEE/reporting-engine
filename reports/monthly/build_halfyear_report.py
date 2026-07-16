"""
Build a half-year (poolaasta) results report for the Tuleva blog.

Compares 1H (Jan–Jun) of the report year against the same period a year earlier,
reusing the consolidated KPI card 2578 (a full monthly time series stored in each
month's ``data/YYYY-MM.yaml``) plus a few survivor cards. Because 2578 spans back
to 2017, both half-years come from a single June data file.

Usage:
    python build_halfyear_report.py 2026 md      # generate MD (with comment placeholders)
    python build_halfyear_report.py 2026 html    # convert the (edited) MD to HTML

Workflow mirrors the monthly report: build MD, hand-edit the narrative comments,
then build HTML.
"""
import os
import sys
import base64
import yaml
import markdown
from pathlib import Path

import kpi_2578 as k
import halfyear_charts as charts
import saver_determination as sd

REPORT_MONTH = 6  # 1H = Jan..June


def pct(cur, prev):
    """Percent change, or None when prev is missing/zero."""
    if not prev:
        return None
    return (cur - prev) / prev


def point_in_time(idx, col, year):
    """June-end value of a stock metric for the given year."""
    row = idx.get((year, REPORT_MONTH))
    return row.get(col) if row else None


def compute_metrics(data, year):
    """Build the full 1H(year) vs 1H(year-1) metric structure from one data file."""
    s = data.get('kpi_2578', {}).get('data', []) or []
    cards = data.get('cards', {})
    idx = k.index_series(s)
    prev = year - 1

    def h1(col, yr):
        return k.ytd_sum(s, yr, REPORT_MONTH, col)

    def pit(col, yr):
        return point_in_time(idx, col, yr)

    m = {}

    # --- AUM (June-end stock, in M EUR) ---
    # Total = published headline from the AUM card (incl. pending switches/exits,
    # "koos ootel vahetuste ja väljumistega") so it matches the monthly report.
    # II/III split = settled Current Aum from 2578 (the AUM card has no split);
    # the ~0.5% gap vs the total is the pending pool not yet allocated to a pillar.
    # Point-in-time metrics are shown at three dates: prior June, prior year-end
    # (31.12), and current June -> triple (cur, year_end, prev).
    def pit_ye(col):
        r = idx.get((prev, 12))
        return r.get(col) if r else None
    aum_card = {r['month']: r['kuu lõpu AUM (M EUR)']
                for r in cards.get('AUM (koos ootel vahetuste ja väljumistega)', {}).get('data', []) or []}
    m['aum'] = {
        'total': (aum_card.get(k.month_label(year, REPORT_MONTH)),
                  aum_card.get(k.month_label(prev, 12)),
                  aum_card.get(k.month_label(prev, REPORT_MONTH))),
        'ii': (pit('Current Aum Second Pillar', year) / 1e6, pit_ye('Current Aum Second Pillar') / 1e6,
               pit('Current Aum Second Pillar', prev) / 1e6),
        'iii': (pit('Current Aum Third Pillar', year) / 1e6, pit_ye('Current Aum Third Pillar') / 1e6,
                pit('Current Aum Third Pillar', prev) / 1e6),
    }

    # --- Growth sources (1H waterfall) ---
    m['growth'] = {
        'sissemaksed': (h1('Second Pillar Contributions Eur', year) + h1('Third Pillar Contributions Eur', year),
                        h1('Second Pillar Contributions Eur', prev) + h1('Third Pillar Contributions Eur', prev)),
        'yletoodav': (h1('New Monthly Mandates Eur', year), h1('New Monthly Mandates Eur', prev)),
        'lahkujad': (-h1('New Monthly Leavers Eur', year), -h1('New Monthly Leavers Eur', prev)),
        'valjavoetud': (-(h1('New Monthly Exiters Eur', year) + h1('New Monthly Withdrawals Third Pillar Eur', year)),
                        -(h1('New Monthly Exiters Eur', prev) + h1('New Monthly Withdrawals Third Pillar Eur', prev))),
        'turg': (h1('Aum Growth Due To Market', year), h1('Aum Growth Due To Market', prev)),
    }

    # --- Savers (June-end active investors) ---
    both_cur = pit('Total Active Investors Both Pillars', year)
    both_ye = pit_ye('Total Active Investors Both Pillars')
    both_prev = pit('Total Active Investors Both Pillars', prev)
    m['savers'] = {
        'total': (pit('Total Active Investors', year), pit_ye('Total Active Investors'),
                  pit('Total Active Investors', prev)),
        'ii_only': (pit('Active Investors Second Pillar', year) - both_cur,
                    pit_ye('Active Investors Second Pillar') - both_ye,
                    pit('Active Investors Second Pillar', prev) - both_prev),
        'iii_only': (pit('Active Investors Third Pillar', year) - both_cur,
                     pit_ye('Active Investors Third Pillar') - both_ye,
                     pit('Active Investors Third Pillar', prev) - both_prev),
        'both': (both_cur, both_ye, both_prev),
    }

    # --- New savers (1H, distinct — from survivor YTD cards) ---
    def ytd_card(name, value_key):
        out = {}
        for r in cards.get(name, {}).get('data', []) or []:
            yr = int(r['reporting_year'][:4])
            out[yr] = r[value_key]
        return out
    ns = ytd_card('uute kogujate arv YTD', 'uute kogujate arv')
    ns_ii = ytd_card('uute II samba kogujate arv YTD', 'uute II samba kogujate arv')
    ns_iii = ytd_card('uute III samba kogujate arv YTD', 'uute III samba kogujate arv')
    m['new_savers'] = {
        'total': (ns.get(year), ns.get(prev)),
        'ii': (ns_ii.get(year), ns_ii.get(prev)),
        'iii': (ns_iii.get(year), ns_iii.get(prev)),
    }

    # --- Contributions (1H) ---
    m['contributions'] = {
        'ii': (h1('Second Pillar Contributions Eur', year), h1('Second Pillar Contributions Eur', prev)),
        'iii': (h1('Third Pillar Contributions Eur', year), h1('Third Pillar Contributions Eur', prev)),
    }
    m['contributions']['total'] = (
        m['contributions']['ii'][0] + m['contributions']['iii'][0],
        m['contributions']['ii'][1] + m['contributions']['iii'][1],
    )

    # --- TKF (1H, from card 2305) ---
    tkf = cards.get('Täiendavasse Kogumisfondi tehtud maksed', {}).get('data', []) or []
    def tkf_h1(yr):
        return sum(r.get('Sum of Amount', 0) for r in tkf
                   if r.get('Created At: Month', '').startswith(str(yr))
                   and r.get('Created At: Month', '')[5:7] <= '06')
    m['tkf'] = {'amount': (tkf_h1(year), tkf_h1(prev))}

    # --- III pillar recurring-payment share (June snapshot) ---
    row = idx.get((year, REPORT_MONTH))
    prow = idx.get((prev, REPORT_MONTH))
    if row and row.get('Third Pillar Contributors'):
        m['iii_recurring'] = (
            row['Active Investors Recurring Payment'] / row['Third Pillar Contributors'],
            (prow['Active Investors Recurring Payment'] / prow['Third Pillar Contributors'])
            if prow and prow.get('Third Pillar Contributors') else None,
        )

    # --- Switching (1H) --- counts are sums of monthly figures (≈ people)
    m['switching'] = {
        'count': (h1('New Monthly Mandates', year), h1('New Monthly Mandates', prev)),
        'aum': (h1('New Monthly Mandates Eur', year), h1('New Monthly Mandates Eur', prev)),
        'out_ii': (h1('New Monthly Leavers', year), h1('New Monthly Leavers', prev)),
        'out_iii': (h1('New Monthly Leavers Third Pillar', year),
                    h1('New Monthly Leavers Third Pillar', prev)),
    }

    # --- Outflows (1H) --- EUR volumes + people counts (sum of monthly figures)
    m['outflows'] = {
        'leavers': (h1('New Monthly Leavers Eur', year), h1('New Monthly Leavers Eur', prev)),
        'exiters': (h1('New Monthly Exiters Eur', year), h1('New Monthly Exiters Eur', prev)),
        'iii_withdrawals': (h1('New Monthly Withdrawals Third Pillar Eur', year),
                            h1('New Monthly Withdrawals Third Pillar Eur', prev)),
        'exiters_ppl': (h1('New Monthly Exiters', year), h1('New Monthly Exiters', prev)),
        'iii_withdrawals_ppl': (h1('New Monthly Withdrawals Third Pillar', year),
                                h1('New Monthly Withdrawals Third Pillar', prev)),
    }

    return m


# ---------- Markdown rendering helpers ----------

def fmt_eur_m(v):
    return f"{v / 1_000_000:.1f} M EUR"


def fmt_m(v):
    """Value already in M EUR -> whole M EUR (AUM headline convention)."""
    return f"{v:,.0f} M EUR".replace(",", " ")


def fmt_int(v):
    return f"{v:,.0f}".replace(",", " ")


def fmt_pct(p):
    if p is None:
        return "–"
    return f"{p:+.1%}".replace("-", "−")


def row3(label, cur, prev, fmt):
    """Flow row: label | 1H cur | 1H prev | YoY change."""
    c = fmt(cur) if cur is not None else "–"
    p = fmt(prev) if prev is not None else "–"
    return f"| {label} | {c} | {p} | *{fmt_pct(pct(cur, prev))}* |"


def row_pit(label, cur, ye, prev, fmt):
    """Stock row shown at 3 dates: prior June | prior year-end | current June,
    plus change since year-end (poolaastaga) and YoY (aastaga)."""
    def f(v):
        return fmt(v) if v is not None else "–"
    return (f"| {label} | {f(prev)} | {f(ye)} | {f(cur)} | "
            f"*{fmt_pct(pct(cur, ye))}* | *{fmt_pct(pct(cur, prev))}* |")


def build_md(year: int) -> Path:
    report_dir = Path(__file__).parent
    data_file = report_dir / 'data' / f'{year}-{REPORT_MONTH:02d}.yaml'
    if not data_file.exists():
        print(f"ERROR: data file not found: {data_file}")
        print(f"HINT: fetch {year}-06 first.")
        return None
    data = yaml.safe_load(data_file.read_text())
    m = compute_metrics(data, year)
    prev = year - 1

    # Saver determination (per-saver card 2324): segment active savers, anchor the
    # base to the official active-investor count (card 2578) so it ties to table 2.
    saver_df = sd.load_card_2324()
    official_total = m['savers']['total'][0]
    det = (sd.compute_determination(saver_df, active_total=official_total)
           if saver_df is not None else None)

    print("Generating charts...")
    chart_dir = report_dir / 'output' / str(year) / 'charts_h1'
    charts.generate_all(m, year, chart_dir)
    if det:
        sd.generate_determination_chart(det, chart_dir / 'determination.png')

    H = f"1. poolaasta {year}"
    Hp = f"1. poolaasta {prev}"
    L = []
    L.append(f"# Tuleva {year}. aasta esimese poolaasta tulemused\n")
    L.append(f"*Võrdlus: {H} vs {Hp}. Andmed seisuga 30.06.{year}.*\n")
    L.append("---\n")

    # 1. AUM
    L.append("## Varade maht ja kasv\n")
    L.append("<!-- comment:aum -->\n\n<!-- /comment:aum -->\n")
    L.append(f"![Varade maht](charts_h1/aum.png)\n")
    L.append(f"| Vara | 30.06.{prev} | 31.12.{prev} | 30.06.{year} | Poolaastaga | Aastaga |")
    L.append("|---|:---:|:---:|:---:|:---:|:---:|")
    L.append(row_pit("Varade maht kokku", *m['aum']['total'], fmt_m))
    L.append(row_pit("sh II sammas", *m['aum']['ii'], fmt_m))
    L.append(row_pit("sh III sammas", *m['aum']['iii'], fmt_m))
    L.append("")
    L.append("<!-- comment:growth -->\n\n<!-- /comment:growth -->\n")
    L.append(f"![Kasvuallikad](charts_h1/growth_waterfall.png)\n")
    L.append(f"| Kasvuallikas | {H} | {Hp} |")
    L.append("|---|:---:|:---:|")
    g = m['growth']
    L.append(f"| Sissemaksed | {fmt_eur_m(g['sissemaksed'][0])} | {fmt_eur_m(g['sissemaksed'][1])} |")
    L.append(f"| Vahetustega ületoodud vara | {fmt_eur_m(g['yletoodav'][0])} | {fmt_eur_m(g['yletoodav'][1])} |")
    L.append(f"| Lahkujate vara | {fmt_eur_m(g['lahkujad'][0])} | {fmt_eur_m(g['lahkujad'][1])} |")
    L.append(f"| Väljavõetud vara | {fmt_eur_m(g['valjavoetud'][0])} | {fmt_eur_m(g['valjavoetud'][1])} |")
    L.append(f"| Turu mõju | {fmt_eur_m(g['turg'][0])} | {fmt_eur_m(g['turg'][1])} |")
    L.append("")
    L.append("---\n")

    # 2. Savers
    L.append("## Kogujad\n")
    L.append("<!-- comment:savers -->\n\n<!-- /comment:savers -->\n")
    L.append(f"![Kogujate arv](charts_h1/savers.png)\n")
    L.append(f"| Kogujad | 30.06.{prev} | 31.12.{prev} | 30.06.{year} | Poolaastaga | Aastaga |")
    L.append("|---|:---:|:---:|:---:|:---:|:---:|")
    L.append(row_pit("Kogujaid kokku", *m['savers']['total'], fmt_int))
    L.append(row_pit("sh ainult II sammas", *m['savers']['ii_only'], fmt_int))
    L.append(row_pit("sh ainult III sammas", *m['savers']['iii_only'], fmt_int))
    L.append(row_pit("sh II ja III sammas", *m['savers']['both'], fmt_int))
    L.append("")
    L.append("### Uued kogujad\n")
    L.append("<!-- comment:new_savers -->\n\n<!-- /comment:new_savers -->\n")
    L.append(f"| Uued kogujad | {H} | {Hp} | Muutus |")
    L.append("|---|:---:|:---:|:---:|")
    L.append(row3("Uusi kogujaid kokku", *m['new_savers']['total'], fmt_int))
    L.append(row3("sh uued II samba kogujad", *m['new_savers']['ii'], fmt_int))
    L.append(row3("sh uued III samba kogujad", *m['new_savers']['iii'], fmt_int))
    L.append("")

    # 2b. Saver determination — YTD comparison vs the 2025 annual-report baseline
    if det:
        n = det['total']
        def drow(label, val):
            return f"| {label} | {fmt_int(val)} | {val / n:.1%} |"
        L.append("### Kui sihikindlad on meie kogujad?\n")
        L.append("<!-- comment:determination -->\n\n<!-- /comment:determination -->\n")
        L.append(f"![Kogujate sihikindlus](charts_h1/determination.png)\n")
        L.append("| Grupp | Kogujaid | Osakaal |")
        L.append("|---|:---:|:---:|")
        L.append(drow("**Sihikindlad** (II 4/6% ja III ≥ 1200 €)", det['determined']))
        L.append(drow("**Sihikindla poole teel**", det['halfway']))
        L.append(drow("&nbsp;&nbsp;– II 4/6%, aga III < 1200 €", det['halfway_a']))
        L.append(drow("&nbsp;&nbsp;– II 2%, aga III ≥ 1200 €", det['halfway_b']))
        L.append(drow("Muud", det['other']))
        L.append(f"| **Kogujaid kokku** | **{fmt_int(n)}** | **100,0%** |")
        L.append("")
        L.append(f"*Hetkeseis. Segment (card 2324): II samba maksemäär × III samba viimase "
                 f"12 kuu sissemaksed. Baas on aktiivsete kogujate arv (card 2578), sama "
                 f"mis ülal — \"Muud\" on jääk.*")
        L.append("")
    L.append("---\n")

    # 3. Contributions
    L.append("## Sissemaksed\n")
    L.append("<!-- comment:contributions -->\n\n<!-- /comment:contributions -->\n")
    L.append(f"![Sissemaksed](charts_h1/contributions.png)\n")
    L.append(f"| Sissemaksed | {H} | {Hp} | Muutus |")
    L.append("|---|:---:|:---:|:---:|")
    L.append(row3("II samba sissemaksed", *m['contributions']['ii'], fmt_eur_m))
    L.append(row3("III samba sissemaksed", *m['contributions']['iii'], fmt_eur_m))
    c = m['contributions']['total']
    L.append(f"| **Sissemaksed kokku** | **{fmt_eur_m(c[0])}** | **{fmt_eur_m(c[1])}** | ***{fmt_pct(pct(*c))}*** |")
    L.append("")
    if m['tkf']['amount'][0]:
        L.append("### Täiendavasse Kogumisfondi tehtud maksed\n")
        L.append(f"| TKF | {H} | {Hp} | Muutus |")
        L.append("|---|:---:|:---:|:---:|")
        t = m['tkf']['amount']
        prev_disp = fmt_eur_m(t[1]) if t[1] else "– (fond ei tegutsenud)"
        change = fmt_pct(pct(*t)) if t[1] else "–"
        L.append(f"| Sissemaksed kokku | {fmt_eur_m(t[0])} | {prev_disp} | *{change}* |")
        L.append("")
    L.append("---\n")

    # 4. Switching
    L.append("## Fondivahetused\n")
    L.append("<!-- comment:switching -->\n\n<!-- /comment:switching -->\n")
    L.append(f"| Fondivahetused | {H} | {Hp} | Muutus |")
    L.append("|---|:---:|:---:|:---:|")
    L.append(row3("Tulevasse vahetanute arv (II sammas)", *m['switching']['count'], fmt_int))
    L.append(row3("sh ületoodud vara", *m['switching']['aum'], fmt_eur_m))
    L.append(row3("Tulevast välja vahetanute arv (II sammas)", *m['switching']['out_ii'], fmt_int))
    L.append(row3("Tulevast välja vahetanute arv (III sammas)", *m['switching']['out_iii'], fmt_int))
    L.append("")
    L.append("---\n")

    # 5. Outflows
    L.append("## Väljavoolud\n")
    L.append("<!-- comment:outflows -->\n\n<!-- /comment:outflows -->\n")
    L.append(f"| Väljavoolud | {H} | {Hp} | Muutus |")
    L.append("|---|:---:|:---:|:---:|")
    L.append(row3("II sambast raha välja võtnute arv", *m['outflows']['exiters_ppl'], fmt_int))
    L.append(row3("II sambast välja võetud vara", *m['outflows']['exiters'], fmt_eur_m))
    L.append(row3("III sambast raha välja võtnute arv", *m['outflows']['iii_withdrawals_ppl'], fmt_int))
    L.append(row3("III sambast välja võetud vara", *m['outflows']['iii_withdrawals'], fmt_eur_m))
    L.append(row3("II samba lahkujate (teise fondi vahetanute) vara", *m['outflows']['leavers'], fmt_eur_m))
    L.append("")
    L.append("---\n")
    L.append("*Aruanne genereeritud [Tuleva Reporting Engine](https://github.com/TulevaEE/reporting-engine)'iga*\n")

    out_dir = report_dir / 'output' / str(year)
    out_dir.mkdir(parents=True, exist_ok=True)
    md_file = out_dir / f'halfyear_report_{year}-H1.md'
    md_file.write_text("\n".join(L))
    print(f"Markdown generated: {md_file}")
    print(f"\nNext: edit narrative comments in {md_file}, then run:")
    print(f"  python build_halfyear_report.py {year} html")
    return md_file


def _embed_images(html, base_path):
    import re
    def repl(match):
        tag = match.group(0)
        srcm = re.search(r'src="([^"]+)"', tag)
        if not srcm:
            return tag
        src = srcm.group(1)
        if src.startswith('data:'):
            return tag
        img_path = (base_path / src).resolve()
        if not img_path.exists():
            print(f"  Warning: image not found: {img_path}")
            return tag
        b64 = base64.b64encode(img_path.read_bytes()).decode()
        return tag.replace(src, f'data:image/png;base64,{b64}')
    return re.sub(r'<img[^>]+>', repl, html)


def build_html(year: int) -> Path:
    report_dir = Path(__file__).parent
    out_dir = report_dir / 'output' / str(year)
    md_file = out_dir / f'halfyear_report_{year}-H1.md'
    if not md_file.exists():
        print(f"ERROR: MD not found: {md_file}. Run md step first.")
        return None
    md_text = md_file.read_text()
    style = (report_dir.parent.parent / 'common' / 'branding' / 'style.css').read_text()
    body = markdown.markdown(md_text, extensions=['tables', 'fenced_code'])
    html = f"""<!DOCTYPE html>
<html lang="et">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Tuleva {year}. aasta I poolaasta tulemused</title>
<style>
{style}
body {{ max-width: 900px; margin: 0 auto; padding: 24px; }}
img {{ max-width: 100%; height: auto; }}
table {{ border-collapse: collapse; width: 100%; margin: 1em 0; }}
</style>
</head>
<body>
{body}
</body>
</html>"""
    html = _embed_images(html, out_dir)
    html_file = out_dir / f'halfyear_report_{year}-H1.html'
    html_file.write_text(html)
    print(f"HTML generated: {html_file}")
    return html_file


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python build_halfyear_report.py <year> <md|html>")
        sys.exit(1)
    year = int(sys.argv[1])
    fmt = sys.argv[2]
    if fmt == 'md':
        r = build_md(year)
    elif fmt == 'html':
        r = build_html(year)
    else:
        print(f"Invalid format: {fmt}")
        sys.exit(1)
    if not r:
        sys.exit(1)
