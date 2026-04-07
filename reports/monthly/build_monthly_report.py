"""
Build monthly board report from Metabase data.
"""
import re
import sys
import yaml
import markdown
import base64
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from generate_monthly_charts import generate_monthly_charts, ESTONIAN_MONTHS
# weasyprint is imported lazily in build_monthly_report() only when format='pdf'


def extract_comments_from_md(md_path: Path) -> dict:
    """Extract comments from an existing markdown report using comment markers."""
    if not md_path.exists():
        return {}
    text = md_path.read_text()
    comments = {}
    for match in re.finditer(
        r'<!-- comment:(\w+) -->\n(.*?)\n<!-- /comment:\1 -->',
        text, re.DOTALL
    ):
        key = match.group(1)
        value = match.group(2).strip()
        if value:
            comments[key] = value
    return comments


def embed_images_as_base64(html_content: str, base_path: Path) -> str:
    """Convert image paths to base64 data URIs for PDF embedding."""
    import re

    def replace_image(match):
        img_tag = match.group(0)
        src_match = re.search(r'src="([^"]+)"', img_tag)
        if not src_match:
            return img_tag

        src = src_match.group(1)

        # Skip if already a data URI
        if src.startswith('data:'):
            return img_tag

        # Resolve the path
        if src.startswith('../'):
            img_path = (base_path / src).resolve()
        else:
            img_path = base_path / src

        if not img_path.exists():
            print(f"  Warning: Image not found: {img_path}")
            return img_tag

        # Read and encode
        img_data = img_path.read_bytes()
        ext = img_path.suffix.lower()
        mime_type = {
            '.png': 'image/png',
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
            '.gif': 'image/gif',
            '.svg': 'image/svg+xml',
        }.get(ext, 'image/png')

        b64 = base64.b64encode(img_data).decode('utf-8')
        new_src = f'data:{mime_type};base64,{b64}'

        return img_tag.replace(src, new_src)

    # Replace all img tags
    return re.sub(r'<img[^>]+>', replace_image, html_content)


def get_month_row(card_data, year, month):
    """Extract the row matching the target month from a time-series card."""
    target = f'{year}-{month:02d}-01'
    for row in card_data.get('data') or []:
        if row.get('kuu: Month') == target:
            return row
    return None


def get_ytd_row(card_data, year):
    """Extract current year's row from a YTD smartscalar card."""
    target = f'{year}-01-01'
    for row in card_data.get('data') or []:
        if row.get('reporting_year') == target:
            return row
    return None


def get_prev_ytd_row(card_data, year):
    """Extract previous year's row from a YTD smartscalar card."""
    target = f'{year - 1}-01-01'
    for row in card_data.get('data') or []:
        if row.get('reporting_year') == target:
            return row
    return None


def preprocess_data(data, year, month):
    """Extract report-month values from raw card data into a clean structure."""
    cards = data.get('cards', {})
    report = {}

    # --- AUM ---
    aum_card = cards.get('AUM (koos ootel vahetuste ja väljumistega)', {})
    aum_month_key = f"Jan-{year % 100:02d}" if month == 1 else \
        f"{'Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec'.split()[month-1]}-{year % 100:02d}"
    for row in aum_card.get('data') or []:
        if row.get('month') == aum_month_key:
            report['aum'] = row
            break

    # --- Savers (kogujad) ---
    row = get_month_row(cards.get('kogujate arv kuus', {}), year, month)
    if row:
        report['savers'] = row

    row = get_month_row(cards.get('uute kogujate arv kuus', {}), year, month)
    if row:
        report['new_savers'] = row

    # YTD new savers
    report['new_savers_ytd'] = get_ytd_row(
        cards.get('uute kogujate arv YTD', {}), year)
    report['new_savers_ytd_prev'] = get_prev_ytd_row(
        cards.get('uute kogujate arv YTD', {}), year)
    report['new_savers_ii_ytd'] = get_ytd_row(
        cards.get('uute II samba kogujate arv YTD', {}), year)
    report['new_savers_iii_ytd'] = get_ytd_row(
        cards.get('uute III samba kogujate arv YTD', {}), year)

    # Monthly II/III new savers breakdown (cards 1519 + 1520)
    ii_joiners_row = get_month_row(
        cards.get('II sambaga liitujate arv kuus', {}), year, month)
    if ii_joiners_row:
        report['new_savers_ii_month'] = (
            ii_joiners_row['2'] + ii_joiners_row['2+3'] + ii_joiners_row['3>2'])

    iii_joiners_row = get_month_row(
        cards.get('III sambaga liitujate arv kuus', {}), year, month)
    if iii_joiners_row:
        report['new_savers_iii_month'] = (
            iii_joiners_row['3'] + iii_joiners_row['2+3'] + iii_joiners_row['2>3'])

    # --- Contributions (sissemaksed) ---
    row = get_month_row(cards.get('II samba sissemaksete summa kuus, M EUR', {}), year, month)
    if row:
        report['ii_contributions'] = row

    row = get_month_row(cards.get('III samba sissemaksete summa kuus, M EUR', {}), year, month)
    if row:
        report['iii_contributions'] = row

    report['ii_contributions_ytd'] = get_ytd_row(
        cards.get('II s sissemaksed YTD', {}), year)
    report['iii_contributions_ytd'] = get_ytd_row(
        cards.get('III s sissemaksed YTD', {}), year)

    # YTD YoY for contributions
    ii_ytd_prev = get_prev_ytd_row(cards.get('II s sissemaksed YTD', {}), year)
    if report.get('ii_contributions_ytd') and ii_ytd_prev:
        cur = report['ii_contributions_ytd']['second_pillar_contributions_eur']
        prev = ii_ytd_prev['second_pillar_contributions_eur']
        if prev:
            report['ii_contributions_ytd_yoy'] = (cur - prev) / prev

    iii_ytd_prev = get_prev_ytd_row(cards.get('III s sissemaksed YTD', {}), year)
    if report.get('iii_contributions_ytd') and iii_ytd_prev:
        cur = report['iii_contributions_ytd']['third_pillar_contributions_eur']
        prev = iii_ytd_prev['third_pillar_contributions_eur']
        if prev:
            report['iii_contributions_ytd_yoy'] = (cur - prev) / prev

    # Contributions total row
    ii_m = report.get('ii_contributions', {}).get('II samba sissemaksed, M EUR', 0)
    iii_m = report.get('iii_contributions', {}).get('III samba sissemaksed, M EUR', 0)
    if ii_m or iii_m:
        total_month = (ii_m + iii_m) / 1000000
        # Previous year month values for YoY
        ii_prev_row = get_month_row(
            cards.get('II samba sissemaksete summa kuus, M EUR', {}), year - 1, month)
        iii_prev_row = get_month_row(
            cards.get('III samba sissemaksete summa kuus, M EUR', {}), year - 1, month)
        prev_total = ((ii_prev_row or {}).get('II samba sissemaksed, M EUR', 0) +
                      (iii_prev_row or {}).get('III samba sissemaksed, M EUR', 0))
        month_yoy = (ii_m + iii_m - prev_total) / prev_total if prev_total else 0

        # YTD totals
        ii_ytd_cur = (report.get('ii_contributions_ytd') or {}).get(
            'second_pillar_contributions_eur', 0)
        iii_ytd_cur = (report.get('iii_contributions_ytd') or {}).get(
            'third_pillar_contributions_eur', 0)
        ytd_total = ii_ytd_cur + iii_ytd_cur

        ii_ytd_p = (ii_ytd_prev or {}).get('second_pillar_contributions_eur', 0)
        iii_ytd_p = (iii_ytd_prev or {}).get('third_pillar_contributions_eur', 0)
        ytd_prev_total = ii_ytd_p + iii_ytd_p
        ytd_yoy = (ytd_total - ytd_prev_total) / ytd_prev_total if ytd_prev_total else 0

        report['contributions_total'] = {
            'month': total_month,
            'month_yoy': month_yoy,
            'ytd': ytd_total,
            'ytd_yoy': ytd_yoy,
        }

    # III pillar contributor count
    row = get_month_row(cards.get('III samba sissemakse tegijate arv kuus', {}), year, month)
    if row:
        report['iii_contributors'] = row

    # III pillar contributors YTD (card 1657 — scalar)
    iii_contributors_ytd_card = cards.get(
        'III s sissemakse tegijate arv YTD', {})
    iii_contributors_ytd_data = iii_contributors_ytd_card.get('data', [])
    if iii_contributors_ytd_data:
        report['iii_contributors_ytd'] = iii_contributors_ytd_data[0].get(
            'Distinct values of Personal ID')

    # Contribution rate changes
    row = get_month_row(cards.get('II samba maksemäära muutmine', {}), year, month)
    if row:
        report['rate_changes'] = row

    # Rate changes YTD and YoY (from card 1573)
    rate_changes_data = cards.get('II samba maksemäära muutmine', {}).get('data', [])
    ytd_raised = 0
    ytd_lowered = 0
    ytd_prev_raised = 0
    ytd_prev_lowered = 0
    for rc_row in rate_changes_data:
        date_str = rc_row.get('kuu: Month', '')
        rc_month = int(date_str[5:7]) if len(date_str) >= 7 else 0
        if date_str.startswith(str(year)):
            ytd_raised += rc_row.get('maksemäära tõstnute arv', 0)
            ytd_lowered += rc_row.get('maksemäära langetanute arv', 0)
        elif date_str.startswith(str(year - 1)) and rc_month <= month:
            ytd_prev_raised += rc_row.get('maksemäära tõstnute arv', 0)
            ytd_prev_lowered += rc_row.get('maksemäära langetanute arv', 0)
    if ytd_raised or ytd_lowered:
        report['rate_changes_ytd'] = {
            'raised': ytd_raised, 'lowered': ytd_lowered}
    if ytd_prev_raised or ytd_prev_lowered:
        report['rate_changes_ytd_prev'] = {
            'raised': ytd_prev_raised, 'lowered': ytd_prev_lowered}

    # Rate changes month YoY
    rate_changes_prev = get_month_row(
        cards.get('II samba maksemäära muutmine', {}), year - 1, month)
    if rate_changes_prev:
        report['rate_changes_prev'] = rate_changes_prev

    # --- Fund switching ---
    row = get_month_row(cards.get('II samba vahetajate arv kuus', {}), year, month)
    if row:
        report['switchers'] = row

    row = get_month_row(cards.get('II samba vahetajate ületoodava vara maht kuus, M EUR', {}), year, month)
    if row:
        report['switchers_aum'] = row

    report['switchers_ytd'] = get_ytd_row(
        cards.get('II s vahetajate arv YTD', {}), year)
    report['switchers_aum_ytd'] = get_ytd_row(
        cards.get('II s vahetustega ületoodav vara YTD', {}), year)

    # Switching YTD YoY
    switchers_ytd_prev = get_prev_ytd_row(
        cards.get('II s vahetajate arv YTD', {}), year)
    if report.get('switchers_ytd') and switchers_ytd_prev:
        cur = report['switchers_ytd']['IIs sissevahetajate arv']
        prev = switchers_ytd_prev['IIs sissevahetajate arv']
        if prev:
            report['switchers_ytd_yoy'] = (cur - prev) / prev

    switchers_aum_ytd_prev = get_prev_ytd_row(
        cards.get('II s vahetustega ületoodav vara YTD', {}), year)
    if report.get('switchers_aum_ytd') and switchers_aum_ytd_prev:
        cur = report['switchers_aum_ytd']['IIs vahetustega ületoodav vara M EUR']
        prev = switchers_aum_ytd_prev['IIs vahetustega ületoodav vara M EUR']
        if prev:
            report['switchers_aum_ytd_yoy'] = (cur - prev) / prev

    # Fund switching details (top 10)
    to_funds = cards.get('II samba vahetusavalduste arv pangafondidesse sel vahetusperioodil', {})
    report['switching_to'] = (to_funds.get('data') or [])[:10]

    from_funds = cards.get('II samba vahetusavalduste arv lähtefondi järgi sel vahetusperioodil', {})
    report['switching_from'] = (from_funds.get('data') or [])[:10]

    # --- Outflows ---
    row = get_month_row(cards.get('II samba lahkujate varade maht kuus, M EUR', {}), year, month)
    if row:
        report['ii_leavers'] = row

    row = get_month_row(cards.get('II samba väljujate varade maht kuus, M EUR', {}), year, month)
    if row:
        report['ii_exiters'] = row

    row = get_month_row(cards.get('III sambast välja võetud varade maht kuus, M EUR', {}), year, month)
    if row:
        report['iii_withdrawals'] = row

    report['ii_leavers_ytd'] = get_ytd_row(
        cards.get('II s vahetustega väljaminevad varad YTD', {}), year)
    report['ii_exiters_ytd'] = get_ytd_row(
        cards.get('II s raha väljavõtmised YTD', {}), year)
    report['iii_withdrawals_ytd'] = get_ytd_row(
        cards.get('III s väljavõetud varad YTD', {}), year)

    # --- Growth sources (waterfall) ---
    report['growth_actual'] = cards.get(
        'Kasvuallikad eelmisel kuul (tegelik), M EUR', {}).get('data', [])
    report['growth_ytd'] = cards.get(
        'Kasvuallikad YTD (tegelik), M EUR', {}).get('data', [])
    report['growth_forecast'] = cards.get(
        'Kasvuallikad (aasta lõpu prognoos), M EUR', {}).get('data', [])

    # --- Täiendav Kogumisfond contributions (card 2305) ---
    tkf_data = cards.get('Täiendavasse Kogumisfondi tehtud maksed', {}).get('data', [])
    target_month_iso = f'{year}-{month:02d}-01T00:00:00Z'
    prev_month_iso = f'{year - 1}-{month:02d}-01T00:00:00Z'
    tkf_month = None
    tkf_prev = None
    tkf_ytd_amount = 0
    tkf_ytd_contributors = set()
    tkf_prev_ytd_amount = 0
    tkf_prev_ytd_contributors = set()
    for row in tkf_data:
        dt = row.get('Created At: Month', '')
        if dt == target_month_iso:
            tkf_month = row
        if dt == prev_month_iso:
            tkf_prev = row
        # YTD: sum all months in target year up to report month
        if dt.startswith(str(year)) and dt <= target_month_iso:
            tkf_ytd_amount += row.get('Sum of Amount', 0)
        # Prev YTD
        if dt.startswith(str(year - 1)) and dt[5:7] <= f'{month:02d}':
            tkf_prev_ytd_amount += row.get('Sum of Amount', 0)

    if tkf_month:
        tkf = {
            'amount': tkf_month['Sum of Amount'],
            'contributors': tkf_month['Distinct values of Remitter ID Code'],
            'ytd_amount': tkf_ytd_amount,
        }
        if tkf_prev:
            tkf['amount_yoy'] = (tkf_month['Sum of Amount'] - tkf_prev['Sum of Amount']) / tkf_prev['Sum of Amount']
            tkf['contributors_yoy'] = (tkf_month['Distinct values of Remitter ID Code'] - tkf_prev['Distinct values of Remitter ID Code']) / tkf_prev['Distinct values of Remitter ID Code']
        if tkf_prev_ytd_amount:
            tkf['ytd_amount_yoy'] = (tkf_ytd_amount - tkf_prev_ytd_amount) / tkf_prev_ytd_amount
        report['tkf_contributions'] = tkf

    # --- Financial results (card 636) ---
    financials_raw = cards.get('Tuleva finantstulemused', {}).get('data', [])
    if financials_raw:
        by_name = {row['Eur']: row for row in financials_raw}
        selected_rows = [
            'brutomarginaal pärast litsentsitasu',
            'tööjõukulud',
            'mitmesugused tegevuskulud',
            'EBITDA/ärikasum',
            'puhaskasum',
            'litsentsitasu',
        ]
        selected = [
            by_name[name] for name in selected_rows if name in by_name
        ]
        # Skip section if any required values are None (financials not yet finalized)
        if all(row.get('Kuu Tulemus') is not None and row.get('YoY %') is not None
               for row in selected):
            report['financials'] = selected

    return report


def md_to_html(md_text: str, title: str) -> str:
    """Convert markdown text to a full HTML document."""
    html_content = markdown.markdown(
        md_text,
        extensions=['tables', 'fenced_code']
    )
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{title}</title>
    <style>
        body {{ max-width: 900px; margin: 0 auto; padding: 20px; font-family: sans-serif; }}
        img {{ max-width: 100%; height: auto; }}
        table {{ border-collapse: collapse; }}
        th, td {{ border: 1px solid #ddd; padding: 6px 12px; text-align: left; }}
        th {{ background-color: #f5f5f5; }}
    </style>
</head>
<body>
{html_content}
</body>
</html>"""


def build_md(year: int, month: int) -> Path:
    """
    Step 1: Generate MD report from template + data with placeholder comments.
    Run this first, then edit the MD manually, then run build_html.
    """
    report_dir = Path(__file__).parent
    data_file = report_dir / 'data' / f'{year}-{month:02d}.yaml'
    template_dir = report_dir / 'content'
    output_dir = report_dir / 'output' / str(year)

    print(f"Loading data from: {data_file}")
    if not data_file.exists():
        print(f"ERROR: Data file not found: {data_file}")
        print(f"HINT: Run 'python fetch_monthly_data.py {year} {month}' first to fetch data.")
        return None

    with open(data_file, 'r') as f:
        data = yaml.safe_load(f)

    # Load comments from YAML placeholders
    comments_file = report_dir / 'comments' / f'{year}-{month:02d}.yaml'
    if comments_file.exists():
        with open(comments_file, 'r') as f:
            comments = yaml.safe_load(f) or {}
        print(f"Loaded comments from: {comments_file}")
    else:
        comments = {}
        print(f"No comments file found, using empty comments")

    print(f"Loaded data for {data.get('month_name', 'Unknown')} {data.get('year', year)}")

    # Generate charts
    print("Generating charts...")
    generate_monthly_charts(year, month)
    chart_paths = {
        'aum': 'charts/aum.png',
        'growth_waterfall': 'charts/growth_waterfall.png',
        'savers': 'charts/savers.png',
        'new_savers_pillar': 'charts/new_savers_pillar.png',
        'new_ii_savers_source': 'charts/new_ii_savers_source.png',
        'contributions': 'charts/contributions.png',
        'iii_contributors': 'charts/iii_contributors.png',
        'switching_volume': 'charts/switching_volume.png',
        'switching_sources': 'charts/switching_sources.png',
        'leavers': 'charts/leavers.png',
        'drawdowns': 'charts/drawdowns.png',
        'unit_price': 'charts/unit_price.png',
        'cumulative_returns': 'charts/cumulative_returns.png',
    }

    # Pre-process data and render template
    report = preprocess_data(data, year, month)
    env = Environment(loader=FileSystemLoader(template_dir))
    template = env.get_template('report.md')
    month_name_et = ESTONIAN_MONTHS.get(month, str(month))
    rendered_md = template.render(report=report, charts=chart_paths, comments=comments,
                                  month_name_et=month_name_et, **data)

    # Save Markdown
    output_dir.mkdir(parents=True, exist_ok=True)
    md_file = output_dir / f'monthly_report_{year}-{month:02d}.md'
    with open(md_file, 'w') as f:
        f.write(rendered_md)
    print(f"Markdown generated: {md_file}")
    print(f"\nNext step: edit comments in {md_file}, then run:")
    print(f"  python build_monthly_report.py {year} {month} html")
    return md_file


def build_html(year: int, month: int) -> Path:
    """
    Step 2: Convert the existing MD report (with your comments) to HTML.
    """
    report_dir = Path(__file__).parent
    data_file = report_dir / 'data' / f'{year}-{month:02d}.yaml'
    output_dir = report_dir / 'output' / str(year)
    md_file = output_dir / f'monthly_report_{year}-{month:02d}.md'

    if not md_file.exists():
        print(f"ERROR: Markdown report not found: {md_file}")
        print(f"HINT: Run 'python build_monthly_report.py {year} {month} md' first.")
        return None

    print(f"Reading markdown from: {md_file}")
    md_text = md_file.read_text()

    # Get month name for HTML title
    if data_file.exists():
        with open(data_file, 'r') as f:
            data = yaml.safe_load(f)
        month_name = data.get('month_name', f'{month:02d}')
    else:
        month_name = ESTONIAN_MONTHS.get(month, f'{month:02d}').capitalize()

    html_document = md_to_html(md_text, f"Tuleva Monthly Board Report - {month_name} {year}")

    html_file = output_dir / f'monthly_report_{year}-{month:02d}.html'
    with open(html_file, 'w') as f:
        f.write(html_document)
    print(f"HTML generated: {html_file}")
    return html_file


def build_pdf(year: int, month: int) -> Path:
    """
    Step 3 (optional): Convert the existing MD report to PDF.
    """
    base_dir = Path(__file__).parent.parent.parent
    report_dir = Path(__file__).parent
    data_file = report_dir / 'data' / f'{year}-{month:02d}.yaml'
    style_file = base_dir / 'common' / 'branding' / 'style.css'
    output_dir = report_dir / 'output' / str(year)
    md_file = output_dir / f'monthly_report_{year}-{month:02d}.md'

    if not md_file.exists():
        print(f"ERROR: Markdown report not found: {md_file}")
        print(f"HINT: Run 'python build_monthly_report.py {year} {month} md' first.")
        return None

    print(f"Reading markdown from: {md_file}")
    md_text = md_file.read_text()

    if data_file.exists():
        with open(data_file, 'r') as f:
            data = yaml.safe_load(f)
        month_name = data.get('month_name', f'{month:02d}')
    else:
        month_name = ESTONIAN_MONTHS.get(month, f'{month:02d}').capitalize()

    html_document = md_to_html(md_text, f"Tuleva Monthly Board Report - {month_name} {year}")

    print("Embedding images for PDF...")
    html_with_images = embed_images_as_base64(html_document, output_dir)

    pdf_file = output_dir / f'monthly_report_{year}-{month:02d}.pdf'
    print(f"Generating PDF with Tuleva branding...")

    from weasyprint import HTML, CSS
    html = HTML(string=html_with_images, base_url=str(report_dir))
    css = CSS(filename=str(style_file))
    html.write_pdf(pdf_file, stylesheets=[css])

    print(f"PDF generated: {pdf_file}")
    return pdf_file


def build_monthly_report(year: int, month: int, output_format: str = 'html') -> Path:
    """
    Build monthly board report.

    Workflow:
        1. `md`  — generate MD from template + data (with placeholder comments)
        2. Edit the MD file manually to add real comments
        3. `html` — convert the edited MD to HTML (no re-render)
        4. `pdf`  — convert the edited MD to PDF (optional)
    """
    if output_format == 'md':
        return build_md(year, month)
    elif output_format == 'html':
        return build_html(year, month)
    elif output_format == 'pdf':
        return build_pdf(year, month)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python build_monthly_report.py <year> <month> [format]")
        print("Example: python build_monthly_report.py 2025 01 html")
        print("Formats: md, html, pdf (default: html)")
        sys.exit(1)

    year = int(sys.argv[1])
    month = int(sys.argv[2])
    output_format = sys.argv[3] if len(sys.argv) > 3 else 'html'

    if output_format not in ('md', 'html', 'pdf'):
        print(f"Invalid format: {output_format}. Use 'md', 'html', or 'pdf'")
        sys.exit(1)

    result = build_monthly_report(year, month, output_format)
    if result:
        print(f"\nReport generated successfully: {result}")
    else:
        sys.exit(1)
