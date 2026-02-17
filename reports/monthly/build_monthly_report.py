"""
Build monthly board report from Metabase data.
"""
import sys
import yaml
import markdown
import base64
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from generate_monthly_charts import generate_monthly_charts, ESTONIAN_MONTHS
# weasyprint is imported lazily in build_monthly_report() only when format='pdf'


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

    # III pillar contributor count
    row = get_month_row(cards.get('III samba sissemakse tegijate arv kuus', {}), year, month)
    if row:
        report['iii_contributors'] = row

    # Contribution rate changes
    row = get_month_row(cards.get('II samba maksemäära muutmine', {}), year, month)
    if row:
        report['rate_changes'] = row

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
        report['financials'] = [
            by_name[name] for name in selected_rows if name in by_name
        ]

    return report


def build_monthly_report(year: int, month: int, output_format: str = 'html') -> Path:
    """
    Build monthly board report by rendering Jinja2 template with YAML data.

    Args:
        year: The reporting year (e.g., 2025)
        month: The reporting month (1-12)
        output_format: 'md', 'html', or 'pdf'

    Returns:
        Path to the generated file
    """
    # Paths
    base_dir = Path(__file__).parent.parent.parent
    report_dir = Path(__file__).parent
    data_file = report_dir / 'data' / f'{year}-{month:02d}.yaml'
    template_dir = report_dir / 'content'
    style_file = base_dir / 'common' / 'branding' / 'style.css'
    output_dir = report_dir / 'output' / str(year)

    # Load data from YAML
    print(f"Loading data from: {data_file}")
    if not data_file.exists():
        print(f"ERROR: Data file not found: {data_file}")
        print(f"HINT: Run 'python fetch_monthly_data.py {year} {month}' first to fetch data.")
        return None

    with open(data_file, 'r') as f:
        data = yaml.safe_load(f)

    # Load optional comments
    comments_file = report_dir / 'comments' / f'{year}-{month:02d}.yaml'
    if comments_file.exists():
        with open(comments_file, 'r') as f:
            comments = yaml.safe_load(f) or {}
        print(f"Loaded comments from: {comments_file}")
    else:
        comments = {}
        print(f"No comments file found at {comments_file}, using empty comments")

    print(f"Loaded data for {data.get('month_name', 'Unknown')} {data.get('year', year)}")

    # Generate charts
    print("Generating charts...")
    charts_dir = generate_monthly_charts(year, month)
    chart_paths = {
        'aum': 'charts/aum.png',
        'growth_month': 'charts/growth_month.png',
        'growth_ytd': 'charts/growth_ytd.png',
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

    # Pre-process data for template
    report = preprocess_data(data, year, month)

    # Set up Jinja2 environment
    env = Environment(loader=FileSystemLoader(template_dir))
    template = env.get_template('report.md')

    # Add Estonian month name to template context
    month_name_et = ESTONIAN_MONTHS.get(month, str(month))

    # Render the Markdown template
    rendered_md = template.render(report=report, charts=chart_paths, comments=comments,
                                  month_name_et=month_name_et, **data)

    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # Save Markdown
    md_file = output_dir / f'monthly_report_{year}-{month:02d}.md'
    with open(md_file, 'w') as f:
        f.write(rendered_md)
    print(f"Markdown generated: {md_file}")

    if output_format == 'md':
        return md_file

    # Convert Markdown to HTML
    html_content = markdown.markdown(
        rendered_md,
        extensions=['tables', 'fenced_code']
    )

    # Wrap in full HTML document
    month_name = data.get('month_name', f'{month:02d}')
    html_document = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Tuleva Monthly Board Report - {month_name} {year}</title>
</head>
<body>
{html_content}
</body>
</html>"""

    # Save HTML
    html_file = output_dir / f'monthly_report_{year}-{month:02d}.html'
    with open(html_file, 'w') as f:
        f.write(html_document)
    print(f"HTML generated: {html_file}")

    if output_format == 'html':
        return html_file

    # For PDF: embed images as base64
    print("Embedding images for PDF...")
    html_with_images = embed_images_as_base64(html_document, output_dir)

    # Generate PDF with WeasyPrint
    pdf_file = output_dir / f'monthly_report_{year}-{month:02d}.pdf'
    print(f"Generating PDF with Tuleva branding...")

    from weasyprint import HTML, CSS
    html = HTML(string=html_with_images, base_url=str(report_dir))
    css = CSS(filename=str(style_file))
    html.write_pdf(pdf_file, stylesheets=[css])

    print(f"PDF generated: {pdf_file}")
    return pdf_file


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
