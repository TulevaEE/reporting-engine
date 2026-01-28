import yaml
import markdown
import base64
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from weasyprint import HTML, CSS


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


def build_report(year: int, output_format: str = 'md'):
    """
    Build the annual report by rendering the Jinja2 template with YAML data.

    Args:
        year: The reporting year (e.g., 2025)
        output_format: 'md', 'html', or 'pdf'

    Returns:
        Path to the generated file
    """
    # Paths
    base_dir = Path(__file__).parent.parent.parent
    report_dir = base_dir / 'reports' / 'annual' / str(year)
    data_file = report_dir / 'data' / 'financials.yaml'
    template_dir = report_dir / 'content'
    style_file = base_dir / 'common' / 'branding' / 'style.css'

    # Load data from YAML
    print(f"Loading data from: {data_file}")
    if not data_file.exists():
        print(f"ERROR: Data file not found: {data_file}")
        print("HINT: Run 'python fetch_data.py fetch' first to generate the data.")
        return None

    with open(data_file, 'r') as f:
        data = yaml.safe_load(f)

    print(f"Loaded data: {data}")

    # Set up Jinja2 environment
    env = Environment(loader=FileSystemLoader(template_dir))
    template = env.get_template('report.md')

    # Render the Markdown template
    rendered_md = template.render(**data)

    # Save Markdown
    md_file = report_dir / f'annual_report_{year}.md'
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
    html_document = f"""<!DOCTYPE html>
<html lang="et">
<head>
    <meta charset="UTF-8">
    <title>Tuleva {year}. aasta tegevusaruanne</title>
</head>
<body>
{html_content}
</body>
</html>"""

    # Save HTML (with relative paths for viewing)
    html_file = report_dir / f'annual_report_{year}.html'
    with open(html_file, 'w') as f:
        f.write(html_document)
    print(f"HTML generated: {html_file}")

    if output_format == 'html':
        return html_file

    # For PDF: embed images as base64
    print("Embedding images for PDF...")
    html_with_images = embed_images_as_base64(html_document, report_dir)

    # Generate PDF with WeasyPrint
    pdf_file = report_dir / f'annual_report_{year}.pdf'
    print(f"Generating PDF with Tuleva branding...")

    html = HTML(string=html_with_images, base_url=str(report_dir))
    css = CSS(filename=str(style_file))
    html.write_pdf(pdf_file, stylesheets=[css])

    print(f"PDF generated: {pdf_file}")
    return pdf_file


if __name__ == "__main__":
    import sys

    year = int(sys.argv[1]) if len(sys.argv) > 1 else 2025
    output_format = sys.argv[2] if len(sys.argv) > 2 else 'pdf'

    if output_format not in ('md', 'html', 'pdf'):
        print(f"Invalid format: {output_format}. Use 'md', 'html', or 'pdf'")
        sys.exit(1)

    build_report(year, output_format)
