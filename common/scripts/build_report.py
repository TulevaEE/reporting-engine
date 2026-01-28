import yaml
from pathlib import Path
from jinja2 import Environment, FileSystemLoader


def build_report(year: int):
    """
    Build the annual report by rendering the Jinja2 template with YAML data.

    Args:
        year: The reporting year (e.g., 2025)
    """
    # Paths
    base_dir = Path(__file__).parent.parent.parent
    report_dir = base_dir / 'reports' / 'annual' / str(year)
    data_file = report_dir / 'data' / 'financials.yaml'
    template_dir = report_dir / 'content'
    output_file = report_dir / f'annual_report_{year}.md'

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

    # Render the template
    rendered = template.render(**data)

    # Save the rendered report
    with open(output_file, 'w') as f:
        f.write(rendered)

    print(f"Report generated: {output_file}")
    return output_file


if __name__ == "__main__":
    import sys

    year = int(sys.argv[1]) if len(sys.argv) > 1 else 2025
    build_report(year)
