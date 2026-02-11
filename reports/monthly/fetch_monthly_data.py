"""
Fetch monthly KPI data from Metabase dashboard 74.
"""
import sys
import yaml
from pathlib import Path
from datetime import datetime

# Add common scripts to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'common' / 'scripts'))

from metabase_client import MetabaseClient


def fetch_monthly_data(year: int, month: int) -> dict:
    """
    Fetch monthly KPI data from Metabase dashboard 74.

    Args:
        year: Report year (e.g., 2025)
        month: Report month (1-12)

    Returns:
        Dictionary containing all KPI data
    """
    print(f"Fetching monthly data for {year}-{month:02d}...")

    client = MetabaseClient()

    # Load config to get dashboard ID
    config_path = Path(__file__).parent.parent.parent / 'common' / 'config' / 'metabase.yaml'
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    dashboard_id = config.get('dashboard_id', 74)

    # Get all cards from the dashboard
    cards = client.get_dashboard_cards(dashboard_id)
    print(f"Found {len(cards)} cards on dashboard {dashboard_id}")

    # Initialize data structure
    data = {
        'year': year,
        'month': month,
        'month_name': datetime(year, month, 1).strftime('%B'),
        'report_date': datetime.now().strftime('%Y-%m-%d'),
        'cards': {},
        'kpis': {}
    }

    # Fetch data from each card
    for card in cards:
        card_id = card['card_id']
        card_name = card['name']

        print(f"  Fetching [{card_id}] {card_name}...")

        try:
            results = client.execute_card(card_id)

            # Store raw results
            data['cards'][card_name] = {
                'card_id': card_id,
                'display': card['display'],
                'data': results
            }

            # For scalar/single-value cards, extract the KPI value
            if results and len(results) == 1 and len(results[0]) == 1:
                value = list(results[0].values())[0]
                data['kpis'][card_name] = value
                print(f"    -> {value}")
            elif results and len(results) == 1:
                # Single row with multiple columns - store as dict
                data['kpis'][card_name] = results[0]
                print(f"    -> {results[0]}")
            else:
                print(f"    -> {len(results)} rows")

        except Exception as e:
            print(f"    ERROR: {e}")
            data['cards'][card_name] = {
                'card_id': card_id,
                'error': str(e)
            }

    return data


def save_monthly_data(year: int, month: int) -> Path:
    """
    Fetch monthly data and save to YAML file.

    Args:
        year: Report year
        month: Report month

    Returns:
        Path to saved file
    """
    data = fetch_monthly_data(year, month)

    # Determine output path
    output_dir = Path(__file__).parent / 'data'
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f'{year}-{month:02d}.yaml'

    # Save to YAML
    with open(output_file, 'w') as f:
        yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)

    print(f"\nSaved monthly data to: {output_file}")
    return output_file


if __name__ == "__main__":
    if len(sys.argv) < 3:
        # Default to current month
        now = datetime.now()
        year = now.year
        month = now.month
        print(f"No date specified, using current month: {year}-{month:02d}")
    else:
        year = int(sys.argv[1])
        month = int(sys.argv[2])

    save_monthly_data(year, month)
