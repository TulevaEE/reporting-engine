"""
Fetch monthly KPI data from Metabase.

The primary source is the consolidated KPI card 2578 ("Mv Kpi New for Claude"):
a wide monthly time series (one row per month) covering AUM, active investors,
contributions, fund switching and outflows split by II/III pillar. YTD and YoY
figures are NOT columns on this card — they are computed downstream in Python
from the full series (build_monthly_report.py / kpi_2578.py).

A small set of "survivor" cards supply data that 2578 does not contain:
  - new-savers distinct-person counts + by-source splits (1518/418/1519/1520/1534/1535)
  - the monthly rate-change flow (1573) — 2578 only has cumulative rate stock
  - distinct III-pillar contributor YTD count (1657)
  - fund-level switching destination/source lists (1911/1912)
  - growth-source waterfalls incl. non-reconstructable forecast (389/392/393)
  - unit-price/returns series (2245), financial results (636), TKF payments (2305)
  - the AUM chart itself (334) which carries forecast bars + pre-rounded growth %

This replaces the previous approach of looping over every card pinned to
dashboard 74 (non-deterministic) plus a few hardcoded standalone cards.
"""
import sys
import yaml
from pathlib import Path
from datetime import datetime

# Add common scripts to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'common' / 'scripts'))

from metabase_client import MetabaseClient


# Consolidated KPI card — the primary data source (monthly time series).
PRIMARY_CARD_ID = 2578
PRIMARY_CARD_NAME = 'Mv Kpi New for Claude'

# Cards 2578 cannot replace. Keyed by card_id -> (downstream name, display).
# The downstream name MUST match how build_monthly_report / generate_monthly_charts
# look the card up in data['cards'].
SURVIVOR_CARDS = {
    334:  ('AUM (koos ootel vahetuste ja väljumistega)', 'line'),
    1518: ('uute kogujate arv kuus', 'combo'),
    418:  ('uute kogujate arv YTD', 'smartscalar'),
    1519: ('II sambaga liitujate arv kuus', 'combo'),
    1520: ('III sambaga liitujate arv kuus', 'combo'),
    1534: ('uute II samba kogujate arv YTD', 'smartscalar'),
    1535: ('uute III samba kogujate arv YTD', 'smartscalar'),
    1573: ('II samba maksemäära muutmine', 'combo'),
    1657: ('III s sissemakse tegijate arv YTD', 'scalar'),
    1911: ('II samba vahetusavalduste arv pangafondidesse sel vahetusperioodil', 'row'),
    1912: ('II samba vahetusavalduste arv lähtefondi järgi sel vahetusperioodil', 'row'),
    389:  ('Kasvuallikad eelmisel kuul (tegelik), M EUR', 'waterfall'),
    392:  ('Kasvuallikad YTD (tegelik), M EUR', 'waterfall'),
    393:  ('Kasvuallikad (aasta lõpu prognoos), M EUR', 'waterfall'),
    2245: ('Osakuhinna võrdlus', 'line'),
    636:  ('Tuleva finantstulemused', 'line'),
    2305: ('Täiendavasse Kogumisfondi tehtud maksed', 'line'),
}


def fetch_monthly_data(year: int, month: int) -> dict:
    """
    Fetch monthly KPI data: the consolidated card 2578 plus the survivor cards.

    Args:
        year: Report year (e.g., 2026)
        month: Report month (1-12)

    Returns:
        Dictionary with a `kpi_2578` block (full monthly series) and a `cards`
        block (survivor cards keyed by their downstream Estonian name).
    """
    print(f"Fetching monthly data for {year}-{month:02d}...")

    client = MetabaseClient()

    data = {
        'year': year,
        'month': month,
        'month_name': datetime(year, month, 1).strftime('%B'),
        'report_date': datetime.now().strftime('%Y-%m-%d'),
        'kpi_2578': {},
        'cards': {},
    }

    # Primary consolidated KPI card (full monthly time series).
    print(f"  Fetching [{PRIMARY_CARD_ID}] {PRIMARY_CARD_NAME} (primary)...")
    try:
        results = client.execute_card(PRIMARY_CARD_ID)
        data['kpi_2578'] = {
            'card_id': PRIMARY_CARD_ID,
            'display': 'table',
            'data': results,
        }
        print(f"    -> {len(results)} monthly rows")
    except Exception as e:
        print(f"    ERROR: {e}")
        data['kpi_2578'] = {'card_id': PRIMARY_CARD_ID, 'error': str(e)}

    # Survivor cards.
    for card_id, (card_name, display) in SURVIVOR_CARDS.items():
        print(f"  Fetching [{card_id}] {card_name}...")
        try:
            results = client.execute_card(card_id)
            data['cards'][card_name] = {
                'card_id': card_id,
                'display': display,
                'data': results,
            }
            print(f"    -> {len(results)} rows")
        except Exception as e:
            print(f"    ERROR: {e}")
            data['cards'][card_name] = {'card_id': card_id, 'error': str(e)}

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
