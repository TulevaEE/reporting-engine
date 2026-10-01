"""
Fetch monthly KPI data from Metabase, through the Tuleva agent gateway.

The personal Metabase key is gone (internal rule 22 p 4.1). Cards are read with
the gateway tool `reports`, which answers only cards on the Monthly KPIs (74)
and Weekly (12) dashboards; see common/scripts/gateway_client.py. The first run
opens a browser to log in with your Tuleva Google account.

The primary source is the consolidated KPI card 2578 ("Mv Kpi New for Claude"):
a wide monthly time series (one row per month) covering AUM, active investors,
contributions, fund switching and outflows split by II/III pillar. YTD and YoY
figures are NOT columns on this card — they are computed downstream in Python
from the full series (build_monthly_report.py / kpi_2578.py).

A small set of "survivor" cards supply data that 2578 does not contain:
  - new-savers distinct-person counts + by-source splits (1518/418/1519/1520/1534/1535)
  - distinct III-pillar contributor YTD count (1657)
  - fund-level switching destination/source lists (1911/1912)
  - growth-source waterfalls (389/392)
  - financial results (636), TKF payments (2305)
  - the AUM chart itself (334): AUM incl. pending and TKF + pre-rounded growth %; its forecast rows are not shown

This replaces the previous approach of looping over every card pinned to
dashboard 74 (non-deterministic) plus a few hardcoded standalone cards.
"""
import sys
import yaml
from pathlib import Path
from datetime import datetime

# Add common scripts to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / 'common' / 'scripts'))

from gateway_client import GatewayClient
from unit_prices import fetch_unit_prices


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
    1657: ('III s sissemakse tegijate arv YTD', 'scalar'),
    1911: ('II samba vahetusavalduste arv pangafondidesse sel vahetusperioodil', 'row'),
    1912: ('II samba vahetusavalduste arv lähtefondi järgi sel vahetusperioodil', 'row'),
    389:  ('Kasvuallikad eelmisel kuul (tegelik), M EUR', 'waterfall'),
    392:  ('Kasvuallikad YTD (tegelik), M EUR', 'waterfall'),
    636:  ('Tuleva finantstulemused', 'line'),
    2305: ('Täiendavasse Kogumisfondi tehtud maksed', 'line'),
    # Missiooni tabloo (mission_kpis.py): 1516 on kaardi 1518 allikas ilma 13 kuu
    # filtrita (2.3 libisev keskmine vajab 48 kuud); 2741 on sihikindluse trepp (2.4).
    1516: ('uute kogujate arv kuus, kogu ajalugu', 'table'),
    2741: ('sihikindluse trepp', 'table'),
}


# Vahetusperioodi eesmärgikaardid (nädalane seeria, VP 2026 sügis). Nende siht on
# kaardi nimes; hoiame sihi siin, et aruanne saaks näidata vahet sihini.
VP_GOAL_CARDS = {
    2631: {
        'key': 'goal_1',
        'title': 'III samba avaldusega tuleb kaasa ka II sammas',
        'target': 30.0,
        'target_label': '30%',
        'unit': 'pct',
    },
    2632: {
        'key': 'goal_2',
        'title': 'Sissemakse teinud OÜd',
        'target': 500,
        'target_label': '500',
        'unit': 'count',
    },
    2633: {
        'key': 'goal_3',
        'title': 'Lapsed, kes koguvad püsimaksega',
        'target': 400,
        'target_label': '400',
        'unit': 'count',
    },
    2634: {
        'key': 'goal_4',
        'title': 'Kõrge palgaga kogujad tõstavad II samba maksemäära',
        'target': 1350,
        'target_label': '1350',
        'unit': 'count',
    },
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

    client = GatewayClient()

    # Gateway annab välja ainult dashboardide kaarte: ütle kohe, mis puudu on.
    needed = {PRIMARY_CARD_ID, *SURVIVOR_CARDS, *VP_GOAL_CARDS}
    available = {card['id'] for card in client.cards()}
    missing = sorted(needed - available)
    if missing:
        print(f"  WARNING: not on the gateway's dashboards, add them in Metabase: {missing}")

    data = {
        'year': year,
        'month': month,
        'month_name': datetime(year, month, 1).strftime('%B'),
        'report_date': datetime.now().strftime('%Y-%m-%d'),
        'kpi_2578': {},
        'cards': {},
        'vp_goals': {},
        'unit_prices': [],
    }

    # Osakuhinna võrdlus avalikest allikatest (kaardi 2245 asemel), vt unit_prices.py.
    print("  Fetching unit prices (pensionikeskus, MSCI, Eurostat)...")
    data['unit_prices'] = fetch_unit_prices(year, month)

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

    # Vahetusperioodi eesmärgikaardid.
    for card_id, spec in VP_GOAL_CARDS.items():
        print(f"  Fetching [{card_id}] {spec['title']}...")
        try:
            results = client.execute_card(card_id)
            data['vp_goals'][spec['key']] = {
                'card_id': card_id,
                **spec,
                'data': results,
            }
            print(f"    -> {len(results)} rows")
        except Exception as e:
            print(f"    ERROR: {e}")
            data['vp_goals'][spec['key']] = {'card_id': card_id, **spec, 'error': str(e)}

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
