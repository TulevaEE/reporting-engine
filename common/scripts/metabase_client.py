"""
Metabase API client for fetching data from dashboards and cards.
"""
import os
import yaml
import requests
from pathlib import Path
from typing import Any, Optional


class MetabaseClient:
    """Client for interacting with Metabase API."""

    def __init__(self, base_url: str = None, api_key: str = None):
        """
        Initialize Metabase client.

        Args:
            base_url: Metabase instance URL. Defaults to config file.
            api_key: API key for authentication. Defaults to env var.
        """
        config = self._load_config()

        self.base_url = (base_url or config.get('base_url', '')).rstrip('/')
        self.api_key = api_key or os.environ.get(config.get('auth_env_var', 'METABASE_API_KEY'))

        if not self.api_key:
            raise ValueError(
                f"Metabase API key not found. Set the {config.get('auth_env_var', 'METABASE_API_KEY')} "
                "environment variable."
            )

    def _load_config(self) -> dict:
        """Load configuration from metabase.yaml."""
        config_path = Path(__file__).parent.parent / 'config' / 'metabase.yaml'
        if config_path.exists():
            with open(config_path, 'r') as f:
                return yaml.safe_load(f)
        return {}

    def _get_headers(self) -> dict:
        """Get request headers with API key authentication."""
        return {
            'x-api-key': self.api_key,
            'Content-Type': 'application/json'
        }

    def _request(self, method: str, endpoint: str, **kwargs) -> Any:
        """
        Make an authenticated request to Metabase API.

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint path
            **kwargs: Additional arguments for requests

        Returns:
            JSON response data
        """
        url = f"{self.base_url}/api/{endpoint.lstrip('/')}"
        response = requests.request(
            method,
            url,
            headers=self._get_headers(),
            **kwargs
        )
        response.raise_for_status()
        return response.json()

    def get_dashboard(self, dashboard_id: int) -> dict:
        """
        Fetch dashboard metadata including all cards.

        Args:
            dashboard_id: The dashboard ID

        Returns:
            Dashboard data including cards/dashcards
        """
        return self._request('GET', f'dashboard/{dashboard_id}')

    def execute_card(self, card_id: int, parameters: dict = None) -> dict:
        """
        Execute a saved question (card) and return results.

        Args:
            card_id: The card/question ID
            parameters: Optional parameters for the query

        Returns:
            Query results with data rows and columns
        """
        data = {}
        if parameters:
            data['parameters'] = parameters

        return self._request('POST', f'card/{card_id}/query/json', json=data)

    def get_card(self, card_id: int) -> dict:
        """
        Get card metadata (without executing).

        Args:
            card_id: The card/question ID

        Returns:
            Card metadata
        """
        return self._request('GET', f'card/{card_id}')

    def get_single_value(self, card_id: int, column: str = None) -> Any:
        """
        Execute a card and return a single scalar value.

        Useful for KPI cards that return one number.

        Args:
            card_id: The card/question ID
            column: Column name to extract. If None, uses first column.

        Returns:
            The scalar value from the first row
        """
        results = self.execute_card(card_id)

        if not results or len(results) == 0:
            return None

        row = results[0]

        if column:
            return row.get(column)

        # Return first value if no column specified
        return list(row.values())[0] if row else None

    def get_dashboard_cards(self, dashboard_id: int) -> list:
        """
        Get all cards from a dashboard with their names and IDs.

        Args:
            dashboard_id: The dashboard ID

        Returns:
            List of dicts with card_id and name
        """
        dashboard = self.get_dashboard(dashboard_id)
        cards = []

        for dashcard in dashboard.get('dashcards', []):
            card = dashcard.get('card')
            if card and card.get('id'):
                cards.append({
                    'card_id': card['id'],
                    'name': card.get('name', 'Unnamed'),
                    'description': card.get('description', ''),
                    'display': card.get('display', 'table')
                })

        return cards


def test_connection():
    """Test Metabase API connection."""
    print("Testing Metabase API connection...")

    try:
        client = MetabaseClient()
        print(f"Connected to: {client.base_url}")
    except ValueError as e:
        print(f"ERROR: {e}")
        return False

    try:
        # Load dashboard ID from config
        config_path = Path(__file__).parent.parent / 'config' / 'metabase.yaml'
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)

        dashboard_id = config.get('dashboard_id', 74)
        dashboard = client.get_dashboard(dashboard_id)
        print(f"SUCCESS! Connected to dashboard: {dashboard.get('name', 'Unknown')}")

        # List cards
        cards = client.get_dashboard_cards(dashboard_id)
        print(f"\nFound {len(cards)} cards:")
        for card in cards:
            print(f"  - [{card['card_id']}] {card['name']} ({card['display']})")

        return True

    except requests.exceptions.HTTPError as e:
        print(f"ERROR: HTTP {e.response.status_code}")
        if e.response.status_code == 401:
            print("HINT: Check that your METABASE_API_KEY is correct")
        elif e.response.status_code == 404:
            print("HINT: Check that the dashboard ID exists")
        return False
    except Exception as e:
        print(f"ERROR: {e}")
        return False


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == 'test':
        success = test_connection()
        sys.exit(0 if success else 1)
    else:
        print("Usage: python metabase_client.py test")
