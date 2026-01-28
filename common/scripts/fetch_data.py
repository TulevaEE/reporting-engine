import os
import json
import gspread
import yaml
from pathlib import Path


# Sheet configuration
SHEET_ID = "1VAQpO7DM1rM_3xJ5tTRSQV-98FUh8VGWMhxKq5XQNq4"


def get_gspread_client():
    """Authenticate and return a gspread client."""
    service_account_info = json.loads(os.environ['GCP_SERVICE_ACCOUNT'])
    return gspread.service_account_from_dict(service_account_info)


def test_connection():
    print("Attempting to connect to Google Sheets...")

    try:
        gc = get_gspread_client()
        print("Authentication successful")
    except KeyError:
        print("ERROR: Could not find 'GCP_SERVICE_ACCOUNT'. Did you add it to GitHub Secrets?")
        return
    except Exception as e:
        print(f"ERROR during auth: {e}")
        return

    try:
        sh = gc.open_by_key(SHEET_ID)
        print(f"Connected to Sheet: '{sh.title}'")

        val = sh.sheet1.get('A1')
        print(f"SUCCESS! Value in A1 is: {val}")

    except Exception as e:
        print(f"ERROR connecting to sheet: {e}")
        print("HINT: Did you share the sheet with the robot email address?")


def fetch_annual_report_data(year: int) -> dict:
    """
    Fetch annual report data from Google Sheets Named Ranges.

    Args:
        year: The reporting year (e.g., 2025)

    Returns:
        Dictionary containing financial data from named ranges.
    """
    print(f"Fetching annual report data for {year}...")

    gc = get_gspread_client()
    sh = gc.open_by_key(SHEET_ID)

    # Named ranges to fetch
    named_ranges = ['Total_NAV', 'Net_Profit', 'Participant_Count']
    data = {'year': year}

    for range_name in named_ranges:
        try:
            # Get the named range value
            values = sh.values_get(range_name)
            # Extract the actual value (first cell)
            if values.get('values'):
                raw_value = values['values'][0][0]
                # Try to convert to number if possible
                try:
                    if '.' in str(raw_value):
                        data[range_name] = float(raw_value.replace(',', ''))
                    else:
                        data[range_name] = int(str(raw_value).replace(',', ''))
                except (ValueError, AttributeError):
                    data[range_name] = raw_value
            else:
                data[range_name] = None
                print(f"WARNING: Named range '{range_name}' is empty")
        except Exception as e:
            print(f"ERROR fetching '{range_name}': {e}")
            data[range_name] = None

    print(f"Fetched data: {data}")
    return data


def save_annual_report_data(year: int):
    """
    Fetch annual report data and save to YAML file.

    Args:
        year: The reporting year (e.g., 2025)
    """
    data = fetch_annual_report_data(year)

    # Determine output path
    script_dir = Path(__file__).parent.parent.parent
    output_dir = script_dir / 'reports' / 'annual' / str(year) / 'data'
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / 'financials.yaml'

    # Save to YAML
    with open(output_file, 'w') as f:
        yaml.dump(data, f, default_flow_style=False, allow_unicode=True)

    print(f"Saved financial data to: {output_file}")
    return output_file


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == 'fetch':
        year = int(sys.argv[2]) if len(sys.argv) > 2 else 2025
        save_annual_report_data(year)
    else:
        test_connection()
