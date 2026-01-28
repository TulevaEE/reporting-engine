import os
import json
import gspread
import yaml

def test_connection():
    print("🔌 Attempting to connect to Google Sheets...")
    
    # 1. Authenticate using the Secret you saved in GitHub Settings
    try:
        service_account_info = json.loads(os.environ['GCP_SERVICE_ACCOUNT'])
        gc = gspread.service_account_from_dict(service_account_info)
        print("✅ Authentication successful (Robot login worked)")
    except KeyError:
        print("❌ ERROR: Could not find 'GCP_SERVICE_ACCOUNT'. Did you add it to GitHub Secrets?")
        return
    except Exception as e:
        print(f"❌ ERROR during auth: {e}")
        return

    # 2. Open the Sheet (We will ask for the ID in a second)
    # REPLACE THIS WITH YOUR ACTUAL SHEET ID
    SHEET_ID = "1VAQpO7DM1rM_3xJ5tTRSQV-98FUh8VGWMhxKq5XQNq4" 
    
    try:
        sh = gc.open_by_key(SHEET_ID)
        print(f"✅ Connected to Sheet: '{sh.title}'")
        
        # 3. Read A1 to prove we can see data
        val = sh.sheet1.get('A1')
        print(f"🎉 SUCCESS! Value in A1 is: {val}")
        
    except Exception as e:
        print(f"❌ ERROR connecting to sheet: {e}")
        print("💡 HINT: Did you share the sheet with the robot email address?")

if __name__ == "__main__":
    test_connection()