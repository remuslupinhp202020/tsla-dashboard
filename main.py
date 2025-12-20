import os
import json
import yfinance as yf
import pandas as pd
import gspread

# --- CONFIGURATION ---
TICKER = 'TSLA'
START_DATE = '2020-01-01'
SHEET_NAME = 'TSLA_Dashboard_Data'

def update_data():
    print("1. Authenticating with Google Service Account...")
    
    # Load the credentials from the GitHub Secret environment variable
    json_creds = os.environ.get("GCP_SERVICE_ACCOUNT")
    
    if not json_creds:
        raise ValueError("Error: GCP_SERVICE_ACCOUNT secret not found.")

    creds_dict = json.loads(json_creds)
    gc = gspread.service_account_from_dict(creds_dict)

    print(f"2. Downloading data for {TICKER}...")
    df = yf.download(TICKER, start=START_DATE, progress=False)
    
    # Fix MultiIndex if present
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    
    df = df[['Close']].reset_index()

    # Logic: Monthly Lows
    df['YearMonth'] = df['Date'].dt.to_period('M')
    monthly_lows = df.groupby('YearMonth')['Close'].transform('min')
    df['Is_Low'] = df['Close'] == monthly_lows

    # Formatting
    df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
    df['Close'] = df['Close'].round(2)
    df['Is_Low'] = df['Is_Low'].astype(str).str.upper()

    final_data = df[['Date', 'Close', 'Is_Low']]
    print(f"   Data processed. Rows: {len(final_data)}")

    # Upload
    print("3. Uploading to Google Sheets...")
    sh = gc.open(SHEET_NAME)
    worksheet = sh.sheet1
    worksheet.clear()
    worksheet.update('A1', [final_data.columns.values.tolist()] + final_data.values.tolist())
    print("SUCCESS: Sheet updated.")

if __name__ == "__main__":
    update_data()
