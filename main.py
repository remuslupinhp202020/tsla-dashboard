import os
import json
import time
import requests
import pandas as pd
import yfinance as yf
import gspread

# --- CONFIGURATION ---
TICKER = 'TSLA'
SHEET_NAME = 'TSLA_Dashboard_Data'

def get_session():
    """Create a browser-like session to avoid 403/404 errors"""
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": "https://finance.yahoo.com/"
    })
    return session

def fetch_data_robust(ticker):
    """Try multiple methods to get data"""
    print(f"--- Attempting to download {ticker} ---")
    session = get_session()
    
    # Method 1: Ticker.history (Best for single stocks)
    try:
        print("Method 1: yf.Ticker().history()...")
        dat = yf.Ticker(ticker, session=session)
        df = dat.history(period="5y") # Get last 5 years
        
        if not df.empty:
            print("   Success!")
            return df
    except Exception as e:
        print(f"   Method 1 failed: {e}")

    # Method 2: yf.download (Fallback)
    time.sleep(2) # Wait a bit
    try:
        print("Method 2: yf.download()...")
        df = yf.download(ticker, period="5y", progress=False, session=session)
        if not df.empty:
            print("   Success!")
            return df
    except Exception as e:
        print(f"   Method 2 failed: {e}")
    
    return pd.DataFrame() # Return empty if all failed

def update_data():
    # 1. AUTHENTICATE
    print("1. Authenticating with Google Sheets...")
    json_creds = os.environ.get("GCP_SERVICE_ACCOUNT")
    if not json_creds:
        raise ValueError("Error: GCP_SERVICE_ACCOUNT secret not found.")
    
    creds_dict = json.loads(json_creds)
    gc = gspread.service_account_from_dict(creds_dict)

    # 2. DOWNLOAD DATA
    df = fetch_data_robust(TICKER)
    
    if df.empty:
        raise Exception("CRITICAL ERROR: Yahoo Finance returned no data. IP might be blocked.")

    # 3. PROCESS DATA
    print("3. Processing Data...")
    
    # Reset index to make Date a column
    df = df.reset_index()
    
    # Fix: yfinance sometimes returns timezone-aware dates. Convert to simple dates.
    if 'Date' in df.columns:
        df['Date'] = pd.to_datetime(df['Date']).dt.tz_localize(None)

    # Ensure we have the Close column (Handle MultiIndex case)
    if isinstance(df.columns, pd.MultiIndex):
        # Flatten columns if they are like ('Close', 'TSLA')
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    
    if 'Close' not in df.columns:
         # Fallback for weird column names
         print(f"Columns found: {df.columns}")
         raise Exception("Missing 'Close' column in data.")

    df = df[['Date', 'Close']].copy()

    # Calculate Monthly Lows
    df['YearMonth'] = df['Date'].dt.to_period('M')
    monthly_lows = df.groupby('YearMonth')['Close'].transform('min')
    df['Is_Low'] = df['Close'] == monthly_lows

    # Format for Sheets
    df['Date'] = df['Date'].dt.strftime('%Y-%m-%d')
    df['Close'] = df['Close'].round(2)
    df['Is_Low'] = df['Is_Low'].astype(str).str.upper()

    final_data = df[['Date', 'Close', 'Is_Low']]
    
    # Sort Newest First (Optional, but looks nice in Sheets)
    final_data = final_data.sort_values(by='Date', ascending=False)

    print(f"   Rows to upload: {len(final_data)}")
    print(f"   Latest Date: {final_data.iloc[0]['Date']}")

    # 4. UPLOAD
    print("4. Uploading to Google Sheets...")
    sh = gc.open(SHEET_NAME)
    worksheet = sh.sheet1
    worksheet.clear()
    worksheet.update('A1', [final_data.columns.values.tolist()] + final_data.values.tolist())
    print("SUCCESS: Dashboard Updated.")

if __name__ == "__main__":
    update_data()
