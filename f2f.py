import requests
import pandas as pd
from urllib.parse import quote
from dotenv import load_dotenv
import os
from datetime import datetime
import json

# Load environment variables
load_dotenv()
API_KEY = os.getenv("UPSTOX_API_KEY")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

# Upstox API configuration
BASE_URL = "https://api.upstox.com/v2/market-quote/ltp"

# Headers for API request
headers = {
    'Accept': 'application/json',
    'Authorization': f'Bearer {ACCESS_TOKEN}'
}

def load_stocks_from_csv(filename='futurestockslist.csv'):
    try:
        df = pd.read_csv(filename)
        # Assuming the CSV has a column named 'Symbol' containing stock symbols
        stocks_list = df['underlying_symbol'].tolist()
        return stocks_list
    except Exception as e:
        print(f"Error reading CSV file {filename}: {e}")
        return []

def fetch_ltp(instrument_keys):
    try:
        encoded_keys = ','.join(quote(key) for key in instrument_keys)
        url = f"{BASE_URL}?instrument_key={encoded_keys}"
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        ltp_data = {}
        if data.get('status') == 'success':
            for _, info in data.get('data', {}).items():
                instrument_key = info.get('instrument_token')
                if instrument_key:
                    ltp_data[instrument_key] = info.get('last_price', None)
        return ltp_data
    except Exception as e:
        print(f"Error fetching LTP: {e}")
        return {}

def load_instrument_data():
    try:
        with open('instruments.json', 'r') as f:
            instrument_data = json.load(f)
        return instrument_data
    except Exception as e:
        print(f"Error loading instruments.json: {e}")
        return []

def get_instrument_keys(symbol, instrument_data):
    equity_key = None
    futures = []
    for instrument in instrument_data:
        if instrument.get('segment') == 'NSE_EQ' and instrument.get('trading_symbol') == symbol:
            equity_key = instrument.get('instrument_key')
        elif (instrument.get('segment') == 'NSE_FO' and 
              instrument.get('underlying_symbol') == symbol and 
              instrument.get('instrument_type') == 'FUT'):
            futures.append({
                'instrument_key': instrument.get('instrument_key'),
                'expiry': instrument.get('expiry', 0),
                'trading_symbol': instrument.get('trading_symbol')
            })
    # Sort futures by expiry
    futures.sort(key=lambda x: x['expiry'])
    return equity_key, futures

def chunk_list(lst, chunk_size):
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]

def export_to_csv(data, filename='f2f_ltp_data.csv'):
    try:
        df = pd.DataFrame(data)
        df.to_csv(filename, index=False)
        print(f"Data exported to {filename}")
    except Exception as e:
        print(f"Error exporting to CSV: {e}")

def main():
    # Load stock symbols from CSV
    stocks_list = load_stocks_from_csv()
    if not stocks_list:
        print("No stock data loaded from CSV. Exiting.")
        return

    # Load instrument data
    instrument_data = load_instrument_data()
    if not instrument_data:
        print("No instrument data loaded. Exiting.")
        return

    # Prepare data structures
    results = []
    all_instrument_keys = []

    # Step 1: Gather instrument keys
    for symbol in stocks_list:
        equity_key, futures = get_instrument_keys(symbol, instrument_data)
        if equity_key:
            all_instrument_keys.append(equity_key)
        for future in futures:
            all_instrument_keys.append(future['instrument_key'])

    # Step 2: Fetch LTP in chunks
    ltp_data = {}
    for chunk in chunk_list(all_instrument_keys, 500):
        ltp_data.update(fetch_ltp(chunk))

    # Step 3: Process and organize results
    for symbol in stocks_list:
        equity_key, futures = get_instrument_keys(symbol, instrument_data)
        eq_ltp = ltp_data.get(equity_key, None) if equity_key else None
        near_ltp = ltp_data.get(futures[0]['instrument_key'], None) if len(futures) > 0 else None
        next_ltp = ltp_data.get(futures[1]['instrument_key'], None) if len(futures) > 1 else None
        far_ltp = ltp_data.get(futures[2]['instrument_key'], None) if len(futures) > 2 else None
        results.append({
            'Symbol': symbol,
            'EQ': eq_ltp,
            'Near Future': near_ltp,
            'Next Future': next_ltp,
            'Far Future': far_ltp
        })

    # Step 4: Display results
    df = pd.DataFrame(results)
    print(df.to_string(index=False))

    export_to_csv(results)

if __name__ == "__main__":
    main()