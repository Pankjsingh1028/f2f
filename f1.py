import json
import os
import pandas as pd
import requests
import gzip
import csv
from datetime import datetime, timezone
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

# Constants
UPSTOX_URL = "https://assets.upstox.com/market-quote/instruments/exchange/complete.json.gz"
COMPRESSED_FILE = "instruments.json.gz"
EXTRACTED_FILE = "instruments.json"
FILTERED_JSON_FILE = "instruments.json"
CSV_FILE = "instruments.csv"
OPTION_CHAIN_CSV = "oc-instru.csv"
OPTION_CHAIN_URL = 'https://api.upstox.com/v2/option/chain'

# Allowed segments
ALLOWED_SEGMENTS = {"NSE_EQ", "NSE_FO", "BSE_FO"}
ALLOWED_INSTRUMENT_TYPE = {"EQ", "FUT", "CE", "PE"}

# List of instruments to track
TRACKED_INSTRUMENTS = ['NSE_INDEX|Nifty 50', 'NSE_INDEX|Nifty Bank', 'BSE_INDEX|SENSEX']

def download_and_extract():
    """Downloads and extracts the Upstox instruments data."""
    try:
        # Download the file
        response = requests.get(UPSTOX_URL, stream=True)
        if response.status_code == 200:
            with open(COMPRESSED_FILE, "wb") as file:
                for chunk in response.iter_content(chunk_size=1024):
                    file.write(chunk)
            print(f"✅ Downloaded: {COMPRESSED_FILE}")
        else:
            print(f"❌ Failed to download instruments. Status Code: {response.status_code}")
            return
        
        # Extract the .gz file
        with gzip.open(COMPRESSED_FILE, "rb") as gz_file:
            with open(EXTRACTED_FILE, "wb") as json_file:
                json_file.write(gz_file.read())
        print(f"✅ Extracted: {EXTRACTED_FILE}")
        
        # Delete the .gz file
        os.remove(COMPRESSED_FILE)
        print(f"🗑️ Deleted: {COMPRESSED_FILE}")
    
    except Exception as e:
        print("❌ Error during download/extraction:", str(e))

def filter_and_save_json():
    """Filters the extracted JSON data for specific segments and saves to a new JSON file."""
    try:
        with open(EXTRACTED_FILE, "r", encoding="utf-8") as file:
            data = json.load(file)
        
        # Filter instruments
        filtered_data = [
            entry for entry in data
            if entry.get("segment") in ALLOWED_SEGMENTS and entry.get("instrument_type") in ALLOWED_INSTRUMENT_TYPE
        ]
        
        # Save filtered data to JSON
        with open(FILTERED_JSON_FILE, "w", encoding="utf-8") as file:
            json.dump(filtered_data, file)
        
        print(f"✅ Filtered data saved to {FILTERED_JSON_FILE}")
    
    except Exception as e:
        print("❌ Error filtering JSON data:", str(e))

def convert_to_csv():
    """Converts the filtered JSON data to CSV."""
    try:
        with open(FILTERED_JSON_FILE, "r", encoding="utf-8") as file:
            data = json.load(file)
        
        # Convert JSON to Pandas DataFrame
        df = pd.DataFrame(data)
        
        # Save as CSV
        df.to_csv(CSV_FILE, index=False)
        print(f"✅ Saved to {CSV_FILE}")
    
    except Exception as e:
        print("❌ Error converting to CSV:", str(e))

def get_nearest_expiry(instrument_key, instruments_data):
    """Finds the nearest expiry date for an instrument."""
    today_utc = datetime.now(timezone.utc).date()
    expiry_dates = []

    for instrument in instruments_data:
        if instrument.get("underlying_key") == instrument_key and "expiry" in instrument and instrument["expiry"]:
            expiry_timestamp = int(instrument["expiry"])  
            expiry_date_utc = datetime.fromtimestamp(expiry_timestamp / 1000, tz=timezone.utc).date()
            expiry_dates.append(expiry_date_utc)

    return min(expiry_dates, key=lambda x: (x - today_utc).days if (x - today_utc).days >= 0 else float('inf')) if expiry_dates else None


def fetch_option_chain():
    """Fetches option chain data and saves it to CSV."""
    if not os.path.exists(EXTRACTED_FILE):
        print("❌ Error: instruments.json not found. Run the download step first.")
        return

    with open(EXTRACTED_FILE, "r") as file:
        instruments_data = json.load(file)

    data_to_write = []
    
    for instrument in TRACKED_INSTRUMENTS:
        expiry_date = get_nearest_expiry(instrument, instruments_data)
        if not expiry_date:
            print(f"⚠️ No valid expiry date found for {instrument}")
            continue

        params = {'instrument_key': instrument, 'expiry_date': str(expiry_date)}
        headers = {'Accept': 'application/json', 'Authorization': f'Bearer {ACCESS_TOKEN}'}

        response = requests.get(OPTION_CHAIN_URL, params=params, headers=headers)

        if response.status_code == 200:
            data = response.json()
            if data["status"] == "success":
                options = data.get("data", [])
                for option in options:
                    strike_price = option.get("strike_price")
                    call_option = option.get("call_options", {}).get("instrument_key")
                    put_option = option.get("put_options", {}).get("instrument_key")

                    if call_option:
                        data_to_write.append([instrument, expiry_date, strike_price, "Call", call_option])
                    if put_option:
                        data_to_write.append([instrument, expiry_date, strike_price, "Put", put_option])
            else:
                print(f"⚠️ API response not successful for {instrument}", data)
        else:
            print(f"❌ Error fetching data for {instrument}: {response.status_code}, {response.text}")

    # Save to CSV
    with open(OPTION_CHAIN_CSV, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Instrument", "Expiry Date", "Strike Price", "Option Type", "Instrument Key"])
        writer.writerows(data_to_write)

    print(f"✅ Option chain data exported to {OPTION_CHAIN_CSV}")


# Run the complete process
download_and_extract()
filter_and_save_json()
convert_to_csv()
fetch_option_chain()
