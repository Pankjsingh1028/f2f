import json
from datetime import datetime
import pytz
import pandas as pd
import json
import csv

def convert_date_to_timestamp(date_str):
    # Convert date string (DD-MM-YYYY) to timestamp in milliseconds
    dt = datetime.strptime(date_str, "%d-%m-%Y")
    dt = dt.replace(tzinfo=pytz.UTC)
    return int(dt.timestamp() * 1000)

def find_nearest_expiry(file_path, segment, instrument_type, underlying_symbol):
    # Read instruments.json
    try:
        with open(file_path, 'r') as file:
            instruments = json.load(file)
    except FileNotFoundError:
        print(f"Error: {file_path} not found")
        return None
    except json.JSONDecodeError:
        print("Error: Invalid JSON format in instruments.json")
        return None

    # Filter for RELIANCE futures
    reliance_futures = [
        instrument for instrument in instruments
        if instrument.get('segment') == segment
        and instrument.get('instrument_type') == instrument_type
        and instrument.get('underlying_symbol') == underlying_symbol
    ]

    if not reliance_futures:
        print(f"No futures found for {underlying_symbol} in {segment} with instrument_type {instrument_type}")
        return None

    # Find earliest expiry
    current_timestamp = int(datetime.now(pytz.UTC).timestamp() * 1000)
    valid_expiries = [inst['expiry'] for inst in reliance_futures if inst['expiry'] >= current_timestamp]
    if not valid_expiries:
        print(f"No future expiries found for {underlying_symbol}")
        return None

    nearest_expiry = min(valid_expiries)
    return nearest_expiry

def fetch_futures_stocks(file_path, segment, instrument_type, expiry, skip_symbols):
    # Read instruments.json
    try:
        with open(file_path, 'r') as file:
            instruments = json.load(file)
    except FileNotFoundError:
        print(f"Error: {file_path} not found")
        return []
    except json.JSONDecodeError:
        print("Error: Invalid JSON format in instruments.json")
        return []

    # Filter for futures with the given expiry
    matching_stocks = [
        instrument for instrument in instruments
        if instrument.get('segment') == segment
        and instrument.get('instrument_type') == instrument_type
        and instrument.get('expiry') == expiry
    ]

    return matching_stocks

def export_to_csv(symbols, output_file):
    # Export list of underlying symbols to CSV
    try:
        with open(output_file, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['underlying_symbol'])  # Write header
            for symbol in sorted(set(symbols)):  # Sort and remove duplicates
                writer.writerow([symbol])
        print(f"Data exported to {output_file}")
    except IOError as e:
        print(f"Error writing to {output_file}: {e}")

def main():
    file_path = 'instruments.json'
    segment = 'NSE_FO'
    instrument_type = 'FUT'
    underlying_symbol = 'RELIANCE'
    skip_symbols = {'NIFTY', 'NIFTYNXT50', 'MIDCPNIFTY', 'FINNIFTY', 'BANKNIFTY'}
    output_file = 'futurestockslist.csv'

    # Step 1: Find nearest expiry for RELIANCE futures
    nearest_expiry = find_nearest_expiry(file_path, segment, instrument_type, underlying_symbol)

    if nearest_expiry is None:
        print("Using assumed expiry of 24-04-2025 (1745519399000) due to lack of FUT data")
        nearest_expiry = convert_date_to_timestamp('24-04-2025')

    # Step 2: Fetch all futures stocks for the expiry
    stocks = fetch_futures_stocks(file_path, segment, instrument_type, nearest_expiry, skip_symbols)
    
    if stocks:
        print(f"Found {len(stocks)} futures stocks for expiry {nearest_expiry}.")
        print("List of underlying symbols:")
        underlying_symbols = [stock.get('underlying_symbol') for stock in stocks
                             if stock.get('underlying_symbol') not in skip_symbols]
        for symbol in sorted(set(underlying_symbols)):  # Sort and remove duplicates
            print(symbol)
        export_to_csv(underlying_symbols, output_file)
    else:
        print(f"No futures stocks found for expiry {nearest_expiry} in {segment}.")

if __name__ == "__main__":
    main()