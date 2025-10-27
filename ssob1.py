import os
import json
import requests
import math
import datetime
import pandas as pd
import dash
from dash import html, dcc, Input, Output, dash_table
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
import time
import pytz
from uuid import uuid4

# Load environment variables
load_dotenv()
API_KEY = os.getenv("UPSTOX_API_KEY")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

if not API_KEY or not ACCESS_TOKEN:
    raise ValueError("Missing API credentials in .env file!")

INSTRUMENTS_FILE = "instruments.json"

# Futures filtering functions
def find_nearest_expiry(file_path, segment, instrument_type, underlying_symbol):
    try:
        with open(file_path, 'r') as file:
            instruments = json.load(file)
    except FileNotFoundError:
        print(f"Error: {file_path} not found")
        return None
    except json.JSONDecodeError:
        print("Error: Invalid JSON format in instruments.json")
        return None

    reliance_futures = [
        instrument for instrument in instruments
        if instrument.get('segment') == segment
        and instrument.get('instrument_type') == instrument_type
        and instrument.get('underlying_symbol') == underlying_symbol
    ]

    if not reliance_futures:
        print(f"No futures found for {underlying_symbol} in {segment} with instrument_type {instrument_type}")
        return None

    current_timestamp = int(datetime.datetime.now(pytz.UTC).timestamp() * 1000)
    valid_expiries = [
        inst['expiry'] for inst in reliance_futures
        if inst['expiry'] >= current_timestamp
    ]
    if not valid_expiries:
        print(f"No future expiries found for {underlying_symbol}")
        return None

    nearest_expiry_ms = min(valid_expiries)
    expiry_dt = datetime.datetime.fromtimestamp(nearest_expiry_ms / 1000, tz=pytz.UTC)
    return expiry_dt.strftime("%Y-%m-%d")

def fetch_expiry_dates(file_path, segment, instrument_type, skip_symbols):
    try:
        with open(file_path, 'r') as file:
            instruments = json.load(file)
    except FileNotFoundError:
        print(f"Error: {file_path} not found")
        return []
    except json.JSONDecodeError:
        print("Error: Invalid JSON format in instruments.json")
        return []

    current_timestamp = int(datetime.datetime.now(pytz.UTC).timestamp() * 1000)
    expiry_instruments = [
        inst for inst in instruments
        if inst.get('segment') == segment
        and inst.get('instrument_type') == instrument_type
        and inst.get('underlying_symbol') not in skip_symbols
        and not inst.get('underlying_symbol', '').endswith('NSETEST')
        and inst['expiry'] >= current_timestamp
    ]
    expiries = []
    for inst in expiry_instruments:
        expiry_ms = inst['expiry']
        expiry_date = datetime.datetime.fromtimestamp(expiry_ms / 1000, tz=pytz.UTC).strftime("%Y-%m-%d")
        expiries.append(expiry_date)
    unique_expiries = sorted(set(expiries))
    if not unique_expiries:
        print("No valid expiries found")
    return unique_expiries

def fetch_futures_stocks(file_path, segment, instrument_type, expiry_date, skip_symbols):
    try:
        with open(file_path, 'r') as file:
            instruments = json.load(file)
    except FileNotFoundError:
        print(f"Error: {file_path} not found")
        return []
    except json.JSONDecodeError:
        print("Error: Invalid JSON format in instruments.json")
        return []

    target_date = datetime.datetime.strptime(expiry_date, "%Y-%m-%d").replace(tzinfo=pytz.UTC)
    matching_stocks = []
    for instrument in instruments:
        if (instrument.get('segment') == segment and
            instrument.get('instrument_type') == instrument_type and
            instrument.get('underlying_symbol') not in skip_symbols):
            expiry_ms = instrument.get('expiry')
            if expiry_ms:
                expiry_date = datetime.datetime.fromtimestamp(expiry_ms / 1000, tz=pytz.UTC).strftime("%Y-%m-%d")
                if expiry_date == target_date.strftime("%Y-%m-%d"):
                    matching_stocks.append(instrument)

    stocks = sorted(set(instrument['underlying_symbol'] for instrument in matching_stocks))
    if not stocks:
        print(f"No stocks found for expiry {expiry_date} in segment {segment}, instrument_type {instrument_type}")
    else:
        print(f"Found stocks for expiry {expiry_date}: {stocks}")
    return stocks

# Initialize instruments and expiry dates
segment = 'NSE_FO'
instrument_type = 'FUT'
underlying_symbol = 'RELIANCE'
skip_symbols = {'NIFTY', 'NIFTYNXT50', 'MIDCPNIFTY', 'FINNIFTY', 'BANKNIFTY'}
default_expiry = find_nearest_expiry(INSTRUMENTS_FILE, segment, instrument_type, underlying_symbol)
EXPIRY_DATES = fetch_expiry_dates(INSTRUMENTS_FILE, segment, instrument_type, skip_symbols)
if not EXPIRY_DATES:
    print("No valid expiries found, using default 2025-05-29")
    EXPIRY_DATES = ["2025-05-29"]
if default_expiry is None:
    print("Using assumed expiry of 2025-05-29")
    default_expiry = "2025-05-29"

with open(INSTRUMENTS_FILE, "r") as file:
    instruments = json.load(file)

instrument_dict = {i["trading_symbol"]: f"NSE_EQ|{i['isin']}" for i in instruments if i.get("segment") == "NSE_EQ"}
option_dict = {
    (
        i["underlying_symbol"],
        i["strike_price"],
        i["instrument_type"],
        datetime.datetime.fromtimestamp(i["expiry"] / 1000, tz=pytz.UTC).strftime("%Y-%m-%d")
    ): {
        "instrument_key": i["instrument_key"],
        "lot_size": i["lot_size"]
    }
    for i in instruments
    if all(k in i for k in ("underlying_symbol", "strike_price", "instrument_type", "expiry"))
}


# Use requests.Session for connection pooling
session = requests.Session()
session.headers.update({"Accept": "application/json", "Authorization": f"Bearer {ACCESS_TOKEN}"})

def fetch_option_chain(instrument_key, expiry_date):
    url = "https://api.upstox.com/v2/option/chain"
    params = {"instrument_key": instrument_key, "expiry_date": expiry_date}
    try:
        response = session.get(url, params=params, timeout=5)
        response.raise_for_status()
        return response.json().get("data", None)
    except requests.RequestException:
        return None

def find_atm_and_iv(option_chain, underlying_price):
    nearest_strike = min(option_chain, key=lambda x: abs(x["strike_price"] - underlying_price))["strike_price"]
    atm_call = next((o["call_options"] for o in option_chain if o["strike_price"] == nearest_strike), None)
    atm_put = next((o["put_options"] for o in option_chain if o["strike_price"] == nearest_strike), None)

    if atm_call and atm_put:
        iv = (atm_call["option_greeks"]["iv"] + atm_put["option_greeks"]["iv"]) / 2
        return nearest_strike, iv
    return nearest_strike, None

def calculate_2sd_range(underlying_price, iv, days_to_expiry, sd_multiplier):
    if iv is None:
        return underlying_price, underlying_price
    sd = underlying_price * (iv / 100) * math.sqrt(days_to_expiry / 365)
    return underlying_price - sd_multiplier * sd, underlying_price + sd_multiplier * sd

def find_nearest_strikes(option_chain, lower_bound, upper_bound):
    strikes = sorted(set(o["strike_price"] for o in option_chain))
    return min(strikes, key=lambda x: abs(x - lower_bound)), min(strikes, key=lambda x: abs(x - upper_bound))

def fetch_margin(ce_key, pe_key, lot_size):
    url = "https://api.upstox.com/v2/charges/margin"
    data = {
        "instruments": [
            {"instrument_key": ce_key, "quantity": lot_size, "transaction_type": "SELL", "product": "D"},
            {"instrument_key": pe_key, "quantity": lot_size, "transaction_type": "SELL", "product": "D"}
        ]
    }
    try:
        response = session.post(url, json=data, timeout=5)
        response.raise_for_status()
        return response.json().get("data", {}).get("final_margin", "N/A")
    except requests.RequestException:
        return "N/A"

def get_option_premiums(option_chain, lower_strike, upper_strike):
    ce_premium = next((o["call_options"]["market_data"].get("ltp") for o in option_chain if o["strike_price"] == upper_strike), None)
    pe_premium = next((o["put_options"]["market_data"].get("ltp") for o in option_chain if o["strike_price"] == lower_strike), None)
    return {"CE_Premium": ce_premium, "PE_Premium": pe_premium}

def process_stock(stock_symbol, expiry_date, sd_multiplier):
    expiry_dt = datetime.datetime.strptime(expiry_date, "%Y-%m-%d")
    days_to_expiry = (expiry_dt - datetime.datetime.today()).days

    instrument_key = instrument_dict.get(stock_symbol)
    if not instrument_key:
        return None

    option_chain = fetch_option_chain(instrument_key, expiry_date)
    if not option_chain:
        return None

    underlying_price = option_chain[0]["underlying_spot_price"]
    atm_strike, avg_iv = find_atm_and_iv(option_chain, underlying_price)
    lower_bound, upper_bound = calculate_2sd_range(underlying_price, avg_iv, days_to_expiry, sd_multiplier)
    iv_value = float(avg_iv) if isinstance(avg_iv, (int, float)) else None
    lower_strike, upper_strike = find_nearest_strikes(option_chain, lower_bound, upper_bound)

    # Use expiry-aware keys
    ce_key_tuple = (stock_symbol, upper_strike, "CE", expiry_date)
    pe_key_tuple = (stock_symbol, lower_strike, "PE", expiry_date)

    ce_data = option_dict.get(ce_key_tuple)
    pe_data = option_dict.get(pe_key_tuple)

    ce_key = ce_data["instrument_key"] if ce_data else None
    pe_key = pe_data["instrument_key"] if pe_data else None
    lot_size = ce_data["lot_size"] if ce_data else (pe_data["lot_size"] if pe_data else "N/A")

    option_premiums = get_option_premiums(option_chain, lower_strike, upper_strike)
    margin = fetch_margin(ce_key, pe_key, lot_size)

    try:
        margin_value = float(margin) if margin not in ["N/A", None] else None
    except ValueError:
        margin_value = None

    net_premium = sum(filter(None, [option_premiums["CE_Premium"], option_premiums["PE_Premium"]]))
    theta = round(net_premium * lot_size, 2) if isinstance(lot_size, (int, float)) else "N/A"
    gross_returns = round((theta / float(margin_value)) * 100, 2) if isinstance(margin_value, (int, float)) else "N/A"

    return {
        "Stock": stock_symbol,
        "Spot Price": round(underlying_price, 2) if isinstance(underlying_price, (int, float)) else "N/A",
        "ATM Strike": atm_strike,
        "ATM IV": round(iv_value, 2) if iv_value is not None else "N/A",
        "Strikes": f"({lower_strike},{upper_strike})",
        "Premiums": f"({option_premiums['PE_Premium']}, {option_premiums['CE_Premium']})",
        "Net Premium": round(net_premium, 2) if isinstance(net_premium, (int, float)) else "N/A",
        "Lot Size": lot_size,
        "Theta": theta,
        "Margin": round(margin_value, 2) if margin_value else "N/A",
        "Returns": gross_returns,
    }


def process_stock_batch(stock_batch, expiry_date, sd_multiplier):
    with ThreadPoolExecutor(max_workers=20) as executor:
        results = list(filter(None, executor.map(lambda stock: process_stock(stock, expiry_date, sd_multiplier), stock_batch)))
    return results

app = dash.Dash(__name__)
app.layout = html.Div([
    html.H1("Stock Option Data"),
    html.Div([
        html.Label("Expiry Date:"),
        dcc.Dropdown(
            id='expiry-dropdown',
            options=[{'label': date, 'value': date} for date in EXPIRY_DATES],
            value=default_expiry,
            style={'width': '140px'},
            clearable=False
        ),
        html.Label("Standard Deviation:"),
        dcc.Input(
            id='sd-input',
            type='number',
            value=2,
            min=0.1,
            max=5.0,
            step=0.1,
            style={'width': '50px'}
        ),
        html.Button('Fetch Data', id='fetch-button', n_clicks=0),
    ], style={'display': 'flex', 'gap': '10px', 'alignItems': 'center', 'marginBottom': '20px'}),
    dash_table.DataTable(
        id='output-table',
        columns=[
            {'name': 'Row', 'id': 'Row'},
            {'name': 'Stock', 'id': 'Stock'},
            {'name': 'Spot Price', 'id': 'Spot Price'},
            {'name': 'ATM Strike', 'id': 'ATM Strike'},
            {'name': 'ATM IV', 'id': 'ATM IV'},
            {'name': 'Strikes', 'id': 'Strikes'},
            {'name': 'Premiums', 'id': 'Premiums'},
            {'name': 'Net Premium', 'id': 'Net Premium'},
            {'name': 'Lot Size', 'id': 'Lot Size'},
            {'name': 'Theta', 'id': 'Theta'},
            {'name': 'Margin', 'id': 'Margin'},
            {'name': 'Returns', 'id': 'Returns'},
        ],
        style_table={'overflowX': 'auto'},
        style_cell={'textAlign': 'center'},
        sort_action='native',
        filter_action='native',
        export_format='csv',
    ),
])

@app.callback(
    Output('output-table', 'data'),
    [
        Input('fetch-button', 'n_clicks'),
        Input('expiry-dropdown', 'value'),
        Input('sd-input', 'value')
    ]
)
def update_output(n_clicks, selected_expiry, sd_multiplier):
    if n_clicks == 0:
        return []
    
    # Fetch futures stocks for the selected expiry
    futures_stocks = fetch_futures_stocks(INSTRUMENTS_FILE, segment, instrument_type, selected_expiry, skip_symbols)
    if not futures_stocks:
        return []

    batch_size = 20
    results = []
    for i in range(0, len(futures_stocks), batch_size):
        batch = futures_stocks[i:i + batch_size]
        batch_results = process_stock_batch(batch, selected_expiry, sd_multiplier)
        results.extend(batch_results)
        if i + batch_size < len(futures_stocks):
            time.sleep(1)  # Wait 1 second between batches to respect rate limit
    
    # Add row numbers
    for idx, result in enumerate(results, 1):
        result['Row'] = idx
    
    return results

server = app.server

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)