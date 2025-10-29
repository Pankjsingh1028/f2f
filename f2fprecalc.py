import os
import json
import time
import pandas as pd
import requests
from datetime import datetime
from dotenv import load_dotenv

# ---------- CONFIG ----------
load_dotenv()
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
if not ACCESS_TOKEN:
    raise ValueError("ACCESS_TOKEN not found in .env file")

INSTRUMENTS_JSON = "instruments.json"
STOCKS_CSV = "futurestockslist.csv"
OUTPUT_CSV = "margin_charges_cache.csv"

# ---------- HELPERS ----------
def safe_float(x):
    try:
        return float(x)
    except:
        return None

def diff(a, b):
    return round(a - b, 4) if (a is not None and b is not None) else None

def load_instruments():
    with open(INSTRUMENTS_JSON, "r") as f:
        return json.load(f)

def load_underlyings():
    df = pd.read_csv(STOCKS_CSV)
    return df["underlying_symbol"].dropna().unique().tolist()

def get_instrument_keys_for_symbol(symbol, instruments):
    futures = []
    for instrument in instruments:
        if (
            instrument.get("segment") == "NSE_FO"
            and instrument.get("instrument_type") == "FUT"
            and instrument.get("underlying_symbol") == symbol
        ):
            futures.append({
                "instrument_key": instrument.get("instrument_key"),
                "expiry": instrument.get("expiry", 0),
                "trading_symbol": instrument.get("trading_symbol")
            })
    futures.sort(key=lambda x: x["expiry"])
    return futures[:3]

# ---------- API CALLS ----------
def fetch_margin_for_spread(near_key, next_key, lot_size):
    url = "https://api.upstox.com/v2/charges/margin"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }
    data = {
        "instruments": [
            {"instrument_key": near_key, "quantity": lot_size, "transaction_type": "BUY", "product": "D"},
            {"instrument_key": next_key, "quantity": lot_size, "transaction_type": "SELL", "product": "D"},
        ]
    }
    try:
        resp = requests.post(url, headers=headers, json=data, timeout=10)
        if resp.status_code == 200:
            js = resp.json()
            if js.get("status") == "success":
                return js["data"].get("final_margin") or js["data"].get("required_margin")
        else:
            print(f"[Margin] HTTP {resp.status_code}: {resp.text[:100]}")
    except Exception as e:
        print(f"[Margin] Error: {e}")
    return None

def fetch_spread_charges(near_key, next_key, lot_size, near_price, next_price):
    url = "https://api.upstox.com/v2/charges/brokerage"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }

    def get_total(instrument_key, qty, txn, price):
        params = {
            "instrument_token": instrument_key,
            "quantity": qty,
            "product": "D",
            "transaction_type": txn,
            "price": price or 0
        }
        try:
            r = requests.get(url, headers=headers, params=params, timeout=10)
            if r.status_code == 200:
                j = r.json()
                if j.get("status") == "success":
                    return j["data"]["charges"].get("total")
        except Exception as e:
            print(f"[Charges] Error: {e}")
        return None

    c1 = (get_total(near_key, lot_size, "BUY", near_price) or 0) + \
         (get_total(next_key, lot_size, "SELL", next_price) or 0)
    time.sleep(0.05)
    c2 = (get_total(near_key, lot_size, "SELL", near_price) or 0) + \
         (get_total(next_key, lot_size, "BUY", next_price) or 0)
    return c1, c2

def calculate_cost_of_carry_from_margin(margin, expiry_timestamp, roi=12.0):
    if not margin or not expiry_timestamp:
        return None
    expiry_date = datetime.fromtimestamp(expiry_timestamp / 1000)
    days_left = max((expiry_date - datetime.now()).days, 0)
    cost = margin * (roi / 100) * (days_left / 365)
    return round(cost, 2)

# ---------- MAIN EXECUTION ----------
if __name__ == "__main__":
    print(f"[{datetime.now()}] Loading instruments and underlyings...")
    instruments = load_instruments()
    underlyings = load_underlyings()

    results = []
    print(f"[{datetime.now()}] Calculating margin + charges + carry for {len(underlyings)} symbols...")

    for sym in underlyings:
        futs = get_instrument_keys_for_symbol(sym, instruments)
        if len(futs) < 2:
            continue
        near, nxt = futs[:2]
        lot_size = next((inst.get("lot_size") for inst in instruments
                         if inst.get("instrument_key") == near["instrument_key"]), 1)
        margin_val = fetch_margin_for_spread(near["instrument_key"], nxt["instrument_key"], lot_size)
        c_forward, c_reverse = fetch_spread_charges(near["instrument_key"], nxt["instrument_key"], lot_size, 0, 0)
        carry_cost = calculate_cost_of_carry_from_margin(margin_val, near["expiry"])

        results.append({
            "Symbol": sym,
            "Lot_Size": lot_size,
            "Margin": margin_val,
            "Charges_f": c_forward,
            "Charges_r": c_reverse,
            "Charges": round((c_forward or 0) + (c_reverse or 0), 2),
            "Cost_of_Carry": carry_cost
        })

        print(f"[{sym}] Margin={margin_val} | Fwd={c_forward} | Rev={c_reverse} | Carry={carry_cost}")
        time.sleep(0.1)

    df = pd.DataFrame(results)
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"[{datetime.now()}] ✅ Saved {len(df)} records to {OUTPUT_CSV}")
