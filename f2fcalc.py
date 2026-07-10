"""
Futures Spread Pre-calculator — Optimized with parallel API calls
==================================================================
Fetches margin + brokerage charges for all futures symbols and caches to CSV.
Run this BEFORE starting the dashboard.

Usage:  python f2fprecalc_optimized.py

Optimization: Uses ThreadPoolExecutor (10 workers) instead of sequential loop.
              ~206 symbols × 5 API calls each → finishes in ~1 min vs ~4 min.
"""

import os
import json
import time
import pandas as pd
import requests
from datetime import datetime
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────
load_dotenv()
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
if not ACCESS_TOKEN:
    raise ValueError("ACCESS_TOKEN not found in .env file")

INSTRUMENTS_JSON = "instruments.json"
STOCKS_CSV = "futurestockslist.csv"
OUTPUT_CSV = "margin_charges_cache.csv"
MARKET_QUOTE_LTP_URL = "https://api.upstox.com/v2/market-quote/ltp"

MAX_WORKERS = 10          # parallel API callers
RATE_LIMIT_DELAY = 0.05   # seconds between API calls within a worker

# Shared session for connection pooling
session = requests.Session()
session.headers.update({
    "accept": "application/json",
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Content-Type": "application/json",
})


# ──────────────────────────────────────────────
# HELPERS
# ──────────────────────────────────────────────
def safe_float(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def load_instruments():
    with open(INSTRUMENTS_JSON, "r") as f:
        return json.load(f)


def load_underlyings():
    df = pd.read_csv(STOCKS_CSV)
    return df["underlying_symbol"].dropna().unique().tolist()


def build_futures_index(instruments):
    """Pre-index futures by underlying_symbol → sorted list of up to 3 nearest."""
    raw = {}
    lot_sizes = {}
    for inst in instruments:
        if (
            inst.get("segment") == "NSE_FO"
            and inst.get("instrument_type") == "FUT"
        ):
            sym = inst.get("underlying_symbol")
            if not sym:
                continue
            raw.setdefault(sym, []).append({
                "instrument_key": inst.get("instrument_key"),
                "expiry": inst.get("expiry", 0),
                "trading_symbol": inst.get("trading_symbol"),
            })
            lot_sizes[inst.get("instrument_key")] = inst.get("lot_size")

    index = {}
    for sym, futs in raw.items():
        futs.sort(key=lambda x: x["expiry"])
        index[sym] = futs[:3]

    return index, lot_sizes


# ──────────────────────────────────────────────
# API CALLS
# ──────────────────────────────────────────────
def fetch_margin(near_key, next_key, lot_size):
    """Fetch combined margin for Near BUY + Next SELL spread."""
    url = "https://api.upstox.com/v2/charges/margin"
    data = {
        "instruments": [
            {"instrument_key": near_key, "quantity": lot_size, "transaction_type": "BUY", "product": "D"},
            {"instrument_key": next_key, "quantity": lot_size, "transaction_type": "SELL", "product": "D"},
        ]
    }
    try:
        resp = session.post(url, json=data, timeout=10)
        if resp.status_code == 200:
            js = resp.json()
            if js.get("status") == "success":
                return js["data"].get("final_margin") or js["data"].get("required_margin")
        elif resp.status_code == 429:
            time.sleep(1)  # rate limited — back off
            return fetch_margin(near_key, next_key, lot_size)  # retry once
    except Exception as e:
        print(f"  [Margin] Error: {e}")
    return None


def fetch_charges_ex_brokerage(instrument_key, qty, txn_type, price):
    """Fetch a single leg's charges as PURE ex-brokerage.

    Removes Upstox brokerage AND the GST levied on that brokerage, leaving only
    the statutory/exchange charges (STT, exchange txn, stamp duty, SEBI,
    clearing) plus the GST on the exchange/SEBI fees.

    GST = rate × (brokerage + transaction + sebi_turnover). We strip the
    brokerage's proportional share of GST, derived from the response so the
    effective rate is used rather than a hardcoded 18%.
    """
    url = "https://api.upstox.com/v2/charges/brokerage"
    params = {
        "instrument_token": instrument_key,
        "quantity": qty,
        "product": "D",
        "transaction_type": txn_type,
        "price": price or 0,
    }
    try:
        r = session.get(url, params=params, timeout=10)
        if r.status_code == 200:
            j = r.json()
            if j.get("status") == "success":
                charges = j["data"]["charges"]
                total = safe_float(charges.get("total")) or 0
                brokerage = safe_float(charges.get("brokerage")) or 0
                gst = safe_float((charges.get("taxes") or {}).get("gst")) or 0
                other = charges.get("otherTaxes") or charges.get("other_charges") or {}
                transaction = safe_float(other.get("transaction")) or 0
                sebi = safe_float(other.get("sebi_turnover")) or 0
                gst_base = brokerage + transaction + sebi
                gst_on_brokerage = gst * (brokerage / gst_base) if gst_base > 0 else 0
                return round(total - brokerage - gst_on_brokerage, 2)
        elif r.status_code == 429:
            time.sleep(1)
            return fetch_charges_ex_brokerage(instrument_key, qty, txn_type, price)
    except Exception as e:
        print(f"  [Charges] Error: {e}")
    return None


def fetch_all_ltps(keys):
    """Batch-fetch last traded prices for all legs → {instrument_key: ltp}.

    The value-based statutory charges (STT, exchange, stamp, GST) are a % of
    turnover (price × qty), so the brokerage endpoint needs a real price to
    return them. We fetch every leg's LTP once here instead of per-symbol.
    """
    ltp_map = {}
    batch_size = 490
    for i in range(0, len(keys), batch_size):
        batch = keys[i:i + batch_size]
        url = f"{MARKET_QUOTE_LTP_URL}?instrument_key={','.join(batch)}"
        try:
            r = session.get(url, timeout=15)
            if r.status_code == 200:
                for _, q in r.json().get("data", {}).items():
                    ik = q.get("instrument_token")
                    if ik:
                        ltp_map[ik] = safe_float(q.get("last_price"))
            elif r.status_code == 429:
                time.sleep(1)
                return fetch_all_ltps(keys)  # retry after back-off
            else:
                print(f"  [LTP] HTTP {r.status_code} for batch {i // batch_size + 1}")
        except Exception as e:
            print(f"  [LTP] batch {i // batch_size + 1} error: {e}")
        time.sleep(RATE_LIMIT_DELAY)
    return ltp_map


def fetch_spread_charges(near_key, next_key, lot_size, near_price, next_price):
    """Fetch charges for both spread directions, priced at each leg's LTP."""
    # Forward: Near BUY + Next SELL
    c_fwd = (fetch_charges_ex_brokerage(near_key, lot_size, "BUY", near_price) or 0) + \
            (fetch_charges_ex_brokerage(next_key, lot_size, "SELL", next_price) or 0)
    time.sleep(RATE_LIMIT_DELAY)

    # Reverse: Near SELL + Next BUY
    c_rev = (fetch_charges_ex_brokerage(near_key, lot_size, "SELL", near_price) or 0) + \
            (fetch_charges_ex_brokerage(next_key, lot_size, "BUY", next_price) or 0)

    return c_fwd, c_rev


def calculate_carry_cost(margin, expiry_timestamp, roi=12.0):
    """Cost of carry = margin × (roi%) × (days_left / 365)."""
    if not margin or not expiry_timestamp:
        return None
    try:
        expiry_date = datetime.fromtimestamp(expiry_timestamp / 1000)
        days_left = max((expiry_date - datetime.now()).days, 0)
        return round(margin * (roi / 100) * (days_left / 365), 2)
    except Exception:
        return None


# ──────────────────────────────────────────────
# PROCESS ONE SYMBOL (called by workers)
# ──────────────────────────────────────────────
def process_symbol(sym, futures_index, lot_sizes, ltp_map):
    """Fetch margin + charges for one symbol. Returns a dict or None."""
    futs = futures_index.get(sym, [])
    if len(futs) < 2:
        return None

    near, nxt = futs[0], futs[1]
    near_key = near["instrument_key"]
    nxt_key = nxt["instrument_key"]
    lot_size = lot_sizes.get(near_key, 1)
    near_price = ltp_map.get(near_key)
    nxt_price = ltp_map.get(nxt_key)

    # API calls (these are the slow part)
    margin_val = fetch_margin(near_key, nxt_key, lot_size)
    time.sleep(RATE_LIMIT_DELAY)
    c_fwd, c_rev = fetch_spread_charges(near_key, nxt_key, lot_size, near_price, nxt_price)
    carry = calculate_carry_cost(margin_val, near.get("expiry"))

    result = {
        "Symbol": sym,
        "Lot_Size": lot_size,
        "Near_Price": near_price,
        "Next_Price": nxt_price,
        "Margin": margin_val,
        "Charges_f": c_fwd,
        "Charges_r": c_rev,
        "Charges": round((c_fwd or 0) + (c_rev or 0), 2),
        "Cost_of_Carry": carry,
    }

    print(f"  [{sym}] Px={near_price}/{nxt_price} | Margin={margin_val} | Fwd={c_fwd} | Rev={c_rev} | Carry={carry}")
    return result


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────
if __name__ == "__main__":
    start_time = time.time()

    print(f"[{datetime.now()}] Loading instruments and symbols...")
    instruments = load_instruments()
    underlyings = load_underlyings()
    futures_index, lot_sizes = build_futures_index(instruments)

    # Pre-fetch LTPs for all near/next legs so brokerage charges are priced
    # correctly (value-based taxes need a real turnover, not price=0).
    price_keys = []
    for sym in underlyings:
        for f in futures_index.get(sym, [])[:2]:
            price_keys.append(f["instrument_key"])
    price_keys = list(dict.fromkeys(price_keys))
    print(f"[{datetime.now()}] Fetching LTPs for {len(price_keys)} legs...")
    ltp_map = fetch_all_ltps(price_keys)
    priced = sum(1 for v in ltp_map.values() if v)
    print(f"[{datetime.now()}] Got {priced}/{len(price_keys)} live prices.")
    if priced == 0:
        print("  WARNING: no LTPs available (market closed or token invalid) — "
              "charges will fall back to price=0 and understate statutory costs.")

    print(f"[{datetime.now()}] Processing {len(underlyings)} symbols with {MAX_WORKERS} workers...")

    results = []
    failed = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_sym = {
            executor.submit(process_symbol, sym, futures_index, lot_sizes, ltp_map): sym
            for sym in underlyings
        }

        for future in as_completed(future_to_sym):
            sym = future_to_sym[future]
            try:
                result = future.result()
                if result:
                    results.append(result)
            except Exception as e:
                failed.append(sym)
                print(f"  [{sym}] FAILED: {e}")

    # Save results
    df = pd.DataFrame(results)
    df.to_csv(OUTPUT_CSV, index=False)

    elapsed = time.time() - start_time
    print(f"\n[{datetime.now()}] Done in {elapsed:.1f}s")
    print(f"  Saved: {len(results)} symbols to {OUTPUT_CSV}")
    if failed:
        print(f"  Failed: {len(failed)} symbols: {failed}")
