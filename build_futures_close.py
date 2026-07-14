"""
NSE F&O Bhavcopy → Futures closing-price dataset (for future-to-future spreads)
================================================================================
Reads every daily bhavcopy zip in this folder, keeps only FUTURES rows
(stock futures = STF, index futures = IDF), and builds one tidy table of the
closing price of each contract on each trading day.

Input files   : BhavCopy_NSE_FO_0_0_0_YYYYMMDD_F_0000.csv.zip  (one per day)
Output files  : futures_close.parquet   (fast, typed — preferred)
                futures_close.csv        (human-readable copy)

Each output row = one contract on one day:

    date | symbol | expiry | inst_type | close | settle | prev_close |
    underlying | open_interest | volume | contract

For a given (date, symbol) there are usually 3 live expiries (near / next / far).
A future-to-future spread is simply the difference of two of those closes, e.g.
    AUG_close - JUL_close  (the near-next calendar spread).
See `spread_table()` at the bottom for a ready-made pivot.

Usage:
    python build_futures_close.py            # incremental: process only bhavcopies
                                             # newer than the latest date on file
    python build_futures_close.py --rebuild  # full rebuild from every zip
    python build_futures_close.py --limit 5  # quick test on the first 5 files

The first run (no futures_close.parquet yet) is always a full build. After that
the parquet is the authoritative store: its max date decides which zips are new
(the trading date is parsed from each filename, so old zips are never opened).
Running the update twice is a no-op — "No new data found."
"""

import argparse
import glob
import os
import re
import sys
import zipfile
from datetime import datetime

import pandas as pd

# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────
HERE = os.path.dirname(os.path.abspath(__file__))

# Folder holding the daily bhavcopy zips (BhavCopy_NSE_FO_0_0_0_YYYYMMDD_F_0000.csv.zip).
# Override with the BHAVCOPY_DIR env var if the zips move.
BHAVCOPY_DIR = os.getenv("BHAVCOPY_DIR", r"C:\Users\ADMIN\Desktop\bhavcopy")

ZIP_GLOB = os.path.join(BHAVCOPY_DIR, "BhavCopy_NSE_FO_0_0_0_*_F_0000.csv.zip")
OUT_PARQUET = os.path.join(HERE, "futures_close.parquet")
OUT_CSV = os.path.join(HERE, "futures_close.csv")

FUTURES_TYPES = {"STF", "IDF"}          # stock future, index future

# Bhavcopy column name → our tidy column name
COLUMNS = {
    "TradDt": "date",
    "TckrSymb": "symbol",
    "XpryDt": "expiry",
    "FinInstrmTp": "inst_type",
    "ClsPric": "close",
    "SttlmPric": "settle",
    "PrvsClsgPric": "prev_close",
    "UndrlygPric": "underlying",
    "OpnIntrst": "open_interest",
    "TtlTradgVol": "volume",
    "FinInstrmNm": "contract",
}

DATE_IN_NAME = re.compile(r"_(\d{8})_F_0000\.csv\.zip$")


# ──────────────────────────────────────────────
# PARSING
# ──────────────────────────────────────────────
def parse_zip(path):
    """Read one bhavcopy zip and return a tidy DataFrame of futures rows only."""
    with zipfile.ZipFile(path) as zf:
        # each zip holds exactly one CSV
        name = zf.namelist()[0]
        with zf.open(name) as fh:
            df = pd.read_csv(
                fh,
                usecols=list(COLUMNS.keys()),
                dtype=str,               # parse fast, coerce numerics afterwards
            )

    df = df[df["FinInstrmTp"].isin(FUTURES_TYPES)].copy()
    df.rename(columns=COLUMNS, inplace=True)

    # numeric coercion
    for col in ("close", "settle", "prev_close", "underlying",
                "open_interest", "volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
    df["expiry"] = pd.to_datetime(df["expiry"], format="%Y-%m-%d")

    # keep column order tidy
    return df[list(COLUMNS.values())]


def file_date(path):
    """Trading date encoded in a bhavcopy filename, as a Timestamp (or None)."""
    m = DATE_IN_NAME.search(os.path.basename(path))
    return pd.Timestamp(datetime.strptime(m.group(1), "%Y%m%d")) if m else None


def parse_files(files):
    """Parse a list of bhavcopy zips into one DataFrame (None if all failed)."""
    print(f"Parsing {len(files)} bhavcopy file(s)...")
    frames = []
    for i, path in enumerate(files, 1):
        try:
            frames.append(parse_zip(path))
        except Exception as e:                       # noqa: BLE001 — report & skip
            print(f"  [WARN] {os.path.basename(path)}: {e}")
            continue
        if i % 25 == 0 or i == len(files):
            print(f"  {i}/{len(files)} done")
    return pd.concat(frames, ignore_index=True) if frames else None


def build(rebuild=False, limit=None):
    files = sorted(glob.glob(ZIP_GLOB))
    if limit:
        files = files[:limit]
    if not files:
        sys.exit(f"No bhavcopy zips found matching:\n  {ZIP_GLOB}")

    existing = None
    if not rebuild and os.path.exists(OUT_PARQUET):
        existing = pd.read_parquet(OUT_PARQUET)
        if existing.empty:
            existing = None

    if existing is None:
        data = parse_files(files)
        if data is None:
            sys.exit("No bhavcopy file could be parsed.")
        added = len(data)
    else:
        latest = existing["date"].max()
        new_files = [f for f in files
                     if file_date(f) is not None and file_date(f) > latest]
        if not new_files:
            print(f"No new data found. Dataset is up to date ({latest:%Y-%m-%d}).")
            return existing
        print(f"Latest date on file: {latest:%Y-%m-%d} -> "
              f"{len(new_files)} newer bhavcopy file(s) to process.")
        new = parse_files(new_files)
        if new is None:
            sys.exit("No new bhavcopy file could be parsed.")
        # filenames decided what was new; re-check the actual dates so a
        # mislabelled zip can never duplicate rows already on file
        new = new[new["date"] > latest]
        added = len(new)
        data = pd.concat([existing, new], ignore_index=True)

    data.sort_values(["date", "symbol", "expiry"], inplace=True, ignore_index=True)

    data.to_parquet(OUT_PARQUET, index=False)
    data.to_csv(OUT_CSV, index=False)

    print(f"\nAdded {added:,} rows; dataset now {len(data):,} contract-day rows "
          f"for {data['symbol'].nunique()} symbols.")
    print(f"Date range : {data['date'].min():%Y-%m-%d} -> {data['date'].max():%Y-%m-%d}")
    print(f"Wrote      : {OUT_PARQUET}")
    print(f"           : {OUT_CSV}")
    return data


# ──────────────────────────────────────────────
# SPREAD HELPER (future-to-future)
# ──────────────────────────────────────────────
def spread_table(data, symbol):
    """
    For one symbol, return a per-day table with each expiry's close as a column,
    ordered near→far, plus the calendar spread between consecutive expiries.

    Columns: date, exp1, exp2, exp3, close1, close2, close3,
             spread_1_2 (exp2-exp1), spread_2_3 (exp3-exp2)
    """
    d = data[data["symbol"] == symbol].copy()
    rows = []
    for date, grp in d.groupby("date"):
        grp = grp.sort_values("expiry")
        closes = grp["close"].tolist()
        exps = [e.date() for e in grp["expiry"]]
        row = {"date": date.date()}
        for i, (e, c) in enumerate(zip(exps, closes), 1):
            row[f"exp{i}"] = e
            row[f"close{i}"] = c
        if len(closes) >= 2:
            row["spread_1_2"] = closes[1] - closes[0]
        if len(closes) >= 3:
            row["spread_2_3"] = closes[2] - closes[1]
        rows.append(row)
    return pd.DataFrame(rows)


# ──────────────────────────────────────────────
if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--rebuild", action="store_true",
                    help="ignore existing output and rebuild from every zip")
    ap.add_argument("--limit", type=int, default=None,
                    help="parse only the first N files (quick test)")
    args = ap.parse_args()

    t0 = datetime.now()
    build(rebuild=args.rebuild, limit=args.limit)
    print(f"Elapsed: {datetime.now() - t0}")
