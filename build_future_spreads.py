"""
futures_close.csv  →  future_spreads.csv
========================================
Collapses the tidy contract-day table into one row per (Trade Date, Symbol),
classifying the (up to) three futures contracts by expiry as Near / Next / Far
and computing the calendar spreads.

  Near = earliest expiry, Next = 2nd, Far = 3rd   (relative to the trade date)

If fewer than three contracts exist, the missing columns are left blank.
If more than three exist, only the first three (earliest expiries) are used.

Usage:  python build_future_spreads.py             # incremental: append rows for
                                                   # dates newer than the last
                                                   # Trade Date already on file
        python build_future_spreads.py --rebuild   # regenerate from scratch

Each output row depends only on the futures_close rows of its own trade date,
so incremental runs compute rows for the new dates only and append them; the
result is identical to a full rebuild. Running twice is a no-op.
"""

import argparse
import os

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
IN_CSV = os.path.join(HERE, "futures_close.csv")
OUT_CSV = os.path.join(HERE, "future_spreads.csv")

OUT_COLUMNS = [
    "Trade Date", "Symbol",
    "Near Expiry", "Near Close",
    "Next Expiry", "Next Close",
    "Far Expiry", "Far Close",
    "Near-Next Spread", "Next-Far Spread", "Near-Far Spread",
]


def compute_rows(df):
    """One output row per (date, symbol), sorted by Trade Date then Symbol."""
    rows = []
    for (date, symbol), grp in df.groupby(["date", "symbol"], sort=True):
        grp = grp.sort_values("expiry")           # earliest expiry first
        grp = grp.drop_duplicates("expiry").head(3)   # take first three expiries

        exps = grp["expiry"].tolist()
        closes = grp["close"].tolist()
        # pad to length 3 so slots are always present
        exps += [None] * (3 - len(exps))
        closes += [None] * (3 - len(closes))
        near_e, next_e, far_e = exps
        near_c, next_c, far_c = closes

        def spread(a, b):
            return round(a - b, 2) if (a is not None and b is not None) else None

        rows.append({
            "Trade Date": date,
            "Symbol": symbol,
            "Near Expiry": near_e,
            "Near Close": near_c,
            "Next Expiry": next_e,
            "Next Close": next_c,
            "Far Expiry": far_e,
            "Far Close": far_c,
            "Near-Next Spread": spread(next_c, near_c),
            "Next-Far Spread": spread(far_c, next_c),
            "Near-Far Spread": spread(far_c, near_c),
        })

    out = pd.DataFrame(rows, columns=OUT_COLUMNS)
    out.sort_values(["Trade Date", "Symbol"], inplace=True, ignore_index=True)
    return out


def build(rebuild=False):
    # dates read as plain strings to preserve the original format;
    # YYYY-MM-DD sorts correctly lexically, so no datetime parsing needed.
    df = pd.read_csv(IN_CSV, dtype={"date": str, "expiry": str})
    df["close"] = pd.to_numeric(df["close"], errors="coerce")

    last_date = None
    if not rebuild and os.path.exists(OUT_CSV):
        done = pd.read_csv(OUT_CSV, usecols=["Trade Date"], dtype=str)["Trade Date"]
        if len(done):
            last_date = done.max()

    if last_date is None:
        out = compute_rows(df)
        out.to_csv(OUT_CSV, index=False)
        print(f"Wrote {len(out):,} rows -> {OUT_CSV}")
        return out

    df = df[df["date"] > last_date]
    if df.empty:
        print(f"No new data found. Spreads are up to date ({last_date}).")
        return None

    # New dates all sort after last_date and the existing file is already
    # (Trade Date, Symbol)-sorted, so appending keeps the file sorted.
    out = compute_rows(df)
    out.to_csv(OUT_CSV, mode="a", header=False, index=False)
    print(f"Appended {len(out):,} rows for dates after {last_date} -> {OUT_CSV}")
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--rebuild", action="store_true",
                    help="ignore existing output and rebuild from scratch")
    args = ap.parse_args()
    build(rebuild=args.rebuild)
