"""
future_close.csv  →  future_spreads.csv
=======================================
Collapses the tidy contract-day table into one row per (Trade Date, Symbol),
classifying the (up to) three futures contracts by expiry as Near / Next / Far
and computing the calendar spreads.

  Near = earliest expiry, Next = 2nd, Far = 3rd   (relative to the trade date)

If fewer than three contracts exist, the missing columns are left blank.
If more than three exist, only the first three (earliest expiries) are used.

Usage:  python build_future_spreads.py
"""

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


def build():
    # dates read as plain strings to preserve the original format;
    # YYYY-MM-DD sorts correctly lexically, so no datetime parsing needed.
    df = pd.read_csv(IN_CSV, dtype={"date": str, "expiry": str})
    df["close"] = pd.to_numeric(df["close"], errors="coerce")

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
    out.to_csv(OUT_CSV, index=False)

    print(f"Wrote {len(out):,} rows -> {OUT_CSV}")
    return out


if __name__ == "__main__":
    build()
