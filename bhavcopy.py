"""
bhavcopy.py - download the daily exchange bhavcopy files into BHAVCOPY_DIR
==========================================================================
Fills the shared bhavcopy folder (the one build_futures_close.py reads) from a
start date up to today. Files that are already in the folder are never fetched
again, so it is safe to run as often as you like.

Default: NSE F&O only (BhavCopy_NSE_FO_0_0_0_YYYYMMDD_F_0000.csv.zip), the only
file F2F reads. Use --all, or --segments, for the cash and BSE files that the
ajira-rms end-of-day run also uses.

Folder:  BHAVCOPY_DIR from .env or the environment,
         else C:\\Users\\AMAN\\Desktop\\bhavcopy

Usage:
    python bhavcopy.py                        # NSE F&O, 01-Jul-2026 -> today
    python bhavcopy.py --from 2026-08-01      # a different start date
    python bhavcopy.py --all                  # NSE F&O + NSE cash + BSE F&O + BSE cash
    python bhavcopy.py --segments NSE_FO,NSE_CM
    python bhavcopy.py --dry-run              # list what would be fetched, download nothing

Weekends are skipped. A weekday the exchange has no file for (a holiday, or
today's file not published yet) is reported, not treated as an error. A
downloaded file is checked to be the right type and for the right trade date
before it is saved, and is written atomically.
"""

import argparse
import io
import os
import sys
import time
import zipfile
from datetime import date, datetime, timedelta, timezone

import requests

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:                      # .env is optional: the environment variable still works
    pass

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8")
    except (AttributeError, ValueError):
        pass

BHAVCOPY_DIR = (os.getenv("BHAVCOPY_DIR") or r"C:\Users\AMAN\Desktop\bhavcopy").strip().strip("\"'")
DEFAULT_START = date(2026, 7, 1)
PAUSE_SECONDS = 0.5                      # between requests: do not hammer the exchange
MAX_CONSECUTIVE_ERRORS = 5               # blocked or offline: stop instead of trying every day
IST = timezone(timedelta(hours=5, minutes=30))

_NSE = "https://nsearchives.nseindia.com/content"
_BSE = "https://www.bseindia.com/download"

# key -> (url folder, file name template, kind)
FILES = {
    "NSE_FO": (f"{_NSE}/fo/", "BhavCopy_NSE_FO_0_0_0_{ymd}_F_0000.csv.zip", "zip"),
    "NSE_CM": (f"{_NSE}/cm/", "BhavCopy_NSE_CM_0_0_0_{ymd}_F_0000.csv.zip", "zip"),
    "BSE_FO": (f"{_BSE}/Bhavcopy/Derivative/", "BhavCopy_BSE_FO_0_0_0_{ymd}_F_0000.CSV", "csv"),
    "BSE_CM": (f"{_BSE}/BhavCopy/Equity/", "BhavCopy_BSE_CM_0_0_0_{ymd}_F_0000.CSV", "csv"),
}
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept-Language": "en-US,en;q=0.9",
}
_REFERER = {"zip": "https://www.nseindia.com", "csv": "https://www.bseindia.com/"}


def bhavcopy_name(d: date, key: str) -> str:
    return FILES[key][1].format(ymd=d.strftime("%Y%m%d"))


def path_for(d: date, key: str) -> str:
    return os.path.join(BHAVCOPY_DIR, bhavcopy_name(d, key))


def weekdays(start: date, end: date):
    d = start
    while d <= end:
        if d.weekday() < 5:
            yield d
        d += timedelta(days=1)


def is_valid(content: bytes, kind: str, d: date) -> bool:
    """Right kind of file AND for the right trade date (the exchange answers with an HTML page for some misses)."""
    if kind == "zip":
        if not zipfile.is_zipfile(io.BytesIO(content)):
            return False
        with zipfile.ZipFile(io.BytesIO(content)) as z:
            names = z.namelist()
            if not names:
                return False
            head = z.open(names[0]).read(4096)
    else:
        head = content[:4096]
    lines = head.lstrip(b"\xef\xbb\xbf").decode("utf-8", errors="replace").splitlines()
    if len(lines) < 2 or not lines[0].startswith("TradDt,"):
        return False
    return lines[1].split(",")[0].strip() == d.isoformat()


def fetch(session, d: date, key: str):
    """Download one file. Returns 'saved', 'absent' (the exchange has no such file) or 'error'."""
    base, _, kind = FILES[key]
    name = bhavcopy_name(d, key)
    try:
        resp = session.get(base + name, headers={**_HEADERS, "Referer": _REFERER[kind]}, timeout=30)
    except requests.RequestException as e:
        print(f"  ERROR  {name}: {e}")
        return "error"
    if resp.status_code == 404:
        return "absent"
    if resp.status_code != 200:
        print(f"  ERROR  {name}: HTTP {resp.status_code}")
        return "error"
    if not is_valid(resp.content, kind, d):
        return "absent"                  # a page that is not the file, or a file for another date
    save_path = path_for(d, key)
    tmp = f"{save_path}.{os.getpid()}.part"
    with open(tmp, "wb") as f:
        f.write(resp.content)
    os.replace(tmp, save_path)           # never leave a half-written file behind
    return "saved"


def run(start: date, end: date, keys: list, dry_run: bool = False) -> int:
    todo = [(d, k) for d in weekdays(start, end) for k in keys if not os.path.exists(path_for(d, k))]
    have = sum(1 for d in weekdays(start, end) for k in keys) - len(todo)
    print(f"Folder   : {BHAVCOPY_DIR}")
    print(f"Range    : {start} to {end}   Segments: {', '.join(keys)}")
    print(f"Already  : {have} file(s) in the folder   To fetch: {len(todo)}")
    if dry_run or not todo:
        if dry_run:
            for d, k in todo:
                print(f"  would fetch {bhavcopy_name(d, k)}")
        return 0
    os.makedirs(BHAVCOPY_DIR, exist_ok=True)

    session = requests.Session()
    if any(FILES[k][2] == "zip" for _, k in todo):
        try:
            session.get("https://www.nseindia.com", headers=_HEADERS, timeout=10)    # cookies the archive expects
        except requests.RequestException as e:
            print(f"  warning: NSE warm-up failed: {e}")

    saved, absent, errors, streak = 0, [], 0, 0
    for i, (d, k) in enumerate(todo, 1):
        result = fetch(session, d, k)
        if result == "saved":
            saved += 1; streak = 0
            print(f"[{i}/{len(todo)}] saved   {bhavcopy_name(d, k)}")
        elif result == "absent":
            absent.append((d, k)); streak = 0
        else:
            errors += 1; streak += 1
            if streak >= MAX_CONSECUTIVE_ERRORS:
                print(f"Stopping: {streak} errors in a row (blocked or offline). Run again later; saved files are kept.")
                break
        time.sleep(PAUSE_SECONDS)

    print(f"\nSaved {saved}, errors {errors}, no file at the exchange {len(absent)}.")
    if absent:
        by_day = {}
        for d, k in absent:
            by_day.setdefault(d, []).append(k)
        print("No file for (holiday, or not published yet):")
        for d in sorted(by_day):
            print(f"  {d} {d.strftime('%a')}: {', '.join(by_day[d])}")
    return 1 if errors else 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Download exchange bhavcopy files into BHAVCOPY_DIR.")
    ap.add_argument("--from", dest="start", type=date.fromisoformat, default=DEFAULT_START,
                    help="first date, YYYY-MM-DD (default 2026-07-01)")
    ap.add_argument("--to", dest="end", type=date.fromisoformat, default=None,
                    help="last date, YYYY-MM-DD (default today, IST)")
    ap.add_argument("--all", action="store_true", help="all four files: NSE F&O, NSE cash, BSE F&O, BSE cash")
    ap.add_argument("--segments", default="NSE_FO", help="comma list from " + ", ".join(FILES) + " (default NSE_FO)")
    ap.add_argument("--dry-run", action="store_true", help="list what would be fetched and exit")
    a = ap.parse_args()

    keys = list(FILES) if a.all else [s.strip().upper() for s in a.segments.split(",") if s.strip()]
    bad = [k for k in keys if k not in FILES]
    if bad or not keys:
        ap.error(f"unknown segment(s) {bad}; choose from {', '.join(FILES)}")
    end = a.end or datetime.now(IST).date()
    if a.start > end:
        ap.error(f"--from {a.start} is after the end date {end}")
    return run(a.start, end, keys, a.dry_run)


if __name__ == "__main__":
    sys.exit(main())
