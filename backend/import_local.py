"""
Local CSV Importer
==================
Reads your existing NSE CSV files and loads them into the breadth_data.db SQLite DB.

Handles:
  - Symbol format:  NSE:VOLTAS  →  VOLTAS
  - Date column:    'datetime'
  - Columns:        index, datetime, symbol, open, high, low, close, volume, change(%), ...
  - Two folders:    data till 2024-DEC  +  data till 2026-Jan  (merged, deduped)
  - Any filename:   VOLTAS.csv, NSE_VOLTAS.csv, VOLTAS_EQ.csv etc.

Usage:
  python import_local.py
  python import_local.py --folder "C:\\NSE Data" --dry-run
"""

import os
import sys
import glob
import argparse
import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

# ── DB path (same folder as this script) ─────────────────────────────────────
DB_PATH = Path(__file__).parent / "breadth_data.db"

# ── Default folder paths — edit these or pass via --folder ───────────────────
DEFAULT_FOLDERS = [
    r"C:\NSE Data\data till 2026-Jan",
    r"C:\NSE Data\data till 2024-DEC\data till 2024-DEC\data",
]


# ─────────────────────────────────────────────────────────────────────────────
# DB SETUP
# ─────────────────────────────────────────────────────────────────────────────

def init_db():
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS ohlcv (
            ticker  TEXT NOT NULL,
            market  TEXT NOT NULL DEFAULT 'India',
            date    TEXT NOT NULL,
            open    REAL,
            high    REAL,
            low     REAL,
            close   REAL NOT NULL,
            volume  INTEGER,
            PRIMARY KEY (ticker, date)
        );
        CREATE INDEX IF NOT EXISTS idx_ticker_date ON ohlcv(ticker, date);
        CREATE INDEX IF NOT EXISTS idx_market_date  ON ohlcv(market, date);

        CREATE TABLE IF NOT EXISTS import_log (
            ticker      TEXT PRIMARY KEY,
            rows        INTEGER,
            date_from   TEXT,
            date_to     TEXT,
            imported_at TEXT
        );
    """)
    conn.commit()
    conn.close()


# ─────────────────────────────────────────────────────────────────────────────
# CSV READER
# ─────────────────────────────────────────────────────────────────────────────

def clean_symbol(raw: str) -> str:
    """NSE:VOLTAS → VOLTAS,  NSE_VOLTAS → VOLTAS,  VOLTAS → VOLTAS"""
    s = str(raw).strip()
    if ':' in s:
        s = s.split(':')[-1]          # NSE:VOLTAS → VOLTAS
    if '_' in s and s.startswith('NSE'):
        s = s.split('_', 1)[-1]       # NSE_VOLTAS → VOLTAS
    return s.upper().strip()

def safe_float(v, default=None):
    try:
        f = float(v)
        return None if (np.isnan(f) or np.isinf(f)) else round(f, 4)
    except:
        return default

def read_csv_file(filepath: str) -> pd.DataFrame:
    """
    Read one CSV file → clean DataFrame with columns:
    ticker, date (str YYYY-MM-DD), open, high, low, close, volume
    Returns None if file can't be parsed.
    """
    try:
        df = pd.read_csv(filepath, low_memory=False)
    except Exception as e:
        logger.warning(f"  Can't read {filepath}: {e}")
        return None

    if df.empty:
        return None

    # ── Normalise column names ────────────────────────────────────────────────
    df.columns = [c.strip().lower().replace(' ', '_').replace('(%)', 'pct')
                  for c in df.columns]

    # ── Find date column ──────────────────────────────────────────────────────
    date_col = next((c for c in df.columns
                     if c in ('datetime','date','time','timestamp','dt')), None)
    if date_col is None:
        # Try first column
        date_col = df.columns[0] if df.columns[0] not in ('unnamed:_0','') else df.columns[1]

    # ── Find symbol column ────────────────────────────────────────────────────
    sym_col = next((c for c in df.columns
                    if c in ('symbol','ticker','scrip','stock','name')), None)

    # ── Find OHLCV columns ────────────────────────────────────────────────────
    def find(names):
        return next((c for c in df.columns if c in names), None)

    open_col   = find(('open','o'))
    high_col   = find(('high','h'))
    low_col    = find(('low','l'))
    close_col  = find(('close','c','ltp','last'))
    volume_col = find(('volume','vol','v','quantity'))

    if close_col is None:
        logger.warning(f"  No close column in {filepath} — skipping")
        return None

    # ── Build clean frame ─────────────────────────────────────────────────────
    out = pd.DataFrame()

    # Date
    try:
        out['date'] = pd.to_datetime(df[date_col], dayfirst=False,
                                     ).dt.strftime('%Y-%m-%d')
    except Exception as e:
        logger.warning(f"  Bad dates in {filepath}: {e}")
        return None

    # Ticker — from column if multi-stock file, else from filename
    if sym_col:
        out['ticker'] = df[sym_col].apply(clean_symbol)
    else:
        # Derive from filename: VOLTAS.csv → VOLTAS
        stem = Path(filepath).stem.upper()
        stem = clean_symbol(stem)
        out['ticker'] = stem

    out['open']   = df[open_col].apply(safe_float)   if open_col   else None
    out['high']   = df[high_col].apply(safe_float)   if high_col   else None
    out['low']    = df[low_col].apply(safe_float)    if low_col    else None
    out['close']  = df[close_col].apply(safe_float)
    out['volume'] = df[volume_col].apply(
        lambda x: int(float(x)) if pd.notna(x) and str(x).replace('.','').isdigit() else None
    ) if volume_col else None

    # Drop rows with no close
    out = out.dropna(subset=['close'])
    out = out[out['close'] > 0]
    out = out.drop_duplicates(subset=['ticker','date'])
    out = out.sort_values('date')

    return out if not out.empty else None


# ─────────────────────────────────────────────────────────────────────────────
# IMPORTER
# ─────────────────────────────────────────────────────────────────────────────

def collect_csv_files(folders: list) -> list:
    """Find all CSV files across all given folders (recursive)"""
    files = []
    for folder in folders:
        folder = folder.strip()
        if not os.path.exists(folder):
            logger.warning(f"Folder not found: {folder}")
            continue
        found = glob.glob(os.path.join(folder, '**', '*.csv'), recursive=True) + \
                glob.glob(os.path.join(folder, '**', '*.CSV'), recursive=True)
        logger.info(f"Found {len(found)} CSV files in {folder}")
        files.extend(found)
    # Deduplicate by filename (prefer longer path = more specific)
    seen = {}
    for f in files:
        name = os.path.basename(f).upper()
        if name not in seen or len(f) > len(seen[name]):
            seen[name] = f
    return list(seen.values())

def import_files(folders: list, dry_run: bool = False,
                 market: str = "India") -> dict:
    """
    Main import function.
    Reads all CSVs, merges duplicates across folders, writes to SQLite.
    """
    init_db()
    files = collect_csv_files(folders)
    if not files:
        logger.error("No CSV files found. Check your folder paths.")
        return {"error": "No files found", "files_found": 0}

    logger.info(f"Processing {len(files)} unique CSV files...")

    conn = sqlite3.connect(str(DB_PATH), timeout=60)
    conn.execute("PRAGMA journal_mode=WAL")

    total_rows = 0
    imported   = 0
    skipped    = 0
    errors     = 0

    # Group files by ticker (handle multiple files for same ticker)
    ticker_files: dict = {}
    for filepath in files:
        df = read_csv_file(filepath)
        if df is None:
            errors += 1
            continue
        for ticker, grp in df.groupby('ticker'):
            if ticker not in ticker_files:
                ticker_files[ticker] = []
            ticker_files[ticker].append(grp)

    logger.info(f"Found data for {len(ticker_files)} unique tickers")

    for ticker, frames in ticker_files.items():
        try:
            # Merge all frames for this ticker, deduplicate by date
            combined = pd.concat(frames, ignore_index=True)
            combined = combined.drop_duplicates(subset=['date'])
            combined = combined.sort_values('date')
            combined = combined[combined['close'] > 0]

            if len(combined) < 5:
                skipped += 1
                continue

            date_from = combined['date'].iloc[0]
            date_to   = combined['date'].iloc[-1]

            if dry_run:
                logger.info(f"  [DRY RUN] {ticker}: {len(combined)} rows  {date_from} → {date_to}")
                total_rows += len(combined)
                imported   += 1
                continue

            # Write to DB
            rows = [(ticker, market, r['date'],
                     r.get('open'), r.get('high'), r.get('low'), r['close'],
                     r.get('volume'))
                    for _, r in combined.iterrows()]

            conn.executemany("""
                INSERT OR REPLACE INTO ohlcv
                (ticker,market,date,open,high,low,close,volume)
                VALUES (?,?,?,?,?,?,?,?)
            """, rows)

            conn.execute("""
                INSERT OR REPLACE INTO import_log
                (ticker, rows, date_from, date_to, imported_at)
                VALUES (?,?,?,?,?)
            """, (ticker, len(rows), date_from, date_to,
                  datetime.utcnow().isoformat()))

            total_rows += len(rows)
            imported   += 1

            if imported % 50 == 0:
                conn.commit()
                logger.info(f"  Progress: {imported}/{len(ticker_files)} tickers, {total_rows:,} rows so far")

        except Exception as e:
            logger.error(f"  Error importing {ticker}: {e}")
            errors += 1

    conn.commit()
    conn.close()

    # Summary
    action = "Would import" if dry_run else "Imported"
    logger.info(f"\n{'='*50}")
    logger.info(f"{action} {imported} tickers, {total_rows:,} rows")
    logger.info(f"Skipped: {skipped}  Errors: {errors}")
    logger.info(f"DB: {DB_PATH}")

    # DB stats
    if not dry_run:
        conn2 = sqlite3.connect(str(DB_PATH))
        total = conn2.execute("SELECT COUNT(*) FROM ohlcv").fetchone()[0]
        tickers_db = conn2.execute("SELECT COUNT(DISTINCT ticker) FROM ohlcv").fetchone()[0]
        oldest = conn2.execute("SELECT MIN(date) FROM ohlcv").fetchone()[0]
        newest = conn2.execute("SELECT MAX(date) FROM ohlcv").fetchone()[0]
        conn2.close()
        logger.info(f"DB now has: {tickers_db} tickers, {total:,} rows, {oldest} → {newest}")

    return {
        "tickers_imported": imported,
        "total_rows": total_rows,
        "skipped": skipped,
        "errors": errors,
        "dry_run": dry_run,
        "db_path": str(DB_PATH),
    }


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import local NSE CSV data into breadth_data.db")
    parser.add_argument("--folder", nargs="+",
                        help="One or more folder paths. Defaults to your NSE Data folders.",
                        default=None)
    parser.add_argument("--dry-run", action="store_true",
                        help="Scan files and show what would be imported without writing anything")
    parser.add_argument("--market", default="India",
                        help="Market label to store (default: India)")
    args = parser.parse_args()

    folders = args.folder if args.folder else DEFAULT_FOLDERS

    print("\n" + "="*60)
    print("  NSE CSV → SQLite Importer")
    print("="*60)
    print(f"  Folders : {folders}")
    print(f"  DB      : {DB_PATH}")
    print(f"  Mode    : {'DRY RUN (no writes)' if args.dry_run else 'IMPORT'}")
    print("="*60 + "\n")

    result = import_files(folders, dry_run=args.dry_run, market=args.market)

    print("\n" + "="*60)
    print(f"  DONE")
    print(f"  Tickers : {result.get('tickers_imported', 0)}")
    print(f"  Rows    : {result.get('total_rows', 0):,}")
    print(f"  Errors  : {result.get('errors', 0)}")
    print("="*60 + "\n")
