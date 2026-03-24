"""
NSE Data Sync — Direct Yahoo Finance v8 API
============================================
Fetches OHLCV data for NSE tickers using Yahoo Finance v8 chart API directly
(bypasses yfinance library which has issues with rate limiting/blocking).

Usage:
    from nse_sync import sync_nifty500
    result = sync_nifty500()  # updates DB with latest data
"""

import sqlite3
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "breadth_data.db"
NIFTY500_PATH = Path(__file__).parent / "data" / "nifty500_clean.csv"

# Yahoo v8 chart API — no auth needed, just User-Agent header
YAHOO_V8_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


def _load_nifty500_tickers():
    """Load NIFTY 500 ticker list from CSV."""
    import csv
    tickers = []
    with open(NIFTY500_PATH) as f:
        for row in csv.DictReader(f):
            sym = (row.get('Symbol') or row.get('symbol') or '').strip()
            if sym:
                tickers.append(sym)
    return tickers


def _get_all_universe_tickers() -> list:
    """
    Get the full ticker universe for EOD sync.
    Priority:
      1. All tickers from nse_index_constituents (after NSE index sync)
      2. Fall back to NIFTY 500 CSV if index DB is empty
    """
    if not DB_PATH.exists():
        return _load_nifty500_tickers()
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=10)
        rows = conn.execute(
            "SELECT DISTINCT ticker FROM nse_index_constituents ORDER BY ticker"
        ).fetchall()
        conn.close()
        if rows:
            tickers = [r[0] for r in rows]
            logger.info(f"Universe: {len(tickers)} tickers from nse_index_constituents")
            return tickers
    except Exception as e:
        logger.warning(f"Could not load index constituents: {e}")
    # Fallback
    tickers = _load_nifty500_tickers()
    logger.info(f"Universe: {len(tickers)} tickers from NIFTY 500 CSV (fallback)")
    return tickers


def _get_stale_tickers(days_threshold=5):
    """Find tickers (from full universe) whose data is older than threshold days."""
    tickers = _get_all_universe_tickers()
    if not DB_PATH.exists():
        return [(t, None) for t in tickers]

    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    cutoff = (datetime.now() - timedelta(days=days_threshold)).strftime('%Y-%m-%d')

    stale = []
    for t in tickers:
        row = conn.execute(
            "SELECT MAX(date) FROM ohlcv WHERE ticker=? AND market='India'", (t,)
        ).fetchone()
        last_date = row[0] if row and row[0] else None
        if not last_date or last_date < cutoff:
            stale.append((t, last_date))

    conn.close()
    logger.info(
        f"Stale tickers (>{days_threshold}d old): "
        f"{len(stale)}/{len(tickers)} from full universe"
    )
    return stale


def _fetch_yahoo_v8(ticker_ns, range_str="3mo", interval="1d"):
    """Fetch OHLCV from Yahoo v8 chart API. Returns list of (date, o, h, l, c, v) or None."""
    import httpx
    try:
        url = YAHOO_V8_URL.format(ticker=ticker_ns)
        params = {"range": range_str, "interval": interval}
        resp = httpx.get(url, params=params, headers=HEADERS, timeout=20, follow_redirects=True)
        if resp.status_code != 200:
            return None

        data = resp.json()
        result = data.get("chart", {}).get("result", [])
        if not result:
            return None

        timestamps = result[0].get("timestamp", [])
        quotes = result[0].get("indicators", {}).get("quote", [{}])[0]

        opens = quotes.get("open", [])
        highs = quotes.get("high", [])
        lows = quotes.get("low", [])
        closes = quotes.get("close", [])
        volumes = quotes.get("volume", [])

        rows = []
        for i, ts in enumerate(timestamps):
            try:
                dt = datetime.utcfromtimestamp(ts).strftime('%Y-%m-%d')
                o = float(opens[i]) if opens[i] is not None else None
                h = float(highs[i]) if highs[i] is not None else None
                l = float(lows[i]) if lows[i] is not None else None
                c = float(closes[i]) if closes[i] is not None else None
                v = int(volumes[i]) if volumes[i] is not None else 0
                if o and h and l and c and c > 0:
                    rows.append((dt, o, h, l, c, v))
            except (IndexError, TypeError, ValueError):
                continue

        return rows
    except Exception as e:
        logger.debug(f"Yahoo v8 failed for {ticker_ns}: {e}")
        return None


def _upsert_rows(conn, ticker, market, rows):
    """Insert or replace OHLCV rows into DB."""
    if not rows:
        return 0
    conn.executemany(
        """INSERT OR REPLACE INTO ohlcv (ticker, market, date, open, high, low, close, volume)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        [(ticker, market, dt, o, h, l, c, v) for dt, o, h, l, c, v in rows]
    )
    return len(rows)


def sync_ticker(ticker, range_str="3mo", progress_cb=None):
    """Sync a single ticker. Returns (ticker, new_rows, error)."""
    ticker_ns = f"{ticker}.NS"
    rows = _fetch_yahoo_v8(ticker_ns, range_str=range_str)
    if rows is None:
        # Retry once with longer range
        time.sleep(0.5)
        rows = _fetch_yahoo_v8(ticker_ns, range_str=range_str)

    if rows is None:
        return (ticker, 0, "download_failed")

    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    try:
        n = _upsert_rows(conn, ticker, "India", rows)
        conn.commit()
        return (ticker, n, None)
    except Exception as e:
        return (ticker, 0, str(e))
    finally:
        conn.close()


def sync_nifty500(range_str="3mo", max_workers=5, progress_state=None):
    """
    Sync all NIFTY 500 tickers that have stale data.
    Uses Yahoo v8 direct API (not yfinance).
    
    Args:
        range_str: "3mo" for 3 months, "1y" for 1 year, "2y" for 2 years
        max_workers: parallel download threads (keep <=5 to avoid rate limits)
        progress_state: optional dict to update with progress (for UI)
    
    Returns: dict with summary stats
    """
    stale = _get_stale_tickers(days_threshold=3)

    if not stale:
        msg = "All NIFTY 500 tickers are up to date!"
        logger.info(msg)
        return {"message": msg, "updated": 0, "failed": 0, "total_new_rows": 0}

    total = len(stale)
    logger.info(f"Syncing {total} stale tickers via Yahoo v8 API (range={range_str})...")

    if progress_state is not None:
        progress_state["total"] = total
        progress_state["progress"] = 0
        progress_state["message"] = f"Syncing {total} tickers..."

    updated = 0
    failed = 0
    total_rows = 0
    failed_tickers = []

    # Process in batches with rate limiting
    batch_size = max_workers
    tickers_to_sync = [t for t, _ in stale]

    for batch_start in range(0, total, batch_size):
        batch = tickers_to_sync[batch_start:batch_start + batch_size]

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(sync_ticker, t, range_str): t for t in batch}
            for future in as_completed(futures):
                ticker, n_rows, error = future.result()
                if error:
                    failed += 1
                    failed_tickers.append(ticker)
                else:
                    updated += 1
                    total_rows += n_rows

                done = updated + failed
                if progress_state is not None:
                    progress_state["progress"] = done
                    progress_state["message"] = f"Synced {done}/{total} ({ticker}{'  ✗' if error else '  ✓'})"

                if done % 50 == 0:
                    logger.info(f"Progress: {done}/{total} ({updated} ok, {failed} failed)")

        # Rate limit between batches
        time.sleep(0.3)

    result = {
        "message": f"Sync complete: {updated} updated, {failed} failed, {total_rows} new rows",
        "updated": updated,
        "failed": failed,
        "total_new_rows": total_rows,
        "failed_tickers": failed_tickers[:20],
    }
    logger.info(result["message"])
    return result


def sync_full_history(range_str="2y", max_workers=3):
    """Sync ALL universe tickers with full history (initial setup or rebalance)."""
    tickers = _get_all_universe_tickers()
    logger.info(f"Full history sync: {len(tickers)} tickers, range={range_str}")

    # Force all tickers (not just stale)
    total = len(tickers)
    updated = 0
    failed = 0
    total_rows = 0

    for batch_start in range(0, total, max_workers):
        batch = tickers[batch_start:batch_start + max_workers]

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(sync_ticker, t, range_str): t for t in batch}
            for future in as_completed(futures):
                ticker, n_rows, error = future.result()
                if error:
                    failed += 1
                else:
                    updated += 1
                    total_rows += n_rows

                done = updated + failed
                if done % 50 == 0:
                    logger.info(f"Full sync: {done}/{total} ({updated} ok, {failed} failed)")

        time.sleep(0.5)  # Longer pause for full sync

    return {
        "message": f"Full sync: {updated} updated, {failed} failed, {total_rows} rows",
        "updated": updated,
        "failed": failed,
        "total_new_rows": total_rows,
    }


if __name__ == "__main__":
    # CLI usage: python nse_sync.py [--full]
    import sys
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

    if "--full" in sys.argv:
        print("Running FULL 2-year sync for all NIFTY 500 tickers...")
        result = sync_full_history()
    else:
        print("Running incremental sync for stale tickers...")
        result = sync_nifty500()

    print(f"\nResult: {result}")
