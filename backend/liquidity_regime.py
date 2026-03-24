"""
Liquidity Regime — IV Footprint (Smart Money) computation.
Scans OHLCV data for Institutional Volume, Pocket Pivot, and Bull Snort signals.
"""
import sqlite3, os, logging
from datetime import datetime, timedelta
import numpy as np

logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), "breadth_data.db")


def compute_iv_footprint(market: str = "India", days: int = 30) -> list:
    """
    For each of the last `days` trading days, count how many tickers had:
      - IV (Institutional Volume): volume > 2x 50-day avg AND close in top 25% of range AND close > prev close
      - PPV (Pocket Pivot Volume): volume > max of last 10 down-day volumes AND close > prev close
      - Bull Snort: gap up > 3% on volume > 1.5x avg
    Returns list of {date, iv_count, ppv_count, bs_count}.
    """
    if not os.path.exists(DB_PATH):
        return []

    conn = sqlite3.connect(DB_PATH)
    try:
        # Get tickers for this market
        cur = conn.execute("SELECT DISTINCT ticker FROM ohlcv LIMIT 5000")
        all_tickers = [r[0] for r in cur.fetchall()]

        # We need ~80 trading days of data to compute 50-day averages
        lookback = days + 60
        cutoff = (datetime.now() - timedelta(days=int(lookback * 1.6))).strftime("%Y-%m-%d")

        # Bulk load recent data for all tickers
        query = """
            SELECT ticker, date, open, high, low, close, volume
            FROM ohlcv WHERE date >= ? ORDER BY ticker, date
        """
        rows = conn.execute(query, (cutoff,)).fetchall()
    finally:
        conn.close()

    if not rows:
        return []

    # Organize by ticker
    from collections import defaultdict
    ticker_data = defaultdict(list)
    for ticker, dt, o, h, l, c, v in rows:
        ticker_data[ticker].append((dt, o, h, l, c, v))

    # Get the last N unique dates (r[1] = date column)
    all_dates = sorted(set(r[1] for r in rows))
    target_dates = all_dates[-days:] if len(all_dates) >= days else all_dates
    target_set = set(target_dates)

    # Count signals per date
    date_counts = {d: {"iv": 0, "ppv": 0, "bs": 0} for d in target_dates}

    for ticker, records in ticker_data.items():
        if len(records) < 55:
            continue

        for i in range(50, len(records)):
            dt, o, h, l, c, v = records[i]
            if dt not in target_set:
                continue

            _, _, _, _, prev_c, prev_v = records[i - 1]
            if not c or not prev_c or c <= 0 or prev_c <= 0:
                continue

            # 50-day volume average
            vols_50 = [r[5] for r in records[max(0, i - 50):i] if r[5] and r[5] > 0]
            avg_vol = np.mean(vols_50) if vols_50 else 0

            if not v or v <= 0 or avg_vol <= 0:
                continue

            vol_ratio = v / avg_vol
            rng = h - l if h and l and h > l else 1
            close_position = (c - l) / rng if rng > 0 else 0

            # IV: volume > 2x avg, close in top 25% of range, close > prev close
            if vol_ratio > 2.0 and close_position > 0.75 and c > prev_c:
                date_counts[dt]["iv"] += 1

            # PPV: volume > max of last 10 down-day volumes, close > prev close
            if c > prev_c:
                down_vols = []
                for j in range(max(0, i - 10), i):
                    _, _, _, _, cj, vj = records[j]
                    if j > 0:
                        _, _, _, _, cprev, _ = records[j - 1]
                        if cj and cprev and cj < cprev and vj and vj > 0:
                            down_vols.append(vj)
                if down_vols and v > max(down_vols):
                    date_counts[dt]["ppv"] += 1

            # Bull Snort: gap up > 3% on volume > 1.5x avg
            gap_pct = (o - prev_c) / prev_c * 100 if o and prev_c else 0
            if gap_pct > 3.0 and vol_ratio > 1.5 and c > o:
                date_counts[dt]["bs"] += 1

    result = []
    for d in target_dates:
        counts = date_counts.get(d, {"iv": 0, "ppv": 0, "bs": 0})
        result.append({
            "date": d,
            "iv_count": counts["iv"],
            "ppv_count": counts["ppv"],
            "bs_count": counts["bs"],
        })

    return result
