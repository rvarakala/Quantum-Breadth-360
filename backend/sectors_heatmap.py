"""
Sector Rotation Heatmap — computes per-sector performance metrics.
"""
import sqlite3, pathlib
from datetime import datetime, timedelta

DB_PATH = pathlib.Path(__file__).parent / "breadth_data.db"

def compute_sector_heatmap(market: str = "India", period: str = "1m") -> dict:
    """Return sector-level performance data for treemap rendering."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    # 1. Load sector map
    rows = cur.execute("SELECT ticker, sector FROM sector_map WHERE sector IS NOT NULL AND sector != ''").fetchall()
    if not rows:
        conn.close()
        return {"sectors": [], "timestamp": datetime.utcnow().isoformat()}

    sector_tickers = {}
    for r in rows:
        sec = r["sector"]
        sector_tickers.setdefault(sec, []).append(r["ticker"])

    # 2. Determine lookback dates
    days_map = {"1d": 1, "1w": 7, "1m": 30, "3m": 90}
    lookback_days = days_map.get(period, 30)
    is_daily = period == "1d"

    # Get latest date in DB
    latest_row = cur.execute("SELECT MAX(date) as d FROM ohlcv").fetchone()
    if not latest_row or not latest_row["d"]:
        conn.close()
        return {"sectors": [], "timestamp": datetime.utcnow().isoformat()}
    latest_date = latest_row["d"]

    lookback_date = (datetime.strptime(latest_date, "%Y-%m-%d") - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    dma50_date = (datetime.strptime(latest_date, "%Y-%m-%d") - timedelta(days=80)).strftime("%Y-%m-%d")
    dma200_date = (datetime.strptime(latest_date, "%Y-%m-%d") - timedelta(days=300)).strftime("%Y-%m-%d")

    sectors_out = []

    for sector, tickers in sector_tickers.items():
        if not tickers:
            continue

        placeholders = ",".join(["?"] * len(tickers))
        changes = []
        above_50 = 0
        above_200 = 0
        valid_50 = 0
        valid_200 = 0
        stock_details = []

        for ticker in tickers:
            # Get latest close and lookback close
            if is_daily:
                # For 1D: compare last two trading days
                last_two = cur.execute(
                    "SELECT close FROM ohlcv WHERE ticker=? ORDER BY date DESC LIMIT 2",
                    (ticker,)
                ).fetchall()
                if len(last_two) < 2 or not last_two[0]["close"] or not last_two[1]["close"]:
                    continue
                close_now = float(last_two[0]["close"])
                close_prev = float(last_two[1]["close"])
                if close_prev > 0:
                    chg = ((close_now - close_prev) / close_prev) * 100
                    changes.append(chg)
                    stock_details.append({"ticker": ticker, "change": round(chg, 2)})
            else:
                latest = cur.execute(
                    "SELECT close FROM ohlcv WHERE ticker=? ORDER BY date DESC LIMIT 1",
                    (ticker,)
                ).fetchone()
                if not latest or not latest["close"]:
                    continue

                past = cur.execute(
                    "SELECT close FROM ohlcv WHERE ticker=? AND date<=? ORDER BY date DESC LIMIT 1",
                    (ticker, lookback_date)
                ).fetchone()

                close_now = float(latest["close"])
                if past and past["close"] and float(past["close"]) > 0:
                    chg = ((close_now - float(past["close"])) / float(past["close"])) * 100
                    changes.append(chg)
                    stock_details.append({"ticker": ticker, "change": round(chg, 2)})

            # 50 DMA check
            rows_50 = cur.execute(
                "SELECT close FROM ohlcv WHERE ticker=? AND date>=? ORDER BY date",
                (ticker, dma50_date)
            ).fetchall()
            if len(rows_50) >= 30:
                closes = [float(r["close"]) for r in rows_50[-50:] if r["close"]]
                if len(closes) >= 30:
                    dma50_val = sum(closes[-50:]) / len(closes[-50:]) if len(closes) >= 50 else sum(closes) / len(closes)
                    valid_50 += 1
                    if close_now > dma50_val:
                        above_50 += 1

            # 200 DMA check
            rows_200 = cur.execute(
                "SELECT close FROM ohlcv WHERE ticker=? AND date>=? ORDER BY date",
                (ticker, dma200_date)
            ).fetchall()
            if len(rows_200) >= 100:
                closes200 = [float(r["close"]) for r in rows_200[-200:] if r["close"]]
                if len(closes200) >= 100:
                    dma200_val = sum(closes200[-200:]) / len(closes200[-200:]) if len(closes200) >= 200 else sum(closes200) / len(closes200)
                    valid_200 += 1
                    if close_now > dma200_val:
                        above_200 += 1

        if not changes:
            continue

        avg_change = sum(changes) / len(changes)
        pct_above_50 = round((above_50 / valid_50 * 100) if valid_50 > 0 else 0, 1)
        pct_above_200 = round((above_200 / valid_200 * 100) if valid_200 > 0 else 0, 1)

        # Top 3 performers
        stock_details.sort(key=lambda x: x["change"], reverse=True)
        top_stocks = stock_details[:3]

        health = "hot" if pct_above_50 >= 60 else "warm" if pct_above_50 >= 40 else "cold"

        sectors_out.append({
            "sector": sector,
            "avg_change": round(avg_change, 2),
            "stock_count": len(changes),
            "pct_above_50dma": pct_above_50,
            "pct_above_200dma": pct_above_200,
            "top_stocks": top_stocks,
            "health": health,
        })

    conn.close()

    # Sort by avg_change desc
    sectors_out.sort(key=lambda x: x["avg_change"], reverse=True)

    return {
        "sectors": sectors_out,
        "period": period,
        "market": market,
        "latest_date": latest_date,
        "timestamp": datetime.utcnow().isoformat(),
    }
