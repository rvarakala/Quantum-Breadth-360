"""
Stockbee Market Breadth — daily 4%+ movers, rolling ratios, momentum, T2108, regime.
"""
import logging
import pathlib
from datetime import datetime, timezone
from collections import defaultdict

logger = logging.getLogger(__name__)


def _compute_stockbee(market: str) -> dict:
    """
    Compute Stockbee Market Breadth metrics for the given market.
    Loads ALL tickers from the SQLite DB (not just NIFTY 500 universe).
    Returns dict with today's metrics, 60-day history, and regime classification.
    """
    import sqlite3

    if market.upper() != "INDIA":
        return {"error": f"Stockbee MB is only available for India market (DB has India data only). '{market}' is not supported."}

    db_path = pathlib.Path(__file__).parent / "breadth_data.db"
    if not db_path.exists():
        return {"error": "Database not found"}

    try:
        conn = sqlite3.connect(str(db_path), timeout=30)
        rows = conn.execute("""
            SELECT ticker, date, close FROM ohlcv
            WHERE market='India' AND date >= date('now', '-750 days')
            ORDER BY ticker, date
        """).fetchall()
        conn.close()
    except Exception as e:
        return {"error": f"DB query failed: {e}"}

    if not rows:
        return {"error": "No data found in database"}

    # Group by ticker
    ticker_data = defaultdict(list)
    for ticker, dt, close in rows:
        if close and close > 0:
            ticker_data[ticker].append((dt, float(close)))

    # Get sorted unique trading dates from all tickers
    all_dates = sorted(set(dt for entries in ticker_data.values() for dt, _ in entries))
    if len(all_dates) < 70:
        return {"error": f"Insufficient trading days: {len(all_dates)} (need 70+)"}

    # Build per-ticker close price lookup: {ticker: {date: close}}
    ticker_closes = {}
    for ticker, entries in ticker_data.items():
        if len(entries) >= 30:  # need enough history
            ticker_closes[ticker] = dict(entries)

    total_tickers = len(ticker_closes)
    logger.info(f"Stockbee: {total_tickers} tickers, {len(all_dates)} trading dates")

    # Compute metrics for last ~80 trading days (to have 60 days of output + lookback)
    compute_days = min(len(all_dates), 130)  # extra buffer for 65-day lookback
    recent_dates = all_dates[-compute_days:]

    # Pre-build date index for offset lookups
    date_to_idx = {d: i for i, d in enumerate(recent_dates)}

    daily_metrics = []

    for i, d in enumerate(recent_dates):
        if i < 65:  # need 65 days lookback for quarter metric
            continue

        up_4 = 0
        dn_4 = 0
        up_25_qtr = 0
        dn_25_qtr = 0
        up_25_month = 0
        dn_25_month = 0
        up_50_month = 0
        dn_50_month = 0
        up_13_34d = 0
        dn_13_34d = 0
        above_40dma = 0
        valid_stocks = 0
        valid_40dma = 0

        prev_date = recent_dates[i - 1] if i > 0 else None
        date_65 = recent_dates[i - 65] if i >= 65 else None
        date_21 = recent_dates[i - 21] if i >= 21 else None
        date_34 = recent_dates[i - 34] if i >= 34 else None

        for ticker, closes in ticker_closes.items():
            close_today = closes.get(d)
            if close_today is None:
                continue
            valid_stocks += 1

            # Daily % change
            close_prev = closes.get(prev_date) if prev_date else None
            if close_prev and close_prev > 0:
                daily_pct = (close_today - close_prev) / close_prev * 100
                if daily_pct >= 4.0:
                    up_4 += 1
                if daily_pct <= -4.0:
                    dn_4 += 1

            # Quarter (65 trading days) % change
            if date_65:
                close_65 = closes.get(date_65)
                if close_65 and close_65 > 0:
                    qtr_pct = (close_today - close_65) / close_65 * 100
                    if qtr_pct >= 25.0:
                        up_25_qtr += 1
                    if qtr_pct <= -25.0:
                        dn_25_qtr += 1

            # Month (21 trading days) % change
            if date_21:
                close_21 = closes.get(date_21)
                if close_21 and close_21 > 0:
                    month_pct = (close_today - close_21) / close_21 * 100
                    if month_pct >= 25.0:
                        up_25_month += 1
                    if month_pct <= -25.0:
                        dn_25_month += 1
                    if month_pct >= 50.0:
                        up_50_month += 1
                    if month_pct <= -50.0:
                        dn_50_month += 1

            # 34 trading days % change
            if date_34:
                close_34 = closes.get(date_34)
                if close_34 and close_34 > 0:
                    d34_pct = (close_today - close_34) / close_34 * 100
                    if d34_pct >= 13.0:
                        up_13_34d += 1
                    if d34_pct <= -13.0:
                        dn_13_34d += 1

            # T2108: % above 40-day moving average
            # Collect last 40 closes for this ticker ending at day i
            closes_40 = []
            for j in range(max(0, i - 39), i + 1):
                c = closes.get(recent_dates[j])
                if c is not None:
                    closes_40.append(c)
            if len(closes_40) >= 30:  # need at least 30 of 40 days
                valid_40dma += 1
                ma40 = sum(closes_40) / len(closes_40)
                if close_today > ma40:
                    above_40dma += 1

        t2108 = round(above_40dma / valid_40dma * 100, 1) if valid_40dma > 0 else 0

        daily_metrics.append({
            "date": d,
            "up_4pct": up_4,
            "dn_4pct": dn_4,
            "up_25pct_qtr": up_25_qtr,
            "dn_25pct_qtr": dn_25_qtr,
            "up_25pct_month": up_25_month,
            "dn_25pct_month": dn_25_month,
            "up_50pct_month": up_50_month,
            "dn_50pct_month": dn_50_month,
            "up_13pct_34d": up_13_34d,
            "dn_13pct_34d": dn_13_34d,
            "t2108": t2108,
            "total_stocks": valid_stocks,
        })

    if not daily_metrics:
        return {"error": "Could not compute metrics — insufficient data"}

    # Compute rolling ratios (5d and 10d)
    for i, m in enumerate(daily_metrics):
        # 5-day ratio
        up5 = sum(daily_metrics[j]["up_4pct"] for j in range(max(0, i - 4), i + 1))
        dn5 = sum(daily_metrics[j]["dn_4pct"] for j in range(max(0, i - 4), i + 1))
        m["ratio_5d"] = round(min(up5 / dn5, 10.0), 2) if dn5 > 0 else (10.0 if up5 > 0 else 1.0)

        # 10-day ratio
        up10 = sum(daily_metrics[j]["up_4pct"] for j in range(max(0, i - 9), i + 1))
        dn10 = sum(daily_metrics[j]["dn_4pct"] for j in range(max(0, i - 9), i + 1))
        m["ratio_10d"] = round(min(up10 / dn10, 10.0), 2) if dn10 > 0 else (10.0 if up10 > 0 else 1.0)

    # Get last 60 days for history output
    history = daily_metrics[-60:]
    today = daily_metrics[-1]

    # Regime classification
    r5 = today["ratio_5d"]
    r10 = today["ratio_10d"]
    t = today["t2108"]

    if r5 > 2.0 and r10 > 1.5:
        regime = "BULL THRUST"
        regime_color = "#22c55e"
    elif r5 > 1.0 and r10 > 1.0 and t > 60:
        regime = "BULLISH"
        regime_color = "#22c55e"
    elif r5 > 1.0 and r10 > 1.0 and 40 <= t <= 60:
        regime = "MODERATE BULL"
        regime_color = "#f59e0b"
    elif r5 < 0.8 and r10 < 0.8 and t < 30:
        regime = "BEARISH"
        regime_color = "#ef4444"
    elif (r5 < 0.8 or r10 < 0.8) and len(daily_metrics) >= 2 and today["t2108"] < daily_metrics[-2]["t2108"]:
        regime = "DISTRIBUTION"
        regime_color = "#ef4444"
    elif 0.8 <= r5 <= 1.2 and 0.8 <= r10 <= 1.2:
        regime = "NEUTRAL"
        regime_color = "#94a3b8"
    else:
        regime = "NEUTRAL"
        regime_color = "#94a3b8"

    return {
        "market": market.upper(),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "universe_size": total_tickers,
        "regime": regime,
        "regime_color": regime_color,
        "today": today,
        "history": history,
    }
