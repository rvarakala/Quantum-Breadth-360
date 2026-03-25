"""
Stock Metrics Panel — compute 9 key metrics for any ticker from DB data.
Metrics: Sector, Market Cap, RS Rating, Trend Template, EPS, RelVolume,
         Off 52w High, Volatility ATR%, Market Condition.
"""

import sqlite3
import pathlib
import logging
import numpy as np
import pandas as pd
from datetime import datetime, timezone

logger = logging.getLogger(__name__)
DB_PATH = pathlib.Path(__file__).parent / "breadth_data.db"


def _query_ohlcv(ticker: str) -> pd.DataFrame:
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    try:
        df = pd.read_sql_query(
            "SELECT date, open, high, low, close, volume FROM ohlcv "
            "WHERE ticker=? ORDER BY date ASC", conn, params=(ticker,))
    finally:
        conn.close()
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date").reset_index(drop=True)


def _get_sector(ticker: str) -> str:
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=10)
        row = conn.execute("SELECT sector FROM sector_map WHERE ticker=?", (ticker,)).fetchone()
        conn.close()
        return row[0] if row else ""
    except Exception:
        return ""


def _get_mcap(ticker: str) -> dict:
    try:
        from market_cap import get_mcap_for_ticker, format_mcap, get_mcap_tier
        data = get_mcap_for_ticker(ticker)
        if data:
            return {
                "mcap_cr": data["mcap_cr"],
                "mcap_tier": data["mcap_tier"],
                "mcap_formatted": format_mcap(data["mcap_cr"]),
            }
    except Exception:
        pass
    return {"mcap_cr": 0, "mcap_tier": "", "mcap_formatted": ""}


def _compute_rs_rating(ticker: str, df: pd.DataFrame) -> tuple:
    """Compute a quick RS rating (0-99) based on 3-month performance rank."""
    if len(df) < 63:
        return 50, "ABOVE AVG"

    close = df["close"].values
    perf_3m = (close[-1] / close[-63] - 1) * 100 if close[-63] > 0 else 0

    # Fast approach: get last close and close ~63 bars back for all India tickers
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=15)
        # Get the two most recent dates needed for 3-month lookback
        all_df = pd.read_sql_query("""
            SELECT ticker, date, close FROM ohlcv
            WHERE market='India'
              AND date >= (SELECT date FROM ohlcv WHERE market='India' ORDER BY date DESC LIMIT 1 OFFSET 70)
            ORDER BY ticker, date ASC
        """, conn)
        conn.close()

        perfs = []
        for t, grp in all_df.groupby("ticker"):
            if len(grp) < 50:
                continue
            closes = grp["close"].values
            if closes[-1] > 0 and closes[0] > 0:
                p = (closes[-1] / closes[0] - 1) * 100
                perfs.append((t, p))

        if len(perfs) < 10:
            rs = max(0, min(99, int(50 + perf_3m)))
        else:
            perfs.sort(key=lambda x: x[1])
            total = len(perfs)
            rank = next((i for i, (t, _) in enumerate(perfs) if t == ticker), total // 2)
            rs = int(rank / total * 99)
    except Exception:
        rs = max(0, min(99, int(50 + perf_3m)))

    if rs >= 90:
        status = "LEADER"
    elif rs >= 80:
        status = "STRONG"
    elif rs >= 60:
        status = "ABOVE AVG"
    else:
        status = "WEAK"

    return rs, status


def _compute_trend_template(df: pd.DataFrame, rs_rating: int) -> tuple:
    """Minervini 8-criteria trend template check + stage classification."""
    if len(df) < 252:
        return "N/A", 0, "Unknown"

    close = df["close"].values
    high = df["high"].values
    low = df["low"].values
    n = len(df)
    price = close[-1]

    sma50 = np.mean(close[-50:])
    sma200 = np.mean(close[-200:])
    sma200_20ago = np.mean(close[-220:-20]) if n >= 220 else sma200

    high_52w = np.max(high[-252:])
    low_52w = np.min(low[-252:])

    criteria = [
        price > sma50,                                         # 1. Price > 50 DMA
        price > sma200,                                        # 2. Price > 200 DMA
        sma50 > sma200,                                        # 3. 50 DMA > 200 DMA
        sma200 > sma200_20ago,                                 # 4. 200 DMA trending up
        (high_52w - price) / high_52w <= 0.25 if high_52w > 0 else False,  # 5. Within 25% of 52w high
        (price - low_52w) / low_52w >= 0.30 if low_52w > 0 else False,     # 6. At least 30% above 52w low
        rs_rating >= 70,                                       # 7. RS >= 70
        price > sma50,                                         # 8. Price above 50 DMA (duplicate reinforcement)
    ]

    met = sum(criteria)

    if met >= 6:
        template = "PASS"
    elif met >= 4:
        template = "PARTIAL"
    else:
        template = "FAIL"

    # Stage classification
    sma50_rising = sma50 > np.mean(close[-70:-20]) if n >= 70 else True
    if price > sma50 and sma50 > sma200 and sma200 > sma200_20ago:
        stage = "Stage 2"
    elif price < sma50 and price < sma200:
        stage = "Stage 4"
    elif price < sma50 and sma50 > sma200:
        stage = "Stage 3"
    else:
        stage = "Stage 1"

    return template, met, stage


def _compute_rel_volume(df: pd.DataFrame) -> tuple:
    """Relative volume: Volume / MA(Volume, 50)."""
    if len(df) < 50:
        return 0, "N/A"
    vol = df["volume"].values
    avg_vol = np.mean(vol[-50:])
    if avg_vol <= 0:
        return 0, "N/A"
    rv = vol[-1] / avg_vol
    rv = round(rv, 2)

    if rv > 2.0:
        status = "Inst. Buy"
    elif rv > 1.5:
        status = "Above Avg"
    elif rv >= 1.0:
        status = "Normal"
    else:
        status = "Dry Up"

    return rv, status


def _compute_off_52w_high(df: pd.DataFrame) -> tuple:
    if len(df) < 252:
        bars = len(df)
    else:
        bars = 252
    high_52w = np.max(df["high"].values[-bars:])
    price = df["close"].values[-1]
    if high_52w <= 0:
        return 0, "N/A"
    pct = (price - high_52w) / high_52w * 100
    pct = round(pct, 2)

    if pct >= -5:
        status = "Breakout Ready"
    elif pct >= -15:
        status = "Pullback"
    else:
        status = "Deep Correction"

    return pct, status


def _compute_atr_pct(df: pd.DataFrame) -> tuple:
    """ATR(14) / Close * 100."""
    if len(df) < 15:
        return 0, "N/A"
    high = df["high"].values[-15:]
    low = df["low"].values[-15:]
    close = df["close"].values[-15:]

    prev_close = np.roll(close, 1)
    prev_close[0] = close[0]

    tr = np.maximum(high - low, np.maximum(np.abs(high - prev_close), np.abs(low - prev_close)))
    atr14 = np.mean(tr[-14:])
    atr_pct = round(atr14 / close[-1] * 100, 2) if close[-1] > 0 else 0

    if atr_pct < 3:
        status = "Low"
    elif atr_pct <= 6:
        status = "Medium"
    else:
        status = "High"

    return atr_pct, status


def _get_market_condition() -> tuple:
    """Get market condition from cached breadth data."""
    try:
        from cache import get_cache
        cached = get_cache("breadth_INDIA")
        if cached:
            score = cached.get("score", 50)
            vix = cached.get("vix", 15)

            if score >= 60:
                condition = "OPTIMAL"
            elif score >= 40:
                condition = "CAUTION"
            else:
                condition = "ADVERSE"

            if vix > 25:
                vol_status = "Extreme Vol"
            elif vix > 18:
                vol_status = "High Vol"
            else:
                vol_status = "Normal Vol"

            return condition, vol_status
    except Exception:
        pass
    return "N/A", "N/A"


def _compute_avg_turnover(df: pd.DataFrame) -> float:
    """Average daily turnover in Cr (close * volume / 1e7) over last 20 days."""
    if len(df) < 20:
        return 0
    close = df["close"].values[-20:]
    vol = df["volume"].values[-20:]
    turnover = close * vol / 1e7  # approx Cr
    return round(np.mean(turnover), 2)


def fetch_eps_yahoo(ticker: str) -> tuple:
    """Fetch EPS (TTM) and PE. Tries TradingView batch first, then per-ticker, then yfinance."""
    # ── 1. Try TradingView batch data (instant, no network call) ──────────────
    try:
        from tv_fundamentals import get_batch_fundamental
        batch = get_batch_fundamental(ticker)
        if batch and batch.get("eps_ttm") is not None:
            eps = batch["eps_ttm"]
            return round(eps, 2), "Positive" if eps > 0 else "Negative"
    except Exception as e:
        logger.debug(f"TV batch EPS failed for {ticker}: {e}")

    # ── 2. Try TradingView per-ticker detail (cached 24h) ─────────────────────
    try:
        from tv_fundamentals import fetch_ticker_detail
        data = fetch_ticker_detail(ticker)
        if data and not data.get("error"):
            quarterly = data.get("quarterly", [])
            if quarterly:
                eps = quarterly[-1].get("eps")
                if eps is not None:
                    return round(eps, 2), "Positive" if eps > 0 else "Negative"
            ratios = data.get("ratios", {})
            if ratios.get("pe_ratio") and data.get("price"):
                try:
                    eps = round(float(data["price"]) / float(ratios["pe_ratio"]), 2)
                    return eps, "Positive" if eps > 0 else "Negative"
                except: pass
    except Exception as e:
        logger.debug(f"TV detail EPS failed for {ticker}: {e}")

    # ── 3. Fallback: yfinance cache ───────────────────────────────────────────
    try:
        from fundamentals_sync import get_eps_for_ticker
        cached = get_eps_for_ticker(ticker)
        if cached and cached.get("eps_ttm") is not None:
            eps = cached["eps_ttm"]
            return round(eps, 2), "Positive" if eps > 0 else "Negative"
    except: pass

    return None, "N/A"


def fetch_pe_ratio(ticker: str) -> float:
    """Fetch PE ratio from screener.in cache or fundamentals_sync."""
    # Try screener.in
    try:
        
        data = fetch_screener_data(ticker)
        if data and data.get("ratios"):
            for key in ["Stock P/E", "PE", "P/E"]:
                pe_str = data["ratios"].get(key)
                if pe_str:
                    clean = str(pe_str).replace(",", "").replace("₹", "").strip()
                    try:
                        return round(float(clean), 2)
                    except: pass
    except: pass
    
    # Fallback: fundamentals_sync DB cache
    try:
        from fundamentals_sync import get_eps_for_ticker
        cached = get_eps_for_ticker(ticker)
        if cached and cached.get("pe_ratio") is not None:
            return round(float(cached["pe_ratio"]), 2)
    except: pass
    
    return None


def compute_stock_metrics(ticker: str) -> dict:
    """Compute all 9 metrics for a ticker. EPS is included synchronously (caller can make it async)."""
    ticker = ticker.upper().strip()

    df = _query_ohlcv(ticker)
    if df.empty:
        return {"error": f"No OHLCV data for {ticker}", "ticker": ticker}

    # 1. Sector
    sector = _get_sector(ticker)

    # 2. Market Cap
    mcap = _get_mcap(ticker)

    # 3. RS Rating
    rs_rating, rs_status = _compute_rs_rating(ticker, df)

    # 4. Trend Template
    trend_template, criteria_met, stage = _compute_trend_template(df, rs_rating)

    # 5. EPS + PE — fetch from screener.in cache or yfinance
    eps_ttm, eps_status = fetch_eps_yahoo(ticker)
    pe_ratio = fetch_pe_ratio(ticker)

    # 6. Rel Volume
    rel_volume, rv_status = _compute_rel_volume(df)

    # 7. Off 52w High
    off_52w_pct, off_52w_status = _compute_off_52w_high(df)

    # 8. ATR%
    atr_pct, atr_status = _compute_atr_pct(df)

    # 9. Market Condition
    market_condition, vol_status = _get_market_condition()

    # Bonus: avg turnover
    avg_turnover = _compute_avg_turnover(df)

    def _py(v):
        """Convert numpy types to native Python for JSON serialization."""
        if isinstance(v, (np.integer,)):
            return int(v)
        if isinstance(v, (np.floating,)):
            return float(v)
        if isinstance(v, np.bool_):
            return bool(v)
        return v

    return {
        "ticker": ticker,
        "sector": sector,
        "mcap_cr": _py(mcap["mcap_cr"]),
        "mcap_tier": mcap["mcap_tier"],
        "mcap_formatted": mcap["mcap_formatted"],
        "rs_rating": _py(rs_rating),
        "rs_status": rs_status,
        "trend_template": trend_template,
        "trend_criteria_met": _py(criteria_met),
        "stage": stage,
        "eps_ttm": _py(eps_ttm) if eps_ttm is not None else None,
        "eps_status": eps_status,
        "pe_ratio": _py(pe_ratio) if pe_ratio is not None else None,
        "rel_volume": _py(rel_volume),
        "rel_volume_status": rv_status,
        "off_52w_high_pct": _py(off_52w_pct),
        "off_52w_status": off_52w_status,
        "atr_pct": _py(atr_pct),
        "atr_status": atr_status,
        "market_condition": market_condition,
        "market_vol_status": vol_status,
        "avg_turnover_cr": _py(avg_turnover),
    }


def compute_eps_async(ticker: str) -> dict:
    """Fetch EPS from Yahoo — called separately so chart loading isn't blocked."""
    ticker = ticker.upper().strip()
    eps_ttm, eps_status = fetch_eps_yahoo(ticker)
    return {"ticker": ticker, "eps_ttm": eps_ttm, "eps_status": eps_status}
