"""
Chart data computation — OHLCV from SQLite, indicators, volume coloring.
"""
import sqlite3
import pathlib
import numpy as np
import pandas as pd
import logging

logger = logging.getLogger(__name__)

DB_PATH = pathlib.Path(__file__).parent / "breadth_data.db"

# ── Volume color constants ────────────────────────────────────────────────────
CLR_GREY      = "#6b7280"   # default/noise
CLR_ORANGE    = "#f97316"   # dry-up / very low volume
CLR_DKGREY    = "#4b5563"   # below average
CLR_GREEN     = "#22c55e"   # above avg up day
CLR_RED       = "#ef4444"   # above avg down day
CLR_BLUE      = "#3b82f6"   # Pocket Pivot
CLR_YELLOW    = "#eab308"   # Bull Snort

# ── Volume parameters (from AFL) ─────────────────────────────────────────────
PPV_PERIOD = 10
MA_PERIOD = 50
LOW_VOL_FRACTION = 5
BULL_SNORT_MULT = 3
BULL_SNORT_RANGE_PCT = 35
HVQ_PERIOD = 63
HVY_PERIOD = 252
HVE_PERIOD = 2520


def _query_ohlcv(ticker: str, extra_days: int = 300, limit: int = 500) -> pd.DataFrame:
    """Fetch raw daily OHLCV from SQLite. Pulls extra_days beyond limit for MA warmup."""
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    try:
        df = pd.read_sql_query(
            "SELECT date, open, high, low, close, volume FROM ohlcv "
            "WHERE ticker=? ORDER BY date ASC",
            conn, params=(ticker,),
        )
    finally:
        conn.close()
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    return df


def _aggregate_weekly(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["week"] = df["date"].dt.isocalendar().year.astype(str) + "-W" + df["date"].dt.isocalendar().week.astype(str).str.zfill(2)
    agg = df.groupby("week", sort=False).agg(
        date=("date", "last"),
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
    ).reset_index(drop=True).sort_values("date").reset_index(drop=True)
    return agg


def _aggregate_monthly(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["month"] = df["date"].dt.to_period("M")
    agg = df.groupby("month", sort=False).agg(
        date=("date", "last"),
        open=("open", "first"),
        high=("high", "max"),
        low=("low", "min"),
        close=("close", "last"),
        volume=("volume", "sum"),
    ).reset_index(drop=True).sort_values("date").reset_index(drop=True)
    return agg


def _sma(series: pd.Series, period: int) -> pd.Series:
    return series.rolling(window=period, min_periods=period).mean()


def _compute_overlays(df: pd.DataFrame) -> dict:
    c = df["close"]
    overlays = {}
    for period, key in [(20, "dma20"), (50, "dma50"), (200, "dma200")]:
        ma = _sma(c, period)
        vals = []
        for i in range(len(df)):
            v = ma.iloc[i]
            if pd.notna(v):
                vals.append({"time": df.iloc[i]["date"].strftime("%Y-%m-%d"), "value": round(float(v), 2)})
        overlays[key] = vals

    # Bollinger Bands (20, 2)
    mid = _sma(c, 20)
    std = c.rolling(window=20, min_periods=20).std()
    bb_upper, bb_lower, bb_mid = [], [], []
    for i in range(len(df)):
        if pd.notna(mid.iloc[i]):
            t = df.iloc[i]["date"].strftime("%Y-%m-%d")
            m = float(mid.iloc[i])
            s = float(std.iloc[i]) if pd.notna(std.iloc[i]) else 0
            bb_mid.append({"time": t, "value": round(m, 2)})
            bb_upper.append({"time": t, "value": round(m + 2 * s, 2)})
            bb_lower.append({"time": t, "value": round(m - 2 * s, 2)})
    overlays["bb_upper"] = bb_upper
    overlays["bb_lower"] = bb_lower
    overlays["bb_mid"] = bb_mid

    # RS line — skip since no NIFTY index in DB
    overlays["rs_line"] = []

    return overlays


def _compute_volume(df: pd.DataFrame) -> dict:
    """Compute volume bars with AFL-style coloring, MA, PPV, Bull Snort, HV labels."""
    n = len(df)
    close = df["close"].values.astype(float)
    high = df["high"].values.astype(float)
    low = df["low"].values.astype(float)
    volume = df["volume"].values.astype(float)

    vol_ma = pd.Series(volume).rolling(window=MA_PERIOD, min_periods=MA_PERIOD).mean().values
    low_vol_thresh = np.where(np.isnan(vol_ma), np.nan, vol_ma / LOW_VOL_FRACTION)

    # Up/down day
    prev_close = np.roll(close, 1)
    prev_close[0] = close[0]
    up_day = close > prev_close
    down_day = close < prev_close

    # Pocket Pivot Volume
    down_vol = np.where(down_day, volume, 0.0)
    # max down-vol in prior PPV_PERIOD bars (shifted by 1)
    down_vol_shifted = np.roll(down_vol, 1)
    down_vol_shifted[0] = 0
    max_down_vol_prior = pd.Series(down_vol_shifted).rolling(window=PPV_PERIOD, min_periods=1).max().values
    is_ppv = up_day & (volume > max_down_vol_prior)

    # Bull Snort
    bar_range = high - low
    bull_snort_close_cond = close >= (high - (BULL_SNORT_RANGE_PCT / 100) * bar_range)
    is_bull_snort = (
        (volume >= BULL_SNORT_MULT * np.where(np.isnan(vol_ma), np.inf, vol_ma))
        & bull_snort_close_cond
        & up_day
    )

    # Color assignment (last match wins, same priority as AFL)
    colors = np.full(n, CLR_GREY, dtype=object)
    for i in range(n):
        v = volume[i]
        ma = vol_ma[i]
        thresh = low_vol_thresh[i]
        if np.isnan(ma):
            continue
        if v < thresh:
            colors[i] = CLR_ORANGE
        elif v < ma:
            colors[i] = CLR_DKGREY
        elif v >= ma and up_day[i] and not is_ppv[i]:
            colors[i] = CLR_GREEN
        elif v >= ma and down_day[i]:
            colors[i] = CLR_RED
        if is_ppv[i]:
            colors[i] = CLR_BLUE
        if is_bull_snort[i]:
            colors[i] = CLR_YELLOW

    # Build bars
    bars = []
    ma50_line = []
    for i in range(n):
        t = df.iloc[i]["date"].strftime("%Y-%m-%d")
        bars.append({"time": t, "value": int(volume[i]), "color": colors[i]})
        if pd.notna(vol_ma[i]):
            ma50_line.append({"time": t, "value": round(float(vol_ma[i]), 0)})

    return {"bars": bars, "ma50": ma50_line}


def _compute_markers(df: pd.DataFrame) -> dict:
    """Compute PPV, Bull Snort, HV labels, VCP, Pivot Points."""
    n = len(df)
    close = df["close"].values.astype(float)
    high = df["high"].values.astype(float)
    low = df["low"].values.astype(float)
    volume = df["volume"].values.astype(float)

    vol_ma = pd.Series(volume).rolling(window=MA_PERIOD, min_periods=MA_PERIOD).mean().values
    prev_close = np.roll(close, 1)
    prev_close[0] = close[0]
    up_day = close > prev_close
    down_day = close < prev_close

    # PPV
    down_vol = np.where(down_day, volume, 0.0)
    down_vol_shifted = np.roll(down_vol, 1)
    down_vol_shifted[0] = 0
    max_down_vol_prior = pd.Series(down_vol_shifted).rolling(window=PPV_PERIOD, min_periods=1).max().values
    is_ppv = up_day & (volume > max_down_vol_prior)

    pocket_pivots = []
    for i in range(n):
        if is_ppv[i]:
            pocket_pivots.append({
                "time": df.iloc[i]["date"].strftime("%Y-%m-%d"),
                "price": float(close[i]),
                "volume": int(volume[i]),
            })

    # Bull Snort
    bar_range = high - low
    bull_snort_close_cond = close >= (high - (BULL_SNORT_RANGE_PCT / 100) * bar_range)
    is_bull_snort = (
        (volume >= BULL_SNORT_MULT * np.where(np.isnan(vol_ma), np.inf, vol_ma))
        & bull_snort_close_cond
        & up_day
    )
    bull_snorts = []
    for i in range(n):
        if is_bull_snort[i]:
            bull_snorts.append({
                "time": df.iloc[i]["date"].strftime("%Y-%m-%d"),
                "price": float(close[i]),
                "volume": int(volume[i]),
            })

    # HV labels
    hv_labels = []
    for i in range(n):
        v = volume[i]
        ma = vol_ma[i]
        pct_above = ((v / ma) - 1) * 100 if pd.notna(ma) and ma > 0 else 0
        label_type = None
        if i >= HVE_PERIOD and v > np.max(volume[max(0, i - HVE_PERIOD):i]):
            label_type = "HVE"
        elif i >= HVY_PERIOD and v > np.max(volume[max(0, i - HVY_PERIOD):i]):
            label_type = "HVY"
        elif i >= HVQ_PERIOD and v > np.max(volume[max(0, i - HVQ_PERIOD):i]):
            label_type = "HVQ"
        if label_type:
            hv_labels.append({
                "time": df.iloc[i]["date"].strftime("%Y-%m-%d"),
                "type": label_type,
                "volume": int(v),
                "pct_above_ma": round(pct_above, 1),
            })

    # VCP — successive contraction in range and volume
    vcp_signals = []
    if n >= 20:
        for i in range(20, n):
            r20 = (np.max(high[i-20:i]) - np.min(low[i-20:i])) / max(np.min(low[i-20:i]), 0.01)
            r10 = (np.max(high[i-10:i]) - np.min(low[i-10:i])) / max(np.min(low[i-10:i]), 0.01)
            r5 = (np.max(high[i-5:i]) - np.min(low[i-5:i])) / max(np.min(low[i-5:i]), 0.01)
            vol5 = np.mean(volume[i-5:i])
            vol20 = np.mean(volume[i-20:i])
            if r5 < r10 < r20 and vol5 < vol20:
                vcp_signals.append({
                    "time": df.iloc[i]["date"].strftime("%Y-%m-%d"),
                    "price": float(close[i]),
                })

    # Pivot Points (from last completed bar)
    if n >= 2:
        last = n - 1
        pp = (high[last] + low[last] + close[last]) / 3
        pivot_points = {
            "pp": round(float(pp), 2),
            "r1": round(float(2 * pp - low[last]), 2),
            "r2": round(float(pp + (high[last] - low[last])), 2),
            "r3": round(float(high[last] + 2 * (pp - low[last])), 2),
            "s1": round(float(2 * pp - high[last]), 2),
            "s2": round(float(pp - (high[last] - low[last])), 2),
            "s3": round(float(low[last] - 2 * (high[last] - pp)), 2),
        }
    else:
        pivot_points = {}

    return {
        "pocket_pivots": pocket_pivots,
        "bull_snorts": bull_snorts,
        "vcp_signals": vcp_signals,
        "pivot_points": pivot_points,
        "hv_labels": hv_labels,
    }


def _compute_rel_volume(df: pd.DataFrame) -> dict:
    """Compute Relative Volume (z-score of volume vs SMA). Period=23, clamped >=0."""
    period = 23
    spike_threshold = 2.0
    volume = df["volume"].values.astype(float)

    avg_vol = pd.Series(volume).rolling(window=period, min_periods=period).mean().values
    std_vol = pd.Series(volume).rolling(window=period, min_periods=period).std().values

    bars = []
    for i in range(len(df)):
        t = df.iloc[i]["date"].strftime("%Y-%m-%d")
        if pd.notna(avg_vol[i]) and pd.notna(std_vol[i]) and std_vol[i] > 0:
            rv = (volume[i] - avg_vol[i]) / std_vol[i]
            rv = max(rv, 0.0)  # clamp negative to 0
            is_spike = rv > spike_threshold
            color = "#06b6d4" if is_spike else "#4b5563"  # cyan for spike, grey for normal
            bars.append({"time": t, "value": round(rv, 2), "color": color})
        else:
            bars.append({"time": t, "value": 0, "color": "#4b5563"})

    return {"bars": bars, "spike_threshold": spike_threshold}


def _compute_iv_signals(df: pd.DataFrame) -> list:
    """Compute Institutional Volume signals per AFL logic. Returns last 10 signals."""
    n = len(df)
    if n < 11:
        return []

    close = df["close"].values.astype(float)
    high = df["high"].values.astype(float)
    low = df["low"].values.astype(float)
    volume = df["volume"].values.astype(float)

    # HHV(Volume, 10)
    max_vol_10 = pd.Series(volume).rolling(window=10, min_periods=10).max().values
    # MA(Volume, 10)
    avg_vol_10 = pd.Series(volume).rolling(window=10, min_periods=10).mean().values

    prev_close = np.roll(close, 1)
    prev_close[0] = close[0]
    prev_max_vol = np.roll(max_vol_10, 1)  # Ref(MaxVol, -1)

    signals = []
    for i in range(10, n):
        if np.isnan(prev_max_vol[i]) or np.isnan(avg_vol_10[i]):
            continue
        vol_avg_ok = avg_vol_10[i] > 30000  # VolAvg filter
        vol_spike = volume[i] > prev_max_vol[i] * 2  # Vol > Ref(MaxVol,-1)*2
        up_day = close[i] > prev_close[i]  # Cl = Close > Ref(Close,-1)
        bar_range = high[i] - low[i]
        dcr = close[i] >= (bar_range / 2 + low[i]) if bar_range > 0 else True  # Close in upper half

        if vol_spike and vol_avg_ok and up_day and dcr:
            signals.append({
                "time": df.iloc[i]["date"].strftime("%Y-%m-%d"),
                "high": round(float(high[i]), 2),
                "low": round(float(low[i]), 2),
                "volume": int(volume[i]),
            })

    # Return only last 10 signals
    return signals[-10:]


def _compute_fvg_zones(df: pd.DataFrame) -> list:
    """Compute Bullish FVG (BISI) zones per AFL logic."""
    n = len(df)
    if n < 3:
        return []

    close = df["close"].values.astype(float)
    opn = df["open"].values.astype(float)
    high = df["high"].values.astype(float)
    low = df["low"].values.astype(float)

    zones = []
    for i in range(2, n):
        # Bar -1 (second candle) is green: Close[-1] > Open[-1]
        second_green = close[i - 1] > opn[i - 1]
        # Current bar is green: Close > Open
        third_green = close[i] > opn[i]
        # Gap between first and third: Low[current] - High[-2] > 0
        gap = low[i] - high[i - 2]
        if second_green and third_green and gap > 0:
            zones.append({
                "time": df.iloc[i]["date"].strftime("%Y-%m-%d"),
                "upper": round(float(low[i]), 2),      # zone upper = current bar's Low
                "lower": round(float(high[i - 2]), 2),  # zone lower = High[-2]
            })

    return zones


def _auto_sync_if_stale(ticker: str):
    """Auto-fetch latest data from Yahoo v8 if ticker is behind the market's latest date."""
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=10)
        # Get this ticker's latest date
        row = conn.execute("SELECT MAX(date) FROM ohlcv WHERE ticker=?", (ticker,)).fetchone()
        ticker_latest = row[0] if row and row[0] else None
        # Get the market's latest date (what the freshest tickers have)
        market_row = conn.execute("SELECT MAX(date) FROM ohlcv WHERE market='India'").fetchone()
        market_latest = market_row[0] if market_row and market_row[0] else None
        conn.close()
        
        if ticker_latest and market_latest and ticker_latest >= market_latest:
            return  # ticker is as fresh as the market
        
        # Ticker is behind — fetch latest data
        from nse_sync import sync_ticker
        t, n, err = sync_ticker(ticker, "1mo")
        if n > 0:
            logger.info(f"Auto-synced {ticker}: {n} new rows (was {ticker_latest}, market at {market_latest})")
    except Exception as e:
        logger.debug(f"Auto-sync failed for {ticker}: {e}")

def get_chart_data(ticker: str, tf: str = "daily", days: int = None) -> dict:
    """Main entry point — returns full chart payload for a ticker."""
    ticker = ticker.upper().strip()
    # Auto-sync if data is stale
    _auto_sync_if_stale(ticker)
    if not ticker:
        return {"error": "No ticker provided"}

    default_days = {"daily": 500, "weekly": 260, "monthly": 120}
    if days is None:
        days = default_days.get(tf, 500)

    df = _query_ohlcv(ticker)
    if df.empty:
        return {"error": f"No data for {ticker}"}

    # Aggregate if needed
    if tf == "weekly":
        df = _aggregate_weekly(df)
    elif tf == "monthly":
        df = _aggregate_monthly(df)

    # Trim to requested number of bars (keep extra for MA warmup, trim output later)
    warmup = max(200, MA_PERIOD, HVE_PERIOD) if tf == "daily" else 200
    total_needed = days + warmup
    if len(df) > total_needed:
        df = df.iloc[-total_needed:].reset_index(drop=True)

    # Compute all indicators on full data
    overlays = _compute_overlays(df)
    vol_data = _compute_volume(df)
    markers = _compute_markers(df)
    rel_volume = _compute_rel_volume(df)
    iv_signals = _compute_iv_signals(df)
    fvg_zones = _compute_fvg_zones(df)

    # Now trim to output window
    if len(df) > days:
        trim_start = len(df) - days
        df = df.iloc[trim_start:].reset_index(drop=True)
        cutoff_date = df.iloc[0]["date"].strftime("%Y-%m-%d")
        # Trim overlays
        for key in overlays:
            overlays[key] = [p for p in overlays[key] if p["time"] >= cutoff_date]
        # Trim volume
        vol_data["bars"] = [p for p in vol_data["bars"] if p["time"] >= cutoff_date]
        vol_data["ma50"] = [p for p in vol_data["ma50"] if p["time"] >= cutoff_date]
        # Trim markers
        markers["pocket_pivots"] = [p for p in markers["pocket_pivots"] if p["time"] >= cutoff_date]
        markers["bull_snorts"] = [p for p in markers["bull_snorts"] if p["time"] >= cutoff_date]
        markers["vcp_signals"] = [p for p in markers["vcp_signals"] if p["time"] >= cutoff_date]
        markers["hv_labels"] = [p for p in markers["hv_labels"] if p["time"] >= cutoff_date]
        # Trim new indicators
        rel_volume["bars"] = [p for p in rel_volume["bars"] if p["time"] >= cutoff_date]
        iv_signals = [p for p in iv_signals if p["time"] >= cutoff_date]
        fvg_zones = [p for p in fvg_zones if p["time"] >= cutoff_date]

    # Build candles
    candles = []
    for _, r in df.iterrows():
        candles.append({
            "time": r["date"].strftime("%Y-%m-%d"),
            "open": round(float(r["open"]), 2) if pd.notna(r["open"]) else round(float(r["close"]), 2),
            "high": round(float(r["high"]), 2) if pd.notna(r["high"]) else round(float(r["close"]), 2),
            "low": round(float(r["low"]), 2) if pd.notna(r["low"]) else round(float(r["close"]), 2),
            "close": round(float(r["close"]), 2),
        })

    last_row = df.iloc[-1]

    # Market cap + sector info
    mcap_info = {}
    try:
        from market_cap import get_mcap_for_ticker, format_mcap
        mcap_data = get_mcap_for_ticker(ticker)
        if mcap_data:
            mcap_info["mcap_cr"] = mcap_data["mcap_cr"]
            mcap_info["mcap_tier"] = mcap_data["mcap_tier"]
            mcap_info["mcap_formatted"] = format_mcap(mcap_data["mcap_cr"])
    except Exception:
        pass

    sector = ""
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=10)
        row = conn.execute("SELECT sector FROM sector_map WHERE ticker=?", (ticker,)).fetchone()
        conn.close()
        if row:
            sector = row[0]
    except Exception:
        pass

    return {
        "ticker": ticker,
        "timeframe": tf,
        "candles": candles,
        "volume": vol_data,
        "overlays": overlays,
        "markers": markers,
        "rel_volume": rel_volume,
        "iv_signals": iv_signals,
        "fvg_zones": fvg_zones,
        "info": {
            "name": ticker,
            "last_close": round(float(last_row["close"]), 2),
            "last_date": last_row["date"].strftime("%Y-%m-%d"),
            "total_bars": len(candles),
            "sector": sector,
            **mcap_info,
        },
    }
