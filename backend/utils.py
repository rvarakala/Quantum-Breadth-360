"""
Core math helpers, data loading, and shared constants.
"""
import logging
import pathlib
import numpy as np
import pandas as pd
import yfinance as yf
from typing import Dict
from collections import defaultdict

logger = logging.getLogger(__name__)

# ── Default sector maps (always available) ────────────────────────────────────
US_SECTORS = {
    "Technology":  ["AAPL","MSFT","NVDA","GOOGL","META","AMZN","TSLA","AVGO","ORCL","AMD"],
    "Financial":   ["JPM","BAC","WFC","GS","MS","BLK","C","AXP","USB","PNC"],
    "Healthcare":  ["JNJ","UNH","PFE","MRK","ABBV","TMO","ABT","DHR","BMY","AMGN"],
    "Consumer":    ["WMT","HD","PG","COST","NKE","MCD","SBUX","TGT","LOW","TJX"],
    "Energy":      ["XOM","CVX","SLB","EOG","PSX","VLO","MPC","OXY","COP","HAL"],
    "Industrial":  ["GE","HON","CAT","UPS","BA","DE","MMM","LMT","RTX","NOC"],
    "Utilities":   ["NEE","DUK","SO","AEP","EXC","SRE","PEG","ED","FE","PPL"],
    "Real Estate": ["AMT","PLD","EQIX","PSA","O","SPG","DLR","WELL","EQR","AVB"],
}
INDIA_SECTORS = {
    "IT":      ["TCS","INFY","HCLTECH","WIPRO","TECHM"],
    "Banking": ["HDFCBANK","ICICIBANK","SBIN","AXISBANK","KOTAKBANK"],
    "Pharma":  ["SUNPHARMA","CIPLA","DRREDDY","DIVISLAB","APOLLOHOSP"],
    "Auto":    ["MARUTI","TATAMOTORS","EICHERMOT","BAJAJ-AUTO"],
    "FMCG":    ["HINDUNILVR","ITC","NESTLEIND","BRITANNIA","MARICO"],
    "Metal":   ["TATASTEEL","JSWSTEEL","HINDALCO","VEDL"],
    "Energy":  ["RELIANCE","ONGC","NTPC","POWERGRID","ADANIENT"],
    "Infra":   ["LT","ADANIPORTS","AMBUJACEM","HAVELLS"],
}

# ── Import data store ─────────────────────────────────────────────────────────
try:
    from data_store import (
        init_db, smart_load_market, run_full_backfill, run_daily_update,
        db_stats, load_market, INDIA_TICKERS, SP500_TICKERS,
        load_sector_map, load_sector_counts, import_sectors_csv,
        save_ticker_universe, load_ticker_universe, import_ticker_universe_csv,
        save_sector_map, import_nifty500_csv,
    )
    init_db()
    DB_AVAILABLE = True
    logger.info("✅ Local DB initialised")
except Exception as e:
    logger.warning(f"data_store not available: {e} — using live yfinance only")
    DB_AVAILABLE = False
    INDIA_TICKERS = [
        "RELIANCE","TCS","HDFCBANK","ICICIBANK","BHARTIARTL","SBIN","INFY","HINDUNILVR",
        "ITC","LT","BAJFINANCE","HCLTECH","WIPRO","AXISBANK","ASIANPAINT","MARUTI",
        "NESTLEIND","KOTAKBANK","TITAN","SUNPHARMA","ULTRACEMCO","ADANIENT","ONGC",
        "NTPC","POWERGRID","BAJAJFINSV","TECHM","TATAMOTORS","INDUSINDBK","CIPLA",
        "DRREDDY","GRASIM","COALINDIA","JSWSTEEL","TATASTEEL","HDFCLIFE","DIVISLAB",
        "EICHERMOT","HINDALCO","BAJAJ-AUTO","BRITANNIA","APOLLOHOSP","PIDILITIND",
        "TATACONSUM","ADANIPORTS","AMBUJACEM","HAVELLS","MARICO","VEDL","MCDOWELL-N",
    ]
    SP500_TICKERS = [
        "AAPL","MSFT","AMZN","NVDA","GOOGL","META","TSLA","BRK-B","UNH","JNJ",
        "XOM","V","JPM","PG","MA","HD","CVX","MRK","ABBV","LLY",
        "PEP","KO","COST","AVGO","MCD","WMT","CSCO","TMO","ACN","ABT",
        "DHR","NEE","VZ","ADBE","CRM","NKE","CMCSA","TXN","PM","WFC",
    ]
    US_SECTORS = {
        "Technology":["AAPL","MSFT","NVDA","ADBE","CRM","INTC"],
        "Healthcare":["UNH","JNJ","LLY","ABBV","ABT","DHR"],
        "Financials":["JPM","WFC","MS","V","MA"],
        "Energy":["XOM","CVX","COP"],
        "Consumer D":["AMZN","TSLA","HD","MCD","NKE"],
        "Industrials":["HON","RTX","UPS"],
        "Comm Svcs":["GOOGL","META","CMCSA"],
        "Utilities":["NEE","DUK","SO"],
    }
    INDIA_SECTORS = {
        "IT":["TCS","INFY","HCLTECH","WIPRO","TECHM"],
        "Banking":["HDFCBANK","ICICIBANK","SBIN","AXISBANK","KOTAKBANK"],
        "Pharma":["SUNPHARMA","CIPLA","DRREDDY","DIVISLAB","APOLLOHOSP"],
        "Auto":["MARUTI","TATAMOTORS","EICHERMOT","BAJAJ-AUTO"],
        "FMCG":["HINDUNILVR","ITC","NESTLEIND","BRITANNIA","MARICO"],
        "Metal":["TATASTEEL","JSWSTEEL","HINDALCO","VEDL"],
        "Energy":["RELIANCE","ONGC","NTPC","POWERGRID","ADANIENT"],
        "Infra":["LT","ADANIPORTS","AMBUJACEM","HAVELLS"],
    }

    # Stubs for functions that won't be available without data_store
    def init_db(): pass
    def smart_load_market(*a, **kw): return {}
    def run_full_backfill(*a, **kw): return {}
    def run_daily_update(*a, **kw): return {}
    def db_stats(*a, **kw): return {}
    def load_market(*a, **kw): return {}
    def load_sector_map(*a, **kw): return {}
    def load_sector_counts(*a, **kw): return []
    def import_sectors_csv(*a, **kw): return 0
    def save_ticker_universe(*a, **kw): pass
    def load_ticker_universe(*a, **kw): return []
    def import_ticker_universe_csv(*a, **kw): return 0
    def save_sector_map(*a, **kw): return 0
    def import_nifty500_csv(*a, **kw): return 0


# ── Core math ─────────────────────────────────────────────────────────────────
def safe_float(val, default=0.0):
    try:
        # Handle pandas Series (MultiIndex yfinance 1.2.0 returns Series for single ticker)
        if hasattr(val, 'iloc'):
            val = val.iloc[0] if len(val) > 0 else default
        v = float(val)
        return default if (np.isnan(v) or np.isinf(v)) else v
    except:
        return default

def flatten_df(df):
    """Flatten MultiIndex columns from yfinance 1.2.0 single-ticker download"""
    if df is None or df.empty:
        return df
    if not isinstance(df.columns, pd.MultiIndex):
        return df  # already flat
    df = df.copy()
    lvl0 = df.columns.get_level_values(0).tolist()
    lvl1 = df.columns.get_level_values(1).tolist()
    metrics = {'Open','High','Low','Close','Volume','Adj Close'}
    # Use whichever level contains OHLCV metric names
    if any(m in lvl0 for m in metrics):
        df.columns = lvl0
    elif any(m in lvl1 for m in metrics):
        df.columns = lvl1
    else:
        df.columns = lvl0
    return df

def safe_download(ticker: str, period: str = "10d") -> pd.DataFrame:
    """Download single ticker and return flat OHLCV DataFrame.
    Uses 10d period by default to handle weekends/holidays."""
    try:
        df = yf.download(ticker, period=period, progress=False, timeout=20)
        return flatten_df(df)
    except Exception as e:
        logger.warning(f"Download failed {ticker}: {e}")
        return pd.DataFrame()

def get_close(df) -> float:
    """Get latest close price from a flat DataFrame"""
    if df is None or df.empty:
        return 0.0
    if "Close" not in df.columns:
        return 0.0
    try:
        val = df["Close"].dropna().iloc[-1]
        if hasattr(val, 'iloc'):
            val = val.iloc[0]
        v = float(val)
        return 0.0 if (np.isnan(v) or np.isinf(v)) else round(v, 4)
    except:
        return 0.0

def get_change_pct(df) -> float:
    """Get latest 1-day % change from a flat DataFrame"""
    if df is None or df.empty or "Close" not in df.columns:
        return 0.0
    try:
        closes = df["Close"].dropna()
        if len(closes) < 2:
            return 0.0
        c1, c0 = float(closes.iloc[-1]), float(closes.iloc[-2])
        if c0 == 0:
            return 0.0
        return round((c1 - c0) / c0 * 100, 2)
    except:
        return 0.0

def fetch_batch(tickers, suffix="", period="1y"):
    """Live yfinance batch download — used as fallback when DB is empty"""
    results = {}
    yf_tickers = [f"{t}{suffix}" for t in tickers]
    logger.info(f"Live fetch: {len(yf_tickers)} tickers...")
    try:
        raw = yf.download(" ".join(yf_tickers), period=period, interval="1d",
                          group_by="ticker", auto_adjust=True,
                          threads=True, progress=False, timeout=60)
        if raw is None or raw.empty:
            return results
        for i, ticker in enumerate(tickers):
            yf_t = yf_tickers[i]
            try:
                if len(yf_tickers) == 1:
                    df = flatten_df(raw.copy())
                elif isinstance(raw.columns, pd.MultiIndex):
                    if yf_t not in raw.columns.get_level_values(0): continue
                    df = raw[yf_t].copy()
                else:
                    continue
                df = df.dropna(subset=["Close"])
                if len(df) >= 20:
                    results[ticker] = df
            except: continue
    except Exception as e:
        logger.error(f"Live fetch failed: {e}")
    logger.info(f"Live fetch: got {len(results)}/{len(tickers)}")
    return results

def get_stock_data(market: str, custom_tickers: Dict[str, list] = None) -> Dict[str, pd.DataFrame]:
    """
    Load OHLCV data STRICTLY for the NIFTY 500 universe only.
    Queries SQLite directly — bypasses load_market() filtering issues.
    Falls back to yfinance for any missing tickers.
    """
    import sqlite3

    if custom_tickers is None:
        custom_tickers = {}

    # ── Get NIFTY 500 universe (500 tickers) ──────────────────────────────
    universe = []
    if market in custom_tickers and len(custom_tickers[market]) > 0:
        universe = custom_tickers[market]
    elif DB_AVAILABLE:
        universe = load_ticker_universe(market)

    if not universe:
        logger.warning(f"No universe found for {market}, using default tickers")
        universe = INDIA_TICKERS if market == "India" else SP500_TICKERS

    universe_set = set(universe)
    logger.info(f"Target universe: {len(universe_set)} {market} tickers (NIFTY 500)")

    result = {}

    # ── Query SQLite directly for ALL universe tickers ────────────────────
    if DB_AVAILABLE:
        try:
            db_path = pathlib.Path(__file__).parent / "breadth_data.db"
            conn = sqlite3.connect(str(db_path), timeout=30)

            # Build placeholders for IN clause
            placeholders = ",".join("?" * len(universe))
            query = f"""
                SELECT ticker, date, open, high, low, close, volume
                FROM ohlcv
                WHERE ticker IN ({placeholders})
                  AND market = ?
                  AND date >= date('now', '-750 days')
                ORDER BY ticker, date
            """
            # Normalize market — DB stores as "India" not "INDIA"
            db_market = "India" if market.upper() == "INDIA" else market
            params = list(universe) + [db_market]
            rows = conn.execute(query, params).fetchall()
            conn.close()

            logger.info(f"SQLite returned {len(rows):,} rows for {len(universe_set)} universe tickers")

            # Group into per-ticker DataFrames
            ticker_rows = defaultdict(list)
            for row in rows:
                ticker_rows[row[0]].append(row)

            # ── Partial-day detection ─────────────────────────────────────
            # Count how many tickers have data on each recent date.
            # If the most recent date has < 60% of the previous full day,
            # it's a partial sync — cap data to the last COMPLETE date.
            from collections import Counter
            date_ticker_counts = Counter()
            for row in rows:
                date_ticker_counts[row[1]] += 1  # row[1] = date

            recent_dates = sorted(date_ticker_counts.keys(), reverse=True)[:5]
            cap_date = None  # None means use all data as-is

            # Log recent date coverage for diagnostics
            for _rd in recent_dates[:3]:
                logger.info(f"Date coverage: {_rd} = {date_ticker_counts[_rd]} tickers")

            if len(recent_dates) >= 2:
                newest_date = recent_dates[0]
                newest_count = date_ticker_counts[newest_date]
                # Find the most recent date with at least 300 tickers (a "full" day)
                full_day = None
                full_day_count = 0
                for d in recent_dates:
                    if date_ticker_counts[d] >= 300:
                        full_day = d
                        full_day_count = date_ticker_counts[d]
                        break

                if full_day and newest_date != full_day:
                    coverage_pct = newest_count / full_day_count * 100
                    if coverage_pct < 60:
                        cap_date = full_day
                        logger.warning(
                            f"PARTIAL DATA DETECTED: {newest_date} has {newest_count} tickers "
                            f"({coverage_pct:.0f}% of {full_day}'s {full_day_count}). "
                            f"Capping to last complete day: {full_day}"
                        )

            if cap_date:
                logger.info(f"Capping data to {cap_date} (partial data protection)")

            # Find the effective latest date (after capping)
            if cap_date:
                latest_date_row = cap_date
            else:
                latest_date_row = ticker_rows and max(
                    (max(r[1] for r in trows) for trows in ticker_rows.values()),
                    default=None
                )

            for ticker, trows in ticker_rows.items():
                if len(trows) < 30:  # need at least 30 days
                    continue
                df = pd.DataFrame(trows, columns=['ticker','date','open','high','low','close','volume'])
                df['date'] = pd.to_datetime(df['date'])
                df = df.set_index('date').sort_index()
                df.rename(columns={'open':'Open','high':'High','low':'Low',
                                   'close':'Close','volume':'Volume'}, inplace=True)
                # If capping to a complete date, trim data beyond that date
                if cap_date:
                    df = df[df.index <= pd.Timestamp(cap_date)]
                    if len(df) < 30:
                        continue
                # FRESHNESS CHECK: only include tickers whose latest data
                # is within 30 trading days of the effective latest date.
                # This prevents stale Jan-2025 data from mixing with Mar-2026 data.
                if latest_date_row:
                    days_stale = (pd.Timestamp(latest_date_row) - df.index[-1]).days
                    if days_stale > 45:  # ~30 trading days ≈ 45 calendar days
                        continue
                result[ticker] = df

            logger.info(f"Loaded {len(result)}/{len(universe_set)} NIFTY 500 tickers from SQLite (freshness-filtered{', capped to ' + cap_date if cap_date else ''})")
            if cap_date:
                logger.info(f"💡 Run 'Force EOD Sync' to get full data for the latest trading day")

        except Exception as e:
            logger.error(f"Direct SQLite query failed: {e}")

    # ── Log missing tickers (no yfinance fallback — DB only) ─────────────
    missing = universe_set - set(result.keys())
    if missing:
        logger.warning(
            f"{len(missing)} universe tickers have no usable DB data "
            f"(need >=30 rows in 750-day window). "
            f"Run Data Import to refresh: {', '.join(sorted(missing)[:20])}"
            f"{'...' if len(missing) > 20 else ''}"
        )

    if not result:
        logger.error("No stock data available from DB!")
        return {}

    logger.info(f"Final: {len(result)} NIFTY 500 tickers ready for breadth calculation")
    return result



def get_screener_data(market: str) -> dict:
    """
    Load OHLCV data for the ENTIRE NSE universe (all 2,585 tickers).
    Used ONLY by the screener — never for breadth calculation.
    RS is always calculated vs ^CRSLDX (NIFTY 500 benchmark).
    """
    import sqlite3

    result = {}

    # ── Query SQLite for ALL tickers in this market (no universe filter) ─
    if DB_AVAILABLE:
        try:
            db_path = pathlib.Path(__file__).parent / "breadth_data.db"
            conn = sqlite3.connect(str(db_path), timeout=30)

            # Get ALL tickers for this market — no IN filter
            query = """
                SELECT ticker, date, open, high, low, close, volume
                FROM ohlcv
                WHERE market = ?
                  AND date >= date('now', '-750 days')
                ORDER BY ticker, date
            """
            # Normalize market — DB stores as "India" not "INDIA"
            db_market = "India" if market.upper() == "INDIA" else market
            rows = conn.execute(query, [db_market]).fetchall()
            conn.close()

            logger.info(f"Screener SQLite: {len(rows):,} rows across all {market} tickers")

            # Group into per-ticker DataFrames
            ticker_rows = defaultdict(list)
            for row in rows:
                ticker_rows[row[0]].append(row)

            for ticker, trows in ticker_rows.items():
                if len(trows) < 60:  # screener needs at least 60 days for RS calc
                    continue
                df = pd.DataFrame(
                    trows,
                    columns=['ticker','date','open','high','low','close','volume']
                )
                df['date'] = pd.to_datetime(df['date'])
                df = df.set_index('date').sort_index()
                df.rename(columns={
                    'open':'Open','high':'High','low':'Low',
                    'close':'Close','volume':'Volume'
                }, inplace=True)
                result[ticker] = df

            logger.info(
                f"Screener: loaded {len(result):,} tickers "
                f"from full NSE universe (RS vs ^CRSLDX)"
            )
            return result

        except Exception as e:
            logger.error(f"Screener SQLite query failed: {e}")

    # ── No yfinance fallback — DB only ───────────────────────────────
    logger.warning("Screener: no DB data available")
    return {}


# ── Screener math helpers ─────────────────────────────────────────────────────
def _safe(val, default=0.0):
    try:
        v = float(val)
        return default if (np.isnan(v) or np.isinf(v)) else v
    except:
        return default


def _ma(s, n):
    return s.rolling(n, min_periods=max(1, n//2)).mean()

def _ema(s, n):
    return s.ewm(span=n, adjust=False, min_periods=max(1, n//2)).mean()

def _hhv(s, n):
    return s.rolling(n, min_periods=1).max()

def _llv(s, n):
    return s.rolling(n, min_periods=1).min()

def _roc(s, n):
    return s.pct_change(n) * 100

def _atr(df, n=14):
    h, l, c = df['High'], df['Low'], df['Close']
    tr = pd.concat([h - l, (h - c.shift()).abs(), (l - c.shift()).abs()], axis=1).max(axis=1)
    return tr.rolling(n, min_periods=1).mean()
