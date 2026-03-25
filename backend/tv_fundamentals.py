"""
Fundamentals Service — TradingView + yfinance
=============================================
LAYER 1 — BATCH (tradingview-screener):
  fetch_batch_fundamentals()  → one call, all NSE stocks, ~10 seconds
  get_batch_fundamental(tick) → instant DB lookup
  Stores: tv_fundamentals table (PE, ROE, EPS, Margins, D/E, MCap)
  Refresh: weekly (financial data changes slowly)

LAYER 2 — PER-TICKER QUARTERLY (yfinance):
  fetch_ticker_detail(tick)   → 5-quarter EPS/Revenue/Profit history
  Stores: tv_fundamentals_detail table (24h cache)
  Used by: Smart Metrics OM score, SMART Screener Pass 2

LAYER 3 — FAST RATIO ONLY (TV batch, no network):
  get_screener_data_fast(tick) → instant from DB, ratios only
"""

import sqlite3
import json
import logging
import time
import pathlib
import math
from datetime import datetime, timezone, timedelta
from typing import Optional

logger = logging.getLogger(__name__)
DB_PATH = pathlib.Path(__file__).parent / "breadth_data.db"


# ══════════════════════════════════════════════════════════════════════════════
# DB SETUP
# ══════════════════════════════════════════════════════════════════════════════
def _ensure_tables():
    conn = sqlite3.connect(str(DB_PATH), timeout=15)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS tv_fundamentals (
            ticker           TEXT PRIMARY KEY,
            pe_ratio         REAL,
            pb_ratio         REAL,
            roe              REAL,
            roa              REAL,
            gross_margin     REAL,
            operating_margin REAL,
            net_margin       REAL,
            debt_to_equity   REAL,
            current_ratio    REAL,
            eps_ttm          REAL,
            eps_growth_ttm   REAL,
            revenue_ttm      REAL,
            revenue_growth   REAL,
            market_cap       REAL,
            company_name     TEXT,
            sector           TEXT,
            industry         TEXT,
            fetched_at       TEXT
        );
        CREATE TABLE IF NOT EXISTS tv_fundamentals_detail (
            ticker     TEXT PRIMARY KEY,
            data       TEXT,
            fetched_at TEXT
        );
    """)
    conn.commit()
    conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# LAYER 1 — BATCH via tradingview-screener
# One call gets ALL NSE stocks' summary ratios
# ══════════════════════════════════════════════════════════════════════════════

TV_FIELDS = [
    'name',
    'close',
    'market_cap_basic',
    'price_earnings_ttm',        # PE TTM
    'price_book_fq',             # P/B
    'return_on_equity',          # ROE %
    'return_on_assets',          # ROA %
    'gross_margin',              # Gross Margin %
    'operating_margin',          # OPM %
    'net_margin',                # NPM %
    'debt_to_equity',            # D/E
    'current_ratio',             # Current Ratio
    'earnings_per_share_basic_ttm',  # EPS TTM
    'earnings_per_share_diluted_ttm',# EPS Diluted TTM
    'total_revenue',             # Revenue TTM
    'sector',
    'industry',
    'description',
]


def fetch_batch_fundamentals(market: str = "india") -> dict:
    """
    Fetch fundamental summary for ALL NSE stocks in one API call.
    Returns {ticker: {pe, roe, eps_ttm, ...}} dict.
    Stores in tv_fundamentals SQLite table.
    """
    _ensure_tables()

    try:
        from tradingview_screener import Query
    except ImportError:
        logger.error("tradingview-screener not installed: pip install tradingview-screener")
        return {}

    logger.info(f"Fetching batch fundamentals for {market} via TradingView screener...")
    t0 = time.time()

    try:
        count, df = (Query()
            .set_markets(market)
            .select(*TV_FIELDS)
            .limit(2000)
            .get_scanner_data()
        )
        logger.info(f"TradingView returned {count} stocks in {round(time.time()-t0,1)}s")
    except Exception as e:
        logger.error(f"TradingView batch fetch failed: {e}")
        return {}

    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    now  = datetime.now(timezone.utc).isoformat()
    stored = 0
    result = {}

    for _, row in df.iterrows():
        raw_ticker = str(row.get('ticker', '') or row.get('name', '') or '')
        ticker = raw_ticker.split(':')[-1].strip().upper()
        if not ticker:
            continue

        def _f(col, default=None):
            v = row.get(col)
            if v is None:
                return default
            try:
                f = float(v)
                return None if (math.isnan(f) or math.isinf(f)) else f
            except:
                return default

        def _s(col, default=''):
            v = row.get(col)
            return str(v).strip() if v and str(v) not in ('nan','None','') else default

        entry = {
            "pe_ratio":         _f('price_earnings_ttm'),
            "pb_ratio":         _f('price_book_fq'),
            "roe":              _f('return_on_equity'),
            "roa":              _f('return_on_assets'),
            "gross_margin":     _f('gross_margin'),
            "operating_margin": _f('operating_margin'),
            "net_margin":       _f('net_margin'),
            "debt_to_equity":   _f('debt_to_equity'),
            "current_ratio":    _f('current_ratio'),
            "eps_ttm":          _f('earnings_per_share_basic_ttm'),
            "eps_growth_ttm":   _f('earnings_per_share_basic_yoy_growth_fy'),
            "revenue_ttm":      _f('total_revenue'),
            "revenue_growth":   _f('revenue_growth_quarterly_yoy'),
            "market_cap":       _f('market_cap_basic'),
            "company_name":     _s('description') or _s('name'),
            "sector":           _s('sector'),
            "industry":         _s('industry'),
        }
        result[ticker] = entry

        conn.execute("""
            INSERT OR REPLACE INTO tv_fundamentals
            (ticker, pe_ratio, pb_ratio, roe, roa, gross_margin, operating_margin,
             net_margin, debt_to_equity, current_ratio, eps_ttm, eps_growth_ttm,
             revenue_ttm, revenue_growth, market_cap, company_name, sector, industry,
             fetched_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            ticker,
            entry["pe_ratio"],    entry["pb_ratio"],
            entry["roe"],         entry["roa"],
            entry["gross_margin"],entry["operating_margin"], entry["net_margin"],
            entry["debt_to_equity"], entry["current_ratio"],
            entry["eps_ttm"],     entry["eps_growth_ttm"],
            entry["revenue_ttm"], entry["revenue_growth"],
            entry["market_cap"],
            entry["company_name"], entry["sector"], entry["industry"],
            now,
        ))
        stored += 1

    conn.commit()
    conn.close()
    logger.info(f"✅ TV batch fundamentals: {stored} tickers stored")
    return result


def get_batch_fundamental(ticker: str) -> Optional[dict]:
    """Get fundamental summary for one ticker from tv_fundamentals table."""
    _ensure_tables()
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    row = conn.execute("""
        SELECT pe_ratio, pb_ratio, roe, roa, gross_margin, operating_margin,
               net_margin, debt_to_equity, current_ratio, eps_ttm, eps_growth_ttm,
               revenue_ttm, revenue_growth, market_cap, company_name, sector,
               industry, fetched_at
        FROM tv_fundamentals WHERE ticker = ?
    """, (ticker.upper(),)).fetchone()
    conn.close()

    if not row:
        return None

    age_h = 999
    if row[17]:
        try:
            age_h = (datetime.now(timezone.utc) -
                     datetime.fromisoformat(row[17])).total_seconds() / 3600
        except:
            pass

    return {
        "pe_ratio":         row[0],
        "pb_ratio":         row[1],
        "roe":              row[2],
        "roa":              row[3],
        "gross_margin":     row[4],
        "operating_margin": row[5],
        "net_margin":       row[6],
        "debt_to_equity":   row[7],
        "current_ratio":    row[8],
        "eps_ttm":          row[9],
        "eps_growth_ttm":   row[10],
        "revenue_ttm":      row[11],
        "revenue_growth":   row[12],
        "market_cap":       row[13],
        "company_name":     row[14],
        "sector":           row[15],
        "industry":         row[16],
        "age_hours":        round(age_h, 1),
        "fresh":            age_h < 24,
    }


def is_batch_fresh(max_age_hours: int = 24) -> bool:
    """Check if batch data was fetched recently."""
    _ensure_tables()
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    row = conn.execute(
        "SELECT MAX(fetched_at) FROM tv_fundamentals"
    ).fetchone()
    conn.close()
    if not row or not row[0]:
        return False
    try:
        age = (datetime.now(timezone.utc) -
               datetime.fromisoformat(row[0])).total_seconds() / 3600
        return age < max_age_hours
    except:
        return False


# ══════════════════════════════════════════════════════════════════════════════
# LAYER 2 — PER-TICKER via yfinance quarterly statements
# Provides quarterly + annual EPS/Revenue/Profit time series
# Exactly what compute_om_score needs
# ══════════════════════════════════════════════════════════════════════════════

def _get_cached_detail(ticker: str) -> Optional[dict]:
    """Return cached detail if fresh (< 24h)."""
    _ensure_tables()
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    row = conn.execute(
        "SELECT data, fetched_at FROM tv_fundamentals_detail WHERE ticker = ?",
        (ticker.upper(),)
    ).fetchone()
    conn.close()
    if not row:
        return None
    try:
        fetched = datetime.fromisoformat(row[1])
        if (datetime.now(timezone.utc) - fetched) < timedelta(hours=24):
            return json.loads(row[0])
    except:
        pass
    return None


def _set_cached_detail(ticker: str, data: dict):
    _ensure_tables()
    conn = sqlite3.connect(str(DB_PATH), timeout=15)
    conn.execute("""
        INSERT OR REPLACE INTO tv_fundamentals_detail (ticker, data, fetched_at)
        VALUES (?, ?, ?)
    """, (ticker.upper(), json.dumps(data),
          datetime.now(timezone.utc).isoformat()))
    conn.commit()
    conn.close()


def _safe_float(v):
    """Convert to float safely, return None on failure."""
    if v is None:
        return None
    try:
        import numpy as np
        if isinstance(v, (np.integer, np.floating)):
            v = float(v)
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else f
    except:
        return None


def _yf_quarterly_to_list(df) -> list:
    """
    Convert yfinance quarterly_income_stmt DataFrame to list of dicts.
    yfinance returns columns = dates (newest first), rows = metrics.
    We reverse to oldest-first for compute_om_score.
    """
    if df is None or df.empty:
        return []

    # Normalise row index to lowercase for consistent access
    df.index = [str(i).lower().strip() for i in df.index]

    quarters = []
    # Columns are dates, newest first — reverse to oldest first
    for col in reversed(df.columns):
        def _get(*keys):
            for k in keys:
                if k in df.index:
                    return _safe_float(df.loc[k, col])
            return None

        revenue    = _get('total revenue', 'revenue', 'total revenues')
        net_profit = _get('net income', 'net income common stockholders', 'net income from continuing operations')
        eps        = _get('diluted eps', 'basic eps', 'eps')
        op_income  = _get('operating income', 'ebit')
        opm = round(op_income / revenue * 100, 1) if revenue and op_income and revenue > 0 else None
        npm = round(net_profit / revenue * 100, 1) if revenue and net_profit and revenue > 0 else None

        quarters.append({
            "period":     str(col)[:10],
            "sales":      revenue,
            "net_profit": net_profit,
            "eps":        eps,
            "opm":        opm,
            "npm":        npm,
        })
    return quarters


def _yf_annual_to_list(df) -> list:
    """Convert yfinance financials (annual) to list of dicts."""
    if df is None or df.empty:
        return []
    df.index = [str(i).lower().strip() for i in df.index]
    annual = []
    for col in reversed(df.columns):
        def _get(*keys):
            for k in keys:
                if k in df.index:
                    return _safe_float(df.loc[k, col])
            return None
        revenue    = _get('total revenue', 'revenue')
        net_profit = _get('net income', 'net income common stockholders')
        eps        = _get('diluted eps', 'basic eps', 'eps')
        annual.append({
            "period":     str(col)[:10],
            "sales":      revenue,
            "net_profit": net_profit,
            "eps":        eps,
        })
    return annual


def fetch_ticker_detail(ticker: str) -> dict:
    """
    Fetch quarterly + annual financials for one ticker.
    Returns screener.in-compatible dict: {quarterly[], annual[], ratios{}}
    Priority: cache → yfinance statements → TV batch fallback
    """
    cached = _get_cached_detail(ticker)
    if cached:
        return cached

    result = _fetch_yf_statements(ticker)

    if not result or "error" in result:
        # Fallback: build minimal dict from batch summary data
        batch = get_batch_fundamental(ticker)
        if batch:
            result = _build_from_batch(ticker, batch)
        else:
            result = {"error": f"No fundamental data available for {ticker}",
                      "ticker": ticker}

    _set_cached_detail(ticker, result)
    return result


def _fetch_yf_statements(ticker: str) -> dict:
    """
    Fetch quarterly + annual income statements via yfinance.
    Uses threading timeout to prevent hanging in thread pools.
    """
    try:
        import yfinance as yf
    except ImportError:
        return {"error": "yfinance not installed"}

    try:
        t = yf.Ticker(f"{ticker}.NS")

        # Quarterly income statement — with timeout protection
        quarterly = []
        try:
            import threading, queue

            def _fetch_q(q):
                try:
                    q.put(t.quarterly_income_stmt)
                except Exception as e:
                    q.put(e)

            q = queue.Queue()
            th = threading.Thread(target=_fetch_q, args=(q,), daemon=True)
            th.start()
            th.join(timeout=15)  # 15 second timeout per ticker

            if not q.empty():
                result = q.get_nowait()
                if not isinstance(result, Exception) and result is not None and not result.empty:
                    quarterly = _yf_quarterly_to_list(result)
        except Exception as e:
            logger.debug(f"yfinance quarterly failed {ticker}: {e}")

        # Annual income statement
        annual = []
        try:
            a_df = t.income_stmt
            if a_df is not None and not a_df.empty:
                annual = _yf_annual_to_list(a_df)
        except Exception as e:
            logger.debug(f"yfinance annual failed {ticker}: {e}")

        if not quarterly and not annual:
            return {"error": f"No yfinance statement data for {ticker}"}

        # Ratios: try TV batch first (instant), then yfinance .info
        ratios = {}
        batch = get_batch_fundamental(ticker)
        if batch:
            ratios = {
                "roe":            batch.get("roe"),
                "debt_to_equity": batch.get("debt_to_equity"),
                "pe_ratio":       batch.get("pe_ratio"),
                "current_ratio":  batch.get("current_ratio"),
                "operating_margin": batch.get("operating_margin"),
                "net_margin":     batch.get("net_margin"),
            }
        else:
            # Fallback: yfinance .info for ratios
            try:
                info = t.info
                ratios = {
                    "roe":            _safe_float(info.get("returnOnEquity")),
                    "debt_to_equity": _safe_float(info.get("debtToEquity")),
                    "pe_ratio":       _safe_float(info.get("trailingPE")),
                    "current_ratio":  _safe_float(info.get("currentRatio")),
                }
                # Convert decimal to percentage for ROE (yfinance returns 0.18 not 18%)
                if ratios["roe"] and ratios["roe"] < 2:
                    ratios["roe"] = round(ratios["roe"] * 100, 1)
            except Exception as e:
                logger.debug(f"yfinance .info failed {ticker}: {e}")

        company_name = ticker
        if batch and batch.get("company_name"):
            company_name = batch["company_name"]
        else:
            try:
                info = t.info
                company_name = info.get("shortName") or info.get("longName") or ticker
            except:
                pass

        return {
            "ticker":       ticker,
            "company_name": company_name,
            "quarterly":    quarterly,
            "annual":       annual,
            "ratios":       ratios,
            "source":       "yfinance",
        }

    except Exception as e:
        logger.warning(f"yfinance statements failed for {ticker}: {e}")
        return {"error": str(e), "ticker": ticker}


def _build_from_batch(ticker: str, batch: dict) -> dict:
    """Build minimal screener-compatible dict from batch summary only."""
    ratios = {
        "roe":              batch.get("roe"),
        "debt_to_equity":   batch.get("debt_to_equity"),
        "pe_ratio":         batch.get("pe_ratio"),
        "current_ratio":    batch.get("current_ratio"),
        "operating_margin": batch.get("operating_margin"),
        "net_margin":       batch.get("net_margin"),
    }
    # Minimal quarterly from EPS TTM (single data point)
    eps_ttm = batch.get("eps_ttm")
    quarterly = []
    if eps_ttm is not None:
        quarterly = [{
            "period":     "TTM",
            "eps":        eps_ttm,
            "sales":      batch.get("revenue_ttm"),
            "net_profit": None,
            "opm":        batch.get("operating_margin"),
            "npm":        batch.get("net_margin"),
        }]
    return {
        "ticker":       ticker,
        "company_name": batch.get("company_name", ticker),
        "quarterly":    quarterly,
        "annual":       [],
        "ratios":       ratios,
        "source":       "tv_batch_fallback",
    }


# ══════════════════════════════════════════════════════════════════════════════
# FAST SCREENER DATA — Build from TV batch only (no network calls)
# Used by SMART Screener Pass 2 — instant, no yfinance/scraper calls
# ══════════════════════════════════════════════════════════════════════════════

def get_screener_data_fast(ticker: str) -> dict:
    """
    Build screener-compatible fundamental dict purely from TV batch DB.
    No network calls — instant. Used by SMART Screener Pass 2.

    Returns same format as fetch_ticker_detail() but with limited quarterly data.
    compute_om_score gracefully handles missing quarters by skipping those criteria.
    """
    batch = get_batch_fundamental(ticker)

    if not batch:
        return {"error": f"No TV batch data for {ticker}", "ticker": ticker}

    # Build ratios from batch — include ALL available TV fields
    roe        = batch.get("roe")
    de         = batch.get("debt_to_equity")
    op_margin  = batch.get("operating_margin")
    net_margin = batch.get("net_margin")
    gross_m    = batch.get("gross_margin")
    eps_ttm    = batch.get("eps_ttm")
    eps_growth = batch.get("eps_growth_ttm")   # YoY % from TV
    rev_ttm    = batch.get("revenue_ttm")
    rev_growth = batch.get("revenue_growth")   # quarterly YoY %

    ratios = {
        "roe":              roe,
        "debt_to_equity":   de,
        "pe_ratio":         batch.get("pe_ratio"),
        "current_ratio":    batch.get("current_ratio"),
        "operating_margin": op_margin,
        "net_margin":       net_margin,
        "gross_margin":     gross_m,
        # Growth proxies from TV batch (used by compute_om_score)
        "eps_growth_ttm":   eps_growth,
        "revenue_growth":   rev_growth,
    }

    net_profit = rev_ttm * net_margin / 100 if rev_ttm and net_margin else None
    op_income  = rev_ttm * op_margin  / 100 if rev_ttm and op_margin  else None

    # Build 5 synthetic quarterly rows from TTM data
    # This lets compute_om_score evaluate all criteria instead of skipping
    # Each row has same values (TTM is a proxy for all 5 quarters)
    quarterly = []
    if eps_ttm is not None or rev_ttm is not None:
        base_row = {
            "sales":      rev_ttm / 4   if rev_ttm    else None,
            "net_profit": net_profit / 4 if net_profit else None,
            "eps":        eps_ttm / 4    if eps_ttm    else None,
            "opm":        op_margin,
            "npm":        net_margin,
        }
        # Apply growth to make rows realistic for growth criteria
        # If eps_growth is known, simulate trend; otherwise flat
        growth_factor = 1 + (eps_growth or 0) / 400  # quarterly equivalent
        for i in range(5):
            factor = growth_factor ** (i)
            quarterly.append({
                "period":     f"Q-{4-i}",
                "sales":      (base_row["sales"]      * factor) if base_row["sales"]      else None,
                "net_profit": (base_row["net_profit"]  * factor) if base_row["net_profit"] else None,
                "eps":        (base_row["eps"]          * factor) if base_row["eps"]        else None,
                "opm":        op_margin,
                "npm":        net_margin,
            })

    # Build 4 synthetic annual rows similarly
    annual = []
    if rev_ttm is not None:
        ann_growth = 1 + (rev_growth or 0) / 100
        for i in range(4):
            factor = ann_growth ** i
            annual.append({
                "period":     f"FY-{3-i}",
                "sales":      rev_ttm     * factor if rev_ttm     else None,
                "net_profit": net_profit  * factor if net_profit  else None,
                "eps":        eps_ttm     * factor if eps_ttm     else None,
            })

    return {
        "ticker":       ticker,
        "company_name": batch.get("company_name", ticker),
        "quarterly":    quarterly,
        "annual":       annual,
        "ratios":       ratios,
        "source":       "tv_batch_fast",
    }
