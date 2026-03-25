"""
TradingView Fundamentals — replaces screener.in scraping
=========================================================
Two data layers:

Layer 1 — BATCH (tradingview-screener):
    One API call fetches fundamental summary for ALL NSE stocks at once.
    Fields: PE, ROE, ROA, Debt/Equity, Market Cap, Margins, EPS TTM
    Used by: SMART Screener Pass 2 (instant lookup, no per-ticker calls)
    Refresh: Daily (store in SQLite tv_fundamentals table)

Layer 2 — PER-TICKER (tradingview-scraper):
    Fetches quarterly + annual time-series per ticker on demand.
    Fields: EPS per quarter, Sales per quarter, Net Profit, OPM
    Used by: Smart Metrics tab (detailed OM score for one ticker)
    Cache: 24h in tv_fundamentals_detail table

Together these replace ALL screener.in usage:
    - SMART screener: uses Layer 1 (batch, instant)
    - Smart Metrics tab: uses Layer 2 (per-ticker, on demand)
    - Charts tab EPS: uses Layer 1 eps_ttm field
"""

import sqlite3
import json
import logging
import time
import pathlib
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
            ticker          TEXT PRIMARY KEY,
            -- Valuation
            pe_ratio        REAL,
            pb_ratio        REAL,
            ev_ebitda       REAL,
            -- Profitability
            roe             REAL,
            roa             REAL,
            gross_margin    REAL,
            operating_margin REAL,
            net_margin      REAL,
            -- Growth
            eps_ttm         REAL,
            eps_growth_ttm  REAL,
            revenue_ttm     REAL,
            revenue_growth  REAL,
            -- Balance Sheet
            debt_to_equity  REAL,
            current_ratio   REAL,
            -- Meta
            company_name    TEXT,
            sector          TEXT,
            industry        TEXT,
            market_cap      REAL,
            fetched_at      TEXT
        );
        CREATE TABLE IF NOT EXISTS tv_fundamentals_detail (
            ticker      TEXT PRIMARY KEY,
            data        TEXT,
            fetched_at  TEXT
        );
    """)
    conn.commit()
    conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# LAYER 1 — BATCH FETCH via tradingview-screener
# Fetches ALL NSE stocks in one call — stores summary in tv_fundamentals
# ══════════════════════════════════════════════════════════════════════════════

# TradingView field → our DB column mapping
TV_FIELDS = [
    'name',
    'close',
    'market_cap_basic',
    'price_earnings_ttm',          # PE TTM
    'price_book_fq',               # P/B
    'return_on_equity',            # ROE %
    'return_on_assets',            # ROA %
    'gross_margin',                # Gross Margin %
    'operating_margin',            # OPM %
    'net_margin',                  # NPM %
    'debt_to_equity',              # D/E
    'current_ratio',               # Current Ratio
    'earnings_per_share_basic_ttm',# EPS TTM
    'earnings_per_share_basic_yoy_growth_fy',  # EPS growth YoY annual
    'total_revenue',               # Revenue TTM
    'revenue_growth_quarterly_yoy',# Revenue growth quarterly YoY
    'sector',
    'industry',
    'description',
]


def fetch_batch_fundamentals(market: str = "india") -> dict:
    """
    Fetch fundamental summary for ALL stocks in the market in one API call.
    Returns {ticker: {pe, roe, eps_ttm, ...}} dict.
    Stores results in tv_fundamentals SQLite table.
    """
    _ensure_tables()

    try:
        from tradingview_screener import Query
    except ImportError:
        logger.error("tradingview-screener not installed. Run: pip install tradingview-screener")
        return {}

    logger.info(f"Fetching batch fundamentals for {market} from TradingView...")
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

    # Parse and store
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    now = datetime.now(timezone.utc).isoformat()
    stored = 0
    result = {}

    for _, row in df.iterrows():
        raw_ticker = str(row.get('ticker', '') or row.get('name', '') or '')
        # TradingView returns "NSE:RELIANCE" — strip exchange prefix
        ticker = raw_ticker.split(':')[-1].strip().upper()
        if not ticker:
            continue

        def _f(col, default=None):
            v = row.get(col)
            if v is None or (isinstance(v, float) and str(v) == 'nan'):
                return default
            try:
                return float(v)
            except:
                return default

        def _s(col, default=''):
            v = row.get(col)
            return str(v).strip() if v and str(v) != 'nan' else default

        entry = {
            "pe_ratio":        _f('price_earnings_ttm'),
            "pb_ratio":        _f('price_book_fq'),
            "roe":             _f('return_on_equity'),
            "roa":             _f('return_on_assets'),
            "gross_margin":    _f('gross_margin'),
            "operating_margin":_f('operating_margin'),
            "net_margin":      _f('net_margin'),
            "debt_to_equity":  _f('debt_to_equity'),
            "current_ratio":   _f('current_ratio'),
            "eps_ttm":         _f('earnings_per_share_basic_ttm'),
            "eps_growth_ttm":  _f('earnings_per_share_basic_yoy_growth_fy'),
            "revenue_ttm":     _f('total_revenue'),
            "revenue_growth":  _f('revenue_growth_quarterly_yoy'),
            "market_cap":      _f('market_cap_basic'),
            "company_name":    _s('description') or _s('name'),
            "sector":          _s('sector'),
            "industry":        _s('industry'),
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
            entry["pe_ratio"], entry["pb_ratio"],
            entry["roe"], entry["roa"],
            entry["gross_margin"], entry["operating_margin"], entry["net_margin"],
            entry["debt_to_equity"], entry["current_ratio"],
            entry["eps_ttm"], entry["eps_growth_ttm"],
            entry["revenue_ttm"], entry["revenue_growth"],
            entry["market_cap"],
            entry["company_name"], entry["sector"], entry["industry"],
            now,
        ))
        stored += 1

    conn.commit()
    conn.close()
    logger.info(f"✅ Stored {stored} tickers in tv_fundamentals")
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

    fetched_at = row[17]
    age_h = 999
    if fetched_at:
        try:
            age_h = (datetime.now(timezone.utc) -
                     datetime.fromisoformat(fetched_at)).total_seconds() / 3600
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
    """Check if the batch data was fetched recently."""
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
# LAYER 2 — PER-TICKER DETAIL via tradingview-scraper
# Fetches quarterly + annual time-series for one ticker on demand
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


def fetch_ticker_detail(ticker: str) -> dict:
    """
    Fetch quarterly + annual financials for one ticker via tradingview-scraper.
    Returns screener.in-compatible dict with quarterly[], annual[], ratios{}.
    Caches 24h.
    """
    cached = _get_cached_detail(ticker)
    if cached:
        return cached

    # Try tradingview-scraper first (per-ticker quarterly data)
    result = _fetch_tv_scraper(ticker)

    # Fallback: build from batch data if scraper fails
    if not result or "error" in result:
        batch = get_batch_fundamental(ticker)
        if batch:
            result = _build_from_batch(ticker, batch)
        else:
            result = {"error": f"No fundamental data for {ticker}", "ticker": ticker}

    _set_cached_detail(ticker, result)
    return result


def _fetch_tv_scraper(ticker: str) -> dict:
    """
    Fetch per-ticker quarterly/annual data via tradingview-scraper.
    Converts to screener.in-compatible format.
    """
    try:
        from tradingview_scraper.symbols.financials import Financials
        from tradingview_scraper.symbols.overview import Overview
    except ImportError:
        logger.warning("tradingview-scraper not installed. Run: pip install tradingview-scraper")
        return {}

    symbol = f"NSE:{ticker.upper()}"

    # ── Quarterly financials ───────────────────────────────────────────────────
    quarterly = []
    annual = []
    ratios = {}
    company_name = ticker

    try:
        fin = Financials(symbol=symbol)

        # Income statement — quarterly
        try:
            inc_q = fin.get_income_statement(period='quarterly')
            if inc_q and inc_q.get('status') == 'success':
                data = inc_q.get('data', {})
                # Build quarterly rows
                # Keys: total_revenue, net_income, earnings_per_share_basic_ttm, operating_income
                periods = data.get('dates', [])
                revenues = data.get('total_revenue', [None]*len(periods))
                net_incomes = data.get('net_income', [None]*len(periods))
                eps_vals = data.get('earnings_per_share_diluted', [None]*len(periods))
                op_incomes = data.get('operating_income', [None]*len(periods))

                for i, period in enumerate(periods):
                    rev = revenues[i] if i < len(revenues) else None
                    ni  = net_incomes[i] if i < len(net_incomes) else None
                    eps = eps_vals[i] if i < len(eps_vals) else None
                    oi  = op_incomes[i] if i < len(op_incomes) else None
                    opm = round(oi / rev * 100, 1) if rev and oi and rev > 0 else None
                    npm = round(ni / rev * 100, 1) if rev and ni and rev > 0 else None

                    quarterly.append({
                        "period": str(period),
                        "sales": _safe_num(rev),
                        "net_profit": _safe_num(ni),
                        "eps": _safe_num(eps),
                        "opm": opm,
                        "npm": npm,
                    })
        except Exception as e:
            logger.debug(f"TV quarterly income failed {ticker}: {e}")

        # Income statement — annual
        try:
            inc_a = fin.get_income_statement(period='annual')
            if inc_a and inc_a.get('status') == 'success':
                data = inc_a.get('data', {})
                periods = data.get('dates', [])
                revenues = data.get('total_revenue', [])
                net_incomes = data.get('net_income', [])
                eps_vals = data.get('earnings_per_share_diluted', [])

                for i, period in enumerate(periods):
                    rev = revenues[i] if i < len(revenues) else None
                    ni  = net_incomes[i] if i < len(net_incomes) else None
                    eps = eps_vals[i] if i < len(eps_vals) else None
                    annual.append({
                        "period": str(period),
                        "sales": _safe_num(rev),
                        "net_profit": _safe_num(ni),
                        "eps": _safe_num(eps),
                    })
        except Exception as e:
            logger.debug(f"TV annual income failed {ticker}: {e}")

    except Exception as e:
        logger.debug(f"TV Financials failed {ticker}: {e}")

    # ── Overview / Ratios ──────────────────────────────────────────────────────
    try:
        ov = Overview(symbol=symbol)
        stats = ov.get_statistics()
        if stats and stats.get('status') == 'success':
            d = stats.get('data', {})
            ratios = {
                "roe":            _safe_num(d.get('return_on_equity_fq')),
                "debt_to_equity": _safe_num(d.get('debt_to_equity')),
                "pe_ratio":       _safe_num(d.get('price_earnings_ttm')),
                "current_ratio":  _safe_num(d.get('current_ratio')),
            }
            company_name = str(d.get('description', '') or ticker)
    except Exception as e:
        logger.debug(f"TV Overview failed {ticker}: {e}")

    # ── Fallback to batch for ratios if scraper had no data ────────────────────
    if not ratios:
        batch = get_batch_fundamental(ticker)
        if batch:
            ratios = {
                "roe":            batch.get("roe"),
                "debt_to_equity": batch.get("debt_to_equity"),
                "pe_ratio":       batch.get("pe_ratio"),
                "current_ratio":  batch.get("current_ratio"),
            }
            if not company_name or company_name == ticker:
                company_name = batch.get("company_name", ticker)

    if not quarterly and not annual and not ratios:
        return {"error": f"No TV data for {ticker}", "ticker": ticker}

    return {
        "ticker":       ticker,
        "company_name": company_name,
        "quarterly":    quarterly,
        "annual":       annual,
        "ratios":       ratios,
        "source":       "tradingview",
    }


def _build_from_batch(ticker: str, batch: dict) -> dict:
    """
    Build a minimal screener-compatible dict from batch summary data.
    Used when per-ticker scraper fails — gives ratios but no time series.
    """
    ratios = {
        "roe":            batch.get("roe"),
        "debt_to_equity": batch.get("debt_to_equity"),
        "pe_ratio":       batch.get("pe_ratio"),
        "current_ratio":  batch.get("current_ratio"),
    }

    # Build minimal quarterly from EPS TTM (single point)
    eps_ttm = batch.get("eps_ttm")
    quarterly = []
    if eps_ttm is not None:
        quarterly = [{"period": "TTM", "eps": eps_ttm, "sales": None,
                      "net_profit": None, "opm": batch.get("operating_margin"),
                      "npm": batch.get("net_margin")}]

    return {
        "ticker":       ticker,
        "company_name": batch.get("company_name", ticker),
        "quarterly":    quarterly,
        "annual":       [],
        "ratios":       ratios,
        "source":       "tv_batch_fallback",
    }


def _safe_num(v):
    """Convert to float safely, return None on failure."""
    if v is None:
        return None
    try:
        f = float(v)
        import math
        return None if math.isnan(f) or math.isinf(f) else f
    except:
        return None
