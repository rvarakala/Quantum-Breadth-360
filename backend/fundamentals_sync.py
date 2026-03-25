"""
Fundamentals Sync — Fetch EPS, Market Cap, Sector from Yahoo Finance
====================================================================
Uses yfinance .info for EPS/PE/sector and Yahoo v8 for market cap fallback.
Stores in market_cap table (extended with eps, pe columns).
"""

import sqlite3
import logging
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)
DB_PATH = Path(__file__).parent / "breadth_data.db"


def _ensure_columns():
    """Add eps/pe/industry columns to market_cap table if missing."""
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    # Check existing columns
    cols = [r[1] for r in conn.execute("PRAGMA table_info(market_cap)").fetchall()]
    if "eps_ttm" not in cols:
        conn.execute("ALTER TABLE market_cap ADD COLUMN eps_ttm REAL DEFAULT NULL")
    if "pe_ratio" not in cols:
        conn.execute("ALTER TABLE market_cap ADD COLUMN pe_ratio REAL DEFAULT NULL")
    if "industry" not in cols:
        conn.execute("ALTER TABLE market_cap ADD COLUMN industry TEXT DEFAULT NULL")
    if "yf_mcap" not in cols:
        conn.execute("ALTER TABLE market_cap ADD COLUMN yf_mcap REAL DEFAULT NULL")
    conn.commit()
    conn.close()


def _fetch_fundamentals_yf(ticker):
    """Fetch fundamentals for one ticker using yfinance."""
    import yfinance as yf
    try:
        t = yf.Ticker(f"{ticker}.NS")
        info = t.info
        if not info or info.get("trailingPE") is None and info.get("marketCap") is None:
            return None
        return {
            "ticker": ticker,
            "eps_ttm": info.get("trailingEps"),
            "pe_ratio": info.get("trailingPE"),
            "mcap": info.get("marketCap"),  # in INR (not Crores)
            "sector": info.get("sector"),
            "industry": info.get("industry"),
            "name": info.get("shortName") or info.get("longName"),
        }
    except Exception as e:
        logger.debug(f"yfinance info failed for {ticker}: {e}")
        return None


def _fetch_fundamentals_v8(ticker):
    """Fallback: fetch basic data from Yahoo v8 chart API."""
    import httpx
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}.NS"
        r = httpx.get(url, params={"range": "5d", "interval": "1d"},
                      headers={"User-Agent": "Mozilla/5.0"}, timeout=15, follow_redirects=True)
        if r.status_code == 200:
            meta = r.json().get("chart", {}).get("result", [{}])[0].get("meta", {})
            return {
                "ticker": ticker,
                "name": meta.get("shortName") or meta.get("longName"),
                "mcap": None,  # v8 doesn't have market cap
                "eps_ttm": None,
                "pe_ratio": None,
                "sector": None,
                "industry": None,
            }
    except:
        pass
    return None


def sync_fundamentals(tickers=None, max_workers=3, progress_state=None):
    """
    Sync EPS, PE, market cap fundamentals.
    Strategy:
      1. Try TradingView batch (one call, all NSE stocks, ~10 seconds) 
      2. Fall back to yfinance per-ticker only if TV batch fails
    """
    _ensure_columns()

    # ── Try TV batch first ─────────────────────────────────────────────────────
    if progress_state:
        progress_state["total"]    = 1
        progress_state["progress"] = 0
        progress_state["message"]  = "Fetching fundamentals via TradingView batch..."

    try:
        from tv_fundamentals import fetch_batch_fundamentals, _ensure_tables as _tv_ensure
        _tv_ensure()
        tv_data = fetch_batch_fundamentals(market="india")

        if tv_data and len(tv_data) > 100:
            # TV batch succeeded — write to market_cap table too
            # so all existing code that reads market_cap still works
            conn = sqlite3.connect(str(DB_PATH), timeout=30)
            updated = 0
            for ticker, d in tv_data.items():
                pe   = d.get("pe_ratio")
                eps  = d.get("eps_ttm")
                mcap = d.get("market_cap")   # already in native currency units from TV
                name = d.get("company_name") or ticker
                ind  = d.get("industry") or ""

                # Convert TV market_cap to Crores (TV returns in native currency)
                mcap_cr = None
                tier    = "Unknown"
                if mcap and mcap > 0:
                    # TV returns market cap in INR (for NSE)
                    mcap_cr = mcap / 10_000_000   # INR → Crores
                    if mcap_cr >= 100_000: tier = "Mega Cap"
                    elif mcap_cr >= 20_000: tier = "Large Cap"
                    elif mcap_cr >= 5_000:  tier = "Mid Cap"
                    elif mcap_cr >= 500:    tier = "Small Cap"
                    else:                   tier = "Micro Cap"

                try:
                    conn.execute("""
                        INSERT INTO market_cap
                            (ticker, company_name, mcap_cr, mcap_tier,
                             eps_ttm, pe_ratio, industry, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                        ON CONFLICT(ticker) DO UPDATE SET
                            eps_ttm      = COALESCE(excluded.eps_ttm, market_cap.eps_ttm),
                            pe_ratio     = COALESCE(excluded.pe_ratio, market_cap.pe_ratio),
                            industry     = COALESCE(excluded.industry, market_cap.industry),
                            company_name = COALESCE(excluded.company_name, market_cap.company_name),
                            mcap_cr      = CASE WHEN excluded.mcap_cr > 0
                                                THEN excluded.mcap_cr
                                                ELSE market_cap.mcap_cr END,
                            mcap_tier    = CASE WHEN excluded.mcap_tier != 'Unknown'
                                                THEN excluded.mcap_tier
                                                ELSE market_cap.mcap_tier END,
                            updated_at   = CURRENT_TIMESTAMP
                    """, (ticker, name, mcap_cr, tier, eps, pe, ind))
                    updated += 1
                except Exception as e:
                    logger.debug(f"market_cap upsert failed {ticker}: {e}")

            conn.commit()
            conn.close()

            msg = f"✅ TV batch: {updated}/{len(tv_data)} tickers synced (EPS/PE/MCap)"
            if progress_state:
                progress_state["message"]  = msg
                progress_state["progress"] = 1
                progress_state["total"]    = 1
            logger.info(msg)
            return {"message": msg, "updated": updated, "failed": 0,
                    "total": len(tv_data), "source": "tradingview_batch"}

    except Exception as e:
        logger.warning(f"TV batch failed ({e}) — falling back to yfinance per-ticker")

    # ── Fallback: yfinance per-ticker ─────────────────────────────────────────
    logger.info("Falling back to yfinance per-ticker fundamentals sync...")

    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    if tickers is None:
        rows = conn.execute("""
            SELECT DISTINCT o.ticker FROM ohlcv o
            LEFT JOIN market_cap m ON o.ticker = m.ticker
            WHERE o.market = 'India'
              AND (m.ticker IS NULL OR m.eps_ttm IS NULL)
        """).fetchall()
        tickers = [r[0] for r in rows]
    conn.close()

    total = len(tickers)
    if total == 0:
        return {"message": "All tickers already have fundamentals", "updated": 0}

    logger.info(f"yfinance fallback: {total} tickers...")
    if progress_state:
        progress_state["total"]    = total
        progress_state["progress"] = 0
        progress_state["message"]  = f"yfinance fallback: {total} tickers..."

    updated = 0
    failed  = 0

    for batch_start in range(0, total, max_workers):
        batch = tickers[batch_start:batch_start + max_workers]

        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            futures = {pool.submit(_fetch_fundamentals_yf, t): t for t in batch}
            for future in as_completed(futures):
                ticker = futures[future]
                result = future.result()

                if result and (result.get("eps_ttm") is not None
                               or result.get("mcap") is not None):
                    conn = sqlite3.connect(str(DB_PATH), timeout=30)
                    mcap_cr = result["mcap"] / 10_000_000 if result.get("mcap") else None
                    tier = "Unknown"
                    if mcap_cr:
                        if mcap_cr >= 100_000: tier = "Mega Cap"
                        elif mcap_cr >= 20_000: tier = "Large Cap"
                        elif mcap_cr >= 5_000:  tier = "Mid Cap"
                        elif mcap_cr >= 500:    tier = "Small Cap"
                        else:                   tier = "Micro Cap"
                    conn.execute("""
                        INSERT INTO market_cap
                            (ticker, company_name, mcap_cr, mcap_tier,
                             eps_ttm, pe_ratio, industry, yf_mcap, updated_at)
                        VALUES (?,?,COALESCE(?,(SELECT mcap_cr FROM market_cap WHERE ticker=?)),
                                COALESCE(?,(SELECT mcap_tier FROM market_cap WHERE ticker=?)),
                                ?,?,?,?,CURRENT_TIMESTAMP)
                        ON CONFLICT(ticker) DO UPDATE SET
                            eps_ttm      = COALESCE(excluded.eps_ttm, market_cap.eps_ttm),
                            pe_ratio     = COALESCE(excluded.pe_ratio, market_cap.pe_ratio),
                            industry     = COALESCE(excluded.industry, market_cap.industry),
                            yf_mcap      = excluded.yf_mcap,
                            mcap_cr      = CASE WHEN excluded.mcap_cr > 0
                                                THEN excluded.mcap_cr
                                                ELSE market_cap.mcap_cr END,
                            mcap_tier    = CASE WHEN excluded.mcap_tier != 'Unknown'
                                                THEN excluded.mcap_tier
                                                ELSE market_cap.mcap_tier END,
                            company_name = COALESCE(excluded.company_name,
                                                    market_cap.company_name),
                            updated_at   = CURRENT_TIMESTAMP
                    """, (ticker, result.get("name"), mcap_cr, ticker, tier, ticker,
                          result.get("eps_ttm"), result.get("pe_ratio"),
                          result.get("industry"), result.get("mcap")))
                    conn.commit()
                    conn.close()
                    updated += 1
                else:
                    failed += 1

                done = updated + failed
                if progress_state:
                    progress_state["progress"] = done
                    progress_state["message"] = (
                        f"Fundamentals: {done}/{total} ({ticker}) "
                        f"— {updated} ok, {failed} failed"
                    )
                if done % 50 == 0:
                    logger.info(
                        f"Fundamentals: {done}/{total} ({updated} ok, {failed} failed)"
                    )
        time.sleep(1)

    msg = f"Fundamentals sync: {updated} updated, {failed} failed"
    logger.info(msg)
    return {"message": msg, "updated": updated, "failed": failed, "total": total}


def get_eps_for_ticker(ticker):
    """Get EPS for a single ticker. Fetches from Yahoo if not in DB."""
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    _ensure_columns()
    row = conn.execute("SELECT eps_ttm, pe_ratio FROM market_cap WHERE ticker=?", (ticker,)).fetchone()
    conn.close()
    
    if row and row[0] is not None:
        return {"eps_ttm": row[0], "pe_ratio": row[1]}
    
    # Fetch on demand
    data = _fetch_fundamentals_yf(ticker)
    if data and data.get("eps_ttm") is not None:
        # Store it
        conn = sqlite3.connect(str(DB_PATH), timeout=10)
        conn.execute("""
            INSERT INTO market_cap (ticker, company_name, eps_ttm, pe_ratio, industry, updated_at)
            VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(ticker) DO UPDATE SET
                eps_ttm = excluded.eps_ttm,
                pe_ratio = excluded.pe_ratio,
                industry = COALESCE(excluded.industry, market_cap.industry),
                updated_at = CURRENT_TIMESTAMP
        """, (ticker, data.get("name"), data["eps_ttm"], data.get("pe_ratio"), data.get("industry")))
        conn.commit()
        conn.close()
        return {"eps_ttm": data["eps_ttm"], "pe_ratio": data.get("pe_ratio")}
    
    return {"eps_ttm": None, "pe_ratio": None}


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    
    if "--test" in sys.argv:
        # Test single ticker
        ticker = sys.argv[sys.argv.index("--test") + 1] if len(sys.argv) > sys.argv.index("--test") + 1 else "RELIANCE"
        print(f"Testing {ticker}...")
        data = _fetch_fundamentals_yf(ticker)
        print(f"Result: {data}")
    else:
        # Sync all missing
        result = sync_fundamentals(max_workers=3)
        print(f"Result: {result}")
