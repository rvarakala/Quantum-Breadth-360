"""
Market Breadth Engine — Backend
- Reads OHLCV from local SQLite DB (10 years)
- Falls back to live yfinance if DB is empty
- /api/sync/start   → kick off 10-year backfill
- /api/sync/update  → daily incremental update
- /api/sync/status  → see what's stored locally
"""
from fastapi import FastAPI, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from datetime import datetime, timezone, date, timedelta
from typing import Dict
import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor
import os, pathlib

from cache import _cache, get_cache, set_cache, _load_disk_cache
from utils import (
    safe_float, flatten_df, safe_download, get_close, get_change_pct,
    get_stock_data, get_screener_data, fetch_batch,
    DB_AVAILABLE, INDIA_TICKERS, SP500_TICKERS,
    US_SECTORS, INDIA_SECTORS,
    db_stats, load_sector_map, load_sector_counts, import_sectors_csv,
    save_ticker_universe, load_ticker_universe, import_ticker_universe_csv,
    save_sector_map, import_nifty500_csv,
    run_full_backfill, run_daily_update,
)
from breadth import (
    compute_breadth, _compute_market, _regime_interp,
)
from screeners import (
    CUSTOM_SCREENER_MAP, apply_custom_screener,
    _rs_cache, RS_CACHE_TTL, _rs_cache_key,
    _compute_rs_rankings, _build_sector_map,
    LEADERS_CACHE_TTL, _leaders_cache, _compute_leaders,
)
from stockbee import _compute_stockbee
from nse_sync import sync_nifty500, sync_full_history, _get_stale_tickers
from fundamentals_sync import sync_fundamentals, get_eps_for_ticker
from charts import get_chart_data
from stock_metrics import compute_stock_metrics, compute_eps_async
from smart_metrics_service import get_smart_metrics
from liquidity_regime import compute_iv_footprint
from peep_into_past import compute_historical_breadth
from sectors_heatmap import compute_sector_heatmap
from watchlist import (
    list_watchlists, create_watchlist, delete_watchlist,
    add_ticker as wl_add_ticker, remove_ticker as wl_remove_ticker,
    get_watchlist_data, create_alert, list_alerts, delete_alert, check_alerts,
)
from email_digest import generate_market_summary, generate_summary_html
from nse_indices import (
    sync_nse_indices, get_index_constituents, get_index_registry_status,
    get_ticker_indices, NSE_INDEX_REGISTRY, _ensure_tables as ensure_nse_tables,
    get_universe_stats, get_stale_constituent_tickers, get_all_constituent_tickers,
)
from market_cap import (
    import_market_cap_csv, get_mcap_for_ticker, get_all_mcaps,
    filter_by_mcap, format_mcap, get_mcap_tier, _ensure_table as ensure_mcap_table,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(title="Market Breadth Engine", version="2.0.0")
app.add_middleware(CORSMiddleware,
                   allow_origins=["*"],
                   allow_credentials=False,
                   allow_methods=["*"],
                   allow_headers=["*"])
executor = ThreadPoolExecutor(max_workers=4)

# ── Serve frontend from backend (no separate server needed) ──────────────────
FRONTEND_DIR = pathlib.Path(__file__).parent.parent / "frontend"

# Auto cache-bust: version = server start timestamp
_ASSET_VERSION = str(int(datetime.now(timezone.utc).timestamp()))
_NO_CACHE = {"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache", "Expires": "0"}

def _serve_index_html():
    """Read index.html and inject current asset version."""
    from fastapi.responses import HTMLResponse
    html = (FRONTEND_DIR / "index.html").read_text(encoding="utf-8")
    html = html.replace("__ASSET_VERSION__", _ASSET_VERSION)
    return HTMLResponse(content=html, headers=_NO_CACHE)

@app.get("/")
def serve_root():
    return _serve_index_html()

@app.get("/index.html")
def serve_index():
    return _serve_index_html()

# Serve static assets if any
if FRONTEND_DIR.exists():
    try:
        app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")
    except Exception:
        pass

# NSE Indices sync state
_nse_indices_state = {"running": False, "progress": 0, "total": 0, "message": "", "result": None}

# Sync state (in-memory progress tracker)
_sync_state = {"running": False, "progress": 0, "total": 0,
               "message": "Idle", "last_run": None, "result": None}

# ── Dynamic ticker universe (set by frontend upload) ─────────────────────────
_custom_tickers: Dict[str, list] = {}   # { 'India': [...], 'US': [...] }


def _enrich_stocks_mcap(stocks: list):
    """Add mcap_cr and mcap_tier to each stock dict in-place."""
    all_mcaps = get_all_mcaps()
    for s in stocks:
        mcap_data = all_mcaps.get(s.get("ticker", ""), {})
        s["mcap_cr"] = mcap_data.get("mcap_cr", 0)
        s["mcap_tier"] = mcap_data.get("mcap_tier", "")

# ── API Routes ────────────────────────────────────────────────────────────────

@app.get("/api/health")
def health():
    return {"status":"ok","db":DB_AVAILABLE,
            "cached":list(_cache.keys()),
            "time":datetime.now(timezone.utc).isoformat()}

@app.get("/api/debug")
def debug():
    tests={"RELIANCE.NS":"India stock","AAPL":"US stock","^CRSLDX":"NIFTY 500 index","^VIX":"VIX"}
    results={}
    for ticker,desc in tests.items():
        try:
            df=safe_download(ticker)
            results[ticker]={"desc":desc,"ok":not df.empty,"rows":len(df),
                             "close":get_close(df) or None}
        except Exception as e:
            results[ticker]={"desc":desc,"ok":False,"error":str(e)[:120]}
    return {"all_ok":all(v["ok"] for v in results.values()),
            "tests":results,"db_available":DB_AVAILABLE,
            "time":datetime.now(timezone.utc).isoformat()}

@app.get("/api/sync/status")
def sync_status():
    """Check what's stored in the local DB"""
    if not DB_AVAILABLE:
        return {"error":"data_store.py not loaded"}
    stats = db_stats()
    return {**stats, "sync_running": _sync_state["running"],
            "sync_message": _sync_state["message"],
            "sync_progress": _sync_state["progress"],
            "sync_total": _sync_state["total"],
            "last_sync": _sync_state["last_run"]}

@app.post("/api/sync/start")
async def sync_start(background_tasks: BackgroundTasks,
                     market: str = "all", years: int = 10):
    """
    Kick off 10-year historical backfill in the background.
    market = 'India' | 'US' | 'Index' | 'all'
    This runs in background — poll /api/sync/status to track progress.
    """
    if not DB_AVAILABLE:
        return {"error":"data_store.py not found"}
    if _sync_state["running"]:
        return {"message":"Sync already running","progress":_sync_state["progress"],
                "total":_sync_state["total"]}

    def _run():
        _sync_state["running"] = True
        _sync_state["message"] = f"Downloading {years}y history for {market}..."
        _sync_state["last_run"] = datetime.now(timezone.utc).isoformat()
        try:
            result = run_full_backfill(market=market)
            _sync_state["result"]  = result
            _sync_state["message"] = f"Done! {result['done']} tickers, {result['total_rows']:,} rows stored"
        except Exception as e:
            _sync_state["message"] = f"Error: {e}"
            _sync_state["result"]  = {"error": str(e)}
        finally:
            _sync_state["running"] = False

    background_tasks.add_task(_run)
    return {"message":f"Backfill started for {market} ({years} years). Poll /api/sync/status for progress.",
            "tip":"This takes 5–15 minutes. Dashboard will auto-use DB data when ready."}

@app.post("/api/sync/update")
async def sync_update(background_tasks: BackgroundTasks):
    """Daily incremental update — only fetches new candles"""
    if not DB_AVAILABLE:
        return {"error":"data_store.py not found"}
    if _sync_state["running"]:
        return {"message":"Sync already running"}

    def _run():
        _sync_state["running"] = True
        _sync_state["message"] = "Running daily update..."
        try:
            result = run_daily_update()
            _sync_state["message"] = f"Update done: {result['updated']} tickers, {result['total_new_rows']} new rows"
            _sync_state["result"]  = result
        except Exception as e:
            _sync_state["message"] = f"Error: {e}"
        finally:
            _sync_state["running"] = False

    background_tasks.add_task(_run)
    return {"message":"Daily update started. Poll /api/sync/status to track."}



# ── NSE Direct Sync (Yahoo v8 API — no yfinance dependency) ──────────────────
_nse_sync_state = {"running": False, "progress": 0, "total": 0, "message": "", "result": None}

@app.get("/api/nse-sync/status")
def nse_sync_status():
    return _nse_sync_state

@app.post("/api/nse-sync/start")
async def nse_sync_start(background_tasks: BackgroundTasks, range: str = "3mo"):
    """Sync stale NIFTY 500 tickers via Yahoo v8 direct API."""
    if _nse_sync_state["running"]:
        return {"message": "Sync already running", **_nse_sync_state}

    def _run():
        _nse_sync_state["running"] = True
        _nse_sync_state["message"] = "Starting NSE sync..."
        try:
            result = sync_nifty500(range_str=range, max_workers=5, progress_state=_nse_sync_state)
            _nse_sync_state["result"] = result
            _nse_sync_state["message"] = result["message"]
        except Exception as e:
            _nse_sync_state["message"] = f"Error: {e}"
            _nse_sync_state["result"] = {"error": str(e)}
        finally:
            _nse_sync_state["running"] = False

    background_tasks.add_task(_run)
    return {"message": "NSE sync started. Poll /api/nse-sync/status for progress."}

@app.post("/api/nse-sync/full")
async def nse_sync_full(background_tasks: BackgroundTasks):
    """Full 2-year history sync for ALL NIFTY 500 tickers."""
    if _nse_sync_state["running"]:
        return {"message": "Sync already running", **_nse_sync_state}

    def _run():
        _nse_sync_state["running"] = True
        _nse_sync_state["message"] = "Full 2-year sync starting..."
        try:
            result = sync_full_history(range_str="2y", max_workers=3)
            _nse_sync_state["result"] = result
            _nse_sync_state["message"] = result["message"]
        except Exception as e:
            _nse_sync_state["message"] = f"Error: {e}"
        finally:
            _nse_sync_state["running"] = False

    background_tasks.add_task(_run)
    return {"message": "Full 2-year sync started. This takes 10-15 minutes."}

@app.post("/api/nse-sync/force-today")
async def nse_sync_force_today(background_tasks: BackgroundTasks):
    """Force sync ALL NIFTY 500 tickers with latest 1 month data.
    Use after market close to get today's EOD data."""
    if _nse_sync_state["running"]:
        return {"message": "Sync already running", **_nse_sync_state}

    def _run():
        _nse_sync_state["running"] = True
        _nse_sync_state["message"] = "Force EOD sync starting..."
        try:
            result = sync_full_history(range_str="1mo", max_workers=5)
            _nse_sync_state["result"] = result
            _nse_sync_state["message"] = result["message"]
            # Clear breadth cache so next load computes fresh
            from cache import CACHE
            keys_to_clear = [k for k in CACHE if k.startswith("breadth_") or k.startswith("stockbee_") or k.startswith("rs_")]
            for k in keys_to_clear:
                CACHE.pop(k, None)
            if keys_to_clear:
                _nse_sync_state["message"] += f" | Cache cleared ({len(keys_to_clear)} keys)"
        except Exception as e:
            _nse_sync_state["message"] = f"Error: {e}"
        finally:
            _nse_sync_state["running"] = False

    background_tasks.add_task(_run)
    return {"message": "Force EOD sync started for all 500 tickers (1 month). Takes 3-5 minutes."}


@app.post("/api/fundamentals/sync")
async def fundamentals_sync_endpoint(background_tasks: BackgroundTasks):
    """Sync EPS, PE, market cap from Yahoo Finance for all tickers missing data."""
    if _nse_sync_state["running"]:
        return {"message": "Another sync is already running"}
    
    def _run():
        _nse_sync_state["running"] = True
        _nse_sync_state["message"] = "Fetching fundamentals (EPS, PE, Market Cap)..."
        try:
            result = sync_fundamentals(max_workers=3, progress_state=_nse_sync_state)
            _nse_sync_state["result"] = result
            _nse_sync_state["message"] = result["message"]
        except Exception as e:
            _nse_sync_state["message"] = f"Error: {e}"
        finally:
            _nse_sync_state["running"] = False
    
    background_tasks.add_task(_run)
    return {"message": "Fundamentals sync started. This fetches EPS/PE/MCap for all tickers."}

@app.get("/api/eps/{ticker}")
async def get_eps(ticker: str):
    """Get EPS for a ticker. Fetches from Yahoo if not cached."""
    ticker = ticker.upper().strip()
    return get_eps_for_ticker(ticker)

@app.get("/api/nse-sync/stale")
def nse_sync_stale():
    """Check which NIFTY 500 tickers have stale data."""
    stale = _get_stale_tickers(days_threshold=3)
    return {"stale_count": len(stale), "total": 500, "fresh": 500 - len(stale),
            "stale_tickers": [(t, d) for t, d in stale[:30]]}

@app.get("/api/breadth/iv-footprint")
async def iv_footprint(market: str = "India", days: int = 30):
    """Count IV/PPV/Bull Snort signals per day across the universe."""
    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(executor, compute_iv_footprint, market, days)
    return {"market": market, "days": days, "data": data}

@app.get("/api/breadth/{market}")
async def get_breadth(market: str, refresh: bool = False):
    market=market.upper()
    if market not in ("INDIA","US"):
        return {"error":"Market must be INDIA or US"}
    key=f"breadth_{market}"
    if not refresh:
        cached=get_cache(key)
        if cached:
            return {**cached,"cached":True}
    loop=asyncio.get_event_loop()
    result=await loop.run_in_executor(executor, lambda: _compute_market(market, custom_tickers=_custom_tickers))
    if "error" not in result:
        set_cache(key,result)
    return {**result,"cached":False}

@app.get("/api/compare")
async def compare():
    def s(d):
        if not d or "error" in d: return None
        return {k:d.get(k) for k in ("score","regime","regime_color","pct_above_50",
                "pct_above_200","ad_ratio","nh_nl","vix","index_price",
                "index_change_pct","index_name")}
    return {"India":s(get_cache("breadth_INDIA")),"US":s(get_cache("breadth_US"))}


@app.get("/api/ohlcv/fetch")
async def fetch_ohlcv_gap(ticker: str, from_date: str = None, to_date: str = None):
    """
    Fetch OHLCV for a single ticker between two dates.
    Used by the browser sync engine to fill gaps.
    ticker: yfinance format e.g. RELIANCE.NS or AAPL
    """
    import yfinance as yf

    if not from_date:
        from_date = (date.today() - timedelta(days=7)).isoformat()
    if not to_date:
        to_date = (date.today() + timedelta(days=1)).isoformat()

    try:
        df = yf.download(ticker, start=from_date, end=to_date,
                         interval="1d", auto_adjust=True,
                         progress=False, timeout=20)
        if df is None or df.empty:
            return {"rows": [], "ticker": ticker, "from": from_date, "to": to_date}

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        rows = []
        for idx, row in df.iterrows():
            d = str(idx.date()) if hasattr(idx, 'date') else str(idx)[:10]
            close_val = safe_float(row.get("Close"))
            if not close_val:
                continue
            rows.append({
                "date":   d,
                "open":   safe_float(row.get("Open")),
                "high":   safe_float(row.get("High")),
                "low":    safe_float(row.get("Low")),
                "close":  close_val,
                "volume": int(row.get("Volume", 0) or 0),
            })

        logger.info(f"Gap fetch {ticker} {from_date}→{to_date}: {len(rows)} rows")
        return {"rows": rows, "ticker": ticker, "from": from_date, "to": to_date}

    except Exception as e:
        logger.error(f"Gap fetch failed {ticker}: {e}")
        return {"rows": [], "error": str(e)}


@app.get("/api/ohlcv/lastdate")
async def get_last_dates(market: str = "India"):
    """Return the last stored date per ticker from local DB (if available)"""
    if not DB_AVAILABLE:
        return {"error": "DB not available"}
    try:
        return db_stats()
    except Exception as e:
        return {"error": str(e)}

@app.post("/api/universe/set")
async def set_universe(payload: dict):
    """Set custom ticker universe from frontend upload"""
    tickers = payload.get("tickers", [])
    market  = payload.get("market", "India")
    if not tickers:
        return {"error": "Invalid ticker list"}
    clean = list(set([t.upper().replace(".NS","").replace(".BO","").strip()
                      for t in tickers if t and len(str(t)) <= 20]))
    _custom_tickers[market] = clean
    if DB_AVAILABLE:
        save_ticker_universe(market, clean)
    if f"breadth_{market.upper()}" in _cache:
        del _cache[f"breadth_{market.upper()}"]
    logger.info(f"Universe set: {len(clean)} {market} tickers")
    return {"ok": True, "market": market, "tickers_set": len(clean), "sample": clean[:5]}

@app.get("/api/universe/status")
def get_universe_status():
    """Check active ticker universes"""
    india = _custom_tickers.get("India") or (load_ticker_universe("India") if DB_AVAILABLE else []) or INDIA_TICKERS
    us    = _custom_tickers.get("US")    or SP500_TICKERS
    return {
        "India": {"source":"custom" if "India" in _custom_tickers else ("sqlite" if DB_AVAILABLE and load_ticker_universe("India") else "default"),
                  "count": len(india), "sample": india[:5]},
        "US":    {"source":"default", "count": len(us), "sample": us[:5]}
    }

@app.post("/api/sectors/upload")
async def upload_sectors(payload: dict):
    """Save sector mapping from frontend JSON upload"""
    records = payload.get("records", [])
    if not records:
        return {"error": "No records provided"}
    if DB_AVAILABLE:
        saved = save_sector_map(records)
        return {"ok": True, "saved": saved}
    return {"error": "DB not available"}

@app.get("/api/sectors/status")
def get_sectors_status():
    """Get sector map summary from SQLite"""
    if not DB_AVAILABLE:
        return {"available": False}
    counts = load_sector_counts()
    return {"available": True, "sectors": len(counts),
            "total_tickers": sum(s['tickers'] for s in counts),
            "breakdown": counts[:10]}

@app.get("/api/db/stats")
def get_db_stats():
    """Full database statistics"""
    if not DB_AVAILABLE:
        return {"available": False, "message": "Run import first"}
    stats = db_stats()
    if DB_AVAILABLE:
        try:
            stats["sector_count"]  = len(load_sector_counts())
            stats["india_universe"] = len(load_ticker_universe("India"))
        except: pass
    return {"available": True, **stats}

@app.get("/api/debug/india")
def debug_india():
    """Debug India index and VIX symbols specifically"""
    results = {}
    for ticker in ["^CRSLDX", "^INDIAVIX", "^NSEI", "^NIFTY50"]:
        try:
            df = safe_download(ticker, period="5d")
            if not df.empty:
                close = get_close(df)
                high  = round(float(df["High"].dropna().iloc[-1]), 2) if "High" in df.columns else None
                low   = round(float(df["Low"].dropna().iloc[-1]),  2) if "Low"  in df.columns else None
                results[ticker] = {"ok": True, "rows": len(df), "close": close,
                                   "high": high, "low": low,
                                   "columns": df.columns.tolist()}
            else:
                results[ticker] = {"ok": False, "rows": 0}
        except Exception as e:
            results[ticker] = {"ok": False, "error": str(e)[:80]}
    return results

@app.post("/api/universe/import-nifty500")
async def import_nifty500(payload: dict = {}):
    """
    Import NIFTY 500 CSV from backend/data/nifty500_clean.csv
    Stores 500 tickers + industry mapping in SQLite permanently.
    """
    if not DB_AVAILABLE:
        return {"error": "DB not available"}
    filepath = payload.get("filepath", "data/nifty500_clean.csv")
    # Try relative path from backend folder
    if not os.path.isabs(filepath):
        backend_dir = os.path.dirname(os.path.abspath(__file__))
        filepath = os.path.join(backend_dir, filepath)
    if not os.path.exists(filepath):
        return {"error": f"File not found: {filepath}"}
    n = import_nifty500_csv(filepath)
    # Invalidate cache
    if "breadth_INDIA" in _cache:
        del _cache["breadth_INDIA"]
    return {"ok": True, "tickers_imported": n, "filepath": filepath}


@app.get("/api/screener/rs")
async def get_rs_rankings(
    market: str = "India",
    min_rs: int = 0,
    min_mcap: float = 0,
    refresh: bool = False
):
    """
    IBD-Style RS Rankings for NIFTY 500 universe.
    Returns stocks ranked 1-99 by relative strength.
    """
    cache_key = _rs_cache_key(market)

    if not refresh and cache_key in _rs_cache:
        age = (datetime.now(timezone.utc) - _rs_cache[cache_key]["ts"]).total_seconds()
        if age < RS_CACHE_TTL:
            data = _rs_cache[cache_key]["data"]
            stocks = data["stocks"]
            if min_rs > 0:
                stocks = [s for s in stocks if s["rs_rating"] >= min_rs]
            if min_mcap > 0:
                stocks = [s for s in stocks if s.get("mcap_cr", 0) >= min_mcap]
            return {**data, "stocks": stocks, "filtered": len(stocks), "cached": True}

    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(executor, _compute_rs_rankings, market)

    if "error" not in result:
        # Enrich stocks with mcap data
        _enrich_stocks_mcap(result.get("stocks", []))
        result.pop("_stock_data", None)  # strip DataFrames before caching
        _rs_cache[cache_key] = {"data": result, "ts": datetime.now(timezone.utc)}

    stocks = result.get("stocks", [])
    if min_rs > 0:
        stocks = [s for s in stocks if s["rs_rating"] >= min_rs]
    if min_mcap > 0:
        stocks = [s for s in stocks if s.get("mcap_cr", 0) >= min_mcap]

    return {**result, "stocks": stocks, "filtered": len(stocks)}


@app.get("/api/leaders")
async def get_leaders(
    market: str = "India",
    min_mcap: float = 0,
    refresh: bool = False
):
    """
    Leaders tab endpoint.
    Returns: regime, sector health, tiered stocks, leader scores.
    Uses cached RS rankings + breadth data — no extra DB queries.
    """
    cache_key = f"leaders_{market}"

    if not refresh and cache_key in _leaders_cache:
        age = (datetime.now(timezone.utc) - _leaders_cache[cache_key]["ts"]).total_seconds()
        if age < LEADERS_CACHE_TTL:
            return {**_leaders_cache[cache_key]["data"], "cached": True}

    loop = asyncio.get_event_loop()

    # Get RS rankings (uses cache if available)
    rs_result = await loop.run_in_executor(executor, _compute_rs_rankings, market)
    if "error" in rs_result:
        return rs_result

    stocks = rs_result.get("stocks", [])
    if not stocks:
        return {"error": "No stock data available"}

    # Get breadth data for Q-BRAM regime
    # Uses same cache as /api/market — key is "breadth_INDIA" or "breadth_US"
    db_market_upper = market.upper() if market.upper() in ("INDIA","US") else "INDIA"
    breadth_cache_key = f"breadth_{db_market_upper}"
    breadth_data = {}
    cached_breadth = get_cache(breadth_cache_key)
    if cached_breadth:
        breadth_data = cached_breadth
    else:
        try:
            # _compute_market returns the full breadth dict
            bd = await loop.run_in_executor(executor, lambda: _compute_market(db_market_upper, custom_tickers=_custom_tickers))
            breadth_data = bd if isinstance(bd, dict) and "error" not in bd else {}
        except Exception as e:
            logger.warning(f"Breadth data unavailable for leaders: {e}")

    # Compute leaders
    result = _compute_leaders(market, stocks, breadth_data)

    # Enrich all tier stocks with mcap data
    for tier_key in (result.get("tiers") or {}):
        _enrich_stocks_mcap(result["tiers"][tier_key])

    # Filter by min_mcap if specified
    if min_mcap > 0:
        for tier_key in (result.get("tiers") or {}):
            result["tiers"][tier_key] = [
                s for s in result["tiers"][tier_key] if s.get("mcap_cr", 0) >= min_mcap
            ]

    result["market"] = market
    result["cached"] = False
    result["total_stocks"] = len(stocks)

    _leaders_cache[cache_key] = {
        "data": result,
        "ts": datetime.now(timezone.utc)
    }
    return result


@app.post("/api/leaders/clear-cache")
async def clear_leaders_cache(market: str = "India"):
    """Clear leaders cache to force refresh."""
    key = f"leaders_{market}"
    if key in _leaders_cache:
        del _leaders_cache[key]
    return {"cleared": True, "market": market}

@app.get("/api/db/check")
async def db_check():
    """Check what's actually in the SQLite DB"""
    if not DB_AVAILABLE:
        return {"error": "DB not available"}
    try:
        conn = __import__('sqlite3').connect(
            str(__import__('pathlib').Path(__file__).parent / 'breadth_data.db'),
            timeout=10
        )
        # Check total rows
        total = conn.execute("SELECT COUNT(*) FROM ohlcv").fetchone()[0]
        # Check market labels
        markets = conn.execute(
            "SELECT market, COUNT(DISTINCT ticker), COUNT(*), MIN(date), MAX(date) FROM ohlcv GROUP BY market"
        ).fetchall()
        # Check ticker_universe
        universe = conn.execute(
            "SELECT market, COUNT(*) FROM ticker_universe GROUP BY market"
        ).fetchall()
        # Check sector_map
        sectors = conn.execute("SELECT COUNT(*) FROM sector_map").fetchone()[0]
        conn.close()

        return {
            "total_ohlcv_rows": total,
            "markets": [{"market":r[0],"tickers":r[1],"rows":r[2],"from":r[3],"to":r[4]} for r in markets],
            "ticker_universe": [{"market":r[0],"count":r[1]} for r in universe],
            "sector_map_rows": sectors,
            "db_available": DB_AVAILABLE,
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/screener/multi")
async def get_multi_screener(
    market: str = "India",
    screeners: str = "rs80",
    min_mcap: float = 0,
    refresh: bool = False
):
    """
    Multi-screener intersection.
    screeners = comma-separated list of screener IDs.
    Returns stocks that pass ALL selected screeners.
    """
    scr_ids = [s.strip() for s in screeners.split(',') if s.strip()]
    cache_key = f"multi_scr_{market}_{'_'.join(sorted(scr_ids))}"

    if not refresh and cache_key in _rs_cache:
        age = (datetime.now(timezone.utc) - _rs_cache[cache_key]["ts"]).total_seconds()
        if age < RS_CACHE_TTL:
            return {**_rs_cache[cache_key]["data"], "cached": True}

    import time
    t0 = time.time()
    loop = asyncio.get_event_loop()

    # Always compute RS rankings as base
    try:
        rs_result = await loop.run_in_executor(executor, _compute_rs_rankings, market)
    except Exception as e:
        logger.error(f"RS rankings failed: {e}")
        return {"error": f"RS rankings error: {str(e)}", "stocks": []}

    if "error" in rs_result:
        return rs_result

    stocks = rs_result.get("stocks", [])
    if not stocks:
        return {"error": "No stocks returned from RS rankings", "stocks": []}

    # Apply each screener filter
    def apply_screener(stocks_list, scr_id):
        """Filter stocks by screener criteria"""
        filtered = []
        for s in stocks_list:
            passes = True
            if scr_id == 'rs90'   and s.get('rs_rating',0) < 90: passes = False
            if scr_id == 'rs80'   and s.get('rs_rating',0) < 80: passes = False
            if scr_id == 'rs_up'  and s.get('rs_trend') != '↑': passes = False
            if scr_id == 'near_high' and (s.get('pct_from_high') or -999) < -5: passes = False
            if scr_id == 'vol_dry' and (s.get('vol_ratio') or 999) >= 0.5: passes = False  # vol < 50% of avg
            if scr_id == 'stage2':
                # Use full trend_template if available, else proxy
                if s.get('trend_template') is not None:
                    passes = s.get('trend_template', False)
                else:
                    passes = (s.get('chg_3m',0) > 0 and s.get('rs_rating',0) >= 60)
            if scr_id == 'mtt':
                # Minervini Trend Template: uses full 8-condition check
                if s.get('trend_template') is not None:
                    passes = (s.get('trend_template', False) and
                              s.get('rs_rating', 0) >= 70)
                else:
                    passes = (s.get('rs_rating',0) >= 70 and
                              (s.get('pct_from_high') or -999) >= -15 and
                              s.get('rs_trend') == '↑')
            if scr_id == 'vcp':
                # VCP: high RS, volume drying up, near 52W high
                passes = (s.get('rs_rating',0) >= 75 and
                          (s.get('vol_ratio') or 999) < 1.2 and
                          (s.get('pct_from_high') or -999) >= -12)
            if scr_id == 'pocket':
                # Pocket pivot: RS>70, today vol > any down day in last 10
                passes = (s.get('rs_rating',0) >= 70 and
                          (s.get('vol_ratio') or 0) >= 1.5)
            if scr_id == 'mean_rev':
                # Mean reversion: oversold, below 20D MA
                passes = (s.get('chg_1m',0) < -5 and
                          (s.get('pct_from_high') or 0) < -15)
            if passes:
                filtered.append(s)
        return filtered

    # Intersect: stock must pass ALL screeners
    # Route: built-in screeners use apply_screener()
    #        custom AFL screeners use apply_custom_screener()
    CUSTOM_IDS = set(CUSTOM_SCREENER_MAP.keys())
    builtin_ids = [sid for sid in scr_ids if sid not in CUSTOM_IDS]
    custom_ids  = [sid for sid in scr_ids if sid in CUSTOM_IDS]

    result_stocks = stocks

    # Apply built-in filters first
    for scr_id in builtin_ids:
        result_stocks = apply_screener(result_stocks, scr_id)

    # Apply custom AFL screeners — need raw OHLCV DataFrames
    if custom_ids:
        # Normalize market for DB query
        db_market = "India" if market.upper() == "INDIA" else market

        # Fetch OHLCV data for remaining stocks only (already filtered by built-ins)
        remaining_tickers = [s['ticker'] for s in result_stocks]
        rs_lookup = {s['ticker']: s.get('rs_rating', 0) for s in result_stocks}

        # Load from screener data (full NSE universe already in memory from RS calc)
        # Re-use get_screener_data which queries SQLite directly
        try:
            raw_data = await loop.run_in_executor(executor, get_screener_data, db_market)
        except Exception as e:
            logger.error(f"Could not load OHLCV for custom screeners: {e}")
            raw_data = {}

        logger.info(f"Custom screeners: {len(custom_ids)} filters on {len(remaining_tickers)} stocks, {len(raw_data)} OHLCV frames available")

        for scr_id in custom_ids:
            filtered = []
            for s in result_stocks:
                ticker = s['ticker']
                df = raw_data.get(ticker)
                if df is None:
                    continue
                try:
                    rs_val = rs_lookup.get(ticker, 0)
                    passes, _ = apply_custom_screener(scr_id, df, rs_rating=rs_val)
                    if passes:
                        filtered.append(s)
                except Exception as e:
                    logger.debug(f"Custom screener {scr_id} error on {ticker}: {e}")
                    continue
            result_stocks = filtered
            logger.info(f"Custom screener '{scr_id}': {len(result_stocks)} stocks pass")

    # Enrich with mcap data
    _enrich_stocks_mcap(result_stocks)

    # Filter by min_mcap if specified
    if min_mcap > 0:
        result_stocks = [s for s in result_stocks if s.get("mcap_cr", 0) >= min_mcap]

    # Tag which screeners each stock matched
    for s in result_stocks:
        s['screeners_matched'] = scr_ids

    # Re-rank
    for i, s in enumerate(result_stocks):
        s['rank'] = i + 1

    elapsed = round(time.time() - t0, 2)
    result = {
        "market": market,
        "screeners": scr_ids,
        "stocks": result_stocks,
        "total": len(result_stocks),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "elapsed": elapsed,
        "cached": False,
    }
    # Strip internal DataFrame dict before caching (not JSON-serializable)
    result.pop("_stock_data", None)
    _rs_cache[cache_key] = {"data": result, "ts": datetime.now(timezone.utc)}
    return result


# ── NSE Index Universe Endpoints ─────────────────────────────────────────────

@app.get("/api/nse-indices/status")
def nse_indices_status():
    """Get sync status for all NSE indices grouped by category."""
    try:
        return get_index_registry_status()
    except Exception as e:
        return {"error": str(e), "broad": [], "sectoral": [], "thematic": []}

@app.post("/api/nse-indices/sync")
async def nse_indices_sync(background_tasks: BackgroundTasks):
    """Sync all NSE index constituent CSVs from niftyindices.com."""
    if _nse_indices_state["running"]:
        return {"message": "Sync already running", **_nse_indices_state}

    def _run():
        _nse_indices_state["running"]  = True
        _nse_indices_state["progress"] = 0
        _nse_indices_state["message"]  = "Starting NSE index sync..."
        try:
            result = sync_nse_indices(progress_state=_nse_indices_state)
            _nse_indices_state["result"]  = result
            _nse_indices_state["message"] = result["message"]
        except Exception as e:
            _nse_indices_state["message"] = f"Error: {e}"
            _nse_indices_state["result"]  = {"error": str(e)}
        finally:
            _nse_indices_state["running"] = False

    background_tasks.add_task(_run)
    return {"message": f"Syncing {len(NSE_INDEX_REGISTRY)} NSE indices in background. Poll /api/nse-indices/sync/status"}

@app.get("/api/nse-indices/sync/status")
def nse_indices_sync_status():
    return _nse_indices_state

@app.get("/api/nse-indices/constituents")
def nse_index_constituents(index_name: str = "NIFTY 500"):
    """Get all constituent tickers for a given index."""
    try:
        stocks = get_index_constituents(index_name)
        return {"index_name": index_name, "count": len(stocks), "stocks": stocks}
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/nse-indices/ticker")
def nse_ticker_indices(ticker: str):
    """Get all indices a ticker belongs to."""
    try:
        return {"ticker": ticker, "indices": get_ticker_indices(ticker.upper())}
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/nse-indices/list")
def nse_indices_list():
    """Return the full index registry grouped by category."""
    result = {"broad": [], "sectoral": [], "thematic": []}
    for name, (cat, csv_file) in NSE_INDEX_REGISTRY.items():
        result[cat].append({"index_name": name, "csv_file": csv_file})
    return result

@app.get("/api/nse-indices/universe-stats")
def nse_universe_stats():
    """Summary: unique tickers, OHLCV coverage, last sync."""
    try:
        return get_universe_stats()
    except Exception as e:
        return {"error": str(e), "unique_tickers": 0, "tickers_with_ohlcv": 0}

@app.post("/api/nse-indices/backfill-missing")
async def nse_backfill_missing(background_tasks: BackgroundTasks):
    """Backfill 2-year OHLCV for any constituent tickers missing history."""
    if _nse_indices_state["running"]:
        return {"message": "Sync already running"}

    def _run():
        from nse_indices import get_tickers_missing_ohlcv
        from nse_sync import sync_ticker
        _nse_indices_state["running"] = True
        try:
            missing = get_tickers_missing_ohlcv(years=2)
            total = len(missing)
            _nse_indices_state["total"]   = total
            _nse_indices_state["message"] = f"Backfilling {total} tickers with <2y history..."
            done = 0
            for ticker, count, last_date in missing:
                done += 1
                _nse_indices_state["progress"] = done
                _nse_indices_state["message"]  = f"Backfilling {ticker} ({done}/{total}) — had {count} days"
                sync_ticker(ticker, range_str="2y")
                import time; time.sleep(0.2)
            _nse_indices_state["message"] = f"✅ Backfill complete — {total} tickers updated"
        except Exception as e:
            _nse_indices_state["message"] = f"Error: {e}"
        finally:
            _nse_indices_state["running"] = False

    background_tasks.add_task(_run)
    return {"message": "Backfill started — poll /api/nse-indices/sync/status"}

# ── Startup: load disk cache + pre-warm breadth in background ───────────────
@app.on_event("startup")
async def startup_event():
    """On startup: load disk cache first (instant), then pre-warm if stale."""
    # 1. Load disk cache immediately — dashboard shows data right away
    _load_disk_cache()
    _build_sector_map()  # load ticker→sector map

    # Ensure NSE index tables exist
    try:
        ensure_nse_tables()
        logger.info("✅ NSE index tables ready")
    except Exception as e:
        logger.warning(f"NSE index table init failed: {e}")

    # 1b. Auto-import market cap data if table is empty
    try:
        ensure_mcap_table()
        import sqlite3 as _sql
        _mc_conn = _sql.connect(str(pathlib.Path(__file__).parent / "breadth_data.db"), timeout=10)
        _mc_count = _mc_conn.execute("SELECT COUNT(*) FROM market_cap").fetchone()[0]
        _mc_conn.close()
        if _mc_count == 0:
            csv_path = pathlib.Path(__file__).parent / "data" / "market_cap.csv"
            if csv_path.exists():
                result = import_market_cap_csv(str(csv_path))
                logger.info(f"✅ Auto-imported market cap: {result['matched']} matched, {result['total_in_db']} in DB")
            else:
                logger.info("⏭ No market_cap.csv found — skipping auto-import")
        else:
            logger.info(f"✅ Market cap table already has {_mc_count} entries")
    except Exception as e:
        logger.warning(f"Market cap auto-import failed: {e}")

    # 2. Check if breadth cache is fresh enough
    india_cache = get_cache("breadth_INDIA")
    if india_cache:
        logger.info("✅ Startup: India breadth loaded from disk cache instantly")
    else:
        logger.info("⏳ Startup: No fresh cache — pre-warming India breadth in background")
        # Pre-warm in background so first dashboard load is fast
        asyncio.create_task(_prewarm_breadth())

async def _prewarm_breadth():
    """Pre-compute breadth in background so first load is instant."""
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(executor, lambda: _compute_market("INDIA", custom_tickers=_custom_tickers))
        if "error" not in result:
            set_cache("breadth_INDIA", result)
            logger.info(f"✅ Pre-warm done: India breadth cached ({result.get('universe_size',0)} stocks)")
    except Exception as e:
        logger.warning(f"Pre-warm failed: {e}")


@app.post("/api/cache/clear-breadth")
async def clear_breadth_cache():
    """Nuclear option: clear ALL breadth caches (memory + disk + peep_cache).
    Use when cached data is corrupt/stale (e.g. showing wrong score)."""
    cleared = []
    # 1. Clear in-memory breadth entries
    keys_to_remove = [k for k in _cache if k.startswith("breadth_")]
    for k in keys_to_remove:
        del _cache[k]
        cleared.append(k)
    # 2. Delete disk cache file
    disk_file = pathlib.Path(__file__).parent / "breadth_cache.json"
    if disk_file.exists():
        disk_file.unlink()
        cleared.append("breadth_cache.json")
    # 3. Clear peep_cache table
    try:
        import sqlite3
        db_path = str(pathlib.Path(__file__).parent / "breadth_data.db")
        conn = sqlite3.connect(db_path, timeout=10)
        conn.execute("DELETE FROM peep_cache")
        conn.commit()
        conn.close()
        cleared.append("peep_cache_table")
    except Exception as e:
        logger.warning(f"Could not clear peep_cache: {e}")
    # 4. Trigger fresh recompute
    asyncio.create_task(_prewarm_breadth())
    logger.info(f"🗑️ Breadth cache cleared: {cleared}. Recomputing...")
    return {"ok": True, "cleared": cleared, "message": "All breadth caches cleared. Recomputing fresh data..."}


@app.post("/api/screener/clear-cache")
async def clear_screener_cache(payload: dict = {}):
    """
    Clear screener cache for a specific market+screener combo.
    Called by Force Refresh button — forces fresh recompute on next run.
    """
    market   = payload.get("market", "India")
    screeners = payload.get("screeners", [])

    cleared = []

    # Clear specific screener combo cache
    if screeners:
        key = f"multi_scr_{market}_{'_'.join(sorted(screeners))}"
        if key in _rs_cache:
            del _rs_cache[key]
            cleared.append(key)

    # Also clear the base RS rankings cache for this market
    rs_key = f"rs_rankings_{market}"
    if rs_key in _rs_cache:
        del _rs_cache[rs_key]
        cleared.append(rs_key)

    # Clear ALL screener caches for this market (catches all combos)
    keys_to_del = [k for k in list(_rs_cache.keys()) if market.upper() in k.upper() or market.lower() in k.lower()]
    for k in keys_to_del:
        if k not in cleared:
            del _rs_cache[k]
            cleared.append(k)

    logger.info(f"Force refresh: cleared {len(cleared)} cache entries: {cleared}")
    return {
        "ok": True,
        "cleared": len(cleared),
        "keys": cleared,
        "message": f"Cache cleared — next screener run will fetch fresh data"
    }


# ── Stockbee Market Breadth ──────────────────────────────────────────────────

@app.get("/api/stockbee/{market}")
async def get_stockbee(market: str, refresh: bool = False):
    market = market.upper()
    if market not in ("INDIA", "US"):
        return {"error": "Market must be INDIA or US"}
    key = f"stockbee_{market}"
    if not refresh:
        cached = get_cache(key)
        if cached:
            return {**cached, "cached": True}
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(executor, _compute_stockbee, market)
    if "error" not in result:
        set_cache(key, result)
    return {**result, "cached": False}


@app.get("/api/chart/{ticker}")
async def chart_data(ticker: str, tf: str = "daily", days: int = None):
    from fastapi.responses import JSONResponse
    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(executor, get_chart_data, ticker, tf, days)
    if "error" in data:
        return JSONResponse(status_code=404, content=data)
    return data


# ── Stock Metrics ─────────────────────────────────────────────────────────────

@app.get("/api/stock-metrics/{ticker}")
async def stock_metrics(ticker: str):
    """Compute 9 key metrics for a ticker (fast, from DB data). EPS excluded."""
    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(executor, compute_stock_metrics, ticker)
    if "error" in data:
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=404, content=data)
    return data

@app.get("/api/stock-metrics/{ticker}/eps")
async def stock_metrics_eps(ticker: str):
    """Fetch EPS from Yahoo Finance (async, don't block chart loading)."""
    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(executor, compute_eps_async, ticker)
    return data


# ── Smart Metrics (Techno-Fundamental Analysis) ──────────────────────────────

@app.get("/api/smart-metrics/{ticker}")
async def smart_metrics(ticker: str):
    """Full techno-fundamental analysis: screener.in scrape + OM score + technicals + Smart Score."""
    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(executor, get_smart_metrics, ticker)
    return data


# ── Ticker Search (Autocomplete) ──────────────────────────────────────────────

@app.get("/api/tickers/search")
def ticker_search(q: str = "", limit: int = 10):
    """Search tickers by ticker prefix or company name substring. Sorted by mcap."""
    q = q.strip().upper()
    if len(q) < 2:
        return []
    import sqlite3
    conn = sqlite3.connect(str(pathlib.Path(__file__).parent / "breadth_data.db"), timeout=10)
    try:
        rows = conn.execute("""
            SELECT ticker, company_name, mcap_cr, mcap_tier
            FROM market_cap
            WHERE UPPER(ticker) LIKE ? OR UPPER(company_name) LIKE ?
            ORDER BY mcap_cr DESC
            LIMIT ?
        """, (f"{q}%", f"%{q}%", limit)).fetchall()
    except Exception:
        rows = []
    conn.close()

    # Also search ohlcv tickers not in market_cap (fallback)
    if len(rows) < limit:
        existing = {r[0] for r in rows}
        try:
            conn2 = sqlite3.connect(str(pathlib.Path(__file__).parent / "breadth_data.db"), timeout=10)
            ohlcv_rows = conn2.execute(
                "SELECT DISTINCT ticker FROM ohlcv WHERE UPPER(ticker) LIKE ? LIMIT ?",
                (f"{q}%", limit)
            ).fetchall()
            conn2.close()
            for r in ohlcv_rows:
                if r[0] not in existing and len(rows) < limit:
                    rows.append((r[0], "", 0, ""))
        except Exception:
            pass

    # Also add sector from sector_map
    try:
        smap = load_sector_map()
    except Exception:
        smap = {}
    return [
        {
            "ticker": r[0],
            "company_name": r[1] or "",
            "mcap_tier": r[3] or "",
            "sector": (smap.get(r[0], {}) or {}).get("sector", "") if isinstance(smap.get(r[0]), dict) else "",
        }
        for r in rows
    ]


# ── Fetch Live (Force Refresh) ───────────────────────────────────────────────

@app.post("/api/fetch-live")
async def fetch_live(background_tasks: BackgroundTasks):
    """Force live data refresh — clears all caches and syncs stale tickers."""
    # 1. Clear breadth cache
    _cache.clear()
    try:
        disk_cache = pathlib.Path(__file__).parent / "breadth_cache.json"
        if disk_cache.exists():
            disk_cache.unlink()
    except Exception:
        pass

    # 2. Clear screener/leaders/RS caches
    _rs_cache.clear()
    _leaders_cache.clear()

    # 3. Sync ALL tickers that don't have today's data
    try:
        from nse_sync import _get_stale_tickers, sync_nifty500
        stale = _get_stale_tickers(days_threshold=0)  # anything not updated today
        logger.info(f"Fetch Live: {len(stale)} tickers need today's data (full universe)")
        if stale:
            result = sync_nifty500(range_str="5d", max_workers=10)
        else:
            result = {"message": "All tickers already have today's data", "updated": 0}
    except Exception as e:
        result = {"error": str(e)}

    return {"message": "Live data refreshed", "sync": result}


# ── Sector Heatmap ─────────────────────────────────────────────────────────────

@app.get("/api/sectors/heatmap")
async def sectors_heatmap(market: str = "India", period: str = "1m"):
    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(executor, compute_sector_heatmap, market, period)
    return data

# ── Watchlist CRUD ─────────────────────────────────────────────────────────────

@app.get("/api/watchlist")
def api_list_watchlists():
    return {"watchlists": list_watchlists()}

@app.post("/api/watchlist")
async def api_create_watchlist(req: dict = None):
    from fastapi import Request
    if not req or "name" not in req:
        return {"error": "name required"}
    return create_watchlist(req["name"])

@app.delete("/api/watchlist/{wid}")
def api_delete_watchlist(wid: int):
    return delete_watchlist(wid)

@app.post("/api/watchlist/{wid}/add")
async def api_add_ticker(wid: int, req: dict = None):
    if not req or "ticker" not in req:
        return {"error": "ticker required"}
    return wl_add_ticker(wid, req["ticker"], req.get("notes", ""))

@app.delete("/api/watchlist/{wid}/remove/{ticker}")
def api_remove_ticker(wid: int, ticker: str):
    return wl_remove_ticker(wid, ticker)

@app.get("/api/watchlist/{wid}/data")
async def api_watchlist_data(wid: int):
    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(executor, get_watchlist_data, wid)
    return data

# ── Alerts CRUD ────────────────────────────────────────────────────────────────

@app.post("/api/alerts")
async def api_create_alert(req: dict = None):
    if not req or "ticker" not in req or "condition_type" not in req:
        return {"error": "ticker and condition_type required"}
    return create_alert(req["ticker"], req["condition_type"], req.get("condition_value"))

@app.get("/api/alerts")
def api_list_alerts():
    return {"alerts": list_alerts()}

@app.delete("/api/alerts/{aid}")
def api_delete_alert(aid: int):
    return delete_alert(aid)

@app.get("/api/alerts/check")
async def api_check_alerts():
    loop = asyncio.get_event_loop()
    triggered = await loop.run_in_executor(executor, check_alerts)
    return {"triggered": triggered, "count": len(triggered)}

# ── Market Summary ─────────────────────────────────────────────────────────────

# ── Market Cap Endpoints ───────────────────────────────────────────────────────

@app.get("/api/market-cap/import")
async def mcap_import():
    """Trigger market cap import from CSV."""
    csv_path = pathlib.Path(__file__).parent / "data" / "market_cap.csv"
    if not csv_path.exists():
        return {"error": "market_cap.csv not found"}
    result = import_market_cap_csv(str(csv_path))
    return result

@app.get("/api/market-cap/stats")
async def mcap_stats():
    """Get market cap tier breakdown stats."""
    import sqlite3 as _sql
    conn = _sql.connect(str(pathlib.Path(__file__).parent / "breadth_data.db"), timeout=10)
    total = conn.execute("SELECT COUNT(*) FROM market_cap").fetchone()[0]
    tiers = conn.execute(
        "SELECT mcap_tier, COUNT(*), ROUND(AVG(mcap_cr),0) FROM market_cap GROUP BY mcap_tier ORDER BY AVG(mcap_cr) DESC"
    ).fetchall()
    conn.close()
    return {
        "total": total,
        "tiers": {t: {"count": c, "avg_mcap_cr": a} for t, c, a in tiers},
    }

@app.get("/api/market-cap/{ticker}")
async def mcap_ticker(ticker: str):
    """Get market cap for a single ticker."""
    data = get_mcap_for_ticker(ticker.upper())
    if not data:
        return {"error": f"No market cap data for {ticker}"}
    data["mcap_formatted"] = format_mcap(data["mcap_cr"])
    return data


@app.get("/api/summary")
async def api_summary(market: str = "India"):
    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(executor, generate_market_summary, market)
    return data

@app.get("/api/summary/html")
async def api_summary_html(market: str = "India"):
    from fastapi.responses import HTMLResponse
    loop = asyncio.get_event_loop()
    html = await loop.run_in_executor(executor, generate_summary_html, market)
    return HTMLResponse(content=html)


@app.get("/api/peep-into-past")
async def api_peep_into_past(date: str = "2020-03-23", market: str = "India"):
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(executor, compute_historical_breadth, date, market)
        return result
    except Exception as e:
        logger.error(f"Peep Into Past error for {date}: {e}", exc_info=True)
        return {"error": str(e), "target_date": date}


if __name__=="__main__":
    import uvicorn
    print("\n🚀  Market Breadth Engine v2.0")
    print("    API    →  http://localhost:8001")
    print("    Debug  →  http://localhost:8001/api/debug")
    print("    Sync   →  POST http://localhost:8001/api/sync/start")
    print("    Status →  http://localhost:8001/api/sync/status\n")
    uvicorn.run("main:app",host="0.0.0.0",port=8001,reload=False)
