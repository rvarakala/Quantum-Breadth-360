"""
NSE Index Universe Manager
==========================
Downloads official constituent CSVs from niftyindices.com
Permanently stores in SQLite — refreshed semi-annually (NSE rebalances Jan/Jul)

Tables:
  nse_index_constituents  — ticker, index_name, category, company, industry, isin
  nse_index_registry      — index_name, category, csv_file, count, last_synced

After constituent sync → triggers OHLCV backfill for any NEW tickers (2y history)
EOD sync / Live fetch automatically covers ALL constituent tickers going forward.
"""

import sqlite3
import logging
import time
import pathlib
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

DB_PATH  = pathlib.Path(__file__).parent / "breadth_data.db"
BASE_URL = "https://www.niftyindices.com/IndexConstituent/"

# ══════════════════════════════════════════════════════════════════════════════
# INDEX REGISTRY — 44 indices across 3 categories
# CSV filenames verified against niftyindices.com
# ══════════════════════════════════════════════════════════════════════════════
NSE_INDEX_REGISTRY = {

    # ── BROAD MARKET (15) ─────────────────────────────────────────────────────
    "NIFTY 50":               ("broad",    "ind_nifty50list.csv"),
    "NIFTY Next 50":          ("broad",    "ind_niftynext50list.csv"),
    "NIFTY 100":              ("broad",    "ind_nifty100list.csv"),
    "NIFTY 200":              ("broad",    "ind_nifty200list.csv"),
    "NIFTY 500":              ("broad",    "ind_nifty500list.csv"),
    "NIFTY Midcap 50":        ("broad",    "ind_niftymidcap50list.csv"),
    "NIFTY Midcap 100":       ("broad",    "ind_niftymidcap100list.csv"),
    "NIFTY Midcap 150":       ("broad",    "ind_niftymidcap150list.csv"),
    "NIFTY Smallcap 50":      ("broad",    "ind_niftysmallcap50list.csv"),
    "NIFTY Smallcap 100":     ("broad",    "ind_niftysmallcap100list.csv"),
    "NIFTY Smallcap 250":     ("broad",    "ind_niftysmallcap250list.csv"),
    "NIFTY MidSmallcap 400":  ("broad",    "ind_niftymidsmallcap400list.csv"),
    "NIFTY Microcap 250":     ("broad",    "ind_niftymicrocap250_list.csv"),
    "NIFTY LargeMidcap 250":  ("broad",    "ind_nifty_largemidcap_250list.csv"),
    "NIFTY Total Market":     ("broad",    "ind_niftytotalmarket_list.csv"),

    # ── SECTORAL (19) ─────────────────────────────────────────────────────────
    "NIFTY Auto":             ("sectoral", "ind_niftyautolist.csv"),
    "NIFTY Bank":             ("sectoral", "ind_niftybanklist.csv"),
    "NIFTY Financial Services":("sectoral","ind_niftyfinancelist.csv"),
    "NIFTY FMCG":             ("sectoral", "ind_niftyfmcglist.csv"),
    "NIFTY Healthcare":       ("sectoral", "ind_niftyhealthcarelist.csv"),
    "NIFTY IT":               ("sectoral", "ind_nifty_it_list.csv"),
    "NIFTY Media":            ("sectoral", "ind_niftymedialist.csv"),
    "NIFTY Metal":            ("sectoral", "ind_niftymetalist.csv"),
    "NIFTY Oil & Gas":        ("sectoral", "ind_niftyoilgaslist.csv"),
    "NIFTY Pharma":           ("sectoral", "ind_niftypharma_list.csv"),
    "NIFTY PSU Bank":         ("sectoral", "ind_niftypsubanklist.csv"),
    "NIFTY Private Bank":     ("sectoral", "ind_niftyprivatebankList.csv"),
    "NIFTY Realty":           ("sectoral", "ind_niftyrealty_list.csv"),
    "NIFTY Consumer Durables":("sectoral", "ind_niftyconsumerdurableslist.csv"),
    "NIFTY Infrastructure":   ("sectoral", "ind_niftyinfrastructurelist.csv"),
    "NIFTY Energy":           ("sectoral", "ind_niftyenergylist.csv"),
    "NIFTY Construction":     ("sectoral", "ind_niftyconstructionlist.csv"),
    "NIFTY Commodities":      ("sectoral", "ind_niftycommoditieslist.csv"),
    "NIFTY India Manufacturing":("sectoral","ind_niftyindiamanufacturing_list.csv"),

    # ── THEMATIC (10) ─────────────────────────────────────────────────────────
    "NIFTY Alpha 50":         ("thematic", "ind_niftyalpha50list.csv"),
    "NIFTY High Beta 50":     ("thematic", "ind_niftyhighbeta50list.csv"),
    "NIFTY Low Volatility 50":("thematic", "ind_niftylowvol50list.csv"),
    "NIFTY Quality 30":       ("thematic", "ind_niftyquality30_list.csv"),
    "NIFTY500 Momentum 50":   ("thematic", "ind_nifty500momentum50_list.csv"),
    "NIFTY200 Momentum 30":   ("thematic", "ind_nifty200momentum30_list.csv"),
    "NIFTY CPSE":             ("thematic", "ind_niftycpse_list.csv"),
    "NIFTY Dividend Opp 50":  ("thematic", "ind_niftydividendopportunities50list.csv"),
    "NIFTY100 ESG":           ("thematic", "ind_nifty100esgsectorleaderslist.csv"),
    "NIFTY India Defence":    ("thematic", "ind_niftyindiadefence_list.csv"),
}


# ══════════════════════════════════════════════════════════════════════════════
# DB SCHEMA
# ══════════════════════════════════════════════════════════════════════════════
def _ensure_tables():
    conn = sqlite3.connect(str(DB_PATH), timeout=15)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS nse_index_constituents (
            ticker      TEXT NOT NULL,
            index_name  TEXT NOT NULL,
            category    TEXT NOT NULL,
            company     TEXT DEFAULT '',
            industry    TEXT DEFAULT '',
            series      TEXT DEFAULT 'EQ',
            isin        TEXT DEFAULT '',
            added_date  TEXT DEFAULT (date('now')),
            PRIMARY KEY (ticker, index_name)
        );
        CREATE TABLE IF NOT EXISTS nse_index_registry (
            index_name        TEXT PRIMARY KEY,
            category          TEXT NOT NULL,
            csv_file          TEXT NOT NULL,
            constituent_count INTEGER DEFAULT 0,
            last_synced       TEXT,
            status            TEXT DEFAULT 'pending'
        );
        CREATE INDEX IF NOT EXISTS idx_nic_ticker   ON nse_index_constituents(ticker);
        CREATE INDEX IF NOT EXISTS idx_nic_index    ON nse_index_constituents(index_name);
        CREATE INDEX IF NOT EXISTS idx_nic_category ON nse_index_constituents(category);
        CREATE INDEX IF NOT EXISTS idx_nic_industry ON nse_index_constituents(industry);
    """)
    conn.commit()
    conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# CSV DOWNLOADER
# ══════════════════════════════════════════════════════════════════════════════
def _download_index_csv(index_name: str, csv_file: str) -> list:
    """
    Download constituent CSV from niftyindices.com using session cookies.
    Returns list of dicts: {ticker, company, industry, series, isin}
    """
    import urllib.request
    import http.cookiejar
    import csv
    import io
    import gzip

    url = BASE_URL + csv_file
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept":          "text/csv,application/csv,text/plain,*/*;q=0.9",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Referer":         "https://www.niftyindices.com/",
        "Origin":          "https://www.niftyindices.com",
    }

    # Cookie jar — niftyindices requires session cookies
    cookie_jar = http.cookiejar.CookieJar()
    opener = urllib.request.build_opener(
        urllib.request.HTTPCookieProcessor(cookie_jar)
    )

    raw = None

    # Step 1: warm up session with homepage
    try:
        home_req = urllib.request.Request("https://www.niftyindices.com/", headers=headers)
        with opener.open(home_req, timeout=10) as _:
            pass
        time.sleep(0.3)
    except Exception:
        pass  # warmup failed — try direct anyway

    # Step 2: download CSV (try main URL + fallback)
    for attempt_url in [url, url.replace("www.", "")]:
        try:
            req = urllib.request.Request(attempt_url, headers=headers)
            with opener.open(req, timeout=30) as resp:
                content = resp.read()
                try:
                    content = gzip.decompress(content)
                except Exception:
                    pass
                raw = content.decode("utf-8-sig")
            if raw:
                break
        except Exception as e:
            logger.debug(f"URL attempt failed {attempt_url}: {e}")
            continue

    if not raw:
        logger.warning(f"All download attempts failed for {index_name} ({csv_file})")
        return []

    # Validate: must be CSV not HTML
    stripped = raw.strip()
    if not stripped or len(stripped) < 30:
        logger.warning(f"Empty response for {index_name}")
        return []
    if stripped.lower().startswith(("<!doctype", "<html", "<!-")):
        logger.warning(f"Got HTML error page for {index_name} — server blocked request")
        return []

    # Parse CSV with safe field extraction
    rows = []
    try:
        reader = csv.DictReader(io.StringIO(raw))
        for row in reader:
            # Safe normalise — handle None keys/values
            row = {
                str(k).strip(): str(v).strip() if v is not None else ""
                for k, v in row.items() if k is not None
            }

            def _get(*keys, default=""):
                for k in keys:
                    v = row.get(k, "")
                    if v and str(v).strip() not in ("", "-", "N/A", "NA"):
                        return str(v).strip()
                return default

            ticker = _get("Symbol", "symbol", "SYMBOL", "Ticker").upper()
            if not ticker:
                continue

            rows.append({
                "ticker":   ticker,
                "company":  _get("Company Name", "company name", "CompanyName", "Company"),
                "industry": _get("Industry", "industry", "Sector", "sector"),
                "series":   _get("Series", "series") or "EQ",
                "isin":     _get("ISIN Code", "ISIN", "isin code"),
            })
    except Exception as e:
        logger.warning(f"CSV parse error for {index_name}: {e}")
        return []

    return rows


# ══════════════════════════════════════════════════════════════════════════════
# GET ALL UNIQUE TICKERS ACROSS ALL SYNCED INDICES
# ══════════════════════════════════════════════════════════════════════════════
def get_all_constituent_tickers() -> list:
    """Return all unique tickers stored across all indices."""
    _ensure_tables()
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    rows = conn.execute(
        "SELECT DISTINCT ticker FROM nse_index_constituents ORDER BY ticker"
    ).fetchall()
    conn.close()
    return [r[0] for r in rows]


def get_tickers_missing_ohlcv(years: int = 2) -> list:
    """
    Return tickers that are in index constituents but have insufficient OHLCV history.
    'Insufficient' = fewer than (years * 200) trading days stored.
    """
    _ensure_tables()
    conn = sqlite3.connect(str(DB_PATH), timeout=15)

    # All constituent tickers
    all_tickers = [r[0] for r in conn.execute(
        "SELECT DISTINCT ticker FROM nse_index_constituents"
    ).fetchall()]

    # Check OHLCV coverage
    min_days = years * 200  # ~200 trading days per year
    needs_sync = []
    for ticker in all_tickers:
        row = conn.execute(
            "SELECT COUNT(*), MAX(date) FROM ohlcv WHERE ticker=? AND market='India'",
            (ticker,)
        ).fetchone()
        count     = row[0] or 0
        last_date = row[1] or ""
        if count < min_days:
            needs_sync.append((ticker, count, last_date))

    conn.close()
    logger.info(
        f"Tickers needing {years}y OHLCV sync: "
        f"{len(needs_sync)}/{len(all_tickers)}"
    )
    return needs_sync


def get_stale_constituent_tickers(days_threshold: int = 3) -> list:
    """
    Return constituent tickers whose OHLCV data is older than threshold.
    Used by EOD sync and Live fetch.
    """
    _ensure_tables()
    from datetime import timedelta
    conn   = sqlite3.connect(str(DB_PATH), timeout=15)
    cutoff = (datetime.now() - timedelta(days=days_threshold)).strftime("%Y-%m-%d")

    all_tickers = [r[0] for r in conn.execute(
        "SELECT DISTINCT ticker FROM nse_index_constituents"
    ).fetchall()]

    stale = []
    for ticker in all_tickers:
        row = conn.execute(
            "SELECT MAX(date) FROM ohlcv WHERE ticker=? AND market='India'",
            (ticker,)
        ).fetchone()
        last_date = row[0] if row and row[0] else None
        if not last_date or last_date < cutoff:
            stale.append((ticker, last_date))

    conn.close()
    return stale


# ══════════════════════════════════════════════════════════════════════════════
# MAIN SYNC — Download all indices + trigger OHLCV backfill for new tickers
# ══════════════════════════════════════════════════════════════════════════════
def sync_nse_indices(
    progress_state: Optional[dict] = None,
    backfill_new: bool = True,
) -> dict:
    """
    Download all NSE index constituent CSVs → store permanently in SQLite.
    After storing constituents, triggers 2-year OHLCV backfill for any NEW tickers.

    Called from Data Import tab — "Sync All Indices" button.
    """
    _ensure_tables()

    # Snapshot tickers already in DB before sync
    existing_tickers = set(get_all_constituent_tickers())

    conn = sqlite3.connect(str(DB_PATH), timeout=15)
    total      = len(NSE_INDEX_REGISTRY)
    done       = 0
    succeeded  = 0
    failed     = []
    total_rows = 0
    now        = datetime.now(timezone.utc).isoformat()

    for index_name, (category, csv_file) in NSE_INDEX_REGISTRY.items():
        done += 1
        if progress_state:
            progress_state["progress"] = done
            progress_state["total"]    = total
            progress_state["message"]  = (
                f"[{done}/{total}] Downloading {index_name}..."
            )

        logger.info(f"Syncing {index_name} ({csv_file})...")
        constituents = _download_index_csv(index_name, csv_file)

        if not constituents:
            failed.append(index_name)
            conn.execute("""
                INSERT INTO nse_index_registry
                    (index_name, category, csv_file, status, last_synced)
                VALUES (?, ?, ?, 'failed', ?)
                ON CONFLICT(index_name) DO UPDATE SET
                    status='failed', last_synced=excluded.last_synced
            """, (index_name, category, csv_file, now))
            conn.commit()
            time.sleep(0.5)
            continue

        # Delete old constituents for this index → insert fresh
        conn.execute(
            "DELETE FROM nse_index_constituents WHERE index_name = ?",
            (index_name,)
        )
        for c in constituents:
            conn.execute("""
                INSERT OR REPLACE INTO nse_index_constituents
                    (ticker, index_name, category, company, industry, series, isin)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (c["ticker"], index_name, category,
                  c["company"], c["industry"], c["series"], c["isin"]))

        # Update registry
        conn.execute("""
            INSERT INTO nse_index_registry
                (index_name, category, csv_file, constituent_count, last_synced, status)
            VALUES (?, ?, ?, ?, ?, 'ok')
            ON CONFLICT(index_name) DO UPDATE SET
                constituent_count = excluded.constituent_count,
                last_synced       = excluded.last_synced,
                status            = 'ok'
        """, (index_name, category, csv_file, len(constituents), now))

        conn.commit()
        succeeded  += 1
        total_rows += len(constituents)
        logger.info(f"  ✅ {index_name}: {len(constituents)} constituents")
        time.sleep(0.8)  # polite delay

    conn.close()

    # ── Find NEW tickers not previously in DB → trigger 2-year backfill ────────
    new_tickers = set(get_all_constituent_tickers()) - existing_tickers
    backfill_count = 0
    backfill_failed = 0

    if new_tickers and backfill_new:
        new_list = sorted(new_tickers)
        logger.info(
            f"Found {len(new_list)} NEW tickers — "
            f"triggering 2-year OHLCV backfill..."
        )
        if progress_state:
            progress_state["message"] = (
                f"Downloading 2-year history for {len(new_list)} new tickers..."
            )

        # Import here to avoid circular imports
        from nse_sync import sync_ticker
        total_new = len(new_list)
        for i, ticker in enumerate(new_list, 1):
            if progress_state:
                progress_state["message"] = (
                    f"Backfilling {ticker} ({i}/{total_new})..."
                )
                progress_state["progress"] = total + i
                progress_state["total"]    = total + total_new
            _, n_rows, err = sync_ticker(ticker, range_str="2y")
            if err:
                backfill_failed += 1
                logger.debug(f"Backfill failed {ticker}: {err}")
            else:
                backfill_count += n_rows
            time.sleep(0.2)

    msg = (
        f"✅ Synced {succeeded}/{total} indices — "
        f"{total_rows:,} constituent records"
    )
    if new_tickers and backfill_new:
        msg += (
            f" | {len(new_tickers)} new tickers backfilled "
            f"({backfill_count:,} OHLCV rows)"
        )
    if failed:
        msg += f" | ⚠ Failed: {len(failed)} indices"
        if len(failed) <= 5:
            msg += f" ({', '.join(failed)})"

    if progress_state:
        progress_state["message"] = msg
        progress_state["running"] = False

    logger.info(msg)
    return {
        "message":         msg,
        "succeeded":       succeeded,
        "failed":          failed,
        "total_rows":      total_rows,
        "new_tickers":     len(new_tickers),
        "backfill_rows":   backfill_count,
        "backfill_failed": backfill_failed,
    }


# ══════════════════════════════════════════════════════════════════════════════
# QUERY HELPERS
# ══════════════════════════════════════════════════════════════════════════════
def get_index_constituents(index_name: str) -> list:
    """Get all tickers for a given index with company/industry info."""
    _ensure_tables()
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    rows = conn.execute("""
        SELECT ticker, company, industry, isin
        FROM nse_index_constituents
        WHERE index_name = ?
        ORDER BY ticker
    """, (index_name,)).fetchall()
    conn.close()
    return [{"ticker": r[0], "company": r[1],
             "industry": r[2], "isin": r[3]} for r in rows]


def get_ticker_indices(ticker: str) -> list:
    """Get all indices a ticker belongs to."""
    _ensure_tables()
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    rows = conn.execute("""
        SELECT index_name, category, industry
        FROM nse_index_constituents
        WHERE ticker = ?
        ORDER BY category, index_name
    """, (ticker,)).fetchall()
    conn.close()
    return [{"index_name": r[0], "category": r[1], "industry": r[2]}
            for r in rows]


def get_index_registry_status() -> dict:
    """Return sync status for all indices grouped by category."""
    _ensure_tables()
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    rows = conn.execute("""
        SELECT index_name, category, constituent_count, last_synced, status
        FROM nse_index_registry
        ORDER BY category, index_name
    """).fetchall()
    conn.close()

    result = {
        "broad": [], "sectoral": [], "thematic": [],
        "total_synced": 0, "total_constituents": 0,
    }
    synced_names = set()
    for r in rows:
        cat = r[1] if r[1] in ("broad", "sectoral", "thematic") else "broad"
        entry = {
            "index_name":        r[0],
            "constituent_count": r[2] or 0,
            "last_synced":       r[3],
            "status":            r[4] or "pending",
        }
        result[cat].append(entry)
        synced_names.add(r[0])
        if r[4] == "ok":
            result["total_synced"]       += 1
            result["total_constituents"] += (r[2] or 0)

    # Add pending indices not yet in registry
    for name, (cat, _) in NSE_INDEX_REGISTRY.items():
        if name not in synced_names:
            result[cat].append({
                "index_name":        name,
                "constituent_count": 0,
                "last_synced":       None,
                "status":            "pending",
            })
    return result


def get_industry_for_ticker(ticker: str) -> str:
    """Official NSE industry for a ticker (NIFTY 500 preferred)."""
    _ensure_tables()
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    row = conn.execute("""
        SELECT industry FROM nse_index_constituents
        WHERE ticker = ? AND index_name = 'NIFTY 500'
          AND industry != '' LIMIT 1
    """, (ticker,)).fetchone()
    if not row:
        row = conn.execute("""
            SELECT industry FROM nse_index_constituents
            WHERE ticker = ? AND industry != '' LIMIT 1
        """, (ticker,)).fetchone()
    conn.close()
    return row[0] if row else ""


def get_universe_stats() -> dict:
    """Summary stats: unique tickers, coverage, last sync."""
    _ensure_tables()
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    unique = conn.execute(
        "SELECT COUNT(DISTINCT ticker) FROM nse_index_constituents"
    ).fetchone()[0]
    in_ohlcv = conn.execute("""
        SELECT COUNT(DISTINCT n.ticker) FROM nse_index_constituents n
        INNER JOIN ohlcv o ON o.ticker = n.ticker AND o.market = 'India'
    """).fetchone()[0]
    last_sync = conn.execute(
        "SELECT MAX(last_synced) FROM nse_index_registry WHERE status='ok'"
    ).fetchone()[0]
    conn.close()
    return {
        "unique_tickers":  unique,
        "tickers_with_ohlcv": in_ohlcv,
        "coverage_pct":   round(in_ohlcv / unique * 100, 1) if unique > 0 else 0,
        "last_synced":    last_sync,
    }
