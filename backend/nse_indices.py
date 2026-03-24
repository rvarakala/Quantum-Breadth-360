"""
NSE Index Universe Manager
Downloads official constituent CSVs from niftyindices.com
Stores ticker → index membership + industry in SQLite

Tables created:
  nse_index_constituents  — ticker, index_name, category, company, industry, isin
  nse_index_registry      — index_name, category, csv_file, last_synced
"""

import sqlite3
import logging
import time
import pathlib
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)

DB_PATH = pathlib.Path(__file__).parent / "breadth_data.db"
BASE_URL = "https://www.niftyindices.com/IndexConstituent/"

# ── Full index registry ───────────────────────────────────────────────────────
NSE_INDEX_REGISTRY = {

    # ── BROAD MARKET ──────────────────────────────────────────────────────────
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

    # ── SECTORAL ──────────────────────────────────────────────────────────────
    "NIFTY Auto":             ("sectoral", "ind_niftyautolist.csv"),
    "NIFTY Bank":             ("sectoral", "ind_niftybanklist.csv"),
    "NIFTY Financial Services": ("sectoral","ind_niftyfinancelist.csv"),
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
    "NIFTY India Manufacturing": ("sectoral","ind_niftyindiamanufacturing_list.csv"),
    "NIFTY India Digital":    ("sectoral", "ind_niftyindiadigital_list.csv"),

    # ── THEMATIC ──────────────────────────────────────────────────────────────
    "NIFTY Alpha 50":         ("thematic", "ind_niftyalpha50list.csv"),
    "NIFTY High Beta 50":     ("thematic", "ind_niftyhighbeta50list.csv"),
    "NIFTY Low Volatility 50":("thematic", "ind_niftylowvol50list.csv"),
    "NIFTY Quality 30":       ("thematic", "ind_niftyquality30_list.csv"),
    "NIFTY500 Momentum 50":   ("thematic", "ind_nifty500momentum50_list.csv"),
    "NIFTY200 Momentum 30":   ("thematic", "ind_nifty200momentum30_list.csv"),
    "NIFTY CPSE":             ("thematic", "ind_niftycpse_list.csv"),
    "NIFTY Dividend Opp 50":  ("thematic", "ind_niftydividendopportunities50list.csv"),
    "NIFTY100 ESG":           ("thematic", "ind_nifty100esgsectorleaderslist.csv"),
    "NIFTY India Defence":    ("thematic", "ind_niftyi_ndiadefence_list.csv"),
    "NIFTY MNC":              ("thematic", "ind_niftymnc_list.csv"),
    "NIFTY India Consumption":("thematic", "ind_niftyindiaconsumption_list.csv"),
    "NIFTY PSE":              ("thematic", "ind_niftypse_list.csv"),
    "NIFTY500 Value 50":      ("thematic", "ind_nifty500value50_list.csv"),
}

# ── DB Setup ──────────────────────────────────────────────────────────────────
def _ensure_tables():
    conn = sqlite3.connect(str(DB_PATH), timeout=15)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS nse_index_constituents (
            ticker      TEXT NOT NULL,
            index_name  TEXT NOT NULL,
            category    TEXT NOT NULL,
            company     TEXT,
            industry    TEXT,
            series      TEXT,
            isin        TEXT,
            PRIMARY KEY (ticker, index_name)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS nse_index_registry (
            index_name  TEXT PRIMARY KEY,
            category    TEXT NOT NULL,
            csv_file    TEXT NOT NULL,
            constituent_count INTEGER DEFAULT 0,
            last_synced TEXT,
            status      TEXT DEFAULT 'pending'
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_nic_ticker ON nse_index_constituents(ticker)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_nic_index  ON nse_index_constituents(index_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_nic_cat    ON nse_index_constituents(category)")
    conn.commit()
    conn.close()

# ── Download one CSV ──────────────────────────────────────────────────────────
def _download_index_csv(index_name: str, csv_file: str) -> list:
    """
    Download constituent CSV from niftyindices.com
    Returns list of dicts: {ticker, company, industry, series, isin}
    """
    import urllib.request
    import csv
    import io

    url = BASE_URL + csv_file
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": "https://www.niftyindices.com/",
    }

    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8-sig")  # strip BOM if present
    except Exception as e:
        logger.warning(f"Download failed for {index_name} ({csv_file}): {e}")
        return []

    rows = []
    reader = csv.DictReader(io.StringIO(raw))
    for row in reader:
        # Normalise column names (NSE uses inconsistent caps)
        row = {k.strip(): v.strip() for k, v in row.items()}

        # Symbol column — try multiple names
        ticker = (row.get("Symbol") or row.get("symbol") or
                  row.get("SYMBOL") or "").strip().upper()
        if not ticker:
            continue

        company  = (row.get("Company Name") or row.get("company name") or
                    row.get("Company") or "").strip()
        industry = (row.get("Industry") or row.get("industry") or
                    row.get("Sector") or "").strip()
        series   = (row.get("Series") or row.get("series") or "EQ").strip()
        isin     = (row.get("ISIN Code") or row.get("ISIN") or
                    row.get("isin code") or "").strip()

        rows.append({
            "ticker":   ticker,
            "company":  company,
            "industry": industry,
            "series":   series,
            "isin":     isin,
        })

    return rows


# ── Sync all indices ──────────────────────────────────────────────────────────
def sync_nse_indices(progress_state: Optional[dict] = None) -> dict:
    """
    Download all NSE index constituent CSVs and store in SQLite.
    Called from the Data Import tab — "Sync NSE Index Data" button.
    """
    _ensure_tables()
    conn = sqlite3.connect(str(DB_PATH), timeout=15)

    total     = len(NSE_INDEX_REGISTRY)
    done      = 0
    succeeded = 0
    failed    = []
    total_rows = 0

    now = datetime.now(timezone.utc).isoformat()

    for index_name, (category, csv_file) in NSE_INDEX_REGISTRY.items():
        done += 1
        if progress_state:
            progress_state["progress"] = done
            progress_state["total"]    = total
            progress_state["message"]  = f"Syncing {index_name} ({done}/{total})..."

        logger.info(f"Syncing {index_name}...")

        constituents = _download_index_csv(index_name, csv_file)

        if not constituents:
            failed.append(index_name)
            conn.execute("""
                INSERT INTO nse_index_registry (index_name, category, csv_file, status, last_synced)
                VALUES (?, ?, ?, 'failed', ?)
                ON CONFLICT(index_name) DO UPDATE SET status='failed', last_synced=excluded.last_synced
            """, (index_name, category, csv_file, now))
            conn.commit()
            time.sleep(0.5)
            continue

        # Delete old data for this index
        conn.execute("DELETE FROM nse_index_constituents WHERE index_name = ?", (index_name,))

        # Insert new data
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
        succeeded += 1
        total_rows += len(constituents)
        logger.info(f"  ✅ {index_name}: {len(constituents)} constituents")

        # Polite delay — avoid hammering the server
        time.sleep(0.8)

    conn.close()

    msg = (f"✅ Synced {succeeded}/{total} indices — "
           f"{total_rows:,} constituent records stored")
    if failed:
        msg += f" | ⚠ Failed: {', '.join(failed[:5])}"
    if progress_state:
        progress_state["message"]  = msg
        progress_state["running"]  = False

    logger.info(msg)
    return {
        "message":     msg,
        "succeeded":   succeeded,
        "failed":      failed,
        "total_rows":  total_rows,
        "total":       total,
    }


# ── Query helpers ─────────────────────────────────────────────────────────────
def get_index_constituents(index_name: str) -> list:
    """Get all tickers for a given index."""
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

    result = {"broad": [], "sectoral": [], "thematic": [], "total_synced": 0}
    for r in rows:
        entry = {
            "index_name":        r[0],
            "constituent_count": r[2] or 0,
            "last_synced":       r[3],
            "status":            r[4],
        }
        cat = r[1] if r[1] in result else "broad"
        result[cat].append(entry)
        if r[4] == "ok":
            result["total_synced"] += 1

    # Add pending indices not yet in registry
    synced_names = {r[0] for r in rows}
    for name, (cat, _) in NSE_INDEX_REGISTRY.items():
        if name not in synced_names:
            entry = {"index_name": name, "constituent_count": 0,
                     "last_synced": None, "status": "pending"}
            result[cat].append(entry)

    return result


def get_industry_for_ticker(ticker: str) -> str:
    """Get official NSE industry for a ticker (from NIFTY 500 first, then any)."""
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    row = conn.execute("""
        SELECT industry FROM nse_index_constituents
        WHERE ticker = ? AND index_name = 'NIFTY 500' AND industry != ''
        LIMIT 1
    """, (ticker,)).fetchone()
    if not row:
        row = conn.execute("""
            SELECT industry FROM nse_index_constituents
            WHERE ticker = ? AND industry != ''
            LIMIT 1
        """, (ticker,)).fetchone()
    conn.close()
    return row[0] if row else ""


def get_sector_constituents_map() -> dict:
    """Return {sector: [tickers]} from NIFTY 500 industry classification."""
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    rows = conn.execute("""
        SELECT ticker, industry FROM nse_index_constituents
        WHERE index_name = 'NIFTY 500' AND industry != ''
    """).fetchall()
    conn.close()
    result = {}
    for ticker, industry in rows:
        result.setdefault(industry, []).append(ticker)
    return result
