"""
Watchlist & Alerts — CRUD operations with SQLite storage.
"""
import sqlite3, pathlib
from datetime import datetime

DB_PATH = pathlib.Path(__file__).parent / "breadth_data.db"

def _conn():
    c = sqlite3.connect(str(DB_PATH))
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA journal_mode=WAL")
    return c

def init_tables():
    """Create watchlist/alert tables if they don't exist."""
    c = _conn()
    c.executescript("""
        CREATE TABLE IF NOT EXISTS watchlists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS watchlist_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            watchlist_id INTEGER,
            ticker TEXT NOT NULL,
            added_at TEXT DEFAULT CURRENT_TIMESTAMP,
            notes TEXT,
            FOREIGN KEY (watchlist_id) REFERENCES watchlists(id)
        );
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            condition_type TEXT NOT NULL,
            condition_value REAL,
            active INTEGER DEFAULT 1,
            triggered INTEGER DEFAULT 0,
            triggered_at TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
    """)
    c.commit()
    c.close()

# ── Watchlist CRUD ─────────────────────────────────────────────────────────────

def list_watchlists():
    c = _conn()
    rows = c.execute("""
        SELECT w.*, COUNT(wi.id) as item_count
        FROM watchlists w LEFT JOIN watchlist_items wi ON w.id = wi.watchlist_id
        GROUP BY w.id ORDER BY w.created_at DESC
    """).fetchall()
    c.close()
    return [dict(r) for r in rows]

def create_watchlist(name: str):
    c = _conn()
    cur = c.execute("INSERT INTO watchlists (name) VALUES (?)", (name,))
    wid = cur.lastrowid
    c.commit()
    c.close()
    return {"id": wid, "name": name}

def delete_watchlist(wid: int):
    c = _conn()
    c.execute("DELETE FROM watchlist_items WHERE watchlist_id=?", (wid,))
    c.execute("DELETE FROM watchlists WHERE id=?", (wid,))
    c.commit()
    c.close()
    return {"deleted": wid}

def add_ticker(wid: int, ticker: str, notes: str = ""):
    c = _conn()
    # Check duplicate
    existing = c.execute(
        "SELECT id FROM watchlist_items WHERE watchlist_id=? AND ticker=?",
        (wid, ticker.upper())
    ).fetchone()
    if existing:
        c.close()
        return {"error": "Ticker already in watchlist"}
    c.execute(
        "INSERT INTO watchlist_items (watchlist_id, ticker, notes) VALUES (?,?,?)",
        (wid, ticker.upper(), notes)
    )
    c.commit()
    c.close()
    return {"added": ticker.upper(), "watchlist_id": wid}

def remove_ticker(wid: int, ticker: str):
    c = _conn()
    c.execute(
        "DELETE FROM watchlist_items WHERE watchlist_id=? AND ticker=?",
        (wid, ticker.upper())
    )
    c.commit()
    c.close()
    return {"removed": ticker.upper(), "watchlist_id": wid}

def get_watchlist_data(wid: int):
    """Get watchlist items with live OHLCV data."""
    c = _conn()

    wl = c.execute("SELECT * FROM watchlists WHERE id=?", (wid,)).fetchone()
    if not wl:
        c.close()
        return {"error": "Watchlist not found"}

    items = c.execute(
        "SELECT ticker, notes FROM watchlist_items WHERE watchlist_id=? ORDER BY added_at",
        (wid,)
    ).fetchall()

    stocks = []
    for item in items:
        ticker = item["ticker"]
        notes = item["notes"] or ""

        # Get latest 200+ days of data for indicators
        rows = c.execute(
            "SELECT date, close, volume FROM ohlcv WHERE ticker=? ORDER BY date DESC LIMIT 250",
            (ticker,)
        ).fetchall()

        if not rows:
            stocks.append({"ticker": ticker, "notes": notes, "price": None, "error": "No data"})
            continue

        closes = [float(r["close"]) for r in rows if r["close"]]
        if not closes:
            stocks.append({"ticker": ticker, "notes": notes, "price": None, "error": "No close data"})
            continue

        price = closes[0]
        chg_1w = ((price - closes[4]) / closes[4] * 100) if len(closes) > 4 and closes[4] > 0 else None
        chg_1m = ((price - closes[21]) / closes[21] * 100) if len(closes) > 21 and closes[21] > 0 else None
        chg_3m = ((price - closes[63]) / closes[63] * 100) if len(closes) > 63 and closes[63] > 0 else None

        dma50 = sum(closes[:50]) / min(len(closes), 50) if len(closes) >= 30 else None
        dma200 = sum(closes[:200]) / min(len(closes), 200) if len(closes) >= 100 else None

        above_50 = price > dma50 if dma50 else None
        above_200 = price > dma200 if dma200 else None

        # Get sector
        sec_row = c.execute("SELECT sector FROM sector_map WHERE ticker=?", (ticker,)).fetchone()
        sector = sec_row["sector"] if sec_row else None

        # Get market cap
        mcap_cr = 0
        mcap_tier = ""
        try:
            mc_row = c.execute("SELECT mcap_cr, mcap_tier FROM market_cap WHERE ticker=?", (ticker,)).fetchone()
            if mc_row:
                mcap_cr = mc_row["mcap_cr"] or 0
                mcap_tier = mc_row["mcap_tier"] or ""
        except Exception:
            pass

        # Get alerts for this ticker
        alerts_rows = c.execute(
            "SELECT id, condition_type, condition_value, active, triggered, triggered_at FROM alerts WHERE ticker=?",
            (ticker,)
        ).fetchall()

        stocks.append({
            "ticker": ticker,
            "notes": notes,
            "price": round(price, 2),
            "chg_1w": round(chg_1w, 2) if chg_1w is not None else None,
            "chg_1m": round(chg_1m, 2) if chg_1m is not None else None,
            "chg_3m": round(chg_3m, 2) if chg_3m is not None else None,
            "above_50dma": above_50,
            "above_200dma": above_200,
            "sector": sector,
            "mcap_cr": mcap_cr,
            "mcap_tier": mcap_tier,
            "alerts": [dict(a) for a in alerts_rows],
        })

    c.close()
    return {
        "id": wl["id"],
        "name": wl["name"],
        "created_at": wl["created_at"],
        "stocks": stocks,
    }

# ── Alerts CRUD ────────────────────────────────────────────────────────────────

def create_alert(ticker: str, condition_type: str, condition_value: float = None):
    c = _conn()
    cur = c.execute(
        "INSERT INTO alerts (ticker, condition_type, condition_value) VALUES (?,?,?)",
        (ticker.upper(), condition_type, condition_value)
    )
    aid = cur.lastrowid
    c.commit()
    c.close()
    return {"id": aid, "ticker": ticker.upper(), "condition_type": condition_type}

def list_alerts():
    c = _conn()
    rows = c.execute("SELECT * FROM alerts ORDER BY created_at DESC").fetchall()
    c.close()
    return [dict(r) for r in rows]

def delete_alert(aid: int):
    c = _conn()
    c.execute("DELETE FROM alerts WHERE id=?", (aid,))
    c.commit()
    c.close()
    return {"deleted": aid}

def check_alerts():
    """Check all active alerts against current data. Return triggered ones."""
    c = _conn()
    active = c.execute("SELECT * FROM alerts WHERE active=1 AND triggered=0").fetchall()
    triggered = []

    for a in active:
        ticker = a["ticker"]
        ctype = a["condition_type"]
        cval = a["condition_value"]

        rows = c.execute(
            "SELECT close, volume FROM ohlcv WHERE ticker=? ORDER BY date DESC LIMIT 50",
            (ticker,)
        ).fetchall()
        if not rows:
            continue

        closes = [float(r["close"]) for r in rows if r["close"]]
        if not closes:
            continue

        price = closes[0]
        fire = False

        if ctype == "price_above" and cval and price >= cval:
            fire = True
        elif ctype == "price_below" and cval and price <= cval:
            fire = True
        elif ctype == "above_dma":
            dma_len = int(cval) if cval else 200
            if len(closes) >= dma_len:
                dma_val = sum(closes[:dma_len]) / dma_len
                if price > dma_val:
                    fire = True
        elif ctype == "below_dma":
            dma_len = int(cval) if cval else 200
            if len(closes) >= dma_len:
                dma_val = sum(closes[:dma_len]) / dma_len
                if price < dma_val:
                    fire = True

        if fire:
            now = datetime.utcnow().isoformat()
            c.execute(
                "UPDATE alerts SET triggered=1, triggered_at=? WHERE id=?",
                (now, a["id"])
            )
            triggered.append({
                "id": a["id"], "ticker": ticker,
                "condition_type": ctype, "condition_value": cval,
                "triggered_at": now,
            })

    c.commit()
    c.close()
    return triggered

# Init tables on import
init_tables()
