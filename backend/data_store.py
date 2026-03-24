"""
Local Data Store — SQLite-backed OHLCV storage
- Downloads 10 years of history on first run
- Daily incremental updates (only fetches new candles)
- All breadth calculations read from local DB (instant)
- Never re-downloads what you already have
"""

import sqlite3
import pandas as pd
import numpy as np
import yfinance as yf
from datetime import datetime, timezone, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional
import logging
import time

logger = logging.getLogger(__name__)

# ── DB location — sits next to main.py ───────────────────────────────────────
DB_PATH = Path(__file__).parent / "breadth_data.db"

# ── All tickers we track ──────────────────────────────────────────────────────
INDIA_TICKERS = [
    "360ONE",
    "3MINDIA",
    "ABB",
    "ACC",
    "ACMESOLAR",
    "AIAENG",
    "APLAPOLLO",
    "AUBANK",
    "AWL",
    "AADHARHFC",
    "AARTIIND",
    "AAVAS",
    "ABBOTINDIA",
    "ACE",
    "ADANIENSOL",
    "ADANIENT",
    "ADANIGREEN",
    "ADANIPORTS",
    "ADANIPOWER",
    "ATGL",
    "ABCAPITAL",
    "ABFRL",
    "ABLBL",
    "ABREL",
    "ABSLAMC",
    "AEGISLOG",
    "AEGISVOPAK",
    "AFCONS",
    "AFFLE",
    "AJANTPHARM",
    "AKUMS",
    "AKZOINDIA",
    "APLLTD",
    "ALKEM",
    "ALKYLAMINE",
    "ALOKINDS",
    "ARE&M",
    "AMBER",
    "AMBUJACEM",
    "ANANDRATHI",
    "ANANTRAJ",
    "ANGELONE",
    "APARINDS",
    "APOLLOHOSP",
    "APOLLOTYRE",
    "APTUS",
    "ASAHIINDIA",
    "ASHOKLEY",
    "ASIANPAINT",
    "ASTERDM",
    "ASTRAZEN",
    "ASTRAL",
    "ATHERENERG",
    "ATUL",
    "AUROPHARMA",
    "AIIL",
    "DMART",
    "AXISBANK",
    "BASF",
    "BEML",
    "BLS",
    "BSE",
    "BAJAJ-AUTO",
    "BAJFINANCE",
    "BAJAJFINSV",
    "BAJAJHLDNG",
    "BAJAJHFL",
    "BALKRISIND",
    "BALRAMCHIN",
    "BANDHANBNK",
    "BANKBARODA",
    "BANKINDIA",
    "MAHABANK",
    "BATAINDIA",
    "BAYERCROP",
    "BERGEPAINT",
    "BDL",
    "BEL",
    "BHARATFORG",
    "BHEL",
    "BPCL",
    "BHARTIARTL",
    "BHARTIHEXA",
    "BIKAJI",
    "BIOCON",
    "BSOFT",
    "BLUEDART",
    "BLUEJET",
    "BLUESTARCO",
    "BBTC",
    "BOSCHLTD",
    "FIRSTCRY",
    "BRIGADE",
    "BRITANNIA",
    "MAPMYINDIA",
    "CCL",
    "CESC",
    "CGPOWER",
    "CRISIL",
    "CAMPUS",
    "CANFINHOME",
    "CANBK",
    "CAPLIPOINT",
    "CGCL",
    "CARBORUNIV",
    "CASTROLIND",
    "CEATLTD",
    "CENTRALBK",
    "CDSL",
    "CENTURYPLY",
    "CERA",
    "CHALET",
    "CHAMBLFERT",
    "CHENNPETRO",
    "CHOICEIN",
    "CHOLAHLDNG",
    "CHOLAFIN",
    "CIPLA",
    "CUB",
    "CLEAN",
    "COALINDIA",
    "COCHINSHIP",
    "COFORGE",
    "COHANCE",
    "COLPAL",
    "CAMS",
    "CONCORDBIO",
    "CONCOR",
    "COROMANDEL",
    "CRAFTSMAN",
    "CREDITACC",
    "CROMPTON",
    "CUMMINSIND",
    "CYIENT",
    "DCMSHRIRAM",
    "DLF",
    "DOMS",
    "DABUR",
    "DALBHARAT",
    "DATAPATTNS",
    "DEEPAKFERT",
    "DEEPAKNTR",
    "DELHIVERY",
    "DEVYANI",
    "DIVISLAB",
    "DIXON",
    "AGARWALEYE",
    "LALPATHLAB",
    "DRREDDY",
    "EIDPARRY",
    "EIHOTEL",
    "EICHERMOT",
    "ELECON",
    "ELGIEQUIP",
    "EMAMILTD",
    "EMCURE",
    "ENDURANCE",
    "ENGINERSIN",
    "ERIS",
    "ESCORTS",
    "ETERNAL",
    "EXIDEIND",
    "NYKAA",
    "FEDERALBNK",
    "FACT",
    "FINCABLES",
    "FINPIPE",
    "FSL",
    "FIVESTAR",
    "FORCEMOT",
    "FORTIS",
    "GAIL",
    "GVT&D",
    "GMRAIRPORT",
    "GRSE",
    "GICRE",
    "GILLETTE",
    "GLAND",
    "GLAXO",
    "GLENMARK",
    "MEDANTA",
    "GODIGIT",
    "GPIL",
    "GODFRYPHLP",
    "GODREJAGRO",
    "GODREJCP",
    "GODREJIND",
    "GODREJPROP",
    "GRANULES",
    "GRAPHITE",
    "GRASIM",
    "GRAVITA",
    "GESHIP",
    "FLUOROCHEM",
    "GUJGASLTD",
    "GMDCLTD",
    "GSPL",
    "HEG",
    "HBLENGINE",
    "HCLTECH",
    "HDFCAMC",
    "HDFCBANK",
    "HDFCLIFE",
    "HFCL",
    "HAPPSTMNDS",
    "HAVELLS",
    "HEROMOTOCO",
    "HEXT",
    "HSCL",
    "HINDALCO",
    "HAL",
    "HINDCOPPER",
    "HINDPETRO",
    "HINDUNILVR",
    "HINDZINC",
    "POWERINDIA",
    "HOMEFIRST",
    "HONASA",
    "HONAUT",
    "HUDCO",
    "HYUNDAI",
    "ICICIBANK",
    "ICICIGI",
    "ICICIPRULI",
    "IDBI",
    "IDFCFIRSTB",
    "IFCI",
    "IIFL",
    "INOXINDIA",
    "IRB",
    "IRCON",
    "ITCHOTELS",
    "ITC",
    "ITI",
    "INDGN",
    "INDIACEM",
    "INDIAMART",
    "INDIANB",
    "IEX",
    "INDHOTEL",
    "IOC",
    "IOB",
    "IRCTC",
    "IRFC",
    "IREDA",
    "IGL",
    "INDUSTOWER",
    "INDUSINDBK",
    "NAUKRI",
    "INFY",
    "INOXWIND",
    "INTELLECT",
    "INDIGO",
    "IGIL",
    "IKS",
    "IPCALAB",
    "JBCHEPHARM",
    "JKCEMENT",
    "JBMA",
    "JKTYRE",
    "JMFINANCIL",
    "JSWCEMENT",
    "JSWENERGY",
    "JSWINFRA",
    "JSWSTEEL",
    "JPPOWER",
    "J&KBANK",
    "JINDALSAW",
    "JSL",
    "JINDALSTEL",
    "JIOFIN",
    "JUBLFOOD",
    "JUBLINGREA",
    "JUBLPHARMA",
    "JWL",
    "JYOTHYLAB",
    "JYOTICNC",
    "KPRMILL",
    "KEI",
    "KPITTECH",
    "KSB",
    "KAJARIACER",
    "KPIL",
    "KALYANKJIL",
    "KARURVYSYA",
    "KAYNES",
    "KEC",
    "KFINTECH",
    "KIRLOSBROS",
    "KIRLOSENG",
    "KOTAKBANK",
    "KIMS",
    "LTF",
    "LTTS",
    "LICHSGFIN",
    "LTFOODS",
    "LTM",
    "LT",
    "LATENTVIEW",
    "LAURUSLABS",
    "THELEELA",
    "LEMONTREE",
    "LICI",
    "LINDEINDIA",
    "LLOYDSME",
    "LODHA",
    "LUPIN",
    "MMTC",
    "MRF",
    "MGL",
    "MAHSCOOTER",
    "MAHSEAMLES",
    "M&MFIN",
    "M&M",
    "MANAPPURAM",
    "MRPL",
    "MANKIND",
    "MARICO",
    "MARUTI",
    "MFSL",
    "MAXHEALTH",
    "MAZDOCK",
    "METROPOLIS",
    "MINDACORP",
    "MSUMI",
    "MOTILALOFS",
    "MPHASIS",
    "MCX",
    "MUTHOOTFIN",
    "NATCOPHARM",
    "NBCC",
    "NCC",
    "NHPC",
    "NLCINDIA",
    "NMDC",
    "NSLNISP",
    "NTPCGREEN",
    "NTPC",
    "NH",
    "NATIONALUM",
    "NAVA",
    "NAVINFLUOR",
    "NESTLEIND",
    "NETWEB",
    "NEULANDLAB",
    "NEWGEN",
    "NAM-INDIA",
    "NIVABUPA",
    "NUVAMA",
    "NUVOCO",
    "OBEROIRLTY",
    "ONGC",
    "OIL",
    "OLAELEC",
    "OLECTRA",
    "PAYTM",
    "ONESOURCE",
    "OFSS",
    "POLICYBZR",
    "PCBL",
    "PGEL",
    "PIIND",
    "PNBHOUSING",
    "PTCIL",
    "PVRINOX",
    "PAGEIND",
    "PATANJALI",
    "PERSISTENT",
    "PETRONET",
    "PFIZER",
    "PHOENIXLTD",
    "PIDILITIND",
    "PPLPHARMA",
    "POLYMED",
    "POLYCAB",
    "POONAWALLA",
    "PFC",
    "POWERGRID",
    "PRAJIND",
    "PREMIERENE",
    "PRESTIGE",
    "PGHH",
    "PNB",
    "RRKABEL",
    "RBLBANK",
    "RECLTD",
    "RHIM",
    "RITES",
    "RADICO",
    "RVNL",
    "RAILTEL",
    "RAINBOW",
    "RKFORGE",
    "RCF",
    "REDINGTON",
    "RELIANCE",
    "RELINFRA",
    "RPOWER",
    "SBFC",
    "SBICARD",
    "SBILIFE",
    "SJVN",
    "SRF",
    "SAGILITY",
    "SAILIFE",
    "SAMMAANCAP",
    "MOTHERSON",
    "SAPPHIRE",
    "SARDAEN",
    "SAREGAMA",
    "SCHAEFFLER",
    "SCHNEIDER",
    "SCI",
    "SHREECEM",
    "SHRIRAMFIN",
    "SHYAMMETL",
    "ENRIN",
    "SIEMENS",
    "SIGNATURE",
    "SOBHA",
    "SOLARINDS",
    "SONACOMS",
    "SONATSOFTW",
    "STARHEALTH",
    "SBIN",
    "SAIL",
    "SUMICHEM",
    "SUNPHARMA",
    "SUNTV",
    "SUNDARMFIN",
    "SUNDRMFAST",
    "SUPREMEIND",
    "SUZLON",
    "SWANCORP",
    "SWIGGY",
    "SYNGENE",
    "SYRMA",
    "TBOTEK",
    "TVSMOTOR",
    "TATACHEM",
    "TATACOMM",
    "TCS",
    "TATACONSUM",
    "TATAELXSI",
    "TATAINVEST",
    "TMPV",
    "TATAPOWER",
    "TATASTEEL",
    "TATATECH",
    "TTML",
    "TECHM",
    "TECHNOE",
    "TEJASNET",
    "NIACL",
    "RAMCOCEM",
    "THERMAX",
    "TIMKEN",
    "TITAGARH",
    "TITAN",
    "TORNTPHARM",
    "TORNTPOWER",
    "TARIL",
    "TRENT",
    "TRIDENT",
    "TRIVENI",
    "TRITURBINE",
    "TIINDIA",
    "UCOBANK",
    "UNOMINDA",
    "UPL",
    "UTIAMC",
    "ULTRACEMCO",
    "UNIONBANK",
    "UBL",
    "UNITDSPR",
    "USHAMART",
    "VGUARD",
    "DBREALTY",
    "VTL",
    "VBL",
    "MANYAVAR",
    "VEDL",
    "VENTIVE",
    "VIJAYA",
    "VMM",
    "IDEA",
    "VOLTAS",
    "WAAREEENER",
    "WELCORP",
    "WELSPUNLIV",
    "WHIRLPOOL",
    "WIPRO",
    "WOCKPHARMA",
    "YESBANK",
    "ZFCVINDIA",
    "ZEEL",
    "ZENTEC",
    "ZENSARTECH",
    "ZYDUSLIFE",
    "ECLERX"
]

SP500_TICKERS = [
    "AAPL","MSFT","AMZN","NVDA","GOOGL","META","TSLA","BRK-B","UNH","JNJ",
    "XOM","V","JPM","PG","MA","HD","CVX","MRK","ABBV","LLY",
    "PEP","KO","COST","AVGO","MCD","WMT","CSCO","TMO","ACN","ABT",
    "DHR","NEE","VZ","ADBE","CRM","NKE","CMCSA","TXN","PM","WFC",
    "BMY","RTX","UPS","QCOM","HON","ORCL","COP","LOW","MS","INTC",
    "AMGN","SBUX","IBM","GS","BLK","AMD","AXP","BA","CAT","MDLZ",
    "DE","GE","NOW","INTU","SPGI","AMAT","LMT","SCHW","ADI","SYK",
]

INDEX_TICKERS = {
    "^NSEI":     "NIFTY50",
    "^GSPC":     "SP500",
    "^INDIAVIX": "INDIAVIX",
    "^VIX":      "VIX",
}

ALL_TICKERS = {
    **{t: "India" for t in INDIA_TICKERS},
    **{t: "US"    for t in SP500_TICKERS},
    **{k: "Index" for k in INDEX_TICKERS},
}

HISTORY_YEARS = 10


# ─────────────────────────────────────────────────────────────────────────────
# DB SETUP
# ─────────────────────────────────────────────────────────────────────────────

def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")   # faster concurrent reads
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn

def init_db():
    """Create tables if they don't exist"""
    conn = get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS ohlcv (
            ticker      TEXT    NOT NULL,
            market      TEXT    NOT NULL,
            date        TEXT    NOT NULL,   -- ISO date YYYY-MM-DD
            open        REAL,
            high        REAL,
            low         REAL,
            close       REAL    NOT NULL,
            volume      INTEGER,
            PRIMARY KEY (ticker, date)
        );

        CREATE INDEX IF NOT EXISTS idx_ticker_date ON ohlcv(ticker, date);
        CREATE INDEX IF NOT EXISTS idx_market_date  ON ohlcv(market, date);

        CREATE TABLE IF NOT EXISTS sync_log (
            ticker      TEXT    PRIMARY KEY,
            market      TEXT,
            last_date   TEXT,               -- last date we have data for
            last_sync   TEXT,               -- when we last ran the sync
            total_rows  INTEGER DEFAULT 0,
            status      TEXT DEFAULT 'pending'
        );

        CREATE TABLE IF NOT EXISTS sync_meta (
            key     TEXT PRIMARY KEY,
            value   TEXT
        );

        CREATE TABLE IF NOT EXISTS sector_map (
            ticker      TEXT PRIMARY KEY,
            company     TEXT,
            sector      TEXT,
            subsector   TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_sector ON sector_map(sector);

        CREATE TABLE IF NOT EXISTS ticker_universe (
            market      TEXT NOT NULL,
            ticker      TEXT NOT NULL,
            PRIMARY KEY (market, ticker)
        );
    """)
    conn.commit()
    conn.close()
    logger.info(f"DB initialised at {DB_PATH}")


# ─────────────────────────────────────────────────────────────────────────────
# WRITE HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def upsert_df(ticker: str, market: str, df: pd.DataFrame):
    """Write a DataFrame of OHLCV rows into SQLite, ignoring duplicates"""
    if df is None or df.empty:
        return 0

    rows = []
    for idx, row in df.iterrows():
        d = idx.date() if hasattr(idx, 'date') else idx
        rows.append((
            ticker, market, str(d),
            _f(row.get("Open")),  _f(row.get("High")),
            _f(row.get("Low")),   _f(row.get("Close")),
            int(row.get("Volume", 0) or 0),
        ))

    conn = get_conn()
    conn.executemany("""
        INSERT OR REPLACE INTO ohlcv (ticker,market,date,open,high,low,close,volume)
        VALUES (?,?,?,?,?,?,?,?)
    """, rows)

    last_date = str(df.index[-1].date()) if hasattr(df.index[-1], 'date') else str(df.index[-1])[:10]
    conn.execute("""
        INSERT OR REPLACE INTO sync_log (ticker,market,last_date,last_sync,total_rows,status)
        VALUES (?,?,?,?,?,?)
    """, (ticker, market, last_date,
          datetime.now(timezone.utc).isoformat(),
          len(rows), "ok"))

    conn.commit()
    conn.close()
    return len(rows)

def _f(v):
    try:
        f = float(v)
        return None if np.isnan(f) else round(f, 4)
    except:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# READ HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def load_ticker(ticker: str, days: int = 365*10) -> Optional[pd.DataFrame]:
    """Load OHLCV from local DB for a single ticker"""
    since = (date.today() - timedelta(days=days)).isoformat()
    conn  = get_conn()
    df = pd.read_sql_query("""
        SELECT date,open,high,low,close,volume
        FROM ohlcv WHERE ticker=? AND date>=?
        ORDER BY date ASC
    """, conn, params=(ticker, since))
    conn.close()
    if df.empty:
        return None
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date")
    df.columns = ["Open","High","Low","Close","Volume"]
    return df

def load_market(market: str, days: int = 365) -> Dict[str, pd.DataFrame]:
    """Load all tickers for a market from local DB"""
    since = (date.today() - timedelta(days=days)).isoformat()
    conn  = get_conn()
    raw = pd.read_sql_query("""
        SELECT ticker,date,open,high,low,close,volume
        FROM ohlcv WHERE market=? AND date>=?
        ORDER BY ticker,date ASC
    """, conn, params=(market, since))
    conn.close()

    if raw.empty:
        return {}

    result = {}
    for ticker, grp in raw.groupby("ticker"):
        df = grp.drop("ticker", axis=1).copy()
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date")
        df.columns = ["Open","High","Low","Close","Volume"]
        if len(df) >= 20:
            result[ticker] = df
    return result

def get_last_date(ticker: str) -> Optional[str]:
    """Get the latest date we have for a ticker"""
    conn = get_conn()
    row  = conn.execute(
        "SELECT last_date FROM sync_log WHERE ticker=?", (ticker,)
    ).fetchone()
    conn.close()
    return row[0] if row else None

def db_stats() -> dict:
    """Return row counts and coverage info"""
    conn = get_conn()
    total  = conn.execute("SELECT COUNT(*) FROM ohlcv").fetchone()[0]
    tickers= conn.execute("SELECT COUNT(DISTINCT ticker) FROM ohlcv").fetchone()[0]
    markets= conn.execute(
        "SELECT market, COUNT(DISTINCT ticker), COUNT(*) FROM ohlcv GROUP BY market"
    ).fetchall()
    oldest = conn.execute("SELECT MIN(date) FROM ohlcv").fetchone()[0]
    newest = conn.execute("SELECT MAX(date) FROM ohlcv").fetchone()[0]
    pending= conn.execute(
        "SELECT COUNT(*) FROM sync_log WHERE status='pending'"
    ).fetchone()[0]
    conn.close()
    return {
        "total_rows":   total,
        "total_tickers": tickers,
        "oldest_date":  oldest,
        "newest_date":  newest,
        "pending_tickers": pending,
        "by_market": {m: {"tickers": t, "rows": r} for m,t,r in markets},
    }


# ─────────────────────────────────────────────────────────────────────────────
# DOWNLOAD ENGINE
# ─────────────────────────────────────────────────────────────────────────────

def _yf_suffix(ticker: str, market: str) -> str:
    if market == "India":
        return f"{ticker}.NS"
    if market == "Index":
        return ticker          # already has ^ prefix
    return ticker              # US — no suffix

def download_full_history(ticker: str, market: str) -> int:
    """Download 10 years of history for one ticker"""
    yf_ticker = _yf_suffix(ticker, market)
    try:
        df = yf.download(yf_ticker, period=f"{HISTORY_YEARS}y",
                         interval="1d", auto_adjust=True,
                         progress=False, timeout=30)
        if df is None or df.empty:
            logger.warning(f"  No data for {yf_ticker}")
            return 0
        # Flatten multi-index if single ticker returns one
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        rows = upsert_df(ticker, market, df)
        logger.info(f"  {yf_ticker}: {rows} rows ({str(df.index[0])[:10]} → {str(df.index[-1])[:10]})")
        return rows
    except Exception as e:
        logger.error(f"  {yf_ticker}: download failed — {e}")
        return 0

def download_incremental(ticker: str, market: str) -> int:
    """Only download candles newer than what we already have"""
    last = get_last_date(ticker)
    if not last:
        return download_full_history(ticker, market)

    last_dt   = datetime.strptime(last, "%Y-%m-%d").date()
    today     = date.today()
    days_behind = (today - last_dt).days

    if days_behind <= 1:
        logger.debug(f"  {ticker}: already up to date ({last})")
        return 0

    yf_ticker = _yf_suffix(ticker, market)
    start     = last_dt + timedelta(days=1)
    try:
        df = yf.download(yf_ticker, start=str(start), end=str(today + timedelta(days=1)),
                         interval="1d", auto_adjust=True,
                         progress=False, timeout=30)
        if df is None or df.empty:
            return 0
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        rows = upsert_df(ticker, market, df)
        if rows:
            logger.info(f"  {ticker}: +{rows} new rows (up to {str(df.index[-1])[:10]})")
        return rows
    except Exception as e:
        logger.error(f"  {ticker}: incremental failed — {e}")
        return 0


# ─────────────────────────────────────────────────────────────────────────────
# BULK SYNC FUNCTIONS  (called from API routes)
# ─────────────────────────────────────────────────────────────────────────────

def run_full_backfill(market: str = "all",
                      batch_size: int = 10,
                      delay: float = 1.0) -> dict:
    """
    Download 10 years of history for all tickers.
    Processes in batches to be friendly to yfinance rate limits.
    market = 'India' | 'US' | 'Index' | 'all'
    """
    init_db()
    targets = {t: m for t, m in ALL_TICKERS.items()
               if market == "all" or m == market}

    logger.info(f"Starting full backfill: {len(targets)} tickers, {HISTORY_YEARS}y history")
    total_rows = done = skipped = failed = 0
    ticker_list = list(targets.items())

    for i in range(0, len(ticker_list), batch_size):
        batch = ticker_list[i:i+batch_size]
        yf_batch = [_yf_suffix(t, m) for t, m in batch]

        logger.info(f"Batch {i//batch_size+1}/{(len(ticker_list)-1)//batch_size+1}: {yf_batch}")
        try:
            # Batch download all at once — much faster than one-by-one
            raw = yf.download(
                " ".join(yf_batch),
                period=f"{HISTORY_YEARS}y",
                interval="1d",
                group_by="ticker",
                auto_adjust=True,
                threads=True,
                progress=False,
                timeout=60,
            )
            if raw is None or raw.empty:
                logger.warning(f"  Batch returned empty")
                failed += len(batch)
                continue

            for ticker, market_name in batch:
                yf_t = _yf_suffix(ticker, market_name)
                try:
                    if len(yf_batch) == 1:
                        df = raw.copy()
                    elif isinstance(raw.columns, pd.MultiIndex):
                        if yf_t not in raw.columns.get_level_values(0):
                            failed += 1; continue
                        df = raw[yf_t].copy()
                    else:
                        df = raw.copy()

                    df = df.dropna(subset=["Close"])
                    if df.empty:
                        failed += 1; continue

                    rows = upsert_df(ticker, market_name, df)
                    total_rows += rows
                    done += 1
                except Exception as e:
                    logger.error(f"  Error saving {ticker}: {e}")
                    failed += 1

        except Exception as e:
            logger.error(f"  Batch download error: {e}")
            failed += len(batch)

        if i + batch_size < len(ticker_list):
            time.sleep(delay)   # be polite to Yahoo

    logger.info(f"Backfill complete: {done} tickers, {total_rows:,} rows, {failed} failed")
    return {"done": done, "total_rows": total_rows, "failed": failed,
            "skipped": skipped, "total_tickers": len(targets)}

def run_daily_update() -> dict:
    """
    Incremental update — only fetch candles newer than last stored date.
    Run this once per day after market close.
    """
    init_db()
    total_rows = updated = skipped = 0
    for ticker, market in ALL_TICKERS.items():
        last = get_last_date(ticker)
        if last:
            days_behind = (date.today() - datetime.strptime(last, "%Y-%m-%d").date()).days
            if days_behind <= 1:
                skipped += 1
                continue
        rows = download_incremental(ticker, market)
        total_rows += rows
        if rows > 0:
            updated += 1
        time.sleep(0.05)   # small delay between tickers

    logger.info(f"Daily update: {updated} tickers updated, {total_rows} new rows, {skipped} already current")
    return {"updated": updated, "total_new_rows": total_rows, "skipped": skipped}


# ─────────────────────────────────────────────────────────────────────────────
# SMART LOADER  (used by main.py — tries local DB first, falls back to live)
# ─────────────────────────────────────────────────────────────────────────────

def smart_load_market(market: str, days: int = 365) -> Dict[str, pd.DataFrame]:
    """
    Primary: load from local SQLite DB
    Fallback: download live from yfinance if DB is empty
    """
    local = load_market(market, days=days)
    if local and len(local) >= 10:
        logger.info(f"Loaded {len(local)} {market} tickers from local DB")
        return local

    # DB empty — fall back to live download
    logger.warning(f"Local DB empty for {market} — fetching live from yfinance")
    tickers = INDIA_TICKERS if market == "India" else SP500_TICKERS
    suffix  = ".NS" if market == "India" else ""

    from main import fetch_batch
    return fetch_batch(tickers, suffix=suffix, period="1y")


# ─────────────────────────────────────────────────────────────────────────────
# SECTOR MAP STORAGE
# ─────────────────────────────────────────────────────────────────────────────

def save_sector_map(sector_data: list):
    """Save sector mapping from sectors.csv into SQLite"""
    if not sector_data:
        return 0
    conn = get_conn()
    rows = [(r['ticker'], r.get('company',''), r.get('sector',''), r.get('subsector',''))
            for r in sector_data if r.get('ticker')]
    conn.executemany("""
        INSERT OR REPLACE INTO sector_map (ticker, company, sector, subsector)
        VALUES (?,?,?,?)
    """, rows)
    conn.commit()
    conn.close()
    logger.info(f"Saved {len(rows)} sector mappings to SQLite")
    return len(rows)

def load_sector_map() -> dict:
    """Load sector map from SQLite — returns {ticker: {sector, subsector, company}}"""
    conn = get_conn()
    rows = conn.execute("SELECT ticker, company, sector, subsector FROM sector_map").fetchall()
    conn.close()
    return {r[0]: {'ticker':r[0],'company':r[1],'sector':r[2],'subsector':r[3]} for r in rows}

def load_sector_counts() -> list:
    """Return sector breakdown sorted by ticker count"""
    conn = get_conn()
    rows = conn.execute("""
        SELECT sector, COUNT(*) as cnt, COUNT(DISTINCT subsector) as subs
        FROM sector_map GROUP BY sector ORDER BY cnt DESC
    """).fetchall()
    conn.close()
    return [{'sector':r[0],'tickers':r[1],'subsectors':r[2]} for r in rows]


# ─────────────────────────────────────────────────────────────────────────────
# TICKER UNIVERSE STORAGE
# ─────────────────────────────────────────────────────────────────────────────

def save_ticker_universe(market: str, tickers: list):
    """Save custom ticker universe for a market"""
    conn = get_conn()
    conn.execute("DELETE FROM ticker_universe WHERE market=?", (market,))
    conn.executemany("INSERT INTO ticker_universe (market, ticker) VALUES (?,?)",
                     [(market, t.upper()) for t in tickers])
    conn.commit()
    conn.close()
    logger.info(f"Saved {len(tickers)} {market} tickers to universe")

def load_ticker_universe(market: str) -> list:
    """Load saved ticker universe for a market"""
    conn = get_conn()
    rows = conn.execute("SELECT ticker FROM ticker_universe WHERE market=?",
                        (market,)).fetchall()
    conn.close()
    return [r[0] for r in rows]


# ─────────────────────────────────────────────────────────────────────────────
# IMPORT SECTORS FROM CSV FILE
# ─────────────────────────────────────────────────────────────────────────────

def import_sectors_csv(filepath: str) -> int:
    """
    Import sectors.csv directly into SQLite sector_map table.
    Format: ticker,company,sector,subsector (no header)
    """
    import csv
    init_db()
    rows = []
    try:
        with open(filepath, 'r', encoding='utf-8-sig') as f:
            for line in f:
                parts = line.strip().split(',')
                if len(parts) < 3:
                    continue
                ticker    = parts[0].strip().upper()
                company   = parts[1].strip()
                sector    = parts[2].strip()
                subsector = parts[3].strip() if len(parts) > 3 else sector
                if ticker and sector and ticker != 'Z':
                    rows.append((ticker, company, sector, subsector))
    except Exception as e:
        logger.error(f"Error reading sectors CSV: {e}")
        return 0

    if rows:
        conn = get_conn()
        conn.executemany("""
            INSERT OR REPLACE INTO sector_map (ticker, company, sector, subsector)
            VALUES (?,?,?,?)
        """, rows)
        conn.commit()
        conn.close()

    logger.info(f"Imported {len(rows)} sector mappings from {filepath}")
    return len(rows)


# ─────────────────────────────────────────────────────────────────────────────
# IMPORT TICKER UNIVERSE FROM CSV
# ─────────────────────────────────────────────────────────────────────────────

def import_ticker_universe_csv(filepath: str, market: str = 'India') -> int:
    """
    Import ticker list CSV into SQLite universe table.
    Supports: single column (ticker), or with header row.
    """
    init_db()
    tickers = []
    try:
        with open(filepath, 'r', encoding='utf-8-sig') as f:
            lines = f.read().replace('\r','').splitlines()
        for line in lines:
            t = line.split(',')[0].strip().upper()
            t = t.replace('.NS','').replace('.BO','').replace('"','')
            if t and t not in ('TICKER','SYMBOL','NSE CODE','') and len(t) <= 20:
                tickers.append(t)
        tickers = list(set(tickers))
    except Exception as e:
        logger.error(f"Error reading ticker CSV: {e}")
        return 0

    save_ticker_universe(market, tickers)
    return len(tickers)


def import_nifty500_csv(filepath: str) -> int:
    """
    Import NIFTY 500 CSV with Company Name, Industry, Symbol columns.
    Stores tickers in universe AND sector map simultaneously.
    """
    init_db()
    tickers = []
    sector_rows = []
    try:
        with open(filepath, 'r', encoding='utf-8-sig') as f:
            import csv as _csv
            reader = _csv.DictReader(f)
            for row in reader:
                ticker  = row.get('Symbol','').strip().upper()
                company = row.get('Company Name','').strip()
                sector  = row.get('Industry','').strip()
                if not ticker: continue
                tickers.append(ticker)
                sector_rows.append((ticker, company, sector, sector))
    except Exception as e:
        logger.error(f"Error reading NIFTY 500 CSV: {e}")
        return 0

    conn = get_conn()
    # Save universe
    conn.execute("DELETE FROM ticker_universe WHERE market='India'")
    conn.executemany("INSERT INTO ticker_universe (market, ticker) VALUES ('India',?)",
                     [(t,) for t in tickers])
    # Save sector map (Industry as both sector and subsector)
    conn.executemany("""
        INSERT OR REPLACE INTO sector_map (ticker, company, sector, subsector)
        VALUES (?,?,?,?)
    """, sector_rows)
    conn.commit()
    conn.close()
    logger.info(f"Imported {len(tickers)} NIFTY 500 tickers with industry mapping")
    return len(tickers)
