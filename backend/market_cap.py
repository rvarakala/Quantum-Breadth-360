"""
Market Cap Data Management
==========================
Imports market cap data, matches company names to tickers,
provides filtering functions for screeners/leaders.
"""

import sqlite3
import csv
import logging
import re
from pathlib import Path
from typing import Dict, Optional, List, Tuple

logger = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent / "breadth_data.db"
NIFTY500_PATH = Path(__file__).parent / "data" / "nifty500_clean.csv"

# Market cap tiers (in Crores INR)
MCAP_TIERS = {
    "Mega Cap":   100000,  # > 1 Lakh Cr
    "Large Cap":   20000,  # 20,000 - 1,00,000 Cr
    "Mid Cap":      5000,  # 5,000 - 20,000 Cr
    "Small Cap":     500,  # 500 - 5,000 Cr
    "Micro Cap":       0,  # < 500 Cr
}

def get_mcap_tier(mcap_cr: float) -> str:
    """Classify market cap into tier."""
    if mcap_cr >= 100000: return "Mega Cap"
    if mcap_cr >= 20000:  return "Large Cap"
    if mcap_cr >= 5000:   return "Mid Cap"
    if mcap_cr >= 500:    return "Small Cap"
    return "Micro Cap"

def _ensure_table():
    """Create market_cap table if not exists."""
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS market_cap (
            ticker TEXT PRIMARY KEY,
            company_name TEXT,
            mcap_cr REAL DEFAULT 0,
            mcap_tier TEXT DEFAULT 'Unknown',
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()

def _load_name_to_ticker_map() -> Dict[str, str]:
    """Build company name -> ticker mapping from nifty500_clean.csv + sector_map."""
    mapping = {}
    
    # From nifty500_clean.csv
    if NIFTY500_PATH.exists():
        with open(NIFTY500_PATH) as f:
            for row in csv.DictReader(f):
                name = (row.get('Company Name') or '').strip()
                sym = (row.get('Symbol') or '').strip()
                if name and sym:
                    mapping[name.lower()] = sym
                    # Also add simplified versions
                    clean = re.sub(r'\s+(ltd\.?|limited|corporation|inc\.?)\s*$', '', name, flags=re.IGNORECASE).strip()
                    mapping[clean.lower()] = sym
    
    # From DB ticker list (use ticker as-is for direct matches)
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    tickers = [r[0] for r in conn.execute("SELECT DISTINCT ticker FROM ohlcv WHERE market='India'").fetchall()]
    conn.close()
    
    for t in tickers:
        mapping[t.lower()] = t
    
    return mapping

def _match_company_to_ticker(company: str, mapping: Dict[str, str]) -> Optional[str]:
    """Try to match a company name to a ticker symbol."""
    company_lower = company.lower().strip()
    
    # Exact match
    if company_lower in mapping:
        return mapping[company_lower]
    
    # Remove "Ltd.", "Limited", etc
    clean = re.sub(r'\s+(ltd\.?|limited|corporation of india|corporation)\s*$', '', company_lower, flags=re.IGNORECASE).strip()
    if clean in mapping:
        return mapping[clean]
    
    # Try prefix matching (first 15 chars)
    prefix = clean[:15] if len(clean) >= 15 else clean
    for name, ticker in mapping.items():
        if name.startswith(prefix) and len(name) < len(clean) + 10:
            return ticker
    
    # Try removing common suffixes more aggressively
    aggressive_clean = re.sub(r'\s+(ltd\.?|limited|pvt\.?|private|india|industries|pharma|tech|technologies|infra|infrastructure)\s*', ' ', company_lower).strip()
    aggressive_clean = re.sub(r'\s+', ' ', aggressive_clean).strip()
    for name, ticker in mapping.items():
        name_clean = re.sub(r'\s+(ltd\.?|limited|pvt\.?|private|india|industries|pharma|tech|technologies|infra|infrastructure)\s*', ' ', name).strip()
        name_clean = re.sub(r'\s+', ' ', name_clean).strip()
        if aggressive_clean == name_clean:
            return ticker
    
    return None

def import_market_cap_csv(csv_path: str) -> Dict:
    """Import market cap data from CSV (Company, Market Cap (Cr)) into DB."""
    _ensure_table()
    mapping = _load_name_to_ticker_map()
    
    matched = 0
    unmatched = 0
    unmatched_list = []
    
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    
    with open(csv_path, 'r') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 2:
                continue
            company = row[0].strip()
            mcap_str = row[1].strip().replace(',', '')
            
            if not company or company == 'Company' or not mcap_str:
                continue
            
            try:
                mcap = float(mcap_str)
            except ValueError:
                continue
            
            ticker = _match_company_to_ticker(company, mapping)
            if ticker:
                tier = get_mcap_tier(mcap)
                conn.execute("""
                    INSERT OR REPLACE INTO market_cap (ticker, company_name, mcap_cr, mcap_tier, updated_at)
                    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                """, (ticker, company, mcap, tier))
                matched += 1
            else:
                unmatched += 1
                if unmatched <= 30:
                    unmatched_list.append(company)
    
    conn.commit()
    
    # Stats
    total_in_db = conn.execute("SELECT COUNT(*) FROM market_cap").fetchone()[0]
    tier_stats = conn.execute("""
        SELECT mcap_tier, COUNT(*), ROUND(AVG(mcap_cr),0)
        FROM market_cap GROUP BY mcap_tier ORDER BY AVG(mcap_cr) DESC
    """).fetchall()
    
    conn.close()
    
    result = {
        "matched": matched,
        "unmatched": unmatched,
        "total_in_db": total_in_db,
        "tiers": {t: {"count": c, "avg_mcap_cr": a} for t, c, a in tier_stats},
        "unmatched_samples": unmatched_list,
    }
    logger.info(f"Market cap import: {matched} matched, {unmatched} unmatched, {total_in_db} total in DB")
    return result

def get_mcap_for_ticker(ticker: str) -> Optional[Dict]:
    """Get market cap data for a single ticker."""
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    row = conn.execute(
        "SELECT company_name, mcap_cr, mcap_tier FROM market_cap WHERE ticker=?", (ticker,)
    ).fetchone()
    conn.close()
    if row:
        return {"company_name": row[0], "mcap_cr": row[1], "mcap_tier": row[2]}
    return None

def get_all_mcaps() -> Dict[str, Dict]:
    """Get market cap data for all tickers. Returns {ticker: {mcap_cr, mcap_tier}}."""
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    rows = conn.execute("SELECT ticker, mcap_cr, mcap_tier FROM market_cap").fetchall()
    conn.close()
    return {r[0]: {"mcap_cr": r[1], "mcap_tier": r[2]} for r in rows}

def filter_by_mcap(tickers: List[str], min_mcap_cr: float = 500) -> List[str]:
    """Filter tickers by minimum market cap. Default: exclude micro caps (<500 Cr)."""
    mcaps = get_all_mcaps()
    return [t for t in tickers if mcaps.get(t, {}).get("mcap_cr", 0) >= min_mcap_cr]

def format_mcap(mcap_cr: float) -> str:
    """Format market cap for display: 19,12,544 -> ₹19.1L Cr"""
    if mcap_cr >= 100000:
        return f"₹{mcap_cr/100000:.1f}L Cr"
    elif mcap_cr >= 1000:
        return f"₹{mcap_cr/1000:.1f}K Cr"
    else:
        return f"₹{mcap_cr:.0f} Cr"


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO)
    if len(sys.argv) > 1:
        result = import_market_cap_csv(sys.argv[1])
        print(f"\nResult: {result}")
    else:
        print("Usage: python market_cap.py <path_to_csv>")
