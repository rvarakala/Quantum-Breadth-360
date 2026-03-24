"""
Smart Metrics — Techno-Fundamental Analysis
=============================================
1. fetch_screener_data(ticker)  — scrape screener.in for fundamentals
2. compute_om_score(data)       — 14-criteria O'Neil+Minervini scoring
3. compute_technicals(ticker)   — stage, RS, A/D, ADR%, pressure, TPR from DB
4. compute_smart_score(om, tech)— composite Smart Score
5. get_smart_metrics(ticker)    — main entry combining all
"""

import sqlite3
import pathlib
import logging
import json
import re
import time
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError

logger = logging.getLogger(__name__)
DB_PATH = pathlib.Path(__file__).parent / "breadth_data.db"

SCREENER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# ─── Helpers ──────────────────────────────────────────────────────────────────

def _py(v):
    """Convert numpy types to native Python for JSON serialization."""
    if v is None:
        return None
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return float(v)
    if isinstance(v, np.bool_):
        return bool(v)
    if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
        return None
    return v


def _safe_float(s):
    """Parse a string to float, stripping commas and %. Returns None on failure."""
    if s is None:
        return None
    if isinstance(s, (int, float)):
        return float(s)
    s = str(s).strip().replace(",", "").replace("%", "").replace("₹", "").replace("Cr.", "").strip()
    if not s or s == "--" or s == "—":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _ensure_screener_cache_table():
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS screener_cache (
            ticker TEXT PRIMARY KEY,
            data TEXT,
            fetched_at TEXT
        )
    """)
    conn.commit()
    conn.close()


def _get_cached_screener(ticker: str) -> dict | None:
    """Return cached screener data if fresh (< 24h)."""
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=10)
        row = conn.execute(
            "SELECT data, fetched_at FROM screener_cache WHERE ticker=?", (ticker,)
        ).fetchone()
        conn.close()
        if row:
            fetched = datetime.fromisoformat(row[1])
            if datetime.now(timezone.utc) - fetched < timedelta(hours=24):
                return json.loads(row[0])
    except Exception as e:
        logger.debug(f"Cache read failed for {ticker}: {e}")
    return None


def _set_cached_screener(ticker: str, data: dict):
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=30)
        conn.execute(
            "INSERT OR REPLACE INTO screener_cache (ticker, data, fetched_at) VALUES (?, ?, ?)",
            (ticker, json.dumps(data), datetime.now(timezone.utc).isoformat())
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.debug(f"Cache write failed for {ticker}: {e}")


# ─── 1. Screener.in Scraper ──────────────────────────────────────────────────

def _fetch_html(url: str) -> str | None:
    """Fetch HTML from URL. Returns None on 404/error."""
    try:
        req = Request(url, headers=SCREENER_HEADERS)
        with urlopen(req, timeout=10) as resp:
            if resp.status == 200:
                return resp.read().decode("utf-8", errors="replace")
    except HTTPError as e:
        if e.code == 404:
            return None
        logger.debug(f"HTTP {e.code} fetching {url}")
    except (URLError, Exception) as e:
        logger.debug(f"Error fetching {url}: {e}")
    return None


def _search_screener_slug(ticker: str) -> str | None:
    """Use screener.in search API to find the correct company slug."""
    url = f"https://www.screener.in/api/company/search/?q={ticker}"
    try:
        req = Request(url, headers=SCREENER_HEADERS)
        with urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read().decode())
            # Response is a list of dicts with "name" and "url" keys
            if data and len(data) > 0:
                # Return the slug from the URL field
                first = data[0]
                slug_url = first.get("url", "")
                # e.g. "/company/RELIANCE/consolidated/"
                return slug_url
    except Exception as e:
        logger.debug(f"Screener search failed for {ticker}: {e}")
    return None


def _parse_key_ratios(soup) -> dict:
    """Extract key ratios from the top-of-page ratio list."""
    ratios = {}
    # screener.in uses <ul id="top-ratios"> or <div class="company-ratios">
    # Each ratio is in a <li> with <span class="name"> and <span class="value">
    for li in soup.select("#top-ratios li, .company-ratios li, .ratios-table li"):
        name_el = li.select_one(".name")
        val_el = li.select_one(".value, .number")
        if name_el and val_el:
            name = name_el.get_text(strip=True)
            val = val_el.get_text(strip=True)
            ratios[name] = val

    # Also try the newer format with data tables
    for row in soup.select(".company-info .flex-row, .company-info .row-full-width"):
        spans = row.select("span")
        if len(spans) >= 2:
            ratios[spans[0].get_text(strip=True)] = spans[1].get_text(strip=True)

    return ratios


def _parse_table(soup, section_id: str) -> list[dict]:
    """Parse a screener.in data table (quarterly/annual) by section ID or heading."""
    rows_data = []

    # Try to find section by ID
    section = soup.find("section", id=section_id)
    if not section:
        # Try finding by heading text
        for h2 in soup.find_all(["h2", "h3"]):
            text = h2.get_text(strip=True).lower()
            if section_id.lower().replace("-", " ") in text:
                section = h2.find_parent("section")
                break

    if not section:
        return rows_data

    table = section.find("table")
    if not table:
        return rows_data

    # Get headers (period labels)
    headers = []
    thead = table.find("thead")
    if thead:
        for th in thead.find_all("th"):
            headers.append(th.get_text(strip=True))
    else:
        first_row = table.find("tr")
        if first_row:
            for th in first_row.find_all(["th", "td"]):
                headers.append(th.get_text(strip=True))

    # Get data rows
    tbody = table.find("tbody")
    data_rows = tbody.find_all("tr") if tbody else table.find_all("tr")[1:]

    for tr in data_rows:
        cells = tr.find_all(["td", "th"])
        if len(cells) < 2:
            continue
        row_name = cells[0].get_text(strip=True)
        values = [_safe_float(c.get_text(strip=True)) for c in cells[1:]]
        rows_data.append({"metric": row_name, "values": values})

    return rows_data


def _parse_pros_cons(soup) -> dict:
    """Extract pros and cons lists."""
    pros = []
    cons = []
    for section in soup.select(".pros-cons .pros li, .company-pros li"):
        pros.append(section.get_text(strip=True))
    for section in soup.select(".pros-cons .cons li, .company-cons li"):
        cons.append(section.get_text(strip=True))

    # Alternate approach: look for headings
    if not pros and not cons:
        for div in soup.select(".pros, .cons"):
            items = [li.get_text(strip=True) for li in div.find_all("li")]
            cls = " ".join(div.get("class", []))
            if "pros" in cls:
                pros = items
            elif "cons" in cls:
                cons = items

    return {"pros": pros[:10], "cons": cons[:10]}


def _extract_quarterly(table_data: list) -> list[dict]:
    """Convert table rows into per-quarter dicts."""
    if not table_data:
        return []
    # Find the relevant metrics
    metric_map = {}
    for row in table_data:
        key = row["metric"].lower()
        if "sales" in key or "revenue" in key:
            metric_map["sales"] = row["values"]
        elif "operating profit" in key and "opm" not in key:
            metric_map["operating_profit"] = row["values"]
        elif "opm" in key:
            metric_map["opm"] = row["values"]
        elif "net profit" in key:
            metric_map["net_profit"] = row["values"]
        elif key == "eps" or "eps (rs)" in key or "eps in rs" in key:
            metric_map["eps"] = row["values"]

    n = max((len(v) for v in metric_map.values()), default=0)
    quarters = []
    for i in range(n):
        q = {}
        for k, vals in metric_map.items():
            q[k] = vals[i] if i < len(vals) else None
        quarters.append(q)
    return quarters


def _extract_annual(table_data: list) -> list[dict]:
    """Convert annual P&L table rows into per-year dicts."""
    return _extract_quarterly(table_data)  # Same structure


def fetch_screener_data(ticker: str) -> dict:
    """
    Fetch fundamental data from screener.in.
    Tries consolidated → standalone → search API.
    Caches results for 24 hours.
    """
    from bs4 import BeautifulSoup

    ticker = ticker.upper().strip()

    # Check cache first
    _ensure_screener_cache_table()
    cached = _get_cached_screener(ticker)
    if cached:
        logger.info(f"Screener cache hit for {ticker}")
        return cached

    # Try fetching HTML
    html = None
    source_url = None

    # 1. Consolidated
    url1 = f"https://www.screener.in/company/{ticker}/consolidated/"
    html = _fetch_html(url1)
    if html:
        source_url = url1

    # 2. Standalone
    if not html:
        url2 = f"https://www.screener.in/company/{ticker}/"
        html = _fetch_html(url2)
        if html:
            source_url = url2

    # 3. Search API
    if not html:
        slug = _search_screener_slug(ticker)
        if slug:
            url3 = f"https://www.screener.in{slug}"
            html = _fetch_html(url3)
            if html:
                source_url = url3

    if not html:
        return {"error": f"Could not fetch data for {ticker} from screener.in", "ticker": ticker}

    # Parse with BeautifulSoup
    soup = BeautifulSoup(html, "lxml")

    # Company name
    company_name = ""
    h1 = soup.find("h1")
    if h1:
        company_name = h1.get_text(strip=True)

    # Key ratios
    ratios = _parse_key_ratios(soup)

    # Parse specific ratio values
    def _ratio_val(keys):
        for k in keys:
            for rk, rv in ratios.items():
                if k.lower() in rk.lower():
                    return _safe_float(rv)
        return None

    parsed_ratios = {
        "market_cap": _ratio_val(["Market Cap"]),
        "pe": _ratio_val(["Stock P/E"]),
        "book_value": _ratio_val(["Book Value"]),
        "roce": _ratio_val(["ROCE"]),
        "roe": _ratio_val(["ROE"]),
        "dividend_yield": _ratio_val(["Dividend Yield"]),
        "face_value": _ratio_val(["Face Value"]),
        "debt_to_equity": _ratio_val(["Debt to equity", "Debt / Equity"]),
        "current_price": _ratio_val(["Current Price"]),
        "high_low": ratios.get("High / Low", ""),
        "promoter_holding": _ratio_val(["Promoter holding"]),
    }

    # Quarterly results
    quarterly_raw = _parse_table(soup, "quarters")
    quarterly = _extract_quarterly(quarterly_raw)

    # Annual P&L
    annual_raw = _parse_table(soup, "profit-loss")
    annual = _extract_annual(annual_raw)

    # Pros/Cons
    pros_cons = _parse_pros_cons(soup)

    result = {
        "ticker": ticker,
        "company_name": company_name,
        "source_url": source_url,
        "ratios": parsed_ratios,
        "ratios_raw": ratios,
        "quarterly": quarterly,
        "quarterly_raw": quarterly_raw,
        "annual": annual,
        "annual_raw": annual_raw,
        "pros": pros_cons["pros"],
        "cons": pros_cons["cons"],
    }

    # Cache the result
    _set_cached_screener(ticker, result)
    logger.info(f"Screener data fetched for {ticker} from {source_url}")
    return result


# ─── 2. OM Screener — 14 Criteria Scoring ────────────────────────────────────

def _growth_pct(new, old):
    """Calculate growth percentage. Returns None if either value is invalid."""
    if new is None or old is None or old == 0:
        return None
    return (new - old) / abs(old) * 100


def compute_om_score(screener_data: dict) -> dict:
    """
    14-criteria O'Neil + Minervini scoring.
    Max weighted score = 124. Normalized to 0-100.
    """
    if "error" in screener_data:
        return {
            "score": 0, "score_pct": 0, "pass_count": 0, "total": 14,
            "grade": "N/A", "grade_color": "grey",
            "criteria": [], "summary": "No fundamental data available."
        }

    quarterly = screener_data.get("quarterly", [])
    annual = screener_data.get("annual", [])
    ratios = screener_data.get("ratios", {})

    criteria = []

    def _add(name, weight, passed, value_str="N/A"):
        criteria.append({
            "name": name, "weight": weight,
            "passed": bool(passed), "value": value_str,
        })

    # ── Criterion 1: Current Qtr EPS Growth ≥25% YoY ──
    c1_pass = False
    c1_val = "N/A"
    if len(quarterly) >= 5:
        curr_eps = quarterly[-1].get("eps")
        yoy_eps = quarterly[-5].get("eps") if len(quarterly) >= 5 else None
        g = _growth_pct(curr_eps, yoy_eps)
        if g is not None:
            c1_pass = g >= 25
            c1_val = f"{g:.1f}%"
    _add("Current Qtr EPS Growth ≥25% YoY", 15, c1_pass, c1_val)

    # ── Criterion 2: EPS Acceleration (QoQ trend rising) ──
    c2_pass = False
    c2_val = "N/A"
    if len(quarterly) >= 4:
        growths = []
        for i in range(-3, 0):
            prev = quarterly[i - 1].get("eps") if abs(i - 1) <= len(quarterly) else None
            curr = quarterly[i].get("eps")
            g = _growth_pct(curr, prev)
            if g is not None:
                growths.append(g)
        if len(growths) >= 2:
            c2_pass = growths[-1] > growths[0]
            c2_val = f"{growths[-1]:.1f}% vs {growths[0]:.1f}%"
    _add("EPS Acceleration (QoQ rising)", 12, c2_pass, c2_val)

    # ── Criterion 3: Annual EPS Growth ≥25% (3 yrs) ──
    c3_pass = False
    c3_val = "N/A"
    if len(annual) >= 4:
        ann_growths = []
        for i in range(-3, 0):
            prev = annual[i - 1].get("eps") if abs(i - 1) <= len(annual) else None
            curr = annual[i].get("eps")
            g = _growth_pct(curr, prev)
            if g is not None:
                ann_growths.append(g)
        if ann_growths:
            avg_g = sum(ann_growths) / len(ann_growths)
            c3_pass = avg_g >= 25
            c3_val = f"Avg {avg_g:.1f}%"
    _add("Annual EPS Growth ≥25% (3yr)", 15, c3_pass, c3_val)

    # ── Criterion 4: EPS Positive (no losses) ──
    c4_pass = False
    c4_val = "N/A"
    recent_eps = [q.get("eps") for q in quarterly[-4:] if q.get("eps") is not None]
    if recent_eps:
        c4_pass = all(e > 0 for e in recent_eps)
        c4_val = f"{sum(1 for e in recent_eps if e > 0)}/{len(recent_eps)} positive"
    _add("EPS Positive (no losses)", 8, c4_pass, c4_val)

    # ── Criterion 5: Current Qtr Revenue Growth ≥25% YoY ──
    c5_pass = False
    c5_val = "N/A"
    if len(quarterly) >= 5:
        curr_rev = quarterly[-1].get("sales")
        yoy_rev = quarterly[-5].get("sales") if len(quarterly) >= 5 else None
        g = _growth_pct(curr_rev, yoy_rev)
        if g is not None:
            c5_pass = g >= 25
            c5_val = f"{g:.1f}%"
    _add("Current Qtr Revenue Growth ≥25% YoY", 12, c5_pass, c5_val)

    # ── Criterion 6: Revenue Acceleration ──
    c6_pass = False
    c6_val = "N/A"
    if len(quarterly) >= 4:
        rev_growths = []
        for i in range(-3, 0):
            prev = quarterly[i - 1].get("sales") if abs(i - 1) <= len(quarterly) else None
            curr = quarterly[i].get("sales")
            g = _growth_pct(curr, prev)
            if g is not None:
                rev_growths.append(g)
        if len(rev_growths) >= 2:
            c6_pass = rev_growths[-1] > rev_growths[0]
            c6_val = f"{rev_growths[-1]:.1f}% vs {rev_growths[0]:.1f}%"
    _add("Revenue Acceleration", 8, c6_pass, c6_val)

    # ── Criterion 7: Revenue Confirms EPS ──
    c7_pass = False
    c7_val = "N/A"
    if len(quarterly) >= 5:
        curr_rev = quarterly[-1].get("sales")
        yoy_rev = quarterly[-5].get("sales") if len(quarterly) >= 5 else None
        curr_eps = quarterly[-1].get("eps")
        yoy_eps = quarterly[-5].get("eps") if len(quarterly) >= 5 else None
        rev_g = _growth_pct(curr_rev, yoy_rev)
        eps_g = _growth_pct(curr_eps, yoy_eps)
        if rev_g is not None and eps_g is not None:
            c7_pass = rev_g > 0 and eps_g > 0
            c7_val = f"Rev {rev_g:.0f}%, EPS {eps_g:.0f}%"
    _add("Revenue Confirms EPS", 8, c7_pass, c7_val)

    # ── Criterion 8: ROE ≥17% ──
    roe = ratios.get("roe")
    c8_pass = roe is not None and roe >= 17
    c8_val = f"{roe:.1f}%" if roe is not None else "N/A"
    _add("ROE ≥17%", 10, c8_pass, c8_val)

    # ── Criterion 9: Net Profit Margin Expanding ──
    c9_pass = False
    c9_val = "N/A"
    if len(quarterly) >= 2:
        def _npm(q):
            s = q.get("sales")
            np_ = q.get("net_profit")
            if s and s > 0 and np_ is not None:
                return np_ / s * 100
            return None
        npm_curr = _npm(quarterly[-1])
        npm_prev = _npm(quarterly[-2])
        if npm_curr is not None and npm_prev is not None:
            c9_pass = npm_curr > npm_prev
            c9_val = f"{npm_curr:.1f}% vs {npm_prev:.1f}%"
    _add("Net Profit Margin Expanding", 7, c9_pass, c9_val)

    # ── Criterion 10: Operating Margin Expanding ──
    c10_pass = False
    c10_val = "N/A"
    if len(quarterly) >= 2:
        opm_curr = quarterly[-1].get("opm")
        opm_prev = quarterly[-2].get("opm")
        if opm_curr is not None and opm_prev is not None:
            c10_pass = opm_curr > opm_prev
            c10_val = f"{opm_curr:.1f}% vs {opm_prev:.1f}%"
    _add("Operating Margin Expanding", 5, c10_pass, c10_val)

    # ── Criterion 11: Debt Under Control ──
    de = ratios.get("debt_to_equity")
    c11_pass = de is not None and de < 1.0
    c11_val = f"D/E {de:.2f}" if de is not None else "N/A"
    if de is None:
        c11_pass = True  # No debt info = assume OK
        c11_val = "No debt data"
    _add("Debt Under Control", 5, c11_pass, c11_val)

    # ── Criterion 12: EPS Positive Trend (3 of 4 Qtrs QoQ up) ──
    c12_pass = False
    c12_val = "N/A"
    if len(quarterly) >= 5:
        up_count = 0
        for i in range(-4, 0):
            curr = quarterly[i].get("eps")
            prev = quarterly[i - 1].get("eps") if abs(i - 1) <= len(quarterly) else None
            if curr is not None and prev is not None and curr > prev:
                up_count += 1
        c12_pass = up_count >= 3
        c12_val = f"{up_count}/4 quarters up"
    _add("EPS Positive Trend (3/4 QoQ)", 5, c12_pass, c12_val)

    # ── Criterion 13: Annual Revenue Growth ≥15% (3yr avg) ──
    c13_pass = False
    c13_val = "N/A"
    if len(annual) >= 4:
        ann_rev_growths = []
        for i in range(-3, 0):
            prev = annual[i - 1].get("sales") if abs(i - 1) <= len(annual) else None
            curr = annual[i].get("sales")
            g = _growth_pct(curr, prev)
            if g is not None:
                ann_rev_growths.append(g)
        if ann_rev_growths:
            avg = sum(ann_rev_growths) / len(ann_rev_growths)
            c13_pass = avg >= 15
            c13_val = f"Avg {avg:.1f}%"
    _add("Annual Revenue Growth ≥15% (3yr)", 8, c13_pass, c13_val)

    # ── Criterion 14: Profitability Improving (NP growing faster than Rev) ──
    c14_pass = False
    c14_val = "N/A"
    if len(annual) >= 2:
        rev_curr = annual[-1].get("sales")
        rev_prev = annual[-2].get("sales")
        np_curr = annual[-1].get("net_profit")
        np_prev = annual[-2].get("net_profit")
        rev_g = _growth_pct(rev_curr, rev_prev)
        np_g = _growth_pct(np_curr, np_prev)
        if rev_g is not None and np_g is not None:
            c14_pass = np_g > rev_g
            c14_val = f"NP {np_g:.0f}% vs Rev {rev_g:.0f}%"
    _add("Profitability Improving", 6, c14_pass, c14_val)

    # ── Scoring ──
    total_weight = sum(c["weight"] for c in criteria)  # 124
    earned = sum(c["weight"] for c in criteria if c["passed"])
    score_pct = round(earned / total_weight * 100) if total_weight > 0 else 0
    pass_count = sum(1 for c in criteria if c["passed"])

    if score_pct >= 80:
        grade, grade_color = "A+", "green"
    elif score_pct >= 65:
        grade, grade_color = "A", "green"
    elif score_pct >= 50:
        grade, grade_color = "B", "amber"
    elif score_pct >= 35:
        grade, grade_color = "C", "amber"
    else:
        grade, grade_color = "D", "red"

    # Summary
    company = screener_data.get("company_name", screener_data.get("ticker", ""))
    roe_str = f"ROE of {roe:.1f}%" if roe else "unknown ROE"
    summary = f"{company} passes {pass_count}/14 fundamental criteria (Grade {grade}). "
    if score_pct >= 65:
        summary += f"Strong fundamentals with {roe_str}. Earnings and revenue trends are positive."
    elif score_pct >= 45:
        summary += f"Mixed fundamentals with {roe_str}. Some growth criteria met, others need improvement."
    else:
        summary += f"Weak fundamentals with {roe_str}. Most growth criteria are not met."

    return {
        "score": earned,
        "score_pct": score_pct,
        "pass_count": pass_count,
        "total": 14,
        "max_score": total_weight,
        "grade": grade,
        "grade_color": grade_color,
        "criteria": criteria,
        "summary": summary,
    }


# ─── 3. Technicals Panel ─────────────────────────────────────────────────────

def _query_ohlcv(ticker: str) -> pd.DataFrame:
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    try:
        df = pd.read_sql_query(
            "SELECT date, open, high, low, close, volume FROM ohlcv "
            "WHERE ticker=? ORDER BY date ASC", conn, params=(ticker,))
    finally:
        conn.close()
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    return df.sort_values("date").reset_index(drop=True)


def _weinstein_stage(close, sma50, sma200, sma200_prev) -> tuple:
    """Classify Weinstein stage. Returns (stage_num, stage_label)."""
    price = close[-1]
    if price > sma50 and sma50 > sma200 and sma200 > sma200_prev:
        return 2, "Stage 2"
    elif price < sma50 and sma50 > sma200:
        return 3, "Stage 3"
    elif price < sma50 and price < sma200:
        return 4, "Stage 4"
    else:
        return 1, "Stage 1"


def _ad_rating(df: pd.DataFrame) -> tuple:
    """Accumulation/Distribution rating based on volume + price action."""
    if len(df) < 50:
        return "C", 50

    close = df["close"].values[-50:]
    vol = df["volume"].values[-50:]

    up_vol = 0
    down_vol = 0
    for i in range(1, len(close)):
        if close[i] > close[i - 1]:
            up_vol += vol[i]
        elif close[i] < close[i - 1]:
            down_vol += vol[i]

    total = up_vol + down_vol
    if total == 0:
        return "C", 50

    ratio = up_vol / total * 100

    if ratio >= 70:
        return "A+", ratio
    elif ratio >= 62:
        return "A", ratio
    elif ratio >= 55:
        return "B", ratio
    elif ratio >= 45:
        return "C", ratio
    else:
        return "D", ratio


def _pressure(df: pd.DataFrame) -> int:
    """Net up days vs down days in last 20 bars. Range: -10 to +10."""
    if len(df) < 20:
        return 0
    close = df["close"].values[-21:]
    up = sum(1 for i in range(1, len(close)) if close[i] > close[i - 1])
    down = sum(1 for i in range(1, len(close)) if close[i] < close[i - 1])
    return up - down


def _rs_rank(ticker: str, df: pd.DataFrame) -> tuple:
    """RS Rank 0-99 with letter grade."""
    if len(df) < 63:
        return 50, "C"

    close = df["close"].values
    perf = (close[-1] / close[-63] - 1) * 100 if close[-63] > 0 else 0

    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=15)
        all_df = pd.read_sql_query("""
            SELECT ticker, date, close FROM ohlcv
            WHERE market='India'
              AND date >= (SELECT date FROM ohlcv WHERE market='India' ORDER BY date DESC LIMIT 1 OFFSET 70)
            ORDER BY ticker, date ASC
        """, conn)
        conn.close()

        perfs = []
        for t, grp in all_df.groupby("ticker"):
            if len(grp) < 50:
                continue
            closes = grp["close"].values
            if closes[-1] > 0 and closes[0] > 0:
                p = (closes[-1] / closes[0] - 1) * 100
                perfs.append((t, p))

        if len(perfs) >= 10:
            perfs.sort(key=lambda x: x[1])
            total = len(perfs)
            rank = next((i for i, (t, _) in enumerate(perfs) if t == ticker), total // 2)
            rs = int(rank / total * 99)
        else:
            rs = max(0, min(99, int(50 + perf)))
    except Exception:
        rs = max(0, min(99, int(50 + perf)))

    if rs >= 90:
        grade = "A+"
    elif rs >= 80:
        grade = "A"
    elif rs >= 60:
        grade = "B"
    elif rs >= 40:
        grade = "C"
    else:
        grade = "D"

    return rs, grade


def _tpr_score(stage_num: int, rs_rank: int, pressure: int) -> int:
    """TPR = Trend + Price + RS composite. Returns 0-100."""
    stage_score = {1: 60, 2: 90, 3: 40, 4: 5}.get(stage_num, 50)
    pressure_norm = max(0, min(100, (pressure + 10) * 5))  # -10..+10 → 0..100
    tpr = int(stage_score * 0.4 + rs_rank * 0.35 + pressure_norm * 0.25)
    return max(0, min(100, tpr))


def compute_technicals(ticker: str) -> dict:
    """Compute technical metrics from DB OHLCV data."""
    ticker = ticker.upper().strip()
    df = _query_ohlcv(ticker)

    if df.empty:
        return {
            "ticker": ticker, "has_data": False,
            "stage_num": 0, "stage": "Unknown",
            "rs_rank": 0, "rs_grade": "N/A",
            "ad_rating": "N/A", "ad_pct": 0,
            "adr_pct": 0, "rel_volume": 0,
            "pressure": 0, "tpr": 0,
            "price": 0, "change_pct": 0,
            "tech_health": 0,
        }

    close = df["close"].values
    n = len(df)

    # Price info
    price = float(close[-1])
    prev = float(close[-2]) if n >= 2 else price
    change_pct = round((price - prev) / prev * 100, 2) if prev > 0 else 0

    # SMAs
    sma50 = float(np.mean(close[-50:])) if n >= 50 else price
    sma200 = float(np.mean(close[-200:])) if n >= 200 else price
    sma200_prev = float(np.mean(close[-220:-20])) if n >= 220 else sma200

    # Stage
    stage_num, stage_label = _weinstein_stage(close, sma50, sma200, sma200_prev)

    # RS Rank
    rs, rs_grade = _rs_rank(ticker, df)

    # A/D Rating
    ad, ad_pct = _ad_rating(df)

    # ADR% (ATR/Close * 100)
    adr_pct = 0
    if n >= 15:
        high = df["high"].values[-15:]
        low = df["low"].values[-15:]
        cls = df["close"].values[-15:]
        prev_c = np.roll(cls, 1)
        prev_c[0] = cls[0]
        tr = np.maximum(high - low, np.maximum(np.abs(high - prev_c), np.abs(low - prev_c)))
        adr_pct = round(float(np.mean(tr[-14:])) / price * 100, 2) if price > 0 else 0

    # RelVolume
    rel_vol = 0.0
    if n >= 50:
        vol = df["volume"].values
        avg_vol = np.mean(vol[-50:])
        if avg_vol > 0:
            rel_vol = round(float(vol[-1] / avg_vol), 2)

    # Pressure
    press = _pressure(df)

    # TPR
    tpr = _tpr_score(stage_num, rs, press)

    # Tech health score (0-100) based on MA structure
    tech_health = 0
    if price > sma50:
        tech_health += 25
    if price > sma200:
        tech_health += 25
    if sma50 > sma200:
        tech_health += 20
    if sma200 > sma200_prev:
        tech_health += 15
    if rel_vol > 1.0:
        tech_health += 10
    if press > 0:
        tech_health += 5
    tech_health = min(100, tech_health)

    return {
        "ticker": ticker,
        "has_data": True,
        "price": _py(price),
        "change_pct": _py(change_pct),
        "stage_num": _py(stage_num),
        "stage": stage_label,
        "rs_rank": _py(rs),
        "rs_grade": rs_grade,
        "ad_rating": ad,
        "ad_pct": _py(round(ad_pct, 1)),
        "adr_pct": _py(adr_pct),
        "rel_volume": _py(rel_vol),
        "pressure": _py(press),
        "tpr": _py(tpr),
        "tech_health": _py(tech_health),
    }


# ─── 4. Smart Score Composite ────────────────────────────────────────────────

def compute_smart_score(om_score: dict, technicals: dict) -> dict:
    """
    Composite Smart Score.
    Formula: fund(35%) + tech(25%) + rs(20%) + stage(12%) + tpr(8%)
    """
    fund = om_score.get("score_pct", 0)
    tech = technicals.get("tech_health", 0)
    rs = technicals.get("rs_rank", 0)
    stage_num = technicals.get("stage_num", 1)
    stage_map = {1: 60, 2: 90, 3: 40, 4: 5}
    stage_score = stage_map.get(stage_num, 50)
    tpr = technicals.get("tpr", 0)

    smart = round(fund * 0.35 + tech * 0.25 + rs * 0.20 + stage_score * 0.12 + tpr * 0.08)
    smart = max(0, min(100, smart))

    if smart >= 70:
        verdict, verdict_color = "Strong", "green"
    elif smart >= 50:
        verdict, verdict_color = "Good", "amber"
    else:
        verdict, verdict_color = "Avoid", "red"

    # Component breakdown
    components = {
        "fund": {"score": fund, "weight": 35, "label": "Fundamentals"},
        "tech": {"score": tech, "weight": 25, "label": "Technicals"},
        "rs":   {"score": rs,   "weight": 20, "label": "RS Rank"},
        "stage":{"score": stage_score, "weight": 12, "label": "Stage"},
        "tpr":  {"score": tpr,  "weight": 8,  "label": "TPR"},
    }

    # Tags
    tags = []
    stage_label = technicals.get("stage", "")
    if stage_num == 3:
        tags.append("Stage 3 Caution")
    elif stage_num == 2:
        tags.append("Stage 2 Uptrend")
    elif stage_num == 4:
        tags.append("Stage 4 Decline")

    if fund >= 65:
        tags.append("Strong Fundamentals")
    elif fund < 35:
        tags.append("Weak Fundamentals")

    # Check specific criteria
    for c in om_score.get("criteria", []):
        if c["name"].startswith("ROE") and c["passed"]:
            tags.append("High ROE")
        if "Revenue Growth" in c["name"] and not c["passed"] and "Current Qtr" in c["name"]:
            tags.append("Revenue Slowdown")
        if "EPS Growth" in c["name"] and not c["passed"] and "Current Qtr" in c["name"]:
            tags.append("EPS Slowdown")

    if rs >= 80:
        tags.append("RS Leader")
    elif rs < 30:
        tags.append("RS Laggard")

    # Sector tag from screener data
    # (will be added by get_smart_metrics if available)

    # AI Insight (rule-based text generation)
    om_grade = om_score.get("grade", "N/A")
    pass_count = om_score.get("pass_count", 0)

    insight = ""
    if fund >= 65:
        insight += f"Fundamentals are strong (Grade {om_grade}, {pass_count}/14 criteria met). "
    elif fund >= 45:
        insight += f"Fundamentals are mixed (Grade {om_grade}, {pass_count}/14 criteria met). "
    else:
        insight += f"Fundamentals are weak (Grade {om_grade}, {pass_count}/14 criteria met). "

    if stage_num == 2:
        insight += "The stock is in a Stage 2 uptrend — the ideal technical setup for growth stocks. "
    elif stage_num == 1:
        insight += "The stock is basing in Stage 1 — building a potential launchpad. "
    elif stage_num == 3:
        insight += "The stock is in Stage 3 topping — caution warranted as momentum fades. "
    elif stage_num == 4:
        insight += "The stock is in Stage 4 decline — avoid until structure improves. "

    if rs >= 80:
        insight += f"RS Rank of {rs} places it among the strongest performers. "
    elif rs < 30:
        insight += f"RS Rank of {rs} indicates significant underperformance vs peers. "

    if smart >= 70:
        insight += f"Overall Smart Score of {smart} suggests a strong techno-fundamental setup."
    elif smart >= 50:
        insight += f"Overall Smart Score of {smart} indicates a decent but not ideal setup."
    else:
        insight += f"Overall Smart Score of {smart} suggests this stock should be avoided for now."

    return {
        "score": smart,
        "verdict": verdict,
        "verdict_color": verdict_color,
        "components": components,
        "tags": tags[:8],
        "insight": insight,
    }


# ─── 5. Main Entry Point ─────────────────────────────────────────────────────

def get_smart_metrics(ticker: str) -> dict:
    """
    Main entry — combines screener scraping, OM scoring, technicals, and Smart Score.
    """
    ticker = ticker.upper().strip()
    t0 = time.time()

    # 1. Fetch screener.in fundamentals
    screener_data = fetch_screener_data(ticker)

    # 2. Compute OM Score
    om = compute_om_score(screener_data)

    # 3. Compute Technicals from DB
    tech = compute_technicals(ticker)

    # 4. Compute Smart Score
    smart = compute_smart_score(om, tech)

    # Add sector tag if available
    sector = ""
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=10)
        row = conn.execute("SELECT sector FROM sector_map WHERE ticker=?", (ticker,)).fetchone()
        conn.close()
        if row:
            sector = row[0]
            # Add short sector name as tag
            short_sector = sector.split("&")[0].strip() if "&" in sector else sector
            if short_sector and short_sector not in smart["tags"]:
                smart["tags"].append(short_sector)
    except Exception:
        pass

    # Add mcap info
    mcap_info = {}
    try:
        from market_cap import get_mcap_for_ticker, format_mcap
        mcap_data = get_mcap_for_ticker(ticker)
        if mcap_data:
            mcap_info = {
                "mcap_cr": mcap_data["mcap_cr"],
                "mcap_tier": mcap_data["mcap_tier"],
                "mcap_formatted": format_mcap(mcap_data["mcap_cr"]),
            }
    except Exception:
        pass

    elapsed = round(time.time() - t0, 2)

    return {
        "ticker": ticker,
        "company_name": screener_data.get("company_name", ticker),
        "sector": sector,
        "mcap": mcap_info,
        "screener": {
            "ratios": screener_data.get("ratios", {}),
            "pros": screener_data.get("pros", []),
            "cons": screener_data.get("cons", []),
            "has_data": "error" not in screener_data,
            "source_url": screener_data.get("source_url", ""),
        },
        "om_score": om,
        "technicals": tech,
        "smart_score": smart,
        "elapsed": elapsed,
    }


# ══════════════════════════════════════════════════════════════════════════════
# SMART SCREENER — Run SMART Score across entire NIFTY universe
# Two-pass: RS+Stage pre-filter → SMART score on candidates only
# ══════════════════════════════════════════════════════════════════════════════

import threading
_smart_screener_cache: dict = {}
_smart_screener_lock = threading.Lock()

SMART_SCREENER_CACHE_TTL = 14400  # 4 hours


def run_smart_screener(
    min_smart: int = 70,
    min_rs: int = 60,
    require_stage2: bool = True,
    min_mcap_cr: float = 500,
    market: str = "India",
    progress_state: dict = None,
) -> dict:
    """
    Two-pass SMART screener across the full NIFTY universe.

    Pass 1 — Instant pre-filter (uses already-computed RS rankings data):
        RS ≥ min_rs + Stage 2 (optional) + mcap filter
        Reduces ~900 stocks → ~30-80 candidates

    Pass 2 — SMART score each candidate (scrapes screener.in fundamentals):
        compute_om_score + compute_technicals + compute_smart_score
        Filter by smart ≥ min_smart

    Returns: list of stocks with full SMART breakdown, sorted by score.
    """
    import time as _time
    t0 = _time.time()

    # ── Cache key ──────────────────────────────────────────────────────────────
    cache_k = f"smart_scr_{market}_{min_smart}_{min_rs}_{require_stage2}_{min_mcap_cr}"
    with _smart_screener_lock:
        if cache_k in _smart_screener_cache:
            entry = _smart_screener_cache[cache_k]
            age = _time.time() - entry["ts"]
            if age < SMART_SCREENER_CACHE_TTL:
                return {**entry["data"], "cached": True, "cache_age_min": round(age/60)}

    # ── Pass 1: Pre-filter from OHLCV + RS data ────────────────────────────────
    if progress_state:
        progress_state["message"] = "Pass 1: Pre-filtering universe..."
        progress_state["progress"] = 0
        progress_state["total"] = 100

    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=30)

        # Get all tickers with sufficient OHLCV data
        tickers_with_data = [
            r[0] for r in conn.execute("""
                SELECT DISTINCT ticker FROM ohlcv
                WHERE market = 'India'
                GROUP BY ticker HAVING COUNT(*) >= 60
                ORDER BY ticker
            """).fetchall()
        ]
        conn.close()
    except Exception as e:
        return {"error": f"DB read failed: {e}", "stocks": [], "total": 0}

    if not tickers_with_data:
        return {"error": "No OHLCV data found — run a sync first", "stocks": [], "total": 0}

    # ── Compute lightweight pre-filter metrics for all tickers ─────────────────
    candidates = []
    total_tickers = len(tickers_with_data)

    if progress_state:
        progress_state["message"] = f"Pass 1: Scanning {total_tickers} tickers..."
        progress_state["total"] = total_tickers

    conn = sqlite3.connect(str(DB_PATH), timeout=30)

    for i, ticker in enumerate(tickers_with_data):
        if progress_state and i % 50 == 0:
            progress_state["progress"] = i
            progress_state["message"] = f"Pass 1: {i}/{total_tickers} scanned, {len(candidates)} candidates so far..."

        try:
            rows = conn.execute("""
                SELECT date, close, high, low, volume
                FROM ohlcv WHERE ticker=? AND market='India'
                ORDER BY date DESC LIMIT 260
            """, (ticker,)).fetchall()

            if len(rows) < 60:
                continue

            closes  = [r[1] for r in reversed(rows)]
            highs   = [r[2] for r in reversed(rows)]
            lows    = [r[3] for r in reversed(rows)]
            vols    = [r[4] for r in reversed(rows)]

            price   = closes[-1]
            if not price or price <= 0:
                continue

            # MA checks
            ma50  = sum(closes[-50:]) / 50 if len(closes) >= 50 else None
            ma150 = sum(closes[-150:]) / 150 if len(closes) >= 150 else None
            ma200 = sum(closes[-200:]) / 200 if len(closes) >= 200 else None

            # Stage 2 check: price > ma50 > ma150 > ma200
            is_stage2 = False
            if ma50 and ma150 and ma200:
                is_stage2 = (price > ma50 > ma150 > ma200)

            if require_stage2 and not is_stage2:
                continue

            # Quick RS proxy (3M + 6M momentum vs simple benchmark)
            roc_3m = ((closes[-1] / closes[-63]) - 1) * 100 if len(closes) >= 63 else 0
            roc_6m = ((closes[-1] / closes[-126]) - 1) * 100 if len(closes) >= 126 else 0
            rs_proxy = round(roc_3m * 0.6 + roc_6m * 0.4)

            if rs_proxy < (min_rs - 40):  # loose pre-filter
                continue

            # Volume check — avg vol > 100K (liquidity)
            avg_vol = sum(vols[-20:]) / 20 if len(vols) >= 20 else 0
            if avg_vol < 10000:
                continue

            # pct from high
            hi52 = max(highs[-252:]) if len(highs) >= 252 else max(highs)
            pct_from_high = round((price - hi52) / hi52 * 100, 1) if hi52 > 0 else -99

            candidates.append({
                "ticker":         ticker,
                "price":          round(price, 2),
                "is_stage2":      is_stage2,
                "rs_proxy":       rs_proxy,
                "pct_from_high":  pct_from_high,
                "avg_vol":        int(avg_vol),
            })

        except Exception:
            continue

    conn.close()

    if not candidates:
        return {
            "stocks": [], "total": 0,
            "message": "No candidates passed pre-filter. Try lowering thresholds.",
            "elapsed": round(_time.time() - t0, 2),
        }

    # Sort candidates by rs_proxy desc — best first
    candidates.sort(key=lambda x: x["rs_proxy"], reverse=True)

    # Cap at 120 candidates for Pass 2 (screener.in rate limits)
    candidates = candidates[:120]

    # ── Pass 2: Full SMART score on candidates ─────────────────────────────────
    if progress_state:
        progress_state["message"] = (
            f"Pass 2: Computing SMART score for {len(candidates)} candidates..."
        )
        progress_state["total"]    = len(candidates)
        progress_state["progress"] = 0

    results = []
    from market_cap import get_mcap_for_ticker, format_mcap

    for i, cand in enumerate(candidates):
        ticker = cand["ticker"]

        if progress_state:
            progress_state["progress"] = i + 1
            progress_state["message"]  = (
                f"Pass 2: Scoring {ticker} ({i+1}/{len(candidates)})..."
            )

        try:
            # Fundamentals from screener.in (cached 24h)
            screener_data = fetch_screener_data(ticker)
            om   = compute_om_score(screener_data)
            tech = compute_technicals(ticker)
            smart = compute_smart_score(om, tech)
            score = smart["score"]

            if score < min_smart:
                continue

            # mcap filter
            mcap_data = get_mcap_for_ticker(ticker) or {}
            mcap_cr   = mcap_data.get("mcap_cr", 0) or 0
            if min_mcap_cr > 0 and mcap_cr < min_mcap_cr:
                continue

            results.append({
                "ticker":         ticker,
                "company":        screener_data.get("company_name", ticker),
                "price":          cand["price"],
                "smart_score":    score,
                "verdict":        smart["verdict"],
                "verdict_color":  smart["verdict_color"],
                # Component scores
                "fund_score":     smart["components"]["fund"]["score"],
                "tech_score":     smart["components"]["tech"]["score"],
                "rs_score":       smart["components"]["rs"]["score"],
                "stage_score":    smart["components"]["stage"]["score"],
                "tpr_score":      smart["components"]["tpr"]["score"],
                # Technicals
                "stage":          tech.get("stage", ""),
                "stage_num":      tech.get("stage_num", 0),
                "rs_rank":        tech.get("rs_rank", 0),
                "ad_rating":      tech.get("ad_rating", "N/A"),
                "tpr":            tech.get("tpr", 0),
                "pct_from_high":  cand["pct_from_high"],
                # OM Grade
                "om_grade":       om.get("grade", "N/A"),
                "om_pass_count":  om.get("pass_count", 0),
                # mcap
                "mcap_cr":        mcap_cr,
                "mcap_tier":      mcap_data.get("mcap_tier", ""),
                "mcap_fmt":       format_mcap(mcap_cr),
                # Tags
                "tags":           smart.get("tags", []),
                "sector":         screener_data.get("sector", ""),
            })

        except Exception as e:
            logger.debug(f"SMART score failed for {ticker}: {e}")
            continue

        # Polite delay between screener.in scrapes
        _time.sleep(0.4)

    # Sort by smart score descending
    results.sort(key=lambda x: x["smart_score"], reverse=True)

    elapsed = round(_time.time() - t0, 2)
    total_screened = len(candidates)

    result = {
        "stocks":          results,
        "total":           len(results),
        "screened":        total_screened,
        "pre_filter_total": len(tickers_with_data),
        "min_smart":       min_smart,
        "min_rs":          min_rs,
        "require_stage2":  require_stage2,
        "elapsed":         elapsed,
        "cached":          False,
        "message": (
            f"✅ {len(results)} stocks with SMART ≥{min_smart} "
            f"from {total_screened} candidates ({len(tickers_with_data)} universe)"
        ),
    }

    # Cache result
    with _smart_screener_lock:
        _smart_screener_cache[cache_k] = {
            "data": result,
            "ts":   _time.time(),
        }

    if progress_state:
        progress_state["message"]  = result["message"]
        progress_state["running"]  = False

    return result
