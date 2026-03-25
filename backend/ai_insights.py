"""
AI Insights Service — Groq (Qwen 3 32B)
========================================
Two AI-powered features:
  1. Market Intelligence  — Q-BRAM regime narrative for Overview tab
  2. Stock Analysis       — Per-stock setup quality for Smart Metrics tab

Primary:  Groq qwen-qwq-32b (reasoning model)
Fallback: groq llama-3.3-70b-versatile
"""

import logging
import json
import time
from datetime import datetime, timezone
from pathlib import Path
import sqlite3

logger = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────
GROQ_API_URL  = "https://api.groq.com/openai/v1/chat/completions"
# qwen-qwq-32b decommissioned — use llama as primary (confirmed working)
# Update PRIMARY_MODEL when Groq releases new Qwen version
PRIMARY_MODEL = "llama-3.3-70b-versatile"    # Fast, capable, confirmed on Groq free
FALLBACK_MODEL = "llama3-70b-8192"            # Older fallback if above rate-limited

DB_PATH = Path(__file__).parent / "breadth_data.db"

# ── Simple in-memory cache (avoid re-calling same inputs) ─────────────────────
_insight_cache: dict = {}
CACHE_TTL = 3600  # 1 hour for market insights, 6h for stock


def _get_api_key() -> str:
    """Load Groq API key from DB settings table."""
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=10)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS app_settings (
                key   TEXT PRIMARY KEY,
                value TEXT,
                updated TEXT
            )
        """)
        row = conn.execute(
            "SELECT value FROM app_settings WHERE key='groq_api_key'"
        ).fetchone()
        conn.close()
        return row[0].strip() if row and row[0] else ""
    except Exception as e:
        logger.warning(f"Could not load API key: {e}")
        return ""


def save_api_key(api_key: str) -> bool:
    """Save Groq API key to DB."""
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=10)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS app_settings (
                key TEXT PRIMARY KEY, value TEXT, updated TEXT
            )
        """)
        conn.execute("""
            INSERT INTO app_settings (key, value, updated)
            VALUES ('groq_api_key', ?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated=excluded.updated
        """, (api_key.strip(), datetime.now(timezone.utc).isoformat()))
        conn.commit()
        conn.close()
        logger.info("✅ Groq API key saved")
        return True
    except Exception as e:
        logger.error(f"Failed to save API key: {e}")
        return False


def _call_groq(prompt: str, system: str, api_key: str,
               max_tokens: int = 600, model: str = PRIMARY_MODEL) -> str:
    """Call Groq API. Returns response text or raises exception."""
    import httpx

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user",   "content": prompt},
        ],
        "max_tokens":   max_tokens,
        "temperature":  0.3,   # low temp = consistent, analytical output
        "stream":       False,
    }

    resp = httpx.post(
        GROQ_API_URL, headers=headers,
        json=payload, timeout=30
    )

    if resp.status_code == 429:
        raise Exception("Rate limited — try again in a moment")
    if resp.status_code == 401:
        raise Exception("Invalid API key")
    if resp.status_code != 200:
        raise Exception(f"Groq API error {resp.status_code}: {resp.text[:200]}")

    data = resp.json()
    content = data["choices"][0]["message"]["content"]

    # Strip any thinking blocks (some models wrap output in <think>...</think>)
    if "<think>" in content:
        import re
        content = re.sub(r"<think>.*?</think>", "", content, flags=re.DOTALL).strip()

    return content.strip()


def _call_groq_with_fallback(prompt: str, system: str, api_key: str,
                              max_tokens: int = 600) -> str:
    """Try primary model, fall back to llama if needed."""
    try:
        return _call_groq(prompt, system, api_key, max_tokens, PRIMARY_MODEL)
    except Exception as e:
        logger.warning(f"Primary model failed ({e}), trying fallback...")
        try:
            return _call_groq(prompt, system, api_key, max_tokens, FALLBACK_MODEL)
        except Exception as e2:
            raise Exception(f"Both models failed: {e2}")


# ══════════════════════════════════════════════════════════════════════════════
# FEATURE 1 — MARKET INTELLIGENCE (Overview Tab)
# ══════════════════════════════════════════════════════════════════════════════

MARKET_SYSTEM = """You are Q-BRAM Intelligence, an expert market breadth analyst 
specialising in Indian equities (NIFTY 500). You analyse quantitative breadth 
signals and provide concise, actionable trading insights for swing traders.

Your analysis style:
- Direct and data-driven (no fluff)
- Reference specific numbers from the data
- Use Minervini/O'Neil/Qullamaggie framework language
- Always end with a clear 1-line trading bias

Output format — exactly 4 sections:
1. REGIME ASSESSMENT (1-2 sentences)
2. BREADTH ANALYSIS (2-3 sentences with specific numbers)  
3. SECTOR INTELLIGENCE (1-2 sentences on leading/lagging sectors)
4. TRADING BIAS (1 clear sentence: what to do now)

Keep total response under 200 words. Be specific, not generic."""


def get_market_intelligence(breadth_data: dict) -> dict:
    """
    Generate AI market intelligence for Overview tab.
    breadth_data: the full Q-BRAM breadth dict from /api/breadth/INDIA
    """
    api_key = _get_api_key()
    if not api_key:
        return {"error": "no_api_key", "message": "Add Groq API key in Settings"}

    # Build cache key from key metrics
    score   = breadth_data.get("score", 0)
    regime  = breadth_data.get("regime", "")
    b50     = breadth_data.get("pct_above_50", 0)
    cache_k = f"market_{regime}_{int(score)}_{int(b50)}"

    if cache_k in _insight_cache:
        cached = _insight_cache[cache_k]
        age = (datetime.now(timezone.utc).timestamp() -
               cached["ts"])
        if age < CACHE_TTL:
            return {**cached["data"], "cached": True}

    # Build prompt from breadth data
    b200    = breadth_data.get("pct_above_200", 0)
    b20     = breadth_data.get("pct_above_20",  0)
    nh_nl   = breadth_data.get("nh_nl",  0)
    ad_roc  = breadth_data.get("ad_roc",  0)
    adv     = breadth_data.get("advancers", 0)
    dec     = breadth_data.get("decliners", 0)
    vix     = breadth_data.get("vix", 0)
    div     = breadth_data.get("divergence")

    # Sector breadth summary (top 3 + bottom 3)
    sector_breadth = breadth_data.get("sector_breadth", [])
    top_sectors    = sorted(sector_breadth,
                            key=lambda x: x.get("pct_above_50", 0),
                            reverse=True)[:3]
    bot_sectors    = sorted(sector_breadth,
                            key=lambda x: x.get("pct_above_50", 0))[:3]

    top_str = ", ".join(
        f"{s['sector']} ({s.get('pct_above_50',0):.0f}%)"
        for s in top_sectors if s.get("sector")
    )
    bot_str = ", ".join(
        f"{s['sector']} ({s.get('pct_above_50',0):.0f}%)"
        for s in bot_sectors if s.get("sector")
    )

    div_str = ""
    if div:
        div_str = f"\nDivergence Alert: {div.get('type','')} — {div.get('message','')}"

    prompt = f"""Current NIFTY 500 Market Breadth Data ({datetime.now().strftime('%d %b %Y')}):

Q-BRAM Score:      {score}/100
Regime:            {regime}
% Above 50 DMA:    {b50:.1f}%
% Above 200 DMA:   {b200:.1f}%  
% Above 20 DMA:    {b20:.1f}%
New Highs - Lows:  {nh_nl:+.0f}
A/D ROC (5d):      {ad_roc:+.1f}%
Advancers/Decliners: {adv}/{dec}
VIX India:         {vix:.1f}
Leading Sectors:   {top_str or 'N/A'}
Lagging Sectors:   {bot_str or 'N/A'}{div_str}

Provide your Q-BRAM market intelligence analysis."""

    try:
        t0   = time.time()
        text = _call_groq_with_fallback(prompt, MARKET_SYSTEM, api_key,
                                        max_tokens=400)
        elapsed = round(time.time() - t0, 2)

        result = {
            "text":    text,
            "score":   score,
            "regime":  regime,
            "elapsed": elapsed,
            "model":   PRIMARY_MODEL,
            "cached":  False,
            "ts":      datetime.now(timezone.utc).isoformat(),
        }

        _insight_cache[cache_k] = {
            "data": result,
            "ts":   datetime.now(timezone.utc).timestamp(),
        }
        return result

    except Exception as e:
        logger.error(f"Market intelligence failed: {e}")
        return {"error": str(e), "text": None}


# ══════════════════════════════════════════════════════════════════════════════
# FEATURE 2 — STOCK ANALYSIS (Smart Metrics Tab)
# ══════════════════════════════════════════════════════════════════════════════

STOCK_SYSTEM = """You are a professional swing trader using Minervini SEPA, 
O'Neil CAN SLIM, and Qullamaggie momentum frameworks for Indian equities (NSE).

Analyse the given stock metrics and provide a setup quality assessment.

Output format — exactly 5 sections:
1. SETUP QUALITY: [A+ / A / B+ / B / C / D] — one line explanation
2. STAGE ANALYSIS: Current Weinstein stage and trend strength (1-2 sentences)
3. STRENGTHS: 2-3 bullet points of what's working
4. RISKS: 1-2 bullet points of what could go wrong  
5. TRADER'S NOTE: 1 sentence — specific actionable observation

Keep total response under 180 words. Use exact numbers from the data.
Do NOT give buy/sell advice. Provide analytical observations only."""


def get_stock_analysis(ticker: str, metrics: dict) -> dict:
    """
    Generate AI stock analysis for Smart Metrics tab.
    metrics: the full smart_metrics dict for the ticker
    """
    api_key = _get_api_key()
    if not api_key:
        return {"error": "no_api_key", "message": "Add Groq API key in Settings"}

    # Cache per ticker — 6 hour TTL
    cache_k = f"stock_{ticker}_{metrics.get('rs_rating', 0)}"
    if cache_k in _insight_cache:
        cached = _insight_cache[cache_k]
        if (datetime.now(timezone.utc).timestamp() - cached["ts"]) < 21600:
            return {**cached["data"], "cached": True}

    # Extract key metrics safely
    def g(key, default="N/A"):
        v = metrics.get(key)
        return v if v not in (None, "", 0) else default

    prompt = f"""Stock: {ticker} (NSE India)
Date: {datetime.now().strftime('%d %b %Y')}

PRICE & PERFORMANCE:
Price:           ₹{g('price')}
1W Change:       {g('chg_1w')}%
1M Change:       {g('chg_1m')}%  
3M Change:       {g('chg_3m')}%
From 52W High:   {g('pct_from_high')}%
From 52W Low:    {g('pct_from_low')}%

RELATIVE STRENGTH:
RS Rating:       {g('rs_rating')} / 99
RS Trend:        {g('rs_trend')} (rising/falling vs NIFTY 500)
A/D Rating:      {g('ad_rating')} (Accum/Distrib)

TREND TEMPLATE (Minervini):
Trend Template:  {g('trend_template')} (all 8 conditions met?)
TT Score:        {g('trend_score_tt')} / 8 conditions
Stage:           {g('stage', 'Unknown')}

VOLUME:
Vol Ratio:       {g('vol_ratio')}x (vs 50-day avg)

SECTOR:
Sector:          {g('sector')}
Mcap Tier:       {g('mcap_tier')}

SMART SCORE:     {g('smart_score', 'N/A')} / 100

Provide your swing trader analysis of {ticker}."""

    try:
        t0   = time.time()
        text = _call_groq_with_fallback(prompt, STOCK_SYSTEM, api_key,
                                        max_tokens=350)
        elapsed = round(time.time() - t0, 2)

        result = {
            "ticker":  ticker,
            "text":    text,
            "elapsed": elapsed,
            "model":   PRIMARY_MODEL,
            "cached":  False,
            "ts":      datetime.now(timezone.utc).isoformat(),
        }

        _insight_cache[cache_k] = {
            "data": result,
            "ts":   datetime.now(timezone.utc).timestamp(),
        }
        return result

    except Exception as e:
        logger.error(f"Stock analysis failed for {ticker}: {e}")
        return {"error": str(e), "ticker": ticker, "text": None}


# ══════════════════════════════════════════════════════════════════════════════
# VALIDATE KEY
# ══════════════════════════════════════════════════════════════════════════════
def validate_api_key(api_key: str) -> dict:
    """Test if API key is valid with a minimal call."""
    try:
        result = _call_groq(
            prompt="Reply with exactly: OK",
            system="You are a test assistant. Reply with exactly: OK",
            api_key=api_key,
            max_tokens=5,
            model=FALLBACK_MODEL,   # use faster model for validation
        )
        return {"valid": True, "model": FALLBACK_MODEL, "response": result}
    except Exception as e:
        return {"valid": False, "error": str(e)}
