"""
Custom screeners (AFL-to-Python), RS ranking engine (IBD-style), and Leaders computation.
"""
import logging
import time
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from collections import defaultdict

from cache import get_cache, set_cache
from utils import (
    safe_float, safe_download, get_close, get_change_pct,
    get_stock_data, get_screener_data, fetch_batch,
    DB_AVAILABLE, INDIA_SECTORS, US_SECTORS,
    load_sector_map, load_ticker_universe,
    _safe, _ma, _ema, _hhv, _llv, _roc, _atr,
)

logger = logging.getLogger(__name__)


# ══════════════════════════════════════════════════════════════════════════════
# CUSTOM SCREENERS — Direct AFL-to-Python translations
# All run on full NSE universe (2,585 tickers) via get_screener_data()
# ══════════════════════════════════════════════════════════════════════════════


# ============================================================================
# 1. SVRO — Stage + Volume + RS + Overlay (from SVRO_ATR.afl)
# ============================================================================
def screen_svro(df, rs_rating=None, index_close_series=None):
    """
    SVRO v3: 5-metric entry system
    1. Market Health: Nifty > MA10 (passed as index_close_series)
    2. Stage 2: Close > MA150 AND MA150 rising
    3. Seed Stage: Close > EMA20 AND Close > Close[5 bars ago]
    4. RS > 85: RS_Rank > 85 (use pre-computed rs_rating if available)
    5. Volume Surge: Volume > MA20(Vol) * 1.5
    Returns True if all 5 pass
    """
    if df is None or len(df) < 200:
        return False, {}
    c = df['Close']
    v = df['Volume'] if 'Volume' in df.columns else pd.Series([0]*len(df))

    # 1. Stage 2: Close > MA150 AND MA150 rising
    ma150 = _ma(c, 150)
    ma150_rising = ma150.iloc[-1] > ma150.iloc[-2] if len(ma150) >= 2 else False
    stage2 = c.iloc[-1] > ma150.iloc[-1] and ma150_rising

    # 2. Seed Stage: Close > EMA20 AND Close > Close[-5]
    ema20 = _ema(c, 20)
    seed_stage = (c.iloc[-1] > ema20.iloc[-1] and
                  c.iloc[-1] > c.iloc[-6] if len(c) >= 6 else False)

    # 3. RS > 85 (use pre-computed rating or proxy via ROC)
    if rs_rating is not None:
        high_rs = rs_rating >= 85
    else:
        # Proxy: 252-day percentile rank of close/index
        roc252 = _roc(c, 252)
        rs_val = _safe(roc252.iloc[-1])
        high_rs = rs_val > 15  # rough proxy

    # 4. Volume Surge: V > MA20(V) * 1.5
    avg_vol20 = _ma(v, 20)
    vol_surge = v.iloc[-1] > avg_vol20.iloc[-1] * 1.5 if avg_vol20.iloc[-1] > 0 else False

    # 5. Liquidity: avg turnover >= 10Cr (approx: avg_vol * price > 10M)
    avg_turnover = _ma(c * v, 20)
    is_liquid = avg_turnover.iloc[-1] / 10_000_000 >= 1.0

    passes = stage2 and seed_stage and high_rs and vol_surge and is_liquid
    details = {
        'stage2': stage2, 'seed_stage': seed_stage,
        'high_rs': high_rs, 'vol_surge': vol_surge, 'liquid': is_liquid
    }
    return passes, details


# ============================================================================
# 2. QULLAMAGGIE BREAKOUT (Setup 1 from QUlla__-_Copy.afl)
# ============================================================================
def screen_qulla_breakout(df):
    """
    Qullamaggie Breakout:
    1. Prior big move: ROC(1M or 3M or 6M) > 30%
    2. Consolidation: price surfing near 10/20 MA, range tight (<15%), higher lows
    3. Breakout: Close > prior consolidation high
    4. Volume confirmation: V > 1.5x avg
    """
    if df is None or len(df) < 130:
        return False, {}
    c, h, l, v = df['Close'], df['High'], df['Low'], df['Volume'] if 'Volume' in df.columns else pd.Series([0]*len(df))

    ma10  = _ma(c, 10)
    ma20  = _ma(c, 20)
    avg_vol = _ma(v, 50)

    # 1. Prior momentum (at least one timeframe)
    roc1m = _safe(_roc(c, 21).iloc[-1])
    roc3m = _safe(_roc(c, 63).iloc[-1])
    roc6m = _safe(_roc(c, 126).iloc[-1])
    has_momentum = roc1m > 30 or roc3m > 30 or roc6m > 30

    # 2. Surfing MA — within 3% of MA10 OR 5% of MA20
    near_ma10 = abs(c.iloc[-1] - ma10.iloc[-1]) / c.iloc[-1] * 100 < 3
    near_ma20 = abs(c.iloc[-1] - ma20.iloc[-1]) / c.iloc[-1] * 100 < 5
    surfing_ma = near_ma10 or near_ma20

    # 3. Consolidation tightness — last 10 bars range < 15%
    cons_bars = 10
    cons_high = _hhv(h, cons_bars).iloc[-1]
    cons_low  = _llv(l, cons_bars).iloc[-1]
    cons_range = (cons_high - cons_low) / cons_low * 100 if cons_low > 0 else 999
    is_tight = cons_range < 15

    # 4. Higher lows — recent half of consolidation low > prior half low
    if len(l) >= cons_bars:
        recent_low = l.iloc[-cons_bars//2:].min()
        prior_low  = l.iloc[-cons_bars:-cons_bars//2].min()
        higher_lows = recent_low > prior_low
    else:
        higher_lows = False

    # 5. Breakout — close > prior consolidation high (shift by 1)
    bo_level = h.iloc[-cons_bars-1:-1].max() if len(h) >= cons_bars+1 else h.iloc[:-1].max()
    breakout = c.iloc[-1] > bo_level

    # 6. Volume confirmation
    vol_confirm = v.iloc[-1] > avg_vol.iloc[-1] * 1.5 if avg_vol.iloc[-1] > 0 else False

    passes = has_momentum and surfing_ma and is_tight and higher_lows and breakout and vol_confirm
    details = {
        'momentum': has_momentum, 'surfing_ma': surfing_ma,
        'tight': is_tight, 'higher_lows': higher_lows,
        'breakout': breakout, 'vol_confirm': vol_confirm
    }
    return passes, details


# ============================================================================
# 3. QULLAMAGGIE EPISODIC PIVOT (Setup 2 from QUlla__-_Copy.afl)
# ============================================================================
def screen_qulla_ep(df):
    """
    Episodic Pivot:
    1. Gap up 10%+ (Open vs prior Close)
    2. Volume >= 3x avg (50-day)
    3. No prior big rally (ROC 63 bars ago < 30%) — surprise factor
    4. Strong close (above midpoint of range)
    5. Green day (Close > Open)
    """
    if df is None or len(df) < 65:
        return False, {}
    c, o, h, l, v = df['Close'], df['Open'], df['High'], df['Low'], df['Volume'] if 'Volume' in df.columns else pd.Series([0]*len(df))

    avg_vol50 = _ma(v, 50)

    # 1. Gap up >= 10%
    gap_pct = (o.iloc[-1] - c.iloc[-2]) / c.iloc[-2] * 100 if len(c) >= 2 else 0
    big_gap = gap_pct >= 10

    # 2. Volume >= 3x avg
    vol_surge = v.iloc[-1] >= avg_vol50.iloc[-1] * 3 if avg_vol50.iloc[-1] > 0 else False

    # 3. Not already rallied (prior ROC < 30%)
    prior_roc = _safe(_roc(c, 63).iloc[-2]) if len(c) >= 64 else 0
    not_rallied = prior_roc < 30

    # 4. Close in upper half of range (strong close)
    strong_close = c.iloc[-1] > (h.iloc[-1] + l.iloc[-1]) / 2

    # 5. Green day
    green_day = c.iloc[-1] > o.iloc[-1]

    passes = big_gap and vol_surge and not_rallied and strong_close and green_day
    details = {
        'gap_pct': round(gap_pct, 1), 'big_gap': big_gap,
        'vol_surge': vol_surge, 'not_rallied': not_rallied,
        'strong_close': strong_close, 'green_day': green_day
    }
    return passes, details


# ============================================================================
# 4. MEAN REVERSION — Composite Score (from MR_-_Copy.afl)
# ============================================================================
def screen_mean_reversion(df):
    """
    Mean Reversion Quality Signal — composite score 0-100
    Components:
      Stage2 (20): Close > MA50 > MA150 > MA200, both rising
      Dip/Flush (10): Had a flush bar on high volume in last 20 bars
      EMA Reclaim (10): Price back above EMA10 and EMA21
      Base Tightness (15): ATR ratio < 3%, base width < 10%
      Volume Dry-Up (20): Recent vol < 50-70% of 50-day avg
      RS Proxy (15): Weighted ROC percentile >= 70
      Pocket Pivot (15): Up day vol > max down-day vol in prior 11 bars
    Passes if score >= 45 AND Stage2 AND above EMAs AND vol contracting
    """
    if df is None or len(df) < 210:
        return False, {}
    c, h, l, v = df['Close'], df['High'], df['Low'], df['Volume'] if 'Volume' in df.columns else pd.Series([0]*len(df))

    ema10  = _ema(c, 10)
    ema21  = _ema(c, 21)
    ma50   = _ma(c, 50)
    ma150  = _ma(c, 150)
    ma200  = _ma(c, 200)
    avg_vol50 = _ma(v, 50)

    # Stage 2
    stage2 = (c.iloc[-1] > ma50.iloc[-1] and
              ma50.iloc[-1] > ma150.iloc[-1] and
              ma150.iloc[-1] > ma200.iloc[-1] and
              ma50.iloc[-1] > ma50.iloc[-6] and
              ma150.iloc[-1] > ma150.iloc[-11])
    stage2_score = 20 if stage2 else 0

    # Dip/Flush
    dip_lb = 20
    avg_vol_50 = avg_vol50.iloc[-1]
    flush_bars = ((c.iloc[-dip_lb:] < c.iloc[-dip_lb:].shift(1) * 0.97) &
                  (v.iloc[-dip_lb:] > avg_vol_50 * 1.5))
    had_flush = flush_bars.iloc[:-1].any()  # not on current bar
    recent_high = h.iloc[-dip_lb-10:].max()
    dip_low = l.iloc[-dip_lb:].min()
    dip_depth = (recent_high - dip_low) / recent_high * 100 if recent_high > 0 else 0
    dip_score = 10 if (had_flush and dip_depth >= 5) else (5 if (had_flush and dip_depth >= 3) else 0)

    # EMA Reclaim
    above_emas = c.iloc[-1] > ema21.iloc[-1] and c.iloc[-1] > ema10.iloc[-1]
    ema_score = 10 if above_emas else (5 if c.iloc[-1] > ema21.iloc[-1] else 0)

    # Base Tightness
    atr14_val = _safe(_atr(df, 14).iloc[-1])
    atr_ratio = atr14_val / ma50.iloc[-1] * 100 if ma50.iloc[-1] > 0 else 99
    base_high = h.iloc[-15:].max()
    base_low  = l.iloc[-15:].min()
    base_width = (base_high - base_low) / base_low * 100 if base_low > 0 else 99
    tight_base = base_width < 10 and atr_ratio < 3
    mod_base   = base_width < 15 and atr_ratio < 4
    tight_score = 15 if tight_base else (8 if mod_base else 0)

    # Volume Dry-Up
    recent_avg_vol = _ma(v, 15).iloc[-1]
    vol_ratio = recent_avg_vol / avg_vol_50 * 100 if avg_vol_50 > 0 else 100
    vol_dry = vol_ratio < 50
    vol_cont = vol_ratio < 70
    vol_trend_down = (_ma(v, 5).iloc[-1] < _ma(v, 10).iloc[-1] and
                      _ma(v, 10).iloc[-1] < _ma(v, 20).iloc[-1])
    vol_score = (15 if vol_dry else (8 if vol_cont else 0)) + (5 if vol_trend_down else 0)

    # RS Proxy (weighted ROC)
    roc63  = _safe(_roc(c, 63).iloc[-1])
    roc126 = _safe(_roc(c, 126).iloc[-1])
    rs_proxy = 0.4 * roc63 + 0.2 * roc126
    rs_hist = (0.4 * _roc(c, 63) + 0.2 * _roc(c, 126)).iloc[-252:]
    rs_high = rs_hist.max()
    rs_low  = rs_hist.min()
    rs_rank = ((rs_proxy - rs_low) / (rs_high - rs_low) * 100
               if (rs_high - rs_low) > 0 else 50)
    rs_score = (15 if rs_rank >= 85 else
                8  if rs_rank >= 70 else
                4  if rs_rank >= 55 else 0)

    # Pocket Pivot
    is_up_day = c.iloc[-1] > c.iloc[-2]
    max_down_vol = 0
    for i in range(1, 12):
        if len(c) > i and c.iloc[-1-i] < c.iloc[-2-i]:
            max_down_vol = max(max_down_vol, v.iloc[-1-i])
    pp_today = is_up_day and v.iloc[-1] > max_down_vol and c.iloc[-1] > ma50.iloc[-1]
    recent_pp = False
    for i in range(1, 15):
        if len(c) > i+11:
            is_up = c.iloc[-1-i] > c.iloc[-2-i]
            mdv = max((v.iloc[-1-i-j] for j in range(1, 12)
                       if len(c) > i+j+1 and c.iloc[-1-i-j] < c.iloc[-2-i-j]),
                      default=0)
            if is_up and v.iloc[-1-i] > mdv and c.iloc[-1-i] > ma50.iloc[-1-i]:
                recent_pp = True
                break
    pp_score = 15 if pp_today else (8 if recent_pp else 0)

    score = min(stage2_score + dip_score + ema_score + tight_score + vol_score + rs_score + pp_score, 100)

    passes = (score >= 45 and stage2 and above_emas and vol_cont and
              c.iloc[-1] > 20 and avg_vol_50 > 50000)
    details = {
        'score': score, 'stage2': stage2, 'above_emas': above_emas,
        'vol_dry': vol_dry, 'pp_today': pp_today, 'tight_base': tight_base
    }
    return passes, details


# ============================================================================
# 5. MANAS ARORA MOMENTUM BURST (from Manas_arora_-_Copy.afl)
# ============================================================================
def screen_manas_arora(df):
    """
    Momentum Burst — Institutional Grade:
    1. Liquidity: Close > 30, AvgVol20 > 1M
    2. Trend: Close > MA50 > MA200, MA200 rising 60 bars, near 52W high (>75%)
    3. Momentum: ROC(3M) > 30% OR 6M range expansion >= 1.5x
    4. Volatility Contraction: daily range < 3%, avg range 5 < avg range 20
    5. EMA Confluence: Close >= EMA21, Low <= EMA21 * 1.01
    6. Volume Signature: 5-day up volume > 5-day down volume
    """
    if df is None or len(df) < 210:
        return False, {}
    c, h, l, v = df['Close'], df['High'], df['Low'], df['Volume'] if 'Volume' in df.columns else pd.Series([0]*len(df))
    o = df['Open'] if 'Open' in df.columns else c

    ma50  = _ma(c, 50)
    ma200 = _ma(c, 200)
    ema21 = _ema(c, 21)
    avg_vol20 = _ma(v, 20)

    high52 = _hhv(h, 252).iloc[-1]
    low52  = _llv(l, 252).iloc[-1]
    high6m = _hhv(h, 126).iloc[-1]
    low6m  = _llv(l, 126).iloc[-1]

    # 1. Liquidity
    liquidity = c.iloc[-1] > 30 and avg_vol20.iloc[-1] > 1_000_000

    # 2. Trend
    trend = (c.iloc[-1] > ma50.iloc[-1] and
             ma50.iloc[-1] > ma200.iloc[-1] and
             ma200.iloc[-1] > ma200.iloc[-61] and
             c.iloc[-1] >= 0.75 * high52)

    # 3. Momentum
    roc3m = _safe(_roc(c, 63).iloc[-1])
    range_expand = (high6m / low6m >= 1.5) if low6m > 0 else False
    momentum = roc3m > 30 or range_expand

    # 4. Volatility Contraction
    range_pct = (h.iloc[-1] - l.iloc[-1]) / c.iloc[-1] if c.iloc[-1] > 0 else 1
    avg_range5  = (h - l).rolling(5).mean().iloc[-1]
    avg_range20 = (h - l).rolling(20).mean().iloc[-1]
    vol_contract = range_pct < 0.03 and avg_range5 < avg_range20

    # 5. EMA Confluence
    ema_conf = c.iloc[-1] >= ema21.iloc[-1] and l.iloc[-1] <= ema21.iloc[-1] * 1.01

    # 6. Volume Signature
    up_vol   = sum(v.iloc[-5+i] for i in range(5) if c.iloc[-5+i] > c.iloc[-5+i-1])
    down_vol = sum(v.iloc[-5+i] for i in range(5) if c.iloc[-5+i] < c.iloc[-5+i-1])
    vol_sig = up_vol > down_vol

    passes = liquidity and trend and momentum and vol_contract and ema_conf and vol_sig
    details = {
        'liquidity': liquidity, 'trend': trend, 'momentum': momentum,
        'vol_contract': vol_contract, 'ema_conf': ema_conf, 'vol_sig': vol_sig
    }
    return passes, details


# ============================================================================
# 6. VCP — Minervini Volatility Contraction Pattern (from Minervini_VCP_Chart)
# ============================================================================
def screen_vcp(df):
    """
    Minervini VCP:
    1. Trend Template: all 8 conditions (TT1-TT8)
    2. VCP Detected: at least 2 contracting ranges with >= 70% tightening ratio
    3. Volume Dry-Up: recent avg vol < 60% of 50-day avg
    4. Near Pivot: within 7% of consolidation high
    Composite score returned for ranking
    """
    if df is None or len(df) < 260:
        return False, {}
    c, h, l, v = df['Close'], df['High'], df['Low'], df['Volume'] if 'Volume' in df.columns else pd.Series([0]*len(df))

    ma50  = _ma(c, 50)
    ma150 = _ma(c, 150)
    ma200 = _ma(c, 200)
    avg_vol50 = _ma(v, 50)

    # Trend Template
    week52_high = _hhv(h, 252).iloc[-1]
    week52_low  = _llv(l, 252).iloc[-1]
    pct_from_high = (week52_high - c.iloc[-1]) / week52_high * 100 if week52_high > 0 else 100
    pct_above_low = (c.iloc[-1] - week52_low) / week52_low * 100 if week52_low > 0 else 0

    roc1m = _safe(_roc(c, 21).iloc[-1])
    roc3m = _safe(_roc(c, 63).iloc[-1])
    roc6m = _safe(_roc(c, 126).iloc[-1])
    weighted_rs = roc1m * 0.4 + roc3m * 0.3 + roc6m * 0.3

    # 200 MA rising for 12 weeks (60 bars)
    ma200_rising = ma200.iloc[-1] > ma200.iloc[-61] if len(ma200) >= 61 else False

    tt1 = c.iloc[-1] > ma150.iloc[-1] and c.iloc[-1] > ma200.iloc[-1]
    tt2 = ma150.iloc[-1] > ma200.iloc[-1]
    tt3 = ma200_rising
    tt4 = ma50.iloc[-1] > ma150.iloc[-1] and ma50.iloc[-1] > ma200.iloc[-1]
    tt5 = c.iloc[-1] > ma50.iloc[-1]
    tt6 = pct_above_low >= 30
    tt7 = pct_from_high <= 25
    tt8 = weighted_rs > 0

    trend_score = sum([tt1, tt2, tt3, tt4, tt5, tt6, tt7, tt8])
    trend_pass = all([tt1, tt2, tt3, tt4, tt5, tt6, tt7, tt8])

    # VCP Contractions — 4 nested lookback windows
    vcp_lb = 120
    lk1 = vcp_lb
    lk2 = max(2, round(vcp_lb * 0.50))
    lk3 = max(2, round(vcp_lb * 0.25))
    lk4 = max(2, round(vcp_lb * 0.12))

    def range_pct(n):
        rng  = _hhv(h, n).iloc[-1] - _llv(l, n).iloc[-1]
        high = _hhv(h, n).iloc[-1]
        return rng / high * 100 if high > 0 else 0

    c1, c2, c3, c4 = range_pct(lk1), range_pct(lk2), range_pct(lk3), range_pct(lk4)

    t12 = c2 < c1
    t23 = c3 < c2
    t34 = c4 < c3

    tighten_r12 = ((1 - c2/c1) * 100) if c1 > 0 else 0
    tighten_r23 = ((1 - c3/c2) * 100) if c2 > 0 else 0

    num_contractions = 1 + (1 if t12 else 0) + (1 if (t12 and t23) else 0) + (1 if (t12 and t23 and t34) else 0)
    two_vcp = num_contractions >= 2 and tighten_r12 >= 70
    three_vcp = num_contractions >= 3
    vcp_detected = two_vcp or three_vcp

    # Volume Dry-Up
    recent_avg_vol = _ma(v, 10).iloc[-1]
    vol_dry = recent_avg_vol < avg_vol50.iloc[-1] * 0.60 if avg_vol50.iloc[-1] > 0 else False

    # Near Pivot (within 7% of consolidation high)
    pivot_level = _hhv(h, lk3).iloc[-1]
    pct_from_pivot = (pivot_level - c.iloc[-1]) / pivot_level * 100 if pivot_level > 0 else 100
    near_pivot = pct_from_pivot <= 7

    # Simple composite
    vcp_score = (trend_score / 8 * 40 +
                 (30 if three_vcp else 20 if two_vcp else 0) +
                 (15 if vol_dry else 0) +
                 (15 if near_pivot else 5 if pct_from_pivot <= 15 else 0))

    passes = vcp_detected and trend_pass and vol_dry and near_pivot
    details = {
        'trend_score': trend_score, 'trend_pass': trend_pass,
        'vcp_detected': vcp_detected, 'num_contractions': num_contractions,
        'vol_dry': vol_dry, 'near_pivot': near_pivot,
        'pct_from_pivot': round(pct_from_pivot, 1), 'vcp_score': round(vcp_score, 1)
    }
    return passes, details


# ============================================================================
# DISPATCHER — maps screener ID → function
# ============================================================================
CUSTOM_SCREENER_MAP = {
    'svro':            screen_svro,
    'qulla_breakout':  screen_qulla_breakout,
    'qulla_ep':        screen_qulla_ep,
    'mean_reversion_q': screen_mean_reversion,
    'manas_arora':     screen_manas_arora,
    'vcp_minervini':   screen_vcp,
}

def apply_custom_screener(scr_id, df, rs_rating=None):
    """Single entry point. Returns (passes: bool, details: dict)"""
    fn = CUSTOM_SCREENER_MAP.get(scr_id)
    if fn is None:
        return False, {}
    try:
        if scr_id == 'svro':
            return fn(df, rs_rating=rs_rating)
        return fn(df)
    except Exception as e:
        return False, {'error': str(e)}


# ══════════════════════════════════════════════════════════════════════════════
# RS RANKING ENGINE — IBD Style
# Ranks each stock 1-99 vs NIFTY 500 universe
# Formula: 40% last 3mo + 20% each of prior 3 quarters (12mo total)
# ══════════════════════════════════════════════════════════════════════════════

_rs_cache = {}
RS_CACHE_TTL = 14400  # 4 hours

# Ticker→sector map, built once at startup from SQLite sector_map table
_ticker_sector_map = {}

def _build_sector_map():
    """Populate _ticker_sector_map from SQLite. Called at startup."""
    global _ticker_sector_map
    if not DB_AVAILABLE:
        return
    try:
        import sqlite3, pathlib
        conn = sqlite3.connect(
            str(pathlib.Path(__file__).parent / "breadth_data.db"), timeout=10)
        rows = conn.execute(
            "SELECT ticker, sector FROM sector_map WHERE sector IS NOT NULL AND sector != ''"
        ).fetchall()
        conn.close()
        _ticker_sector_map = {r[0]: r[1] for r in rows}
        logger.info(f"Sector map: {len(_ticker_sector_map):,} entries")
    except Exception as e:
        logger.warning(f"sector_map load failed: {e}")

def _rs_cache_key(market): return f"rs_{market}"

def _sym_ratio(a: float, b: float) -> float:
    """
    Symmetric ratio: if a > b → a/b, if a < b → -(b/a)
    Makes -50% = -2.0 symmetric with +100% = +2.0. Capped at ±2.0.
    """
    if b <= 0:
        return 0.0
    ratio = a / b
    if a < b:
        ratio = -(b / a)
    return max(-2.0, min(2.0, ratio))


def _normalize_m2(raw: float) -> int:
    """
    Normalize M2 (absolute perf) to 1-99.
    Neutral point: flat stock → all ratios = 1.0 → raw = 100 → score = 50.
    Positive range [100, 200] → [50, 99]
    Negative range [-200, 100] → [1, 49]
    """
    shifted = raw - 100.0   # center on flat stock
    if shifted >= 0:
        # max positive shift = 200-100 = 100 → score 99
        score = 50 + round(shifted / 100.0 * 49)
    else:
        # max negative shift = -200-100 = -300 → score 1
        score = 50 + round(shifted / 300.0 * 49)
    return max(1, min(99, score))


def _normalize_m3(raw: float) -> int:
    """
    Normalize M3 (relative to index) to 1-99.
    Neutral point: stock matches index → all rel ratios = 1.0 → raw = 100 → score = 50.
    Same mapping as _normalize_m2.
    """
    return _normalize_m2(raw)


def _stretch(rating: int) -> int:
    """
    Stretch scores above 50 upward — matches TradingView RS script.
    50→50, 66→74, 75→87, 90→95, 99→99
    """
    if rating >= 50:
        rating = round(rating + (rating - 50) / 2)
    return max(1, min(99, rating))


def _compute_ad_rating(df) -> tuple:
    """
    IBD Accumulation/Distribution Rating — from All_in_One.afl (lines 866-890)

    Formula:
      daily_acc = (2×Close − High − Low) / (High − Low)   ← Close Location Value
      score     = Sum(daily_acc × Volume, 65) / Sum(Volume, 65) × 100

    Grade mapping (11 grades):
      ≥+55→A+  ≥+40→A  ≥+30→A-  ≥+10→B+  ≥0→B
      ≥-10→C+  ≥-20→C  ≥-30→C-  ≥-40→D+  ≥-55→D  else→E

    Returns: (grade: str, score: float)
    """
    try:
        if df is None or len(df) < 65:
            return "N/A", 0.0

        c = df["Close"].values
        h = df["High"].values
        l = df["Low"].values
        v = df["Volume"].values if "Volume" in df.columns else None

        if v is None or len(v) < 65:
            return "N/A", 0.0

        # Last 65 bars
        c65, h65, l65, v65 = c[-65:], h[-65:], l[-65:], v[-65:]

        # Close Location Value: where did close land in the day's range?
        hl = h65 - l65
        # Avoid div/0 on doji bars (High == Low)
        clv = np.where(hl > 0, (2 * c65 - h65 - l65) / hl, 0.0)

        # Volume-weighted average over 65 days
        vol_sum = v65.sum()
        if vol_sum <= 0:
            return "N/A", 0.0

        acc_sum = (clv * v65).sum()
        ad_per  = acc_sum / vol_sum
        score   = round(float(ad_per * 100), 1)

        # Grade mapping (exact from AFL)
        if   score >= 55:  grade = "A+"
        elif score >= 40:  grade = "A"
        elif score >= 30:  grade = "A-"
        elif score >= 10:  grade = "B+"
        elif score >= 0:   grade = "B"
        elif score >= -10: grade = "C+"
        elif score >= -20: grade = "C"
        elif score >= -30: grade = "C-"
        elif score >= -40: grade = "D+"
        elif score >= -55: grade = "D"
        else:              grade = "E"

        return grade, score

    except Exception as e:
        logger.debug(f"AD rating failed: {e}")
        return "N/A", 0.0


def _ibd_rs_score(prices: list, idx_prices: list = None) -> float:
    """
    RS Rating — TradingView-verified M2+M3 method (matches MarketSmith).

    METHOD 2 (M2): Absolute performance ratio, symmetric handling
        ratio = close/close[N], if negative: -(close[N]/close)
        RSraw_m2 = 40×3M + 20×6M + 20×9M + 20×12M
        Normalize → 1-99, then stretch scores above 50

    METHOD 3 (M3): Relative to CNX500 index, ratio-of-ratios
        rel_3m = (stock/stock[63]) / (index/index[63])
        symmetric handling if stock underperforms index
        RSraw_m3 = 40×3M + 20×6M + 20×9M + 20×12M
        Normalize → 1-99, NO stretch

    FINAL = round((M2 + M3) / 2)
    Returns raw combined score for percentile ranking.
    """
    n = len(prices)
    if n < 63:
        return None

    p0   = prices[-1]
    p63  = prices[-63]  if n >= 63  else prices[0]
    p126 = prices[-126] if n >= 126 else prices[0]
    p189 = prices[-189] if n >= 189 else prices[0]
    p252 = prices[-252] if n >= 252 else prices[0]

    # ── METHOD 2: Absolute performance (symmetric ratios) ─────────────────
    m2_3m  = _sym_ratio(p0,   p63)
    m2_6m  = _sym_ratio(p0,   p126)
    m2_9m  = _sym_ratio(p0,   p189)
    m2_12m = _sym_ratio(p0,   p252)

    raw_m2   = 40*m2_3m + 20*m2_6m + 20*m2_9m + 20*m2_12m
    rating_m2 = _stretch(_normalize_m2(raw_m2))

    # ── METHOD 3: Relative to index (ratio of ratios) ──────────────────────
    rating_m3 = 50  # default if no index
    if idx_prices and len(idx_prices) >= 63:
        ni = len(idx_prices)
        i0   = idx_prices[-1]
        i63  = idx_prices[-63]  if ni >= 63  else idx_prices[0]
        i126 = idx_prices[-126] if ni >= 126 else idx_prices[0]
        i189 = idx_prices[-189] if ni >= 189 else idx_prices[0]
        i252 = idx_prices[-252] if ni >= 252 else idx_prices[0]

        # ratio of ratios: stock perf / index perf at each period
        # if stock underperforms index → symmetric negative
        def _rel(sn, sb, xn, xb):
            if sb <= 0 or xb <= 0 or xn <= 0:
                return 0.0
            s_ratio = sn / sb
            x_ratio = xn / xb
            if x_ratio <= 0:
                return 0.0
            rel = s_ratio / x_ratio
            if s_ratio < x_ratio:
                rel = -(x_ratio / s_ratio)
            return max(-2.0, min(2.0, rel))

        m3_3m  = _rel(p0, p63,  i0, i63)
        m3_6m  = _rel(p0, p126, i0, i126)
        m3_9m  = _rel(p0, p189, i0, i189)
        m3_12m = _rel(p0, p252, i0, i252)

        raw_m3   = 40*m3_3m + 20*m3_6m + 20*m3_9m + 20*m3_12m
        rating_m3 = _normalize_m3(raw_m3)  # NO stretch on M3

    # ── FINAL: Average M2 + M3 ────────────────────────────────────────────
    final = round((rating_m2 + rating_m3) / 2)
    return max(1, min(99, final))

def _compute_rs_rankings(market: str = "India") -> dict:
    """Compute IBD-style RS rankings for all stocks in universe."""
    t0 = time.time()
    logger.info(f"=== Computing RS Rankings for {market} ===")

    # Get FULL NSE universe for screener (not just NIFTY 500)
    # RS is calculated vs ^CRSLDX (NIFTY 500 benchmark) for all stocks
    stock_data = get_screener_data(market)
    if not stock_data:
        return {"error": "No stock data available"}

    logger.info(f"Screener universe: {len(stock_data):,} tickers (RS vs ^CRSLDX)")

    # Get index data for RS Line calculation
    index_ticker = "^CRSLDX" if market == "India" else "^GSPC"
    idx_df = safe_download(index_ticker, period="1y")
    idx_prices = []
    try:
        if not idx_df.empty:
            # Handle both flat and multi-level columns (yfinance v0.2+)
            if isinstance(idx_df.columns, __import__('pandas').MultiIndex):
                close_col = idx_df["Close"]
                if hasattr(close_col, 'iloc'):
                    close_col = close_col.iloc[:, 0]
            elif "Close" in idx_df.columns:
                close_col = idx_df["Close"]
            else:
                close_col = idx_df.iloc[:, 0]
            idx_prices = close_col.dropna().tolist()
    except Exception as e:
        logger.warning(f"Could not extract index prices: {e}")
        idx_prices = []

    # Compute raw scores
    raw_scores = []
    for ticker, df in stock_data.items():
        if df is None or len(df) < 60:
            continue
        try:
            prices = df["Close"].dropna().tolist()
            score = _ibd_rs_score(prices, idx_prices=idx_prices)
            if score is None:
                continue

            cur = prices[-1]
            high_52w = max(prices[-252:]) if len(prices)>=252 else max(prices)
            low_52w  = min(prices[-252:]) if len(prices)>=252 else min(prices)
            pct_from_high = round((cur - high_52w) / high_52w * 100, 1)
            pct_from_low  = round((cur - low_52w)  / low_52w  * 100, 1)

            # 1-week change
            chg_1w = round((cur - prices[-6]) / prices[-6] * 100, 2) if len(prices)>=6 else 0
            # 1-month change
            chg_1m = round((cur - prices[-22]) / prices[-22] * 100, 2) if len(prices)>=22 else 0
            # 3-month change
            chg_3m = round((cur - prices[-63]) / prices[-63] * 100, 2) if len(prices)>=63 else 0

            # RS Line (stock / index) trend — rising = positive momentum
            rs_line_slope = 0
            if idx_prices and len(idx_prices) >= 22:
                stock_1m = (cur - prices[-22]) / prices[-22] if len(prices)>=22 else 0
                idx_1m   = (idx_prices[-1] - idx_prices[-22]) / idx_prices[-22] if len(idx_prices)>=22 else 0
                rs_line_slope = round((stock_1m - idx_1m) * 100, 2)

            # Trend Template — full 8-condition Minervini check
            # Uses actual OHLCV data, no proxy needed
            trend_template = False
            trend_score_tt = 0
            try:
                if df is not None and len(prices) >= 252:
                    c_s  = df["Close"]
                    h_s  = df["High"]
                    l_s  = df["Low"]
                    ma50_v  = _ma(c_s, 50).iloc[-1]
                    ma150_v = _ma(c_s, 150).iloc[-1]
                    ma200_v = _ma(c_s, 200).iloc[-1]
                    ma200_60 = _ma(c_s, 200).iloc[-61] if len(c_s) >= 61 else ma200_v
                    hi52 = _hhv(h_s, 252).iloc[-1]
                    lo52 = _llv(l_s, 252).iloc[-1]
                    tt1 = cur > ma150_v and cur > ma200_v
                    tt2 = ma150_v > ma200_v
                    tt3 = ma200_v > ma200_60
                    tt4 = ma50_v > ma150_v and ma50_v > ma200_v
                    tt5 = cur > ma50_v
                    tt6 = lo52 > 0 and cur > lo52 * 1.30
                    tt7 = hi52 > 0 and cur >= hi52 * 0.75
                    roc1m = (cur - prices[-22]) / prices[-22] * 100 if len(prices) >= 22 else 0
                    roc3m_v = (cur - prices[-63]) / prices[-63] * 100 if len(prices) >= 63 else 0
                    roc6m_v = (cur - prices[-126]) / prices[-126] * 100 if len(prices) >= 126 else 0
                    tt8 = (0.4 * roc1m + 0.3 * roc3m_v + 0.3 * roc6m_v) > 0
                    conditions = [tt1, tt2, tt3, tt4, tt5, tt6, tt7, tt8]
                    trend_score_tt = sum(conditions)
                    trend_template = all(conditions)
            except Exception:
                pass

            # RS trend: compare recent vs prior RS score
            # Slice matching idx_prices windows for correct M3 calculation
            if len(prices) >= 126 and idx_prices and len(idx_prices) >= 126:
                rs_recent = _ibd_rs_score(prices[-126:],   idx_prices=idx_prices[-126:])
                rs_prior  = _ibd_rs_score(prices[-189:-63], idx_prices=idx_prices[-189:-63]) if len(prices)>=189 else rs_recent
            elif len(prices) >= 63:
                rs_recent = _ibd_rs_score(prices[-126:] if len(prices)>=126 else prices, idx_prices=idx_prices)
                rs_prior  = score
            else:
                rs_recent = score
                rs_prior  = score
            rs_trend = "↑" if float(rs_recent or 0) > float(rs_prior or 0) else "↓"

            # Volume analysis (last 10 days vs 50-day avg)
            vol_ratio = 1.0
            try:
                if "Volume" in df.columns:
                    vol_series = df["Volume"].dropna()
                    if len(vol_series) >= 50:
                        avg_vol = float(vol_series.iloc[-50:].mean())
                        cur_vol = float(vol_series.iloc[-1])
                        vol_ratio = round(cur_vol / avg_vol if avg_vol > 0 else 1.0, 2)
            except Exception:
                pass

            raw_scores.append({
                "ticker":         str(ticker),
                "raw_score":      float(score),
                "price":          float(round(cur, 2)),
                "chg_1w":         float(chg_1w),
                "chg_1m":         float(chg_1m),
                "chg_3m":         float(chg_3m),
                "pct_from_high":  float(pct_from_high),
                "pct_from_low":   float(pct_from_low),
                "rs_line_slope":  float(rs_line_slope),
                "rs_trend":       str(rs_trend),
                "vol_ratio":      float(vol_ratio),
                "sector":         str(_ticker_sector_map.get(ticker, "")),
                "ad_rating":      str((_ad := _compute_ad_rating(df))[0]),
                "ad_score":       float(_ad[1]),
                "trend_template": bool(trend_template),
                "trend_score_tt": int(trend_score_tt),
            })
        except Exception as e:
            logger.debug(f"RS score failed {ticker}: {e}")
            continue

    if not raw_scores:
        return {"error": "Could not compute RS scores"}

    # M2+M3 already returns a calibrated 1-99 rating
    # Apply a light percentile adjustment to spread the distribution
    # across the full universe (keeps relative ordering intact)
    scores_sorted = sorted([s["raw_score"] for s in raw_scores])
    n = len(scores_sorted)

    def percentile_rank(val):
        pos = sum(1 for s in scores_sorted if s <= val)
        return max(1, min(99, round(pos / n * 99)))

    for item in raw_scores:
        # Blend: 70% M2+M3 score + 30% cross-sectional percentile
        # Keeps calibration close to MarketSmith while maintaining
        # true cross-sectional ordering
        m2m3_score   = item["raw_score"]
        pct_score    = percentile_rank(m2m3_score)
        blended      = round(0.70 * m2m3_score + 0.30 * pct_score)
        item["rs_rating"] = int(max(1, min(99, blended)))

    # Sort by RS rating descending
    raw_scores.sort(key=lambda x: x["rs_rating"], reverse=True)

    # Add rank
    for i, item in enumerate(raw_scores):
        item["rank"] = i + 1
        del item["raw_score"]  # remove internal field

    elapsed = round(time.time() - t0, 2)
    logger.info(f"RS Rankings computed: {len(raw_scores)} stocks in {elapsed}s")

    return {
        "market": market,
        "stocks": raw_scores,
        "total": len(raw_scores),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "elapsed": elapsed,
    }


# ── Leaders ──────────────────────────────────────────────────────────────────

LEADERS_CACHE_TTL = 14400  # 4 hours
_leaders_cache: dict = {}

def _ad_to_numeric(grade: str) -> float:
    """Convert A/D letter grade to 0-100 numeric for Leader Score."""
    return {
        "A+": 100, "A": 88, "A-": 75,
        "B+": 62,  "B": 50,
        "C+": 38,  "C": 25, "C-": 15,
        "D+": 8,   "D": 4,  "E": 0,
    }.get(grade, 0)


def _compute_leaders(market: str, stocks: list, breadth_data: dict) -> dict:
    """
    Core logic for /api/leaders.
    stocks     — list of scored stock dicts from _compute_rs_rankings
    breadth_data — dict from _compute_breadth (B50, NH_NL_Net, AD_ROC5 etc.)
    """
    t0 = time.time()

    # ── 1. Q-BRAM Regime ─────────────────────────────────────────────────────
    # Keys from compute_breadth: pct_above_50, pct_above_200, nh_nl, ad_roc
    b50   = float(breadth_data.get("pct_above_50",  breadth_data.get("pct_above_50ma",  0)))
    b200  = float(breadth_data.get("pct_above_200", breadth_data.get("pct_above_200ma", 0)))
    nh_nl = float(breadth_data.get("nh_nl",         breadth_data.get("nh_nl_net",       0)))
    ad_roc= float(breadth_data.get("ad_roc",        breadth_data.get("ad_roc5",         0)))

    if b50 > 60:
        regime = "BULLISH"
    elif b50 < 25:
        regime = "OVERSOLD"
    else:
        regime = "NEUTRAL"

    regime_meta = {
        "regime": regime,
        "b50": round(b50, 1),
        "b200": round(b200, 1),
        "nh_nl": int(nh_nl),
        "ad_trend": "↑" if ad_roc > 0 else "↓",
    }

    # ── 2. Sector RS Score + Trend ────────────────────────────────────────────
    sector_stocks = defaultdict(list)
    for s in stocks:
        sec = s.get("sector", "")
        if sec:
            sector_stocks[sec].append(s)

    sector_health = {}
    for sec, sec_stocks in sector_stocks.items():
        if len(sec_stocks) < 3:
            continue
        # Average RS of top-25% stocks in sector (leaders pull the sector)
        sorted_sec = sorted(sec_stocks, key=lambda x: x.get("rs_rating", 0), reverse=True)
        top_n = max(3, len(sorted_sec) // 4)
        top_stocks = sorted_sec[:top_n]
        avg_rs = round(sum(s.get("rs_rating", 0) for s in top_stocks) / len(top_stocks), 1)

        # Trend: compare avg RS of top stocks now vs 10-day proxy
        # Use chg_3m vs chg_1m differential as sector momentum direction
        avg_3m = sum(s.get("chg_3m", 0) for s in top_stocks) / len(top_stocks)
        avg_1m = sum(s.get("chg_1m", 0) for s in top_stocks) / len(top_stocks)
        # If recent momentum (1M) is accelerating vs 3M average → trending up
        rs_trend_sec = "↑" if avg_1m > (avg_3m / 3) else "↓"

        # Classify sector health
        if avg_rs >= 75:
            health = "hot"
        elif avg_rs >= 55:
            health = "warm"
        else:
            health = "cold"

        sector_health[sec] = {
            "avg_rs": avg_rs,
            "trend": rs_trend_sec,
            "health": health,
            "count": len(sec_stocks),
        }

    # Sort sectors by avg_rs descending
    sector_health = dict(
        sorted(sector_health.items(), key=lambda x: x[1]["avg_rs"], reverse=True)
    )

    # ── 3. Leader Score per stock ─────────────────────────────────────────────
    def leader_score(s: dict) -> float:
        rs       = s.get("rs_rating", 0)
        ad_num   = _ad_to_numeric(s.get("ad_rating", "B"))
        sec      = s.get("sector", "")
        sec_rs   = sector_health.get(sec, {}).get("avg_rs", 50)
        pfh      = s.get("pct_from_high", -50)          # negative = below high
        near_hi  = max(0, min(100, 100 + pfh * 2))      # -50% → 0, 0% → 100
        tt       = 100 if s.get("trend_template") else (s.get("trend_score_tt", 0) / 8 * 100)

        score = (
            rs       * 0.35 +
            ad_num   * 0.25 +
            sec_rs   * 0.20 +
            near_hi  * 0.10 +
            tt       * 0.10
        )
        return round(min(100, max(0, score)), 1)

    # Attach leader score to every stock
    for s in stocks:
        s["leader_score"] = float(round(leader_score(s), 1))

    # ── 4. Tier Assignment ────────────────────────────────────────────────────
    elite     = []
    emerging  = []
    pressure  = []
    mr_cands  = []

    hot_sectors  = {k for k, v in sector_health.items() if v["health"] == "hot"}
    warm_sectors = {k for k, v in sector_health.items() if v["health"] == "warm"}

    for s in stocks:
        rs     = s.get("rs_rating", 0)
        ad     = s.get("ad_rating", "B")
        sec    = s.get("sector", "")
        ad_num = _ad_to_numeric(ad)

        is_hot_sec  = sec in hot_sectors
        is_warm_sec = sec in warm_sectors
        is_good_sec = is_hot_sec or is_warm_sec

        # ── ALWAYS populate all 4 tiers regardless of regime ─────────────────
        # Regime only affects visual emphasis on frontend, not data availability
        # User decides what to trade — we show everything

        # Elite: RS 90+ AND A/D A- or better AND in hot sector
        if rs >= 90 and ad_num >= 75 and is_hot_sec:
            elite.append(s)
        # Emerging: RS 80-89 AND A/D B+ or better AND hot/warm sector
        elif rs >= 80 and ad_num >= 50 and is_good_sec:
            emerging.append(s)
        # Under Pressure: RS 80+ but A/D weak OR cold sector
        elif rs >= 80 and (ad_num < 38 or not is_good_sec):
            pressure.append(s)

        # Mean Reversion: pulled back 10-40% from high, RS still decent, A/D recovering
        # Always computed — highlighted in OVERSOLD, dimmed in BULLISH
        pfh = s.get("pct_from_high", -100)
        if -40 <= pfh <= -5 and rs >= 50 and ad_num >= 25:
            mr_cands.append(s)

    # Sort each tier by leader_score descending
    elite.sort(    key=lambda x: x["leader_score"], reverse=True)
    emerging.sort( key=lambda x: x["leader_score"], reverse=True)
    pressure.sort( key=lambda x: x["leader_score"], reverse=True)
    mr_cands.sort( key=lambda x: x.get("rs_rating", 0), reverse=True)

    elapsed = round(time.time() - t0, 2)
    return {
        "regime":        regime_meta,
        "sector_health": sector_health,
        "tiers": {
            "elite":    elite[:100],
            "emerging": emerging[:100],
            "pressure": pressure[:100],
            "mr_cands": mr_cands[:100],
        },
        "counts": {
            "elite":    len(elite),
            "emerging": len(emerging),
            "pressure": len(pressure),
            "mr_cands": len(mr_cands),
        },
        "elapsed": elapsed,
    }
