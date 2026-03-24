"""
Peep Into Past — Historical Breadth Analysis
Reconstructs Q-BRAM score & regime for any historical date using DB OHLCV data.
"""
import sqlite3, os, json, time, logging
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)
DB = os.path.join(os.path.dirname(__file__), "breadth_data.db")

# ── Q-BRAM scoring (mirrors breadth.py exactly) ────────────────────────────

def _qbram_score(p50, nh_nl_ratio, ad_roc, p200, b20_accel, vol_ratio):
    pts = {}
    # B50: 25 pts
    if   p50 >= 80: b = 25
    elif p50 >= 65: b = 22
    elif p50 >= 50: b = 20
    elif p50 >= 40: b = 15
    elif p50 >= 30: b = 10
    elif p50 >= 20: b = 5
    else:           b = 0
    pts["B50"] = {"value": round(p50, 1), "points": b, "max": 25}

    # NH-NL: 20 pts
    r = nh_nl_ratio
    if   r >= 0.20: n = 20
    elif r >= 0.10: n = 16
    elif r >= 0.05: n = 12
    elif r >= 0.00: n = 8
    elif r >= -0.10: n = 4
    else:            n = 0
    pts["NH_NL"] = {"value": round(r * 100, 1), "points": n, "max": 20}

    # A/D ROC: 20 pts
    if   ad_roc >= 50:  a = 20
    elif ad_roc >= 25:  a = 16
    elif ad_roc >= 10:  a = 12
    elif ad_roc >= 0:   a = 8
    elif ad_roc >= -15: a = 4
    else:               a = 0
    pts["AD_ROC"] = {"value": round(ad_roc, 1), "points": a, "max": 20}

    # B200: 15 pts
    if   p200 >= 70: b2 = 15
    elif p200 >= 60: b2 = 12
    elif p200 >= 50: b2 = 9
    elif p200 >= 40: b2 = 6
    elif p200 >= 30: b2 = 3
    else:            b2 = 0
    pts["B200"] = {"value": round(p200, 1), "points": b2, "max": 15}

    # B20 Accel: 10 pts
    if   b20_accel >= 15: ba = 10
    elif b20_accel >= 8:  ba = 8
    elif b20_accel >= 3:  ba = 6
    elif b20_accel >= 0:  ba = 4
    elif b20_accel >= -5: ba = 2
    else:                 ba = 0
    pts["B20_ACCEL"] = {"value": round(b20_accel, 1), "points": ba, "max": 10}

    # Volume: 10 pts
    if   vol_ratio >= 3.0: v = 10
    elif vol_ratio >= 2.0: v = 8
    elif vol_ratio >= 1.5: v = 6
    elif vol_ratio >= 1.0: v = 4
    elif vol_ratio >= 0.7: v = 2
    else:                  v = 0
    pts["VOLUME"] = {"value": round(vol_ratio, 2), "points": v, "max": 10}

    total = min(100, b + n + a + b2 + ba + v)
    return total, pts


def _regime(s):
    if   s >= 80: return "EXPANSION"
    elif s >= 60: return "ACCUMULATION"
    elif s >= 40: return "TRANSITION"
    elif s >= 20: return "DISTRIBUTION"
    else:         return "PANIC"


def _rcolor(r):
    return {
        "EXPANSION": "#22c55e", "ACCUMULATION": "#86efac",
        "TRANSITION": "#f59e0b", "DISTRIBUTION": "#ef4444",
        "PANIC": "#7f1d1d",
    }.get(r, "#64748b")


# ── Cache ───────────────────────────────────────────────────────────────────

def _ensure_cache_table():
    conn = sqlite3.connect(DB)
    conn.execute("""CREATE TABLE IF NOT EXISTS peep_cache (
        date TEXT, market TEXT, result TEXT, created_at TEXT,
        PRIMARY KEY (date, market))""")
    conn.commit()
    conn.close()


def _get_cached(target_date, market):
    try:
        conn = sqlite3.connect(DB)
        row = conn.execute(
            "SELECT result FROM peep_cache WHERE date=? AND market=?",
            (target_date, market)
        ).fetchone()
        conn.close()
        if row:
            return json.loads(row[0])
    except:
        pass
    return None


def _set_cache(target_date, market, result):
    try:
        conn = sqlite3.connect(DB)
        conn.execute(
            "INSERT OR REPLACE INTO peep_cache (date, market, result, created_at) VALUES (?,?,?,?)",
            (target_date, market, json.dumps(result, default=str), datetime.now().isoformat())
        )
        conn.commit()
        conn.close()
    except:
        pass


# ── Nearest trading day ─────────────────────────────────────────────────────

def _nearest_trading_day(target_date, market="India"):
    conn = sqlite3.connect(DB)
    # Try exact, then look ±5 days
    for delta in range(0, 6):
        for d in [target_date + timedelta(days=-delta), target_date + timedelta(days=delta)]:
            ds = d.strftime("%Y-%m-%d")
            row = conn.execute(
                "SELECT COUNT(*) FROM ohlcv WHERE date=? AND market=?", (ds, market)
            ).fetchone()
            if row and row[0] > 50:
                conn.close()
                return ds
    conn.close()
    return target_date.strftime("%Y-%m-%d")


# ── Compute breadth for a single date ───────────────────────────────────────

def _compute_for_date(all_data, target_ds, prev_ds, prev5_ds, valid_tickers):
    """Compute breadth metrics for one date given pre-loaded DataFrame."""
    t_data = all_data[all_data["date"] == target_ds]
    p_data = all_data[all_data["date"] == prev_ds] if prev_ds else pd.DataFrame()

    if t_data.empty:
        return None

    t_indexed = t_data.set_index("ticker")
    p_indexed = p_data.set_index("ticker") if not p_data.empty else pd.DataFrame()

    adv = dec = unc = 0
    up_vol = dn_vol = 0.0

    for tk in t_indexed.index:
        cur = t_indexed.at[tk, "close"]
        if tk in p_indexed.index:
            prev = p_indexed.at[tk, "close"]
            vol = t_indexed.at[tk, "volume"] if "volume" in t_indexed.columns else 0
            if   cur > prev * 1.001: adv += 1; up_vol += vol
            elif cur < prev * 0.999: dec += 1; dn_vol += vol
            else: unc += 1; dn_vol += vol * 0.5; up_vol += vol * 0.5

    total = adv + dec + unc
    if total == 0:
        return None

    ad_ratio = round(adv / dec, 2) if dec > 0 else float(adv)
    vol_ratio = round(up_vol / (dn_vol + 1), 2)

    # DMA calculations — vectorized per ticker using pre-grouped data
    above_20 = above_50 = above_200 = 0
    w200 = 0
    nh = nl = 0

    for tk in t_indexed.index:
        tk_data = all_data[all_data["ticker"] == tk].sort_values("date")
        # Only use data up to target date
        tk_data = tk_data[tk_data["date"] <= target_ds]
        if tk_data.empty:
            continue
        cur = tk_data["close"].iloc[-1]
        n = len(tk_data)

        if n >= 20:
            m20 = tk_data["close"].tail(20).mean()
            if cur > m20: above_20 += 1
        if n >= 50:
            m50 = tk_data["close"].tail(50).mean()
            if cur > m50: above_50 += 1
        if n >= 200:
            w200 += 1
            m200 = tk_data["close"].tail(200).mean()
            if cur > m200: above_200 += 1

        # 52w high/low
        lb = min(n, 252)
        h52 = tk_data["high"].tail(lb).max()
        l52 = tk_data["low"].tail(lb).min()
        if cur >= h52 * 0.98: nh += 1
        if cur <= l52 * 1.02: nl += 1

    p20 = round(above_20 / total * 100, 1)
    p50 = round(above_50 / total * 100, 1)
    p200 = round(above_200 / w200 * 100, 1) if w200 > 0 else 0
    nh_nl = nh - nl
    nh_nl_ratio = nh_nl / total if total > 0 else 0

    # Simplified A/D ROC and B20 accel (0 for single-date calc)
    ad_roc = 0.0
    b20_accel = 0.0

    score, components = _qbram_score(p50, nh_nl_ratio, ad_roc, p200, b20_accel, vol_ratio)
    regime = _regime(score)

    return {
        "date": target_ds,
        "universe_size": total,
        "advancers": adv, "decliners": dec, "unchanged": unc,
        "ad_ratio": ad_ratio, "vol_ratio": vol_ratio,
        "pct_above_20": p20, "pct_above_50": p50, "pct_above_200": p200,
        "with_200dma": w200,
        "new_highs": nh, "new_lows": nl, "nh_nl": nh_nl,
        "score": score, "regime": regime, "regime_color": _rcolor(regime),
        "score_components": components,
    }


# ── Optimized: compute for multiple dates at once ───────────────────────────

def _compute_multi_date(market, dates_list):
    """Efficiently compute breadth for multiple dates using bulk data load.
    Pre-computes rolling DMAs per ticker, then aggregates per date."""
    if not dates_list:
        return {}

    conn = sqlite3.connect(DB)
    earliest = min(dates_list)
    latest = max(dates_list)
    lookback_date = (datetime.strptime(earliest, "%Y-%m-%d") - timedelta(days=400)).strftime("%Y-%m-%d")

    df = pd.read_sql_query(
        "SELECT ticker, date, high, low, close, volume FROM ohlcv WHERE market=? AND date BETWEEN ? AND ? ORDER BY ticker, date",
        conn, params=(market, lookback_date, latest))
    conn.close()

    if df.empty:
        return {}

    dates_set = set(dates_list)
    all_days = sorted(df["date"].unique())
    day_idx = {d: i for i, d in enumerate(all_days)}

    # Pre-compute rolling indicators per ticker (vectorized)
    # Columns: date, ticker, close, prev_close, ma20, ma50, ma200, h252, l252
    records = []  # list of dicts per (ticker, date) for target dates only

    for tk, g in df.groupby("ticker"):
        g = g.sort_values("date").reset_index(drop=True)
        closes = g["close"].values
        highs = g["high"].values
        lows = g["low"].values
        vols = g["volume"].values
        dates = g["date"].values
        n = len(g)

        # Vectorized rolling: use pandas for DMA
        cs = g["close"]
        ma20 = cs.rolling(20, min_periods=20).mean().values
        ma50 = cs.rolling(50, min_periods=50).mean().values
        ma200 = cs.rolling(200, min_periods=200).mean().values
        h252 = g["high"].rolling(252, min_periods=20).max().values
        l252 = g["low"].rolling(252, min_periods=20).min().values

        for i in range(1, n):
            d = dates[i]
            if d not in dates_set:
                continue
            records.append({
                "date": d, "ticker": tk,
                "close": closes[i], "prev_close": closes[i-1],
                "volume": vols[i],
                "above_20": 1 if (not np.isnan(ma20[i]) and closes[i] > ma20[i]) else 0,
                "above_50": 1 if (not np.isnan(ma50[i]) and closes[i] > ma50[i]) else 0,
                "above_200": 1 if (not np.isnan(ma200[i]) and closes[i] > ma200[i]) else 0,
                "has_200": 0 if np.isnan(ma200[i]) else 1,
                "is_nh": 1 if (not np.isnan(h252[i]) and closes[i] >= h252[i] * 0.98) else 0,
                "is_nl": 1 if (not np.isnan(l252[i]) and closes[i] <= l252[i] * 1.02) else 0,
            })

    if not records:
        return {}

    rdf = pd.DataFrame(records)

    # Also need A/D ROC and B20 accel — compute 5-trading-day-ago metrics
    # We'll compute per-date aggregates and derive ROC from those
    results = {}

    for target_ds in dates_list:
        day_data = rdf[rdf["date"] == target_ds]
        if day_data.empty:
            continue

        total = len(day_data)
        # A/D
        chg_ratio = day_data["close"] / day_data["prev_close"]
        adv = int((chg_ratio > 1.001).sum())
        dec = int((chg_ratio < 0.999).sum())
        unc = total - adv - dec

        up_mask = chg_ratio > 1.001
        dn_mask = chg_ratio < 0.999
        up_vol = float(day_data.loc[up_mask, "volume"].sum())
        dn_vol = float(day_data.loc[dn_mask, "volume"].sum())

        ad_ratio = round(adv / dec, 2) if dec > 0 else float(adv)
        vol_ratio = round(up_vol / (dn_vol + 1), 2)

        above_20 = int(day_data["above_20"].sum())
        above_50 = int(day_data["above_50"].sum())
        above_200 = int(day_data["above_200"].sum())
        w200 = int(day_data["has_200"].sum())
        nh = int(day_data["is_nh"].sum())
        nl = int(day_data["is_nl"].sum())

        p20 = round(above_20 / total * 100, 1)
        p50 = round(above_50 / total * 100, 1)
        p200 = round(above_200 / w200 * 100, 1) if w200 > 0 else 0.0
        nh_nl = nh - nl
        nh_nl_ratio = nh_nl / total if total > 0 else 0

        # A/D ROC: compare with 5 trading days ago
        ti = day_idx.get(target_ds, 0)
        ad_roc = 0.0
        b20_accel = 0.0

        if ti >= 5:
            prev5_ds = all_days[ti - 5]
            d5_data = rdf[rdf["date"] == prev5_ds]
            if not d5_data.empty:
                t5 = len(d5_data)
                cr5 = d5_data["close"] / d5_data["prev_close"]
                adv5 = int((cr5 > 1.001).sum())
                dec5 = int((cr5 < 0.999).sum())
                ad_today = adv / dec if dec > 0 else float(adv)
                ad_5d = adv5 / dec5 if dec5 > 0 else float(adv5)
                ad_roc = round((ad_today - ad_5d) / max(ad_5d, 0.1) * 100, 1)
                # B20 accel
                b20_5d = int(d5_data["above_20"].sum())
                p20_today = round(above_20 / total * 100, 1)
                p20_5d_val = round(b20_5d / t5 * 100, 1) if t5 > 0 else p20_today
                b20_accel = round(p20_today - p20_5d_val, 1)

        score, components = _qbram_score(p50, nh_nl_ratio, ad_roc, p200, b20_accel, vol_ratio)
        regime = _regime(score)

        results[target_ds] = {
            "date": target_ds,
            "universe_size": total,
            "advancers": adv, "decliners": dec, "unchanged": unc,
            "ad_ratio": ad_ratio, "vol_ratio": vol_ratio,
            "pct_above_20": p20, "pct_above_50": p50, "pct_above_200": p200,
            "with_200dma": w200,
            "new_highs": nh, "new_lows": nl, "nh_nl": nh_nl,
            "score": score, "regime": regime, "regime_color": _rcolor(regime),
            "score_components": components,
        }

    return results


# ── Insight generator ───────────────────────────────────────────────────────

def _generate_insight(metrics, target_date):
    s = metrics["score"]
    r = metrics["regime"]
    p50 = metrics["pct_above_50"]
    ad = metrics["ad_ratio"]
    nh = metrics["new_highs"]
    nl = metrics["new_lows"]
    nh_nl = metrics["nh_nl"]

    dt = datetime.strptime(target_date, "%Y-%m-%d")
    date_str = dt.strftime("%B %d, %Y")

    parts = [f"On {date_str}, the market was in {r} with a Q-BRAM score of {s}."]

    if p50 < 15:
        parts.append(f"Only {p50}% of stocks were above their 50-day moving average — extreme weakness across the board.")
    elif p50 < 30:
        parts.append(f"Just {p50}% of stocks were above their 50-day MA — broad-based selling pressure.")
    elif p50 > 70:
        parts.append(f"{p50}% of stocks were above their 50-day MA — strong breadth participation.")
    elif p50 > 55:
        parts.append(f"{p50}% of stocks held above their 50-day MA — healthy market internals.")
    else:
        parts.append(f"{p50}% of stocks were above their 50-day MA.")

    if nl > 100:
        parts.append(f"There were {nl} new 52-week lows, signaling widespread capitulation.")
    elif nh > 50:
        parts.append(f"New 52-week highs hit {nh} — momentum was broad and strong.")
    elif nh_nl < -50:
        parts.append(f"Net new highs-lows at {nh_nl} indicated bearish dominance.")

    if ad < 0.3:
        parts.append("Decliners overwhelmed advancers by more than 3:1.")
    elif ad > 2.0:
        parts.append("Advancers led decliners by more than 2:1 — bullish conviction.")

    # Known events
    known = {
        "2020-03-23": "This was the COVID-19 crash bottom — global markets hit panic lows before the historic recovery rally.",
        "2020-11-09": "Vaccine news from Pfizer triggered the reopening rally, rotating into beaten-down sectors.",
        "2021-10-18": "NIFTY hit its peak near 18,600 before the distribution phase began.",
        "2022-06-17": "Aggressive Fed rate hikes triggered a global risk-off move.",
        "2023-03-28": "Markets were still processing the Adani crisis fallout.",
        "2024-06-04": "Election results triggered a strong sentiment-driven rally.",
        "2025-01-20": "Broad distribution began across mid and small caps.",
        "2025-09-26": "Panic-level readings hit as FII selling intensified.",
    }
    if target_date in known:
        parts.append(known[target_date])

    return " ".join(parts)


# ── Main entry point ────────────────────────────────────────────────────────

def compute_historical_breadth(target_date_str, market="India"):
    _ensure_cache_table()

    # Check cache first
    cached = _get_cached(target_date_str, market)
    if cached:
        cached["cached"] = True
        return cached

    t0 = time.time()

    # Parse and snap to nearest trading day
    target_dt = datetime.strptime(target_date_str, "%Y-%m-%d")
    actual_date = _nearest_trading_day(target_dt, market)

    conn = sqlite3.connect(DB)

    # Get trading days around target for score history
    all_trading_days = pd.read_sql_query(
        "SELECT DISTINCT date FROM ohlcv WHERE market=? AND date BETWEEN ? AND ? ORDER BY date",
        conn, params=(market,
                      (target_dt - timedelta(days=45)).strftime("%Y-%m-%d"),
                      (target_dt + timedelta(days=30)).strftime("%Y-%m-%d"))
    )["date"].tolist()
    conn.close()

    if not all_trading_days:
        return {"error": f"No trading data found around {target_date_str}"}

    # Find target index and get ±15 trading days
    try:
        target_idx = all_trading_days.index(actual_date)
    except ValueError:
        # Find closest
        target_idx = min(range(len(all_trading_days)),
                         key=lambda i: abs(datetime.strptime(all_trading_days[i], "%Y-%m-%d") - target_dt))
        actual_date = all_trading_days[target_idx]

    start_idx = max(0, target_idx - 15)
    end_idx = min(len(all_trading_days), target_idx + 16)
    history_dates = all_trading_days[start_idx:end_idx]

    # Compute breadth for all dates in range
    multi_results = _compute_multi_date(market, history_dates)

    if actual_date not in multi_results:
        return {"error": f"Could not compute breadth for {actual_date}"}

    target_metrics = multi_results[actual_date]

    # Build score history
    score_history = []
    for d in history_dates:
        if d in multi_results:
            r = multi_results[d]
            score_history.append({
                "date": d,
                "score": r["score"],
                "regime": r["regime"],
                "regime_color": r["regime_color"],
            })

    # Get today's breadth from the LIVE cached breadth (same as dashboard)
    # This ensures consistency — Peep Into Past shows same score as the nav bar
    today_result = None
    try:
        from cache import get_cache
        cached_breadth = get_cache(f"breadth_{market.upper()}")
        if cached_breadth and "score" in cached_breadth:
            latest_day = datetime.now().strftime("%Y-%m-%d")
            today_result = {
                "score": cached_breadth["score"],
                "regime": cached_breadth["regime"],
                "regime_color": cached_breadth.get("regime_color", "#64748b"),
                "pct_above_50": cached_breadth.get("pct_above_50", 0),
                "pct_above_200": cached_breadth.get("pct_above_200", 0),
                "ad_ratio": cached_breadth.get("ad_ratio", 0),
                "nh_nl": cached_breadth.get("nh_nl", 0),
                "new_highs": cached_breadth.get("new_highs", 0),
                "new_lows": cached_breadth.get("new_lows", 0),
                "advancers": cached_breadth.get("advancers", 0),
                "decliners": cached_breadth.get("decliners", 0),
                "universe_size": cached_breadth.get("valid", cached_breadth.get("universe_size", 0)),
            }
            logger.info(f"Peep comparison: using live cached breadth (score={today_result['score']} {today_result['regime']})")
    except Exception as e:
        logger.warning(f"Could not get live breadth for comparison: {e}")
        # Fallback: compute for the most recent trading day with good coverage
        conn = sqlite3.connect(DB)
        latest_day = conn.execute(
            "SELECT date FROM ohlcv WHERE market=? GROUP BY date HAVING COUNT(DISTINCT ticker) >= 300 ORDER BY date DESC LIMIT 1",
            (market,)
        ).fetchone()
        conn.close()
        if latest_day:
            latest_day = latest_day[0]
            today_results = _compute_multi_date(market, [latest_day])
            if latest_day in today_results:
                today_result = today_results[latest_day]

    # Build comparison
    comparison = None
    if today_result:
        score_diff = target_metrics["score"] - today_result["score"]
        if score_diff > 20:
            interp = f"Market conditions on {actual_date} were significantly better than today ({today_result['regime']}, score {today_result['score']})."
        elif score_diff > 0:
            interp = f"Market was somewhat stronger on {actual_date} compared to today ({today_result['regime']}, score {today_result['score']})."
        elif score_diff > -20:
            interp = f"Market conditions were similar — {actual_date} scored {target_metrics['score']} vs today's {today_result['score']}."
        else:
            interp = f"Market conditions on {actual_date} were significantly worse than today ({today_result['regime']}, score {today_result['score']})."

        comparison = {
            "today_date": latest_day,
            "today_score": today_result["score"],
            "today_regime": today_result["regime"],
            "today_regime_color": today_result["regime_color"],
            "today_metrics": {
                "pct_above_50": today_result["pct_above_50"],
                "pct_above_200": today_result["pct_above_200"],
                "ad_ratio": today_result["ad_ratio"],
                "nh_nl": today_result["nh_nl"],
                "new_highs": today_result["new_highs"],
                "new_lows": today_result["new_lows"],
                "advancers": today_result["advancers"],
                "decliners": today_result["decliners"],
                "universe_size": today_result["universe_size"],
            },
            "score_diff": score_diff,
            "interpretation": interp,
        }

    elapsed = round(time.time() - t0, 2)

    result = {
        "target_date": actual_date,
        "market": market,
        "universe_size": target_metrics["universe_size"],
        "score": target_metrics["score"],
        "regime": target_metrics["regime"],
        "regime_color": target_metrics["regime_color"],
        "metrics": {
            "pct_above_20": target_metrics["pct_above_20"],
            "pct_above_50": target_metrics["pct_above_50"],
            "pct_above_200": target_metrics["pct_above_200"],
            "advancers": target_metrics["advancers"],
            "decliners": target_metrics["decliners"],
            "unchanged": target_metrics["unchanged"],
            "ad_ratio": target_metrics["ad_ratio"],
            "new_highs": target_metrics["new_highs"],
            "new_lows": target_metrics["new_lows"],
            "nh_nl": target_metrics["nh_nl"],
            "vol_ratio": target_metrics["vol_ratio"],
            "with_200dma": target_metrics["with_200dma"],
        },
        "score_components": target_metrics["score_components"],
        "score_history": score_history,
        "comparison": comparison,
        "insight": _generate_insight(target_metrics, actual_date),
        "elapsed": elapsed,
        "cached": False,
    }

    # Cache result
    _set_cache(actual_date, market, result)

    return result
