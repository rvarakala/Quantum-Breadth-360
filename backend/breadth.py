"""
Q-BRAM: Quantitative Breadth Regime Assessment Model
Breadth computation, sector breadth, A/D history, DMA history, NH/NL history.
"""
import logging
import numpy as np
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor as _TPE

from cache import get_cache, set_cache
from utils import (
    safe_float, safe_download, get_close, get_change_pct,
    get_stock_data, fetch_batch,
    DB_AVAILABLE, INDIA_SECTORS, US_SECTORS,
    load_sector_map, load_ticker_universe,
)

logger = logging.getLogger(__name__)


def compute_breadth(stock_data, index_ticker):
    """
    Q-BRAM: Quantitative Breadth Regime Assessment Model
    Components:
      B50       25% — % stocks above 50 DMA
      NH-NL     20% — Net New Highs minus New Lows
      A/D ROC   20% — Advance/Decline Rate of Change (5-day momentum)
      B200      15% — % stocks above 200 DMA
      B20 Accel 10% — Acceleration in short-term breadth (20 DMA)
      Volume    10% — Up volume vs Down volume ratio
    """
    adv=dec=unc=a20=a50=a200=nh=nl=valid=w200=0
    up_vol=dn_vol=0.0

    # Historical A/D for ROC — track last 6 days
    daily_adv = [0]*6
    daily_dec = [0]*6
    daily_a20 = [0]*6   # for B20 acceleration

    for ticker, df in stock_data.items():
        if df is None or len(df)<21: continue
        valid+=1
        c  = df["Close"]
        v  = df["Volume"] if "Volume" in df.columns else None
        cur = safe_float(c.iloc[-1])
        prev= safe_float(c.iloc[-2])

        # Today's A/D
        if   cur > prev*1.001: adv+=1
        elif cur < prev*0.999: dec+=1
        else: unc+=1

        # Volume attribution
        if v is not None and len(v)>=1:
            vol_today = safe_float(v.iloc[-1])
            if cur > prev: up_vol += vol_today
            else:          dn_vol += vol_today

        # DMA checks
        if len(df)>=20:
            m20 = safe_float(c.rolling(20).mean().iloc[-1])
            if cur > m20: a20+=1
        if len(df)>=50:
            m50 = safe_float(c.rolling(50).mean().iloc[-1])
            if cur > m50: a50+=1
        if len(df)>=200:
            w200+=1
            m200 = safe_float(c.rolling(200).mean().iloc[-1])
            if cur > m200: a200+=1

        # 52-week NH/NL
        lb = min(len(df),252)
        if cur >= safe_float(df["High"].tail(lb).max())*0.98: nh+=1
        if cur <= safe_float(df["Low"].tail(lb).min())*1.02:  nl+=1

        # Historical A/D for ROC (last 5 days + today = 6)
        n = len(df)
        for d_back in range(min(6, n-2)):
            try:
                idx_cur  = n - 1 - d_back
                idx_prev = idx_cur - 1
                if idx_prev < 0: continue
                c_cur  = safe_float(c.iloc[idx_cur])
                c_prev = safe_float(c.iloc[idx_prev])
                if   c_cur > c_prev*1.001: daily_adv[d_back] += 1
                elif c_cur < c_prev*0.999: daily_dec[d_back] += 1
                # B20 accel — simple: compare to 20-bar rolling mean
                if n >= 21:
                    start = max(0, idx_cur - 19)
                    roll_mean = float(c.iloc[start:idx_cur+1].mean())
                    if c_cur > roll_mean: daily_a20[d_back] += 1
            except: continue

    if valid==0:
        return {"error":"No valid stock data. Run /api/sync/start first, or wait 1 min."}

    # ── Core metrics ─────────────────────────────────────────────────────────
    adr   = round(adv/dec, 2) if dec>0 else float(adv)
    p20   = round(a20/valid*100, 1)
    p50   = round(a50/valid*100, 1)
    p200  = round(a200/w200*100, 1) if w200>0 else 0
    nh_nl = nh - nl

    # ── A/D ROC (5-day) ──────────────────────────────────────────────────────
    # Compare today's A/D ratio vs 5 days ago
    ad_today = daily_adv[0]/daily_dec[0] if daily_dec[0]>0 else float(daily_adv[0])
    ad_5d    = daily_adv[5]/daily_dec[5] if daily_dec[5]>0 else float(daily_adv[5])
    ad_roc   = round((ad_today - ad_5d) / max(ad_5d, 0.1) * 100, 1)  # % change

    # ── B20 Acceleration ─────────────────────────────────────────────────────
    # Compare % above 20 DMA today vs 5 days ago
    p20_today = round(daily_a20[0]/valid*100, 1) if valid>0 else 0
    p20_5d    = round(daily_a20[5]/valid*100, 1) if valid>0 else 0
    b20_accel = round(p20_today - p20_5d, 1)  # point change in 5 days

    # ── Volume ratio ─────────────────────────────────────────────────────────
    vol_ratio = round(up_vol/(dn_vol+1), 2)  # +1 avoids div by zero

    # ── Q-BRAM Score (6 components) ──────────────────────────────────────────
    score, score_components = _qbram_score(p50, nh_nl, ad_roc, p200, b20_accel, vol_ratio, valid)
    regime = _regime(score)

    # ── Divergence detection ─────────────────────────────────────────────────
    div = None
    try:
        idx_df = safe_download(index_ticker)
        if not idx_df.empty and len(idx_df)>=2:
            c1 = get_close(idx_df)
            c2 = float(idx_df["Close"].dropna().iloc[-2])
            ic = (c1-c2)/c2*100 if c2 else 0
            if   ic > 0.5  and adr < 1.0: div={"type":"Narrow Rally","severity":"warning","message":"Index rising on narrow breadth — unsustainable."}
            elif ic < -0.5 and adr > 1.2: div={"type":"Stealth Strength","severity":"positive","message":"Breadth holding despite index dip — accumulation signal."}
    except: pass

    return dict(
        valid=valid, with_200dma=w200,
        advancers=adv, decliners=dec, unchanged=unc,
        ad_ratio=adr, ad_roc=ad_roc,
        pct_above_20=p20, pct_above_50=p50, pct_above_200=p200,
        b20_accel=b20_accel, vol_ratio=vol_ratio,
        new_highs=nh, new_lows=nl, nh_nl=nh_nl,
        score=score, score_components=score_components,
        regime=regime, regime_color=_rcolor(regime),
        divergence=div,
        timestamp=datetime.now(timezone.utc).isoformat()
    )

def _qbram_score(p50, nh_nl, ad_roc, p200, b20_accel, vol_ratio, total):
    """
    Q-BRAM 6-Component Scoring (0-100)
    B50       25pts — % above 50 DMA
    NH-NL     20pts — Net New Highs/Lows ratio
    A/D ROC   20pts — A/D 5-day Rate of Change
    B200      15pts — % above 200 DMA
    B20 Accel 10pts — Short-term breadth acceleration
    Volume    10pts — Up/Down volume ratio
    """
    components = {}

    # ── B50: 25 points ────────────────────────────────────────────────────────
    # 0-20%=0, 20-30%=5, 30-40%=10, 40-50%=15, 50-65%=20, 65-80%=22, >80%=25
    if   p50 >= 80: b50_pts = 25
    elif p50 >= 65: b50_pts = 22
    elif p50 >= 50: b50_pts = 20
    elif p50 >= 40: b50_pts = 15
    elif p50 >= 30: b50_pts = 10
    elif p50 >= 20: b50_pts = 5
    else:           b50_pts = 0
    components["B50"] = {"value": p50, "points": b50_pts, "max": 25, "weight": "25%"}

    # ── NH-NL: 20 points ──────────────────────────────────────────────────────
    # Ratio: nh_nl / total
    r = nh_nl/total if total>0 else 0
    if   r >= 0.20: nhnl_pts = 20
    elif r >= 0.10: nhnl_pts = 16
    elif r >= 0.05: nhnl_pts = 12
    elif r >= 0.00: nhnl_pts = 8
    elif r >= -0.10: nhnl_pts = 4
    else:            nhnl_pts = 0
    components["NH_NL"] = {"value": round(r*100,1), "points": nhnl_pts, "max": 20, "weight": "20%"}

    # ── A/D ROC: 20 points ────────────────────────────────────────────────────
    # 5-day rate of change in A/D ratio
    if   ad_roc >= 50:  adroc_pts = 20
    elif ad_roc >= 25:  adroc_pts = 16
    elif ad_roc >= 10:  adroc_pts = 12
    elif ad_roc >= 0:   adroc_pts = 8
    elif ad_roc >= -15: adroc_pts = 4
    else:               adroc_pts = 0
    components["AD_ROC"] = {"value": ad_roc, "points": adroc_pts, "max": 20, "weight": "20%"}

    # ── B200: 15 points ───────────────────────────────────────────────────────
    if   p200 >= 70: b200_pts = 15
    elif p200 >= 60: b200_pts = 12
    elif p200 >= 50: b200_pts = 9
    elif p200 >= 40: b200_pts = 6
    elif p200 >= 30: b200_pts = 3
    else:            b200_pts = 0
    components["B200"] = {"value": p200, "points": b200_pts, "max": 15, "weight": "15%"}

    # ── B20 Acceleration: 10 points ───────────────────────────────────────────
    # Point change in % above 20 DMA over 5 days
    if   b20_accel >= 15: b20_pts = 10
    elif b20_accel >= 8:  b20_pts = 8
    elif b20_accel >= 3:  b20_pts = 6
    elif b20_accel >= 0:  b20_pts = 4
    elif b20_accel >= -5: b20_pts = 2
    else:                 b20_pts = 0
    components["B20_ACCEL"] = {"value": b20_accel, "points": b20_pts, "max": 10, "weight": "10%"}

    # ── Volume Ratio: 10 points ───────────────────────────────────────────────
    # Up volume / Down volume
    if   vol_ratio >= 3.0: vol_pts = 10
    elif vol_ratio >= 2.0: vol_pts = 8
    elif vol_ratio >= 1.5: vol_pts = 6
    elif vol_ratio >= 1.0: vol_pts = 4
    elif vol_ratio >= 0.7: vol_pts = 2
    else:                  vol_pts = 0
    components["VOLUME"] = {"value": vol_ratio, "points": vol_pts, "max": 10, "weight": "10%"}

    total_score = min(100, b50_pts + nhnl_pts + adroc_pts + b200_pts + b20_pts + vol_pts)
    return total_score, components

def _regime(s):
    """Q-BRAM Regime Classification"""
    if   s >= 80: return "EXPANSION"
    elif s >= 60: return "ACCUMULATION"
    elif s >= 40: return "TRANSITION"
    elif s >= 20: return "DISTRIBUTION"
    else:         return "PANIC"

def _rcolor(r):
    return {
        "EXPANSION":    "#22c55e",   # Green
        "ACCUMULATION": "#86efac",   # Light green
        "TRANSITION":   "#f59e0b",   # Amber
        "DISTRIBUTION": "#ef4444",   # Red
        "PANIC":        "#7f1d1d",   # Dark red
    }.get(r, "#64748b")

def _regime_interp(r):
    """Q-BRAM regime descriptions matching the doc"""
    return {
        "EXPANSION":    "Broad-based rally, most stocks participating. Full risk-on, aggressive buying.",
        "ACCUMULATION": "Healthy uptrend, smart money buying. Normal long exposure, buy dips.",
        "TRANSITION":   "Mixed signals, regime uncertainty. Selective trades, wait for clarity.",
        "DISTRIBUTION": "Smart money selling, narrow leadership. Reduced exposure, tight stops.",
        "PANIC":        "Extreme fear, potential capitulation. Defensive/Cash, watch for reversal.",
    }.get(r, "Regime analysis unavailable.")

def _sector_breadth(sector_map, stock_data):
    out=[]
    for name,tickers in sector_map.items():
        a50=tot=0; rets=[]
        for t in tickers:
            df=stock_data.get(t)
            if df is None or len(df)<51: continue
            tot+=1; c=df["Close"]; cur=safe_float(c.iloc[-1])
            if cur>safe_float(c.rolling(50).mean().iloc[-1]): a50+=1
            if len(df)>=5:
                p5=safe_float(c.iloc[-5])
                if p5>0: rets.append((cur-p5)/p5*100)
        pct=round(a50/tot*100,1) if tot>0 else 0
        out.append({"sector":name,"pct_above_50":pct,
                    "week_return":round(float(np.mean(rets)),2) if rets else 0,"stocks_counted":tot})
    return sorted(out,key=lambda x:x["pct_above_50"],reverse=True)

def _ad_history(stock_data,days=252):
    dates=sorted(set(d for df in stock_data.values() if df is not None and len(df)>=2
                     for d in df.index[-days:]))[-days:]
    cum=0; out=[]
    for date_val in dates:
        adv=dec=0
        for df in stock_data.values():
            if df is None or len(df)<2 or date_val not in df.index: continue
            loc=df.index.get_loc(date_val)
            if loc==0: continue
            try:
                cur=safe_float(df["Close"].iloc[loc]); prv=safe_float(df["Close"].iloc[loc-1])
                if cur>prv*1.001: adv+=1
                elif cur<prv*0.999: dec+=1
            except: pass
        cum+=adv-dec
        out.append({"date":str(date_val)[:10],"advancers":adv,"decliners":dec,
                    "net":adv-dec,"cumulative":cum})
    return out

def _dma_history(stock_data,days=252):
    dates=sorted(set(d for df in stock_data.values() if df is not None and len(df)>=51
                     for d in df.index[-days:]))[-days:]
    out=[]
    for date_val in dates:
        a=tot=0
        for df in stock_data.values():
            if df is None or len(df)<51 or date_val not in df.index: continue
            loc=df.index.get_loc(date_val)
            if loc<50: continue
            try:
                cur=safe_float(df["Close"].iloc[loc])
                ma=float(df["Close"].iloc[loc-50:loc+1].mean())
                tot+=1
                if cur>ma: a+=1
            except: pass
        if tot>0: out.append({"date":str(date_val)[:10],"pct_above_50":round(a/tot*100,1)})
    return out

def _nh_nl_history(stock_data,days=252):
    dates=sorted(set(d for df in stock_data.values() if df is not None and len(df)>=2
                     for d in df.index[-days:]))[-days:]
    out=[]
    for date_val in dates:
        h=l=0
        for df in stock_data.values():
            if df is None or len(df)<20 or date_val not in df.index: continue
            loc=df.index.get_loc(date_val)
            try:
                cur=safe_float(df["Close"].iloc[loc])
                h52=safe_float(df["High"].iloc[max(0,loc-251):loc+1].max())
                l52=safe_float(df["Low"].iloc[max(0,loc-251):loc+1].min())
                if cur>=h52*0.98: h+=1
                if cur<=l52*1.02: l+=1
            except: pass
        out.append({"date":str(date_val)[:10],"new_highs":h,"new_lows":l,"net":h-l})
    return out

def _compute_market(market: str, custom_tickers: dict = None) -> dict:
    if custom_tickers is None:
        custom_tickers = {}

    # Load sector map from SQLite if available
    db_sector_map = {}
    if DB_AVAILABLE:
        try:
            raw_map = load_sector_map()
            if raw_map:
                for ticker, info in raw_map.items():
                    s = info['sector']
                    if s not in db_sector_map:
                        db_sector_map[s] = []
                    db_sector_map[s].append(ticker)
        except Exception as e:
            logger.warning(f"Could not load sector map: {e}")

    cfg={
        "INDIA":dict(index="^CRSLDX",index_name="NIFTY 500",vix="^INDIAVIX",nifty50="^NSEI",
                     sectors=db_sector_map if db_sector_map else INDIA_SECTORS,
                     db_market="India"),
        "US":   dict(index="^GSPC",index_name="S&P 500", vix="^VIX",
                     sectors=US_SECTORS, db_market="US"),
    }[market]
    logger.info(f"=== Computing {market} ===")
    stock_data = get_stock_data(cfg["db_market"], custom_tickers=custom_tickers)
    if not stock_data:
        return {"error":"No data available. Run /api/sync/start or check connection.",
                "market":market,"timestamp":datetime.now(timezone.utc).isoformat()}
    metrics=compute_breadth(stock_data,cfg["index"])
    if "error" in metrics:
        return {**metrics,"market":market}
    ip=ic=vv=n50=n50c=0.0
    # Fetch index, VIX, NIFTY50 SEQUENTIALLY with retry
    ip = ic = vv = n50 = n50c = 0.0

    def _dl(ticker, retries=2):
        """Download with retry — yfinance can be flaky.
        Falls back to httpx direct Yahoo Finance API if yfinance fails."""
        import time
        # Try yfinance first
        for attempt in range(retries + 1):
            try:
                df = safe_download(ticker, period="10d")
                if df is not None and not df.empty and "Close" in df.columns:
                    return df
                logger.warning(f"Empty result for {ticker} (attempt {attempt+1})")
            except Exception as e:
                logger.warning(f"Download {ticker} attempt {attempt+1} failed: {e}")
            if attempt < retries:
                time.sleep(1)

        # Fallback: direct Yahoo Finance v8 API via httpx
        try:
            import httpx
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
            params = {"range": "10d", "interval": "1d"}
            headers = {"User-Agent": "Mozilla/5.0"}
            resp = httpx.get(url, params=params, headers=headers, timeout=15, follow_redirects=True)
            if resp.status_code == 200:
                data = resp.json()
                result = data.get("chart", {}).get("result", [])
                if result:
                    meta = result[0].get("meta", {})
                    price = meta.get("regularMarketPrice", 0)
                    prev = meta.get("chartPreviousClose", 0)
                    if price > 0:
                        chg_pct = ((price - prev) / prev * 100) if prev > 0 else 0
                        # Build a minimal DataFrame
                        import pandas as _pd
                        df = _pd.DataFrame({
                            "Close": [prev, price],
                            "Open": [prev, price],
                            "High": [prev, price],
                            "Low": [prev, price],
                            "Volume": [0, 0]
                        })
                        logger.info(f"Fallback API got {ticker}: {price} ({chg_pct:+.2f}%)")
                        return df
            logger.warning(f"Fallback API also failed for {ticker}: HTTP {resp.status_code}")
        except Exception as e:
            logger.warning(f"Fallback API error for {ticker}: {e}")

        import pandas as _pd
        return _pd.DataFrame()

    try:
        df = _dl(cfg["index"])
        ip, ic = get_close(df), get_change_pct(df)
        logger.info(f"Index {cfg['index']}: {ip} ({ic}%)")
    except Exception as e:
        logger.error(f"Index fetch failed: {e}")

    try:
        df = _dl(cfg["vix"])
        vv = get_close(df)
        logger.info(f"VIX {cfg['vix']}: {vv}")
    except Exception as e:
        logger.error(f"VIX fetch failed: {e}")

    if market == "INDIA" and cfg.get("nifty50"):
        try:
            df = _dl(cfg["nifty50"])
            n50, n50c = get_close(df), get_change_pct(df)
            logger.info(f"NIFTY50 {cfg['nifty50']}: {n50} ({n50c}%)")
        except Exception as e:
            logger.error(f"NIFTY50 fetch failed: {e}")

    logger.info(f"Live prices: index={ip} vix={vv} nifty50={n50}")

    # Cache last good values — fallback when yfinance fails
    _idx_cache_key = f"_index_prices_{market}"
    if ip > 0 or vv > 0 or n50 > 0:
        # Got at least some data — save it
        from cache import set_cache, get_cache
        set_cache(_idx_cache_key, {"ip": ip, "ic": ic, "vv": vv, "n50": n50, "n50c": n50c})
    else:
        # All zeros — try to use cached values
        from cache import get_cache
        cached_idx = get_cache(_idx_cache_key)
        if cached_idx:
            ip = cached_idx.get("ip", 0)
            ic = cached_idx.get("ic", 0)
            vv = cached_idx.get("vv", 0)
            n50 = cached_idx.get("n50", 0)
            n50c = cached_idx.get("n50c", 0)
            logger.info(f"Using cached index prices: index={ip} vix={vv} nifty50={n50}")

    # Use up to 252 days for charts (1 year), or all available if more
    chart_days = 252
    # ── Determine last OHLCV date across universe ──────────────────────────
    last_ohlcv_date = "unknown"
    try:
        import sqlite3 as _sql, pathlib as _pl
        _db = _pl.Path(__file__).parent / "breadth_data.db"
        if _db.exists():
            _conn = _sql.connect(str(_db), timeout=10)
            _row  = _conn.execute(
                "SELECT MAX(date) FROM ohlcv WHERE market='India'"
            ).fetchone()
            _conn.close()
            if _row and _row[0]:
                last_ohlcv_date = _row[0]   # e.g. "2026-03-21"
    except Exception:
        pass

    # Is data from today or yesterday?
    from datetime import date as _date
    _today = _date.today().isoformat()
    _data_freshness = "today" if last_ohlcv_date == _today else (
        "EOD" if last_ohlcv_date >= (_date.today().replace(
            day=_date.today().day-1)).isoformat() else "stale"
    )

    return {**metrics,"market":market,"index_name":cfg["index_name"],"nifty50_price":round(n50,2),"nifty50_change_pct":round(n50c,2),
            "index_price":round(ip,2),"index_change_pct":round(ic,2),"vix":round(vv,2),
            "ad_history":_ad_history(stock_data,chart_days),
            "dma_history":_dma_history(stock_data,chart_days),
            "nh_nl_history":_nh_nl_history(stock_data,chart_days),
            "sector_breadth":_sector_breadth(cfg["sectors"],stock_data),
            "universe_size":len(stock_data),
            "data_source":"local_db" if DB_AVAILABLE else "live_yfinance",
            "last_ohlcv_date": last_ohlcv_date,
            "data_freshness":  _data_freshness,
            "computed_at":     datetime.now(timezone.utc).isoformat()}


