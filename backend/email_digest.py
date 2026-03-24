"""
Market Dashboard Summary — generates a daily digest from cached breadth data.
"""
from datetime import datetime
from cache import get_cache


def generate_market_summary(market: str = "India") -> dict:
    """Compile a market summary from cached breadth/screener/leaders data."""
    key = f"breadth_{market.upper() if market.upper() != 'INDIA' else 'INDIA'}"
    breadth = get_cache(key)

    if not breadth:
        return {"error": f"No cached breadth data for {market}. Load the dashboard first."}

    # Basic metrics
    score = breadth.get("score", 0)
    regime = breadth.get("regime", "UNKNOWN")
    pct_50 = breadth.get("pct_above_50", 0)
    pct_200 = breadth.get("pct_above_200", 0)
    pct_20 = breadth.get("pct_above_20", 0)
    ad_ratio = breadth.get("ad_ratio", 0)
    advancers = breadth.get("advancers", 0)
    decliners = breadth.get("decliners", 0)
    new_highs = breadth.get("new_highs", 0)
    new_lows = breadth.get("new_lows", 0)

    # Sector breadth
    sector_breadth = breadth.get("sector_breadth", [])
    sorted_sectors = sorted(sector_breadth, key=lambda x: x.get("week_return", 0), reverse=True)
    top_sectors = sorted_sectors[:3] if len(sorted_sectors) >= 3 else sorted_sectors
    bottom_sectors = sorted_sectors[-3:] if len(sorted_sectors) >= 3 else []

    # Stockbee data
    sb_key = f"stockbee_{market.upper() if market.upper() != 'INDIA' else 'INDIA'}"
    stockbee = get_cache(sb_key) or {}
    sb_up4 = stockbee.get("up_4pct", 0)
    sb_dn4 = stockbee.get("down_4pct", 0)
    sb_r5 = stockbee.get("ratio_5d", 0)
    sb_r10 = stockbee.get("ratio_10d", 0)

    summary = {
        "market": market,
        "date": datetime.utcnow().strftime("%Y-%m-%d"),
        "regime": regime,
        "score": score,
        "metrics": {
            "pct_above_50dma": round(pct_50, 1),
            "pct_above_200dma": round(pct_200, 1),
            "pct_above_20dma": round(pct_20, 1),
            "ad_ratio": round(ad_ratio, 2),
            "advancers": advancers,
            "decliners": decliners,
            "new_highs": new_highs,
            "new_lows": new_lows,
        },
        "top_sectors": [
            {"sector": s.get("sector", "?"), "week_return": round(s.get("week_return", 0), 2),
             "pct_above_50": round(s.get("pct_above_50", 0), 1)}
            for s in top_sectors
        ],
        "bottom_sectors": [
            {"sector": s.get("sector", "?"), "week_return": round(s.get("week_return", 0), 2),
             "pct_above_50": round(s.get("pct_above_50", 0), 1)}
            for s in bottom_sectors
        ],
        "stockbee": {
            "up_4pct": sb_up4,
            "down_4pct": sb_dn4,
            "ratio_5d": round(sb_r5, 2) if sb_r5 else 0,
            "ratio_10d": round(sb_r10, 2) if sb_r10 else 0,
        },
        "timestamp": datetime.utcnow().isoformat(),
    }

    return summary


def generate_summary_html(market: str = "India") -> str:
    """Generate an email-ready HTML summary."""
    data = generate_market_summary(market)
    if "error" in data:
        return f"<html><body><p style='color:red'>{data['error']}</p></body></html>"

    regime = data["regime"]
    score = data["score"]
    m = data["metrics"]

    # Regime color
    rc = "#22c55e" if regime in ("BULLISH", "RECOVERY") else "#ef4444" if regime in ("DISTRIBUTION", "BEARISH") else "#f59e0b"

    top_sec_rows = "".join(
        f'<tr><td style="padding:6px 12px;border-bottom:1px solid #eee">{s["sector"]}</td>'
        f'<td style="padding:6px 12px;border-bottom:1px solid #eee;color:{"#22c55e" if s["week_return"]>=0 else "#ef4444"}">'
        f'{s["week_return"]:+.2f}%</td>'
        f'<td style="padding:6px 12px;border-bottom:1px solid #eee">{s["pct_above_50"]}%</td></tr>'
        for s in data["top_sectors"]
    )
    bot_sec_rows = "".join(
        f'<tr><td style="padding:6px 12px;border-bottom:1px solid #eee">{s["sector"]}</td>'
        f'<td style="padding:6px 12px;border-bottom:1px solid #eee;color:{"#22c55e" if s["week_return"]>=0 else "#ef4444"}">'
        f'{s["week_return"]:+.2f}%</td>'
        f'<td style="padding:6px 12px;border-bottom:1px solid #eee">{s["pct_above_50"]}%</td></tr>'
        for s in data["bottom_sectors"]
    )

    sb = data["stockbee"]

    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width"></head>
<body style="margin:0;padding:0;background:#f5f5f5;font-family:Arial,Helvetica,sans-serif">
<div style="max-width:600px;margin:20px auto;background:#fff;border-radius:8px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,.08)">

  <!-- Header -->
  <div style="background:linear-gradient(135deg,#0f0c29,#302b63);padding:24px 28px;color:#fff">
    <div style="font-size:10px;letter-spacing:2px;opacity:.7;margin-bottom:4px">BREADTH ENGINE</div>
    <div style="font-size:22px;font-weight:700">Market Dashboard — {data["market"]}</div>
    <div style="font-size:12px;opacity:.6;margin-top:4px">{data["date"]}</div>
  </div>

  <!-- Regime + Score -->
  <div style="display:flex;padding:20px 28px;border-bottom:1px solid #eee;gap:20px">
    <div style="flex:1">
      <div style="font-size:10px;color:#888;letter-spacing:1px;margin-bottom:6px">REGIME</div>
      <div style="display:inline-block;padding:6px 16px;border-radius:6px;font-weight:700;font-size:14px;background:{rc}15;color:{rc};border:1px solid {rc}44">{regime}</div>
    </div>
    <div style="text-align:right">
      <div style="font-size:10px;color:#888;letter-spacing:1px;margin-bottom:6px">SCORE</div>
      <div style="font-size:32px;font-weight:800;color:{rc}">{score}</div>
    </div>
  </div>

  <!-- Key Metrics -->
  <div style="padding:20px 28px;border-bottom:1px solid #eee">
    <div style="font-size:11px;font-weight:700;letter-spacing:1px;color:#333;margin-bottom:12px">KEY BREADTH METRICS</div>
    <table style="width:100%;border-collapse:collapse;font-size:13px">
      <tr>
        <td style="padding:5px 0;color:#666">% Above 50 DMA</td>
        <td style="padding:5px 0;text-align:right;font-weight:700">{m["pct_above_50dma"]}%</td>
      </tr>
      <tr>
        <td style="padding:5px 0;color:#666">% Above 200 DMA</td>
        <td style="padding:5px 0;text-align:right;font-weight:700">{m["pct_above_200dma"]}%</td>
      </tr>
      <tr>
        <td style="padding:5px 0;color:#666">% Above 20 DMA</td>
        <td style="padding:5px 0;text-align:right;font-weight:700">{m["pct_above_20dma"]}%</td>
      </tr>
      <tr>
        <td style="padding:5px 0;color:#666">A/D Ratio</td>
        <td style="padding:5px 0;text-align:right;font-weight:700">{m["ad_ratio"]}</td>
      </tr>
      <tr>
        <td style="padding:5px 0;color:#666">Advancers / Decliners</td>
        <td style="padding:5px 0;text-align:right;font-weight:700">{m["advancers"]} / {m["decliners"]}</td>
      </tr>
      <tr>
        <td style="padding:5px 0;color:#666">New Highs / Lows</td>
        <td style="padding:5px 0;text-align:right;font-weight:700">{m["new_highs"]} / {m["new_lows"]}</td>
      </tr>
    </table>
  </div>

  <!-- Top Sectors -->
  <div style="padding:20px 28px;border-bottom:1px solid #eee">
    <div style="font-size:11px;font-weight:700;letter-spacing:1px;color:#333;margin-bottom:12px">TOP SECTORS (5D RETURN)</div>
    <table style="width:100%;border-collapse:collapse;font-size:13px">
      <tr style="background:#f8f8f8">
        <th style="padding:6px 12px;text-align:left;font-weight:600;font-size:10px;color:#888">Sector</th>
        <th style="padding:6px 12px;text-align:left;font-weight:600;font-size:10px;color:#888">5D Return</th>
        <th style="padding:6px 12px;text-align:left;font-weight:600;font-size:10px;color:#888">&gt;50 DMA</th>
      </tr>
      {top_sec_rows}
    </table>
  </div>

  <!-- Bottom Sectors -->
  <div style="padding:20px 28px;border-bottom:1px solid #eee">
    <div style="font-size:11px;font-weight:700;letter-spacing:1px;color:#333;margin-bottom:12px">LAGGING SECTORS</div>
    <table style="width:100%;border-collapse:collapse;font-size:13px">
      <tr style="background:#f8f8f8">
        <th style="padding:6px 12px;text-align:left;font-weight:600;font-size:10px;color:#888">Sector</th>
        <th style="padding:6px 12px;text-align:left;font-weight:600;font-size:10px;color:#888">5D Return</th>
        <th style="padding:6px 12px;text-align:left;font-weight:600;font-size:10px;color:#888">&gt;50 DMA</th>
      </tr>
      {bot_sec_rows}
    </table>
  </div>

  <!-- Stockbee Readings -->
  <div style="padding:20px 28px;border-bottom:1px solid #eee">
    <div style="font-size:11px;font-weight:700;letter-spacing:1px;color:#333;margin-bottom:12px">STOCKBEE READINGS</div>
    <table style="width:100%;border-collapse:collapse;font-size:13px">
      <tr><td style="padding:5px 0;color:#666">Up 4%+ Today</td><td style="text-align:right;font-weight:700;color:#22c55e">{sb["up_4pct"]}</td></tr>
      <tr><td style="padding:5px 0;color:#666">Down 4%+ Today</td><td style="text-align:right;font-weight:700;color:#ef4444">{sb["down_4pct"]}</td></tr>
      <tr><td style="padding:5px 0;color:#666">5-Day Ratio</td><td style="text-align:right;font-weight:700">{sb["ratio_5d"]}</td></tr>
      <tr><td style="padding:5px 0;color:#666">10-Day Ratio</td><td style="text-align:right;font-weight:700">{sb["ratio_10d"]}</td></tr>
    </table>
  </div>

  <!-- Footer -->
  <div style="padding:16px 28px;text-align:center;font-size:10px;color:#aaa;background:#fafafa">
    Generated by Breadth Engine &bull; {data["timestamp"][:19]}
  </div>

</div>
</body></html>"""

    return html
