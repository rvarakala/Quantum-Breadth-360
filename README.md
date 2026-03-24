# Quantum Breadth 360

**Q-BRAM powered Market Breadth Intelligence Platform**

Real-time market breadth dashboard for **India (NIFTY 500)** and **US (S&P 500)**, powered by the proprietary Q-BRAM (Quantitative Breadth-Regime Alpha Model) engine.

## Features

- **Q-BRAM Regime Engine** — 3-state model (BULLISH / NEUTRAL / OVERSOLD) backtested to 62.4% accuracy on NIFTY
- **Leaders Tab** — Elite Leaders, Emerging Leaders, Under Pressure & Mean Reversion — all 4 tiers always visible
- **RS Rankings** — IBD-style M2+M3 formula matching MarketSmith, full NIFTY 500 universe
- **A/D Rating** — IBD Accumulation/Distribution 11-grade system (A+ through E)
- **Sector Health** — Sector RS scores with ↑↓ trend arrows, click-to-filter
- **6 Custom Screeners** — SVRO, Qullamaggie Breakout, Episodic Pivot, Mean Reversion, Manas Arora, VCP Minervini
- **Full-View Modal** — Paginated sortable table (20/page) with PNG/Excel/PDF export
- **Market Cap Filter** — Mega / Large / Mid / Small / Micro tier filtering
- **Breadth Charts** — A/D Line, % Above DMA, NH-NL, IV Footprint
- **Screener Tab** — 16 screeners (10 built-in + 6 custom AFL translations)
- **Scanner Tab** — Top movers, volume spikes, popular scans
- **Stockbee MB** — T2108, Up/Down 4%, 5D & 10D ratios
- **Smart Metrics** — Techno-fundamental analysis per ticker
- **Charts Tab** — Lightweight-charts OHLCV with overlays (VCP, PPV, Bull Snort, FVG, RS Line)
- **Peep Into Past** — Historical breadth analysis for any date
- **Watchlist + Alerts** — Full CRUD with price/DMA alerts
- **Light/Dark Theme** — Toggle with system preference detection

## Quick Start

```bash
cd backend
pip install -r requirements.txt
python main.py
# Open http://localhost:8001
```

## Architecture

```
Quantum-Breadth-360/
├── backend/
│   ├── main.py                 # FastAPI orchestration layer
│   ├── screeners.py            # RS Rankings, Leaders, Custom Screeners
│   ├── breadth.py              # Q-BRAM compute engine
│   ├── data_store.py           # SQLite OHLCV storage
│   ├── nse_sync.py             # NIFTY 500 Yahoo v8 sync
│   ├── charts.py               # Chart data endpoints
│   ├── smart_metrics_service.py # Techno-fundamental analysis
│   └── ... (17 modules total)
└── frontend/
    ├── index.html              # App shell
    ├── css/styles.css          # Full design system
    └── js/                     # Modular JS (one file per tab)
        ├── leaders.js          # Leaders tab engine
        ├── screeners.js        # Screeners + Scanner
        ├── app.js              # Core app, routing, theme
        └── ... (14 JS modules)
```

## Data

- **India:** NIFTY 500 universe via Yahoo Finance / SQLite local DB
- **US:** S&P 500 via Yahoo Finance
- **Benchmark:** ^CRSLDX (NIFTY 500 index) for RS calculations
- **Local DB:** 10-year OHLCV history in SQLite

## About

Built on the Q-BRAM engine — a proprietary quantitative breadth regime model with validated signal stack:
`BULLISH regime + Pocket Pivot + RS>85 + Stage 2 = 61.8% win rate, +2.74% mean 20-day return`
