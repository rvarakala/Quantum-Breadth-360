// ─── CONFIG ─────────────────────────────────────────────────────────────────
const API = 'http://localhost:8001';
let currentMarket = 'INDIA';
let currentData = {};
let charts = {};
let lastUpdated = {};

// ─── UTILITIES ───────────────────────────────────────────────────────────────
const $ = id => document.getElementById(id);
const fmt = (v, d=1) => v === null || v === undefined || v !== v ? '—' : Number(v).toFixed(d);
const fmtPct = v => v !== null && v !== undefined && v === v ? `${Number(v).toFixed(1)}%` : '—';
const fmtK = v => v >= 1000 ? `${(v/1000).toFixed(1)}K` : String(v ?? '—');

function scoreColor(s) {
  if (s >= 80) return '#22c55e';
  if (s >= 65) return '#86efac';
  if (s >= 50) return '#f59e0b';
  if (s >= 35) return '#fb923c';
  if (s >= 20) return '#ef4444';
  return '#7f1d1d';
}

function regimeInterpretation(regime, score) {
  const map = {
    'EXPANSION': 'Broad-based rally, most stocks participating. Full risk-on, aggressive buying.',
    'ACCUMULATION': 'Healthy uptrend, smart money buying. Normal long exposure, buy dips.',
    'TRANSITION': 'Mixed signals, regime uncertainty. Selective trades, wait for clarity.',
    'DISTRIBUTION': 'Smart money selling, narrow leadership. Reduced exposure, tight stops.',
    'PANIC': 'Extreme fear, potential capitulation. Defensive/Cash, watch for reversal.',
    'BULLISH': 'Momentum breakouts favoured — all tiers active.',
    'NEUTRAL': 'Selective conditions — highest conviction only.',
    'OVERSOLD': 'Avoid breakouts — mean reversion candidates shown.',
  };
  return map[regime] || `Market regime: ${regime}. Monitor conditions closely.`;
}

function vixLevel(v, market) {
  if (market === 'INDIA') {
    if (v > 25) return ['ELEVATED', '#ef4444', 'rgba(239,68,68,.1)'];
    if (v > 18) return ['MODERATE', '#f59e0b', 'rgba(245,158,11,.1)'];
    return ['CALM', '#22c55e', 'rgba(34,197,94,.1)'];
  } else {
    if (v > 30) return ['PANIC', '#ef4444', 'rgba(239,68,68,.1)'];
    if (v > 20) return ['ELEVATED', '#f59e0b', 'rgba(245,158,11,.1)'];
    return ['LOW', '#22c55e', 'rgba(34,197,94,.1)'];
  }
}

// ─── CHART SETUP ─────────────────────────────────────────────────────────────
Chart.defaults.color = '#4b5e7a';
Chart.defaults.borderColor = '#1e2d4a';
Chart.defaults.font.family = "'Space Mono', monospace";
Chart.defaults.font.size = 9;

function makeLineChart(canvasId, labels, datasets, opts = {}) {
  const ctx = $(canvasId).getContext('2d');
  if (charts[canvasId]) charts[canvasId].destroy();
  charts[canvasId] = new Chart(ctx, {
    type: 'line',
    data: { labels, datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      animation: { duration: 800, easing: 'easeInOutQuart' },
      plugins: { legend: { display: false }, tooltip: {
        backgroundColor: '#111827', borderColor: '#253552', borderWidth: 1,
        titleFont: { family: "'Space Mono', monospace", size: 9 },
        bodyFont:  { family: "'Space Mono', monospace", size: 10 },
        padding: 10,
      }},
      scales: {
        x: { grid: { color: '#1e2d4a44' }, ticks: { maxTicksLimit: 6, maxRotation: 0 } },
        y: { grid: { color: '#1e2d4a44' }, ...opts.y },
      },
      elements: { point: { radius: 0, hoverRadius: 4 }, line: { tension: 0.3 } },
      ...opts
    }
  });
}

function makeBarChart(canvasId, labels, datasets, opts = {}) {
  const ctx = $(canvasId).getContext('2d');
  if (charts[canvasId]) charts[canvasId].destroy();
  charts[canvasId] = new Chart(ctx, {
    type: 'bar',
    data: { labels, datasets },
    options: {
      responsive: true, maintainAspectRatio: false,
      animation: { duration: 600 },
      plugins: { legend: { display: false }, tooltip: {
        backgroundColor: '#111827', borderColor: '#253552', borderWidth: 1,
        titleFont: { family: "'Space Mono', monospace", size: 9 },
        bodyFont:  { family: "'Space Mono', monospace", size: 10 },
        padding: 10,
      }},
      scales: {
        x: { grid: { display: false }, ticks: { maxRotation: 30 } },
        y: { grid: { color: '#1e2d4a44' }, ...opts.y },
      },
      ...opts
    }
  });
}

