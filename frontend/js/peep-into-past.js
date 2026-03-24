/* ═══════════════════════════════════════════════════════════════════════════
   Peep Into Past — Historical Breadth Analysis
   ═══════════════════════════════════════════════════════════════════════════ */

const PEEP_API = (typeof API !== 'undefined' ? API : 'http://localhost:8001');

const NOTABLE_DATES = [
  { date: '2020-03-23', label: 'COVID Bottom' },
  { date: '2020-11-09', label: 'Vaccine Rally' },
  { date: '2021-10-18', label: 'NIFTY Peak' },
  { date: '2022-06-17', label: 'Fed Hike Panic' },
  { date: '2023-03-28', label: 'Adani Crisis' },
  { date: '2024-06-04', label: 'Election Rally' },
  { date: '2025-01-20', label: 'Distribution Start' },
  { date: '2025-09-26', label: 'Recent Panic' },
];

let _peepChart = null;

function _peepScoreColor(s) {
  if (s >= 80) return '#22c55e';
  if (s >= 60) return '#86efac';
  if (s >= 40) return '#f59e0b';
  if (s >= 20) return '#ef4444';
  return '#7f1d1d';
}

function _peepFmtDate(ds) {
  const d = new Date(ds + 'T00:00:00');
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
}

function _peepFmtDateShort(ds) {
  const d = new Date(ds + 'T00:00:00');
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
}

/* ── Loading skeleton ────────────────────────────────────────────────────── */

function _peepShowLoading() {
  const el = document.getElementById('peep-results');
  el.innerHTML = `
    <div class="peep-loading">
      <div class="peep-loading-spinner"></div>
      <div class="peep-loading-text">Reconstructing historical breadth...</div>
      <div class="peep-loading-sub">Processing 1000+ tickers with DMA calculations — this may take 15-45 seconds on first load (cached after)</div>
    </div>`;
}

/* ── Main load function ──────────────────────────────────────────────────── */

async function loadPeepIntoPast(dateStr) {
  const input = document.getElementById('peep-date-input');
  const d = dateStr || input.value;
  if (!d) return;
  input.value = d;

  // Highlight active chip
  document.querySelectorAll('.peep-date-chip').forEach(c =>
    c.classList.toggle('active', c.dataset.date === d));

  _peepShowLoading();

  try {
    const resp = await fetch(`${PEEP_API}/api/peep-into-past?date=${d}&market=India`);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const data = await resp.json();
    if (data.error) throw new Error(data.error);
    _renderPeepResults(data);
  } catch (e) {
    document.getElementById('peep-results').innerHTML =
      `<div class="peep-error">Failed to load: ${e.message}</div>`;
  }
}

/* ── Render results ──────────────────────────────────────────────────────── */

function _renderPeepResults(d) {
  const el = document.getElementById('peep-results');
  if (!d || !d.metrics) {
    el.innerHTML = '<div class="peep-error">No data returned for this date.</div>';
    return;
  }
  const m = d.metrics || {};
  const c = d.comparison || null;
  const sc = _peepScoreColor(d.score || 0);
  // Ensure universe_size exists
  if (!d.universe_size) d.universe_size = (m.advancers||0) + (m.decliners||0) + (m.unchanged||0);

  let html = '';

  // Header with elapsed + cached badge
  html += `<div class="peep-header-bar">
    <span class="peep-date-label">${_peepFmtDate(d.target_date)}</span>
    <span class="peep-regime-badge" style="background:${sc}22;color:${sc};border:1px solid ${sc}44">${d.regime}</span>
    <span class="peep-universe-badge">${d.universe_size.toLocaleString()} stocks</span>
    ${d.cached ? '<span class="peep-cache-badge">CACHED</span>' : ''}
    <span class="peep-elapsed">${d.elapsed}s</span>
  </div>`;

  // Two-column comparison
  html += '<div class="peep-comparison">';
  html += _renderPeepColumn(d.target_date, d.score, d.regime, sc, m, 'HISTORICAL', d.universe_size);
  if (c) {
    const tsc = _peepScoreColor(c.today_score);
    const todayUniverse = c.today_metrics.universe_size || ((c.today_metrics.advancers||0) + (c.today_metrics.decliners||0) + (c.today_metrics.unchanged||0));
    html += _renderPeepColumn(c.today_date, c.today_score, c.today_regime, tsc, c.today_metrics, 'TODAY', todayUniverse);
  }
  html += '</div>';

  // Score difference indicator
  if (c) {
    const diff = c.score_diff;
    const diffColor = diff > 0 ? '#ef4444' : diff < 0 ? '#22c55e' : '#f59e0b';
    const arrow = diff > 0 ? '▼' : diff < 0 ? '▲' : '●';
    html += `<div class="peep-diff-bar" style="border-color:${diffColor}33;background:${diffColor}08">
      <span style="color:${diffColor};font-weight:700">${arrow} Score difference: ${diff > 0 ? '+' : ''}${diff}</span>
      <span class="peep-diff-interp">${c.interpretation}</span>
    </div>`;
  }

  // Score history chart
  html += `<div class="peep-panel">
    <div class="peep-panel-head">
      <span class="peep-panel-title">Q-BRAM SCORE HISTORY (±15 SESSIONS)</span>
    </div>
    <div class="peep-chart-wrap"><canvas id="peep-score-chart" height="180"></canvas></div>
  </div>`;

  // Score components
  if (d.score_components) {
    html += '<div class="peep-panel">';
    html += '<div class="peep-panel-head"><span class="peep-panel-title">SCORE COMPONENTS</span></div>';
    html += '<div class="peep-comp-grid">';
    for (const [key, comp] of Object.entries(d.score_components)) {
      const pct = Math.round(comp.points / comp.max * 100);
      const color = pct >= 70 ? '#22c55e' : pct >= 40 ? '#f59e0b' : '#ef4444';
      html += `<div class="peep-comp-item">
        <div class="peep-comp-header">
          <span class="peep-comp-label">${key.replace('_', ' ')}</span>
          <span class="peep-comp-score" style="color:${color}">${comp.points}/${comp.max}</span>
        </div>
        <div class="peep-comp-bar-bg"><div class="peep-comp-bar-fill" style="width:${pct}%;background:${color}"></div></div>
        <div class="peep-comp-val">${comp.value}${key === 'B50' || key === 'B200' || key === 'NH_NL' || key === 'B20_ACCEL' ? '%' : ''}</div>
      </div>`;
    }
    html += '</div></div>';
  }

  // Insight
  html += `<div class="peep-insight">
    <strong>INSIGHT</strong>
    <p>${d.insight}</p>
  </div>`;

  el.innerHTML = html;

  // Render chart
  _renderPeepScoreChart(d.score_history, d.target_date);
}

/* ── Comparison column ───────────────────────────────────────────────────── */

function _renderPeepColumn(date, score, regime, sc, m, label, universeSize) {
  const isToday = label === 'TODAY';
  const uSize = universeSize || m.universe_size || ((m.advancers||0) + (m.decliners||0) + (m.unchanged||0));
  return `<div class="peep-col ${isToday ? 'peep-col-today' : 'peep-col-hist'}">
    <div class="peep-col-label">${label}</div>
    <div class="peep-col-date">${_peepFmtDate(date)}</div>
    <div class="peep-score-row">
      <div class="peep-score-num" style="color:${sc}">${score}</div>
      <div class="peep-score-regime" style="background:${sc}18;color:${sc};border:1px solid ${sc}44">${regime}</div>
    </div>
    <div class="peep-metrics-list">
      ${_peepMetricRow('% > 50 DMA', m.pct_above_50 + '%', m.pct_above_50 >= 50 ? '#22c55e' : m.pct_above_50 >= 30 ? '#f59e0b' : '#ef4444')}
      ${_peepMetricRow('% > 200 DMA', m.pct_above_200 + '%', m.pct_above_200 >= 50 ? '#22c55e' : m.pct_above_200 >= 30 ? '#f59e0b' : '#ef4444')}
      ${_peepMetricRow('A/D Ratio', m.ad_ratio, m.ad_ratio >= 1.0 ? '#22c55e' : '#ef4444')}
      ${_peepMetricRow('NH - NL', m.nh_nl >= 0 ? '+' + m.nh_nl : m.nh_nl, m.nh_nl >= 0 ? '#22c55e' : '#ef4444')}
      ${_peepMetricRow('New Highs', m.new_highs, '#22c55e')}
      ${_peepMetricRow('New Lows', m.new_lows, '#ef4444')}
      ${_peepMetricRow('Advancers', m.advancers, '#22c55e')}
      ${_peepMetricRow('Decliners', m.decliners, '#ef4444')}
      ${_peepMetricRow('Universe', uSize, 'var(--text2)')}
    </div>
  </div>`;
}

function _peepMetricRow(label, value, color) {
  return `<div class="peep-metric-row">
    <span class="peep-metric-label">${label}</span>
    <span class="peep-metric-value" style="color:${color}">${value}</span>
  </div>`;
}

/* ── Score history chart ─────────────────────────────────────────────────── */

function _renderPeepScoreChart(history, targetDate) {
  if (!history || !history.length) return;

  const canvas = document.getElementById('peep-score-chart');
  if (!canvas) return;

  if (_peepChart) { _peepChart.destroy(); _peepChart = null; }

  const labels = history.map(h => _peepFmtDateShort(h.date));
  const scores = history.map(h => h.score);
  const colors = history.map(h => _peepScoreColor(h.score));
  const targetIdx = history.findIndex(h => h.date === targetDate);

  const bgColors = history.map((h, i) => i === targetIdx ? '#ffffff44' : 'transparent');
  const pointRadii = history.map((h, i) => i === targetIdx ? 7 : 3);

  const style = getComputedStyle(document.documentElement);
  const textColor = style.getPropertyValue('--text3').trim() || '#94a3b8';
  const gridColor = style.getPropertyValue('--border').trim() || '#1e293b';

  _peepChart = new Chart(canvas, {
    type: 'line',
    data: {
      labels,
      datasets: [{
        data: scores,
        borderColor: colors,
        backgroundColor: bgColors,
        pointBackgroundColor: colors,
        pointBorderColor: colors,
        pointRadius: pointRadii,
        pointHoverRadius: 6,
        borderWidth: 2,
        tension: 0.3,
        fill: false,
        segment: {
          borderColor: ctx => _peepScoreColor(ctx.p1.parsed.y),
        },
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: ctx => {
              const h = history[ctx.dataIndex];
              return `Score: ${h.score} (${h.regime})`;
            }
          }
        }
      },
      scales: {
        y: {
          min: 0, max: 100,
          grid: { color: gridColor },
          ticks: { color: textColor, font: { size: 10 } }
        },
        x: {
          grid: { display: false },
          ticks: { color: textColor, font: { size: 9 }, maxRotation: 45 }
        }
      }
    }
  });
}

/* ── Init notable date chips ─────────────────────────────────────────────── */

let _peepChipsInit = false;
function _initPeepChips() {
  if (_peepChipsInit) return;
  _peepChipsInit = true;
  const wrap = document.getElementById('peep-date-chips');
  if (!wrap) return;
  wrap.innerHTML = NOTABLE_DATES.map(nd =>
    `<span class="peep-date-chip" data-date="${nd.date}" onclick="loadPeepIntoPast('${nd.date}')">${nd.label}</span>`
  ).join('');
}

/* ── Event bindings ──────────────────────────────────────────────────────── */

document.addEventListener('DOMContentLoaded', () => {
  const input = document.getElementById('peep-date-input');
  if (input) {
    input.addEventListener('keydown', e => {
      if (e.key === 'Enter') loadPeepIntoPast();
    });
  }
});
