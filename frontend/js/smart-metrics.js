// ════════════════════════════════════════════════════════════════════════════
// SMART METRICS — Techno-Fundamental Analysis UI
// ════════════════════════════════════════════════════════════════════════════

let _smData = null;
let _smCriteriaExpanded = false;

async function loadSmartMetrics(ticker) {
  const input = document.getElementById('sm-ticker-input');
  const t = (ticker || input?.value || '').trim().toUpperCase();
  if (!t) return;
  if (input) input.value = t;

  // Show loading
  const wrap = document.getElementById('sm-results');
  if (!wrap) return;
  wrap.innerHTML = `
    <div class="sm-loading">
      <div class="sm-skeleton-row"></div>
      <div class="sm-skeleton-grid">
        <div class="sm-skeleton-card"></div>
        <div class="sm-skeleton-card"></div>
      </div>
      <div class="sm-skeleton-card sm-skeleton-wide"></div>
    </div>`;

  // Highlight active quick chip
  document.querySelectorAll('.sm-quick-chip').forEach(c => {
    c.classList.toggle('active', c.dataset.ticker === t);
  });

  try {
    const res = await fetch(`${API}/api/smart-metrics/${encodeURIComponent(t)}`);
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const data = await res.json();
    _smData = data;
    _smCriteriaExpanded = false;
    _renderSmartMetrics(data, wrap);
  } catch (e) {
    wrap.innerHTML = `<div class="sm-error">Error loading ${t}: ${e.message}</div>`;
  }

  // Trigger AI analysis
  if (typeof loadStockAnalysis === 'function') {
    try {
      const t = document.getElementById('sm-ticker-input')?.value?.trim().toUpperCase();
      if (t) setTimeout(() => loadStockAnalysis(t), 800);
    } catch(e) {}
  }
}

function _renderSmartMetrics(d, wrap) {
  const omHtml = _renderOMPanel(d);
  const techHtml = _renderTechPanel(d);
  const smartHtml = _renderSmartPanel(d);

  wrap.innerHTML = `
    <div class="sm-header-bar">
      <span class="sm-company-name">${d.company_name || d.ticker}</span>
      ${d.sector ? `<span class="sm-sector-badge">${d.sector}</span>` : ''}
      ${d.mcap?.mcap_formatted ? `<span class="sm-mcap-badge">${d.mcap.mcap_tier} ${d.mcap.mcap_formatted}</span>` : ''}
      <span class="sm-elapsed">${d.elapsed}s</span>
    </div>
    <div class="sm-two-col">
      <div class="sm-panel sm-om-panel">${omHtml}</div>
      <div class="sm-panel sm-tech-panel">${techHtml}</div>
    </div>
    <div class="sm-panel sm-smart-panel">${smartHtml}</div>
  `;
}

// ── OM Screener Panel ───────────────────────────────────────────────────────

function _renderOMPanel(d) {
  const om = d.om_score;
  if (!om) return '<div class="sm-no-data">No fundamental data</div>';

  const pct = om.score_pct || 0;
  const circumference = 2 * Math.PI * 40; // ~251.3
  const offset = circumference - (pct / 100) * circumference;
  const ringColor = om.grade_color === 'green' ? 'var(--green)' : om.grade_color === 'amber' ? 'var(--amber)' : 'var(--red)';
  const gradeLabel = pct >= 65 ? 'GOOD' : pct >= 45 ? 'FAIR' : 'WEAK';

  // Criteria list
  const sorted = [...(om.criteria || [])].sort((a, b) => b.passed - a.passed || b.weight - a.weight);
  const showCount = _smCriteriaExpanded ? sorted.length : 6;
  const criteriaHtml = sorted.slice(0, showCount).map(c => `
    <div class="sm-criterion ${c.passed ? 'pass' : 'fail'}">
      <span class="sm-crit-icon">${c.passed ? '✓' : '✗'}</span>
      <span class="sm-crit-name">${c.name}</span>
      <span class="sm-crit-val">${c.value}</span>
    </div>`).join('');

  const toggleBtn = sorted.length > 6 ? `
    <button class="sm-show-all-btn" onclick="_toggleOMCriteria()">
      ${_smCriteriaExpanded ? '▲ Show Less' : `▼ Show All (${om.total})`}
    </button>` : '';

  return `
    <div class="sm-panel-head">
      <span class="sm-panel-title">OM SCREENER</span>
      <span class="sm-grade-badge" style="background:${ringColor}20;color:${ringColor}">${gradeLabel}</span>
    </div>
    <div class="sm-om-ring-row">
      <svg class="sm-ring" viewBox="0 0 100 100" width="90" height="90">
        <circle cx="50" cy="50" r="40" fill="none" stroke="var(--surface-border)" stroke-width="7"/>
        <circle cx="50" cy="50" r="40" fill="none" stroke="${ringColor}" stroke-width="7"
                stroke-dasharray="${circumference}" stroke-dashoffset="${offset}"
                stroke-linecap="round" transform="rotate(-90 50 50)"/>
        <text x="50" y="48" text-anchor="middle" font-size="22" font-weight="bold" fill="var(--text)">${pct}</text>
        <text x="50" y="64" text-anchor="middle" font-size="10" fill="var(--text3)">/ 100</text>
      </svg>
      <div class="sm-om-stats">
        <div class="sm-om-pass">Pass <strong>${om.pass_count}</strong> / ${om.total}</div>
        <div class="sm-om-grade">Grade <strong style="color:${ringColor}">${om.grade}</strong></div>
      </div>
    </div>
    <div class="sm-criteria-list">${criteriaHtml}</div>
    ${toggleBtn}
    <div class="sm-om-summary">${om.summary || ''}</div>
  `;
}

function _toggleOMCriteria() {
  _smCriteriaExpanded = !_smCriteriaExpanded;
  if (_smData) {
    const wrap = document.getElementById('sm-results');
    if (wrap) _renderSmartMetrics(_smData, wrap);
  }
}

// ── Technicals Panel ────────────────────────────────────────────────────────

function _renderTechPanel(d) {
  const t = d.technicals;
  if (!t || !t.has_data) return '<div class="sm-no-data">No technical data in DB</div>';

  const price = t.price ? Number(t.price).toLocaleString('en-IN', {maximumFractionDigits: 2}) : '—';
  const chgColor = (t.change_pct || 0) >= 0 ? 'var(--green)' : 'var(--red)';
  const chgSign = (t.change_pct || 0) >= 0 ? '+' : '';

  // Stage bar
  const stageNum = t.stage_num || 0;
  const stageBarHtml = [1, 2, 3, 4].map(s => {
    const active = s === stageNum;
    const colors = { 1: 'var(--blue)', 2: 'var(--green)', 3: 'var(--amber)', 4: 'var(--red)' };
    return `<div class="sm-stage-seg ${active ? 'active' : ''}" style="${active ? `background:${colors[s]};color:#fff` : ''}">
      <span>Stage ${s}</span>
    </div>`;
  }).join('');

  // RS grade color
  const rsColors = { 'A+': 'var(--green)', 'A': 'var(--green)', 'B': 'var(--blue)', 'C': 'var(--amber)', 'D': 'var(--red)' };
  const rsColor = rsColors[t.rs_grade] || 'var(--text3)';

  // A/D color
  const adColors = { 'A+': 'var(--green)', 'A': 'var(--green)', 'B': 'var(--blue)', 'C': 'var(--amber)', 'D': 'var(--red)' };
  const adColor = adColors[t.ad_rating] || 'var(--text3)';

  // Pressure color
  const pressColor = (t.pressure || 0) > 2 ? 'var(--green)' : (t.pressure || 0) < -2 ? 'var(--red)' : 'var(--amber)';

  // Tech health description
  let techDesc = '';
  if (stageNum === 2) techDesc = 'Stage 2 uptrend — ideal setup for growth. Price above key moving averages with rising momentum.';
  else if (stageNum === 1) techDesc = 'Stage 1 basing — consolidating near support. Watch for breakout above 50 DMA.';
  else if (stageNum === 3) techDesc = 'Stage 3 topping — momentum fading. Price below 50 DMA, caution warranted.';
  else if (stageNum === 4) techDesc = 'Stage 4 decline — avoid. Price below all major moving averages.';

  return `
    <div class="sm-panel-head">
      <span class="sm-panel-title">TECHNICALS</span>
      <span class="sm-stage-label" style="color:${stageNum === 2 ? 'var(--green)' : stageNum <= 1 ? 'var(--blue)' : stageNum === 3 ? 'var(--amber)' : 'var(--red)'}">${t.stage}</span>
    </div>
    <div class="sm-tech-price-row">
      <span class="sm-tech-ticker">${d.ticker}</span>
      <span class="sm-tech-price">₹${price}</span>
      <span class="sm-tech-chg" style="color:${chgColor}">${chgSign}${(t.change_pct || 0).toFixed(2)}%</span>
    </div>
    <div class="sm-stage-bar">${stageBarHtml}</div>
    <div class="sm-tech-metrics">
      <div class="sm-tech-row">
        <span class="sm-tech-label">RS RANK</span>
        <span class="sm-tech-val">${t.rs_rank}</span>
        <span class="sm-tech-badge" style="color:${rsColor}">${t.rs_grade}</span>
      </div>
      <div class="sm-tech-row">
        <span class="sm-tech-label">A/D</span>
        <span class="sm-tech-val" style="color:${adColor}">${t.ad_rating}</span>
        <span class="sm-tech-badge" style="color:var(--text3)">${t.ad_pct}%</span>
      </div>
      <div class="sm-tech-row">
        <span class="sm-tech-label">ADR%</span>
        <span class="sm-tech-val">${t.adr_pct}%</span>
        <span class="sm-tech-badge">${t.adr_pct < 3 ? 'Low' : t.adr_pct <= 6 ? 'Med' : 'High'}</span>
      </div>
      <div class="sm-tech-row">
        <span class="sm-tech-label">RELVOL</span>
        <span class="sm-tech-val">${t.rel_volume}x</span>
        <span class="sm-tech-badge">${t.rel_volume > 1.5 ? 'High' : 'Normal'}</span>
      </div>
      <div class="sm-tech-row">
        <span class="sm-tech-label">PRESSURE</span>
        <span class="sm-tech-val" style="color:${pressColor}">${t.pressure > 0 ? '+' : ''}${t.pressure}</span>
        <span class="sm-tech-badge">${t.pressure > 2 ? 'Bullish' : t.pressure < -2 ? 'Bearish' : 'Neutral'}</span>
      </div>
      <div class="sm-tech-row">
        <span class="sm-tech-label">TPR</span>
        <span class="sm-tech-val">${t.tpr}</span>
        <span class="sm-tech-badge">${t.tpr >= 60 ? 'Strong' : t.tpr >= 40 ? 'Neutral' : 'Weak'}</span>
      </div>
    </div>
    <div class="sm-tech-desc">${techDesc}</div>
  `;
}

// ── Smart Score Panel ───────────────────────────────────────────────────────

function _renderSmartPanel(d) {
  const s = d.smart_score;
  if (!s) return '';

  const score = s.score || 0;
  const circumference = 2 * Math.PI * 40;
  const offset = circumference - (score / 100) * circumference;
  const scoreColor = s.verdict_color === 'green' ? 'var(--green)' : s.verdict_color === 'amber' ? 'var(--amber)' : 'var(--red)';

  // Component bars
  const comps = s.components || {};
  const compOrder = ['fund', 'tech', 'rs', 'stage', 'tpr'];
  const compBarsHtml = compOrder.map(k => {
    const c = comps[k];
    if (!c) return '';
    const barColor = c.score >= 60 ? 'var(--green)' : c.score >= 40 ? 'var(--amber)' : 'var(--red)';
    return `
      <div class="sm-comp-item">
        <div class="sm-comp-header">
          <span class="sm-comp-label">${c.label}</span>
          <span class="sm-comp-score">${c.score}</span>
        </div>
        <div class="sm-comp-bar-bg"><div class="sm-comp-bar-fill" style="width:${c.score}%;background:${barColor}"></div></div>
        <div class="sm-comp-weight">${c.weight}%</div>
      </div>`;
  }).join('');

  // Tags
  const tagsHtml = (s.tags || []).map(tag => {
    let cls = 'sm-tag';
    const tl = tag.toLowerCase();
    if (tl.includes('caution') || tl.includes('decline') || tl.includes('weak') || tl.includes('slowdown') || tl.includes('laggard')) cls += ' sm-tag-warn';
    else if (tl.includes('uptrend') || tl.includes('strong') || tl.includes('leader') || tl.includes('high roe')) cls += ' sm-tag-good';
    return `<span class="${cls}">${tag}</span>`;
  }).join('');

  return `
    <div class="sm-smart-head">
      <svg class="sm-ring" viewBox="0 0 100 100" width="70" height="70">
        <circle cx="50" cy="50" r="40" fill="none" stroke="var(--surface-border)" stroke-width="7"/>
        <circle cx="50" cy="50" r="40" fill="none" stroke="${scoreColor}" stroke-width="7"
                stroke-dasharray="${circumference}" stroke-dashoffset="${offset}"
                stroke-linecap="round" transform="rotate(-90 50 50)"/>
        <text x="50" y="48" text-anchor="middle" font-size="22" font-weight="bold" fill="var(--text)">${score}</text>
        <text x="50" y="64" text-anchor="middle" font-size="10" fill="var(--text3)">/ 100</text>
      </svg>
      <div class="sm-smart-title-block">
        <span class="sm-smart-verdict" style="color:${scoreColor}">${s.verdict}</span>
        <span class="sm-smart-subtitle">SMART — Techno-Fundamental Rating</span>
      </div>
    </div>
    <div class="sm-comp-grid">${compBarsHtml}</div>
    ${s.insight ? `<div class="sm-insight"><strong>AI INSIGHT:</strong> ${s.insight}</div>` : ''}
    ${tagsHtml ? `<div class="sm-tags">${tagsHtml}</div>` : ''}
  `;
}

// ── Init & event listeners ──────────────────────────────────────────────────

document.addEventListener('keydown', e => {
  if (e.key === 'Enter' && document.activeElement?.id === 'sm-ticker-input') {
    loadSmartMetrics();
  }
});
