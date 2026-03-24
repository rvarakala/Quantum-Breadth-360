// ─── RENDER FUNCTIONS ─────────────────────────────────────────────────────────

function renderOverview(d) {
  if (!d || d.error) return;
  const score = d.score ?? 0;
  const sc = scoreColor(score);
  const regime = d.regime ?? 'UNKNOWN';

  // Score card
  $('score-value').textContent = score;
  $('score-value').style.color = sc;
  $('score-market-label').textContent = d.market === 'INDIA' ? 'INDIA — NIFTY 500 Breadth' : `US — S&P 500 Breadth`;
  const rp = $('regime-pill');
  rp.textContent = regime;
  rp.style.background = sc + '18';
  rp.style.border = `1px solid ${sc}44`;
  rp.style.color = sc;
  $('score-interp').textContent = regimeInterpretation(regime, score);

  // Show data freshness notice on overview
  const _ohlcvDate = d.last_ohlcv_date;
  const _fresh = d.data_freshness;
  const _noticeEl = document.getElementById('data-freshness-notice');
  if (_noticeEl && _ohlcvDate && _ohlcvDate !== 'unknown') {
    const _fmt = new Date(_ohlcvDate).toLocaleDateString('en-IN',
      {day:'2-digit',month:'short',year:'numeric'});
    if (_fresh === 'stale' || _fresh === 'EOD') {
      _noticeEl.style.display = 'flex';
      _noticeEl.innerHTML = `<span style="color:#f59e0b">⚠</span>
        <span>Data as of <strong>${_fmt}</strong> EOD —
        <button onclick="startNseForceSync()" style="background:none;border:none;
          color:var(--accent1);cursor:pointer;font-family:var(--font-mono);
          font-size:10px;padding:0;text-decoration:underline">
          Sync today's data
        </button></span>`;
    } else {
      _noticeEl.style.display = 'none';
    }
  }
  $('universe-size').textContent = d.universe_size ?? d.valid ?? '—';
  $('valid-count').textContent = d.valid ?? '—';

  // Metrics
  const p50 = d.pct_above_50 ?? 0;
  const p200 = d.pct_above_200 ?? 0;
  const p20 = d.pct_above_20 ?? 0;
  const adr = d.ad_ratio ?? 0;

  $('pct50').textContent = fmtPct(p50);
  $('pct50').style.color = p50 >= 60 ? '#22c55e' : p50 >= 40 ? '#f59e0b' : '#ef4444';
  $('pct50-sub').textContent = p50 >= 60 ? '▲ Bullish zone' : p50 >= 40 ? '⚡ Neutral zone' : '▼ Bearish zone';
  $('pct50-bar').style.width = p50 + '%';

  $('pct200').textContent = fmtPct(p200);
  $('pct200').style.color = p200 >= 60 ? '#22c55e' : p200 >= 40 ? '#f59e0b' : '#ef4444';
  $('pct200-sub').textContent = `${d.with_200dma ?? '?'} stocks with 200D data`;
  $('pct200-bar').style.width = p200 + '%';

  $('pct20').textContent = fmtPct(p20);
  $('pct20').style.color = p20 >= 60 ? '#22c55e' : p20 >= 40 ? '#f59e0b' : '#ef4444';
  $('pct20-sub').textContent = p20 >= 60 ? '▲ Short-term healthy' : '▼ Short-term weak';
  $('pct20-bar').style.width = p20 + '%';

  $('ad-ratio').textContent = fmt(adr, 2);
  $('ad-ratio').style.color = adr >= 1.5 ? '#22c55e' : adr >= 1 ? '#86efac' : adr >= 0.7 ? '#f59e0b' : '#ef4444';
  $('ad-ratio-sub').textContent = adr >= 1 ? `${d.advancers} up vs ${d.decliners} down` : 'Decliners dominating';
  $('ad-ratio-bar').style.width = Math.min(adr / 3 * 100, 100) + '%';
  $('ad-ratio-bar').style.background = adr >= 1.2 ? 'var(--green)' : adr >= 1 ? 'var(--amber)' : 'var(--red)';

  $('new-highs').textContent = d.new_highs ?? '—';
  $('new-highs-sub').textContent = `52-week new highs`;
  $('new-lows').textContent = d.new_lows ?? '—';
  $('new-lows-sub').textContent = `52-week new lows`;

  // VIX side card — sanity check (India VIX: 5-80, US VIX: 5-100)
  let vix = d.vix ?? 0;
  const maxVix = d.market === 'INDIA' ? 80 : 100;
  if (vix < 5 || vix > maxVix) vix = 0; // invalid value, show N/A
  const [vlabel, vcolor, vbg] = vixLevel(vix, d.market);
  $('vix-val').textContent = vix > 0 ? fmt(vix, 1) : 'N/A';
  $('vix-val').style.color = vix > 0 ? vcolor : 'var(--text3)';
  const vl = $('vix-level');
  vl.textContent = vix > 0 ? vlabel : '—';
  vl.style.background = vix > 0 ? vbg : 'transparent';
  vl.style.color = vix > 0 ? vcolor : 'var(--text3)';
  vl.style.border = `1px solid ${vix > 0 ? vcolor : 'var(--border)'}44`;

  $('index-label').textContent = d.index_name ?? 'INDEX';
  $('index-price').textContent = fmtK(d.index_price);
  const ic = d.index_change_pct ?? 0;
  $('index-chg').textContent = `${ic >= 0 ? '+' : ''}${fmt(ic, 2)}%`;
  $('index-chg').style.color = ic >= 0 ? '#22c55e' : '#ef4444';

  const adv = d.advancers ?? 0;
  const dec = d.decliners ?? 0;
  const unc = d.unchanged ?? 0;
  const total = adv + dec + unc || 1;
  $('adv-count').textContent = adv;
  $('dec-count').textContent = dec;
  $('unch-count').textContent = unc;
  const advPct = (adv / total * 100).toFixed(0);
  const decPct = (dec / total * 100).toFixed(0);
  $('ad-bar-adv').style.width = advPct + '%';
  $('ad-bar-dec').style.width = decPct + '%';
  $('adv-pct').textContent = advPct + '% ADV';
  $('dec-pct').textContent = decPct + '% DEC';

  const nhNl = d.nh_nl ?? 0;
  $('nh-nl-val').textContent = nhNl >= 0 ? `+${nhNl}` : String(nhNl);
  $('nh-nl-val').style.color = nhNl > 0 ? '#22c55e' : nhNl < 0 ? '#ef4444' : '#f59e0b';
  const bar = $('nh-nl-bar');
  const pct = Math.min(Math.abs(nhNl) / (total * 0.3) * 50, 50);
  if (nhNl >= 0) {
    bar.style.left = '50%'; bar.style.width = pct + '%';
    bar.style.background = '#22c55e';
  } else {
    bar.style.right = '50%'; bar.style.left = ''; bar.style.width = pct + '%';
    bar.style.background = '#ef4444';
  }

  // Divergence
  const div = d.divergence;
  const da = $('divergence-alert');
  if (div) {
    da.style.display = 'flex';
    da.className = `alert-box ${div.severity}`;
    const icon = div.severity === 'warning' ? '⚠️' : '✅';
    da.innerHTML = `<span class="alert-icon">${icon}</span>
      <div class="alert-content">
        <h4 style="color:${div.severity==='warning'?'#f59e0b':'#22c55e'}">${div.type.toUpperCase()}</h4>
        <p>${div.message}</p>
      </div>`;
  } else {
    da.style.display = 'none';
  }

  // Nav bar update
  if (d.market === 'INDIA') {
    // Show NIFTY 50 in nav header (benchmark index), NIFTY 500 in side card
    const n50p = d.nifty50_price || d.index_price || 0;
    const n50c = d.nifty50_change_pct ?? ic;
    $('nav-nifty-price').textContent = n50p > 0 ? n50p.toLocaleString('en-IN', {maximumFractionDigits:1}) : '—';
    $('nav-nifty-chg').textContent = n50c >= 0 ? `+${fmt(n50c,2)}%` : `${fmt(n50c,2)}%`;
    $('nav-nifty-chg').style.color = n50c >= 0 ? '#22c55e' : '#ef4444';
  } else {
    $('nav-sp-price').textContent = d.index_price > 0 ? d.index_price.toLocaleString('en-US', {maximumFractionDigits:1}) : '—';
    $('nav-sp-chg').textContent = ic >= 0 ? `+${fmt(ic,2)}%` : `${fmt(ic,2)}%`;
    $('nav-sp-chg').style.color = ic >= 0 ? '#22c55e' : '#ef4444';
  }
  // VIX in nav bar
  const vixVal = d.vix;
  if (vixVal != null && !isNaN(vixVal)) {
    const vEl = $('nav-vix-price');
    if (vEl) {
      vEl.textContent = Number(vixVal).toFixed(1);
      const [, vColor] = vixLevel(vixVal, d.market || 'INDIA');
      vEl.style.color = vColor;
    }
  }

  $('nav-score').textContent = score;
  $('nav-score').style.color = sc;
  $('nav-regime').textContent = regime;
  $('nav-regime').style.background = sc + '18';
  $('nav-regime').style.border = `1px solid ${sc}44`;
  $('nav-regime').style.color = sc;

  // New components
  renderLiquidityStress(d);
  renderQBRAMAlerts(d);
}

function renderCharts(d) {
  if (!d || d.error) return;

  // A-D Line (show last 25 days)
  const adHRaw = d.ad_history ?? [];
  const adH = adHRaw.slice(-25);
  if (adH.length > 0) {
    const labels = adH.map(x => x.date.slice(5)); // MM-DD
    const cumVals = adH.map(x => x.cumulative);
    const minCum = Math.min(...cumVals);
    const maxCum = Math.max(...cumVals);
    const gradient = charts['chart-ad']?.ctx;
    makeLineChart('chart-ad', labels, [{
      data: cumVals,
      borderColor: '#3b82f6',
      backgroundColor: 'rgba(59,130,246,0.07)',
      fill: true,
      borderWidth: 1.5,
    }], { y: { min: minCum * 0.99, max: maxCum * 1.01 } });
  }

  // % Above 50 DMA (show last 25 days)
  const dmaHRaw = d.dma_history ?? [];
  const dmaH = dmaHRaw.slice(-25);
  if (dmaH.length > 0) {
    const labels = dmaH.map(x => x.date.slice(5));
    const vals = dmaH.map(x => x.pct_above_50);
    const colors = vals.map(v => v >= 60 ? 'rgba(34,197,94,0.8)' : v >= 40 ? 'rgba(245,158,11,0.8)' : 'rgba(239,68,68,0.8)');
    makeLineChart('chart-dma', labels, [{
      data: vals,
      borderColor: '#a855f7',
      backgroundColor: 'rgba(168,85,247,0.06)',
      fill: true,
      borderWidth: 1.5,
      pointBackgroundColor: colors,
      pointRadius: 2,
    }], { y: { min: 0, max: 100 } });
  }

  // NH-NL (show last 25 days)
  const nhHRaw = d.nh_nl_history ?? [];
  const nhH = nhHRaw.slice(-25);
  if (nhH.length > 0) {
    const labels = nhH.map(x => x.date.slice(5));
    const nets = nhH.map(x => x.net);
    const colors = nets.map(v => v >= 0 ? 'rgba(34,197,94,0.75)' : 'rgba(239,68,68,0.75)');
    makeBarChart('chart-nh', labels, [{
      data: nets,
      backgroundColor: colors,
      borderWidth: 0,
    }]);
  }

  // Score gauge zones
  const zones = $('score-zones');
  const score = d.score ?? 0;
  const zoneData = [
    { label: 'PANIC', pct: 20, color: '#7f1d1d' },
    { label: 'DISTRIBUTION', pct: 20, color: '#ef4444' },
    { label: 'TRANSITION', pct: 20, color: '#f59e0b' },
    { label: 'ACCUMULATION', pct: 20, color: '#86efac' },
    { label: 'EXPANSION', pct: 20, color: '#22c55e' },
  ];
  zones.innerHTML = zoneData.map(z => `
    <div style="flex:${z.pct};background:${z.color}20;border-right:1px solid #1e2d4a22;
      display:flex;align-items:center;justify-content:center;font-size:8px;
      font-family:var(--font-mono);color:${z.color};letter-spacing:.08em">
      ${z.label}
    </div>`).join('');

  // Score marker
  const marker = document.createElement('div');
  marker.style.cssText = `position:absolute;top:-6px;left:${score}%;
    width:2px;height:40px;background:white;border-radius:2px;
    box-shadow:0 0 8px white;transform:translateX(-50%);transition:left .8s ease`;
  zones.style.position = 'relative';
  zones.appendChild(marker);

  // New breadth chart components
  renderRegimeTimeline(d);
  renderScoreHistory(d);  // async — fetches real scores from DB
  _ivFootprintLoaded = false; // reset so it reloads on refresh
  renderIVFootprint();
}

function renderSectors(d) {
  if (!d || !d.sector_breadth?.length) return;
  const sectors = d.sector_breadth;
  const grid = $('sector-grid');

  grid.innerHTML = sectors.map(s => {
    const pct = s.pct_above_50 ?? 0;
    const ret = s.week_return ?? 0;
    let bg, tc, bc;
    if (pct >= 70) { bg='rgba(34,197,94,.08)'; tc='#22c55e'; bc='#22c55e'; }
    else if (pct >= 50) { bg='rgba(34,197,94,.04)'; tc='#86efac'; bc='#86efac'; }
    else if (pct >= 35) { bg='rgba(245,158,11,.06)'; tc='#f59e0b'; bc='#f59e0b'; }
    else { bg='rgba(239,68,68,.06)'; tc='#ef4444'; bc='#ef4444'; }

    return `<div class="sector-tile" style="background:${bg};border-color:${bc}22">
      <div class="sector-name" style="color:${tc}">${s.sector}</div>
      <div class="sector-pct" style="color:${tc}">${fmtPct(pct)}</div>
      <div class="sector-ret" style="color:${ret>=0?'#22c55e':'#ef4444'}">
        ${ret>=0?'▲':'▼'} ${Math.abs(ret).toFixed(1)}% (5D)
      </div>
      <div class="sector-bar"><div class="sector-bar-fill" style="width:${pct}%;background:${bc}"></div></div>
      <div style="position:absolute;bottom:0;left:0;right:0;height:2px;background:${bc}40"></div>
    </div>`;
  }).join('');

  // Sector bar chart
  const labels = sectors.map(s => s.sector);
  const vals = sectors.map(s => s.week_return ?? 0);
  const colors = vals.map(v => v >= 0 ? 'rgba(34,197,94,0.7)' : 'rgba(239,68,68,0.7)');
  makeBarChart('chart-sector-bar', labels, [{
    data: vals, backgroundColor: colors, borderWidth: 0,
    label: '5D Return %',
  }], { y: {} });
}

function renderCompare(india, us) {
  function fill(prefix, d) {
    if (!d) return;
    const sc = scoreColor(d.score ?? 0);
    $(`cmp-${prefix}-score`).textContent = d.score ?? '—';
    $(`cmp-${prefix}-score`).style.color = sc;
    const rp = $(`cmp-${prefix}-regime`);
    rp.textContent = d.regime ?? '—';
    rp.style.color = sc;
    rp.style.background = sc + '18';
    rp.style.border = `1px solid ${sc}44`;
    $(`cmp-${prefix}-50`).textContent = fmtPct(d.pct_above_50);
    $(`cmp-${prefix}-200`).textContent = fmtPct(d.pct_above_200);
    $(`cmp-${prefix}-ad`).textContent = fmt(d.ad_ratio, 2);
    const nhNl = d.nh_nl ?? 0;
    $(`cmp-${prefix}-nhc`).textContent = nhNl >= 0 ? `+${nhNl}` : String(nhNl);
    $(`cmp-${prefix}-nhc`).style.color = nhNl > 0 ? '#22c55e' : '#ef4444';
    $(`cmp-${prefix}-vix`).textContent = fmt(d.vix, 1);
    const ic = d.index_change_pct ?? 0;
    $(`cmp-${prefix}-idxchg`).textContent = `${ic>=0?'+':''}${fmt(ic,2)}%`;
    $(`cmp-${prefix}-idxchg`).style.color = ic >= 0 ? '#22c55e' : '#ef4444';
  }
  fill('india', india);
  fill('us', us);

  // Comparison bar chart
  if (india && us) {
    const labels = ['Score', '% >50D', '% >200D', 'A/D ×10', 'VIX ÷3'];
    const indiaVals = [
      india.score ?? 0, india.pct_above_50 ?? 0, india.pct_above_200 ?? 0,
      (india.ad_ratio ?? 0) * 10, (india.vix ?? 0) / 3,
    ];
    const usVals = [
      us.score ?? 0, us.pct_above_50 ?? 0, us.pct_above_200 ?? 0,
      (us.ad_ratio ?? 0) * 10, (us.vix ?? 0) / 3,
    ];
    makeBarChart('chart-compare', labels, [
      { label: 'India', data: indiaVals, backgroundColor: 'rgba(249,115,22,0.7)', borderWidth: 0 },
      { label: 'US', data: usVals, backgroundColor: 'rgba(59,130,246,0.7)', borderWidth: 0 },
    ], {
      plugins: { legend: { display: true, labels: { color: '#94a3b8', font: { family: "'Space Mono'" } } } }
    });
  }
}

// ════════════════════════════════════════════════════════════════════════════════
// COMPONENT 1: LIQUIDITY STRESS MONITOR
// ════════════════════════════════════════════════════════════════════════════════

function _stressFromValue(val, threshLow, threshHigh, invert) {
  // Returns 0-100 stress. invert=true means lower value = more stress.
  if (invert) {
    if (val <= threshLow) return 100;
    if (val >= threshHigh) return 0;
    return Math.round((1 - (val - threshLow) / (threshHigh - threshLow)) * 100);
  }
  if (val >= threshHigh) return 100;
  if (val <= threshLow) return 0;
  return Math.round((val - threshLow) / (threshHigh - threshLow) * 100);
}

function renderLiquidityStress(d) {
  const el = document.getElementById('liq-stress-card');
  if (!el) return;

  const volRatio = d.vol_ratio ?? 1;
  const p50 = d.pct_above_50 ?? 50;
  const adr = d.ad_ratio ?? 1;
  const nhNl = d.nh_nl ?? 0;
  const vix = d.vix ?? 15;
  const total = d.valid ?? 500;

  // Compute individual stress (0-100, higher = more stress)
  const volStress = _stressFromValue(volRatio, 0.3, 1.5, true);
  const breadthStress = _stressFromValue(p50, 15, 70, true);
  const adStress = _stressFromValue(adr, 0.5, 1.5, true);
  const nlRatio = d.new_lows ? (d.new_lows / Math.max(d.new_highs || 1, 1)) : 0;
  const nhStress = _stressFromValue(nlRatio, 0.5, 4.0, false);
  const vixStress = _stressFromValue(vix, 12, 35, false);

  // Composite: weighted average
  const composite = Math.round(
    volStress * 0.25 + breadthStress * 0.25 + adStress * 0.20 + nhStress * 0.15 + vixStress * 0.15
  );

  const stressLabel = composite >= 75 ? 'EXTREME' : composite >= 50 ? 'ELEVATED' : composite >= 25 ? 'MODERATE' : 'LOW STRESS';
  const stressColor = composite >= 75 ? '#ef4444' : composite >= 50 ? '#f59e0b' : composite >= 25 ? '#eab308' : '#22c55e';

  const components = [
    { name: 'Volume', val: volStress, raw: `UpVol/DnVol: ${volRatio}` },
    { name: 'Breadth', val: breadthStress, raw: `%>50D: ${p50.toFixed(1)}%` },
    { name: 'A/D', val: adStress, raw: `A/D: ${adr.toFixed(2)}` },
    { name: 'NH-NL', val: nhStress, raw: `NL/NH: ${nlRatio.toFixed(1)}` },
    { name: 'VIX', val: vixStress, raw: `VIX: ${vix.toFixed(1)}` },
  ];

  el.innerHTML = `
    <div class="ls-header">
      <span class="ls-title">LIQUIDITY STRESS MONITOR</span>
      <span class="ls-badge" style="background:${stressColor}18;color:${stressColor};border:1px solid ${stressColor}44">${stressLabel}</span>
    </div>
    <div class="ls-gauge-row">
      <div class="ls-gauge-wrap">
        <div class="ls-gauge-bg">
          <div class="ls-gauge-fill" style="width:${composite}%;background:${stressColor}"></div>
        </div>
        <div class="ls-gauge-labels">
          <span>0</span><span>25</span><span>50</span><span>75</span><span>100</span>
        </div>
      </div>
      <div class="ls-score" style="color:${stressColor}">${composite}</div>
    </div>
    <div class="ls-components">
      ${components.map(c => {
        const cc = c.val >= 60 ? '#ef4444' : c.val >= 35 ? '#f59e0b' : '#22c55e';
        return `<div class="ls-comp-row">
          <span class="ls-comp-name">${c.name}</span>
          <div class="ls-comp-bar-bg"><div class="ls-comp-bar-fill" style="width:${c.val}%;background:${cc}"></div></div>
          <span class="ls-comp-val" style="color:${cc}">${c.val}</span>
        </div>`;
      }).join('')}
    </div>`;
}

// ════════════════════════════════════════════════════════════════════════════════
// COMPONENT 2: Q-BRAM ALERTS
// ════════════════════════════════════════════════════════════════════════════════

function renderQBRAMAlerts(d) {
  const el = document.getElementById('qbram-alerts-card');
  if (!el) return;

  const alerts = [];
  const score = d.score ?? 50;
  const p50 = d.pct_above_50 ?? 50;
  const adr = d.ad_ratio ?? 1;
  const vix = d.vix ?? 15;
  const nhNl = d.nh_nl ?? 0;
  const volRatio = d.vol_ratio ?? 1;
  const regime = d.regime ?? 'TRANSITION';
  const ic = d.index_change_pct ?? 0;

  // Extreme Readings
  if (p50 < 15) alerts.push({ icon: '🔴', title: 'Panic Breadth', desc: `Only ${p50.toFixed(1)}% above 50 DMA — extreme selling`, type: 'bearish' });
  if (p50 > 85) alerts.push({ icon: '🟡', title: 'Overbought Breadth', desc: `${p50.toFixed(1)}% above 50 DMA — stretched`, type: 'neutral' });
  if (nhNl < -50) alerts.push({ icon: '🔴', title: 'Extreme New Lows', desc: `NH-NL net: ${nhNl} — broad selling pressure`, type: 'bearish' });
  if (vix > 30) alerts.push({ icon: '🔴', title: 'VIX Fear Spike', desc: `VIX at ${vix.toFixed(1)} — elevated fear`, type: 'bearish' });

  // Breadth Thrust
  if (adr > 3.0) alerts.push({ icon: '🟢', title: 'Breadth Thrust!', desc: `A/D ratio ${adr.toFixed(1)} — rare bullish signal`, type: 'bullish' });

  // Volume Capitulation
  if (volRatio < 0.2) alerts.push({ icon: '🔴', title: 'Volume Capitulation', desc: `Down volume overwhelms up volume (ratio: ${volRatio})`, type: 'bearish' });

  // Score Divergence
  if (ic > 0.5 && score < 40) alerts.push({ icon: '🟡', title: 'Bearish Divergence', desc: `Index up ${ic.toFixed(1)}% but breadth score only ${score}`, type: 'neutral' });
  if (ic < -0.5 && score > 65) alerts.push({ icon: '🟢', title: 'Bullish Divergence', desc: `Index down but breadth remains healthy at ${score}`, type: 'bullish' });

  // Regime-based
  if (regime === 'EXPANSION' && score >= 80) alerts.push({ icon: '🟢', title: 'Strong Expansion', desc: `Score ${score} — broad participation in rally`, type: 'bullish' });
  if (regime === 'PANIC' && score < 20) alerts.push({ icon: '🔴', title: 'Panic Regime', desc: `Score ${score} — market in capitulation mode`, type: 'bearish' });

  const displayed = alerts.slice(0, 5);
  const typeColor = { bearish: '#ef4444', bullish: '#22c55e', neutral: '#f59e0b' };

  el.innerHTML = `
    <div class="qa-header">
      <span class="qa-title">Q-BRAM ALERTS</span>
      <span class="qa-count">${displayed.length} active</span>
    </div>
    <div class="qa-list">
      ${displayed.length === 0 ? '<div class="qa-empty">No active alerts — market within normal parameters</div>' :
        displayed.map(a => `<div class="qa-item" style="border-left:3px solid ${typeColor[a.type]}">
          <span class="qa-icon">${a.icon}</span>
          <div class="qa-body">
            <div class="qa-item-title" style="color:${typeColor[a.type]}">${a.title}</div>
            <div class="qa-item-desc">${a.desc}</div>
          </div>
        </div>`).join('')}
    </div>`;
}

// ════════════════════════════════════════════════════════════════════════════════
// COMPONENT 3: REGIME TIMELINE (30 DAYS)
// ════════════════════════════════════════════════════════════════════════════════

function _pctToRegime(pct) {
  if (pct >= 80) return { name: 'EXP', color: '#22c55e', full: 'EXPANSION' };
  if (pct >= 60) return { name: 'ACC', color: '#86efac', full: 'ACCUMULATION' };
  if (pct >= 40) return { name: 'TRN', color: '#f59e0b', full: 'TRANSITION' };
  if (pct >= 20) return { name: 'DIS', color: '#ef4444', full: 'DISTRIBUTION' };
  return { name: 'PAN', color: '#7f1d1d', full: 'PANIC' };
}

function renderRegimeTimeline(d) {
  const el = document.getElementById('regime-timeline-card');
  if (!el) return;

  const dmaH = (d.dma_history ?? []).slice(-30);
  if (dmaH.length === 0) {
    el.innerHTML = '<div class="rt-header"><span class="rt-title">REGIME TIMELINE — 30 DAYS</span></div><div class="qa-empty">No history data</div>';
    return;
  }

  const days = dmaH.map(h => {
    const r = _pctToRegime(h.pct_above_50);
    return { date: h.date, pct: h.pct_above_50, ...r };
  });

  // Get the last OHLCV date from breadth data for accurate labelling
  const ohlcvDate  = currentData?.[currentMarket]?.last_ohlcv_date || null;
  const freshness  = currentData?.[currentMarket]?.data_freshness  || 'unknown';
  const lastDay    = days[days.length - 1];
  const isToday    = ohlcvDate && ohlcvDate === new Date().toISOString().slice(0,10);

  // Data staleness banner
  let dataNotice = '';
  if (freshness === 'stale' || freshness === 'EOD') {
    const dateLabel = ohlcvDate
      ? new Date(ohlcvDate).toLocaleDateString('en-IN',{day:'2-digit',month:'short',year:'numeric'})
      : 'unknown';
    dataNotice = `<div style="font-size:9px;color:#f59e0b;font-family:var(--font-mono);
      padding:4px 10px;background:rgba(245,158,11,.08);border-radius:4px;margin-bottom:8px">
      ⚠ Data as of ${dateLabel} EOD — run Force EOD Sync for today's data
    </div>`;
  }

  el.innerHTML = `
    <div class="rt-header">
      <span class="rt-title">REGIME TIMELINE — ${days.length} DAYS</span>
      <div class="rt-legend">
        <span style="color:#22c55e">● EXP</span>
        <span style="color:#86efac">● ACC</span>
        <span style="color:#f59e0b">● TRN</span>
        <span style="color:#ef4444">● DIS</span>
        <span style="color:#7f1d1d">● PAN</span>
      </div>
    </div>
    ${dataNotice}
    <div class="rt-blocks">
      ${days.map((day, i) => {
        const isLast = i === days.length - 1;
        const label  = isLast && !isToday ? `${day.name}*` : day.name;
        const border = isLast ? 'border:2px solid rgba(255,255,255,.5)' : '';
        const tip    = isLast && !isToday
          ? `${day.date}: ${day.full} (${day.pct.toFixed(1)}%) — LAST EOD`
          : `${day.date}: ${day.full} (${day.pct.toFixed(1)}%)`;
        return `<div class="rt-block" style="background:${day.color};${border}"
          title="${tip}">
          <span class="rt-block-label">${label}</span>
        </div>`;
      }).join('')}
    </div>
    <div class="rt-dates">
      ${days.filter((_, i) => i % Math.max(1, Math.floor(days.length / 6)) === 0 || i === days.length - 1)
        .map((day, i, arr) => {
          const isLast = i === arr.length - 1;
          const suffix = isLast && !isToday ? '*EOD' : '';
          return `<span style="${isLast?'color:var(--amber)':''}">${day.date.slice(5)}${suffix}</span>`;
        }).join('')}
    </div>
    <div style="font-size:9px;color:var(--text3);font-family:var(--font-mono);margin-top:6px">
      * Last bar = most recent EOD data${!isToday?' — not yet today':''}
    </div>`;
}

// ════════════════════════════════════════════════════════════════════════════════
// COMPONENT 4: Q-BRAM SCORE HISTORY (15 SESSIONS)
// ════════════════════════════════════════════════════════════════════════════════

async function renderScoreHistory(d) {
  const canvas = document.getElementById('chart-score-history');
  if (!canvas) return;

  // Try real stored scores first (accurate Q-BRAM scores from DB)
  let history = [];
  try {
    const mkt = (typeof currentMarket !== 'undefined') ? currentMarket : 'INDIA';
    const res  = await fetch(`${API}/api/breadth/score-history?market=${mkt}&days=30`);
    const data = await res.json();
    if (data.history && data.history.length > 0) {
      history = data.history.slice(-15);
    }
  } catch(e) { /* fallback below */ }

  if (history.length >= 3) {
    // ✅ Real stored scores
    const labels  = history.map(x => x.date.slice(5));
    const scores  = history.map(x => x.score);
    const regimes = history.map(x => x.regime || '');
    const colors  = scores.map(s =>
      s >= 60 ? 'rgba(34,197,94,0.9)' : s >= 40 ? 'rgba(245,158,11,0.9)' : 'rgba(239,68,68,0.9)'
    );

    makeLineChart('chart-score-history', labels, [{
      data: scores,
      borderColor: '#a855f7',
      backgroundColor: 'rgba(168,85,247,0.08)',
      fill: true,
      borderWidth: 2,
      pointBackgroundColor: colors,
      pointRadius: 3,
      pointHoverRadius: 5,
    }], {
      y: { min: 0, max: 100 },
      plugins: {
        tooltip: {
          callbacks: {
            title: (items) => labels[items[0].dataIndex],
            label: (ctx) => [
              `Score: ${ctx.parsed.y}`,
              `Regime: ${regimes[ctx.dataIndex] || '—'}`,
            ]
          }
        }
      }
    });

    // Update chart title to show it's real data
    const hdr = canvas.closest('.chart-card')?.querySelector('.chart-title');
    if (hdr) hdr.textContent = `Q-BRAM SCORE HISTORY (${history.length} SESSIONS)`;
    return;
  }

  // Fallback: estimate from dma_history (only if no stored scores yet)
  const dmaH = (d.dma_history ?? []).slice(-15);
  if (dmaH.length === 0) return;

  const adH  = (d.ad_history  ?? []).slice(-15);
  const nhH  = (d.nh_nl_history ?? []).slice(-15);
  const scores = dmaH.map((dma, i) => {
    const p50     = dma.pct_above_50 ?? 50;
    const adRatio = adH[i]
      ? adH[i].advancers / Math.max(adH[i].decliners, 1) : 1;
    const net     = nhH[i]?.net ?? 0;
    let pts = 0;
    if (p50 >= 70) pts += 25; else if (p50 >= 55) pts += 20;
    else if (p50 >= 40) pts += 12; else if (p50 >= 25) pts += 5;
    if (net > 50) pts += 20; else if (net > 10) pts += 15;
    else if (net > -10) pts += 10; else if (net > -50) pts += 5;
    if (adRatio >= 2.0) pts += 15; else if (adRatio >= 1.3) pts += 12;
    else if (adRatio >= 1.0) pts += 8; else if (adRatio >= 0.7) pts += 4;
    return Math.min(Math.round(pts / 60 * 100), 100);
  });

  const labels = dmaH.map(x => x.date.slice(5));
  const colors = scores.map(s =>
    s >= 60 ? 'rgba(34,197,94,0.9)' : s >= 40 ? 'rgba(245,158,11,0.9)' : 'rgba(239,68,68,0.9)'
  );

  // Mark as estimated
  const hdr = canvas.closest('.chart-card')?.querySelector('.chart-title');
  if (hdr) hdr.textContent = `Q-BRAM SCORE ESTIMATE (${dmaH.length} SESSIONS) *`;

  makeLineChart('chart-score-history', labels, [{
    data: scores,
    borderColor: '#a855f7',
    backgroundColor: 'rgba(168,85,247,0.08)',
    fill: true,
    borderWidth: 2,
    pointBackgroundColor: colors,
    pointRadius: 3,
    pointHoverRadius: 5,
  }], {
    y: { min: 0, max: 100 },
    plugins: {
      tooltip: {
        callbacks: {
          label: (ctx) => `Est. Score: ${ctx.parsed.y} (*approximate)`
        }
      }
    }
  });
}

// ════════════════════════════════════════════════════════════════════════════════
// COMPONENT 5: IV FOOTPRINT (SMART MONEY)
// ════════════════════════════════════════════════════════════════════════════════

let _ivFootprintLoaded = false;

function renderIVFootprint() {
  const canvas = document.getElementById('chart-iv-footprint');
  if (!canvas || _ivFootprintLoaded) return;
  _ivFootprintLoaded = true;

  const mkt = currentMarket === 'INDIA' ? 'India' : currentMarket === 'US' ? 'US' : currentMarket;
  fetch(`${API}/api/breadth/iv-footprint?market=${mkt}&days=30`)
    .then(r => r.json())
    .then(resp => {
      const data = resp.data ?? [];
      if (!data.length) return;

      const labels = data.map(x => x.date.slice(5));
      const ivVals = data.map(x => x.iv_count);
      const ppvVals = data.map(x => x.ppv_count);
      const bsVals = data.map(x => x.bs_count);

      const ctx = canvas.getContext('2d');
      if (charts['chart-iv-footprint']) charts['chart-iv-footprint'].destroy();
      charts['chart-iv-footprint'] = new Chart(ctx, {
        type: 'bar',
        data: {
          labels,
          datasets: [
            { label: 'IV Buy', data: ivVals, backgroundColor: 'rgba(34,197,94,0.75)', borderWidth: 0 },
            { label: 'PPV', data: ppvVals, backgroundColor: 'rgba(59,130,246,0.75)', borderWidth: 0 },
            { label: 'Bull Snort', data: bsVals, backgroundColor: 'rgba(234,179,8,0.75)', borderWidth: 0 },
          ],
        },
        options: {
          responsive: true, maintainAspectRatio: false,
          animation: { duration: 600 },
          plugins: {
            legend: {
              display: true,
              position: 'top',
              labels: { color: '#94a3b8', font: { family: "'Space Mono'", size: 9 }, boxWidth: 12, padding: 8 },
            },
            tooltip: {
              backgroundColor: '#111827', borderColor: '#253552', borderWidth: 1,
              titleFont: { family: "'Space Mono', monospace", size: 9 },
              bodyFont: { family: "'Space Mono', monospace", size: 10 },
              padding: 10,
            },
          },
          scales: {
            x: { stacked: true, grid: { display: false }, ticks: { maxRotation: 30, color: '#4b5e7a', font: { size: 9 } } },
            y: { stacked: true, grid: { color: '#1e2d4a44' }, ticks: { color: '#4b5e7a' } },
          },
        },
      });
    })
    .catch(e => console.warn('IV footprint fetch failed:', e));
}

