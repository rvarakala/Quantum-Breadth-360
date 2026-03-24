// ════════════════════════════════════════════════════════════════════════════
// INTERACTIVE STOCK CHARTS — TradingView Lightweight Charts v4
// Two-pane: price (top 75%) + volume (bottom 25%), synced time scales
// ════════════════════════════════════════════════════════════════════════════

let _chartState = {
  ticker: '',
  tf: 'daily',
  priceChart: null,
  volChart: null,
  priceSeries: null,
  volSeries: null,
  overlayLines: {},
  data: null,
};

let _popupState = {
  ticker: '',
  tf: 'daily',
  priceChart: null,
  volChart: null,
  priceSeries: null,
  volSeries: null,
  overlayLines: {},
  data: null,
};

const CHART_COLORS = {
  dma20: '#06b6d4',
  dma50: '#eab308',
  dma200: '#ef4444',
  bb_upper: 'rgba(168,85,247,0.4)',
  bb_lower: 'rgba(168,85,247,0.4)',
  bb_mid: 'rgba(168,85,247,0.6)',
  rs_line: '#f59e0b',
  volMa: 'rgba(6,182,212,0.7)',
};

const CHART_THEMES = {
  dark: {
    layout: { background: { type: 'solid', color: '#0a0f1e' }, textColor: '#94a3b8', fontSize: 11 },
    grid: { vertLines: { color: '#1e293b' }, horzLines: { color: '#1e293b' } },
    crosshair: { mode: 0 },
    timeScale: { borderColor: '#1e293b', timeVisible: false },
    rightPriceScale: { borderColor: '#1e293b' },
  },
  light: {
    layout: { background: { type: 'solid', color: '#FFFFFF' }, textColor: '#28251D', fontSize: 11 },
    grid: { vertLines: { color: '#E5E4E0' }, horzLines: { color: '#E5E4E0' } },
    crosshair: { mode: 0 },
    timeScale: { borderColor: '#E5E4E0', timeVisible: false },
    rightPriceScale: { borderColor: '#E5E4E0' },
  },
};

function _currentChartTheme() {
  const isLight = document.documentElement.getAttribute('data-theme') === 'light';
  return isLight ? CHART_THEMES.light : CHART_THEMES.dark;
}

// Alias for backward compat within this file
const CHART_THEME = CHART_THEMES.dark;

// ── Chart Creation ───────────────────────────────────────────────────────────

function _createChartPair(priceContainerId, volContainerId, state) {
  const priceEl = document.getElementById(priceContainerId);
  const volEl = document.getElementById(volContainerId);
  if (!priceEl || !volEl) return;

  // Destroy existing
  if (state.priceChart) { try { state.priceChart.remove(); } catch(e) {} state.priceChart = null; }
  if (state.volChart) { try { state.volChart.remove(); } catch(e) {} state.volChart = null; }
  state.overlayLines = {};

  // Clear containers
  priceEl.innerHTML = '';
  volEl.innerHTML = '';

  const parent = priceEl.parentElement;
  const w = parent.clientWidth || 900;
  const totalH = parent.clientHeight || parent.offsetHeight || 600;
  const priceH = Math.max(200, Math.round(totalH * 0.75));
  const volH = Math.max(80, totalH - priceH);

  priceEl.style.height = priceH + 'px';
  volEl.style.height = volH + 'px';

  const theme = _currentChartTheme();

  const priceChart = LightweightCharts.createChart(priceEl, {
    ...theme,
    width: w, height: priceH,
    handleScroll: { mouseWheel: true, pressedMouseMove: true },
    handleScale: { axisPressedMouseMove: true, mouseWheel: true, pinch: true },
  });

  const volChart = LightweightCharts.createChart(volEl, {
    ...theme,
    width: w, height: volH,
    handleScroll: { mouseWheel: true, pressedMouseMove: true },
    handleScale: { axisPressedMouseMove: true, mouseWheel: true, pinch: true },
    rightPriceScale: { borderColor: theme.rightPriceScale.borderColor, scaleMargins: { top: 0.1, bottom: 0 } },
  });

  // Candlestick series
  const candleSeries = priceChart.addCandlestickSeries({
    upColor: '#22c55e', downColor: '#ef4444',
    borderUpColor: '#22c55e', borderDownColor: '#ef4444',
    wickUpColor: '#22c55e', wickDownColor: '#ef4444',
  });

  // Volume histogram
  const volSeries = volChart.addHistogramSeries({
    priceFormat: { type: 'volume' },
    priceScaleId: 'right',
  });

  state.priceChart = priceChart;
  state.volChart = volChart;
  state.priceSeries = candleSeries;
  state.volSeries = volSeries;

  // Sync time scales
  priceChart.timeScale().subscribeVisibleLogicalRangeChange(range => {
    if (range) volChart.timeScale().setVisibleLogicalRange(range);
  });
  volChart.timeScale().subscribeVisibleLogicalRangeChange(range => {
    if (range) priceChart.timeScale().setVisibleLogicalRange(range);
  });

  // Sync crosshair (safely — not all LWC versions support setCrosshairPosition)
  if (typeof priceChart.setCrosshairPosition === 'function') {
    priceChart.subscribeCrosshairMove(param => {
      if (param.time) {
        try { volChart.setCrosshairPosition(undefined, param.time, volSeries); } catch(e) {}
      } else {
        try { volChart.clearCrosshairPosition(); } catch(e) {}
      }
    });
    volChart.subscribeCrosshairMove(param => {
      if (param.time) {
        try { priceChart.setCrosshairPosition(undefined, param.time, candleSeries); } catch(e) {}
      } else {
        try { priceChart.clearCrosshairPosition(); } catch(e) {}
      }
    });
  }

  return { priceChart, volChart, candleSeries, volSeries };
}

function _renderChartData(state, data) {
  if (!state.priceChart || !data) return;
  state.data = data;

  // Set candles
  state.priceSeries.setData(data.candles);

  // Set volume bars
  state.volSeries.setData(data.volume.bars);

  // Clear old overlays
  for (const key of Object.keys(state.overlayLines)) {
    try { state.priceChart.removeSeries(state.overlayLines[key]); } catch(e) {}
  }
  state.overlayLines = {};

  // Volume MA50 — handled by _applyVolMa
  if (state._volMaLine) {
    try { state.volChart.removeSeries(state._volMaLine); } catch(e) {}
    state._volMaLine = null;
  }

  // Clean up RelVolume, IV, FVG series from prior render
  if (state._rvSeries) { try { state.volChart.removeSeries(state._rvSeries); } catch(e) {} state._rvSeries = null; }
  if (state._rvThresholdLine) { try { state.volChart.removeSeries(state._rvThresholdLine); } catch(e) {} state._rvThresholdLine = null; }
  if (state._ivLines) { for (const s of state._ivLines) { try { state.priceChart.removeSeries(s); } catch(e) {} } state._ivLines = []; }
  if (state._fvgLines) { for (const s of state._fvgLines) { try { state.priceChart.removeSeries(s); } catch(e) {} } state._fvgLines = []; }

  // Overlays — check checkboxes
  const isPopup = state === _popupState;
  _applyOverlays(state, data, isPopup);

  // Markers on price chart: PPV, Bull Snort, VCP, HV labels
  _applyMarkers(state, data, isPopup);

  // Pivot points as horizontal lines
  _applyPivots(state, data, isPopup);

  // Volume MA toggle
  _applyVolMa(state, data, isPopup);

  // New indicators: RelVolume, IV, FVG
  _applyRelVolume(state, data, isPopup);
  _applyIV(state, data, isPopup);
  _applyFVG(state, data, isPopup);

  // Fit content
  state.priceChart.timeScale().fitContent();
  state.volChart.timeScale().fitContent();
}

function _applyOverlays(state, data, isPopup) {
  // Remove existing overlay lines
  for (const key of Object.keys(state.overlayLines)) {
    try { state.priceChart.removeSeries(state.overlayLines[key]); } catch(e) {}
  }
  state.overlayLines = {};

  const ov = data.overlays;

  const show = (id) => {
    if (isPopup) {
      const pid = 'p' + id;  // e.g. chk-dma20 -> pchk-dma20
      const el = document.getElementById(pid);
      return el ? el.checked : true;  // default on if no popup checkbox
    }
    return document.getElementById(id)?.checked ?? false;
  };

  // DMAs
  if (show('chk-dma20') && ov.dma20.length > 0) {
    const s = state.priceChart.addLineSeries({ color: CHART_COLORS.dma20, lineWidth: 1, lastValueVisible: false, priceLineVisible: false });
    s.setData(ov.dma20);
    state.overlayLines.dma20 = s;
  }
  if (show('chk-dma50') && ov.dma50.length > 0) {
    const s = state.priceChart.addLineSeries({ color: CHART_COLORS.dma50, lineWidth: 1, lastValueVisible: false, priceLineVisible: false });
    s.setData(ov.dma50);
    state.overlayLines.dma50 = s;
  }
  if (show('chk-dma200') && ov.dma200.length > 0) {
    const s = state.priceChart.addLineSeries({ color: CHART_COLORS.dma200, lineWidth: 1, lastValueVisible: false, priceLineVisible: false });
    s.setData(ov.dma200);
    state.overlayLines.dma200 = s;
  }

  // Bollinger Bands
  if (show('chk-bb') && ov.bb_upper.length > 0) {
    const su = state.priceChart.addLineSeries({ color: CHART_COLORS.bb_upper, lineWidth: 1, lastValueVisible: false, priceLineVisible: false, lineStyle: 2 });
    su.setData(ov.bb_upper);
    state.overlayLines.bb_upper = su;
    const sl = state.priceChart.addLineSeries({ color: CHART_COLORS.bb_lower, lineWidth: 1, lastValueVisible: false, priceLineVisible: false, lineStyle: 2 });
    sl.setData(ov.bb_lower);
    state.overlayLines.bb_lower = sl;
    const sm = state.priceChart.addLineSeries({ color: CHART_COLORS.bb_mid, lineWidth: 1, lastValueVisible: false, priceLineVisible: false, lineStyle: 1 });
    sm.setData(ov.bb_mid);
    state.overlayLines.bb_mid = sm;
  }

  // RS Line
  if (show('chk-rs') && ov.rs_line && ov.rs_line.length > 0) {
    const s = state.priceChart.addLineSeries({
      color: CHART_COLORS.rs_line, lineWidth: 1, lastValueVisible: false, priceLineVisible: false,
      priceScaleId: 'rs',
    });
    s.setData(ov.rs_line);
    state.overlayLines.rs_line = s;
    state.priceChart.priceScale('rs').applyOptions({ scaleMargins: { top: 0.7, bottom: 0 } });
  }
}

function _applyMarkers(state, data, isPopup) {
  const show = (id) => {
    if (isPopup) {
      const pid = 'p' + id;
      const el = document.getElementById(pid);
      return el ? el.checked : true;
    }
    return document.getElementById(id)?.checked ?? true;
  };

  // HV labels — on volume chart as markers
  const hvMarkers = [];
  if (show('chk-hv')) {
    for (const h of data.markers.hv_labels || []) {
      const clr = h.type === 'HVE' ? '#ef4444' : h.type === 'HVY' ? '#f59e0b' : '#3b82f6';
      hvMarkers.push({
        time: h.time, position: 'aboveBar', color: clr,
        shape: 'circle', text: h.type,
      });
    }
  }
  hvMarkers.sort((a, b) => a.time.localeCompare(b.time));
  state.volSeries.setMarkers(hvMarkers);

  // All price markers (PPV, BS, VCP, IV, FVG) via unified function
  _reapplyPriceMarkers(state, data, isPopup);
}

function _applyPivots(state, data, isPopup) {
  // Remove existing pivot lines
  if (state._pivotLines) {
    for (const s of state._pivotLines) {
      try { state.priceChart.removeSeries(s); } catch(e) {}
    }
  }
  state._pivotLines = [];

  const showPivots = isPopup
    ? (document.getElementById('pchk-pivots')?.checked ?? false)
    : (document.getElementById('chk-pivots')?.checked ?? false);
  if (!showPivots) return;

  const pp = data.markers.pivot_points;
  if (!pp || !pp.pp) return;

  const levels = [
    { key: 'r3', color: '#ef4444', label: 'R3' },
    { key: 'r2', color: '#f87171', label: 'R2' },
    { key: 'r1', color: '#fca5a5', label: 'R1' },
    { key: 'pp', color: '#94a3b8', label: 'PP' },
    { key: 's1', color: '#86efac', label: 'S1' },
    { key: 's2', color: '#4ade80', label: 'S2' },
    { key: 's3', color: '#22c55e', label: 'S3' },
  ];

  for (const lvl of levels) {
    if (pp[lvl.key] == null) continue;
    const s = state.priceChart.addLineSeries({
      color: lvl.color, lineWidth: 1, lineStyle: 2,
      lastValueVisible: true, priceLineVisible: false,
      title: lvl.label,
    });
    // Create a horizontal line spanning the visible range
    const last = data.candles[data.candles.length - 1];
    const first = data.candles[Math.max(0, data.candles.length - 30)];
    s.setData([
      { time: first.time, value: pp[lvl.key] },
      { time: last.time, value: pp[lvl.key] },
    ]);
    state._pivotLines.push(s);
  }
}

function _applyVolMa(state, data, isPopup) {
  // Remove existing
  if (state._volMaLine) {
    try { state.volChart.removeSeries(state._volMaLine); } catch(e) {}
    state._volMaLine = null;
  }
  const showVolMa = isPopup
    ? (document.getElementById('pchk-volma')?.checked ?? true)
    : (document.getElementById('chk-volma')?.checked ?? true);
  if (showVolMa && data.volume.ma50.length > 0) {
    const volMaLine = state.volChart.addLineSeries({
      color: CHART_COLORS.volMa, lineWidth: 1, priceScaleId: 'right',
      lastValueVisible: false, priceLineVisible: false,
    });
    volMaLine.setData(data.volume.ma50);
    state._volMaLine = volMaLine;
  }
}

function _applyRelVolume(state, data, isPopup) {
  // Remove existing RelVolume series
  if (state._rvSeries) {
    try { state.volChart.removeSeries(state._rvSeries); } catch(e) {}
    state._rvSeries = null;
  }
  if (state._rvThresholdLine) {
    try { state.volChart.removeSeries(state._rvThresholdLine); } catch(e) {}
    state._rvThresholdLine = null;
  }

  const show = isPopup
    ? (document.getElementById('pchk-rv')?.checked ?? true)
    : (document.getElementById('chk-rv')?.checked ?? true);
  if (!show || !data.rel_volume || !data.rel_volume.bars.length) return;

  // Render as histogram on volume chart with separate scale
  const rvSeries = state.volChart.addHistogramSeries({
    priceScaleId: 'rv',
    priceFormat: { type: 'price', precision: 1, minMove: 0.1 },
    lastValueVisible: false,
    priceLineVisible: false,
  });
  rvSeries.setData(data.rel_volume.bars);
  state.volChart.priceScale('rv').applyOptions({
    scaleMargins: { top: 0.0, bottom: 0.6 },
  });
  state._rvSeries = rvSeries;
}

function _applyIV(state, data, isPopup) {
  // Remove existing IV lines and markers
  if (state._ivLines) {
    for (const s of state._ivLines) {
      try { state.priceChart.removeSeries(s); } catch(e) {}
    }
  }
  state._ivLines = [];

  const show = isPopup
    ? (document.getElementById('pchk-iv')?.checked ?? true)
    : (document.getElementById('chk-iv')?.checked ?? true);
  if (!show || !data.iv_signals || !data.iv_signals.length) return;

  const lastCandle = data.candles[data.candles.length - 1];

  for (const iv of data.iv_signals) {
    // High line extending to right
    const hLine = state.priceChart.addLineSeries({
      color: 'rgba(107,114,128,0.5)', lineWidth: 1, lineStyle: 2,
      lastValueVisible: false, priceLineVisible: false,
    });
    hLine.setData([
      { time: iv.time, value: iv.high },
      { time: lastCandle.time, value: iv.high },
    ]);
    state._ivLines.push(hLine);

    // Low line extending to right
    const lLine = state.priceChart.addLineSeries({
      color: 'rgba(107,114,128,0.4)', lineWidth: 1, lineStyle: 2,
      lastValueVisible: false, priceLineVisible: false,
    });
    lLine.setData([
      { time: iv.time, value: iv.low },
      { time: lastCandle.time, value: iv.low },
    ]);
    state._ivLines.push(lLine);
  }

  // Add green arrow markers for IV signals on price series
  // We need to merge with existing markers, so get current and add
  const existing = state.priceSeries.markers ? [] : [];
  const ivMarkers = data.iv_signals.map(iv => ({
    time: iv.time, position: 'belowBar', color: '#22c55e',
    shape: 'arrowUp', text: 'IV',
  }));

  // Re-apply all markers including IV
  _reapplyPriceMarkers(state, data, isPopup);
}

function _applyFVG(state, data, isPopup) {
  // Remove existing FVG lines
  if (state._fvgLines) {
    for (const s of state._fvgLines) {
      try { state.priceChart.removeSeries(s); } catch(e) {}
    }
  }
  state._fvgLines = [];

  const show = isPopup
    ? (document.getElementById('pchk-fvg')?.checked ?? true)
    : (document.getElementById('chk-fvg')?.checked ?? true);
  if (!show || !data.fvg_zones || !data.fvg_zones.length) return;

  const lastCandle = data.candles[data.candles.length - 1];

  for (const fvg of data.fvg_zones) {
    // Upper line (brown/orange dashed)
    const uLine = state.priceChart.addLineSeries({
      color: 'rgba(180,120,60,0.6)', lineWidth: 1, lineStyle: 2,
      lastValueVisible: false, priceLineVisible: false,
    });
    uLine.setData([
      { time: fvg.time, value: fvg.upper },
      { time: lastCandle.time, value: fvg.upper },
    ]);
    state._fvgLines.push(uLine);

    // Lower line (brown/orange dashed)
    const lLine = state.priceChart.addLineSeries({
      color: 'rgba(180,120,60,0.6)', lineWidth: 1, lineStyle: 2,
      lastValueVisible: false, priceLineVisible: false,
    });
    lLine.setData([
      { time: fvg.time, value: fvg.lower },
      { time: lastCandle.time, value: fvg.lower },
    ]);
    state._fvgLines.push(lLine);
  }
}

function _reapplyPriceMarkers(state, data, isPopup) {
  const show = (id) => {
    if (isPopup) {
      const pid = 'p' + id;
      const el = document.getElementById(pid);
      return el ? el.checked : true;
    }
    return document.getElementById(id)?.checked ?? true;
  };

  const markers = [];
  const mk = data.markers;

  if (show('chk-ppv')) {
    for (const pp of mk.pocket_pivots || []) {
      markers.push({ time: pp.time, position: 'belowBar', color: '#3b82f6', shape: 'arrowUp', text: 'PPV' });
    }
  }
  if (show('chk-bs')) {
    for (const bs of mk.bull_snorts || []) {
      markers.push({ time: bs.time, position: 'belowBar', color: '#eab308', shape: 'arrowUp', text: 'BS' });
    }
  }
  if (show('chk-vcp')) {
    for (const v of mk.vcp_signals || []) {
      markers.push({ time: v.time, position: 'aboveBar', color: '#a855f7', shape: 'circle', text: 'VCP' });
    }
  }

  // IV markers
  const showIV = isPopup
    ? (document.getElementById('pchk-iv')?.checked ?? true)
    : (document.getElementById('chk-iv')?.checked ?? true);
  if (showIV && data.iv_signals) {
    for (const iv of data.iv_signals) {
      markers.push({ time: iv.time, position: 'belowBar', color: '#22c55e', shape: 'arrowUp', text: 'IV' });
    }
  }

  // FVG markers (blue circles)
  const showFVG = isPopup
    ? (document.getElementById('pchk-fvg')?.checked ?? true)
    : (document.getElementById('chk-fvg')?.checked ?? true);
  if (showFVG && data.fvg_zones) {
    for (const fvg of data.fvg_zones) {
      markers.push({ time: fvg.time, position: 'aboveBar', color: '#b4783c', shape: 'circle', text: 'FVG' });
    }
  }

  markers.sort((a, b) => a.time.localeCompare(b.time));
  state.priceSeries.setMarkers(markers);
}

function onPopupOverlayToggle() {
  if (_popupState.data) {
    _applyOverlays(_popupState, _popupState.data, true);
    _applyPivots(_popupState, _popupState.data, true);
    _applyMarkers(_popupState, _popupState.data, true);
    _applyVolMa(_popupState, _popupState.data, true);
    _applyRelVolume(_popupState, _popupState.data, true);
    _applyIV(_popupState, _popupState.data, true);
    _applyFVG(_popupState, _popupState.data, true);
  }
}

// ── Metrics Strip Rendering ──────────────────────────────────────────────────

function _metricsStatusClass(metric, value, status) {
  const map = {
    rs_status:   { LEADER: 'cms-green', STRONG: 'cms-blue', 'ABOVE AVG': 'cms-cyan', WEAK: 'cms-red' },
    trend:       { PASS: 'cms-green', PARTIAL: 'cms-amber', FAIL: 'cms-red', 'N/A': 'cms-grey' },
    eps_status:  { Positive: 'cms-green', Negative: 'cms-red', 'N/A': 'cms-grey' },
    rv_status:   { 'Inst. Buy': 'cms-green', 'Above Avg': 'cms-cyan', Normal: 'cms-grey', 'Dry Up': 'cms-red', 'N/A': 'cms-grey' },
    off_52w:     { 'Breakout Ready': 'cms-green', Pullback: 'cms-amber', 'Deep Correction': 'cms-red' },
    atr_status:  { Low: 'cms-green', Medium: 'cms-amber', High: 'cms-red', 'N/A': 'cms-grey' },
    mkt:         { OPTIMAL: 'cms-green', CAUTION: 'cms-amber', ADVERSE: 'cms-red', 'N/A': 'cms-grey' },
    mcap_tier:   { 'Mega Cap': 'cms-purple', 'Large Cap': 'cms-blue', 'Mid Cap': 'cms-cyan', 'Small Cap': 'cms-amber', 'Micro Cap': 'cms-red' },
  };
  return (map[metric] || {})[status] || 'cms-grey';
}

function _renderMetricsStrip(containerId, metrics) {
  const el = document.getElementById(containerId);
  if (!el) return;

  const cells = [
    { label: 'Sector', value: metrics.sector || '—', status: '—', statusCls: 'cms-grey' },
    { label: 'Market Cap', value: metrics.mcap_formatted || '—',
      status: metrics.mcap_tier || '—',
      statusCls: _metricsStatusClass('mcap_tier', null, metrics.mcap_tier) },
    { label: 'RS Rating', value: metrics.rs_rating != null ? String(metrics.rs_rating) : '—',
      status: metrics.rs_status || '—',
      statusCls: _metricsStatusClass('rs_status', null, metrics.rs_status) },
    { label: 'Trend', value: metrics.trend_template || '—',
      status: metrics.stage || '—',
      statusCls: _metricsStatusClass('trend', null, metrics.trend_template) },
    { label: 'EPS (TTM)', value: metrics.eps_ttm != null ? String(metrics.eps_ttm) : '...',
      status: metrics.eps_status || '...',
      statusCls: _metricsStatusClass('eps_status', null, metrics.eps_status),
      id: containerId + '-eps' },
    { label: 'P/E Ratio', value: metrics.pe_ratio != null ? String(metrics.pe_ratio) : '...',
      status: metrics.pe_ratio != null ? (metrics.pe_ratio < 15 ? 'Cheap' : metrics.pe_ratio < 30 ? 'Fair' : 'Expensive') : '—',
      statusCls: metrics.pe_ratio != null ? (metrics.pe_ratio < 15 ? 'cms-green' : metrics.pe_ratio < 30 ? 'cms-amber' : 'cms-red') : 'cms-grey',
      id: containerId + '-pe' },
    { label: 'Rel Volume', value: metrics.rel_volume != null ? metrics.rel_volume + 'x' : '—',
      status: metrics.rel_volume_status || '—',
      statusCls: _metricsStatusClass('rv_status', null, metrics.rel_volume_status) },
    { label: 'Off 52w High', value: metrics.off_52w_high_pct != null ? metrics.off_52w_high_pct + '%' : '—',
      status: metrics.off_52w_status || '—',
      statusCls: _metricsStatusClass('off_52w', null, metrics.off_52w_status) },
    { label: 'ATR%', value: metrics.atr_pct != null ? metrics.atr_pct + '%' : '—',
      status: metrics.atr_status || '—',
      statusCls: _metricsStatusClass('atr_status', null, metrics.atr_status) },
    { label: 'Mkt Condition', value: metrics.market_condition || '—',
      status: metrics.market_vol_status || '—',
      statusCls: _metricsStatusClass('mkt', null, metrics.market_condition) },
  ];

  el.innerHTML = cells.map(c => `
    <div class="chart-metric-cell">
      <div class="chart-metric-label">${c.label}</div>
      <div class="chart-metric-value">${c.value}</div>
      <div class="chart-metric-status ${c.statusCls}" ${c.id ? `id="${c.id}"` : ''}>${c.status}</div>
    </div>
  `).join('');

  el.style.display = 'flex';
}

async function _loadMetricsStrip(ticker, containerId) {
  const showMetrics = document.getElementById('chk-metrics')?.checked ?? true;
  const el = document.getElementById(containerId);
  if (!el) return;
  if (!showMetrics) { el.style.display = 'none'; return; }

  try {
    const res = await fetch(`${API}/api/stock-metrics/${encodeURIComponent(ticker)}`);
    if (!res.ok) { el.style.display = 'none'; return; }
    const metrics = await res.json();
    if (metrics.error) { el.style.display = 'none'; return; }
    _renderMetricsStrip(containerId, metrics);

    // Fetch EPS async — don't block
    fetch(`${API}/api/stock-metrics/${encodeURIComponent(ticker)}/eps`)
      .then(r => r.json())
      .then(eps => {
        const epsEl = document.getElementById(containerId + '-eps');
        if (!epsEl) return;
        const cell = epsEl.parentElement;
        if (!cell) return;
        const valEl = cell.querySelector('.chart-metric-value');
        if (valEl) valEl.textContent = eps.eps_ttm != null ? String(eps.eps_ttm) : 'N/A';
        epsEl.textContent = eps.eps_status || 'N/A';
        epsEl.className = 'chart-metric-status ' + _metricsStatusClass('eps_status', null, eps.eps_status || 'N/A');
      })
      .catch(() => {});
  } catch (e) {
    el.style.display = 'none';
  }
}

// ── Main Tab Chart ───────────────────────────────────────────────────────────

async function loadChart(ticker) {
  const input = document.getElementById('chart-ticker-input');
  const t = (ticker || input?.value || '').trim().toUpperCase();
  if (!t) return;
  if (input) input.value = t;
  _chartState.ticker = t;

  const infoBar = document.getElementById('chart-info-bar');
  if (infoBar) infoBar.innerHTML = `<span class="chart-loading-text">Loading ${t}...</span>`;

  // Create chart instances if not exists
  if (!_chartState.priceChart) {
    _createChartPair('chart-price-pane', 'chart-vol-pane', _chartState);
  }

  // Load metrics strip (non-blocking)
  _loadMetricsStrip(t, 'chart-metrics-strip');

  try {
    const url = `${API}/api/chart/${encodeURIComponent(t)}?tf=${_chartState.tf}`;
    const res = await fetch(url);
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const data = await res.json();
    if (data.error) throw new Error(data.error);

    _chartState.data = data;
    _renderChartData(_chartState, data);

    // Update info bar
    if (infoBar) {
      const info = data.info;
      const lastCandle = data.candles[data.candles.length - 1];
      const prevCandle = data.candles.length > 1 ? data.candles[data.candles.length - 2] : lastCandle;
      const chg = lastCandle.close - prevCandle.close;
      const chgPct = prevCandle.close ? (chg / prevCandle.close * 100) : 0;
      const col = chg >= 0 ? 'var(--green)' : 'var(--red)';
      // Build mcap + sector info
      let mcapHtml = '';
      if (info.mcap_tier) {
        const cls = info.mcap_tier.startsWith('Mega') ? 'mcap-mega'
          : info.mcap_tier.startsWith('Large') ? 'mcap-large'
          : info.mcap_tier.startsWith('Mid') ? 'mcap-mid'
          : info.mcap_tier.startsWith('Small') ? 'mcap-small'
          : 'mcap-micro';
        const short = info.mcap_tier.replace(' Cap', '');
        mcapHtml = `<span class="chart-info-sep">|</span><span class="chart-info-mcap ${cls}">${short} ${info.mcap_formatted || ''}</span>`;
      }
      let sectorHtml = '';
      if (info.sector) {
        sectorHtml = `<span class="chart-info-sep">|</span><span class="chart-info-sector">${info.sector}</span>`;
      }
      infoBar.innerHTML = `
        <span class="chart-info-ticker">${info.name}</span>
        <span class="chart-info-price" style="color:${col}">${lastCandle.close.toLocaleString('en-IN', {maximumFractionDigits:2})}</span>
        <span class="chart-info-chg" style="color:${col}">${chg >= 0 ? '+' : ''}${chg.toFixed(2)} (${chgPct >= 0 ? '+' : ''}${chgPct.toFixed(2)}%)</span>
        <span class="chart-info-date">${info.last_date}</span>
        <span class="chart-info-bars">${info.total_bars} bars</span>
        ${mcapHtml}${sectorHtml}
      `;
    }
  } catch (e) {
    if (infoBar) infoBar.innerHTML = `<span style="color:var(--red)">Error: ${e.message}</span>`;
  }
}

function switchChartTF(tf) {
  _chartState.tf = tf;
  document.querySelectorAll('#tab-charts .chart-tf').forEach(b => {
    b.classList.toggle('active', b.dataset.tf === tf);
  });
  if (_chartState.ticker) loadChart(_chartState.ticker);
}

function _onOverlayToggle() {
  if (_chartState.data) {
    _applyOverlays(_chartState, _chartState.data, false);
    _applyPivots(_chartState, _chartState.data, false);
    _applyMarkers(_chartState, _chartState.data, false);
    _applyVolMa(_chartState, _chartState.data, false);
    _applyRelVolume(_chartState, _chartState.data, false);
    _applyIV(_chartState, _chartState.data, false);
    _applyFVG(_chartState, _chartState.data, false);
  }
}

// ── Chart Popup ──────────────────────────────────────────────────────────────

async function openTickerChart(ticker) {
  const t = ticker.trim().toUpperCase();
  if (!t) return;
  _popupState.ticker = t;
  _popupState.tf = 'daily';

  const el = document.getElementById('chart-popup');
  if (!el) return;
  el.style.display = 'flex';
  document.body.style.overflow = 'hidden';

  document.getElementById('chart-popup-title').textContent = t;

  // Reset TF buttons
  document.querySelectorAll('#chart-popup .chart-tf').forEach(b => {
    b.classList.toggle('active', b.dataset.tf === 'daily');
  });

  // Wait for layout to settle so containers have dimensions
  await new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)));

  // Create chart instances
  _createChartPair('chart-popup-price', 'chart-popup-vol', _popupState);

  // Load metrics strip for popup
  _loadMetricsStrip(t, 'chart-popup-metrics-strip');

  try {
    const url = `${API}/api/chart/${encodeURIComponent(t)}?tf=${_popupState.tf}`;
    const res = await fetch(url);
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const data = await res.json();
    if (data.error) throw new Error(data.error);
    _renderChartData(_popupState, data);
  } catch (e) {
    const body = document.getElementById('chart-popup-body');
    if (body) body.innerHTML = `<div style="padding:40px;text-align:center;color:var(--red)">Error: ${e.message}</div>`;
  }
}

function closeChartPopup() {
  const el = document.getElementById('chart-popup');
  if (el) el.style.display = 'none';
  document.body.style.overflow = '';
  if (_popupState.priceChart) { _popupState.priceChart.remove(); _popupState.priceChart = null; }
  if (_popupState.volChart) { _popupState.volChart.remove(); _popupState.volChart = null; }
  _popupState.overlayLines = {};
}

async function switchPopupTF(tf) {
  _popupState.tf = tf;
  document.querySelectorAll('#chart-popup .chart-tf').forEach(b => {
    b.classList.toggle('active', b.dataset.tf === tf);
  });
  if (!_popupState.ticker) return;

  await new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)));
  _createChartPair('chart-popup-price', 'chart-popup-vol', _popupState);

  try {
    const url = `${API}/api/chart/${encodeURIComponent(_popupState.ticker)}?tf=${tf}`;
    const res = await fetch(url);
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const data = await res.json();
    if (data.error) throw new Error(data.error);
    _renderChartData(_popupState, data);
  } catch (e) {
    console.error('Popup chart error:', e);
  }
}

// ── Init & Event Listeners ───────────────────────────────────────────────────

function initChartsTab() {
  // Re-create charts on tab switch (container may have been hidden)
  if (_chartState.ticker) {
    _chartState.priceChart = null;
    _chartState.volChart = null;
    loadChart(_chartState.ticker);
  }
}

// Keyboard: Enter to load, Escape to close popup
document.addEventListener('keydown', e => {
  if (e.key === 'Enter' && document.activeElement?.id === 'chart-ticker-input') {
    loadChart();
  }
  if (e.key === 'Escape' && document.getElementById('chart-popup')?.style.display === 'flex') {
    closeChartPopup();
  }
});

// Overlay checkbox listeners (deferred until DOM ready)
document.addEventListener('DOMContentLoaded', () => {
  ['chk-dma20','chk-dma50','chk-dma200','chk-bb','chk-rs','chk-vcp','chk-pivots','chk-ppv','chk-bs','chk-hv','chk-volma','chk-rv','chk-iv','chk-fvg'].forEach(id => {
    const el = document.getElementById(id);
    if (el) el.addEventListener('change', _onOverlayToggle);
  });
  // Metrics strip toggle
  const metricsChk = document.getElementById('chk-metrics');
  if (metricsChk) {
    metricsChk.addEventListener('change', () => {
      const strip = document.getElementById('chart-metrics-strip');
      if (strip) strip.style.display = metricsChk.checked ? 'flex' : 'none';
    });
  }
});

// ── Theme Update ─────────────────────────────────────────────────────────────

function updateChartsTheme() {
  const theme = _currentChartTheme();
  // Update main tab chart if it exists
  if (_chartState.priceChart) {
    _chartState.priceChart.applyOptions(theme);
    _chartState.volChart.applyOptions({
      ...theme,
      rightPriceScale: { borderColor: theme.rightPriceScale.borderColor, scaleMargins: { top: 0.1, bottom: 0 } },
    });
  }
  // Update popup chart if it exists
  if (_popupState.priceChart) {
    _popupState.priceChart.applyOptions(theme);
    _popupState.volChart.applyOptions({
      ...theme,
      rightPriceScale: { borderColor: theme.rightPriceScale.borderColor, scaleMargins: { top: 0.1, bottom: 0 } },
    });
  }
}

// Handle resize
window.addEventListener('resize', () => {
  if (_chartState.priceChart) {
    const container = document.getElementById('chart-container');
    if (container && container.offsetWidth > 0) {
      const w = container.clientWidth;
      const h = container.clientHeight;
      _chartState.priceChart.resize(w, Math.round(h * 0.75));
      _chartState.volChart.resize(w, h - Math.round(h * 0.75));
    }
  }
});
