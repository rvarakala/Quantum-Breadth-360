
// ─── CLEAR CACHE ───────────────────────────────────────────────────────────
async function clearBreadthCache() {
  const btn = document.getElementById('sidebar-clear-cache-btn');
  if (btn) btn.style.opacity = '0.5';
  try {
    const resp = await fetch(`${API}/api/cache/clear-breadth`, { method: 'POST' });
    const data = await resp.json();
    if (data.ok) {
      // Show brief feedback
      const label = btn ? btn.querySelector('.nav-label') : null;
      if (label) { label.textContent = 'Recomputing...'; }
      // Reload after 2s to pick up fresh data
      setTimeout(() => { window.location.reload(); }, 2000);
    } else {
      alert('Failed to clear cache: ' + (data.error || 'Unknown error'));
    }
  } catch (e) {
    alert('Failed to clear cache: ' + e.message);
  } finally {
    if (btn) btn.style.opacity = '1';
  }
}

// ─── THEME TOGGLE ──────────────────────────────────────────────────────────
function toggleTheme() {
  const html = document.documentElement;
  const isLight = html.getAttribute('data-theme') === 'light';
  const newTheme = isLight ? 'dark' : 'light';
  if (newTheme === 'light') {
    html.setAttribute('data-theme', 'light');
  } else {
    html.removeAttribute('data-theme');
  }
  localStorage.setItem('breadth-theme', newTheme);
  // Re-render charts with new theme colors if visible
  if (typeof updateChartsTheme === 'function') updateChartsTheme();
}

// Apply saved theme on load
(function initTheme() {
  const saved = localStorage.getItem('breadth-theme');
  if (saved === 'light') {
    document.documentElement.setAttribute('data-theme', 'light');
  }
})();

// ─── SIDEBAR ────────────────────────────────────────────────────────────────
function toggleSidebar() {
  const sidebar = document.getElementById('sidebar');
  const overlay = document.getElementById('sidebar-overlay');
  if (!sidebar) return;
  // Mobile: toggle open class
  if (window.innerWidth <= 768) {
    sidebar.classList.toggle('open');
    if (overlay) overlay.classList.toggle('active', sidebar.classList.contains('open'));
  } else {
    // Desktop: toggle collapsed
    toggleSidebarCollapse();
  }
}

function closeSidebar() {
  const sidebar = document.getElementById('sidebar');
  const overlay = document.getElementById('sidebar-overlay');
  if (sidebar) sidebar.classList.remove('open');
  if (overlay) overlay.classList.remove('active');
}

function toggleSidebarCollapse() {
  const sidebar = document.getElementById('sidebar');
  if (!sidebar) return;
  sidebar.classList.toggle('collapsed');
  localStorage.setItem('sidebar-collapsed', sidebar.classList.contains('collapsed') ? '1' : '');
}

// Restore sidebar collapsed state
(function initSidebar() {
  const collapsed = localStorage.getItem('sidebar-collapsed');
  if (collapsed === '1') {
    const sidebar = document.getElementById('sidebar');
    if (sidebar) sidebar.classList.add('collapsed');
  }
})();

function switchTab(tab) {
  // Update sidebar nav items
  document.querySelectorAll('.sidebar .nav-item[data-tab]').forEach(b => b.classList.toggle('active', b.dataset.tab === tab));
  // Keep old tab-btn compat
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.toggle('active', b.dataset.tab === tab));
  document.querySelectorAll('.tab-content').forEach(c => c.classList.toggle('active', c.id === `tab-${tab}`));

  // Close sidebar on mobile after selection
  if (window.innerWidth <= 768) closeSidebar();

  // Render charts when tab becomes visible
  if (tab === 'breadth' && currentData[currentMarket]) {
    setTimeout(() => renderCharts(currentData[currentMarket]), 50);
  }
  if (tab === 'sectors' && currentData[currentMarket]) {
    setTimeout(() => renderSectors(currentData[currentMarket]), 50);
    setTimeout(() => renderSectors(currentData[currentMarket]), 100);
  }
  if (tab === 'compare') {
    setTimeout(() => loadCompare(), 50);
  }
  if (tab === 'screeners') {
    const list = document.getElementById('scr-list');
    if (list && !list.children.length) initScrList();
  }
  if (tab === 'importer') {
    impCheckStored();
    if (typeof loadNseIndicesStatus === 'function') loadNseIndicesStatus();
  }
  if (tab === 'leaders') { if(!_leadersData) initLeadersTab(); }
  if (tab === 'smart-screener') { /* ready on demand — user clicks Run */ }
  if (tab === 'stockbee') { if(!_stockbeeData) loadStockbee(); }
  if (tab === 'smart-metrics') { /* ready on demand */ }
  if (tab === 'peep-into-past') { _initPeepChips(); }
  if (tab === 'charts') { setTimeout(() => initChartsTab(), 50); }
  if (tab === 'sectors') { setTimeout(() => loadHeatmap(), 200); }
  if (tab === 'watchlist') { initWatchlistTab(); }
  if (tab === 'scanner') {
    const g=document.getElementById('scn-gainers');
    if(g&&g.innerHTML.includes('Loading')) initScannerTab();
    else updateScannerMarketBar();
  }
}

// ─── SUMMARY MODAL ────────────────────────────────────────────────────────────

let _summaryHtml = '';

async function openSummaryModal() {
  const modal = document.getElementById('summary-modal');
  const body = document.getElementById('summary-modal-body');
  if (!modal || !body) return;
  modal.style.display = 'flex';
  body.innerHTML = '<div style="text-align:center;padding:40px;color:var(--text3)">Loading summary...</div>';

  try {
    const market = typeof currentMarket !== 'undefined' ? currentMarket : 'India';
    const res = await fetch(`${API}/api/summary/html?market=${market}`);
    _summaryHtml = await res.text();
    // Display in iframe to isolate email styles
    body.innerHTML = `<iframe id="summary-iframe" style="width:100%;height:100%;border:none;background:#fff;border-radius:6px" srcdoc="${_summaryHtml.replace(/"/g, '&quot;')}"></iframe>`;
  } catch (e) {
    body.innerHTML = `<div style="text-align:center;padding:40px;color:var(--red)">Error: ${e.message}</div>`;
  }
}

function closeSummaryModal() {
  const modal = document.getElementById('summary-modal');
  if (modal) modal.style.display = 'none';
}

function copySummaryHTML() {
  if (!_summaryHtml) return;
  navigator.clipboard.writeText(_summaryHtml).then(() => {
    const btn = document.querySelector('.summary-action-btn');
    if (btn) { const t = btn.textContent; btn.textContent = 'Copied!'; setTimeout(() => btn.textContent = t, 1500); }
  });
}

function downloadSummary() {
  if (!_summaryHtml) return;
  const blob = new Blob([_summaryHtml], { type: 'text/html' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  const market = typeof currentMarket !== 'undefined' ? currentMarket : 'India';
  a.download = `market_summary_${market}_${new Date().toISOString().slice(0, 10)}.html`;
  a.click();
  URL.revokeObjectURL(url);
}

// ─── SCREENER EXPORT ──────────────────────────────────────────────────────────

function _scrExportFilename(prefix) {
  const d = new Date().toISOString().slice(0, 10);
  return `${prefix}_${d}`;
}

function exportScreenerPNG() {
  const el = document.querySelector('.scr-tbl') || document.querySelector('.scn-results-tbl');
  if (!el) { alert('No results to export'); return; }
  html2canvas(el, { backgroundColor: '#0f0c29', scale: 2 }).then(canvas => {
    const a = document.createElement('a');
    a.href = canvas.toDataURL('image/png');
    a.download = _scrExportFilename('screener') + '.png';
    a.click();
  });
}

function exportScreenerExcel() {
  const tbl = document.querySelector('.scr-tbl') || document.querySelector('.scn-results-tbl');
  if (!tbl) { alert('No results to export'); return; }
  const wb = XLSX.utils.table_to_book(tbl, { sheet: 'Screener' });
  XLSX.writeFile(wb, _scrExportFilename('screener') + '.xlsx');
}

function exportScreenerPDF() {
  const tbl = document.querySelector('.scr-tbl') || document.querySelector('.scn-results-tbl');
  if (!tbl) { alert('No results to export'); return; }
  const { jsPDF } = window.jspdf;
  const doc = new jsPDF('l', 'pt', 'a4');
  doc.autoTable({ html: tbl, styles: { fontSize: 8 }, headStyles: { fillColor: [15, 12, 41] } });
  doc.save(_scrExportFilename('screener') + '.pdf');
}

function exportScannerPNG() {
  const el = document.getElementById('scn-qr-body');
  if (!el || !el.querySelector('table')) { alert('No results to export'); return; }
  html2canvas(el, { backgroundColor: '#0f0c29', scale: 2 }).then(canvas => {
    const a = document.createElement('a');
    a.href = canvas.toDataURL('image/png');
    a.download = _scrExportFilename('scanner') + '.png';
    a.click();
  });
}

function exportScannerExcel() {
  const tbl = document.querySelector('#scn-qr-body .scn-results-tbl');
  if (!tbl) { alert('No results to export'); return; }
  const wb = XLSX.utils.table_to_book(tbl, { sheet: 'Scanner' });
  XLSX.writeFile(wb, _scrExportFilename('scanner') + '.xlsx');
}

function exportScannerPDF() {
  const tbl = document.querySelector('#scn-qr-body .scn-results-tbl');
  if (!tbl) { alert('No results to export'); return; }
  const { jsPDF } = window.jspdf;
  const doc = new jsPDF('l', 'pt', 'a4');
  doc.autoTable({ html: tbl, styles: { fontSize: 8 }, headStyles: { fillColor: [15, 12, 41] } });
  doc.save(_scrExportFilename('scanner') + '.pdf');
}

// ─── FETCH LIVE ─────────────────────────────────────────────────────────────

async function fetchLive() {
  const btn = document.getElementById('fetch-live-btn');
  if (!btn) return;
  const origText = btn.textContent;
  btn.textContent = '⏳ Fetching...';
  btn.disabled = true;

  try {
    const res = await fetch(`${API}/api/fetch-live`, { method: 'POST' });
    const data = await res.json();
    console.log('Live fetch result:', data);
    // Re-load current tab data
    loadBreadth(true);
  } catch (e) {
    console.error('Live fetch failed:', e);
  } finally {
    btn.textContent = origText;
    btn.disabled = false;
  }
}

// ─── INIT ─────────────────────────────────────────────────────────────────────
setInterval(updateFreshness, 10000);

// Auto-load both markets in parallel
window.addEventListener('load', async () => {
  // Start India immediately (shown first)
  loadBreadth(false);

  // Load US in background after 2s delay to avoid rate limiting
  setTimeout(async () => {
    if (!currentData['US']) {
      try {
        const res = await fetch(`${API}/api/breadth/US`);
        if (res.ok) {
          const data = await res.json();
          if (!data.error) currentData['US'] = data;
          loadCompare();
        }
      } catch {}
    }
  }, 3000);

  // Wire autocomplete to Charts and Smart Metrics inputs
  if (typeof setupTickerAutocomplete === 'function') {
    setupTickerAutocomplete('chart-ticker-input', (ticker) => loadChart(ticker));
    setupTickerAutocomplete('sm-ticker-input', (ticker) => loadSmartMetrics(ticker));
  }
});


// ── NSE Data Sync (Yahoo v8 Direct API) ──────────────────────────────────────
async function startNseSync(range) {
  const statusEl = document.getElementById('nse-sync-status');
  const msgEl = document.getElementById('nse-sync-msg');
  const progEl = document.getElementById('nse-sync-progress');
  const barEl = document.getElementById('nse-sync-bar');
  if (!statusEl) return;

  statusEl.style.display = 'block';
  msgEl.textContent = 'Starting sync...';
  barEl.style.width = '0%';

  const endpoint = range === '2y' ? '/api/nse-sync/full' : `/api/nse-sync/start?range=${range}`;
  try {
    await fetch(`${API}${endpoint}`, { method: 'POST' });
  } catch(e) {
    msgEl.textContent = 'Failed to start sync: ' + e.message;
    return;
  }

  // Poll status
  const poll = setInterval(async () => {
    try {
      const res = await fetch(`${API}/api/nse-sync/status`);
      const s = await res.json();
      msgEl.textContent = s.message || 'Syncing...';
      if (s.total > 0) {
        const pct = Math.round((s.progress / s.total) * 100);
        progEl.textContent = `${s.progress}/${s.total}`;
        barEl.style.width = pct + '%';
      }
      if (!s.running) {
        clearInterval(poll);
        msgEl.textContent = s.message || 'Sync complete!';
        barEl.style.width = '100%';
        barEl.style.background = 'var(--green)';
        // Auto-refresh breadth data after sync
        setTimeout(() => loadBreadth(true), 2000);
      }
    } catch(e) {
      // ignore polling errors
    }
  }, 2000);
}

// ── Force EOD Sync — pulls today's data for ALL 500 tickers ──────────────────
async function startNseForceSync() {
  const statusEl = document.getElementById('nse-sync-status');
  const msgEl = document.getElementById('nse-sync-msg');
  const progEl = document.getElementById('nse-sync-progress');
  const barEl = document.getElementById('nse-sync-bar');
  if (!statusEl) return;

  statusEl.style.display = 'block';
  msgEl.textContent = 'Force EOD sync starting — all 500 tickers...';
  barEl.style.width = '0%';
  barEl.style.background = 'var(--accent1)';

  try {
    await fetch(`${API}/api/nse-sync/force-today`, { method: 'POST' });
  } catch(e) {
    msgEl.textContent = 'Failed: ' + e.message;
    return;
  }

  const poll = setInterval(async () => {
    try {
      const res = await fetch(`${API}/api/nse-sync/status`);
      const s = await res.json();
      msgEl.textContent = s.message || 'Syncing...';
      if (s.total > 0) {
        const pct = Math.round((s.progress / s.total) * 100);
        progEl.textContent = `${s.progress}/${s.total} (${pct}%)`;
        barEl.style.width = pct + '%';
      }
      if (!s.running) {
        clearInterval(poll);
        msgEl.textContent = '✅ ' + (s.message || 'EOD sync complete!');
        barEl.style.width = '100%';
        barEl.style.background = 'var(--green)';
        // Auto-refresh breadth after 2 sec
        setTimeout(() => { loadBreadth(true); }, 2000);
      }
    } catch(e) {}
  }, 2000);
}

// ── Sync Fundamentals (EPS, PE, Market Cap from Yahoo Finance) ───────────────
async function startFundamentalsSync() {
  const statusEl = document.getElementById('nse-sync-status');
  const msgEl = document.getElementById('nse-sync-msg');
  const progEl = document.getElementById('nse-sync-progress');
  const barEl = document.getElementById('nse-sync-bar');
  if (!statusEl) return;

  statusEl.style.display = 'block';
  msgEl.textContent = 'Fetching EPS, PE, Market Cap for all tickers...';
  barEl.style.width = '0%';
  barEl.style.background = '#f59e0b';

  try {
    await fetch(`${API}/api/fundamentals/sync`, { method: 'POST' });
  } catch(e) {
    msgEl.textContent = 'Failed: ' + e.message;
    return;
  }

  const poll = setInterval(async () => {
    try {
      const res = await fetch(`${API}/api/nse-sync/status`);
      const s = await res.json();
      msgEl.textContent = s.message || 'Syncing fundamentals...';
      if (s.total > 0) {
        const pct = Math.round((s.progress / s.total) * 100);
        progEl.textContent = `${s.progress}/${s.total} (${pct}%)`;
        barEl.style.width = pct + '%';
      }
      if (!s.running) {
        clearInterval(poll);
        msgEl.textContent = '✅ ' + (s.message || 'Fundamentals sync complete!');
        barEl.style.width = '100%';
        barEl.style.background = 'var(--green)';
      }
    } catch(e) {}
  }, 3000);
}
