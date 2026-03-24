// ════════════════════════════════════════════════════════════════════════════
// WATCHLIST TAB
// ════════════════════════════════════════════════════════════════════════════

let _wlList = [];
let _wlActiveId = null;
let _wlData = null;
let _wlInited = false;
let _wlAlertCount = 0;

async function initWatchlistTab() {
  if (_wlInited) { if (_wlActiveId) loadWatchlistData(_wlActiveId); return; }
  _wlInited = true;
  await refreshWatchlists();
  checkAlertsBadge();
}

async function refreshWatchlists() {
  try {
    const res = await fetch(`${API}/api/watchlist`);
    const data = await res.json();
    _wlList = data.watchlists || [];
    _renderWlSidebar();
    if (_wlList.length && !_wlActiveId) {
      selectWatchlist(_wlList[0].id);
    } else if (_wlActiveId) {
      loadWatchlistData(_wlActiveId);
    } else {
      _renderWlEmpty();
    }
  } catch (e) {
    console.warn('Watchlist load error:', e.message);
  }
}

function _renderWlSidebar() {
  const el = document.getElementById('wl-sidebar-list');
  if (!el) return;
  if (!_wlList.length) {
    el.innerHTML = '<div style="padding:16px;color:var(--text3);font-size:11px;text-align:center">No watchlists yet</div>';
    return;
  }
  el.innerHTML = _wlList.map(w => `
    <div class="wl-sidebar-item ${w.id === _wlActiveId ? 'active' : ''}" onclick="selectWatchlist(${w.id})">
      <span class="wl-sidebar-name">${w.name}</span>
      <span class="wl-sidebar-count">${w.item_count || 0}</span>
      <button class="wl-sidebar-del" onclick="event.stopPropagation();deleteWatchlist(${w.id})" title="Delete">✕</button>
    </div>
  `).join('');
}

function _renderWlEmpty() {
  const el = document.getElementById('wl-main-body');
  if (el) el.innerHTML = '<div style="text-align:center;padding:60px 0;color:var(--text3)">Select or create a watchlist to begin</div>';
}

async function selectWatchlist(id) {
  _wlActiveId = id;
  _renderWlSidebar();
  await loadWatchlistData(id);
}

async function loadWatchlistData(id) {
  const body = document.getElementById('wl-main-body');
  if (!body) return;
  body.innerHTML = '<div style="text-align:center;padding:40px;color:var(--text3)">Loading...</div>';

  try {
    const res = await fetch(`${API}/api/watchlist/${id}/data`);
    const data = await res.json();
    if (data.error) throw new Error(data.error);
    _wlData = data;
    _renderWlTable(data);
  } catch (e) {
    body.innerHTML = `<div style="text-align:center;padding:40px;color:var(--red)">Error: ${e.message}</div>`;
  }
}

function _renderWlTable(data) {
  const body = document.getElementById('wl-main-body');
  if (!body) return;

  const f = (v, d = 1) => v == null ? '—' : Number(v).toFixed(d);
  const gc = v => v >= 0 ? 'var(--green)' : 'var(--red)';
  const dmaIcon = v => v === true ? '<span style="color:var(--green)">&#9650;</span>' : v === false ? '<span style="color:var(--red)">&#9660;</span>' : '—';

  const title = document.getElementById('wl-main-title');
  if (title) title.textContent = data.name || 'Watchlist';

  if (!data.stocks || !data.stocks.length) {
    body.innerHTML = '<div style="text-align:center;padding:40px;color:var(--text3)">Empty watchlist. Add stocks above.</div>';
    return;
  }

  body.innerHTML = `
    <table class="wl-table">
      <thead><tr>
        <th class="wl-th">SYMBOL</th>
        <th class="wl-th" style="text-align:right">PRICE</th>
        <th class="wl-th" style="text-align:right">1W%</th>
        <th class="wl-th" style="text-align:right">1M%</th>
        <th class="wl-th" style="text-align:right">3M%</th>
        <th class="wl-th" style="text-align:center">50D</th>
        <th class="wl-th" style="text-align:center">200D</th>
        <th class="wl-th" style="text-align:center">MCAP</th>
        <th class="wl-th">SECTOR</th>
        <th class="wl-th">NOTES</th>
        <th class="wl-th" style="text-align:center">ALERT</th>
        <th class="wl-th" style="text-align:center"></th>
      </tr></thead>
      <tbody>
        ${data.stocks.map(s => `<tr class="wl-tr">
          <td class="wl-td" style="font-weight:700;font-family:var(--font-mono)">
            <span class="ticker-link" onclick="openTickerChart('${s.ticker}')">${s.ticker}</span>
          </td>
          <td class="wl-td" style="text-align:right;font-family:var(--font-mono)">${s.price ? '₹' + s.price.toLocaleString('en-IN', {maximumFractionDigits: 1}) : '—'}</td>
          <td class="wl-td" style="text-align:right;color:${gc(s.chg_1w)}">${s.chg_1w != null ? (s.chg_1w >= 0 ? '+' : '') + f(s.chg_1w) + '%' : '—'}</td>
          <td class="wl-td" style="text-align:right;color:${gc(s.chg_1m)}">${s.chg_1m != null ? (s.chg_1m >= 0 ? '+' : '') + f(s.chg_1m) + '%' : '—'}</td>
          <td class="wl-td" style="text-align:right;color:${gc(s.chg_3m)}">${s.chg_3m != null ? (s.chg_3m >= 0 ? '+' : '') + f(s.chg_3m) + '%' : '—'}</td>
          <td class="wl-td" style="text-align:center">${dmaIcon(s.above_50dma)}</td>
          <td class="wl-td" style="text-align:center">${dmaIcon(s.above_200dma)}</td>
          <td class="wl-td" style="text-align:center">${s.mcap_tier ? (() => {
            const cls = s.mcap_tier.startsWith('Mega') ? 'mcap-mega'
              : s.mcap_tier.startsWith('Large') ? 'mcap-large'
              : s.mcap_tier.startsWith('Mid') ? 'mcap-mid'
              : s.mcap_tier.startsWith('Small') ? 'mcap-small' : 'mcap-micro';
            return `<span class="mcap-bdg ${cls}">${s.mcap_tier.replace(' Cap','')}</span>`;
          })() : '—'}</td>
          <td class="wl-td" style="font-size:10px">${s.sector || '—'}</td>
          <td class="wl-td" style="font-size:10px;color:var(--text3);max-width:100px;overflow:hidden;text-overflow:ellipsis">${s.notes || ''}</td>
          <td class="wl-td" style="text-align:center">
            <button class="wl-alert-btn ${s.alerts && s.alerts.some(a => a.triggered) ? 'triggered' : ''}" onclick="openAlertModal('${s.ticker}')" title="Set alert">
              ${s.alerts && s.alerts.length ? '🔔' : '🔕'}
            </button>
          </td>
          <td class="wl-td" style="text-align:center">
            <button class="wl-remove-btn" onclick="removeWlTicker(${_wlActiveId},'${s.ticker}')" title="Remove">✕</button>
          </td>
        </tr>`).join('')}
      </tbody>
    </table>`;
}

async function createNewWatchlist() {
  const input = document.getElementById('wl-new-name');
  const name = input ? input.value.trim() : '';
  if (!name) return;
  try {
    await fetch(`${API}/api/watchlist`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name })
    });
    input.value = '';
    await refreshWatchlists();
  } catch (e) { console.warn('Create watchlist error:', e.message); }
}

async function deleteWatchlist(id) {
  try {
    await fetch(`${API}/api/watchlist/${id}`, { method: 'DELETE' });
    if (_wlActiveId === id) _wlActiveId = null;
    await refreshWatchlists();
  } catch (e) { console.warn('Delete watchlist error:', e.message); }
}

async function addWlTicker() {
  if (!_wlActiveId) return;
  const input = document.getElementById('wl-add-ticker');
  const ticker = input ? input.value.trim().toUpperCase() : '';
  if (!ticker) return;
  try {
    const res = await fetch(`${API}/api/watchlist/${_wlActiveId}/add`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ticker })
    });
    const data = await res.json();
    if (data.error) { alert(data.error); return; }
    input.value = '';
    await loadWatchlistData(_wlActiveId);
    await refreshWatchlists();
  } catch (e) { console.warn('Add ticker error:', e.message); }
}

async function removeWlTicker(wid, ticker) {
  try {
    await fetch(`${API}/api/watchlist/${wid}/remove/${ticker}`, { method: 'DELETE' });
    await loadWatchlistData(wid);
    await refreshWatchlists();
  } catch (e) { console.warn('Remove ticker error:', e.message); }
}

// ── Alert Modal ───────────────────────────────────────────────────────────────

function openAlertModal(ticker) {
  const modal = document.getElementById('alert-modal');
  if (!modal) return;
  document.getElementById('alert-modal-ticker').textContent = ticker;
  document.getElementById('alert-ticker-hidden').value = ticker;
  document.getElementById('alert-condition').value = 'price_above';
  document.getElementById('alert-value').value = '';
  modal.style.display = 'flex';
}

function closeAlertModal() {
  const modal = document.getElementById('alert-modal');
  if (modal) modal.style.display = 'none';
}

async function saveAlert() {
  const ticker = document.getElementById('alert-ticker-hidden').value;
  const condition = document.getElementById('alert-condition').value;
  const value = parseFloat(document.getElementById('alert-value').value) || null;

  try {
    await fetch(`${API}/api/alerts`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ticker, condition_type: condition, condition_value: value })
    });
    closeAlertModal();
    if (_wlActiveId) loadWatchlistData(_wlActiveId);
  } catch (e) { console.warn('Save alert error:', e.message); }
}

async function checkAlertsBadge() {
  try {
    const res = await fetch(`${API}/api/alerts/check`);
    const data = await res.json();
    _wlAlertCount = data.count || 0;
    const badge = document.getElementById('wl-alert-badge');
    if (badge) {
      badge.textContent = _wlAlertCount;
      badge.style.display = _wlAlertCount > 0 ? 'inline-block' : 'none';
    }
  } catch (e) { /* silent */ }
}
