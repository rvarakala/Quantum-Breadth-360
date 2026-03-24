// ─── DATA FETCHING ────────────────────────────────────────────────────────────

async function loadBreadth(force = false) {
  const btn = $('refresh-btn');
  btn.classList.add('loading');

  try {
    const url = `${API}/api/breadth/${currentMarket}${force ? '?refresh=true' : ''}`;
    const res = await fetch(url);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();

    if (data.error) {
      showError(data.error);
      return;
    }

    currentData[currentMarket] = data;
    lastUpdated[currentMarket] = new Date();

    renderOverview(data);
    renderCharts(data);
    renderSectors(data);
    updateFreshness();

    // Also refresh compare data
    loadCompare();

  } catch (e) {
    console.error('Fetch error:', e);
    showError('Backend not reachable. Start the backend server first.');
  } finally {
    btn.classList.remove('loading');
  }
}

async function loadCompare() {
  try {
    const res = await fetch(`${API}/api/compare`);
    if (!res.ok) return;
    const data = await res.json();
    renderCompare(data.India, data.US);
  } catch {}
}

function showError(msg) {
  const grid = $('sector-grid');
  // Show error in score area
  $('score-interp').textContent = `⚠ ${msg}`;
  $('score-interp').style.color = '#ef4444';
}

function updateFreshness() {
  const lu = lastUpdated[currentMarket];
  const dot = document.querySelector('.freshness-dot');
  const txt = $('freshness-text');
  if (!lu) {
    dot.classList.add('stale');
    txt.textContent = 'Not loaded';
    return;
  }

  // Show OHLCV data date if available — more meaningful than API call time
  const data = currentData[currentMarket];
  const ohlcvDate = data?.last_ohlcv_date;
  const freshness = data?.data_freshness;

  if (ohlcvDate && ohlcvDate !== 'unknown') {
    dot.classList.remove('stale');
    const d = new Date(ohlcvDate);
    const formatted = d.toLocaleDateString('en-IN', {day:'2-digit', month:'short'});
    if (freshness === 'today') {
      txt.textContent = `Data: Today (${formatted})`;
      dot.style.background = 'var(--green)';
    } else if (freshness === 'EOD') {
      txt.textContent = `Data: EOD ${formatted}`;
      dot.style.background = '#f59e0b';
      dot.classList.add('stale');
    } else {
      txt.textContent = `Data: ${formatted} ⚠ stale`;
      dot.style.background = 'var(--red)';
      dot.classList.add('stale');
    }
    return;
  }

  // Fallback: show API call age
  const age = Math.round((Date.now() - lu) / 1000);
  dot.classList.remove('stale');
  if (age < 30) txt.textContent = 'Just updated';
  else if (age < 60) txt.textContent = `${age}s ago`;
  else {
    const m = Math.round(age / 60);
    txt.textContent = `${m}m ago`;
    if (m > 20) dot.classList.add('stale');
  }
}

// ─── NAVIGATION ───────────────────────────────────────────────────────────────

function switchMarket(market) {
  currentMarket = market;
  document.querySelectorAll('.market-tab').forEach(t => {
    t.classList.toggle('active', t.dataset.market === market);
  });

  if (market === 'COMPARE') {
    switchTab('compare');
    loadCompare();
    return;
  }
  switchTab('overview');

  if (currentData[market]) {
    renderOverview(currentData[market]);
    renderCharts(currentData[market]);
    renderSectors(currentData[market]);
    updateFreshness();
  } else {
    loadBreadth(false);
  }
}
