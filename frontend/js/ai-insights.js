// ════════════════════════════════════════════════════════════════════════════
// AI INSIGHTS ENGINE — Groq Qwen 3 32B
// Handles: Market Intelligence (Overview) + Stock Analysis (Smart Metrics)
// ════════════════════════════════════════════════════════════════════════════

let _aiKeyConfigured = null;   // cached key status
let currentAiTicker  = null;   // current stock being analysed

// ── Check key status on load ──────────────────────────────────────────────────
async function checkAiKeyStatus() {
  try {
    const res  = await fetch(`${API}/api/ai/settings`);
    const data = await res.json();
    _aiKeyConfigured = data.configured;
    return data.configured;
  } catch(e) {
    _aiKeyConfigured = false;
    return false;
  }
}

// ── Format AI response text → HTML ───────────────────────────────────────────
function _formatAiText(text) {
  if (!text) return '';

  // Convert section headers (ALL CAPS: or numbered) to styled labels
  let html = text
    // Bold numbered sections: "1. SECTION NAME:" or "SECTION NAME:"
    .replace(/^(\d+\.\s+)?([A-Z][A-Z\s\/&]+):/gm,
      '<span class="ai-section-label">$1$2</span>')
    // Bullet points
    .replace(/^[-•]\s+/gm, '<span style="color:var(--cyan)">▸</span> ')
    // Newlines to paragraphs
    .split('\n\n').map(p => p.trim() ? `<p>${p.replace(/\n/g,'<br>')}</p>` : '')
    .join('');

  return html;
}

// ══════════════════════════════════════════════════════════════════════════════
// FEATURE 1 — MARKET INTELLIGENCE (Overview Tab)
// ══════════════════════════════════════════════════════════════════════════════

async function loadMarketIntelligence(forceRefresh = false) {
  const card    = document.getElementById('ai-market-card');
  const nokey   = document.getElementById('ai-market-nokey');
  const loading = document.getElementById('ai-market-loading');
  const body    = document.getElementById('ai-market-text');
  const meta    = document.getElementById('ai-market-meta');
  if (!card) return;

  // Check key
  const hasKey = _aiKeyConfigured !== null
    ? _aiKeyConfigured
    : await checkAiKeyStatus();

  if (!hasKey) {
    if (nokey)   nokey.style.display   = 'flex';
    if (loading) loading.style.display = 'none';
    if (body)    body.innerHTML        = '';
    return;
  }

  if (nokey)   nokey.style.display   = 'none';
  if (loading) loading.style.display = 'flex';
  if (body)    body.innerHTML        = '';
  if (meta)    meta.style.display    = 'none';

  try {
    const url = `${API}/api/ai/market-intelligence?market=INDIA${forceRefresh ? '&refresh=true' : ''}`;
    const res  = await fetch(url);
    const data = await res.json();

    if (loading) loading.style.display = 'none';

    if (data.error) {
      if (data.error === 'no_api_key') {
        if (nokey) nokey.style.display = 'flex';
        if (_aiKeyConfigured !== false) {
          _aiKeyConfigured = false;
        }
      } else {
        if (body) body.innerHTML =
          `<div class="ai-error">⚠ ${data.error}</div>`;
      }
      return;
    }

    if (body) body.innerHTML = _formatAiText(data.text);

    if (meta) {
      meta.style.display = 'flex';
      meta.innerHTML =
        `<span>Model: ${data.model || 'Qwen 3 32B'}</span>` +
        `<span>Regime: ${data.regime || '—'}</span>` +
        `<span>Score: ${data.score || '—'}/100</span>` +
        (data.cached ? '<span>⚡ cached</span>' : '') +
        `<span>${data.elapsed ? data.elapsed+'s' : ''}</span>`;
    }

  } catch(e) {
    if (loading) loading.style.display = 'none';
    if (body) body.innerHTML =
      `<div class="ai-error">⚠ Failed to load: ${e.message}</div>`;
  }
}


// ══════════════════════════════════════════════════════════════════════════════
// FEATURE 2 — STOCK ANALYSIS (Smart Metrics Tab)
// ══════════════════════════════════════════════════════════════════════════════

async function loadStockAnalysis(ticker, forceRefresh = false) {
  if (!ticker) return;
  currentAiTicker = ticker;

  const card    = document.getElementById('ai-stock-card');
  const loading = document.getElementById('ai-stock-loading');
  const loadTxt = document.getElementById('ai-stock-loading-text');
  const body    = document.getElementById('ai-stock-text');
  const meta    = document.getElementById('ai-stock-meta');
  if (!card) return;

  // Show the card
  card.style.display = 'block';
  if (loading) loading.style.display = 'flex';
  if (loadTxt) loadTxt.textContent   = `Analysing ${ticker}...`;
  if (body)    body.innerHTML        = '';
  if (meta)    meta.style.display    = 'none';

  // Scroll to AI card
  setTimeout(() => card.scrollIntoView({ behavior: 'smooth', block: 'nearest' }), 100);

  try {
    const res  = await fetch(`${API}/api/ai/stock-analysis/${ticker}`);
    const data = await res.json();

    if (loading) loading.style.display = 'none';

    if (data.error === 'no_api_key') {
      if (body) body.innerHTML =
        `<div class="ai-no-key">
           <span>🔑 Groq API key not configured</span>
           <button class="ai-setup-btn" onclick="openAiSettings()">Setup →</button>
         </div>`;
      return;
    }

    if (data.error) {
      if (body) body.innerHTML =
        `<div class="ai-error">⚠ ${data.error}</div>`;
      return;
    }

    if (body) body.innerHTML = _formatAiText(data.text);

    if (meta) {
      meta.style.display = 'flex';
      meta.innerHTML =
        `<span>Ticker: ${ticker}</span>` +
        `<span>Model: ${data.model || 'Qwen 3 32B'}</span>` +
        (data.cached ? '<span>⚡ cached</span>' : '') +
        `<span>${data.elapsed ? data.elapsed+'s' : ''}</span>`;
    }

  } catch(e) {
    if (loading) loading.style.display = 'none';
    if (body) body.innerHTML =
      `<div class="ai-error">⚠ ${e.message}</div>`;
  }
}


// ══════════════════════════════════════════════════════════════════════════════
// AI SETTINGS MODAL
// ══════════════════════════════════════════════════════════════════════════════

function openAiSettings() {
  const modal = document.getElementById('ai-settings-modal');
  if (modal) modal.style.display = 'flex';
  // Pre-fill if already configured
  fetch(`${API}/api/ai/settings`).then(r => r.json()).then(d => {
    const status = document.getElementById('ai-key-status');
    if (status && d.configured) {
      status.textContent = `✅ Current key: ${d.masked_key}`;
      status.style.color = 'var(--green)';
    }
  });
}

function closeAiSettings() {
  const modal = document.getElementById('ai-settings-modal');
  if (modal) modal.style.display = 'none';
}

async function saveAiApiKey() {
  const input  = document.getElementById('ai-key-input');
  const status = document.getElementById('ai-key-status');
  const btn    = document.querySelector('.ai-save-btn');
  if (!input) return;

  const key = input.value.trim();
  if (!key || !key.startsWith('gsk_')) {
    if (status) {
      status.textContent = '⚠ Key must start with gsk_';
      status.style.color = 'var(--red)';
    }
    return;
  }

  if (btn) { btn.textContent = 'Validating...'; btn.disabled = true; }
  if (status) { status.textContent = 'Testing connection...'; status.style.color = 'var(--text3)'; }

  try {
    const res  = await fetch(`${API}/api/ai/settings`, {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ groq_api_key: key }),
    });
    const data = await res.json();

    if (data.ok) {
      if (status) {
        status.textContent = '✅ ' + data.message;
        status.style.color = 'var(--green)';
      }
      _aiKeyConfigured = true;
      setTimeout(() => {
        closeAiSettings();
        // Reload insights now that key is set
        loadMarketIntelligence(false);
      }, 1200);
    } else {
      if (status) {
        status.textContent = '❌ ' + (data.error || 'Invalid key');
        status.style.color = 'var(--red)';
      }
    }
  } catch(e) {
    if (status) {
      status.textContent = '❌ ' + e.message;
      status.style.color = 'var(--red)';
    }
  } finally {
    if (btn) { btn.textContent = 'Save & Validate Key'; btn.disabled = false; }
  }
}

// Close modal on backdrop click
document.addEventListener('click', e => {
  const modal = document.getElementById('ai-settings-modal');
  if (modal && e.target === modal) closeAiSettings();
});


// ══════════════════════════════════════════════════════════════════════════════
// INIT — Load market intelligence on app start
// ══════════════════════════════════════════════════════════════════════════════
window.addEventListener('load', async () => {
  // Check key status first
  await checkAiKeyStatus();
  // Load market intelligence after breadth data loads (2s delay)
  setTimeout(() => loadMarketIntelligence(false), 3000);
});
