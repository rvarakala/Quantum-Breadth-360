// ════════════════════════════════════════════════════════════════════════════
// SMART SCREENER ENGINE
// Two-pass: RS+Stage pre-filter → Full SMART score on candidates
// ════════════════════════════════════════════════════════════════════════════

let _smartScrData    = null;
let _smartScrSorted  = { key: 'smart_score', dir: -1 };
let _smartScrPage    = 1;
const SMART_SCR_PAGE = 20;

// ── Column definitions ────────────────────────────────────────────────────────
const SMART_SCR_COLS = [
  { key: '#',           label: '#',         w: '36px',  align: 'center' },
  { key: 'ticker',      label: 'SYMBOL',    w: '90px',  align: 'left'   },
  { key: 'company',     label: 'COMPANY',   w: '140px', align: 'left'   },
  { key: 'smart_score', label: 'SMART ★',   w: '80px',  align: 'center' },
  { key: 'components',  label: 'BREAKDOWN', w: '150px', align: 'left'   },
  { key: 'stage',       label: 'STAGE',     w: '90px',  align: 'center' },
  { key: 'rs_rank',     label: 'RS',        w: '56px',  align: 'center' },
  { key: 'ad_rating',   label: 'A/D',       w: '56px',  align: 'center' },
  { key: 'om_grade',    label: 'OM',        w: '56px',  align: 'center' },
  { key: 'tpr',         label: 'TPR',       w: '56px',  align: 'center' },
  { key: 'pct_from_high',label:'FROM HI',   w: '76px',  align: 'right'  },
  { key: 'mcap_fmt',    label: 'MCAP',      w: '80px',  align: 'right'  },
  { key: 'tags',        label: 'TAGS',      w: '160px', align: 'left'   },
];

// ── Component mini-bar ────────────────────────────────────────────────────────
function _compBar(stock) {
  const segs = [
    { label: 'Fund',  val: stock.fund_score,  color: '#7c3aed' },
    { label: 'Tech',  val: stock.tech_score,  color: '#2563eb' },
    { label: 'RS',    val: stock.rs_score,    color: '#0891b2' },
    { label: 'Stage', val: stock.stage_score, color: '#16a34a' },
    { label: 'TPR',   val: stock.tpr_score,   color: '#d97706' },
  ];
  const bars = segs.map(seg => {
    const pct  = Math.max(4, seg.val);
    const tip  = `${seg.label}: ${seg.val}`;
    return `<div class="smart-comp-seg" style="background:${seg.color};
      opacity:${0.3 + seg.val/100*0.7};flex:${pct}" title="${tip}"></div>`;
  }).join('');
  return `<div class="smart-comp-bar" title="Fund ${stock.fund_score} | Tech ${stock.tech_score} | RS ${stock.rs_score} | Stage ${stock.stage_score} | TPR ${stock.tpr_score}">${bars}</div>`;
}

// ── Render results table ──────────────────────────────────────────────────────
function _renderSmartScrTable(stocks) {
  const wrap = document.getElementById('smart-scr-tbl-wrap');
  if (!wrap) return;

  if (!stocks || !stocks.length) {
    wrap.innerHTML = `<div class="smart-empty-state">
      <div style="font-size:20px;margin-bottom:8px">🔍</div>
      No stocks matched your criteria.<br>
      <span style="font-size:10px;color:var(--text3)">Try lowering the SMART threshold or removing Stage 2 filter.</span>
    </div>`;
    return;
  }

  // Sort
  const sorted = [...stocks].sort((a, b) => {
    const av = a[_smartScrSorted.key] ?? 0;
    const bv = b[_smartScrSorted.key] ?? 0;
    if (typeof av === 'string') return _smartScrSorted.dir * av.localeCompare(bv);
    return _smartScrSorted.dir * (av - bv);
  });

  // Paginate
  const totalPages = Math.ceil(sorted.length / SMART_SCR_PAGE);
  _smartScrPage = Math.min(_smartScrPage, totalPages);
  const start  = (_smartScrPage - 1) * SMART_SCR_PAGE;
  const page   = sorted.slice(start, start + SMART_SCR_PAGE);

  // Build table
  const thead = SMART_SCR_COLS.map(c => {
    const isSort = c.key === _smartScrSorted.key;
    const arrow  = isSort ? (_smartScrSorted.dir === -1 ? ' ↓' : ' ↑') : ' ↕';
    return `<th class="smart-scr-th${isSort ? ' sorted' : ''}"
      style="width:${c.w};text-align:${c.align}"
      onclick="sortSmartScr('${c.key}')">${c.label}${arrow}</th>`;
  }).join('');

  const tbody = page.map((s, i) => {
    // SMART pill
    const pillCls = s.smart_score >= 70 ? 'strong' : s.smart_score >= 60 ? 'good' : 'avoid';
    const smartPill = `<span class="smart-score-pill ${pillCls}">${s.smart_score}</span>`;

    // Stage badge
    const stageColor = s.stage_num === 2 ? '#22c55e' : s.stage_num === 1 ? '#60a5fa' :
                       s.stage_num === 3 ? '#f59e0b' : '#ef4444';
    const stageBdg = `<span style="color:${stageColor};font-weight:700">${s.stage || '—'}</span>`;

    // RS badge
    const rsCol = s.rs_rank >= 90 ? '#22c55e' : s.rs_rank >= 80 ? '#60a5fa' : '#f59e0b';
    const rsBdg = `<span style="color:${rsCol};font-weight:700">${s.rs_rank}</span>`;

    // A/D badge
    const adCls = s.ad_rating?.startsWith('A') ? '#22c55e' :
                  s.ad_rating?.startsWith('B') ? '#06b6d4' : '#f59e0b';
    const adBdg = `<span style="color:${adCls}">${s.ad_rating || '—'}</span>`;

    // OM grade
    const omCol = ['A+','A','A-'].includes(s.om_grade) ? '#22c55e' :
                  ['B+','B'].includes(s.om_grade) ? '#06b6d4' : '#94a3b8';
    const omBdg = `<span style="color:${omCol}">${s.om_grade}</span>`;

    // From high
    const pfhCol = s.pct_from_high >= -5 ? 'var(--green)' :
                   s.pct_from_high >= -15 ? 'var(--amber)' : 'var(--red)';
    const pfhStr = `<span style="color:${pfhCol}">${Number(s.pct_from_high||0).toFixed(1)}%</span>`;

    // Tags
    const tags = (s.tags||[]).slice(0,3).map(t =>
      `<span class="smart-tag">${t}</span>`).join('');

    const cells = [
      `<td class="smart-scr-td" style="text-align:center;color:var(--text3)">${start+i+1}</td>`,
      `<td class="smart-scr-td"><span class="ticker-link" onclick="loadSmartMetrics('${s.ticker}')" style="font-weight:700;cursor:pointer">${s.ticker}</span></td>`,
      `<td class="smart-scr-td" style="color:var(--text2);max-width:140px;overflow:hidden;text-overflow:ellipsis">${s.company||'—'}</td>`,
      `<td class="smart-scr-td" style="text-align:center">${smartPill}</td>`,
      `<td class="smart-scr-td">${_compBar(s)}</td>`,
      `<td class="smart-scr-td" style="text-align:center">${stageBdg}</td>`,
      `<td class="smart-scr-td" style="text-align:center">${rsBdg}</td>`,
      `<td class="smart-scr-td" style="text-align:center">${adBdg}</td>`,
      `<td class="smart-scr-td" style="text-align:center">${omBdg}</td>`,
      `<td class="smart-scr-td" style="text-align:center">${s.tpr||'—'}</td>`,
      `<td class="smart-scr-td" style="text-align:right">${pfhStr}</td>`,
      `<td class="smart-scr-td" style="text-align:right;color:var(--text3)">${s.mcap_fmt||'—'}</td>`,
      `<td class="smart-scr-td">${tags}</td>`,
    ].join('');

    return `<tr class="smart-scr-tr">${cells}</tr>`;
  }).join('');

  // Pagination
  const pagBtns = totalPages <= 1 ? '' : `
    <div style="display:flex;align-items:center;gap:6px;padding:10px 0;font-family:var(--font-mono)">
      <button onclick="smartScrGoPage(${_smartScrPage-1})"
        style="padding:4px 10px;background:var(--bg2);border:1px solid var(--border2);
        border-radius:4px;color:var(--text2);cursor:pointer;font-size:11px"
        ${_smartScrPage===1?'disabled':''}>← Prev</button>
      <span style="font-size:11px;color:var(--text3)">
        ${start+1}–${Math.min(start+SMART_SCR_PAGE,sorted.length)} of ${sorted.length}
      </span>
      <button onclick="smartScrGoPage(${_smartScrPage+1})"
        style="padding:4px 10px;background:var(--bg2);border:1px solid var(--border2);
        border-radius:4px;color:var(--text2);cursor:pointer;font-size:11px"
        ${_smartScrPage===totalPages?'disabled':''}>Next →</button>
    </div>`;

  wrap.innerHTML = `
    <table class="smart-scr-tbl">
      <thead><tr>${thead}</tr></thead>
      <tbody>${tbody}</tbody>
    </table>
    ${pagBtns}`;
}

// ── Sort handler ─────────────────────────────────────────────────────────────
function sortSmartScr(key) {
  if (key === '#' || key === 'components' || key === 'tags') return;
  if (_smartScrSorted.key === key) {
    _smartScrSorted.dir *= -1;
  } else {
    _smartScrSorted.key = key;
    _smartScrSorted.dir = -1;
  }
  _smartScrPage = 1;
  if (_smartScrData) _renderSmartScrTable(_smartScrData.stocks);
}

function smartScrGoPage(p) {
  if (!_smartScrData) return;
  const total = Math.ceil((_smartScrData.stocks||[]).length / SMART_SCR_PAGE);
  if (p < 1 || p > total) return;
  _smartScrPage = p;
  _renderSmartScrTable(_smartScrData.stocks);
  document.getElementById('tab-smart-screener')?.scrollTo(0, 0);
}

// ── Run screener ──────────────────────────────────────────────────────────────
async function runSmartScreener() {
  const btn      = document.getElementById('smart-scr-run-btn');
  const progress = document.getElementById('smart-scr-progress');
  const msg      = document.getElementById('smart-scr-prog-msg');
  const num      = document.getElementById('smart-scr-prog-num');
  const fill     = document.getElementById('smart-scr-prog-fill');
  const stats    = document.getElementById('smart-scr-stats');

  // Read params
  const minSmart  = document.getElementById('smart-scr-min-smart')?.value || 70;
  const stage2    = document.getElementById('smart-scr-stage2')?.value !== 'false';
  const minRs     = document.getElementById('smart-scr-min-rs')?.value || 60;
  const minMcap   = document.getElementById('smart-scr-mcap')?.value || 500;

  if (btn) { btn.disabled = true; btn.innerHTML = '<span>⏳</span> Running...'; }
  if (progress) progress.style.display = 'block';
  if (fill) { fill.style.width = '0%'; }
  if (stats) stats.style.display = 'none';

  try {
    const params = new URLSearchParams({
      min_smart: minSmart, require_stage2: stage2,
      min_rs: minRs, min_mcap_cr: minMcap,
    });
    await fetch(`${API}/api/screener/smart/run?${params}`, { method: 'POST' });
  } catch(e) {
    if (msg) msg.textContent = 'Failed to start: ' + e.message;
    if (btn) { btn.disabled = false; btn.innerHTML = '<span>▶</span> Run Screener'; }
    return;
  }

  // Poll progress (max 30 minutes timeout)
  let _pollCount = 0;
  const poll = setInterval(async () => {
    _pollCount++;
    if (_pollCount > 900) {  // 900 × 2s = 30 minutes max
      clearInterval(poll);
      if (btn) { btn.disabled = false; btn.innerHTML = '<span>▶</span> Run Screener'; }
      if (msg) msg.textContent = 'Timed out — try again';
      return;
    }
    try {
      const res = await fetch(`${API}/api/screener/smart/status`);
      const s   = await res.json();

      if (msg) msg.textContent = s.message || 'Running...';
      if (s.total > 0 && fill) {
        const pct = Math.min(98, Math.round((s.progress / s.total) * 100));
        fill.style.width = pct + '%';
        if (num) num.textContent = `${s.progress}/${s.total}`;
      }

      if (!s.running && s.result) {
        clearInterval(poll);
        if (fill) fill.style.width = '100%';
        if (btn) { btn.disabled = false; btn.innerHTML = '<span>▶</span> Run Screener'; }
        if (progress) setTimeout(() => { if(progress) progress.style.display='none'; }, 2000);

        _smartScrData = s.result;
        _smartScrPage = 1;

        // Show stats
        if (stats) {
          stats.style.display = 'flex';
          document.getElementById('sss-found').textContent    = s.result.total || 0;
          document.getElementById('sss-screened').textContent = s.result.screened || 0;
          document.getElementById('sss-universe').textContent = s.result.pre_filter_total || 0;
          document.getElementById('sss-elapsed').textContent  = (s.result.elapsed||0) + 's';
          const cachedEl = document.getElementById('sss-cached');
          if (cachedEl) cachedEl.style.display = s.result.cached ? 'inline' : 'none';
        }

        _renderSmartScrTable(s.result.stocks || []);
      }
    } catch(e) { /* ignore polling errors */ }
  }, 2000);
}

// ── Click ticker → go to Smart Metrics ───────────────────────────────────────
// loadSmartMetrics is defined in smart-metrics.js — ticker-link opens it
