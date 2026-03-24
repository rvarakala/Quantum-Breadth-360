// ════════════════════════════════════════════════════════════════════════════
// SCREENERS ENGINE
// ════════════════════════════════════════════════════════════════════════════

const SCR_DEFS = [
  { id:'rs90',     lbl:'RS Rating 90+',            desc:'IBD-style RS ≥ 90 — top 10% momentum', tag:'MOMENTUM', col:'#22c55e' },
  { id:'rs80',     lbl:'RS Rating 80+',            desc:'IBD-style RS ≥ 80 — top 20% momentum', tag:'MOMENTUM', col:'#22c55e' },
  { id:'rs_up',    lbl:'RS Line New High',         desc:'RS Line trending higher than index',    tag:'RS LINE',  col:'#1de9b6' },
  { id:'stage2',   lbl:'Weinstein Stage 2',        desc:'Price > 30W MA, rising trend, RS > 60', tag:'STAGE',    col:'#3b82f6' },
  { id:'mtt',      lbl:'Minervini Trend Template', desc:'RS > 70, within 15% of 52W high, uptrend', tag:'MTT',   col:'#a855f7' },
  { id:'vcp',      lbl:'VCP Pattern',              desc:'Volatility contraction + RS > 75',      tag:'PATTERN',  col:'#06b6d4' },
  { id:'pocket',   lbl:'Pocket Pivot',             desc:'Volume surge above down-day average',   tag:'SIGNAL',   col:'#f59e0b' },
  { id:'near_high',lbl:'Near 52W High',            desc:'Within 5% of 52-week high',             tag:'SETUP',    col:'#f97316' },
  { id:'vol_dry',  lbl:'Volume Dry-Up',            desc:'Today volume < 50% of 50-day average',  tag:'SETUP',    col:'#e879f9' },
  { id:'mean_rev', lbl:'Mean Reversion',           desc:'Oversold: down 5%+ over last month',    tag:'REVERSAL', col:'#f43f5e' },
];

let _scrSel      = new Set();
let _scrData     = [];
let _scrSortCol  = 'rs_rating';
let _scrSortAsc  = false;

// ── MY SCREENERS — AFL translations ──────────────────────────────────────────
const MY_SCR_DEFS = [
  { id:'svro',           lbl:'SVRO',                     desc:'Stage2 + Vol Surge + RS>85 + Market Health + Liquidity',  tag:'SVRO',      col:'#f59e0b' },
  { id:'qulla_breakout', lbl:'Qullamaggie Breakout',     desc:'Prior move + consolidation + breakout + vol confirmation', tag:'QULLA BO',   col:'#f97316' },
  { id:'qulla_ep',       lbl:'Qullamaggie Episodic Pivot',desc:'Gap up 10%+ on 3x avg volume, surprise factor, strong close', tag:'QULLA EP', col:'#fb923c' },
  { id:'mean_reversion_q',lbl:'Mean Reversion Quality',  desc:'Composite 0-100: Stage2 + Flush + EMA Reclaim + Pocket Pivot', tag:'MR SCORE', col:'#34d399' },
  { id:'manas_arora',    lbl:'Manas Arora Momentum Burst',desc:'Trend + Momentum + Vol Contraction + EMA Confluence',     tag:'MANAS',     col:'#06b6d4' },
  { id:'vcp_minervini',  lbl:'VCP (Minervini)',           desc:'Trend Template (8/8) + Volatility Contraction + Near Pivot', tag:'VCP',       col:'#a78bfa' },
];

function _scrItemHtml(s, isCustom) {
  const cls = isCustom ? 'scr-item my-scr' : 'scr-item';
  return `<div class="${cls}" id="si-${s.id}" onclick="_toggleScr('${s.id}')">
    <div class="scr-cb"><span class="scr-cb-tick">✓</span></div>
    <div class="scr-item-body">
      <div class="scr-item-lbl">${s.lbl}</div>
      <div class="scr-item-desc">${s.desc}</div>
      <span class="scr-item-tag" style="color:${s.col};border-color:${s.col}44;background:${s.col}15">${s.tag}</span>
    </div>
  </div>`;
}

function initScrList() {
  const list = document.getElementById('scr-list');
  if (!list) return;

  // Built-in screeners section
  const builtinHtml = `
    <div class="scr-section-hdr">⚡ BUILT-IN SCREENERS</div>
    ${SCR_DEFS.map(s => _scrItemHtml(s, false)).join('')}
  `;

  // My screeners section
  const myHtml = `
    <div class="scr-section-hdr my">★ MY SCREENERS (AFL)</div>
    ${MY_SCR_DEFS.map(s => _scrItemHtml(s, true)).join('')}
  `;

  list.innerHTML = builtinHtml + myHtml;
}

function _toggleScr(id) {
  const el = document.getElementById('si-' + id);
  if (_scrSel.has(id)) {
    _scrSel.delete(id);
    el && el.classList.remove('sel');
  } else {
    _scrSel.add(id);
    el && el.classList.add('sel');
  }
  const n = _scrSel.size;
  document.getElementById('scr-sel-count').textContent =
    n === 0 ? '0 screeners selected' : `${n} screener${n > 1 ? 's' : ''} selected`;
  document.getElementById('scr-run-btn').disabled = (n === 0);
}

function _getMinMcap() {
  const el = document.getElementById('scr-mcap-filter');
  return el ? parseFloat(el.value) || 0 : 0;
}

function _mcapBadgeHtml(tier) {
  if (!tier) return '<span style="color:var(--text3);font-size:10px">—</span>';
  const cls = tier.startsWith('Mega') ? 'mcap-mega'
    : tier.startsWith('Large') ? 'mcap-large'
    : tier.startsWith('Mid') ? 'mcap-mid'
    : tier.startsWith('Small') ? 'mcap-small'
    : 'mcap-micro';
  const short = tier.replace(' Cap', '');
  return `<span class="mcap-bdg ${cls}">${short}</span>`;
}

async function runScreeners() {
  if (_scrSel.size === 0) return;
  const ids = [..._scrSel];
  const names = ids.map(id => SCR_DEFS.find(d => d.id === id)?.lbl || id);

  document.getElementById('scr-right-title').textContent =
    ids.length === 1 ? names[0] : 'MULTI-SCREENER INTERSECTION';
  document.getElementById('scr-status-txt').textContent  = 'Running...';
  document.getElementById('scr-match-badge').style.display = 'none';
  document.getElementById('scr-results-body').innerHTML =
    '<div class="scr-loading">⚡ Computing screener results...</div>';

  try {
    const minMcap = _getMinMcap();
    const params = new URLSearchParams({ market: currentMarket, screeners: ids.join(',') });
    if (minMcap > 0) params.set('min_mcap', minMcap);
    const res    = await fetch(`${API}/api/screener/multi?${params}`);
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const data = await res.json();
    if (data.error) throw new Error(data.error);

    _scrData = data.stocks || [];
    document.getElementById('scr-status-txt').textContent =
      `${_scrData.length} stocks · ${data.elapsed || '?'}s · ${data.cached ? 'CACHED' : 'LIVE'}`;
    const badge = document.getElementById('scr-match-badge');
    badge.textContent    = _scrData.length + ' matches';
    badge.style.display  = 'inline-block';
    _renderScrTable();

  } catch(e) {
    document.getElementById('scr-status-txt').textContent = 'Error: ' + e.message;
    document.getElementById('scr-results-body').innerHTML = `
      <div class="scr-empty" style="color:var(--red)">
        ⚠<br>${e.message}<br>
        <span style="color:var(--text3)">Ensure backend is running at ${API}</span>
      </div>`;
  }
}

// ── Screener pagination state ────────────────────────────────────────────────
let _scrPage = 1;
const SCR_PER_PAGE = 15;

// Sector colour map
const SEC_COLORS = {
  'Banking':    ['rgba(59,130,246,.15)','#60a5fa'],
  'IT':         ['rgba(168,85,247,.15)','#a855f7'],
  'Pharma':     ['rgba(34,197,94,.15)', '#22c55e'],
  'Auto':       ['rgba(245,158,11,.15)','#f59e0b'],
  'FMCG':       ['rgba(6,182,212,.15)', '#06b6d4'],
  'Metal':      ['rgba(156,163,175,.15)','#9ca3af'],
  'Energy':     ['rgba(249,115,22,.15)','#f97316'],
  'Infra':      ['rgba(244,63,94,.15)', '#f43f5e'],
  'Realty':     ['rgba(251,146,60,.15)','#fb923c'],
  'Chemicals':  ['rgba(52,211,153,.15)','#34d399'],
  'Textile':    ['rgba(232,121,249,.15)','#e879f9'],
  'Finance':    ['rgba(96,165,250,.15)', '#60a5fa'],
  'Healthcare': ['rgba(34,197,94,.15)', '#22c55e'],
};
function _secColor(sec) {
  if (!sec) return ['rgba(100,116,139,.12)','#64748b'];
  for (const [k,v] of Object.entries(SEC_COLORS)) {
    if (sec.toLowerCase().includes(k.toLowerCase())) return v;
  }
  return ['rgba(100,116,139,.12)','#64748b'];
}

function _renderScrTable() {
  const body  = document.getElementById('scr-results-body');
  const pagEl = document.getElementById('scr-pag');

  // Show Force Refresh buttons after first run
  const _fb = document.getElementById('scr-force-btn');
  const _hb = document.getElementById('scr-hdr-refresh');
  if (_fb) _fb.style.display = 'block';
  if (_hb) _hb.style.display = 'inline-block';

  if (!_scrData.length) {
    body.innerHTML = '<div class="scr-empty">🔍<br>No stocks matched all selected criteria</div>';
    if (pagEl) pagEl.style.display = 'none';
    return;
  }

  const sorted = [..._scrData].sort((a, b) => {
    const av = a[_scrSortCol] ?? 0, bv = b[_scrSortCol] ?? 0;
    return _scrSortAsc ? (av > bv ? 1 : -1) : (av < bv ? 1 : -1);
  });

  // Clamp page to valid range
  const totalPages = Math.ceil(sorted.length / SCR_PER_PAGE);
  if (_scrPage > totalPages) _scrPage = totalPages;
  if (_scrPage < 1) _scrPage = 1;

  const start   = (_scrPage - 1) * SCR_PER_PAGE;
  const end     = Math.min(start + SCR_PER_PAGE, sorted.length);
  const pageRows = sorted.slice(start, end);

  const cols = [
    { k:'rank',          l:'#',         al:'left'  },
    { k:'ticker',        l:'SYMBOL',    al:'left'  },
    { k:'rs_rating',     l:'RS ★',      al:'right' },
    { k:'rs_trend',      l:'TREND',     al:'right' },
    { k:'mcap_tier',     l:'MCAP',      al:'center'},
    { k:'price',         l:'PRICE',     al:'right' },
    { k:'chg_1w',        l:'1W %',      al:'right' },
    { k:'chg_1m',        l:'1M %',      al:'right' },
    { k:'chg_3m',        l:'3M %',      al:'right' },
    { k:'pct_from_high', l:'FROM HIGH', al:'right' },
    { k:'vol_ratio',     l:'VOL',       al:'right' },
    { k:'sector',        l:'SECTOR',    al:'left'  },
    { k:'ad_rating',     l:'A/D',        al:'center'},
  ];

  const f  = (v, d=1) => (v == null || isNaN(v)) ? '—' : Number(v).toFixed(d);
  const gc = v => v >= 0 ? 'var(--green)' : 'var(--red)';

  body.innerHTML = `
    <table class="scr-tbl">
      <thead><tr>
        ${cols.map(c => `
          <th class="scr-th" style="text-align:${c.al}"
              onclick="_sortScr('${c.k}')">
            ${c.l}${_scrSortCol === c.k ? (_scrSortAsc ? ' ↑' : ' ↓') : ''}
          </th>`).join('')}
      </tr></thead>
      <tbody>
        ${pageRows.map(s => {
          const rs = s.rs_rating;
          const [rb, rf] = rs >= 90 ? ['rgba(34,197,94,.2)','#22c55e']
            : rs >= 80 ? ['rgba(59,130,246,.2)','#60a5fa']
            : rs >= 70 ? ['rgba(245,158,11,.2)','#f59e0b']
            : ['rgba(100,116,139,.12)','#94a3b8'];
          const [sb, sf] = _secColor(s.sector);
          return `
            <tr class="scr-tr">
              <td class="scr-td" style="text-align:left;color:var(--text3);font-size:11px">${s.rank}</td>
              <td class="scr-td" style="text-align:left;font-weight:700;font-family:var(--font-mono)"><span class="ticker-link" onclick="openTickerChart('${s.ticker}')">${s.ticker}</span></td>
              <td class="scr-td">
                <span class="rs-bdg" style="background:${rb};color:${rf}">${rs}</span>
              </td>
              <td class="scr-td" style="color:${s.rs_trend==='↑'?'var(--green)':'var(--red)'};font-size:15px">
                ${s.rs_trend || '—'}
              </td>
              <td class="scr-td" style="text-align:center">${_mcapBadgeHtml(s.mcap_tier)}</td>
              <td class="scr-td" style="font-family:var(--font-mono)">₹${s.price?.toLocaleString('en-IN',{maximumFractionDigits:1}) || '—'}</td>
              <td class="scr-td" style="color:${gc(s.chg_1w)}">${s.chg_1w>=0?'+':''}${f(s.chg_1w)}%</td>
              <td class="scr-td" style="color:${gc(s.chg_1m)}">${s.chg_1m>=0?'+':''}${f(s.chg_1m)}%</td>
              <td class="scr-td" style="color:${gc(s.chg_3m)}">${s.chg_3m>=0?'+':''}${f(s.chg_3m)}%</td>
              <td class="scr-td" style="color:${s.pct_from_high>=-5?'var(--green)':s.pct_from_high>=-15?'var(--amber)':'var(--red)'}">
                ${f(s.pct_from_high)}%
              </td>
              <td class="scr-td" style="color:${s.vol_ratio>=1.5?'var(--cyan)':'var(--text)'}">
                ${f(s.vol_ratio, 2)}x
              </td>
              <td class="scr-td" style="text-align:left">
                ${s.sector ? `<span class="sec-tag" style="background:${sb};color:${sf}">${s.sector}</span>` : '<span style="color:var(--text3);font-size:10px">—</span>'}
              </td>
              <td class="scr-td" style="text-align:center">
                ${_adBadge(s.ad_rating)}
              </td>
            </tr>`;
        }).join('')}
      </tbody>
    </table>`;

  // ── Pagination controls ───────────────────────────────────────────────────
  if (pagEl) pagEl.style.display = 'flex';

  const infoEl = document.getElementById('scr-pag-info');
  const btnsEl = document.getElementById('scr-pag-btns');
  if (infoEl) infoEl.textContent =
    `Showing ${start+1}–${end} of ${sorted.length} stocks`;

  if (btnsEl) {
    let btns = '';
    // Prev
    btns += `<button class="scr-pag-btn" onclick="_scrGoPage(${_scrPage-1})"
      ${_scrPage === 1 ? 'disabled' : ''}>‹</button>`;

    // Page numbers — show up to 7 buttons with ellipsis
    const pages = [];
    if (totalPages <= 7) {
      for (let i=1; i<=totalPages; i++) pages.push(i);
    } else {
      pages.push(1);
      if (_scrPage > 3) pages.push('…');
      for (let i=Math.max(2,_scrPage-1); i<=Math.min(totalPages-1,_scrPage+1); i++) pages.push(i);
      if (_scrPage < totalPages-2) pages.push('…');
      pages.push(totalPages);
    }
    pages.forEach(p => {
      if (p === '…') {
        btns += `<span class="scr-pag-sep">…</span>`;
      } else {
        btns += `<button class="scr-pag-btn${p===_scrPage?' active':''}"
          onclick="_scrGoPage(${p})">${p}</button>`;
      }
    });

    // Next
    btns += `<button class="scr-pag-btn" onclick="_scrGoPage(${_scrPage+1})"
      ${_scrPage === totalPages ? 'disabled' : ''}>›</button>`;

    btnsEl.innerHTML = btns;
  }
}

function _scrGoPage(page) {
  const totalPages = Math.ceil(_scrData.length / SCR_PER_PAGE);
  if (page < 1 || page > totalPages) return;
  _scrPage = page;
  _renderScrTable();
  // Scroll table back to top
  const wrap = document.querySelector('.scr-tbl-wrap');
  if (wrap) wrap.scrollTop = 0;
}

function _sortScr(col) {
  if (_scrSortCol === col) _scrSortAsc = !_scrSortAsc;
  else { _scrSortCol = col; _scrSortAsc = false; }
  _scrPage = 1;  // reset to page 1 on sort change
  _renderScrTable();
}


// ─── FORCE REFRESH SCREENER ──────────────────────────────────────────────────
async function forceRefreshScreener() {
  if (_scrSel.size === 0) { alert('Please select at least one screener first.'); return; }

  const footBtn = document.getElementById('scr-force-btn');
  const hdrBtn  = document.getElementById('scr-hdr-refresh');
  const runBtn  = document.getElementById('scr-run-btn');
  const statusEl = document.getElementById('scr-status-txt');
  const bodyEl   = document.getElementById('scr-results-body');
  const titleEl  = document.getElementById('scr-right-title');
  const badgeEl  = document.getElementById('scr-match-badge');

  // Busy state
  if (footBtn) { footBtn.classList.add('busy'); footBtn.textContent = '⟳ Clearing cache...'; footBtn.disabled = true; }
  if (hdrBtn)  { hdrBtn.textContent = '⟳ Refreshing...'; hdrBtn.disabled = true; }
  if (runBtn)  runBtn.disabled = true;

  const ids    = [..._scrSel];
  const market = typeof currentMarket !== 'undefined' ? currentMarket : 'India';

  // Step 1: Clear backend cache
  try {
    await fetch(`${API}/api/screener/clear-cache`, { method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ market, screeners: ids }) });
  } catch(e) { console.warn('Cache clear (non-critical):', e.message); }

  // Step 2: Re-run with refresh=true
  if (titleEl) titleEl.textContent = ids.length === 1
    ? (SCR_DEFS.find(d => d.id === ids[0])?.lbl || 'RESULTS')
    : 'MULTI-SCREENER INTERSECTION';
  if (statusEl) statusEl.textContent = '⟳ Fetching fresh signals from full NSE universe...';
  if (badgeEl)  badgeEl.style.display = 'none';
  if (bodyEl)   bodyEl.innerHTML = '<div class="scr-loading">⟳ Force refreshing — 2,500+ NSE stocks...</div>';

  try {
    const minMcap = _getMinMcap();
    const params = new URLSearchParams({ market, screeners: ids.join(','), refresh: 'true' });
    if (minMcap > 0) params.set('min_mcap', minMcap);
    const res  = await fetch(`${API}/api/screener/multi?${params}`);
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const data = await res.json();
    if (data.error) throw new Error(data.error);

    _scrData = data.stocks || [];
    if (statusEl) statusEl.textContent =
      `${_scrData.length} stocks · ${data.elapsed || '?'}s · ✅ FRESH DATA`;
    if (badgeEl) { badgeEl.textContent = _scrData.length + ' matches'; badgeEl.style.display = 'inline-block'; }
    _renderScrTable();

  } catch(e) {
    if (statusEl) statusEl.textContent = 'Error: ' + e.message;
    if (bodyEl) bodyEl.innerHTML = `<div class="scr-empty" style="color:var(--red)">⚠<br>${e.message}</div>`;
  } finally {
    if (footBtn) { footBtn.classList.remove('busy'); footBtn.textContent = '⟳ FORCE REFRESH (clear cache)'; footBtn.disabled = false; }
    if (hdrBtn)  { hdrBtn.textContent = '⟳ FORCE REFRESH'; hdrBtn.disabled = false; }
    if (runBtn)  runBtn.disabled = (_scrSel.size === 0);
  }
}


// ── A/D Rating Badge helper ───────────────────────────────────────────────────
function _adBadge(grade) {
  if (!grade || grade === 'N/A') {
    return '<span class="ad-badge ad-NA">N/A</span>';
  }
  const cls = {
    'A+': 'ad-Aplus',  'A': 'ad-A',    'A-': 'ad-Aminus',
    'B+': 'ad-Bplus',  'B': 'ad-B',
    'C+': 'ad-Cplus',  'C': 'ad-C',    'C-': 'ad-Cminus',
    'D+': 'ad-Dplus',  'D': 'ad-D',    'E':  'ad-E',
  }[grade] || 'ad-NA';
  return `<span class="ad-badge ${cls}">${grade}</span>`;
}


// ══════════════════════════════════════════════════════════
// SCANNER TAB ENGINE
// ══════════════════════════════════════════════════════════
const QUICK_SCANS = {
  '52w_high':        {label:'52-Week High',       filter:s=>(s.pct_from_high||0)>=-1},
  '52w_low':         {label:'52-Week Low',        filter:s=>(s.pct_from_low||0)<=5},
  'near_52w_high':   {label:'Near 52W High',      filter:s=>(s.pct_from_high||0)>=-5},
  'ath':             {label:'All-Time High',       filter:s=>(s.pct_from_high||0)>=-0.5},
  'up2pct':          {label:'Up > 2%',            filter:s=>(s.chg_1w||0)>2},
  'down2pct':        {label:'Down > 2%',          filter:s=>(s.chg_1w||0)<-2},
  'vol3x':           {label:'Volume > 3x',        filter:s=>(s.vol_ratio||0)>=3},
  'vol5x':           {label:'Volume > 5x',        filter:s=>(s.vol_ratio||0)>=5},
  'vol_dry':         {label:'Volume Dry-Up',       filter:s=>(s.vol_ratio||0)>0&&(s.vol_ratio||0)<0.5},
  'rs90':            {label:'RS Rating 90+',       filter:s=>(s.rs_rating||0)>=90},
  'rs80':            {label:'RS Rating 80+',       filter:s=>(s.rs_rating||0)>=80},
  'stronger_nifty':  {label:'Stronger Than Nifty', filter:s=>s.rs_trend==='↑'&&(s.rs_rating||0)>=70},
  'breakout_king':   {label:'Breakout King',       filter:s=>(s.pct_from_high||0)>=-8&&(s.vol_ratio||0)>=1.5&&(s.rs_rating||0)>=80},
  'ad_a':            {label:'A/D = A or A+',      filter:s=>['A+','A'].includes(s.ad_rating)},
  'ad_b':            {label:'A/D = B+ or better', filter:s=>['A+','A','A-','B+'].includes(s.ad_rating)},
  'svro':            {label:'SVRO',               backend:'svro'},
  'vcp_minervini':   {label:'VCP (Minervini)',     backend:'vcp_minervini'},
  'manas_arora':     {label:'Manas Arora',         backend:'manas_arora'},
  'qulla_breakout':  {label:'Qullamaggie BO',      backend:'qulla_breakout'},
  'mean_reversion_q':{label:'Mean Reversion',      backend:'mean_reversion_q'},
};
let _scannerData=null;

async function loadScannerData(){
  if(_scannerData) return _scannerData;
  try{
    const minMcap = _getMinMcap();
    const params = new URLSearchParams({market: currentMarket, min_rs: '1'});
    if (minMcap > 0) params.set('min_mcap', minMcap);
    const res=await fetch(`${API}/api/screener/rs?${params}`);
    if(!res.ok) throw new Error('HTTP '+res.status);
    const d=await res.json();
    _scannerData=d.stocks||[]; return _scannerData;
  }catch(e){console.warn('Scanner load failed:',e.message);return[];}
}

function updateScannerMarketBar(){
  const now=new Date();
  const ist=new Date(now.toLocaleString('en-US',{timeZone:'Asia/Kolkata'}));
  const hh=ist.getHours(),mm=ist.getMinutes(),day=ist.getDay();
  const isOpen=day>=1&&day<=5&&((hh===9&&mm>=15)||(hh>=10&&hh<15)||(hh===15&&mm<=30));
  const dot=document.getElementById('scn-mkt-dot');
  const lbl=document.getElementById('scn-mkt-status');
  const tim=document.getElementById('scn-mkt-time');
  if(dot) dot.className='scn-mkt-dot '+(isOpen?'open':'closed');
  if(lbl){lbl.textContent=isOpen?'Market Open':'Market Closed';lbl.style.color=isOpen?'var(--green)':'var(--red)';}
  if(tim) tim.textContent=ist.toLocaleTimeString('en-IN',{hour:'2-digit',minute:'2-digit',second:'2-digit'})+' IST';
  const dbEl=document.getElementById('scn-db-info');
  if(dbEl&&currentData[currentMarket]) dbEl.textContent=(currentData[currentMarket].universe_size||'—')+' stocks';
}

async function loadTopMovers(){
  const stocks=await loadScannerData(); if(!stocks.length) return;
  const f=(v,d=1)=>v==null?'—':Number(v).toFixed(d);
  const row=(s,val,col)=>`<div class="scn-mover-row"><span class="scn-mover-ticker ticker-link" onclick="openTickerChart('${s.ticker}')">${s.ticker}</span><span class="scn-mover-val" style="color:${col}">${val}</span></div>`;
  const gainers=[...stocks].filter(s=>s.chg_1w>0).sort((a,b)=>b.chg_1w-a.chg_1w).slice(0,5);
  const losers=[...stocks].filter(s=>s.chg_1w<0).sort((a,b)=>a.chg_1w-b.chg_1w).slice(0,5);
  const vols=[...stocks].filter(s=>s.vol_ratio>1).sort((a,b)=>b.vol_ratio-a.vol_ratio).slice(0,5);
  const empty='<div class="scn-mover-empty">No data</div>';
  const gEl=document.getElementById('scn-gainers');
  const lEl=document.getElementById('scn-losers');
  const vEl=document.getElementById('scn-volspikes');
  if(gEl) gEl.innerHTML=gainers.length?gainers.map(s=>row(s,`+${f(s.chg_1w)}%`,'var(--green)')).join(''):empty;
  if(lEl) lEl.innerHTML=losers.length?losers.map(s=>row(s,`${f(s.chg_1w)}%`,'var(--red)')).join(''):empty;
  if(vEl) vEl.innerHTML=vols.length?vols.map(s=>row(s,`${f(s.vol_ratio,2)}x`,'var(--cyan)')).join(''):empty;
}

async function runQuickScan(scanId){
  const def=QUICK_SCANS[scanId]; if(!def) return;
  document.querySelectorAll('.scn-sidebar-item').forEach(el=>el.classList.remove('active'));
  const sEl=document.getElementById('qs-'+scanId); if(sEl) sEl.classList.add('active');
  const panel=document.getElementById('scn-qr-panel');
  const title=document.getElementById('scn-qr-title');
  const badge=document.getElementById('scn-qr-badge');
  const stat=document.getElementById('scn-qr-stat');
  const body=document.getElementById('scn-qr-body');
  panel.style.display='block';
  title.textContent=def.label;
  badge.style.display='none';
  stat.textContent='';
  body.innerHTML='<div class="scn-mover-empty"><span class="scn-loading-spinner"></span> Running scan...</div>';
  panel.scrollIntoView({behavior:'smooth',block:'nearest'});
  let results=[]; const t0=Date.now();
  if(def.backend){
    try{
      const p=new URLSearchParams({market:currentMarket,screeners:def.backend});
      const res=await fetch(`${API}/api/screener/multi?${p}`);
      if(!res.ok) throw new Error('HTTP '+res.status);
      const d=await res.json();
      if(d.error) throw new Error(d.error);
      results=d.stocks||[];
    }catch(e){body.innerHTML=`<div class="scn-mover-empty" style="color:var(--red)">⚠ ${e.message}</div>`;return;}
  } else {
    const stocks=await loadScannerData();
    results=stocks.filter(def.filter).sort((a,b)=>b.rs_rating-a.rs_rating);
  }
  const elapsed=((Date.now()-t0)/1000).toFixed(1);
  stat.textContent=`${results.length} stocks · ${elapsed}s`;
  badge.textContent=results.length+' matches'; badge.style.display='inline-block';
  if(!results.length){body.innerHTML='<div class="scn-mover-empty">🔍 No stocks matched</div>';return;}
  const gc=v=>v>=0?'var(--green)':'var(--red)';
  const f=(v,d=1)=>v==null?'—':Number(v).toFixed(d);
  body.innerHTML=`<table class="scn-results-tbl"><thead><tr>
    <th class="scn-results-th">#</th><th class="scn-results-th">SYMBOL</th>
    <th class="scn-results-th">RS ★</th><th class="scn-results-th">A/D</th>
    <th class="scn-results-th">PRICE</th><th class="scn-results-th">1W%</th>
    <th class="scn-results-th">3M%</th><th class="scn-results-th">FROM HIGH</th>
    <th class="scn-results-th">VOL</th><th class="scn-results-th">SECTOR</th>
  </tr></thead><tbody>${results.slice(0,50).map((s,i)=>{
    const rs=s.rs_rating;
    const[rb,rf]=rs>=90?['rgba(34,197,94,.2)','#22c55e']:rs>=80?['rgba(59,130,246,.2)','#60a5fa']:rs>=70?['rgba(245,158,11,.2)','#f59e0b']:['rgba(100,116,139,.12)','#94a3b8'];
    const[sb,sf]=_secColor(s.sector);
    return`<tr class="scn-results-tr">
      <td class="scn-results-td" style="color:var(--text3)">${i+1}</td>
      <td class="scn-results-td" style="font-weight:700;font-family:var(--font-mono)"><span class="ticker-link" onclick="openTickerChart('${s.ticker}')">${s.ticker}</span></td>
      <td class="scn-results-td"><span class="rs-bdg" style="background:${rb};color:${rf}">${rs}</span></td>
      <td class="scn-results-td">${_adBadge(s.ad_rating)}</td>
      <td class="scn-results-td" style="font-family:var(--font-mono)">₹${s.price?.toLocaleString('en-IN',{maximumFractionDigits:1})||'—'}</td>
      <td class="scn-results-td" style="color:${gc(s.chg_1w)}">${s.chg_1w>=0?'+':''}${f(s.chg_1w)}%</td>
      <td class="scn-results-td" style="color:${gc(s.chg_3m)}">${s.chg_3m>=0?'+':''}${f(s.chg_3m)}%</td>
      <td class="scn-results-td" style="color:${s.pct_from_high>=-5?'var(--green)':s.pct_from_high>=-15?'var(--amber)':'var(--red)'}">${f(s.pct_from_high)}%</td>
      <td class="scn-results-td" style="color:${s.vol_ratio>=1.5?'var(--cyan)':'var(--text)'}">${f(s.vol_ratio,2)}x</td>
      <td class="scn-results-td">${s.sector?`<span class="sec-tag" style="background:${sb};color:${sf}">${s.sector}</span>`:'—'}</td>
    </tr>`;
  }).join('')}</tbody></table>${results.length>50?`<div style="padding:8px 14px;font-size:10px;color:var(--text3)">Showing top 50 of ${results.length}</div>`:''}`;
}

function closeQuickResults(){
  document.getElementById('scn-qr-panel').style.display='none';
  document.querySelectorAll('.scn-sidebar-item').forEach(el=>el.classList.remove('active'));
}

function filterScanSidebar(q){
  const ql=q.toLowerCase();
  document.querySelectorAll('.scn-sidebar-item').forEach(el=>{el.style.display=el.textContent.toLowerCase().includes(ql)?'':'none';});
  document.querySelectorAll('.scn-sidebar-section').forEach(el=>{el.style.display=q?'none':'';});
}

async function initScannerTab(){
  updateScannerMarketBar();
  setInterval(updateScannerMarketBar,1000);
  await loadTopMovers();
}
