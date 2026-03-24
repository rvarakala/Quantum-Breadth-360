// ════════════════════════════════════════════════════════════════════════════
// LEADERS TAB ENGINE
// ════════════════════════════════════════════════════════════════════════════
let _leadersData = null;
let _activeSectorF = null;

function _adClass(g) {
  if(!g||g==='N/A') return 'd';
  if(g.startsWith('A')) return 'a';
  if(g.startsWith('B')) return 'b';
  if(g.startsWith('C')) return 'c';
  return 'd';
}
function _rsClass(rs) { return rs>=90?'hi':rs>=80?'md':'lo'; }
function _sectorDotColor(health) {
  return health==='hot'?'#22c55e':health==='warm'?'#f59e0b':'#ef4444';
}

function _renderRegimeBanner(regime) {
  const bar=document.getElementById('ldr-regime-bar');
  const name=document.getElementById('ldr-regime-name');
  const msg=document.getElementById('ldr-regime-msg');
  const stats=document.getElementById('ldr-regime-stats');
  if(!bar) return;
  const r=regime.regime;
  bar.className='ldr-regime '+(r==='BULLISH'?'bull':r==='OVERSOLD'?'os':'neut');
  name.textContent='Q-BRAM: '+r;
  const msgs={
    'BULLISH': 'Momentum breakouts favoured — all tiers active',
    'NEUTRAL': 'Selective conditions — Elite tier only, highest conviction',
    'OVERSOLD':'Avoid breakouts — mean reversion candidates shown',
  };
  msg.textContent=msgs[r]||'';
  stats.innerHTML=`
    <span class="ldr-regime-stat"><span>B50</span>${regime.b50}%</span>
    <span class="ldr-regime-stat"><span>B200</span>${regime.b200}%</span>
    <span class="ldr-regime-stat"><span>NH-NL</span>${regime.nh_nl>0?'+':''}${regime.nh_nl}</span>
    <span class="ldr-regime-stat"><span>AD</span>${regime.ad_trend}</span>`;
}

function _renderSectorChips(sectorHealth) {
  const wrap=document.getElementById('ldr-sector-chips');
  if(!wrap) return;
  const sectors=Object.entries(sectorHealth).slice(0,16);
  const allActive = !_activeSectorF;
  wrap.innerHTML=`
    <div class="ldr-chip ${allActive?'active':''}"
         style="background:var(--bg2);color:var(--text2);border-color:var(--border2)"
         onclick="filterLeadersBySector(null)">All sectors</div>
    ${sectors.map(([sec,d])=>`
      <div class="ldr-chip ${d.health} ${_activeSectorF===sec?'active':''}"
           onclick="filterLeadersBySector('${sec}')">
        <div class="ldr-chip-dot"></div>
        ${sec}
        <span class="ldr-chip-rs">${d.avg_rs}</span>
        <span class="ldr-chip-trend">${d.trend}</span>
      </div>`).join('')}`;
}

function _ldrMcapBadge(tier) {
  if (!tier) return '';
  const cls = tier.startsWith('Mega') ? 'mcap-mega'
    : tier.startsWith('Large') ? 'mcap-large'
    : tier.startsWith('Mid') ? 'mcap-mid'
    : tier.startsWith('Small') ? 'mcap-small'
    : 'mcap-micro';
  const short = tier.replace(' Cap', '');
  return `<span class="mcap-bdg ${cls}">${short}</span>`;
}

function _stockRow(s, tierClass, idx) {
  const rs=s.rs_rating||0, ad=s.ad_rating||'N/A', sc=s.leader_score||0;
  const sec=s.sector||'';
  const sh=_leadersData?.sector_health?.[sec];
  const dotCol=sh?_sectorDotColor(sh.health):'var(--text3)';
  const scoreCol=tierClass==='elite'?'var(--green)':tierClass==='emerging'?'#60a5fa':tierClass==='pressure'?'var(--red)':'#a78bfa';
  return `<div class="ldr-stock-row">
    <span class="ldr-rank">${idx+1}</span>
    <span class="ldr-ticker ticker-link" onclick="event.stopPropagation();openTickerChart('${s.ticker}')">${s.ticker}</span>
    <span class="ldr-rs ${_rsClass(rs)}">${rs}</span>
    <span class="ldr-ad ${_adClass(ad)}">${ad}</span>
    ${_ldrMcapBadge(s.mcap_tier)}
    <div class="ldr-score-wrap">
      <div class="ldr-score-bar"><div class="ldr-score-fill ${tierClass}" style="width:${sc}%"></div></div>
      <span class="ldr-score-num" style="color:${scoreCol}">${sc}</span>
    </div>
    <div class="ldr-sec-dot" style="background:${dotCol}"></div>
    <span class="ldr-sec-name">${sec}</span>
  </div>`;
}

function _tierCard(stocks, tierClass, title, subtitle, sf, dimCls='', hlCls='') {
  const filtered = sf ? stocks.filter(s=>s.sector===sf) : stocks;
  const visible  = filtered.slice(0, 5);
  const extraCls = [dimCls, hlCls].filter(Boolean).join(' ');
  // Store full data for full-view access
  window._ldrTierData = window._ldrTierData || {};
  window._ldrTierData[tierClass] = { stocks: filtered, title, tierClass };
  return `<div class="ldr-tier ${extraCls}">
    <div class="ldr-tier-hdr ${tierClass}">
      <div>
        <div class="ldr-tier-name">${title}</div>
        <div class="ldr-tier-sub">${subtitle}</div>
      </div>
      <span class="ldr-tier-count">${filtered.length}</span>
    </div>
    ${filtered.length===0
      ? `<div class="ldr-empty">No stocks${sf?' in '+sf:''}</div>`
      : visible.map((s,i)=>_stockRow(s,tierClass,i)).join('')
    }
    ${filtered.length>5
      ? `<div class="ldr-view-all" onclick="openLdrFullView('${tierClass}')">
           View all ${filtered.length} →
         </div>`
      : ''
    }
  </div>`;
}

function _renderMRLayout(mrCands, sf) {
  const filtered=sf?mrCands.filter(s=>s.sector===sf):mrCands;
  const visible=filtered.slice(0,15);
  const hidden=filtered.slice(15);
  return `<div class="ldr-mr-wrap">
    <div class="ldr-tier-hdr mr" style="padding:10px 14px">
      <div>
        <div class="ldr-tier-name">MEAN REVERSION CANDIDATES</div>
        <div class="ldr-tier-sub">Dip buys + washout bounces — regime OVERSOLD</div>
      </div>
      <span class="ldr-tier-count">${filtered.length}</span>
    </div>
    <div style="display:grid;grid-template-columns:1fr 1fr 1fr">
      ${filtered.length===0
        ?`<div class="ldr-empty" style="grid-column:1/-1">No MR candidates found</div>`
        :visible.map((s,i)=>_stockRow(s,'mr',i)).join('')}
    </div>
    ${hidden.length>0?`
      <div id="ldr-hidden-mr" style="display:none;grid-template-columns:1fr 1fr 1fr">
        ${hidden.map((s,i)=>_stockRow(s,'mr',15+i)).join('')}
      </div>
      <div class="ldr-view-all" onclick="toggleLdrExpand('mr',this)">View all ${filtered.length} →</div>
    `:''}
  </div>`;
}

function toggleLdrExpand(tierClass, btn) {
  const el=document.getElementById('ldr-hidden-'+tierClass);
  if(!el) return;
  const isHidden=el.style.display==='none'||el.style.display==='';
  if(tierClass==='mr') el.style.display=isHidden?'grid':'none';
  else el.style.display=isHidden?'block':'none';
  btn.textContent=isHidden?'▲ Show less':`View all →`;
}

function filterLeadersBySector(sec) {
  _activeSectorF=sec;
  if(_leadersData) _renderLeadersContent(_leadersData);
}

function _renderLeadersContent(data) {
  _renderRegimeBanner(data.regime);
  _renderSectorChips(data.sector_health);
  const tiers=data.tiers, r=data.regime.regime, sf=_activeSectorF;
  const wrap=document.getElementById('ldr-tiers-wrap');
  if(!wrap) return;

  // ── 4 tiers ALWAYS visible — no dimming ─────────────────────────────────
  const isBull = r==='BULLISH', isOS=r==='OVERSOLD', isNeut=r==='NEUTRAL';

  const eliteDim = '';
  const emergDim = '';
  const pressDim = '';
  const mrDim    = '';
  const mrHL     = isOS ? 'regime-highlight' : '';

  const eliteSub = isBull ? 'RS 90+ · A/D A/A+ · Hot sector'
                 : isNeut ? 'RS 90+ · A/D A or A+ · strict'
                 :          'Regime OVERSOLD — avoid breakouts';
  const emergSub = isBull ? 'RS 80–89 · A/D B+ · Hot/Warm'
                 : isNeut ? 'Caution — regime NEUTRAL'
                 :          'Regime OVERSOLD — avoid breakouts';
  const pressSub = isBull ? 'RS 80+ but A/D C or Cold sector'
                 : isNeut ? 'Caution — regime NEUTRAL'
                 :          'Regime OVERSOLD — avoid breakouts';
  const mrSub    = isOS   ? '★ Q-BRAM Recommended — dip buys + washouts'
                 : isNeut ? 'Monitor — not yet OVERSOLD'
                 :          'Not regime-favoured — standby';

  wrap.innerHTML = `<div class="ldr-tiers">
    ${_tierCard(tiers.elite    ||[],'elite',   'ELITE LEADERS',    eliteSub, sf, eliteDim)}
    ${_tierCard(tiers.emerging ||[],'emerging','EMERGING LEADERS', emergSub, sf, emergDim)}
    ${_tierCard(tiers.pressure ||[],'pressure','UNDER PRESSURE',   pressSub, sf, pressDim)}
    ${_tierCard(tiers.mr_cands ||[],'mr',      'MEAN REVERSION',   mrSub,    sf, mrDim, mrHL)}
  </div>`;
}

function _ldrGetMinMcap() {
  const el = document.getElementById('ldr-mcap-filter');
  return el ? parseFloat(el.value) || 0 : 0;
}

async function loadLeaders(forceRefresh=false) {
  const loading=document.getElementById('ldr-loading');
  const content=document.getElementById('ldr-content');
  if(loading) loading.style.display='block';
  if(content) content.style.display='none';
  try {
    const minMcap = _ldrGetMinMcap();
    const p=new URLSearchParams({market:currentMarket,refresh:forceRefresh?'true':'false'});
    if (minMcap > 0) p.set('min_mcap', minMcap);
    const res=await fetch(`${API}/api/leaders?${p}`);
    if(!res.ok) throw new Error('HTTP '+res.status);
    const data=await res.json();
    if(data.error) throw new Error(data.error);
    _leadersData=data;
    if(loading) loading.style.display='none';
    if(content) content.style.display='block';
    _renderLeadersContent(data);
  } catch(e) {
    if(loading) loading.innerHTML=`
      <div style="color:var(--red);font-size:12px">⚠ ${e.message}</div>
      <button onclick="loadLeaders()" style="margin-top:10px;padding:6px 14px;cursor:pointer">Retry</button>`;
  }
}

async function initLeadersTab() {
  _activeSectorF=null;
  await loadLeaders(false);
}


// ════════════════════════════════════════════════════════════════════════════
// LEADERS FULL VIEW — Paginated sortable table
// ════════════════════════════════════════════════════════════════════════════
const LDR_PAGE_SIZE = 20;
let _ldrFvState = { stocks:[], page:1, sortKey:'leader_score', sortDir:-1, tierClass:'elite', title:'' };

const LDR_FV_COLS = [
  { key:'_idx',          label:'#',         w:'44px',  align:'center',
    fmt:(v,s,i)=>`<span style="color:var(--text3)">${i+1}</span>` },
  { key:'ticker',        label:'SYMBOL',    w:'100px', align:'left',
    fmt:(v,s)=>`<span class="ticker-link" style="font-family:var(--font-mono);font-weight:700;font-size:12px" onclick="openTickerChart('${s.ticker}')">${s.ticker}</span>` },
  { key:'rs_rating',     label:'RS ★',      w:'68px',  align:'center',
    fmt:(v)=>{ const c=v>=90?'rgba(34,197,94,.2)':v>=80?'rgba(59,130,246,.2)':'rgba(245,158,11,.2)';
               const t=v>=90?'#22c55e':v>=80?'#60a5fa':'#f59e0b';
               return `<span class="rs-bdg" style="background:${c};color:${t}">${v}</span>`; } },
  { key:'rs_trend',      label:'TREND',     w:'56px',  align:'center',
    fmt:(v)=>{ const c=v==='↑'?'var(--green)':'var(--red)'; return `<span style="color:${c};font-size:14px">${v||'—'}</span>`; } },
  { key:'ad_rating',     label:'A/D',       w:'56px',  align:'center',
    fmt:(v)=>_adBadge(v) },
  { key:'price',         label:'PRICE',     w:'90px',  align:'right',
    fmt:(v)=>v?`₹${Number(v).toLocaleString('en-IN',{maximumFractionDigits:1})}`:'—' },
  { key:'chg_1w',        label:'1W %',      w:'68px',  align:'right',
    fmt:(v)=>{ const n=Number(v||0); return `<span style="color:${n>=0?'var(--green)':'var(--red)'}">${n>=0?'+':''}${n.toFixed(1)}%</span>`; } },
  { key:'chg_1m',        label:'1M %',      w:'68px',  align:'right',
    fmt:(v)=>{ const n=Number(v||0); return `<span style="color:${n>=0?'var(--green)':'var(--red)'}">${n>=0?'+':''}${n.toFixed(1)}%</span>`; } },
  { key:'chg_3m',        label:'3M %',      w:'68px',  align:'right',
    fmt:(v)=>{ const n=Number(v||0); return `<span style="color:${n>=0?'var(--green)':'var(--red)'}">${n>=0?'+':''}${n.toFixed(1)}%</span>`; } },
  { key:'pct_from_high', label:'FROM HIGH', w:'90px',  align:'right',
    fmt:(v)=>{ const n=Number(v||0); const c=n>=-5?'var(--green)':n>=-15?'var(--amber)':'var(--red)'; return `<span style="color:${c}">${n.toFixed(1)}%</span>`; } },
  { key:'vol_ratio',     label:'VOL',       w:'66px',  align:'right',
    fmt:(v)=>{ const n=Number(v||0); return `<span style="color:${n>=1.5?'var(--cyan)':'var(--text)'}">${n.toFixed(2)}x</span>`; } },
  { key:'mcap_tier',     label:'MCAP',      w:'68px',  align:'center',
    fmt:(v)=>_ldrMcapBadge(v) || '—' },
  { key:'sector',        label:'SECTOR',    w:'130px', align:'left',
    fmt:(v)=>{ const[b,f]=_secColor(v);
               return v?`<span class="sec-tag" style="background:${b};color:${f}">${v}</span>`:'—'; } },
  { key:'leader_score',  label:'SCORE',     w:'68px',  align:'center',
    fmt:(v,s,i,tc)=>{ const c=tc==='elite'?'var(--green)':tc==='emerging'?'#60a5fa':tc==='pressure'?'var(--red)':'#a78bfa';
                       return `<span style="font-family:var(--font-mono);font-weight:700;color:${c}">${Number(v).toFixed(1)}</span>`; } },
];

function openLdrFullView(tierClass) {
  const td = (window._ldrTierData||{})[tierClass];
  if(!td) return;
  _ldrFvState = { stocks:[...td.stocks], page:1, sortKey:'leader_score', sortDir:-1, tierClass, title:td.title };
  _sortLdrFv();
  _renderLdrFvHead();
  _renderLdrFvTable();
  _renderLdrFvFoot();
  document.getElementById('ldr-fullview').style.display = 'flex';
  document.body.style.overflow = 'hidden';
}

function closeLdrFullView() {
  document.getElementById('ldr-fullview').style.display = 'none';
  document.body.style.overflow = '';
}

function _sortLdrFv() {
  const { sortKey, sortDir } = _ldrFvState;
  _ldrFvState.stocks.sort((a,b) => {
    const av = a[sortKey] ?? 0, bv = b[sortKey] ?? 0;
    if(typeof av==='string') return sortDir * av.localeCompare(bv);
    return sortDir * (av - bv);
  });
}

function _sortLdrFvBy(key) {
  _ldrFvState.sortDir = _ldrFvState.sortKey===key ? -_ldrFvState.sortDir : -1;
  _ldrFvState.sortKey = key;
  _ldrFvState.page = 1;
  _sortLdrFv();
  _renderLdrFvTable();
  _renderLdrFvFoot();
  document.querySelectorAll('.ldr-fv-th').forEach(th => {
    const isActive = th.dataset.key===key;
    th.classList.toggle('sorted', isActive);
    const arrow = th.querySelector('.sort-arrow');
    if(arrow) arrow.textContent = isActive ? (_ldrFvState.sortDir===-1?'↓':'↑') : '↕';
  });
}

function _renderLdrFvHead() {
  const thead = document.getElementById('ldr-fv-thead');
  const title = document.getElementById('ldr-fv-title');
  const count = document.getElementById('ldr-fv-count');
  const { stocks, sortKey, sortDir, tierClass, title:t } = _ldrFvState;
  const col = tierClass==='elite'?'var(--green)':tierClass==='emerging'?'#60a5fa':tierClass==='pressure'?'var(--red)':'#a78bfa';
  title.textContent = t; title.style.color = col;
  count.textContent = `${stocks.length} stocks`;
  thead.innerHTML = `<tr>${LDR_FV_COLS.map(c=>`
    <th class="ldr-fv-th${c.key===sortKey?' sorted':''}" data-key="${c.key}"
        style="width:${c.w};min-width:${c.w};text-align:${c.align}"
        onclick="_sortLdrFvBy('${c.key}')">
      ${c.label} <span class="sort-arrow">${c.key===sortKey?(sortDir===-1?'↓':'↑'):'↕'}</span>
    </th>`).join('')}</tr>`;
}

function _renderLdrFvTable() {
  const tbody = document.getElementById('ldr-fv-tbody');
  if(!tbody) return;
  const { stocks, page, tierClass } = _ldrFvState;
  const start = (page-1)*LDR_PAGE_SIZE;
  const rows  = stocks.slice(start, start+LDR_PAGE_SIZE);
  if(!rows.length) {
    tbody.innerHTML=`<tr><td colspan="${LDR_FV_COLS.length}" style="padding:40px;text-align:center;color:var(--text3)">No stocks</td></tr>`;
    return;
  }
  tbody.innerHTML = rows.map((s,i)=>`
    <tr class="ldr-fv-tr">
      ${LDR_FV_COLS.map(c=>{
        const v = c.key==='_idx' ? null : s[c.key];
        const cell = c.fmt ? c.fmt(v,s,start+i,tierClass) : (v??'—');
        return `<td class="ldr-fv-td" style="text-align:${c.align}">${cell}</td>`;
      }).join('')}
    </tr>`).join('');
}

function _renderLdrFvFoot() {
  const footer = document.getElementById('ldr-fv-footer');
  if(!footer) return;
  const { stocks, page } = _ldrFvState;
  const total = Math.ceil(stocks.length/LDR_PAGE_SIZE);
  if(total<=1){footer.innerHTML='';return;}
  const s=(page-1)*LDR_PAGE_SIZE+1, e=Math.min(page*LDR_PAGE_SIZE,stocks.length);
  let pages=[];
  if(total<=7) pages=Array.from({length:total},(_,i)=>i+1);
  else {
    pages=[1];
    if(page>3) pages.push('...');
    for(let p=Math.max(2,page-1);p<=Math.min(total-1,page+1);p++) pages.push(p);
    if(page<total-2) pages.push('...');
    pages.push(total);
  }
  footer.innerHTML=`
    <button class="ldr-fv-page-btn" onclick="_ldrFvPage(${page-1})" ${page===1?'disabled':''}>← Prev</button>
    ${pages.map(p=>p==='...'
      ?`<span style="color:var(--text3);padding:0 4px">…</span>`
      :`<button class="ldr-fv-page-btn${p===page?' active':''}" onclick="_ldrFvPage(${p})">${p}</button>`
    ).join('')}
    <button class="ldr-fv-page-btn" onclick="_ldrFvPage(${page+1})" ${page===total?'disabled':''}>Next →</button>
    <span class="ldr-fv-page-info">${s}–${e} of ${stocks.length}</span>`;
}

function _ldrFvPage(p) {
  const total=Math.ceil(_ldrFvState.stocks.length/LDR_PAGE_SIZE);
  if(p<1||p>total) return;
  _ldrFvState.page=p;
  _renderLdrFvTable();
  _renderLdrFvFoot();
  document.querySelector('.ldr-fv-body').scrollTop=0;
}

document.addEventListener('keydown', e=>{
  if(e.key==='Escape' && document.getElementById('ldr-fullview')?.style.display==='flex')
    closeLdrFullView();
});

// ════════════════════════════════════════════════════════════════════════════
// LEADERS EXPORT FUNCTIONS
// ════════════════════════════════════════════════════════════════════════════
function _ldrExportFilename(ext) {
  const d = new Date().toISOString().slice(0,10);
  const t = _ldrFvState.title.replace(/\s+/g,'_');
  return `${t}_leaders_${d}.${ext}`;
}

function _ldrAllRows() {
  // Return ALL stocks for export (not just current page)
  return _ldrFvState.stocks.map((s,i) => ({
    '#': i+1,
    'Symbol': s.ticker||'',
    'RS Rating': s.rs_rating||0,
    'Trend': s.rs_trend||'',
    'A/D': s.ad_rating||'N/A',
    'Price': s.price ? Number(s.price).toFixed(1) : '',
    '1W%': s.chg_1w ? Number(s.chg_1w).toFixed(1) : '0.0',
    '1M%': s.chg_1m ? Number(s.chg_1m).toFixed(1) : '0.0',
    '3M%': s.chg_3m ? Number(s.chg_3m).toFixed(1) : '0.0',
    'From High': s.pct_from_high ? Number(s.pct_from_high).toFixed(1) : '0.0',
    'Vol Ratio': s.vol_ratio ? Number(s.vol_ratio).toFixed(2) : '0.00',
    'Sector': s.sector||'',
    'Score': s.leader_score ? Number(s.leader_score).toFixed(1) : '0.0',
  }));
}

function exportLdrPNG() {
  if(typeof html2canvas==='undefined'){ alert('html2canvas not loaded'); return; }
  const modal = document.querySelector('.ldr-fv-modal');
  if(!modal) return;
  html2canvas(modal, { backgroundColor:'#0a0f1e', scale:2 }).then(canvas => {
    const a = document.createElement('a');
    a.download = _ldrExportFilename('png');
    a.href = canvas.toDataURL('image/png');
    a.click();
  });
}

function exportLdrExcel() {
  if(typeof XLSX==='undefined'){ alert('SheetJS (XLSX) not loaded'); return; }
  const rows = _ldrAllRows();
  const ws = XLSX.utils.json_to_sheet(rows);
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(wb, ws, _ldrFvState.title||'Leaders');
  XLSX.writeFile(wb, _ldrExportFilename('xlsx'));
}

function exportLdrPDF() {
  if(typeof jspdf==='undefined' && typeof jsPDF==='undefined'){ alert('jsPDF not loaded'); return; }
  const { jsPDF: JsPDF } = typeof jspdf!=='undefined' ? jspdf : window;
  const doc = new JsPDF({ orientation:'landscape', unit:'mm', format:'a4' });
  const rows = _ldrAllRows();
  const d = new Date().toISOString().slice(0,10);
  doc.setFontSize(14);
  doc.setTextColor(255,255,255);
  doc.setFillColor(10,15,30);
  doc.rect(0,0,297,210,'F');
  doc.text(`${_ldrFvState.title} — ${d}`, 14, 16);
  const cols = ['#','Symbol','RS Rating','Trend','A/D','Price','1W%','1M%','3M%','From High','Vol Ratio','Sector','Score'];
  const body = rows.map(r => cols.map(c => String(r[c]??'')));
  doc.autoTable({
    head: [cols],
    body: body,
    startY: 22,
    theme: 'grid',
    styles: { fontSize:7, cellPadding:2, textColor:[232,237,245], fillColor:[17,24,39], lineColor:[30,45,74], lineWidth:0.2 },
    headStyles: { fillColor:[12,18,33], textColor:[148,163,184], fontStyle:'bold', fontSize:7 },
    alternateRowStyles: { fillColor:[8,12,24] },
    margin: { left:8, right:8 },
  });
  doc.save(_ldrExportFilename('pdf'));
}
