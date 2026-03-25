// ══════════════════════════════════════════════════════════════════════════
// IMPORTER ENGINE — IndexedDB + CSV Parser + Local Breadth Compute
// ══════════════════════════════════════════════════════════════════════════

let _impData = {};
const IDB_NAME = 'BreadthEngineDB', IDB_VER = 4;

function _openIDB() {
  return new Promise((res, rej) => {
    const r = indexedDB.open(IDB_NAME, IDB_VER);
    r.onupgradeneeded = e => {
      const db = e.target.result;
      if (!db.objectStoreNames.contains('ohlcv')) {
        const s = db.createObjectStore('ohlcv', { keyPath: ['ticker','date'] });
        s.createIndex('ticker', 'ticker', { unique: false });
        s.createIndex('market', 'market', { unique: false });
      }
      if (!db.objectStoreNames.contains('meta'))
        db.createObjectStore('meta', { keyPath: 'key' });
    };
    r.onsuccess = e => res(e.target.result);
    r.onerror   = e => rej(e.target.error);
  });
}
async function _idbPutBatch(records) {
  const db = await _openIDB();
  return new Promise((res, rej) => {
    const tx = db.transaction(['ohlcv'], 'readwrite');
    const s  = tx.objectStore('ohlcv');
    records.forEach(r => s.put(r));
    tx.oncomplete = () => res(records.length);
    tx.onerror    = e => rej(e.target.error);
  });
}
async function _idbGetMeta() {
  const db = await _openIDB();
  return new Promise(res => {
    const r = db.transaction(['meta'],'readonly').objectStore('meta').get('stats');
    r.onsuccess = e => res(e.target.result?.value || null);
    r.onerror   = () => res(null);
  });
}
async function _idbSetMeta(v) {
  const db = await _openIDB();
  return new Promise(res => {
    const tx = db.transaction(['meta'],'readwrite');
    tx.objectStore('meta').put({ key:'stats', value:v });
    tx.oncomplete = res;
  });
}
async function _idbClear() {
  const db = await _openIDB();
  return new Promise(res => {
    const tx = db.transaction(['ohlcv','meta'],'readwrite');
    tx.objectStore('ohlcv').clear();
    tx.objectStore('meta').clear();
    tx.oncomplete = res;
  });
}
async function _idbLoadMarket(market) {
  const db = await _openIDB();
  return new Promise(res => {
    const tx  = db.transaction(['ohlcv'],'readonly');
    const idx = tx.objectStore('ohlcv').index('market');
    const r   = idx.getAll(market);
    r.onsuccess = e => res(e.target.result || []);
    r.onerror   = () => res([]);
  });
}

function _cleanSym(raw) {
  let s = String(raw||'').trim();
  if (s.includes(':')) s = s.split(':').pop();
  if (s.startsWith('NSE_') || s.startsWith('BSE_')) s = s.slice(4);
  return s.toUpperCase().trim();
}
function _parseDate(raw) {
  const s = String(raw||'').trim();
  if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s.slice(0,10);
  const m = s.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})/);
  if (m) { let y=m[3]; if(y.length===2) y='20'+y; return `${y}-${m[2].padStart(2,'0')}-${m[1].padStart(2,'0')}`; }
  if (/^\d{8}$/.test(s)) return `${s.slice(0,4)}-${s.slice(4,6)}-${s.slice(6,8)}`;
  return null;
}
function _parseCsv(text, filename) {
  const lines = text.replace(/\r/g,'').split('\n').filter(l=>l.trim());
  if (lines.length < 2) return [];
  const heads = lines[0].split(',').map(h => h.trim().toLowerCase()
    .replace(/[^a-z0-9]/g,'_').replace(/_+/g,'_').replace(/^_|_$/g,''));
  const fi = (...ns) => { for(const n of ns){ const i=heads.findIndex(h=>h===n||h.startsWith(n)); if(i>=0) return i; } return -1; };
  const iDate=fi('datetime','date','time','timestamp','dt'), iSym=fi('symbol','ticker','scrip','stock','name');
  const iOpen=fi('open'), iHigh=fi('high'), iLow=fi('low');
  const iClose=fi('close','ltp','last'), iVol=fi('volume','vol','qty','quantity');
  if (iClose<0 || iDate<0) return [];
  const stemTicker = _cleanSym(filename.replace(/\.[^.]+$/,''));
  const rows = [];
  for (let i=1; i<lines.length; i++) {
    const p = lines[i].split(','); if (p.length < 3) continue;
    const date = _parseDate(p[iDate]); if (!date) continue;
    const close = parseFloat(p[iClose]); if (isNaN(close)||close<=0) continue;
    const ticker = iSym>=0 ? _cleanSym(p[iSym]) : stemTicker; if (!ticker) continue;
    rows.push({ ticker, market:'India', date,
      open:iOpen>=0?parseFloat(p[iOpen])||null:null, high:iHigh>=0?parseFloat(p[iHigh])||null:null,
      low:iLow>=0?parseFloat(p[iLow])||null:null, close, volume:iVol>=0?parseInt(p[iVol])||null:null });
  }
  return rows;
}
function _impLog(msg, type='') {
  const box = $('imp-log'), el = document.createElement('span');
  el.className = `import-log-line ${type}`; el.textContent = `[${new Date().toLocaleTimeString()}]  ${msg}`;
  box.appendChild(el); box.scrollTop = box.scrollHeight;
}
function _fmtBig(n) { return n>=1e6?(n/1e6).toFixed(1)+'M':n>=1e3?(n/1e3).toFixed(1)+'K':String(n); }

async function impProcessFiles(files) {
  _impData = {};
  $('imp-prog-wrap').style.display = 'block';
  $('imp-stats').style.display     = 'none';
  $('imp-log').innerHTML           = '';
  _impLog(`Processing ${files.length} files...`, 'info');
  for (let i=0; i<files.length; i++) {
    const f = files[i];
    $('imp-prog-count').textContent = `${i+1} / ${files.length}`;
    $('imp-prog-bar').style.width   = `${((i+1)/files.length*100).toFixed(0)}%`;
    $('imp-prog-label').textContent = `Reading: ${f.name}`;
    try {
      const text = await f.text();
      const rows = _parseCsv(text, f.name);
      if (!rows.length) { _impLog(`⚠ ${f.name} — no data`, 'warn'); continue; }
      let cnt = 0;
      for (const r of rows) { if (!_impData[r.ticker]) _impData[r.ticker]={}; _impData[r.ticker][r.date]=r; cnt++; }
      _impLog(`✓ ${f.name} — ${[...new Set(rows.map(r=>r.ticker))].join(', ')} — ${cnt.toLocaleString()} rows`, 'ok');
    } catch(e) { _impLog(`✗ ${f.name} — ${e.message}`, 'err'); }
    if (i%20===0) await new Promise(r=>setTimeout(r,0));
  }
  $('imp-prog-label').textContent = 'Done processing';
  $('imp-prog-bar').style.width   = '100%';
  _impShowStats();
}
function _impShowStats() {
  const tickers = Object.keys(_impData);
  if (!tickers.length) { _impLog('No valid data found.','err'); return; }
  let total=0, minD='9999', maxD='0000';
  const rows = tickers.map(t => {
    const dates = Object.keys(_impData[t]).sort(); const n = dates.length; total += n;
    if (dates[0] < minD) minD = dates[0]; if (dates[n-1] > maxD) maxD = dates[n-1];
    return { ticker:t, rows:n, from:dates[0], to:dates[n-1] };
  }).sort((a,b) => b.rows-a.rows);
  $('imp-stat-tickers').textContent = tickers.length;
  $('imp-stat-rows').textContent    = _fmtBig(total);
  $('imp-stat-from').textContent    = minD.slice(0,7);
  $('imp-stat-to').textContent      = maxD.slice(0,7);
  const maxR = rows[0]?.rows || 1;
  $('imp-ticker-list').innerHTML = rows.map(s => `
    <div class="import-ticker-row">
      <span class="import-ticker-name">${s.ticker}</span>
      <div class="import-ticker-bar-wrap"><div class="import-ticker-bar" style="width:${Math.round(s.rows/maxR*180)}px"></div></div>
      <span class="import-ticker-num">${s.rows.toLocaleString()}</span>
      <span class="import-ticker-date">${s.from.slice(0,7)}</span>
      <span class="import-ticker-date">${s.to.slice(0,7)}</span>
    </div>`).join('');
  $('imp-stats').style.display = 'block';
  _impLog(`\nReady: ${tickers.length} tickers, ${total.toLocaleString()} rows (${minD} → ${maxD})`, 'info');
}
async function impSaveToDB() {
  const tickers = Object.keys(_impData); if (!tickers.length) return;
  _impLog('\nSaving to browser storage...', 'info');
  $('imp-prog-label').textContent = 'Saving...'; $('imp-prog-bar').style.width = '0%';
  const BATCH = 15;
  for (let i=0; i<tickers.length; i+=BATCH) {
    const records = tickers.slice(i,i+BATCH).flatMap(t => Object.values(_impData[t]));
    await _idbPutBatch(records);
    $('imp-prog-bar').style.width   = `${Math.round((i+BATCH)/tickers.length*100)}%`;
    $('imp-prog-count').textContent = `${Math.min(i+BATCH,tickers.length)} / ${tickers.length}`;
    await new Promise(r=>setTimeout(r,0));
  }
  const allDates = Object.values(_impData).flatMap(d=>Object.keys(d)).sort();
  await _idbSetMeta({ tickers:Object.keys(_impData).sort(),
    total_rows:Object.values(_impData).reduce((s,v)=>s+Object.keys(v).length,0),
    oldest:allDates[0], newest:allDates[allDates.length-1], saved_at:new Date().toISOString() });
  $('imp-prog-label').textContent = '✅ Saved!';
  _impLog(`✅ Saved ${tickers.length} tickers to browser storage!`, 'ok');
  impCheckStored();
}
async function impUseData() {
  let source = _impData;
  if (!Object.keys(source).length) {
    _impLog('Loading from browser storage...','info');
    const rows = await _idbLoadMarket('India');
    if (!rows.length) { alert('No data. Please import CSV files first.'); return; }
    source = {};
    for (const r of rows) { if (!source[r.ticker]) source[r.ticker]={}; source[r.ticker][r.date]=r; }
  }
  const stockData = _buildStockData(source);
  const result    = _computeBreadthLocal(stockData, 'INDIA');
  currentData['INDIA'] = result;
  lastUpdated['INDIA'] = new Date();
  switchTab('overview');
  renderOverview(result); renderCharts(result); renderSectors(result); updateFreshness();
}
function _buildStockData(source) {
  const out = {};
  for (const [ticker, dateMap] of Object.entries(source)) {
    const sorted = Object.values(dateMap).sort((a,b)=>a.date<b.date?-1:1);
    if (sorted.length < 20) continue;
    out[ticker] = { dates:sorted.map(r=>r.date), close:sorted.map(r=>r.close),
      high:sorted.map(r=>r.high||r.close), low:sorted.map(r=>r.low||r.close), open:sorted.map(r=>r.open||r.close) };
  }
  return out;
}
function _computeBreadthLocal(stockData, market) {
  let adv=0,dec=0,unc=0,a20=0,a50=0,a200=0,nh=0,nl=0,valid=0,w200=0;
  const adMap={}, dmaMap={}, nhMap={};
  for (const [ticker, d] of Object.entries(stockData)) {
    const c=d.close, n=c.length; if (n<21) continue; valid++;
    const cur=c[n-1], prev=c[n-2];
    if (cur>prev*1.001) adv++; else if (cur<prev*0.999) dec++; else unc++;
    const avg=(arr,from,to)=>arr.slice(from,to+1).reduce((s,v)=>s+v,0)/(to-from+1);
    if(n>=20&&cur>avg(c,n-20,n-1)) a20++;
    if(n>=50&&cur>avg(c,n-50,n-1)) a50++;
    if(n>=200){w200++; if(cur>avg(c,n-200,n-1)) a200++;}
    const lb=Math.min(n,252);
    const h52=Math.max(...d.high.slice(-lb)), l52=Math.min(...d.low.slice(-lb));
    if(cur>=h52*0.98) nh++; if(cur<=l52*1.02) nl++;
    const hStart=Math.max(1,n-252);
    for(let i=hStart;i<n;i++){
      const dt=d.dates[i];
      if(!adMap[dt]) adMap[dt]={adv:0,dec:0};
      if(c[i]>c[i-1]*1.001) adMap[dt].adv++; else if(c[i]<c[i-1]*0.999) adMap[dt].dec++;
      if(i>=50){const m50=avg(c,i-50,i); if(!dmaMap[dt]) dmaMap[dt]={a:0,t:0}; dmaMap[dt].t++; if(c[i]>m50) dmaMap[dt].a++;}
    }
  }
  if(!valid) return {error:'No valid data. Import CSV files first.'};
  const adr=dec>0?Math.round(adv/dec*100)/100:adv;
  const p20=Math.round(a20/valid*1000)/10, p50=Math.round(a50/valid*1000)/10;
  const p200=w200>0?Math.round(a200/w200*1000)/10:0;
  const nhNl=nh-nl;
  const score=calcScore(adr,p50,p200,nhNl,valid);
  const regime=calcRegime(score);
  let cum=0;
  const adHistory=Object.entries(adMap).sort(([a],[b])=>a<b?-1:1)
    .map(([dt,v])=>{cum+=v.adv-v.dec; return{date:dt,advancers:v.adv,decliners:v.dec,net:v.adv-v.dec,cumulative:cum};});
  const dmaHistory=Object.entries(dmaMap).sort(([a],[b])=>a<b?-1:1)
    .map(([dt,v])=>({date:dt,pct_above_50:Math.round(v.a/v.t*1000)/10}));
  const SECTORS={
    "IT":["TCS","INFY","HCLTECH","WIPRO","TECHM"],
    "Banking":["HDFCBANK","ICICIBANK","SBIN","AXISBANK","KOTAKBANK"],
    "Pharma":["SUNPHARMA","CIPLA","DRREDDY","DIVISLAB","APOLLOHOSP"],
    "Auto":["MARUTI","TATAMOTORS","EICHERMOT","BAJAJ-AUTO"],
    "FMCG":["HINDUNILVR","ITC","NESTLEIND","BRITANNIA","MARICO"],
    "Metal":["TATASTEEL","JSWSTEEL","HINDALCO","VEDL"],
    "Energy":["RELIANCE","ONGC","NTPC","POWERGRID","ADANIENT"],
    "Infra":["LT","ADANIPORTS","AMBUJACEM","HAVELLS"],
  };
  const sectorBreadth=Object.entries(SECTORS).map(([name,tks])=>{
    let sa=0,st=0,rets=[];
    for(const t of tks){const sd=stockData[t];if(!sd||sd.close.length<51)continue;st++;
      const sc=sd.close,sn=sc.length,m50=sc.slice(sn-50).reduce((s,v)=>s+v,0)/50;
      if(sc[sn-1]>m50) sa++;
      if(sn>=5){const p5=sc[sn-5];if(p5>0) rets.push((sc[sn-1]-p5)/p5*100);}}
    return{sector:name,pct_above_50:st?Math.round(sa/st*1000)/10:0,
      week_return:rets.length?Math.round(rets.reduce((s,v)=>s+v,0)/rets.length*100)/100:0,stocks_counted:st};
  }).sort((a,b)=>b.pct_above_50-a.pct_above_50);
  return{valid,with_200dma:w200,advancers:adv,decliners:dec,unchanged:unc,
    ad_ratio:adr,pct_above_20:p20,pct_above_50:p50,pct_above_200:p200,
    new_highs:nh,new_lows:nl,nh_nl:nhNl,score,regime,regime_color:regimeColor(regime),
    divergence:null,market,index_name:'NIFTY 500',index_price:0,index_change_pct:0,vix:0,
    ad_history:adHistory,dma_history:dmaHistory,nh_nl_history:[],
    sector_breadth:sectorBreadth,universe_size:valid,data_source:'local_csv',timestamp:new Date().toISOString()};
}
async function impCheckStored() {
  const meta = await _idbGetMeta();
  const banner = $('stored-banner');
  if (meta && meta.total_rows > 0) {
    banner.style.display = 'flex';
    $('stored-banner-text').textContent =
      `${meta.tickers?.length||'?'} tickers · ${_fmtBig(meta.total_rows)} rows · ${meta.oldest?.slice(0,7)} → ${meta.newest?.slice(0,7)}`;
  } else { banner.style.display = 'none'; }
}
function impClear() {
  if (!confirm('Clear all stored data?')) return;
  _idbClear().then(()=>{
    _impData={};
    $('imp-prog-wrap').style.display='none'; $('imp-stats').style.display='none';
    $('stored-banner').style.display='none'; $('imp-log').innerHTML='';
    _impLog('Storage cleared.','warn'); $('imp-prog-wrap').style.display='block';
  });
}
// Drop zone events
(function(){
  const zone=$('imp-drop-zone'), input=$('imp-file-input');
  zone.addEventListener('dragover',e=>{e.preventDefault();zone.classList.add('dragover')});
  zone.addEventListener('dragleave',()=>zone.classList.remove('dragover'));
  zone.addEventListener('drop',e=>{e.preventDefault();zone.classList.remove('dragover');
    const files=Array.from(e.dataTransfer.files).filter(f=>/\.csv$/i.test(f.name));
    if(files.length) impProcessFiles(files); else alert('Please drop CSV files only.');});
  input.addEventListener('change',e=>{if(e.target.files.length) impProcessFiles(Array.from(e.target.files));});
})();

// Importer check on tab open (handled in main switchTab below)

// Auto-load from IndexedDB on startup
window.addEventListener('load', async ()=>{
  impCheckStored();
  const meta = await _idbGetMeta();
  if (meta && meta.total_rows > 0 && !currentData['INDIA']) {
    setTimeout(async ()=>{
      try {
        const rows = await _idbLoadMarket('India');
        if (rows.length > 100) {
          const source = {};
          for (const r of rows) { if (!source[r.ticker]) source[r.ticker]={}; source[r.ticker][r.date]=r; }
          const sd = _buildStockData(source);
          const result = _computeBreadthLocal(sd, 'INDIA');
          if (!result.error && !currentData['INDIA']) {
            currentData['INDIA'] = result; lastUpdated['INDIA'] = new Date();
            renderOverview(result); renderCharts(result); renderSectors(result); updateFreshness();
            console.log('Auto-loaded from IndexedDB:', result.valid, 'tickers');
          }
        }
      } catch(e){ console.warn('Auto-load failed:', e); }
    }, 500);
  }
});


// ════════════════════════════════════════════════════════════════════════════
// NSE INDEX UNIVERSE — Sync, Status, Display
// ════════════════════════════════════════════════════════════════════════════

let _nseIdxData = null;
let _nseIdxActiveTab = 'broad';

// ── Load status on tab open ──────────────────────────────────────────────────
async function loadNseIndicesStatus() {
  try {
    const res  = await fetch(`${API}/api/nse-indices/status`);
    const data = await res.json();
    _nseIdxData = data;
    _renderNseIdxStatus(data);
    _loadNseUniverseStats();
  } catch(e) {
    console.warn('NSE indices status fetch failed:', e);
  }
}

// ── Render status cards ───────────────────────────────────────────────────────
function _renderNseIdxStatus(data) {
  if (!data) return;

  const totalSynced   = data.total_synced || 0;
  const broadList     = data.broad     || [];
  const sectoralList  = data.sectoral  || [];
  const thematicList  = data.thematic  || [];

  // Update summary bar
  const summary = document.getElementById('nse-idx-summary');
  if (summary && totalSynced > 0) {
    summary.style.display = 'flex';
    document.getElementById('nse-idx-total-synced').textContent = totalSynced;
    document.getElementById('nse-idx-broad-count').textContent  = broadList.filter(i=>i.status==='ok').length;
    document.getElementById('nse-idx-sectoral-count').textContent = sectoralList.filter(i=>i.status==='ok').length;
    document.getElementById('nse-idx-thematic-count').textContent = thematicList.filter(i=>i.status==='ok').length;

    // Last sync time
    const allSynced = [...broadList, ...sectoralList, ...thematicList]
      .filter(i => i.last_synced).map(i => i.last_synced).sort().reverse();
    if (allSynced.length > 0) {
      const d = new Date(allSynced[0]);
      document.getElementById('nse-idx-last-sync-time').textContent =
        d.toLocaleDateString('en-IN', {day:'2-digit', month:'short', year:'numeric'});
    }
  }

  // Render cards for active tab
  _renderNseIdxCards(_nseIdxActiveTab);
}

function _renderNseIdxCards(tab) {
  const grid = document.getElementById('nse-idx-grid');
  if (!grid || !_nseIdxData) return;

  const list = _nseIdxData[tab] || [];
  if (list.length === 0) {
    grid.innerHTML = `<div style="color:var(--text3);font-size:12px;padding:20px;grid-column:1/-1;text-align:center">
      Click "Sync All Indices" to download constituent data from NSE
    </div>`;
    return;
  }

  grid.innerHTML = list.map(idx => {
    const status    = idx.status || 'pending';
    const count     = idx.constituent_count || 0;
    const syncedAt  = idx.last_synced
      ? new Date(idx.last_synced).toLocaleDateString('en-IN',{day:'2-digit',month:'short'})
      : null;

    return `<div class="nse-idx-card">
      <div class="nse-idx-card-name">${idx.index_name}</div>
      <div class="nse-idx-card-count">${count > 0 ? count + ' stocks' : '—'}</div>
      <div class="nse-idx-card-status ${status}">
        ${status === 'ok' ? '✓ Synced' : status === 'failed' ? '✗ Failed' : '○ Pending'}
      </div>
      ${syncedAt ? `<div class="nse-idx-last-sync">${syncedAt}</div>` : ''}
    </div>`;
  }).join('');
}

function showNseIdxTab(tab, btn) {
  _nseIdxActiveTab = tab;
  document.querySelectorAll('.nse-idx-tab').forEach(b => b.classList.remove('active'));
  if (btn) btn.classList.add('active');
  _renderNseIdxCards(tab);
}

// ── Sync button ───────────────────────────────────────────────────────────────
async function syncNseIndices() {
  const btn      = document.getElementById('nse-idx-sync-btn');
  const progress = document.getElementById('nse-idx-progress');
  const msg      = document.getElementById('nse-idx-prog-msg');
  const num      = document.getElementById('nse-idx-prog-num');
  const fill     = document.getElementById('nse-idx-prog-fill');

  if (!btn) return;
  btn.disabled = true;
  btn.innerHTML = '<span>⏳</span> Syncing...';
  if (progress) progress.style.display = 'block';
  if (fill) { fill.style.width = '0%'; fill.style.background = 'var(--accent1)'; }

  try {
    await fetch(`${API}/api/nse-indices/sync`, { method: 'POST' });
  } catch(e) {
    if (msg) msg.textContent = 'Failed to start: ' + e.message;
    btn.disabled = false;
    btn.innerHTML = '<span>⬇</span> Sync All Indices';
    return;
  }

  // Poll for progress
  const poll = setInterval(async () => {
    try {
      const res = await fetch(`${API}/api/nse-indices/sync/status`);
      const s   = await res.json();

      if (msg) msg.textContent = s.message || 'Syncing...';
      if (s.total > 0) {
        const pct = Math.round((s.progress / s.total) * 100);
        if (fill) fill.style.width = pct + '%';
        if (num)  num.textContent  = `${s.progress}/${s.total}`;
      }

      if (!s.running) {
        clearInterval(poll);
        if (fill) { fill.style.width = '100%'; fill.style.background = '#22c55e'; }
        btn.disabled = false;
        btn.innerHTML = '<span>✓</span> Sync Complete';
        setTimeout(() => {
          btn.innerHTML = '<span>⬇</span> Sync All Indices';
        }, 3000);
        // Reload status to show updated cards
        await loadNseIndicesStatus();
      }
    } catch(e) { /* ignore polling errors */ }
  }, 1500);
}



// ── Backfill missing OHLCV ────────────────────────────────────────────────────
async function backfillMissingOhlcv() {
  const btn = document.getElementById('nse-idx-backfill-btn');
  const progress = document.getElementById('nse-idx-progress');
  const msg      = document.getElementById('nse-idx-prog-msg');
  const num      = document.getElementById('nse-idx-prog-num');
  const fill     = document.getElementById('nse-idx-prog-fill');

  if (!btn) return;
  btn.disabled = true;
  btn.innerHTML = '<span>⏳</span> Backfilling...';
  if (progress) progress.style.display = 'block';
  if (fill) { fill.style.width = '0%'; fill.style.background = 'var(--accent2)'; }

  try {
    await fetch(`${API}/api/nse-indices/backfill-missing`, { method: 'POST' });
  } catch(e) {
    if (msg) msg.textContent = 'Failed: ' + e.message;
    btn.disabled = false;
    btn.innerHTML = '<span>📥</span> Backfill Missing OHLCV';
    return;
  }

  // Poll progress
  const poll = setInterval(async () => {
    try {
      const res = await fetch(`${API}/api/nse-indices/sync/status`);
      const s   = await res.json();
      if (msg) msg.textContent = s.message || 'Backfilling...';
      if (s.total > 0 && fill) {
        fill.style.width = Math.round((s.progress / s.total) * 100) + '%';
        if (num) num.textContent = `${s.progress}/${s.total}`;
      }
      if (!s.running) {
        clearInterval(poll);
        if (fill) { fill.style.width = '100%'; fill.style.background = '#22c55e'; }
        btn.disabled = false;
        btn.innerHTML = '<span>✓</span> Backfill Done';
        setTimeout(() => {
          btn.innerHTML = '<span>📥</span> Backfill Missing OHLCV';
        }, 3000);
        // Refresh coverage stats
        await _loadNseUniverseStats();
      }
    } catch(e) {}
  }, 2000);
}

// ── Load universe coverage stats ──────────────────────────────────────────────
async function _loadNseUniverseStats() {
  try {
    const res  = await fetch(`${API}/api/nse-indices/universe-stats`);
    const data = await res.json();
    if (!data || data.error) return;

    const totalEl    = document.getElementById('nse-idx-total-synced');
    const coverageEl = document.getElementById('nse-idx-coverage');
    const summary    = document.getElementById('nse-idx-summary');

    if (data.unique_tickers > 0 && summary) {
      summary.style.display = 'flex';
      if (coverageEl) coverageEl.textContent = (data.coverage_pct || 0) + '%';
    }
  } catch(e) { /* silent */ }
}


// ── TV Fundamentals Sync ──────────────────────────────────────────────────────
async function syncTvFundamentals() {
  const btn     = document.getElementById('tv-fund-btn');
  const progress = document.getElementById('nse-sync-status');
  const msgEl   = document.getElementById('nse-sync-msg');
  const barEl   = document.getElementById('nse-sync-bar');

  if (btn) { btn.disabled = true; btn.textContent = '⏳ Syncing...'; }
  if (progress) progress.style.display = 'block';
  if (msgEl) msgEl.textContent = 'Fetching fundamentals from TradingView...';
  if (barEl) { barEl.style.width = '0%'; barEl.style.background = '#7c3aed'; }

  try {
    await fetch(`${API}/api/fundamentals/tv-sync`, { method: 'POST' });
  } catch(e) {
    if (msgEl) msgEl.textContent = 'Failed: ' + e.message;
    if (btn) { btn.disabled = false; btn.textContent = '🧠 TV Fundamentals'; }
    return;
  }

  // Poll — usually completes in ~10s
  const poll = setInterval(async () => {
    try {
      const res = await fetch(`${API}/api/fundamentals/tv-sync/status`);
      const s   = await res.json();
      if (msgEl) msgEl.textContent = s.message || 'Syncing...';
      if (!s.running) {
        clearInterval(poll);
        if (barEl) { barEl.style.width = '100%'; barEl.style.background = '#22c55e'; }
        if (btn) {
          btn.disabled = false;
          btn.textContent = `✓ ${s.count || '?'} stocks`;
          setTimeout(() => { btn.textContent = '🧠 TV Fundamentals'; }, 4000);
        }
      }
    } catch(e) {}
  }, 1500);
}


// ── Prefetch Quarterly Fundamentals ──────────────────────────────────────────
async function prefetchQuarterly() {
  const btn  = document.getElementById('prefetch-q-btn');
  const msg  = document.getElementById('nse-sync-msg');
  const bar  = document.getElementById('nse-sync-bar');
  const prog = document.getElementById('nse-sync-status');

  if (btn) { btn.disabled = true; btn.textContent = '⏳ Prefetching...'; }
  if (prog) prog.style.display = 'block';
  if (bar)  { bar.style.width = '0%'; bar.style.background = '#0891b2'; }
  if (msg)  msg.textContent = 'Starting quarterly data prefetch...';

  try {
    await fetch(`${API}/api/fundamentals/prefetch-quarterly?max_tickers=200`,
      { method: 'POST' });
  } catch(e) {
    if (msg) msg.textContent = 'Failed: ' + e.message;
    if (btn) { btn.disabled = false; btn.textContent = '📊 Prefetch Quarterly Data'; }
    return;
  }

  const poll = setInterval(async () => {
    try {
      const res = await fetch(`${API}/api/fundamentals/tv-sync/status`);
      const s   = await res.json();
      if (msg) msg.textContent = s.message || 'Running...';
      if (s.total > 0 && bar) {
        bar.style.width = Math.round((s.progress / s.total) * 100) + '%';
      }
      if (!s.running) {
        clearInterval(poll);
        if (bar)  { bar.style.width = '100%'; bar.style.background = '#22c55e'; }
        if (btn) {
          btn.disabled = false;
          btn.textContent = '✓ Quarterly Cached';
          setTimeout(() => { btn.textContent = '📊 Prefetch Quarterly Data'; }, 4000);
        }
      }
    } catch(e) {}
  }, 2000);
}
