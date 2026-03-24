// ── STOCKBEE MARKET BREADTH ─────────────────────────────────────────────────
let _stockbeeData = null;
let _sbChartUpDown = null;
let _sbChartRatio = null;

async function loadStockbee(forceRefresh=false) {
  const loading = document.getElementById('sb-loading');
  const content = document.getElementById('sb-content');
  if(loading) loading.style.display='block';
  if(content) content.style.display='none';
  try {
    const p = new URLSearchParams({refresh: forceRefresh?'true':'false'});
    const res = await fetch(`${API}/api/stockbee/${currentMarket}?${p}`);
    if(!res.ok) throw new Error('HTTP '+res.status);
    const data = await res.json();
    if(data.error) throw new Error(data.error);
    _stockbeeData = data;
    if(loading) loading.style.display='none';
    if(content) content.style.display='block';
    renderStockbee(data);
  } catch(e) {
    if(loading) loading.innerHTML=`
      <div style="color:var(--red);font-size:12px">⚠ ${e.message}</div>
      <button onclick="loadStockbee()" style="margin-top:10px;padding:6px 14px;cursor:pointer;background:var(--bg3);border:1px solid var(--border);color:var(--text);border-radius:4px;font-family:var(--font-mono);font-size:11px">Retry</button>`;
  }
}

function renderStockbee(data) {
  const t = data.today;

  // Regime badge
  const badge = document.getElementById('sb-regime-badge');
  badge.textContent = data.regime;
  badge.style.background = data.regime_color + '22';
  badge.style.color = data.regime_color;
  badge.style.border = '1px solid ' + data.regime_color + '44';

  // Universe info
  document.getElementById('sb-universe-info').textContent = `${data.universe_size} stocks · ${t.date}`;

  // Metric cards
  document.getElementById('sb-up4-val').textContent = t.up_4pct;
  document.getElementById('sb-dn4-val').textContent = t.dn_4pct;

  const r5el = document.getElementById('sb-r5-val');
  r5el.textContent = t.ratio_5d;
  r5el.style.color = t.ratio_5d >= 1.0 ? 'var(--blue)' : 'var(--red)';

  const r10el = document.getElementById('sb-r10-val');
  r10el.textContent = t.ratio_10d;
  r10el.style.color = t.ratio_10d >= 1.0 ? 'var(--blue)' : 'var(--red)';

  // T2108 bar
  const t2108 = t.t2108;
  document.getElementById('sb-t2108-val').textContent = t2108 + '%';
  const bar = document.getElementById('sb-t2108-bar');
  bar.style.width = Math.min(t2108, 100) + '%';
  bar.style.background = t2108 > 60 ? 'var(--green)' : t2108 < 30 ? 'var(--red)' : 'var(--amber)';

  // Charts
  renderStockbeeCharts(data.history);

  // Momentum table
  const tbody = document.getElementById('sb-momentum-tbody');
  const metrics = [
    {label: '25%+ Quarter (65d)', up: t.up_25pct_qtr, dn: t.dn_25pct_qtr},
    {label: '25%+ Month (21d)', up: t.up_25pct_month, dn: t.dn_25pct_month},
    {label: '50%+ Month (21d)', up: t.up_50pct_month, dn: t.dn_50pct_month},
    {label: '13%+ in 34 Days', up: t.up_13pct_34d, dn: t.dn_13pct_34d},
  ];
  tbody.innerHTML = metrics.map(m => {
    const ratio = m.dn > 0 ? (m.up / m.dn).toFixed(2) : (m.up > 0 ? '∞' : '—');
    const ratioColor = m.dn > 0 && m.up/m.dn >= 1.0 ? 'var(--green)' : 'var(--red)';
    return `<tr style="border-bottom:1px solid var(--border)">
      <td style="padding:10px;color:var(--text2)">${m.label}</td>
      <td style="padding:10px;text-align:right;color:var(--green);font-weight:600">${m.up}</td>
      <td style="padding:10px;text-align:right;color:var(--red);font-weight:600">${m.dn}</td>
      <td style="padding:10px;text-align:right;color:${ratioColor};font-weight:700">${ratio}</td>
    </tr>`;
  }).join('');
}

function renderStockbeeCharts(history) {
  const labels = history.map(h => {
    const d = h.date.slice(5); // MM-DD
    return d;
  });

  // Destroy old charts
  if(_sbChartUpDown) { _sbChartUpDown.destroy(); _sbChartUpDown = null; }
  if(_sbChartRatio) { _sbChartRatio.destroy(); _sbChartRatio = null; }

  const chartFont = {family: "'Space Mono', monospace", size: 9};
  const gridColor = 'rgba(30,45,74,.5)';

  // Chart 1: Up vs Down 4%+ daily
  const ctx1 = document.getElementById('sb-chart-updown');
  if(ctx1) {
    _sbChartUpDown = new Chart(ctx1, {
      type: 'bar',
      data: {
        labels,
        datasets: [
          {label:'UP 4%+', data: history.map(h=>h.up_4pct), backgroundColor:'rgba(34,197,94,.7)', borderRadius:2, barPercentage:.45, categoryPercentage:.8},
          {label:'DOWN 4%+', data: history.map(h=>h.dn_4pct), backgroundColor:'rgba(239,68,68,.7)', borderRadius:2, barPercentage:.45, categoryPercentage:.8},
        ]
      },
      options: {
        responsive:true, maintainAspectRatio:false,
        plugins: {legend:{display:true,position:'top',labels:{color:'#94a3b8',font:chartFont,boxWidth:10,padding:8}}},
        scales: {
          x: {ticks:{color:'#4b5e7a',font:chartFont,maxRotation:45,maxTicksLimit:12}, grid:{color:gridColor}},
          y: {ticks:{color:'#4b5e7a',font:chartFont}, grid:{color:gridColor}, beginAtZero:true}
        }
      }
    });
  }

  // Chart 2: 5D & 10D ratio
  const ctx2 = document.getElementById('sb-chart-ratio');
  if(ctx2) {
    _sbChartRatio = new Chart(ctx2, {
      type: 'line',
      data: {
        labels,
        datasets: [
          {label:'5D Ratio', data: history.map(h=>Math.min(h.ratio_5d,4.0)), borderColor:'#06b6d4', backgroundColor:'rgba(6,182,212,.1)', borderWidth:2, pointRadius:0, tension:.3, fill:false},
          {label:'10D Ratio', data: history.map(h=>Math.min(h.ratio_10d,4.0)), borderColor:'#a855f7', backgroundColor:'rgba(168,85,247,.1)', borderWidth:2, pointRadius:0, tension:.3, fill:false},
          {label:'1.0 Line', data: Array(history.length).fill(1.0), borderColor:'rgba(148,163,184,.3)', borderWidth:1, borderDash:[4,4], pointRadius:0, fill:false},
        ]
      },
      options: {
        responsive:true, maintainAspectRatio:false,
        plugins: {legend:{display:true,position:'top',labels:{color:'#94a3b8',font:chartFont,boxWidth:10,padding:8}}},
        scales: {
          x: {ticks:{color:'#4b5e7a',font:chartFont,maxRotation:45,maxTicksLimit:12}, grid:{color:gridColor}},
          y: {ticks:{color:'#4b5e7a',font:chartFont}, grid:{color:gridColor}}
        }
      }
    });
  }
}
