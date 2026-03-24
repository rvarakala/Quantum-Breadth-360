// ════════════════════════════════════════════════════════════════════════════
// SECTOR ROTATION HEATMAP — D3 treemap
// ════════════════════════════════════════════════════════════════════════════

let _heatmapData = null;
let _heatmapPeriod = '1m';

function _hmColor(change) {
  // green for positive, red for negative, grey for 0
  if (change >= 10) return '#15803d';
  if (change >= 5)  return '#22c55e';
  if (change >= 2)  return '#4ade80';
  if (change >= 0.5) return '#86efac';
  if (change >= -0.5) return '#9ca3af';
  if (change >= -2)  return '#fca5a5';
  if (change >= -5)  return '#ef4444';
  if (change >= -10) return '#dc2626';
  return '#991b1b';
}

function _hmTextColor(change) {
  if (Math.abs(change) < 0.5) return '#374151';
  return '#fff';
}

async function loadHeatmap(period) {
  if (period) _heatmapPeriod = period;

  // Update active button
  document.querySelectorAll('.hm-period-btn').forEach(b => {
    b.classList.toggle('active', b.dataset.period === _heatmapPeriod);
  });

  const container = document.getElementById('heatmap-container');
  if (!container) return;
  container.innerHTML = '<div style="text-align:center;padding:40px;color:var(--text3)">Loading heatmap...</div>';

  try {
    const market = typeof currentMarket !== 'undefined' ? currentMarket : 'India';
    const res = await fetch(`${API}/api/sectors/heatmap?market=${market}&period=${_heatmapPeriod}`);
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const data = await res.json();
    _heatmapData = data;
    _renderTreemap(data);
  } catch (e) {
    container.innerHTML = `<div style="text-align:center;padding:40px;color:var(--red)">Error: ${e.message}</div>`;
  }
}

function _renderTreemap(data) {
  const container = document.getElementById('heatmap-container');
  if (!container || !data.sectors || !data.sectors.length) {
    if (container) container.innerHTML = '<div style="text-align:center;padding:40px;color:var(--text3)">No sector data available</div>';
    return;
  }

  const width = container.clientWidth || 800;
  const height = Math.max(400, Math.min(width * 0.55, 550));
  container.innerHTML = '';

  // Build hierarchy
  const root = d3.hierarchy({
    name: 'root',
    children: data.sectors.map(s => ({
      name: s.sector,
      value: Math.max(s.stock_count, 1),
      change: s.avg_change,
      stock_count: s.stock_count,
      pct_above_50: s.pct_above_50dma,
      pct_above_200: s.pct_above_200dma,
      top_stocks: s.top_stocks || [],
      health: s.health,
    }))
  }).sum(d => d.value);

  d3.treemap()
    .size([width, height])
    .padding(2)
    .round(true)(root);

  const svg = d3.select(container)
    .append('svg')
    .attr('width', width)
    .attr('height', height)
    .style('font-family', 'var(--font-mono)')
    .style('cursor', 'default');

  // Tooltip
  let tooltip = document.getElementById('hm-tooltip');
  if (!tooltip) {
    tooltip = document.createElement('div');
    tooltip.id = 'hm-tooltip';
    tooltip.className = 'hm-tooltip';
    document.body.appendChild(tooltip);
  }

  const leaves = svg.selectAll('g')
    .data(root.leaves())
    .join('g')
    .attr('transform', d => `translate(${d.x0},${d.y0})`);

  // Rectangles
  leaves.append('rect')
    .attr('width', d => Math.max(0, d.x1 - d.x0))
    .attr('height', d => Math.max(0, d.y1 - d.y0))
    .attr('fill', d => _hmColor(d.data.change))
    .attr('rx', 3)
    .attr('stroke', 'var(--bg)')
    .attr('stroke-width', 1.5)
    .style('transition', 'opacity 0.15s')
    .on('mouseover', function(event, d) {
      d3.select(this).style('opacity', 0.85);
      const tops = d.data.top_stocks.map(s => `${s.ticker}: ${s.change >= 0 ? '+' : ''}${s.change}%`).join('<br>');
      tooltip.innerHTML = `
        <div style="font-weight:700;font-size:13px;margin-bottom:4px">${d.data.name}</div>
        <div>Change: <span style="font-weight:700;color:${d.data.change >= 0 ? '#22c55e' : '#ef4444'}">${d.data.change >= 0 ? '+' : ''}${d.data.change.toFixed(2)}%</span></div>
        <div>Stocks: ${d.data.stock_count}</div>
        <div>&gt;50 DMA: ${d.data.pct_above_50}%</div>
        <div>&gt;200 DMA: ${d.data.pct_above_200}%</div>
        <div>Health: <span style="color:${d.data.health === 'hot' ? '#22c55e' : d.data.health === 'warm' ? '#f59e0b' : '#ef4444'}">${d.data.health.toUpperCase()}</span></div>
        ${tops ? `<div style="margin-top:4px;border-top:1px solid rgba(255,255,255,.15);padding-top:4px;font-size:10px">Top:<br>${tops}</div>` : ''}
      `;
      tooltip.style.display = 'block';
      tooltip.style.left = (event.pageX + 12) + 'px';
      tooltip.style.top = (event.pageY - 10) + 'px';
    })
    .on('mousemove', function(event) {
      tooltip.style.left = (event.pageX + 12) + 'px';
      tooltip.style.top = (event.pageY - 10) + 'px';
    })
    .on('mouseout', function() {
      d3.select(this).style('opacity', 1);
      tooltip.style.display = 'none';
    });

  // Labels
  leaves.each(function(d) {
    const w = d.x1 - d.x0;
    const h = d.y1 - d.y0;
    const g = d3.select(this);
    const textCol = _hmTextColor(d.data.change);

    if (w > 60 && h > 35) {
      // Full label
      g.append('text')
        .attr('x', (w) / 2)
        .attr('y', (h) / 2 - 6)
        .attr('text-anchor', 'middle')
        .attr('fill', textCol)
        .attr('font-size', Math.min(12, w / 8) + 'px')
        .attr('font-weight', '700')
        .text(d.data.name.length > w / 8 ? d.data.name.substring(0, Math.floor(w / 8)) : d.data.name);

      g.append('text')
        .attr('x', (w) / 2)
        .attr('y', (h) / 2 + 10)
        .attr('text-anchor', 'middle')
        .attr('fill', textCol)
        .attr('font-size', Math.min(11, w / 9) + 'px')
        .attr('opacity', 0.9)
        .text(`${d.data.change >= 0 ? '+' : ''}${d.data.change.toFixed(1)}%`);
    } else if (w > 35 && h > 20) {
      // Abbreviated
      g.append('text')
        .attr('x', (w) / 2)
        .attr('y', (h) / 2 + 4)
        .attr('text-anchor', 'middle')
        .attr('fill', textCol)
        .attr('font-size', '9px')
        .attr('font-weight', '600')
        .text(`${d.data.change >= 0 ? '+' : ''}${d.data.change.toFixed(1)}%`);
    }
  });

  // Date info
  if (data.latest_date) {
    const infoEl = document.getElementById('hm-info');
    if (infoEl) infoEl.textContent = `Data as of ${data.latest_date} · ${data.sectors.length} sectors · ${_heatmapPeriod.toUpperCase()} change`;
  }
}

// Re-render on window resize
window.addEventListener('resize', () => {
  if (_heatmapData && document.getElementById('heatmap-container')?.offsetParent) {
    _renderTreemap(_heatmapData);
  }
});
