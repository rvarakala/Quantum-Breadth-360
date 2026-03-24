// ════════════════════════════════════════════════════════════════════════════
// TICKER AUTOCOMPLETE — Reusable dropdown for any ticker input
// Usage: setupTickerAutocomplete('input-id', (ticker) => { ... })
// ════════════════════════════════════════════════════════════════════════════

function setupTickerAutocomplete(inputId, onSelect) {
  const input = document.getElementById(inputId);
  if (!input) return;

  let dropdown = null;
  let items = [];
  let activeIdx = -1;
  let debounceTimer = null;
  let abortCtrl = null;

  // Create dropdown element — wraps input in a relative container
  function ensureDropdown() {
    if (dropdown) return dropdown;

    // Wrap input in a relative-positioned container for proper absolute dropdown
    const wrapper = document.createElement('div');
    wrapper.className = 'ac-wrapper';
    wrapper.style.position = 'relative';
    wrapper.style.flex = '1';
    wrapper.style.minWidth = '0';
    input.parentElement.insertBefore(wrapper, input);
    wrapper.appendChild(input);

    dropdown = document.createElement('div');
    dropdown.className = 'ac-dropdown';
    dropdown.style.display = 'none';
    wrapper.appendChild(dropdown);
    return dropdown;
  }

  function hideDropdown() {
    if (dropdown) dropdown.style.display = 'none';
    items = [];
    activeIdx = -1;
  }

  function renderDropdown(results) {
    const dd = ensureDropdown();
    items = results;
    activeIdx = -1;

    if (!results.length) {
      dd.innerHTML = '<div class="ac-empty">No matches</div>';
      dd.style.display = 'block';
      return;
    }

    dd.innerHTML = results.map((r, i) => {
      const tier = r.mcap_tier ? `<span class="ac-tier">${r.mcap_tier.replace(' Cap', '')}</span>` : '';
      return `<div class="ac-item" data-idx="${i}">
        <span class="ac-ticker">${r.ticker}</span>
        <span class="ac-name">${r.company_name || ''}</span>
        ${tier}
      </div>`;
    }).join('');

    dd.style.display = 'block';

    // Click handlers
    dd.querySelectorAll('.ac-item').forEach(el => {
      el.addEventListener('mousedown', e => {
        e.preventDefault(); // prevent blur
        const idx = parseInt(el.dataset.idx);
        selectItem(idx);
      });
    });
  }

  function selectItem(idx) {
    if (idx < 0 || idx >= items.length) return;
    const ticker = items[idx].ticker;
    input.value = ticker;
    hideDropdown();
    if (onSelect) onSelect(ticker);
  }

  function highlightItem(idx) {
    if (!dropdown) return;
    dropdown.querySelectorAll('.ac-item').forEach((el, i) => {
      el.classList.toggle('ac-active', i === idx);
    });
    activeIdx = idx;
    // Scroll into view
    const active = dropdown.querySelector('.ac-active');
    if (active) active.scrollIntoView({ block: 'nearest' });
  }

  async function fetchResults(query) {
    if (abortCtrl) abortCtrl.abort();
    abortCtrl = new AbortController();

    try {
      const res = await fetch(`${API}/api/tickers/search?q=${encodeURIComponent(query)}&limit=8`, {
        signal: abortCtrl.signal,
      });
      if (!res.ok) return;
      const data = await res.json();
      renderDropdown(data);
    } catch (e) {
      if (e.name !== 'AbortError') console.error('Autocomplete error:', e);
    }
  }

  // Keyup: trigger search with debounce
  input.addEventListener('input', () => {
    const q = input.value.trim();
    if (q.length < 3) {
      hideDropdown();
      return;
    }
    clearTimeout(debounceTimer);
    debounceTimer = setTimeout(() => fetchResults(q), 200);
  });

  // Keyboard navigation
  input.addEventListener('keydown', e => {
    if (!dropdown || dropdown.style.display === 'none') return;

    if (e.key === 'ArrowDown') {
      e.preventDefault();
      const next = activeIdx < items.length - 1 ? activeIdx + 1 : 0;
      highlightItem(next);
    } else if (e.key === 'ArrowUp') {
      e.preventDefault();
      const prev = activeIdx > 0 ? activeIdx - 1 : items.length - 1;
      highlightItem(prev);
    } else if (e.key === 'Enter' && activeIdx >= 0) {
      e.preventDefault();
      e.stopPropagation(); // prevent document-level Enter handlers (charts, SM)
      e.stopImmediatePropagation();
      selectItem(activeIdx);
    } else if (e.key === 'Escape') {
      hideDropdown();
    }
  });

  // Close on blur (delayed to allow click)
  input.addEventListener('blur', () => {
    setTimeout(hideDropdown, 200);
  });

  // Re-show on focus if there's text
  input.addEventListener('focus', () => {
    const q = input.value.trim();
    if (q.length >= 3 && items.length > 0 && dropdown) {
      dropdown.style.display = 'block';
    }
  });
}
