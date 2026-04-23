# Monitor UI Redesign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rebuild the embedded monitoring page into a responsive dashboard with better styling, client-side sorting, filtering, pagination, percentage-based spread display, and a spread-history line chart.

**Architecture:** Keep the current Axum routes and DTOs unchanged and replace the embedded HTML/CSS/JavaScript in the index route with a richer browser-rendered UI. Store list, filter, sort, pagination, selection, and health state entirely on the client and render desktop and mobile views from the same payload, while converting spread-style values from `bps` to percentage formatting only at presentation time.

**Tech Stack:** Rust, Axum, inline HTML/CSS/vanilla JavaScript, inline SVG chart rendering

---

### Task 1: Replace the placeholder page shell with the dashboard layout

**Files:**
- Modify: `src/web/routes.rs`
- Reference: `docs/superpowers/specs/2026-04-23-monitor-ui-redesign-design.md`
- Verify: `cargo check`

- [ ] **Step 1: Confirm the current route compiles before editing**

Run: `cargo check`
Expected: the project compiles successfully before the UI rewrite begins

- [ ] **Step 2: Replace the old HTML shell with the new responsive layout**

Update the `INDEX_HTML` constant in `src/web/routes.rs` so the page contains:

```html
<main class="app-shell">
  <section class="hero">...</section>
  <section class="summary-grid" id="summary-grid"></section>
  <section class="toolbar">...</section>
  <section class="content-grid">
    <div class="list-panel">...</div>
    <aside class="detail-panel" id="detail-panel">...</aside>
  </section>
</main>
```

- [ ] **Step 3: Add the new shared design system styles**

Define CSS variables and responsive layout rules directly in the page:

```css
:root {
  --bg: #f4efe8;
  --panel: rgba(255, 252, 247, 0.92);
  --ink: #182126;
  --muted: #66727b;
  --line: #d8d7cf;
  --brand: #0f766e;
  --brand-soft: #d8f0ec;
  --warn: #b7791f;
}

.content-grid {
  display: grid;
  grid-template-columns: minmax(0, 1.65fr) minmax(320px, 0.95fr);
  gap: 20px;
}

@media (max-width: 980px) {
  .content-grid {
    grid-template-columns: 1fr;
  }
}
```

- [ ] **Step 4: Re-run the compile check after replacing the shell**

Run: `cargo check`
Expected: the route still compiles with the updated `INDEX_HTML` constant

- [ ] **Step 5: Commit the structural UI shell**

```bash
git add src/web/routes.rs
git commit -m "feat: redesign monitor page shell"
```

### Task 2: Add client-side state, sorting, filtering, and pagination

**Files:**
- Modify: `src/web/index.html`
- Verify: `cargo check`

- [ ] **Step 1: Define client-side state for list rendering**

Add a browser-side state object similar to:

```js
const state = {
  pairs: [],
  health: null,
  selectedPairId: null,
  sortKey: "open_spread_abs",
  sortDir: "desc",
  currentPage: 1,
  pageSize: 20,
  searchTerm: "",
  exchange: "all",
  pairKind: "all",
  onlyFundingDiff: false,
  onlyFresh: false,
  lastPairsAt: null,
};
```

- [ ] **Step 2: Implement filter predicates**

Add helpers that return a filtered list:

```js
function matchesFilters(item) {
  const haystack = [
    item.pair.canonical_symbol,
    item.pair.left.exchange,
    item.pair.right.exchange,
    item.pair.left.instrument.market_kind,
    item.pair.right.instrument.market_kind,
  ].join(" ").toLowerCase();

  if (state.searchTerm && !haystack.includes(state.searchTerm)) {
    return false;
  }

  if (state.pairKind !== "all" && item.pair.kind !== state.pairKind) {
    return false;
  }

  return true;
}
```

- [ ] **Step 3: Add pagination helpers after sorting**

Add helpers that paginate the filtered list:

```js
function paginatedPairs(items) {
  const totalPages = Math.max(1, Math.ceil(items.length / state.pageSize));
  state.currentPage = Math.min(state.currentPage, totalPages);
  const start = (state.currentPage - 1) * state.pageSize;
  return {
    totalPages,
    pageItems: items.slice(start, start + state.pageSize),
  };
}
```

- [ ] **Step 4: Implement sortable metric accessors**

Add a map of sort accessors and apply them before rendering:

```js
const sorters = {
  open_spread_abs: item => Math.abs(item.metrics.open_spread_bps ?? -Infinity),
  close_spread_abs: item => Math.abs(item.metrics.close_spread_bps ?? -Infinity),
  funding_diff_abs: item => Math.abs(item.metrics.funding_diff_bps ?? -Infinity),
  index_diff_abs: item => Math.abs(item.metrics.index_diff_bps ?? -Infinity),
  volume_24h: item => (item.metrics.left_volume_24h ?? 0) + (item.metrics.right_volume_24h ?? 0),
  updated_at: item => item.metrics.updated_at_ms ?? 0,
  symbol: item => item.pair.canonical_symbol,
};
```

- [ ] **Step 5: Wire controls, table-header sorting, and pagination to shared render logic**

Connect:

```js
searchInput.addEventListener("input", event => {
  state.searchTerm = event.target.value.trim().toLowerCase();
  state.currentPage = 1;
  render();
});

sortSelect.addEventListener("change", event => {
  state.sortKey = event.target.value;
  state.currentPage = 1;
  render();
});
```

Also attach click handlers to sortable desktop headers so header interactions and the dropdown stay in sync, and add event handlers for first/previous/page-number/next/last pagination controls.

- [ ] **Step 6: Re-run compile verification**

Run: `cargo check`
Expected: the project still compiles after the browser-state logic is added

- [ ] **Step 7: Commit interactive list behavior**

```bash
git add src/web/index.html
git commit -m "feat: add monitor sorting filtering and pagination"
```

### Task 3: Add desktop balance, percentage formatting, mobile cards, detail summary, and spread-history chart

**Files:**
- Modify: `src/web/index.html`
- Verify: `cargo check`

- [ ] **Step 1: Make the desktop list and detail panels share the same overall height**

Update the desktop layout so both panels use the same container height and scroll internally:

```css
.list-panel,
.detail-panel {
  display: flex;
  flex-direction: column;
  height: min(78vh, 920px);
}
```

- [ ] **Step 2: Convert spread-style values from `bps` to percentage formatting**

Add presentation helpers that convert:

```js
function formatSignedPercentFromBps(value) {
  const percent = Number(value) / 100;
  return `${percent.toFixed(3)}%`;
}
```

Use this helper for open spread, close spread, funding difference, index difference, summary cards, and chart labels.

- [ ] **Step 3: Render mobile cards from the same paginated list**

Add a mobile card container and render cards like:

```js
function renderMobileCards(items) {
  mobileList.innerHTML = items.map(item => `
    <button class="pair-card" data-pair-id="${item.pair.pair_id}">
      <div class="pair-card__header">
        <strong>${item.pair.canonical_symbol}</strong>
        <span class="badge">${item.pair.kind}</span>
      </div>
      <div class="pair-card__metrics">...</div>
    </button>
  `).join("");
}
```

- [ ] **Step 4: Restructure the detail panel into summary blocks**

Render overview cards first, followed by chart and funding sections:

```js
detailPanel.innerHTML = `
  <header class="detail-head">...</header>
  <section class="metric-grid">...</section>
  <section class="chart-card">${renderSpreadChart(payload.spread_history)}</section>
  <section class="funding-grid">...</section>
`;
```

- [ ] **Step 5: Implement the inline SVG line chart with percentage labels**

Add a helper that renders open and close spread paths:

```js
function renderSpreadChart(points) {
  if (!points.length) {
    return '<div class="empty-chart">暂无最近价差历史</div>';
  }

  const values = points.flatMap(point => [point.open_spread_bps, point.close_spread_bps]);
  const min = Math.min(...values);
  const max = Math.max(...values);

  return `<svg viewBox="0 0 640 220">...</svg>`;
}
```

- [ ] **Step 6: Keep selection stable across list refreshes and page changes**

Ensure `loadPairs()` preserves the selected pair when possible and only falls back to the first item on the current page when the current selection disappears.

- [ ] **Step 7: Re-run compile verification**

Run: `cargo check`
Expected: the project still compiles after the detail-panel and chart work

- [ ] **Step 8: Commit detail and chart improvements**

```bash
git add src/web/index.html
git commit -m "feat: add pagination layout balance and percent display"
```

### Task 4: Polish states, run verification, and check requirements coverage

**Files:**
- Modify: `src/web/index.html`
- Modify: `src/web/routes.rs`
- Verify: `cargo fmt`, `cargo check`

- [ ] **Step 1: Add empty, loading, and stale-data states**

Ensure the page renders explicit placeholders for:

```js
const EMPTY_LIST = `
  <div class="empty-state">
    <h3>没有匹配的监控组合</h3>
    <p>试试放宽筛选条件或切换排序方式。</p>
  </div>
`;
```

- [ ] **Step 2: Format the Rust source**

Run: `cargo fmt`
Expected: `src/web/routes.rs` is formatted without errors

- [ ] **Step 3: Run the full compile verification**

Run: `cargo check`
Expected: the build completes successfully with no Rust compilation errors

- [ ] **Step 4: Verify the implementation against the approved spec**

Check these items manually against `docs/superpowers/specs/2026-04-23-monitor-ui-redesign-design.md`:

- responsive layout
- refreshed visual design
- sort controls
- filter controls
- pagination controls
- percent-based spread display
- balanced desktop panel heights
- detail-panel reorganization
- spread-history line chart

- [ ] **Step 5: Commit the polished dashboard**

```bash
git add src/web/index.html src/web/routes.rs
git commit -m "feat: polish responsive monitor dashboard"
```
