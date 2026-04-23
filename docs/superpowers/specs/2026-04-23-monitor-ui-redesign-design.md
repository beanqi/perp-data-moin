# Monitor UI Redesign Design

## Summary

This redesign upgrades the embedded monitor frontend from a static first-pass table into a responsive monitoring dashboard that supports fast opportunity scanning and focused analysis.

The implementation keeps the existing Axum routes and JSON payloads unchanged. All new sorting, filtering, selection, and charting behavior is handled in the browser with vanilla JavaScript so the backend remains simple.

## Goals

- Improve the visual quality of the page so it feels like a purpose-built monitoring console instead of a placeholder screen.
- Make the layout fully responsive across desktop, tablet, and mobile.
- Add practical sorting controls for spread, funding, index difference, update recency, symbol, and liquidity.
- Add practical filtering controls for keyword search, exchange inclusion, pair kind, funding-difference availability, and recent updates.
- Add traditional pagination controls so large result sets remain easy to browse.
- Display spread- and basis-like values as percentages instead of `bps`.
- Replace the plain spread-history list with a compact line chart for open and close spread history.

## Non-Goals

- No backend API changes.
- No persistence of UI filter or sort preferences across reloads.
- No third-party charting or UI libraries.
- No websocket migration for the frontend polling model.
- No backend pagination API changes in this iteration.

## Current Problems

The existing frontend has three main shortcomings:

1. The page layout is visually sparse and inconsistent, with a placeholder feel.
2. The desktop-first table does not adapt well to smaller screens.
3. The list is static, so users cannot quickly sort or narrow results while scanning live opportunities.

## Proposed UX

### Top-Level Layout

The page becomes a two-stage dashboard:

1. A hero header with title, short description, enabled exchanges, pair count, and refresh status.
2. A control and content area with filters, summary stats, the pair list, and the selected-pair detail panel.

Desktop uses a two-column content layout:

- Left: summary cards, filters, and sortable data table
- Right: sticky detail panel for the selected pair

On desktop, the left list panel and right detail panel should share the same overall height so the dashboard feels visually balanced. Each panel can scroll internally.

Mobile collapses into a single column:

- Summary cards first
- Filters next
- Pair cards instead of the full table
- Detail panel below the list

## Visual Direction

The redesign uses a light monitoring-console style:

- Warm off-white background
- Dark neutral text
- Teal as the primary action and positive emphasis color
- Amber for cautionary highlights
- Modern sans-serif typography with stronger hierarchy
- Rounded panels, clearer spacing, and stronger active/hover states

The main table remains high-density on desktop, but the layout should no longer feel cramped or unfinished.

## List Experience

### Desktop

The list remains table-based for fast comparison, but the presentation changes significantly:

- Sticky table header
- Clickable sortable headers
- Stronger first column with symbol and pair-type badge
- Directional visual emphasis for the most important metrics
- Active row state for the selected pair
- Traditional pagination footer with first, previous, numbered pages, next, last, and page-size selection
- Improved empty-state messaging

### Mobile

The page renders a card list instead of relying on horizontal table scrolling.

Each card shows:

- Symbol
- Pair kind
- Exchange combination
- Open spread
- Close spread
- Funding difference
- Update time

Selecting a card updates the shared detail panel.

The same pagination model is used on mobile so card lists do not become unwieldy.

## Sorting

The frontend supports these sort keys:

- Open spread absolute value
- Close spread absolute value
- Funding difference absolute value
- Index difference absolute value
- Combined 24h volume
- Update time
- Symbol

Sorting can be changed through either:

- the desktop table headers
- the toolbar sort dropdown

The current active sort and direction are reflected in both places.

Default sort remains opportunity-oriented:

- sort by open spread absolute value descending

Sorting and filtering are applied before pagination. When sort or filter state changes, pagination resets to the first page.

## Filtering

The frontend supports these filters:

- Keyword search across symbol, exchange names, and market types
- Exchange inclusion filter using the enabled exchanges present in the data
- Pair kind filter for `spot_perp` and `perp_perp`
- Toggle for pairs that currently have funding-difference data
- Toggle for pairs updated recently

The recent-update filter uses a browser-side freshness threshold and is meant to help isolate stale data issues quickly.

## Pagination

The frontend uses browser-side pagination because the current payload already contains all fields needed for sorting and filtering, and the current scale does not justify a backend pagination API.

The list footer supports:

- first page
- previous page
- visible page numbers
- next page
- last page
- page-size selection

Page changes update the currently selected pair so the right-side detail panel stays aligned with the current page contents.

## Units

All spread-style values that were previously displayed as `bps` are displayed as percentages instead. This includes:

- open spread
- close spread
- funding difference
- index difference
- spread-chart axis labels and legends

The backend data format remains unchanged. Conversion happens only in the browser:

- `100 bps = 1%`

Funding rates were already displayed as percentages and remain so.

## Detail Panel

The detail panel is reorganized into four blocks:

1. Pair summary header
2. Key metric cards
3. Spread history chart
4. Funding settlement history for both legs

The spread history becomes an inline SVG line chart with:

- one line for open spread
- one line for close spread
- grid lines
- min/max-aware scaling
- a compact legend

If there is insufficient history, the chart area shows a clear empty state rather than a broken graphic.

## Data Flow

The frontend continues polling:

- `/api/pairs` for list data
- `/api/health` for exchange health and metadata
- `/api/pairs/:pair_id` for the selected pair detail

Client-side state tracks:

- latest list payload
- current filters
- current sort
- current page
- current page size
- selected pair
- health snapshot

Rendering is split into dedicated functions for summary cards, filters, desktop table, mobile cards, and detail content so updates remain predictable.

## Error Handling

- Failed list or detail fetches should not clear previous successful content immediately.
- Empty data states should render user-facing placeholders.
- Missing metric values should render as `-` with no layout breakage.
- Stale data should be visually distinguishable through update-time treatment and the freshness filter.
- Pagination state should clamp safely if filtering or refresh reduces the total number of pages.

## Files

- Modify [src/web/routes.rs](/Users/mlxy039/code/vibe/perp-data-moin/src/web/routes.rs) to replace the embedded HTML, CSS, and JavaScript.

If the embedded HTML becomes too large to maintain comfortably, it can be moved into a compile-time included static file. That refactor is optional and should only be done if it clearly improves readability during implementation.

## Testing Strategy

- Run formatting after the Rust source edit.
- Run targeted tests or checks that exercise the web module compilation path.
- Manually verify the rendered page in desktop and narrow-screen layouts.
- Confirm sort and filter interactions operate on live payloads without breaking selection.

## Acceptance Criteria

The redesign is complete when:

- the page is visually improved and coherent
- the layout works on desktop and mobile widths
- users can sort and filter the pair list interactively
- users can page through the pair list with traditional pagination controls
- the selected pair detail is easier to scan
- spread-style values are displayed as percentages rather than `bps`
- the desktop list and detail panels feel visually balanced in height
- spread history is displayed as a line chart
- no backend API changes are required
