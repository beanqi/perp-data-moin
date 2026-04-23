use axum::Json;
use axum::Router;
use axum::extract::{Path, State};
use axum::response::{Html, IntoResponse};
use axum::routing::get;

use crate::web::dto::{ExchangeHealthView, HealthResponse, PairDetailResponse, PairListResponse};
use crate::web::server::WebState;

pub fn router() -> Router<WebState> {
    Router::new()
        .route("/", get(index))
        .route("/api/health", get(health))
        .route("/api/pairs", get(list_pairs))
        .route("/api/pairs/:pair_id", get(pair_detail))
}

async fn index() -> impl IntoResponse {
    Html(INDEX_HTML)
}

async fn list_pairs(State(state): State<WebState>) -> Json<PairListResponse> {
    let app_view = state.summary();
    Json(PairListResponse {
        items: app_view.pair_views().to_vec(),
    })
}

async fn health(State(state): State<WebState>) -> Json<HealthResponse> {
    let app_view = state.summary();
    Json(HealthResponse {
        exchanges: app_view
            .exchange_statuses()
            .iter()
            .map(|(exchange, status)| ExchangeHealthView {
                exchange: *exchange,
                ok: status.ok,
                message: status.message.clone(),
                updated_at_ms: status.updated_at_ms,
            })
            .collect(),
        enabled_exchanges: state.enabled_exchanges().to_vec(),
        pair_count: app_view.pair_views().len(),
    })
}

async fn pair_detail(
    Path(pair_id): Path<String>,
    State(state): State<WebState>,
) -> Json<PairDetailResponse> {
    let detail = state.pair_detail(pair_id).await;

    Json(PairDetailResponse {
        item: detail.item,
        spread_history: detail.spread_history,
        left_funding_history: detail.left_funding_history,
        right_funding_history: detail.right_funding_history,
    })
}

const INDEX_HTML: &str = r##"<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Perp Data Monitor</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f3efe7;
      --panel: #fffdf8;
      --ink: #1d1d1b;
      --muted: #6f6a61;
      --line: #ddd4c8;
      --accent: #136f63;
      --accent-soft: #dcefe9;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: "Iowan Old Style", "Palatino Linotype", "Book Antiqua", serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top right, rgba(19,111,99,0.12), transparent 30%),
        linear-gradient(180deg, #f8f4ed 0%, var(--bg) 100%);
    }
    main {
      max-width: 1280px;
      margin: 0 auto;
      padding: 32px 20px 56px;
    }
    h1 {
      margin: 0 0 8px;
      font-size: 42px;
      letter-spacing: -0.03em;
    }
    p {
      margin: 0 0 20px;
      color: var(--muted);
    }
    .grid {
      display: grid;
      gap: 16px;
      grid-template-columns: 2fr 1fr;
    }
    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 18px;
      overflow: hidden;
      box-shadow: 0 12px 40px rgba(29,29,27,0.06);
    }
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
    }
    th, td {
      padding: 12px 14px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: top;
    }
    th {
      background: rgba(19,111,99,0.06);
      font-size: 12px;
      letter-spacing: 0.04em;
      text-transform: uppercase;
      color: var(--muted);
    }
    tr:hover td {
      background: rgba(19,111,99,0.05);
      cursor: pointer;
    }
    .meta {
      display: inline-flex;
      gap: 8px;
      align-items: center;
      padding: 6px 10px;
      border-radius: 999px;
      background: var(--accent-soft);
      color: var(--accent);
      font-size: 12px;
    }
    .meta-row {
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      margin-bottom: 18px;
    }
    .detail {
      padding: 18px;
    }
    .detail h2 {
      margin: 0 0 10px;
      font-size: 24px;
    }
    .detail-section {
      margin-top: 16px;
      padding-top: 16px;
      border-top: 1px solid var(--line);
    }
    .detail-section h3 {
      margin: 0 0 10px;
      font-size: 14px;
      text-transform: uppercase;
      color: var(--muted);
      letter-spacing: 0.06em;
    }
    .list {
      display: grid;
      gap: 8px;
      font-size: 14px;
    }
    .row {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      border-bottom: 1px dashed var(--line);
      padding-bottom: 8px;
    }
    .row:last-child {
      border-bottom: none;
      padding-bottom: 0;
    }
    .muted { color: var(--muted); }
    @media (max-width: 960px) {
      .grid { grid-template-columns: 1fr; }
      h1 { font-size: 34px; }
      table { display: block; overflow-x: auto; }
    }
  </style>
</head>
<body>
  <main>
    <div class="meta-row">
      <div class="meta">自动发现 + 自动对齐 + 内存历史窗口</div>
      <div class="meta" id="monitor-meta">当前监控交易所加载中</div>
    </div>
    <h1>Perp Data Monitor</h1>
    <p>当前是第一版骨架页面，列表展示监控组合，右侧展示单条组合的最近明细。</p>
    <div class="grid">
      <section class="panel">
        <table id="pairs-table">
          <thead>
            <tr>
              <th>标的</th>
              <th>组合</th>
              <th>开仓价差 (bps)</th>
              <th>平仓价差 (bps)</th>
              <th>当前费率</th>
              <th>费率差 (bps)</th>
              <th>指数价格</th>
              <th>指数价差 (bps)</th>
              <th>24h 成交额</th>
              <th>更新时间</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </section>
      <aside class="panel detail" id="detail-panel">
        <h2>选择一条监控组合</h2>
        <p class="muted">详情区域会展示最近 30 分钟价差历史和最近 24 小时费率结算历史。</p>
      </aside>
    </div>
  </main>
  <script>
    const tbody = document.querySelector("#pairs-table tbody");
    const detailPanel = document.querySelector("#detail-panel");
    const monitorMeta = document.querySelector("#monitor-meta");
    let selectedPairId = null;

    function formatNumber(value) {
      return value == null ? "-" : Number(value).toFixed(2);
    }

    function formatPrice(value) {
      if (value == null) {
        return "-";
      }
      const abs = Math.abs(Number(value));
      if (abs >= 1000) {
        return Number(value).toFixed(2);
      }
      if (abs >= 1) {
        return Number(value).toFixed(4);
      }
      if (abs >= 0.01) {
        return Number(value).toFixed(6);
      }
      return Number(value).toFixed(8);
    }

    function formatRate(value) {
      if (value == null) {
        return "-";
      }
      return `${(Number(value) * 100).toFixed(4)}%`;
    }

    function formatVolume(value) {
      if (value == null) {
        return "-";
      }
      return Number(value).toLocaleString(undefined, { maximumFractionDigits: 2 });
    }

    function formatPairValue(left, right, formatter) {
      return `${formatter(left)} / ${formatter(right)}`;
    }

    function formatTime(ts) {
      return ts ? new Date(ts).toLocaleString() : "-";
    }

    async function loadPairs() {
      const response = await fetch("/api/pairs");
      const payload = await response.json();
      tbody.innerHTML = "";

      for (const item of payload.items) {
        const tr = document.createElement("tr");
        tr.innerHTML = `
          <td>${item.pair.canonical_symbol}</td>
          <td>${item.pair.left.exchange}/${item.pair.left.instrument.market_kind} -> ${item.pair.right.exchange}/${item.pair.right.instrument.market_kind}</td>
          <td>${formatNumber(item.metrics.open_spread_bps)}</td>
          <td>${formatNumber(item.metrics.close_spread_bps)}</td>
          <td>${formatPairValue(item.metrics.left_funding_rate, item.metrics.right_funding_rate, formatRate)}</td>
          <td>${formatNumber(item.metrics.funding_diff_bps)}</td>
          <td>${formatPairValue(item.metrics.left_index_price, item.metrics.right_index_price, formatPrice)}</td>
          <td>${formatNumber(item.metrics.index_diff_bps)}</td>
          <td>${formatPairValue(item.metrics.left_volume_24h, item.metrics.right_volume_24h, formatVolume)}</td>
          <td>${formatTime(item.metrics.updated_at_ms)}</td>
        `;
        tr.onclick = () => {
          selectedPairId = item.pair.pair_id;
          loadPairDetail(item.pair.pair_id);
        };
        tbody.appendChild(tr);
      }

      if (!selectedPairId && payload.items.length > 0) {
        selectedPairId = payload.items[0].pair.pair_id;
        loadPairDetail(selectedPairId);
      }
    }

    async function loadHealth() {
      const response = await fetch("/api/health");
      const payload = await response.json();
      const exchanges = payload.enabled_exchanges && payload.enabled_exchanges.length > 0
        ? payload.enabled_exchanges.join(", ")
        : "全部";
      monitorMeta.textContent = `当前监控交易所: ${exchanges}`;
    }

    async function loadPairDetail(pairId) {
      const response = await fetch(`/api/pairs/${encodeURIComponent(pairId)}`);
      const payload = await response.json();

      if (!payload.item) {
        detailPanel.innerHTML = "<h2>未找到监控组合</h2>";
        return;
      }

      detailPanel.innerHTML = `
        <h2>${payload.item.pair.canonical_symbol}</h2>
        <p class="muted">${payload.item.pair.left.exchange}/${payload.item.pair.left.instrument.market_kind} -> ${payload.item.pair.right.exchange}/${payload.item.pair.right.instrument.market_kind}</p>
        <div class="detail-section">
          <h3>当前指标</h3>
          <div class="list">
            <div class="row">
              <span>当前资金费率</span>
              <span>${formatRate(payload.item.metrics.left_funding_rate)} / ${formatRate(payload.item.metrics.right_funding_rate)}</span>
            </div>
            <div class="row">
              <span>费率差</span>
              <span>${formatNumber(payload.item.metrics.funding_diff_bps)} bps</span>
            </div>
            <div class="row">
              <span>指数价格</span>
              <span>${formatPrice(payload.item.metrics.left_index_price)} / ${formatPrice(payload.item.metrics.right_index_price)}</span>
            </div>
            <div class="row">
              <span>指数价差</span>
              <span>${formatNumber(payload.item.metrics.index_diff_bps)} bps</span>
            </div>
            <div class="row">
              <span>24h 成交额</span>
              <span>${formatVolume(payload.item.metrics.left_volume_24h)} / ${formatVolume(payload.item.metrics.right_volume_24h)}</span>
            </div>
          </div>
        </div>
        <div class="detail-section">
          <h3>最近价差点位</h3>
          <div class="list">
            ${payload.spread_history.slice(-8).reverse().map(point => `
              <div class="row">
                <span>${formatTime(point.ts_ms)}</span>
                <span>开 ${formatNumber(point.open_spread_bps)} / 平 ${formatNumber(point.close_spread_bps)}</span>
              </div>
            `).join("") || '<div class="muted">暂无历史数据</div>'}
          </div>
        </div>
        <div class="detail-section">
          <h3>左腿费率结算</h3>
          <div class="list">
            ${payload.left_funding_history.slice(-6).reverse().map(item => `
              <div class="row">
                <span>${formatTime(item.settled_at_ms)}</span>
                <span>${formatRate(item.rate)}</span>
              </div>
            `).join("") || '<div class="muted">暂无结算记录</div>'}
          </div>
        </div>
        <div class="detail-section">
          <h3>右腿费率结算</h3>
          <div class="list">
            ${payload.right_funding_history.slice(-6).reverse().map(item => `
              <div class="row">
                <span>${formatTime(item.settled_at_ms)}</span>
                <span>${formatRate(item.rate)}</span>
              </div>
            `).join("") || '<div class="muted">暂无结算记录</div>'}
          </div>
        </div>
      `;
    }

    loadHealth();
    loadPairs();
    setInterval(loadHealth, 15000);
    setInterval(loadPairs, 5000);
  </script>
</body>
</html>
"##;
