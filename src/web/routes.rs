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

const INDEX_HTML: &str = include_str!("index.html");
