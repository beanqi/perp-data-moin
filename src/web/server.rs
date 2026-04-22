use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use tokio::sync::{mpsc, oneshot, watch};

use crate::error::AppError;
use crate::service::aggregator::AggregatorQuery;
use crate::store::view_store::{PairDetailView, SummaryView};

#[derive(Clone)]
pub struct WebState {
    pub summary_rx: watch::Receiver<Arc<SummaryView>>,
    pub query_tx: mpsc::Sender<AggregatorQuery>,
}

impl WebState {
    pub fn new(
        summary_rx: watch::Receiver<Arc<SummaryView>>,
        query_tx: mpsc::Sender<AggregatorQuery>,
    ) -> Self {
        Self { summary_rx, query_tx }
    }

    pub fn summary(&self) -> Arc<SummaryView> {
        self.summary_rx.borrow().clone()
    }

    pub async fn pair_detail(&self, pair_id: String) -> PairDetailView {
        let (respond_to, rx) = oneshot::channel();
        let query = AggregatorQuery::PairDetail { pair_id, respond_to };

        if self.query_tx.send(query).await.is_err() {
            return PairDetailView::default();
        }

        rx.await.unwrap_or_default()
    }
}

pub async fn serve(bind: String, state: WebState) -> Result<(), AppError> {
    let router = Router::new()
        .merge(crate::web::routes::router())
        .with_state(state);

    let addr: SocketAddr = bind
        .parse()
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidInput, err))?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router).await?;
    Ok(())
}
