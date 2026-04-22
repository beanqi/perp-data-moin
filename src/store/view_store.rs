use std::collections::HashMap;
use std::sync::Arc;

use crate::config::DisplaySort;
use crate::domain::{
    ExchangeId, FundingSettlementRecord, MarketKey, MonitorPair, MonitorPairView, SpreadPoint,
};
use crate::store::history_store::HistoryStore;

#[derive(Debug, Clone)]
pub struct ExchangeStatus {
    pub ok: bool,
    pub message: Option<String>,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Default)]
pub struct SummaryView {
    pair_views: Vec<MonitorPairView>,
    exchange_statuses: Vec<(ExchangeId, ExchangeStatus)>,
}

impl SummaryView {
    pub fn pair_views(&self) -> &[MonitorPairView] {
        &self.pair_views
    }

    pub fn exchange_statuses(&self) -> &[(ExchangeId, ExchangeStatus)] {
        &self.exchange_statuses
    }
}

#[derive(Debug, Clone, Default)]
pub struct PairDetailView {
    pub item: Option<MonitorPairView>,
    pub spread_history: Vec<SpreadPoint>,
    pub left_funding_history: Vec<FundingSettlementRecord>,
    pub right_funding_history: Vec<FundingSettlementRecord>,
}

#[derive(Debug)]
pub struct RuntimeState {
    pair_definitions: HashMap<String, MonitorPair>,
    pair_views: HashMap<String, MonitorPairView>,
    pair_order: Vec<String>,
    market_to_pairs: HashMap<MarketKey, Vec<String>>,
    pub history: HistoryStore,
    pub exchange_status: HashMap<ExchangeId, ExchangeStatus>,
}

impl RuntimeState {
    pub fn new(pairs: Vec<MonitorPair>) -> Self {
        let mut pair_definitions = HashMap::new();
        let mut pair_views = HashMap::new();
        let mut pair_order = Vec::new();
        let mut market_to_pairs: HashMap<MarketKey, Vec<String>> = HashMap::new();

        for pair in pairs {
            let pair_id = pair.pair_id.clone();
            market_to_pairs
                .entry(pair.left.key())
                .or_default()
                .push(pair_id.clone());
            market_to_pairs
                .entry(pair.right.key())
                .or_default()
                .push(pair_id.clone());
            pair_views.insert(pair_id.clone(), MonitorPairView::empty(pair.clone()));
            pair_definitions.insert(pair_id.clone(), pair);
            pair_order.push(pair_id);
        }

        Self {
            pair_definitions,
            pair_views,
            pair_order,
            market_to_pairs,
            history: HistoryStore::default(),
            exchange_status: HashMap::new(),
        }
    }

    pub fn pair_ids_for_market(&self, market_key: &MarketKey) -> &[String] {
        self.market_to_pairs
            .get(market_key)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn pair_definition(&self, pair_id: &str) -> Option<&MonitorPair> {
        self.pair_definitions.get(pair_id)
    }

    pub fn update_pair_view(&mut self, pair_id: &str, view: MonitorPairView) {
        self.pair_views.insert(pair_id.to_owned(), view);
    }

    pub fn build_summary(&self, sort_by: DisplaySort) -> Arc<SummaryView> {
        let mut pair_views = self
            .pair_order
            .iter()
            .filter_map(|pair_id| self.pair_views.get(pair_id).cloned())
            .collect::<Vec<_>>();

        sort_pair_views(&mut pair_views, sort_by);

        let mut exchange_statuses = self
            .exchange_status
            .iter()
            .map(|(exchange, status)| (*exchange, status.clone()))
            .collect::<Vec<_>>();
        exchange_statuses.sort_by_key(|(exchange, _)| *exchange);

        Arc::new(SummaryView {
            pair_views,
            exchange_statuses,
        })
    }

    pub fn build_pair_detail(&self, pair_id: &str) -> PairDetailView {
        let item = self.pair_views.get(pair_id).cloned();
        let spread_history = self.history.spread_points(pair_id);

        let (left_funding_history, right_funding_history) = item
            .as_ref()
            .map(|view| {
                (
                    self.history.funding_records(&view.pair.left.key()),
                    self.history.funding_records(&view.pair.right.key()),
                )
            })
            .unwrap_or_default();

        PairDetailView {
            item,
            spread_history,
            left_funding_history,
            right_funding_history,
        }
    }
}

fn sort_pair_views(views: &mut [MonitorPairView], sort_by: DisplaySort) {
    match sort_by {
        DisplaySort::OpenSpreadAbs => views.sort_by(|left, right| {
            let left_value = left
                .metrics
                .open_spread_bps
                .map(f64::abs)
                .unwrap_or_default();
            let right_value = right
                .metrics
                .open_spread_bps
                .map(f64::abs)
                .unwrap_or_default();
            right_value.total_cmp(&left_value)
        }),
        DisplaySort::UpdatedAt => views.sort_by(|left, right| {
            right
                .metrics
                .updated_at_ms
                .cmp(&left.metrics.updated_at_ms)
        }),
        DisplaySort::Symbol => {
            views.sort_by(|left, right| left.pair.canonical_symbol.cmp(&right.pair.canonical_symbol))
        }
    }
}
