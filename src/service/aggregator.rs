use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use crate::config::DisplaySort;
use crate::domain::{
    FundingSettlementRecord, MarketKey, MarketSnapshot, MonitorMetrics, MonitorPair,
    MonitorPairView,
};
use crate::exchange::event::ExchangeEvent;
use crate::store::snapshot_store::SnapshotStore;
use crate::store::view_store::{ExchangeStatus, PairDetailView, RuntimeState, SummaryView};

const SNAPSHOT_INTERVAL_MS: u64 = 250;

#[derive(Debug)]
pub enum AggregatorQuery {
    PairDetail {
        pair_id: String,
        respond_to: oneshot::Sender<PairDetailView>,
    },
}

pub fn spawn_aggregator(
    rx: mpsc::Receiver<ExchangeEvent>,
    pairs: Vec<MonitorPair>,
    sort_by: DisplaySort,
) -> (
    watch::Receiver<Arc<SummaryView>>,
    mpsc::Sender<AggregatorQuery>,
    JoinHandle<()>,
) {
    let state = RuntimeState::new(pairs);
    let initial_view = state.build_summary(sort_by);
    let (summary_tx, summary_rx) = watch::channel(initial_view);
    let (query_tx, query_rx) = mpsc::channel(128);

    let handle = tokio::spawn(async move {
        run_aggregator(rx, query_rx, summary_tx, state, sort_by).await;
    });

    (summary_rx, query_tx, handle)
}

async fn run_aggregator(
    mut rx: mpsc::Receiver<ExchangeEvent>,
    mut query_rx: mpsc::Receiver<AggregatorQuery>,
    tx: watch::Sender<Arc<SummaryView>>,
    mut state: RuntimeState,
    sort_by: DisplaySort,
) {
    let mut snapshots = SnapshotStore::default();
    let mut dirty_pairs = HashSet::<String>::new();
    let mut snapshot_dirty = false;
    let mut ticker = tokio::time::interval(Duration::from_millis(SNAPSHOT_INTERVAL_MS));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            maybe_event = rx.recv() => {
                match maybe_event {
                    Some(event) => {
                        apply_event(event, &mut state, &mut snapshots, &mut dirty_pairs, &mut snapshot_dirty);

                        while let Ok(event) = rx.try_recv() {
                            apply_event(event, &mut state, &mut snapshots, &mut dirty_pairs, &mut snapshot_dirty);
                        }
                    }
                    None => {
                        publish_if_dirty(&mut state, &snapshots, &mut dirty_pairs, &mut snapshot_dirty, sort_by, &tx);
                        break;
                    }
                }
            }
            maybe_query = query_rx.recv() => {
                match maybe_query {
                    Some(query) => handle_query(query, &state),
                    None => {}
                }
            }
            _ = ticker.tick() => {
                publish_if_dirty(&mut state, &snapshots, &mut dirty_pairs, &mut snapshot_dirty, sort_by, &tx);
            }
        }
    }
}

fn handle_query(query: AggregatorQuery, state: &RuntimeState) {
    match query {
        AggregatorQuery::PairDetail {
            pair_id,
            respond_to,
        } => {
            let _ = respond_to.send(state.build_pair_detail(&pair_id));
        }
    }
}

fn apply_event(
    event: ExchangeEvent,
    state: &mut RuntimeState,
    snapshots: &mut SnapshotStore,
    dirty_pairs: &mut HashSet<String>,
    snapshot_dirty: &mut bool,
) {
    tracing::info!(?event, "received exchange event");
    match event {
        ExchangeEvent::TopOfBook {
            market,
            bid,
            ask,
            ts_ms,
        } => {
            snapshots.update_book(&market.key(), bid, ask, ts_ms);
            mark_market_dirty(state, &market.key(), dirty_pairs, snapshot_dirty);
        }
        ExchangeEvent::FundingRate {
            market,
            rate,
            next_settle_ms,
            ts_ms,
        } => {
            snapshots.update_funding_rate(&market.key(), rate, next_settle_ms, ts_ms);
            mark_market_dirty(state, &market.key(), dirty_pairs, snapshot_dirty);
        }
        ExchangeEvent::FundingSettlement {
            market,
            canonical_symbol,
            rate,
            settle_ms,
        } => {
            snapshots.update_last_funding(&market.key(), rate, settle_ms);
            state.history.record_funding(FundingSettlementRecord {
                exchange: market.exchange,
                market_key: market.key(),
                canonical_symbol,
                settled_at_ms: settle_ms,
                rate,
            });
            mark_market_dirty(state, &market.key(), dirty_pairs, snapshot_dirty);
        }
        ExchangeEvent::IndexPrice {
            market,
            price,
            ts_ms,
        } => {
            snapshots.update_index_price(&market.key(), price, ts_ms);
            mark_market_dirty(state, &market.key(), dirty_pairs, snapshot_dirty);
        }
        ExchangeEvent::Volume24h {
            market,
            value,
            ts_ms,
        } => {
            snapshots.update_volume(&market.key(), value, ts_ms);
            mark_market_dirty(state, &market.key(), dirty_pairs, snapshot_dirty);
        }
        ExchangeEvent::Health {
            exchange,
            ok,
            message,
            ts_ms,
        } => {
            state.exchange_status.insert(
                exchange,
                ExchangeStatus {
                    ok,
                    message,
                    updated_at_ms: ts_ms,
                },
            );
            *snapshot_dirty = true;
        }
    }
}

fn mark_market_dirty(
    state: &RuntimeState,
    market_key: &MarketKey,
    dirty_pairs: &mut HashSet<String>,
    snapshot_dirty: &mut bool,
) {
    dirty_pairs.extend(state.pair_ids_for_market(market_key).iter().cloned());
    *snapshot_dirty = true;
}

fn publish_if_dirty(
    state: &mut RuntimeState,
    snapshots: &SnapshotStore,
    dirty_pairs: &mut HashSet<String>,
    snapshot_dirty: &mut bool,
    sort_by: DisplaySort,
    tx: &watch::Sender<Arc<SummaryView>>,
) {
    if !*snapshot_dirty {
        return;
    }

    for pair_id in dirty_pairs.drain() {
        if let Some(pair) = state.pair_definition(&pair_id).cloned() {
            let view = compute_pair_view(&pair, snapshots);
            if let (Some(open_spread_bps), Some(close_spread_bps)) =
                (view.metrics.open_spread_bps, view.metrics.close_spread_bps)
            {
                state.history.record_spread(
                    pair_id.clone(),
                    open_spread_bps,
                    close_spread_bps,
                    view.metrics.updated_at_ms,
                );
            }
            state.update_pair_view(&pair_id, view);
        }
    }

    let _ = tx.send(state.build_summary(sort_by));
    *snapshot_dirty = false;
}

fn compute_pair_view(pair: &MonitorPair, snapshots: &SnapshotStore) -> MonitorPairView {
    let left_snapshot = snapshots.get(&pair.left.key()).cloned();
    let right_snapshot = snapshots.get(&pair.right.key()).cloned();
    let updated_at_ms = max_snapshot_time(&left_snapshot, &right_snapshot).unwrap_or_else(now_ms);

    let metrics = MonitorMetrics {
        open_spread_bps: spread_bps(
            left_snapshot.as_ref().and_then(|snapshot| snapshot.ask),
            right_snapshot.as_ref().and_then(|snapshot| snapshot.bid),
        ),
        close_spread_bps: spread_bps(
            left_snapshot.as_ref().and_then(|snapshot| snapshot.bid),
            right_snapshot.as_ref().and_then(|snapshot| snapshot.ask),
        ),
        left_funding_rate: left_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.funding_rate),
        right_funding_rate: right_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.funding_rate),
        funding_diff_bps: rate_diff_bps(
            left_snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.funding_rate),
            right_snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.funding_rate),
        ),
        left_index_price: left_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.index_price),
        right_index_price: right_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.index_price),
        index_diff_bps: price_diff_bps(
            left_snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.index_price),
            right_snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.index_price),
        ),
        left_volume_24h: left_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.volume_24h),
        right_volume_24h: right_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.volume_24h),
        updated_at_ms,
    };

    MonitorPairView {
        pair: pair.clone(),
        metrics,
        left_snapshot,
        right_snapshot,
    }
}

fn spread_bps(entry_price: Option<f64>, exit_price: Option<f64>) -> Option<f64> {
    let entry_price = entry_price?;
    let exit_price = exit_price?;
    if entry_price <= 0.0 {
        return None;
    }
    Some((exit_price - entry_price) / entry_price * 10_000.0)
}

fn rate_diff_bps(left: Option<f64>, right: Option<f64>) -> Option<f64> {
    let left = left?;
    let right = right?;
    Some((right - left) * 10_000.0)
}

fn price_diff_bps(left: Option<f64>, right: Option<f64>) -> Option<f64> {
    let left = left?;
    let right = right?;
    if left.abs() < f64::EPSILON {
        return Some((right - left) * 10_000.0);
    }
    Some((right - left) / left.abs() * 10_000.0)
}

fn max_snapshot_time(left: &Option<MarketSnapshot>, right: &Option<MarketSnapshot>) -> Option<i64> {
    match (left.as_ref(), right.as_ref()) {
        (Some(left), Some(right)) => Some(left.updated_at_ms.max(right.updated_at_ms)),
        (Some(left), None) => Some(left.updated_at_ms),
        (None, Some(right)) => Some(right.updated_at_ms),
        (None, None) => None,
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis() as i64)
        .unwrap_or_default()
}
