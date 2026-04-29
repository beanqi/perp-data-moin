use std::collections::{HashMap, VecDeque};

use crate::domain::{FundingSettlementRecord, MarketKey, SpreadPoint, SpreadWindowStats};

const SPREAD_WINDOW_MS: i64 = 30 * 60 * 1000;
const FUNDING_WINDOW_MS: i64 = 24 * 60 * 60 * 1000;

#[derive(Debug, Default)]
pub struct HistoryStore {
    spreads: HashMap<String, VecDeque<SpreadPoint>>,
    funding: HashMap<MarketKey, VecDeque<FundingSettlementRecord>>,
}

impl HistoryStore {
    pub fn record_spread(
        &mut self,
        pair_id: String,
        left_buy_right_sell_spread_bps: f64,
        left_buy_right_sell_close_spread_bps: f64,
        right_buy_left_sell_spread_bps: f64,
        right_buy_left_sell_close_spread_bps: f64,
        open_spread_bps: f64,
        close_spread_bps: f64,
        ts_ms: i64,
    ) -> SpreadWindowStats {
        let queue = self.spreads.entry(pair_id).or_default();
        queue.push_back(SpreadPoint {
            ts_ms,
            left_buy_right_sell_spread_bps,
            left_buy_right_sell_close_spread_bps,
            right_buy_left_sell_spread_bps,
            right_buy_left_sell_close_spread_bps,
            open_spread_bps,
            close_spread_bps,
        });
        prune_queue(queue, ts_ms - SPREAD_WINDOW_MS, |item| item.ts_ms);
        spread_window_stats(queue)
    }

    pub fn record_funding(&mut self, record: FundingSettlementRecord) {
        let queue = self.funding.entry(record.market_key.clone()).or_default();
        let settled_at_ms = record.settled_at_ms;
        if queue.iter().any(|item| item.settled_at_ms == settled_at_ms) {
            return;
        }
        queue.push_back(record);
        prune_queue(queue, settled_at_ms - FUNDING_WINDOW_MS, |item| {
            item.settled_at_ms
        });
    }

    pub fn spread_points(&self, pair_id: &str) -> Vec<SpreadPoint> {
        self.spreads
            .get(pair_id)
            .map(|points| points.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn spread_window_stats(&self, pair_id: &str) -> SpreadWindowStats {
        self.spreads
            .get(pair_id)
            .map(spread_window_stats)
            .unwrap_or_default()
    }

    pub fn funding_records(&self, market_key: &MarketKey) -> Vec<FundingSettlementRecord> {
        self.funding
            .get(market_key)
            .map(|records| records.iter().cloned().collect())
            .unwrap_or_default()
    }
}

fn spread_window_stats(points: &VecDeque<SpreadPoint>) -> SpreadWindowStats {
    let mut stats = SpreadWindowStats::default();
    for point in points {
        update_max(
            &mut stats.left_buy_right_sell_open_spread_max_bps,
            point.left_buy_right_sell_spread_bps,
        );
        update_min(
            &mut stats.left_buy_right_sell_close_spread_min_bps,
            point.left_buy_right_sell_close_spread_bps,
        );
        update_max(
            &mut stats.right_buy_left_sell_open_spread_max_bps,
            point.right_buy_left_sell_spread_bps,
        );
        update_min(
            &mut stats.right_buy_left_sell_close_spread_min_bps,
            point.right_buy_left_sell_close_spread_bps,
        );
    }
    stats
}

fn update_max(target: &mut Option<f64>, value: f64) {
    if !value.is_finite() {
        return;
    }
    if target.map(|current| value > current).unwrap_or(true) {
        *target = Some(value);
    }
}

fn update_min(target: &mut Option<f64>, value: f64) {
    if !value.is_finite() {
        return;
    }
    if target.map(|current| value < current).unwrap_or(true) {
        *target = Some(value);
    }
}

fn prune_queue<T, F>(queue: &mut VecDeque<T>, cutoff_ms: i64, ts_getter: F)
where
    F: Fn(&T) -> i64,
{
    while queue
        .front()
        .map(|item| ts_getter(item) < cutoff_ms)
        .unwrap_or(false)
    {
        queue.pop_front();
    }
}
