use std::collections::HashMap;

use crate::domain::{MarketKey, MarketSnapshot};

#[derive(Debug, Default)]
pub struct SnapshotStore {
    markets: HashMap<MarketKey, MarketSnapshot>,
}

impl SnapshotStore {
    pub fn get(&self, key: &MarketKey) -> Option<&MarketSnapshot> {
        self.markets.get(key)
    }

    pub fn update_book(&mut self, key: &MarketKey, bid: f64, ask: f64, ts_ms: i64) {
        let entry = self.markets.entry(key.clone()).or_default();
        entry.bid = Some(bid);
        entry.ask = Some(ask);
        entry.updated_at_ms = ts_ms;
    }

    pub fn update_funding_rate(
        &mut self,
        key: &MarketKey,
        rate: f64,
        next_settle_ms: Option<i64>,
        ts_ms: i64,
    ) {
        let entry = self.markets.entry(key.clone()).or_default();
        entry.funding_rate = Some(rate);
        entry.next_funding_at_ms = next_settle_ms;
        entry.updated_at_ms = ts_ms;
    }

    pub fn update_last_funding(&mut self, key: &MarketKey, rate: f64, settle_ms: i64) {
        let entry = self.markets.entry(key.clone()).or_default();
        entry.last_funding_rate = Some(rate);
        entry.last_funding_at_ms = Some(settle_ms);
        entry.updated_at_ms = settle_ms;
    }

    pub fn update_index_price(&mut self, key: &MarketKey, price: f64, ts_ms: i64) {
        let entry = self.markets.entry(key.clone()).or_default();
        entry.index_price = Some(price);
        entry.updated_at_ms = ts_ms;
    }

    pub fn update_volume(&mut self, key: &MarketKey, volume_24h: f64, ts_ms: i64) {
        let entry = self.markets.entry(key.clone()).or_default();
        entry.volume_24h = Some(volume_24h);
        entry.updated_at_ms = ts_ms;
    }
}
