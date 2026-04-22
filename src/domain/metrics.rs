use serde::{Deserialize, Serialize};

use crate::domain::{MarketSnapshot, MonitorPair};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MonitorMetrics {
    pub open_spread_bps: Option<f64>,
    pub close_spread_bps: Option<f64>,
    pub left_funding_rate: Option<f64>,
    pub right_funding_rate: Option<f64>,
    pub funding_diff_bps: Option<f64>,
    pub left_index_price: Option<f64>,
    pub right_index_price: Option<f64>,
    pub index_diff_bps: Option<f64>,
    pub left_volume_24h: Option<f64>,
    pub right_volume_24h: Option<f64>,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitorPairView {
    pub pair: MonitorPair,
    pub metrics: MonitorMetrics,
    pub left_snapshot: Option<MarketSnapshot>,
    pub right_snapshot: Option<MarketSnapshot>,
}

impl MonitorPairView {
    pub fn empty(pair: MonitorPair) -> Self {
        Self {
            pair,
            metrics: MonitorMetrics::default(),
            left_snapshot: None,
            right_snapshot: None,
        }
    }
}
