use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MarketSnapshot {
    pub bid: Option<f64>,
    pub ask: Option<f64>,
    pub funding_rate: Option<f64>,
    pub next_funding_at_ms: Option<i64>,
    pub last_funding_rate: Option<f64>,
    pub last_funding_at_ms: Option<i64>,
    pub index_price: Option<f64>,
    pub volume_24h: Option<f64>,
    pub updated_at_ms: i64,
}
