use serde::{Deserialize, Serialize};

use crate::domain::{ExchangeId, MarketKey};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpreadPoint {
    pub ts_ms: i64,
    pub open_spread_bps: f64,
    pub close_spread_bps: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FundingSettlementRecord {
    pub exchange: ExchangeId,
    pub market_key: MarketKey,
    pub canonical_symbol: String,
    pub settled_at_ms: i64,
    pub rate: f64,
}
