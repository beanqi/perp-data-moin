use serde::Serialize;

use crate::domain::ExchangeId;
use crate::domain::{FundingSettlementRecord, MonitorPairView, SpreadPoint};

#[derive(Debug, Serialize)]
pub struct PairListResponse {
    pub items: Vec<MonitorPairView>,
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub exchanges: Vec<ExchangeHealthView>,
    pub enabled_exchanges: Vec<ExchangeId>,
    pub pair_count: usize,
}

#[derive(Debug, Serialize)]
pub struct ExchangeHealthView {
    pub exchange: ExchangeId,
    pub ok: bool,
    pub message: Option<String>,
    pub updated_at_ms: i64,
}

#[derive(Debug, Serialize)]
pub struct PairDetailResponse {
    pub item: Option<MonitorPairView>,
    pub spread_history: Vec<SpreadPoint>,
    pub left_funding_history: Vec<FundingSettlementRecord>,
    pub right_funding_history: Vec<FundingSettlementRecord>,
}
