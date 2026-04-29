use serde::{Deserialize, Serialize};

use crate::domain::{ExchangeId, MarketKey};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpreadPoint {
    pub ts_ms: i64,
    pub left_buy_right_sell_spread_bps: f64,
    pub left_buy_right_sell_close_spread_bps: f64,
    pub right_buy_left_sell_spread_bps: f64,
    pub right_buy_left_sell_close_spread_bps: f64,
    pub open_spread_bps: f64,
    pub close_spread_bps: f64,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct SpreadWindowStats {
    // 最近 30 分钟窗口内，用于列表快速扫机会的方向极值。
    pub left_buy_right_sell_open_spread_max_bps: Option<f64>,
    pub left_buy_right_sell_close_spread_min_bps: Option<f64>,
    pub right_buy_left_sell_open_spread_max_bps: Option<f64>,
    pub right_buy_left_sell_close_spread_min_bps: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FundingSettlementRecord {
    pub exchange: ExchangeId,
    pub market_key: MarketKey,
    pub canonical_symbol: String,
    pub settled_at_ms: i64,
    pub rate: f64,
}
