use serde::{Deserialize, Serialize};

use crate::domain::MarketRef;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PairKind {
    SpotPerp,
    PerpPerp,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitorPair {
    pub pair_id: String,
    pub canonical_symbol: String,
    pub left: MarketRef,
    pub right: MarketRef,
    pub kind: PairKind,
}

impl MonitorPair {
    pub fn new(
        canonical_symbol: String,
        left: MarketRef,
        right: MarketRef,
        kind: PairKind,
    ) -> Self {
        let pair_id = format!(
            "{}:{}:{}:{}:{}:{}:{}",
            canonical_symbol.replace('/', "_"),
            left.exchange,
            left.instrument.market_kind,
            left.market_id,
            right.exchange,
            right.instrument.market_kind,
            right.market_id
        );

        Self {
            pair_id,
            canonical_symbol,
            left,
            right,
            kind,
        }
    }
}
