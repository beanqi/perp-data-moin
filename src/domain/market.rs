use serde::{Deserialize, Serialize};

use crate::domain::{ExchangeId, InstrumentKey, MarketKind};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MarketKey {
    pub exchange: ExchangeId,
    pub market_kind: MarketKind,
    pub market_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketRef {
    pub exchange: ExchangeId,
    pub market_id: String,
    pub raw_symbol: String,
    pub instrument: InstrumentKey,
}

impl MarketRef {
    pub fn key(&self) -> MarketKey {
        MarketKey {
            exchange: self.exchange,
            market_kind: self.instrument.market_kind,
            market_id: self.market_id.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveredMarket {
    pub exchange: ExchangeId,
    pub market_id: String,
    pub raw_symbol: String,
    pub market_kind: MarketKind,
}
