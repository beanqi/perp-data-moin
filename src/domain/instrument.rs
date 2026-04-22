use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Ord, PartialOrd)]
#[serde(rename_all = "lowercase")]
pub enum MarketKind {
    Spot,
    Perp,
}

impl Display for MarketKind {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Spot => f.write_str("spot"),
            Self::Perp => f.write_str("perp"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InstrumentKey {
    pub base: String,
    pub quote: String,
    pub canonical_symbol: String,
    pub market_kind: MarketKind,
}

impl InstrumentKey {
    pub fn new(base: impl Into<String>, quote: impl Into<String>, market_kind: MarketKind) -> Self {
        let base = base.into();
        let quote = quote.into();
        let canonical_symbol = format!("{base}/{quote}");
        Self {
            base,
            quote,
            canonical_symbol,
            market_kind,
        }
    }
}
