use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Ord, PartialOrd)]
#[serde(rename_all = "lowercase")]
pub enum ExchangeId {
    Binance,
    Bybit,
    Gate,
    Kucoin,
    Bitget,
    Okx,
    Huobi,
    Aster,
}

impl Display for ExchangeId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Binance => "binance",
            Self::Bybit => "bybit",
            Self::Gate => "gate",
            Self::Kucoin => "kucoin",
            Self::Bitget => "bitget",
            Self::Okx => "okx",
            Self::Huobi => "huobi",
            Self::Aster => "aster",
        };
        f.write_str(value)
    }
}
