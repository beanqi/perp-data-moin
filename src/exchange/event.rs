use tokio::sync::mpsc;

use crate::domain::{ExchangeId, MarketRef};

pub type EventSender = mpsc::Sender<ExchangeEvent>;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum ExchangeEvent {
    TopOfBook {
        market: MarketRef,
        bid: f64,
        ask: f64,
        ts_ms: i64,
    },
    FundingRate {
        market: MarketRef,
        rate: f64,
        next_settle_ms: Option<i64>,
        ts_ms: i64,
    },
    FundingSettlement {
        market: MarketRef,
        canonical_symbol: String,
        rate: f64,
        settle_ms: i64,
    },
    IndexPrice {
        market: MarketRef,
        price: f64,
        ts_ms: i64,
    },
    Volume24h {
        market: MarketRef,
        value: f64,
        ts_ms: i64,
    },
    Health {
        exchange: ExchangeId,
        ok: bool,
        message: Option<String>,
        ts_ms: i64,
    },
}
