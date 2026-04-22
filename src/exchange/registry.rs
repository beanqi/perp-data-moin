use crate::exchange::adapter::ExchangeAdapter;

pub fn build_adapters() -> Vec<Box<dyn ExchangeAdapter>> {
    vec![
        crate::exchange::binance::build(),
        crate::exchange::bybit::build(),
        crate::exchange::gate::build(),
        crate::exchange::kucoin::build(),
        crate::exchange::bitget::build(),
        crate::exchange::okx::build(),
        crate::exchange::huobi::build(),
        crate::exchange::aster::build(),
    ]
}
