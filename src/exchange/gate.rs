use crate::domain::ExchangeId;
use crate::exchange::adapter::{ExchangeAdapter, NoopExchangeAdapter};

pub fn build() -> Box<dyn ExchangeAdapter> {
    Box::new(NoopExchangeAdapter::new(ExchangeId::Gate))
}
