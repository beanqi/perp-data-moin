use async_trait::async_trait;
use tokio::task::JoinHandle;

use crate::domain::{DiscoveredMarket, ExchangeId};
use crate::exchange::event::EventSender;

#[async_trait]
pub trait ExchangeAdapter: Send + Sync {
    fn id(&self) -> ExchangeId;
    async fn discover_markets(&self) -> Result<Vec<DiscoveredMarket>, String>;
    async fn spawn(&self, tx: EventSender) -> Result<Vec<JoinHandle<()>>, String>;
}

pub struct NoopExchangeAdapter {
    id: ExchangeId,
}

impl NoopExchangeAdapter {
    pub fn new(id: ExchangeId) -> Self {
        Self { id }
    }
}

#[async_trait]
impl ExchangeAdapter for NoopExchangeAdapter {
    fn id(&self) -> ExchangeId {
        self.id
    }

    async fn discover_markets(&self) -> Result<Vec<DiscoveredMarket>, String> {
        Ok(Vec::new())
    }

    async fn spawn(&self, _tx: EventSender) -> Result<Vec<JoinHandle<()>>, String> {
        Ok(Vec::new())
    }
}
