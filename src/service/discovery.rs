use tracing::warn;

use crate::domain::MarketRef;
use crate::exchange::adapter::ExchangeAdapter;
use crate::service::aligner::SymbolAligner;

#[derive(Debug, Default)]
pub struct DiscoverySummary {
    pub markets: Vec<MarketRef>,
    pub warnings: Vec<String>,
}

pub async fn discover_markets(
    adapters: &[Box<dyn ExchangeAdapter>],
    aligner: &SymbolAligner,
) -> DiscoverySummary {
    let mut summary = DiscoverySummary::default();

    for adapter in adapters {
        match adapter.discover_markets().await {
            Ok(markets) => {
                for market in markets {
                    match aligner.align(&market) {
                        Some(aligned) => summary.markets.push(aligned),
                        None => {
                            let message = format!(
                                "failed to align symbol from {}: {}",
                                market.exchange, market.raw_symbol
                            );
                            warn!("{message}");
                            summary.warnings.push(message);
                        }
                    }
                }
            }
            Err(err) => {
                let message = format!("failed to discover markets from {}: {err}", adapter.id());
                warn!("{message}");
                summary.warnings.push(message);
            }
        }
    }

    summary
}
