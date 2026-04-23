use std::path::Path;

use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::config::AppConfig;
use crate::error::AppError;
use crate::exchange::registry::build_adapters;
use crate::service::aggregator::spawn_aggregator;
use crate::service::aligner::SymbolAligner;
use crate::service::discovery::discover_markets;
use crate::service::pair_builder::build_monitor_pairs;
use crate::web::{WebState, serve};

pub async fn run() -> Result<(), AppError> {
    let config = AppConfig::load_from_path(Path::new("config.toml"))?;
    let adapters: Vec<_> = build_adapters()
        .into_iter()
        .filter(|adapter| config.monitor.includes_exchange(adapter.id()))
        .collect();
    let enabled_exchanges: Vec<_> = adapters.iter().map(|adapter| adapter.id()).collect();
    let aligner = SymbolAligner::new();

    let discovery = discover_markets(&adapters, &aligner).await;
    for warning in &discovery.warnings {
        warn!("{warning}");
    }

    let pairs = build_monitor_pairs(&discovery.markets);
    info!(
        enabled_exchanges = ?enabled_exchanges,
        markets = discovery.markets.len(),
        pairs = pairs.len(),
        "application bootstrapped discovery graph"
    );

    let (tx, rx) = mpsc::channel(4096);
    let (summary_rx, query_tx, aggregator_handle) =
        spawn_aggregator(rx, pairs, config.display.sort_by);

    for adapter in &adapters {
        match adapter.spawn(tx.clone()).await {
            Ok(handles) => {
                info!(
                    exchange = %adapter.id(),
                    tasks = handles.len(),
                    "exchange adapter started"
                );
            }
            Err(err) => {
                warn!(exchange = %adapter.id(), error = %err, "exchange adapter failed to start")
            }
        }
    }
    drop(tx);

    let web_state = WebState::new(summary_rx, query_tx, enabled_exchanges);
    let web_future = serve(config.web.bind.clone(), web_state);
    tokio::pin!(web_future);

    tokio::select! {
        result = &mut web_future => {
            if let Err(err) = result {
                aggregator_handle.abort();
                return Err(err);
            }
        }
        _ = shutdown_signal() => {
            info!("shutdown signal received");
        }
    }

    aggregator_handle.abort();
    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
