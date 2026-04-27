use std::collections::HashMap;
use std::io::Read;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use flate2::read::GzDecoder;
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Deserializer, Serialize};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, warn};

use crate::domain::{DiscoveredMarket, ExchangeId, InstrumentKey, MarketKind, MarketRef};
use crate::exchange::adapter::ExchangeAdapter;
use crate::exchange::event::{EventSender, ExchangeEvent};

const SPOT_REST_BASE: &str = "https://api.huobi.pro";
const SPOT_WS_URL: &str = "wss://api.huobi.pro/ws";
const PERP_REST_BASE: &str = "https://api.hbdm.com";
const PERP_MARKET_WS_URL: &str = "wss://api.hbdm.com/linear-swap-ws";
const PERP_NOTIFY_WS_URL: &str = "wss://api.hbdm.com/linear-swap-notification";
const PERP_INDEX_WS_URL: &str = "wss://api.hbdm.com/ws_index";
const RECONNECT_DELAY: Duration = Duration::from_secs(5);
const SUBSCRIBE_DELAY: Duration = Duration::from_millis(10);
const FUNDING_SYNC_INTERVAL: Duration = Duration::from_secs(300);
const FUNDING_LOOKBACK_MS: i64 = 24 * 60 * 60 * 1000;
const FUNDING_HISTORY_PAGE_SIZE: usize = 32;

type SharedMarketMap = Arc<HashMap<String, MarketRef>>;
type SharedSymbolList = Arc<Vec<String>>;
type SharedNextSettleMap = Arc<RwLock<HashMap<String, i64>>>;
type WsStream =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

#[derive(Clone)]
pub struct HuobiAdapter {
    client: Client,
}

pub fn build() -> Box<dyn ExchangeAdapter> {
    Box::new(HuobiAdapter::new())
}

impl HuobiAdapter {
    fn new() -> Self {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(120))
            .user_agent("perp-data-moin/0.1.0")
            .build()
            .expect("huobi http client");

        Self { client }
    }

    async fn fetch_catalog(&self) -> Result<HuobiCatalog, String> {
        let spot_symbols = fetch_spot_symbols(&self.client).await?;
        let perp_contracts = fetch_perp_contracts(&self.client).await?;

        let mut discovered = Vec::new();
        let mut spot_market_ids = Vec::new();
        let mut perp_market_ids = Vec::new();
        let mut spot_markets = HashMap::new();
        let mut perp_markets = HashMap::new();
        let mut next_settles = HashMap::new();

        for symbol in spot_symbols {
            if symbol.state != "online" {
                continue;
            }
            if symbol
                .api_trading
                .as_deref()
                .is_some_and(|value| value != "enabled")
            {
                continue;
            }

            let market = build_market_ref(
                ExchangeId::Huobi,
                symbol.symbol.clone(),
                symbol.symbol,
                symbol.base_currency,
                symbol.quote_currency,
                MarketKind::Spot,
            );
            spot_market_ids.push(market.market_id.clone());
            discovered.push(to_discovered_market(&market));
            spot_markets.insert(market.market_id.clone(), market);
        }

        for contract in perp_contracts {
            if contract.contract_status != 1 {
                continue;
            }
            if contract.trade_partition != "USDT" {
                continue;
            }
            if contract
                .business_type
                .as_deref()
                .is_some_and(|value| value != "swap")
            {
                continue;
            }

            let market = build_market_ref(
                ExchangeId::Huobi,
                contract.contract_code.clone(),
                contract.contract_code,
                contract.symbol,
                contract.trade_partition,
                MarketKind::Perp,
            );
            if let Some(next_settle_ms) = contract.settlement_date {
                next_settles.insert(market.market_id.clone(), next_settle_ms);
            }
            perp_market_ids.push(market.market_id.clone());
            discovered.push(to_discovered_market(&market));
            perp_markets.insert(market.market_id.clone(), market);
        }

        Ok(HuobiCatalog {
            discovered,
            spot_market_ids: Arc::new(spot_market_ids),
            perp_market_ids: Arc::new(perp_market_ids),
            spot_markets: Arc::new(spot_markets),
            perp_markets: Arc::new(perp_markets),
            next_settles: Arc::new(RwLock::new(next_settles)),
        })
    }
}

#[async_trait]
impl ExchangeAdapter for HuobiAdapter {
    fn id(&self) -> ExchangeId {
        ExchangeId::Huobi
    }

    async fn discover_markets(&self) -> Result<Vec<DiscoveredMarket>, String> {
        Ok(self.fetch_catalog().await?.discovered)
    }

    async fn spawn(&self, tx: EventSender) -> Result<Vec<JoinHandle<()>>, String> {
        let catalog = self.fetch_catalog().await?;

        if let Err(err) = seed_initial_state(&self.client, &catalog, &tx).await {
            warn!(exchange = %ExchangeId::Huobi, error = %err, "huobi rest seed failed");
            send_health(&tx, false, Some(format!("huobi rest seed failed: {err}"))).await;
        }

        let mut handles = Vec::new();

        if !catalog.spot_markets.is_empty() {
            handles.push(tokio::spawn(run_spot_ticker_stream(
                catalog.spot_market_ids.clone(),
                catalog.spot_markets.clone(),
                tx.clone(),
            )));
        }

        if !catalog.perp_markets.is_empty() {
            handles.push(tokio::spawn(run_perp_detail_stream(
                catalog.perp_market_ids.clone(),
                catalog.perp_markets.clone(),
                tx.clone(),
            )));
            handles.push(tokio::spawn(run_perp_funding_stream(
                catalog.perp_markets.clone(),
                catalog.next_settles.clone(),
                tx.clone(),
            )));
            handles.push(tokio::spawn(run_perp_index_stream(
                catalog.perp_market_ids.clone(),
                catalog.perp_markets.clone(),
                tx.clone(),
            )));
            handles.push(tokio::spawn(run_funding_history_sync(
                self.client.clone(),
                catalog.perp_markets,
                tx,
            )));
        }

        Ok(handles)
    }
}

#[derive(Clone)]
struct HuobiCatalog {
    discovered: Vec<DiscoveredMarket>,
    spot_market_ids: SharedSymbolList,
    perp_market_ids: SharedSymbolList,
    spot_markets: SharedMarketMap,
    perp_markets: SharedMarketMap,
    next_settles: SharedNextSettleMap,
}

fn build_market_ref(
    exchange: ExchangeId,
    market_id: String,
    raw_symbol: String,
    base_asset: String,
    quote_asset: String,
    market_kind: MarketKind,
) -> MarketRef {
    MarketRef {
        exchange,
        market_id,
        raw_symbol,
        instrument: InstrumentKey::new(
            base_asset.to_ascii_uppercase(),
            quote_asset.to_ascii_uppercase(),
            market_kind,
        ),
    }
}

fn to_discovered_market(market: &MarketRef) -> DiscoveredMarket {
    DiscoveredMarket {
        exchange: market.exchange,
        market_id: market.market_id.clone(),
        raw_symbol: market.raw_symbol.clone(),
        market_kind: market.instrument.market_kind,
    }
}

async fn seed_initial_state(
    client: &Client,
    catalog: &HuobiCatalog,
    tx: &EventSender,
) -> Result<(), String> {
    let mut errors = Vec::new();

    if !catalog.spot_markets.is_empty()
        && let Err(err) = seed_spot_tickers(client, &catalog.spot_markets, tx).await
    {
        errors.push(err);
    }

    if !catalog.perp_markets.is_empty() {
        if let Err(err) = seed_perp_details(client, &catalog.perp_markets, tx).await {
            errors.push(err);
        }
        if let Err(err) = seed_perp_index_prices(client, &catalog.perp_markets, tx).await {
            errors.push(err);
        }
        if let Err(err) =
            seed_current_funding(client, &catalog.perp_markets, &catalog.next_settles, tx).await
        {
            errors.push(err);
        }
    }

    if errors.is_empty() {
        send_health(tx, true, Some("huobi rest snapshot initialized".to_owned())).await;
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

async fn seed_spot_tickers(
    client: &Client,
    markets: &SharedMarketMap,
    tx: &EventSender,
) -> Result<(), String> {
    let payload = fetch_huobi_response::<Vec<SpotTickerRest>>(
        client,
        &format!("{SPOT_REST_BASE}/market/tickers"),
    )
    .await?;
    let ts_ms = payload.ts.unwrap_or_else(now_ms);
    let tickers = require_huobi_data(payload, "spot market tickers")?;

    for ticker in tickers {
        let Some(market) = markets.get(&ticker.symbol) else {
            continue;
        };

        if let (Some(bid), Some(ask)) = (ticker.bid, ticker.ask) {
            let _ = tx
                .send(ExchangeEvent::TopOfBook {
                    market: market.clone(),
                    bid,
                    ask,
                    ts_ms,
                })
                .await;
        }

        if let Some(value) = ticker.vol {
            let _ = tx
                .send(ExchangeEvent::Volume24h {
                    market: market.clone(),
                    value,
                    ts_ms,
                })
                .await;
        }
    }

    Ok(())
}

async fn seed_perp_details(
    client: &Client,
    markets: &SharedMarketMap,
    tx: &EventSender,
) -> Result<(), String> {
    let payload = fetch_huobi_batch_merged(client).await?;
    let default_ts = payload.ts.unwrap_or_else(now_ms);

    if payload.status != "ok" {
        return Err(format!(
            "perp batch merged returned status {}{}",
            payload.status,
            format_huobi_error(payload.err_code, payload.err_msg.as_deref())
        ));
    }

    for ticker in payload.ticks {
        let Some(market) = markets.get(&ticker.contract_code) else {
            continue;
        };
        let ts_ms = ticker.ts.unwrap_or(default_ts);

        if let (Some(bid), Some(ask)) = (
            best_price(ticker.bid.as_deref()),
            best_price(ticker.ask.as_deref()),
        ) {
            let _ = tx
                .send(ExchangeEvent::TopOfBook {
                    market: market.clone(),
                    bid,
                    ask,
                    ts_ms,
                })
                .await;
        }

        if let Some(value) = ticker.trade_turnover {
            let _ = tx
                .send(ExchangeEvent::Volume24h {
                    market: market.clone(),
                    value,
                    ts_ms,
                })
                .await;
        }
    }

    Ok(())
}

async fn seed_perp_index_prices(
    client: &Client,
    markets: &SharedMarketMap,
    tx: &EventSender,
) -> Result<(), String> {
    sync_perp_index_prices(client, markets, tx).await
}

async fn seed_current_funding(
    client: &Client,
    markets: &SharedMarketMap,
    next_settles: &SharedNextSettleMap,
    tx: &EventSender,
) -> Result<(), String> {
    sync_current_funding(client, markets, next_settles, tx).await
}

async fn run_spot_ticker_stream(
    symbols: SharedSymbolList,
    markets: SharedMarketMap,
    tx: EventSender,
) {
    loop {
        match connect_async(SPOT_WS_URL).await {
            Ok((mut socket, _)) => {
                let topics = symbols
                    .iter()
                    .map(|symbol| format!("market.{symbol}.ticker"))
                    .collect::<Vec<_>>();

                if let Err(err) = subscribe_market_topics(&mut socket, topics).await {
                    send_health(
                        &tx,
                        false,
                        Some(format!("huobi spot ticker subscribe failed: {err}")),
                    )
                    .await;
                    sleep(RECONNECT_DELAY).await;
                    continue;
                }

                send_health(&tx, true, Some("huobi spot ticker connected".to_owned())).await;

                loop {
                    match socket.next().await {
                        Some(Ok(Message::Text(text))) => {
                            if let Err(err) = handle_spot_market_payload(
                                text.to_string(),
                                &mut socket,
                                &markets,
                                &tx,
                            )
                            .await
                            {
                                debug!(stream = "spot ticker", error = %err, "huobi websocket payload skipped");
                            }
                        }
                        Some(Ok(Message::Binary(binary))) => match decode_huobi_payload(&binary) {
                            Ok(payload) => {
                                if let Err(err) =
                                    handle_spot_market_payload(payload, &mut socket, &markets, &tx)
                                        .await
                                {
                                    debug!(stream = "spot ticker", error = %err, "huobi websocket payload skipped");
                                }
                            }
                            Err(err) => {
                                debug!(stream = "spot ticker", error = %err, "huobi websocket payload decode failed");
                            }
                        },
                        Some(Ok(Message::Ping(payload))) => {
                            if let Err(err) = socket.send(Message::Pong(payload)).await {
                                send_health(
                                    &tx,
                                    false,
                                    Some(format!("huobi spot ticker pong failed: {err}")),
                                )
                                .await;
                                break;
                            }
                        }
                        Some(Ok(Message::Pong(_))) => {}
                        Some(Ok(Message::Frame(_))) => {}
                        Some(Ok(Message::Close(frame))) => {
                            send_health(
                                &tx,
                                false,
                                Some(format!("huobi spot ticker closed: {frame:?}")),
                            )
                            .await;
                            break;
                        }
                        Some(Err(err)) => {
                            send_health(
                                &tx,
                                false,
                                Some(format!("huobi spot ticker read failed: {err}")),
                            )
                            .await;
                            break;
                        }
                        None => {
                            send_health(
                                &tx,
                                false,
                                Some("huobi spot ticker disconnected".to_owned()),
                            )
                            .await;
                            break;
                        }
                    }
                }
            }
            Err(err) => {
                send_health(
                    &tx,
                    false,
                    Some(format!("huobi spot ticker connect failed: {err}")),
                )
                .await;
            }
        }

        sleep(RECONNECT_DELAY).await;
    }
}

async fn run_perp_detail_stream(
    contract_codes: SharedSymbolList,
    markets: SharedMarketMap,
    tx: EventSender,
) {
    loop {
        match connect_async(PERP_MARKET_WS_URL).await {
            Ok((mut socket, _)) => {
                let topics = contract_codes
                    .iter()
                    .map(|contract_code| format!("market.{contract_code}.detail"))
                    .collect::<Vec<_>>();

                if let Err(err) = subscribe_market_topics(&mut socket, topics).await {
                    send_health(
                        &tx,
                        false,
                        Some(format!("huobi perp detail subscribe failed: {err}")),
                    )
                    .await;
                    sleep(RECONNECT_DELAY).await;
                    continue;
                }

                send_health(&tx, true, Some("huobi perp detail connected".to_owned())).await;

                loop {
                    match socket.next().await {
                        Some(Ok(Message::Text(text))) => {
                            if let Err(err) = handle_perp_detail_payload(
                                text.to_string(),
                                &mut socket,
                                &markets,
                                &tx,
                            )
                            .await
                            {
                                debug!(stream = "perp detail", error = %err, "huobi websocket payload skipped");
                            }
                        }
                        Some(Ok(Message::Binary(binary))) => match decode_huobi_payload(&binary) {
                            Ok(payload) => {
                                if let Err(err) =
                                    handle_perp_detail_payload(payload, &mut socket, &markets, &tx)
                                        .await
                                {
                                    debug!(stream = "perp detail", error = %err, "huobi websocket payload skipped");
                                }
                            }
                            Err(err) => {
                                debug!(stream = "perp detail", error = %err, "huobi websocket payload decode failed");
                            }
                        },
                        Some(Ok(Message::Ping(payload))) => {
                            if let Err(err) = socket.send(Message::Pong(payload)).await {
                                send_health(
                                    &tx,
                                    false,
                                    Some(format!("huobi perp detail pong failed: {err}")),
                                )
                                .await;
                                break;
                            }
                        }
                        Some(Ok(Message::Pong(_))) => {}
                        Some(Ok(Message::Frame(_))) => {}
                        Some(Ok(Message::Close(frame))) => {
                            send_health(
                                &tx,
                                false,
                                Some(format!("huobi perp detail closed: {frame:?}")),
                            )
                            .await;
                            break;
                        }
                        Some(Err(err)) => {
                            send_health(
                                &tx,
                                false,
                                Some(format!("huobi perp detail read failed: {err}")),
                            )
                            .await;
                            break;
                        }
                        None => {
                            send_health(
                                &tx,
                                false,
                                Some("huobi perp detail disconnected".to_owned()),
                            )
                            .await;
                            break;
                        }
                    }
                }
            }
            Err(err) => {
                send_health(
                    &tx,
                    false,
                    Some(format!("huobi perp detail connect failed: {err}")),
                )
                .await;
            }
        }

        sleep(RECONNECT_DELAY).await;
    }
}

async fn run_perp_funding_stream(
    markets: SharedMarketMap,
    next_settles: SharedNextSettleMap,
    tx: EventSender,
) {
    loop {
        match connect_async(PERP_NOTIFY_WS_URL).await {
            Ok((mut socket, _)) => {
                if let Err(err) = subscribe_notify_topic(&mut socket, "public.*.funding_rate").await
                {
                    send_health(
                        &tx,
                        false,
                        Some(format!("huobi funding subscribe failed: {err}")),
                    )
                    .await;
                    sleep(RECONNECT_DELAY).await;
                    continue;
                }

                send_health(&tx, true, Some("huobi funding connected".to_owned())).await;

                loop {
                    match socket.next().await {
                        Some(Ok(Message::Text(text))) => {
                            match handle_funding_payload(
                                text.to_string(),
                                &mut socket,
                                &markets,
                                &next_settles,
                                &tx,
                            )
                            .await
                            {
                                Ok(()) => {}
                                Err(err) => {
                                    if err.starts_with("control:") {
                                        send_health(
                                            &tx,
                                            false,
                                            Some(err.trim_start_matches("control:").to_owned()),
                                        )
                                        .await;
                                        break;
                                    }
                                    debug!(stream = "funding", error = %err, "huobi websocket payload skipped");
                                }
                            }
                        }
                        Some(Ok(Message::Binary(binary))) => match decode_huobi_payload(&binary) {
                            Ok(payload) => {
                                match handle_funding_payload(
                                    payload,
                                    &mut socket,
                                    &markets,
                                    &next_settles,
                                    &tx,
                                )
                                .await
                                {
                                    Ok(()) => {}
                                    Err(err) => {
                                        if err.starts_with("control:") {
                                            send_health(
                                                &tx,
                                                false,
                                                Some(err.trim_start_matches("control:").to_owned()),
                                            )
                                            .await;
                                            break;
                                        }
                                        debug!(stream = "funding", error = %err, "huobi websocket payload skipped");
                                    }
                                }
                            }
                            Err(err) => {
                                debug!(stream = "funding", error = %err, "huobi websocket payload decode failed");
                            }
                        },
                        Some(Ok(Message::Ping(payload))) => {
                            if let Err(err) = socket.send(Message::Pong(payload)).await {
                                send_health(
                                    &tx,
                                    false,
                                    Some(format!("huobi funding pong failed: {err}")),
                                )
                                .await;
                                break;
                            }
                        }
                        Some(Ok(Message::Pong(_))) => {}
                        Some(Ok(Message::Frame(_))) => {}
                        Some(Ok(Message::Close(frame))) => {
                            send_health(
                                &tx,
                                false,
                                Some(format!("huobi funding closed: {frame:?}")),
                            )
                            .await;
                            break;
                        }
                        Some(Err(err)) => {
                            send_health(
                                &tx,
                                false,
                                Some(format!("huobi funding read failed: {err}")),
                            )
                            .await;
                            break;
                        }
                        None => {
                            send_health(&tx, false, Some("huobi funding disconnected".to_owned()))
                                .await;
                            break;
                        }
                    }
                }
            }
            Err(err) => {
                send_health(
                    &tx,
                    false,
                    Some(format!("huobi funding connect failed: {err}")),
                )
                .await;
            }
        }

        sleep(RECONNECT_DELAY).await;
    }
}

async fn run_perp_index_stream(
    contract_codes: SharedSymbolList,
    markets: SharedMarketMap,
    tx: EventSender,
) {
    loop {
        match connect_async(PERP_INDEX_WS_URL).await {
            Ok((mut socket, _)) => {
                let topics = contract_codes
                    .iter()
                    .map(|contract_code| format!("market.{contract_code}.index.1min"))
                    .collect::<Vec<_>>();

                if let Err(err) = subscribe_market_topics(&mut socket, topics).await {
                    send_health(
                        &tx,
                        false,
                        Some(format!("huobi index subscribe failed: {err}")),
                    )
                    .await;
                    sleep(RECONNECT_DELAY).await;
                    continue;
                }

                send_health(&tx, true, Some("huobi index connected".to_owned())).await;

                loop {
                    match socket.next().await {
                        Some(Ok(Message::Text(text))) => {
                            if let Err(err) = handle_perp_index_payload(
                                text.to_string(),
                                &mut socket,
                                &markets,
                                &tx,
                            )
                            .await
                            {
                                debug!(stream = "index", error = %err, "huobi websocket payload skipped");
                            }
                        }
                        Some(Ok(Message::Binary(binary))) => match decode_huobi_payload(&binary) {
                            Ok(payload) => {
                                if let Err(err) =
                                    handle_perp_index_payload(payload, &mut socket, &markets, &tx)
                                        .await
                                {
                                    debug!(stream = "index", error = %err, "huobi websocket payload skipped");
                                }
                            }
                            Err(err) => {
                                debug!(stream = "index", error = %err, "huobi websocket payload decode failed");
                            }
                        },
                        Some(Ok(Message::Ping(payload))) => {
                            if let Err(err) = socket.send(Message::Pong(payload)).await {
                                send_health(
                                    &tx,
                                    false,
                                    Some(format!("huobi index pong failed: {err}")),
                                )
                                .await;
                                break;
                            }
                        }
                        Some(Ok(Message::Pong(_))) => {}
                        Some(Ok(Message::Frame(_))) => {}
                        Some(Ok(Message::Close(frame))) => {
                            send_health(&tx, false, Some(format!("huobi index closed: {frame:?}")))
                                .await;
                            break;
                        }
                        Some(Err(err)) => {
                            send_health(
                                &tx,
                                false,
                                Some(format!("huobi index read failed: {err}")),
                            )
                            .await;
                            break;
                        }
                        None => {
                            send_health(&tx, false, Some("huobi index disconnected".to_owned()))
                                .await;
                            break;
                        }
                    }
                }
            }
            Err(err) => {
                send_health(
                    &tx,
                    false,
                    Some(format!("huobi index connect failed: {err}")),
                )
                .await;
            }
        }

        sleep(RECONNECT_DELAY).await;
    }
}

async fn run_funding_history_sync(client: Client, markets: SharedMarketMap, tx: EventSender) {
    loop {
        match sync_funding_history(&client, &markets, &tx).await {
            Ok(()) => {
                send_health(&tx, true, Some("huobi funding history synced".to_owned())).await;
            }
            Err(err) => {
                send_health(
                    &tx,
                    false,
                    Some(format!("huobi funding history sync failed: {err}")),
                )
                .await;
            }
        }

        sleep(FUNDING_SYNC_INTERVAL).await;
    }
}

async fn sync_perp_index_prices(
    client: &Client,
    markets: &SharedMarketMap,
    tx: &EventSender,
) -> Result<(), String> {
    let payload = fetch_huobi_response::<Vec<PerpIndexPriceRest>>(
        client,
        &format!("{PERP_REST_BASE}/linear-swap-api/v1/swap_index"),
    )
    .await?;
    let default_ts = payload.ts.unwrap_or_else(now_ms);
    let prices = require_huobi_data(payload, "swap_index")?;

    for price in prices {
        let Some(market) = markets.get(&price.contract_code) else {
            continue;
        };
        let Some(index_price) = price.index_price else {
            continue;
        };

        let _ = tx
            .send(ExchangeEvent::IndexPrice {
                market: market.clone(),
                price: index_price,
                ts_ms: price.index_ts.unwrap_or(default_ts),
            })
            .await;
    }

    Ok(())
}

async fn sync_current_funding(
    client: &Client,
    markets: &SharedMarketMap,
    next_settles: &SharedNextSettleMap,
    tx: &EventSender,
) -> Result<(), String> {
    let payload = fetch_huobi_response::<Vec<FundingRateRest>>(
        client,
        &format!("{PERP_REST_BASE}/linear-swap-api/v1/swap_batch_funding_rate"),
    )
    .await?;
    let ts_ms = payload.ts.unwrap_or_else(now_ms);
    let updates = require_huobi_data(payload, "swap_batch_funding_rate")?;

    for update in updates {
        let Some(market) = markets.get(&update.contract_code) else {
            continue;
        };
        let Some(rate) = update.funding_rate else {
            continue;
        };

        let next_settle_ms = update
            .funding_time
            .or(update.next_funding_time)
            .or_else(|| {
                next_settles
                    .read()
                    .ok()
                    .and_then(|state| state.get(&update.contract_code).copied())
            });

        if let Some(next_settle_ms) = next_settle_ms
            && let Ok(mut state) = next_settles.write()
        {
            state.insert(update.contract_code.clone(), next_settle_ms);
        }

        let _ = tx
            .send(ExchangeEvent::FundingRate {
                market: market.clone(),
                rate,
                next_settle_ms,
                ts_ms,
            })
            .await;
    }

    Ok(())
}

async fn sync_funding_history(
    client: &Client,
    markets: &SharedMarketMap,
    tx: &EventSender,
) -> Result<(), String> {
    let cutoff_ms = now_ms() - FUNDING_LOOKBACK_MS;
    let mut errors = Vec::new();

    for market in markets.values() {
        match fetch_funding_history(client, &market.market_id).await {
            Ok(records) => {
                for record in records.into_iter().rev() {
                    let Some(settle_ms) = record.funding_time else {
                        continue;
                    };
                    if settle_ms < cutoff_ms {
                        continue;
                    }
                    let Some(rate) = record.funding_rate else {
                        continue;
                    };

                    let _ = tx
                        .send(ExchangeEvent::FundingSettlement {
                            market: market.clone(),
                            canonical_symbol: market.instrument.canonical_symbol.clone(),
                            rate,
                            settle_ms,
                        })
                        .await;
                }
            }
            Err(err) => {
                if errors.len() < 8 {
                    errors.push(format!("{}: {err}", market.market_id));
                }
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

async fn subscribe_market_topics<I>(socket: &mut WsStream, topics: I) -> Result<(), String>
where
    I: IntoIterator<Item = String>,
{
    for (index, topic) in topics.into_iter().enumerate() {
        let payload = serde_json::json!({
            "sub": topic,
            "id": format!("sub-{index}"),
        })
        .to_string();

        socket
            .send(Message::Text(payload.into()))
            .await
            .map_err(|err| format!("subscribe write failed: {err}"))?;

        sleep(SUBSCRIBE_DELAY).await;
    }

    Ok(())
}

async fn subscribe_notify_topic(socket: &mut WsStream, topic: &str) -> Result<(), String> {
    let payload = serde_json::json!({
        "op": "sub",
        "cid": "funding-rate",
        "topic": topic,
    })
    .to_string();

    socket
        .send(Message::Text(payload.into()))
        .await
        .map_err(|err| format!("subscribe write failed: {err}"))
}

async fn handle_spot_market_payload(
    payload: String,
    socket: &mut WsStream,
    markets: &SharedMarketMap,
    tx: &EventSender,
) -> Result<(), String> {
    if let Some(ping) = parse_market_ping(&payload) {
        socket
            .send(Message::Text(format!(r#"{{"pong":{ping}}}"#).into()))
            .await
            .map_err(|err| format!("spot pong failed: {err}"))?;
        return Ok(());
    }

    if is_market_control_payload(&payload) {
        return Ok(());
    }

    let message = serde_json::from_str::<SpotTickerWsMessage>(&payload)
        .map_err(|err| format!("decode spot ticker failed: {err}"))?;
    let Some(symbol) = parse_spot_symbol_from_channel(&message.ch) else {
        return Err(format!("unexpected spot channel {}", message.ch));
    };
    let Some(market) = markets.get(symbol) else {
        return Ok(());
    };
    let ts_ms = message.ts.unwrap_or_else(now_ms);

    if let (Some(bid), Some(ask)) = (message.tick.bid, message.tick.ask) {
        let _ = tx
            .send(ExchangeEvent::TopOfBook {
                market: market.clone(),
                bid,
                ask,
                ts_ms,
            })
            .await;
    }

    if let Some(value) = message.tick.vol {
        let _ = tx
            .send(ExchangeEvent::Volume24h {
                market: market.clone(),
                value,
                ts_ms,
            })
            .await;
    }

    Ok(())
}

async fn handle_perp_detail_payload(
    payload: String,
    socket: &mut WsStream,
    markets: &SharedMarketMap,
    tx: &EventSender,
) -> Result<(), String> {
    if let Some(ping) = parse_market_ping(&payload) {
        socket
            .send(Message::Text(format!(r#"{{"pong":{ping}}}"#).into()))
            .await
            .map_err(|err| format!("perp pong failed: {err}"))?;
        return Ok(());
    }

    if is_market_control_payload(&payload) {
        return Ok(());
    }

    let message = serde_json::from_str::<PerpDetailWsMessage>(&payload)
        .map_err(|err| format!("decode perp detail failed: {err}"))?;
    let Some(contract_code) = parse_perp_symbol_from_channel(&message.ch) else {
        return Err(format!("unexpected perp channel {}", message.ch));
    };
    let Some(market) = markets.get(contract_code) else {
        return Ok(());
    };
    let ts_ms = message.ts.or(message.tick.id).unwrap_or_else(now_ms);

    if let (Some(bid), Some(ask)) = (
        best_price(message.tick.bid.as_deref()),
        best_price(message.tick.ask.as_deref()),
    ) {
        let _ = tx
            .send(ExchangeEvent::TopOfBook {
                market: market.clone(),
                bid,
                ask,
                ts_ms,
            })
            .await;
    }

    if let Some(value) = message.tick.trade_turnover {
        let _ = tx
            .send(ExchangeEvent::Volume24h {
                market: market.clone(),
                value,
                ts_ms,
            })
            .await;
    }

    Ok(())
}

async fn handle_perp_index_payload(
    payload: String,
    socket: &mut WsStream,
    markets: &SharedMarketMap,
    tx: &EventSender,
) -> Result<(), String> {
    if let Some(ping) = parse_market_ping(&payload) {
        socket
            .send(Message::Text(format!(r#"{{"pong":{ping}}}"#).into()))
            .await
            .map_err(|err| format!("index pong failed: {err}"))?;
        return Ok(());
    }

    if is_market_control_payload(&payload) {
        return Ok(());
    }

    let message = serde_json::from_str::<PerpIndexWsMessage>(&payload)
        .map_err(|err| format!("decode perp index failed: {err}"))?;
    let Some(contract_code) = parse_perp_index_symbol_from_channel(&message.ch) else {
        return Err(format!("unexpected index channel {}", message.ch));
    };
    let Some(market) = markets.get(contract_code) else {
        return Ok(());
    };
    let Some(price) = message.tick.close else {
        return Ok(());
    };

    let _ = tx
        .send(ExchangeEvent::IndexPrice {
            market: market.clone(),
            price,
            ts_ms: message.ts.unwrap_or_else(now_ms),
        })
        .await;

    Ok(())
}

async fn handle_funding_payload(
    payload: String,
    socket: &mut WsStream,
    markets: &SharedMarketMap,
    next_settles: &SharedNextSettleMap,
    tx: &EventSender,
) -> Result<(), String> {
    if let Some(ts) = parse_notify_ping(&payload) {
        socket
            .send(Message::Text(
                serde_json::json!({
                    "op": "pong",
                    "ts": ts,
                })
                .to_string()
                .into(),
            ))
            .await
            .map_err(|err| format!("funding pong failed: {err}"))?;
        return Ok(());
    }

    if let Some(message) = parse_notify_control_message(&payload)? {
        return Err(format!("control:{message}"));
    }

    let message = serde_json::from_str::<FundingRateWsMessage>(&payload)
        .map_err(|err| format!("decode funding update failed: {err}"))?;

    for update in message.data {
        let Some(market) = markets.get(&update.contract_code) else {
            continue;
        };
        let Some(rate) = update.funding_rate else {
            continue;
        };

        let next_settle_ms = update
            .funding_time
            .or(update.next_funding_time)
            .or_else(|| {
                next_settles
                    .read()
                    .ok()
                    .and_then(|state| state.get(&update.contract_code).copied())
            });

        if let Some(next_settle_ms) = next_settle_ms
            && let Ok(mut state) = next_settles.write()
        {
            state.insert(update.contract_code.clone(), next_settle_ms);
        }

        let _ = tx
            .send(ExchangeEvent::FundingRate {
                market: market.clone(),
                rate,
                next_settle_ms,
                ts_ms: message.ts.unwrap_or_else(now_ms),
            })
            .await;
    }

    Ok(())
}

fn parse_market_ping(payload: &str) -> Option<i64> {
    serde_json::from_str::<MarketPing>(payload)
        .ok()
        .map(|message| message.ping)
}

fn parse_notify_ping(payload: &str) -> Option<i64> {
    serde_json::from_str::<NotifyPing>(payload)
        .ok()
        .filter(|message| message.op == "ping")
        .and_then(|message| message.ts.or(message.ping))
}

fn parse_notify_control_message(payload: &str) -> Result<Option<String>, String> {
    let message = match serde_json::from_str::<NotifyControlEnvelope>(payload) {
        Ok(message) => message,
        Err(_) => return Ok(None),
    };

    match message.op.as_deref() {
        Some("sub") | Some("pong") | Some("notify") => Ok(None),
        Some("error") | Some("close") => Ok(Some(format!(
            "huobi funding {}{}",
            message.op.unwrap_or_default(),
            format_huobi_error(message.err_code, message.err_msg.as_deref())
        ))),
        Some(other) => Ok(Some(format!("huobi funding control op {other}"))),
        None => {
            if let Some(err_code) = message.err_code {
                Ok(Some(format!(
                    "huobi funding err-code {err_code}{}",
                    message
                        .err_msg
                        .as_deref()
                        .map(|value| format!(": {value}"))
                        .unwrap_or_default()
                )))
            } else {
                Ok(None)
            }
        }
    }
}

fn is_market_control_payload(payload: &str) -> bool {
    payload.contains("\"subbed\"")
        || payload.contains("\"unsubbed\"")
        || payload.contains("\"pong\"")
        || (payload.contains("\"status\":\"ok\"") && !payload.contains("\"ch\""))
}

fn parse_spot_symbol_from_channel(channel: &str) -> Option<&str> {
    channel
        .strip_prefix("market.")
        .and_then(|value| value.strip_suffix(".ticker"))
}

fn parse_perp_symbol_from_channel(channel: &str) -> Option<&str> {
    channel
        .strip_prefix("market.")
        .and_then(|value| value.strip_suffix(".detail"))
}

fn parse_perp_index_symbol_from_channel(channel: &str) -> Option<&str> {
    channel
        .strip_prefix("market.")
        .and_then(|value| value.strip_suffix(".index.1min"))
}

fn decode_huobi_payload(payload: &[u8]) -> Result<String, String> {
    if let Ok(text) = std::str::from_utf8(payload) {
        return Ok(text.to_owned());
    }

    let mut decoder = GzDecoder::new(payload);
    let mut text = String::new();
    decoder
        .read_to_string(&mut text)
        .map_err(|err| format!("gzip decode failed: {err}"))?;
    Ok(text)
}

fn best_price(level: Option<&[f64]>) -> Option<f64> {
    level
        .and_then(|value| value.first().copied())
        .filter(|value| value.is_finite() && *value > 0.0)
}

async fn fetch_spot_symbols(client: &Client) -> Result<Vec<SpotSymbolRest>, String> {
    let payload = fetch_huobi_response::<Vec<SpotSymbolRest>>(
        client,
        &format!("{SPOT_REST_BASE}/v1/common/symbols"),
    )
    .await?;

    require_huobi_data(payload, "spot symbols")
}

async fn fetch_perp_contracts(client: &Client) -> Result<Vec<PerpContractRest>, String> {
    let payload = fetch_huobi_response::<Vec<PerpContractRest>>(
        client,
        &format!("{PERP_REST_BASE}/linear-swap-api/v1/swap_contract_info"),
    )
    .await?;

    require_huobi_data(payload, "swap_contract_info")
}

async fn fetch_huobi_batch_merged(client: &Client) -> Result<HuobiBatchMergedResponse, String> {
    fetch_json_query(
        client,
        &format!("{PERP_REST_BASE}/v2/linear-swap-ex/market/detail/batch_merged"),
        &BatchMergedQuery {
            business_type: "swap",
        },
    )
    .await
}

async fn fetch_funding_history(
    client: &Client,
    contract_code: &str,
) -> Result<Vec<FundingHistoryRest>, String> {
    let payload = fetch_huobi_response_query::<FundingHistoryPage, _>(
        client,
        &format!("{PERP_REST_BASE}/linear-swap-api/v1/swap_historical_funding_rate"),
        &FundingHistoryQuery {
            contract_code,
            page_size: FUNDING_HISTORY_PAGE_SIZE,
            page_index: 1,
        },
    )
    .await?;

    Ok(require_huobi_data(payload, "swap_historical_funding_rate")?.data)
}

async fn fetch_huobi_response<T>(client: &Client, url: &str) -> Result<HuobiApiResponse<T>, String>
where
    T: DeserializeOwned,
{
    fetch_json(client, url).await
}

async fn fetch_huobi_response_query<T, Q>(
    client: &Client,
    url: &str,
    query: &Q,
) -> Result<HuobiApiResponse<T>, String>
where
    T: DeserializeOwned,
    Q: Serialize + ?Sized,
{
    fetch_json_query(client, url, query).await
}

fn require_huobi_data<T>(payload: HuobiApiResponse<T>, label: &str) -> Result<T, String> {
    if payload.status != "ok" {
        return Err(format!(
            "{label} returned status {}{}",
            payload.status,
            format_huobi_error(payload.err_code, payload.err_msg.as_deref())
        ));
    }

    payload
        .data
        .ok_or_else(|| format!("{label} returned no data"))
}

fn format_huobi_error(err_code: Option<i64>, err_msg: Option<&str>) -> String {
    match (err_code, err_msg) {
        (Some(err_code), Some(err_msg)) => format!(" (err-code {err_code}: {err_msg})"),
        (Some(err_code), None) => format!(" (err-code {err_code})"),
        (None, Some(err_msg)) => format!(" ({err_msg})"),
        (None, None) => String::new(),
    }
}

async fn fetch_json<T>(client: &Client, url: &str) -> Result<T, String>
where
    T: DeserializeOwned,
{
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|err| format!("request {url} failed: {err}"))?;
    let response = response
        .error_for_status()
        .map_err(|err| format!("request {url} returned error: {err}"))?;

    response
        .json::<T>()
        .await
        .map_err(|err| format!("decode {url} failed: {err}"))
}

async fn fetch_json_query<T, Q>(client: &Client, url: &str, query: &Q) -> Result<T, String>
where
    T: DeserializeOwned,
    Q: Serialize + ?Sized,
{
    let response = client
        .get(url)
        .query(query)
        .send()
        .await
        .map_err(|err| format!("request {url} failed: {err}"))?;
    let response = response
        .error_for_status()
        .map_err(|err| format!("request {url} returned error: {err}"))?;

    response
        .json::<T>()
        .await
        .map_err(|err| format!("decode {url} failed: {err}"))
}

async fn send_health(tx: &EventSender, ok: bool, message: Option<String>) {
    let _ = tx
        .send(ExchangeEvent::Health {
            exchange: ExchangeId::Huobi,
            ok,
            message,
            ts_ms: now_ms(),
        })
        .await;
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis() as i64)
        .unwrap_or_default()
}

fn de_opt_f64<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    Ok(match value {
        Some(serde_json::Value::Number(number)) => number.as_f64(),
        Some(serde_json::Value::String(value)) => value.parse::<f64>().ok(),
        Some(serde_json::Value::Null) | None => None,
        _ => None,
    })
}

fn de_opt_price_level<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    Ok(match value {
        Some(serde_json::Value::Number(number)) => number.as_f64(),
        Some(serde_json::Value::String(value)) => value.parse::<f64>().ok(),
        Some(serde_json::Value::Array(values)) => values.first().and_then(value_to_f64),
        Some(serde_json::Value::Null) | None => None,
        _ => None,
    })
}

fn value_to_f64(value: &serde_json::Value) -> Option<f64> {
    match value {
        serde_json::Value::Number(number) => number.as_f64(),
        serde_json::Value::String(value) => value.parse::<f64>().ok(),
        _ => None,
    }
}

fn de_opt_i64<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    Ok(match value {
        Some(serde_json::Value::Number(number)) => number.as_i64(),
        Some(serde_json::Value::String(value)) => value.parse::<i64>().ok(),
        Some(serde_json::Value::Null) | None => None,
        _ => None,
    })
}

#[derive(Debug, Deserialize)]
struct HuobiApiResponse<T> {
    status: String,
    data: Option<T>,
    #[serde(default, rename = "err-code")]
    err_code: Option<i64>,
    #[serde(default, rename = "err-msg")]
    err_msg: Option<String>,
    #[serde(default)]
    ts: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct HuobiBatchMergedResponse {
    status: String,
    #[serde(default)]
    ticks: Vec<PerpBatchMergedTick>,
    #[serde(default, rename = "err-code")]
    err_code: Option<i64>,
    #[serde(default, rename = "err-msg")]
    err_msg: Option<String>,
    #[serde(default)]
    ts: Option<i64>,
}

#[derive(Debug, Serialize)]
struct BatchMergedQuery<'a> {
    business_type: &'a str,
}

#[derive(Debug, Serialize)]
struct FundingHistoryQuery<'a> {
    contract_code: &'a str,
    page_size: usize,
    page_index: usize,
}

#[derive(Debug, Deserialize)]
struct SpotSymbolRest {
    symbol: String,
    #[serde(rename = "base-currency")]
    base_currency: String,
    #[serde(rename = "quote-currency")]
    quote_currency: String,
    state: String,
    #[serde(default, rename = "api-trading")]
    api_trading: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SpotTickerRest {
    symbol: String,
    #[serde(default, deserialize_with = "de_opt_f64")]
    bid: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_f64")]
    ask: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_f64")]
    vol: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct PerpContractRest {
    symbol: String,
    contract_code: String,
    trade_partition: String,
    contract_status: i64,
    #[serde(default)]
    business_type: Option<String>,
    #[serde(default, deserialize_with = "de_opt_i64")]
    settlement_date: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct PerpBatchMergedTick {
    contract_code: String,
    #[serde(default)]
    bid: Option<Vec<f64>>,
    #[serde(default)]
    ask: Option<Vec<f64>>,
    #[serde(default, deserialize_with = "de_opt_f64")]
    trade_turnover: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_i64")]
    ts: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct PerpIndexPriceRest {
    contract_code: String,
    #[serde(default, deserialize_with = "de_opt_f64")]
    index_price: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_i64")]
    index_ts: Option<i64>,
}

#[derive(Debug, Deserialize, Clone)]
struct FundingRateRest {
    contract_code: String,
    #[serde(default, deserialize_with = "de_opt_f64")]
    funding_rate: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_i64")]
    funding_time: Option<i64>,
    #[serde(default, deserialize_with = "de_opt_i64")]
    next_funding_time: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct FundingHistoryPage {
    data: Vec<FundingHistoryRest>,
}

#[derive(Debug, Deserialize)]
struct FundingHistoryRest {
    #[serde(default, deserialize_with = "de_opt_f64")]
    funding_rate: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_i64")]
    funding_time: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct SpotTickerWsMessage {
    ch: String,
    #[serde(default)]
    ts: Option<i64>,
    tick: SpotTickerWsTick,
}

#[derive(Debug, Deserialize)]
struct SpotTickerWsTick {
    #[serde(default, deserialize_with = "de_opt_price_level")]
    bid: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_price_level")]
    ask: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_f64")]
    vol: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct PerpDetailWsMessage {
    ch: String,
    #[serde(default)]
    ts: Option<i64>,
    tick: PerpDetailWsTick,
}

#[derive(Debug, Deserialize)]
struct PerpDetailWsTick {
    #[serde(default)]
    bid: Option<Vec<f64>>,
    #[serde(default)]
    ask: Option<Vec<f64>>,
    #[serde(default, deserialize_with = "de_opt_f64")]
    trade_turnover: Option<f64>,
    #[serde(default, deserialize_with = "de_opt_i64")]
    id: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct PerpIndexWsMessage {
    ch: String,
    #[serde(default)]
    ts: Option<i64>,
    tick: PerpIndexWsTick,
}

#[derive(Debug, Deserialize)]
struct PerpIndexWsTick {
    #[serde(default, deserialize_with = "de_opt_f64")]
    close: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct FundingRateWsMessage {
    #[serde(default)]
    ts: Option<i64>,
    data: Vec<FundingRateRest>,
}

#[derive(Debug, Deserialize)]
struct MarketPing {
    ping: i64,
}

#[derive(Debug, Deserialize)]
struct NotifyPing {
    op: String,
    #[serde(default, deserialize_with = "de_opt_i64")]
    ts: Option<i64>,
    #[serde(default, deserialize_with = "de_opt_i64")]
    ping: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct NotifyControlEnvelope {
    #[serde(default)]
    op: Option<String>,
    #[serde(default, rename = "err-code")]
    err_code: Option<i64>,
    #[serde(default, rename = "err-msg")]
    err_msg: Option<String>,
}
