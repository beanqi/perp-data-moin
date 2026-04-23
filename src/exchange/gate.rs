use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tokio::time::{MissedTickBehavior, interval, sleep};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, warn};

use crate::domain::{DiscoveredMarket, ExchangeId, InstrumentKey, MarketKind, MarketRef};
use crate::exchange::adapter::ExchangeAdapter;
use crate::exchange::event::{EventSender, ExchangeEvent};

const REST_BASE: &str = "https://api.gateio.ws/api/v4";
const SPOT_WS_URL: &str = "wss://api.gateio.ws/ws/v4/";
const PERP_WS_URL: &str = "wss://fx-ws.gateio.ws/v4/ws/usdt";
const RECONNECT_DELAY: Duration = Duration::from_secs(5);
const WS_PING_INTERVAL: Duration = Duration::from_secs(20);
const FUNDING_SYNC_INTERVAL: Duration = Duration::from_secs(300);
const FUNDING_LOOKBACK_MS: i64 = 24 * 60 * 60 * 1000;
const WS_SYMBOL_CHUNK: usize = 200;
const FUNDING_HISTORY_LIMIT: usize = 16;

type SharedMarketMap = Arc<HashMap<String, MarketRef>>;
type SharedNextSettleMap = Arc<RwLock<HashMap<String, i64>>>;

#[derive(Clone)]
pub struct GateAdapter {
    client: Client,
}

pub fn build() -> Box<dyn ExchangeAdapter> {
    Box::new(GateAdapter::new())
}

impl GateAdapter {
    fn new() -> Self {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(120))
            .user_agent("perp-data-moin/0.1.0")
            .build()
            .expect("gate http client");

        Self { client }
    }

    async fn fetch_catalog(&self) -> Result<GateCatalog, String> {
        let spot_pairs = fetch_json::<Vec<SpotCurrencyPairRest>>(
            &self.client,
            &format!("{REST_BASE}/spot/currency_pairs"),
        )
        .await?;
        let perp_contracts = fetch_contracts(&self.client).await?;

        let mut discovered = Vec::new();
        let mut spot_symbols = Vec::new();
        let mut perp_symbols = Vec::new();
        let mut spot_markets = HashMap::new();
        let mut perp_markets = HashMap::new();
        let mut perp_next_settle_ms = HashMap::new();

        for pair in spot_pairs {
            if pair.trade_status != "tradable" {
                continue;
            }

            let market = build_market_ref(
                pair.id.clone(),
                pair.id,
                pair.base,
                pair.quote,
                MarketKind::Spot,
            );
            spot_symbols.push(market.market_id.clone());
            discovered.push(to_discovered_market(&market));
            spot_markets.insert(market.market_id.clone(), market);
        }

        for contract in perp_contracts {
            if contract.status != "trading" || contract.in_delisting {
                continue;
            }

            let Some((base, quote)) = split_gate_symbol(&contract.name) else {
                continue;
            };

            let market = build_market_ref(
                contract.name.clone(),
                contract.name,
                base,
                quote,
                MarketKind::Perp,
            );

            if let Some(next_settle_ms) = contract.funding_next_apply.map(normalize_gate_ts_ms) {
                perp_next_settle_ms.insert(market.market_id.clone(), next_settle_ms);
            }

            perp_symbols.push(market.market_id.clone());
            discovered.push(to_discovered_market(&market));
            perp_markets.insert(market.market_id.clone(), market);
        }

        Ok(GateCatalog {
            discovered,
            spot_symbols: Arc::new(spot_symbols),
            perp_symbols: Arc::new(perp_symbols),
            spot_markets: Arc::new(spot_markets),
            perp_markets: Arc::new(perp_markets),
            perp_next_settle_ms: Arc::new(RwLock::new(perp_next_settle_ms)),
        })
    }
}

#[async_trait]
impl ExchangeAdapter for GateAdapter {
    fn id(&self) -> ExchangeId {
        ExchangeId::Gate
    }

    async fn discover_markets(&self) -> Result<Vec<DiscoveredMarket>, String> {
        Ok(self.fetch_catalog().await?.discovered)
    }

    async fn spawn(&self, tx: EventSender) -> Result<Vec<JoinHandle<()>>, String> {
        let catalog = self.fetch_catalog().await?;

        if let Err(err) = seed_initial_state(&self.client, &catalog, &tx).await {
            warn!(exchange = %ExchangeId::Gate, error = %err, "gate rest seed failed");
            send_health(&tx, false, Some(format!("gate rest seed failed: {err}"))).await;
        }

        let mut handles = Vec::new();

        if !catalog.spot_markets.is_empty() {
            for chunk in catalog.spot_symbols.chunks(WS_SYMBOL_CHUNK) {
                if chunk.is_empty() {
                    continue;
                }

                handles.push(tokio::spawn(run_spot_book_ticker_stream(
                    chunk.to_vec(),
                    catalog.spot_markets.clone(),
                    tx.clone(),
                )));
                handles.push(tokio::spawn(run_spot_ticker_stream(
                    chunk.to_vec(),
                    catalog.spot_markets.clone(),
                    tx.clone(),
                )));
            }
        }

        if !catalog.perp_markets.is_empty() {
            for chunk in catalog.perp_symbols.chunks(WS_SYMBOL_CHUNK) {
                if chunk.is_empty() {
                    continue;
                }

                handles.push(tokio::spawn(run_perp_book_ticker_stream(
                    chunk.to_vec(),
                    catalog.perp_markets.clone(),
                    tx.clone(),
                )));
                handles.push(tokio::spawn(run_perp_ticker_stream(
                    chunk.to_vec(),
                    catalog.perp_markets.clone(),
                    catalog.perp_next_settle_ms.clone(),
                    tx.clone(),
                )));
            }

            handles.push(tokio::spawn(run_funding_history_sync(
                self.client.clone(),
                catalog.perp_markets,
                catalog.perp_next_settle_ms,
                tx,
            )));
        }

        Ok(handles)
    }
}

#[derive(Clone)]
struct GateCatalog {
    discovered: Vec<DiscoveredMarket>,
    spot_symbols: Arc<Vec<String>>,
    perp_symbols: Arc<Vec<String>>,
    spot_markets: SharedMarketMap,
    perp_markets: SharedMarketMap,
    perp_next_settle_ms: SharedNextSettleMap,
}

fn build_market_ref(
    market_id: String,
    raw_symbol: String,
    base_asset: String,
    quote_asset: String,
    market_kind: MarketKind,
) -> MarketRef {
    MarketRef {
        exchange: ExchangeId::Gate,
        market_id,
        raw_symbol,
        instrument: InstrumentKey::new(base_asset, quote_asset, market_kind),
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
    catalog: &GateCatalog,
    tx: &EventSender,
) -> Result<(), String> {
    let mut errors = Vec::new();

    if !catalog.spot_markets.is_empty()
        && let Err(err) = seed_spot_tickers(client, &catalog.spot_markets, tx).await
    {
        errors.push(err);
    }

    if !catalog.perp_markets.is_empty()
        && let Err(err) = seed_perp_tickers(
            client,
            &catalog.perp_markets,
            &catalog.perp_next_settle_ms,
            tx,
        )
        .await
    {
        errors.push(err);
    }

    if errors.is_empty() {
        send_health(tx, true, Some("gate rest snapshot initialized".to_owned())).await;
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
    let tickers =
        fetch_json::<Vec<SpotTickerRest>>(client, &format!("{REST_BASE}/spot/tickers")).await?;
    let ts_ms = now_ms();

    for ticker in tickers {
        let Some(market) = markets.get(&ticker.currency_pair) else {
            continue;
        };

        if let (Some(bid), Some(ask)) = (
            ticker.highest_bid.as_deref().and_then(parse_f64),
            ticker.lowest_ask.as_deref().and_then(parse_f64),
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

        if let Some(value) = ticker.quote_volume.as_deref().and_then(parse_f64) {
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

async fn seed_perp_tickers(
    client: &Client,
    markets: &SharedMarketMap,
    next_settles: &SharedNextSettleMap,
    tx: &EventSender,
) -> Result<(), String> {
    let tickers =
        fetch_json::<Vec<PerpTickerRest>>(client, &format!("{REST_BASE}/futures/usdt/tickers"))
            .await?;
    let ts_ms = now_ms();

    for ticker in tickers {
        let Some(market) = markets.get(&ticker.contract) else {
            continue;
        };

        if let (Some(bid), Some(ask)) = (
            ticker.highest_bid.as_deref().and_then(parse_f64),
            ticker.lowest_ask.as_deref().and_then(parse_f64),
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

        if let Some(value) = parse_perp_volume_24h(
            ticker.volume_24h_quote.as_deref(),
            ticker.volume_24h_settle.as_deref(),
        ) {
            let _ = tx
                .send(ExchangeEvent::Volume24h {
                    market: market.clone(),
                    value,
                    ts_ms,
                })
                .await;
        }

        if let Some(price) = ticker.index_price.as_deref().and_then(parse_f64) {
            let _ = tx
                .send(ExchangeEvent::IndexPrice {
                    market: market.clone(),
                    price,
                    ts_ms,
                })
                .await;
        }

        let next_settle_ms = lookup_next_settle_ms(next_settles, &ticker.contract);
        if let (Some(rate), Some(next_settle_ms)) = (
            ticker.funding_rate.as_deref().and_then(parse_f64),
            next_settle_ms,
        ) {
            let _ = tx
                .send(ExchangeEvent::FundingRate {
                    market: market.clone(),
                    rate,
                    next_settle_ms: Some(next_settle_ms),
                    ts_ms,
                })
                .await;
        }
    }

    Ok(())
}

async fn run_spot_book_ticker_stream(
    symbols: Vec<String>,
    markets: SharedMarketMap,
    tx: EventSender,
) {
    run_public_websocket_loop(
        SPOT_WS_URL,
        "spot.book_ticker",
        "spot.ping",
        symbols,
        tx,
        "spot book ticker",
        move |payload, tx| {
            let markets = markets.clone();
            async move { handle_spot_book_ticker_message(&payload, &markets, &tx).await }
        },
    )
    .await;
}

async fn run_spot_ticker_stream(symbols: Vec<String>, markets: SharedMarketMap, tx: EventSender) {
    run_public_websocket_loop(
        SPOT_WS_URL,
        "spot.tickers",
        "spot.ping",
        symbols,
        tx,
        "spot ticker",
        move |payload, tx| {
            let markets = markets.clone();
            async move { handle_spot_ticker_message(&payload, &markets, &tx).await }
        },
    )
    .await;
}

async fn run_perp_book_ticker_stream(
    symbols: Vec<String>,
    markets: SharedMarketMap,
    tx: EventSender,
) {
    run_public_websocket_loop(
        PERP_WS_URL,
        "futures.book_ticker",
        "futures.ping",
        symbols,
        tx,
        "perp book ticker",
        move |payload, tx| {
            let markets = markets.clone();
            async move { handle_perp_book_ticker_message(&payload, &markets, &tx).await }
        },
    )
    .await;
}

async fn run_perp_ticker_stream(
    symbols: Vec<String>,
    markets: SharedMarketMap,
    next_settles: SharedNextSettleMap,
    tx: EventSender,
) {
    run_public_websocket_loop(
        PERP_WS_URL,
        "futures.tickers",
        "futures.ping",
        symbols,
        tx,
        "perp ticker",
        move |payload, tx| {
            let markets = markets.clone();
            let next_settles = next_settles.clone();
            async move { handle_perp_ticker_message(&payload, &markets, &next_settles, &tx).await }
        },
    )
    .await;
}

async fn run_public_websocket_loop<F, Fut>(
    url: &'static str,
    channel: &'static str,
    ping_channel: &'static str,
    symbols: Vec<String>,
    tx: EventSender,
    stream_name: &'static str,
    mut handler: F,
) where
    F: FnMut(String, EventSender) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<(), String>> + Send,
{
    loop {
        match connect_async(url).await {
            Ok((mut socket, _)) => {
                if let Err(err) = subscribe_channel(&mut socket, channel, &symbols).await {
                    send_health(
                        &tx,
                        false,
                        Some(format!("gate {stream_name} subscribe failed: {err}")),
                    )
                    .await;
                    sleep(RECONNECT_DELAY).await;
                    continue;
                }

                send_health(&tx, true, Some(format!("gate {stream_name} connected"))).await;
                let mut ping = interval(WS_PING_INTERVAL);
                ping.set_missed_tick_behavior(MissedTickBehavior::Skip);

                loop {
                    tokio::select! {
                        _ = ping.tick() => {
                            if let Err(err) = send_ping(&mut socket, ping_channel).await {
                                send_health(
                                    &tx,
                                    false,
                                    Some(format!("gate {stream_name} ping failed: {err}")),
                                )
                                .await;
                                break;
                            }
                        }
                        message = socket.next() => {
                            match message {
                                Some(Ok(Message::Text(text))) => {
                                    if is_upgrade_payload(&text) {
                                        send_health(
                                            &tx,
                                            false,
                                            Some(format!("gate {stream_name} upgrade notice: {text}")),
                                        )
                                        .await;
                                        break;
                                    }
                                    if is_control_payload(&text) {
                                        continue;
                                    }
                                    if let Err(err) = handler(text.to_string(), tx.clone()).await {
                                        debug!(stream = stream_name, error = %err, "gate websocket payload skipped");
                                    }
                                }
                                Some(Ok(Message::Binary(binary))) => match std::str::from_utf8(&binary) {
                                    Ok(text) => {
                                        if is_upgrade_payload(text) {
                                            send_health(
                                                &tx,
                                                false,
                                                Some(format!("gate {stream_name} upgrade notice: {text}")),
                                            )
                                            .await;
                                            break;
                                        }
                                        if is_control_payload(text) {
                                            continue;
                                        }
                                        if let Err(err) = handler(text.to_owned(), tx.clone()).await {
                                            debug!(stream = stream_name, error = %err, "gate websocket payload skipped");
                                        }
                                    }
                                    Err(err) => {
                                        debug!(stream = stream_name, error = %err, "gate websocket payload was not utf8");
                                    }
                                },
                                Some(Ok(Message::Ping(payload))) => {
                                    if let Err(err) = socket.send(Message::Pong(payload)).await {
                                        send_health(
                                            &tx,
                                            false,
                                            Some(format!("gate {stream_name} pong failed: {err}")),
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
                                        Some(format!("gate {stream_name} closed: {frame:?}")),
                                    )
                                    .await;
                                    break;
                                }
                                Some(Err(err)) => {
                                    send_health(
                                        &tx,
                                        false,
                                        Some(format!("gate {stream_name} read failed: {err}")),
                                    )
                                    .await;
                                    break;
                                }
                                None => {
                                    send_health(
                                        &tx,
                                        false,
                                        Some(format!("gate {stream_name} disconnected")),
                                    )
                                    .await;
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            Err(err) => {
                send_health(
                    &tx,
                    false,
                    Some(format!("gate {stream_name} connect failed: {err}")),
                )
                .await;
            }
        }

        sleep(RECONNECT_DELAY).await;
    }
}

async fn subscribe_channel(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    channel: &'static str,
    symbols: &[String],
) -> Result<(), String> {
    let payload = serde_json::to_string(&WsRequest {
        time: now_sec(),
        channel,
        event: Some("subscribe"),
        payload: Some(symbols),
    })
    .map_err(|err| format!("subscribe payload encode failed: {err}"))?;

    socket
        .send(Message::Text(payload.into()))
        .await
        .map_err(|err| format!("subscribe write failed: {err}"))
}

async fn send_ping(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    channel: &'static str,
) -> Result<(), String> {
    let payload = serde_json::to_string(&WsRequest {
        time: now_sec(),
        channel,
        event: None,
        payload: None,
    })
    .map_err(|err| format!("ping payload encode failed: {err}"))?;

    socket
        .send(Message::Text(payload.into()))
        .await
        .map_err(|err| format!("ping write failed: {err}"))
}

fn is_control_payload(payload: &str) -> bool {
    payload.contains("\"event\":\"subscribe\"")
        || payload.contains("\"event\":\"unsubscribe\"")
        || payload.contains("\"channel\":\"spot.pong\"")
        || payload.contains("\"channel\":\"futures.pong\"")
}

fn is_upgrade_payload(payload: &str) -> bool {
    payload.contains("\"type\":\"upgrade\"")
}

async fn handle_spot_book_ticker_message(
    payload: &str,
    markets: &SharedMarketMap,
    tx: &EventSender,
) -> Result<(), String> {
    let message = serde_json::from_str::<WsEnvelope<SpotBookTickerWs>>(payload)
        .map_err(|err| format!("spot book ticker parse error: {err}"))?;
    if message.event.as_deref() != Some("update") {
        return Ok(());
    }

    let Some(market) = markets.get(&message.result.s) else {
        return Ok(());
    };
    let Some(bid) = parse_f64(&message.result.b) else {
        return Ok(());
    };
    let Some(ask) = parse_f64(&message.result.a) else {
        return Ok(());
    };

    let _ = tx
        .send(ExchangeEvent::TopOfBook {
            market: market.clone(),
            bid,
            ask,
            ts_ms: extract_ws_ts_ms(message.time_ms, message.time, Some(message.result.t)),
        })
        .await;
    Ok(())
}

async fn handle_spot_ticker_message(
    payload: &str,
    markets: &SharedMarketMap,
    tx: &EventSender,
) -> Result<(), String> {
    let message = serde_json::from_str::<WsEnvelope<SpotTickerWs>>(payload)
        .map_err(|err| format!("spot ticker parse error: {err}"))?;
    if message.event.as_deref() != Some("update") {
        return Ok(());
    }

    let Some(market) = markets.get(&message.result.currency_pair) else {
        return Ok(());
    };
    let ts_ms = extract_ws_ts_ms(message.time_ms, message.time, None);

    if let Some(value) = message.result.quote_volume.as_deref().and_then(parse_f64) {
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

async fn handle_perp_book_ticker_message(
    payload: &str,
    markets: &SharedMarketMap,
    tx: &EventSender,
) -> Result<(), String> {
    let message = serde_json::from_str::<WsEnvelope<PerpBookTickerWs>>(payload)
        .map_err(|err| format!("perp book ticker parse error: {err}"))?;
    if message.event.as_deref() != Some("update") {
        return Ok(());
    }

    let Some(market) = markets.get(&message.result.s) else {
        return Ok(());
    };
    let Some(bid) = parse_f64(&message.result.b) else {
        return Ok(());
    };
    let Some(ask) = parse_f64(&message.result.a) else {
        return Ok(());
    };

    let _ = tx
        .send(ExchangeEvent::TopOfBook {
            market: market.clone(),
            bid,
            ask,
            ts_ms: extract_ws_ts_ms(message.time_ms, message.time, Some(message.result.t)),
        })
        .await;
    Ok(())
}

async fn handle_perp_ticker_message(
    payload: &str,
    markets: &SharedMarketMap,
    next_settles: &SharedNextSettleMap,
    tx: &EventSender,
) -> Result<(), String> {
    let message = serde_json::from_str::<WsEnvelope<Vec<PerpTickerWs>>>(payload)
        .map_err(|err| format!("perp ticker parse error: {err}"))?;
    if message.event.as_deref() != Some("update") {
        return Ok(());
    }

    let ts_ms = extract_ws_ts_ms(message.time_ms, message.time, None);

    for update in message.result {
        let Some(market) = markets.get(&update.contract) else {
            continue;
        };

        if let Some(value) = parse_perp_volume_24h(
            update.volume_24h_quote.as_deref(),
            update.volume_24h_settle.as_deref(),
        ) {
            let _ = tx
                .send(ExchangeEvent::Volume24h {
                    market: market.clone(),
                    value,
                    ts_ms,
                })
                .await;
        }

        if let Some(price) = update.index_price.as_deref().and_then(parse_f64) {
            let _ = tx
                .send(ExchangeEvent::IndexPrice {
                    market: market.clone(),
                    price,
                    ts_ms,
                })
                .await;
        }

        let next_settle_ms = lookup_next_settle_ms(next_settles, &update.contract);
        if let (Some(rate), Some(next_settle_ms)) = (
            update.funding_rate.as_deref().and_then(parse_f64),
            next_settle_ms,
        ) {
            let _ = tx
                .send(ExchangeEvent::FundingRate {
                    market: market.clone(),
                    rate,
                    next_settle_ms: Some(next_settle_ms),
                    ts_ms,
                })
                .await;
        }
    }

    Ok(())
}

async fn run_funding_history_sync(
    client: Client,
    markets: SharedMarketMap,
    next_settles: SharedNextSettleMap,
    tx: EventSender,
) {
    loop {
        match sync_funding_history(&client, &markets, &next_settles, &tx).await {
            Ok(()) => {
                send_health(&tx, true, Some("gate funding history synced".to_owned())).await;
            }
            Err(err) => {
                send_health(
                    &tx,
                    false,
                    Some(format!("gate funding history sync failed: {err}")),
                )
                .await;
            }
        }

        sleep(FUNDING_SYNC_INTERVAL).await;
    }
}

async fn sync_funding_history(
    client: &Client,
    markets: &SharedMarketMap,
    next_settles: &SharedNextSettleMap,
    tx: &EventSender,
) -> Result<(), String> {
    let mut errors = Vec::new();

    if let Err(err) = sync_contract_state(client, markets, next_settles, tx).await {
        errors.push(err);
    }

    let cutoff_ms = now_ms() - FUNDING_LOOKBACK_MS;
    for market in markets.values() {
        match fetch_funding_history(client, &market.market_id).await {
            Ok(records) => {
                for record in records.into_iter().rev() {
                    let settle_ms = normalize_gate_ts_ms(record.t);
                    if settle_ms < cutoff_ms {
                        continue;
                    }
                    let Some(rate) = parse_f64(&record.r) else {
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

async fn sync_contract_state(
    client: &Client,
    markets: &SharedMarketMap,
    next_settles: &SharedNextSettleMap,
    tx: &EventSender,
) -> Result<(), String> {
    let contracts = fetch_contracts(client).await?;
    let mut updates = Vec::new();

    for contract in contracts {
        if contract.status != "trading" || contract.in_delisting {
            continue;
        }

        let Some(market) = markets.get(&contract.name) else {
            continue;
        };

        updates.push(ContractStateUpdate {
            market: market.clone(),
            funding_rate: contract.funding_rate.as_deref().and_then(parse_f64),
            next_settle_ms: contract.funding_next_apply.map(normalize_gate_ts_ms),
            index_price: contract.index_price.as_deref().and_then(parse_f64),
        });
    }

    if let Ok(mut state) = next_settles.write() {
        for update in &updates {
            if let Some(next_settle_ms) = update.next_settle_ms {
                state.insert(update.market.market_id.clone(), next_settle_ms);
            }
        }
    }

    let ts_ms = now_ms();
    for update in updates {
        if let Some(price) = update.index_price {
            let _ = tx
                .send(ExchangeEvent::IndexPrice {
                    market: update.market.clone(),
                    price,
                    ts_ms,
                })
                .await;
        }

        if let (Some(rate), Some(next_settle_ms)) = (update.funding_rate, update.next_settle_ms) {
            let _ = tx
                .send(ExchangeEvent::FundingRate {
                    market: update.market,
                    rate,
                    next_settle_ms: Some(next_settle_ms),
                    ts_ms,
                })
                .await;
        }
    }

    Ok(())
}

async fn fetch_contracts(client: &Client) -> Result<Vec<PerpContractRest>, String> {
    fetch_json(client, &format!("{REST_BASE}/futures/usdt/contracts")).await
}

async fn fetch_funding_history(
    client: &Client,
    contract: &str,
) -> Result<Vec<FundingHistoryRest>, String> {
    fetch_json_query(
        client,
        &format!("{REST_BASE}/futures/usdt/funding_rate"),
        &FundingHistoryQuery {
            contract,
            limit: FUNDING_HISTORY_LIMIT,
        },
    )
    .await
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
            exchange: ExchangeId::Gate,
            ok,
            message,
            ts_ms: now_ms(),
        })
        .await;
}

fn split_gate_symbol(symbol: &str) -> Option<(String, String)> {
    let (base, quote) = symbol.rsplit_once('_')?;
    if base.is_empty() || quote.is_empty() || base == quote {
        return None;
    }
    Some((base.to_owned(), quote.to_owned()))
}

fn lookup_next_settle_ms(next_settles: &SharedNextSettleMap, symbol: &str) -> Option<i64> {
    next_settles
        .read()
        .ok()
        .and_then(|state| state.get(symbol).copied())
}

fn parse_perp_volume_24h(quote_volume: Option<&str>, settle_volume: Option<&str>) -> Option<f64> {
    quote_volume
        .and_then(parse_f64)
        .or_else(|| settle_volume.and_then(parse_f64))
}

fn extract_ws_ts_ms(time_ms: Option<i64>, time: Option<i64>, fallback: Option<i64>) -> i64 {
    time_ms
        .map(normalize_gate_ts_ms)
        .or_else(|| time.map(normalize_gate_ts_ms))
        .or_else(|| fallback.map(normalize_gate_ts_ms))
        .unwrap_or_else(now_ms)
}

fn parse_f64(value: &str) -> Option<f64> {
    value.parse::<f64>().ok()
}

fn normalize_gate_ts_ms(value: i64) -> i64 {
    if value >= 1_000_000_000_000 {
        value
    } else {
        value.saturating_mul(1000)
    }
}

fn now_sec() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_secs() as i64)
        .unwrap_or_default()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis() as i64)
        .unwrap_or_default()
}

#[derive(Debug, Serialize)]
struct FundingHistoryQuery<'a> {
    contract: &'a str,
    limit: usize,
}

#[derive(Debug, Serialize)]
struct WsRequest<'a> {
    time: i64,
    channel: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    event: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload: Option<&'a [String]>,
}

#[derive(Debug, Deserialize)]
struct SpotCurrencyPairRest {
    id: String,
    base: String,
    quote: String,
    trade_status: String,
}

#[derive(Debug, Deserialize)]
struct SpotTickerRest {
    currency_pair: String,
    #[serde(default)]
    lowest_ask: Option<String>,
    #[serde(default)]
    highest_bid: Option<String>,
    #[serde(default)]
    quote_volume: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PerpContractRest {
    name: String,
    status: String,
    #[serde(default)]
    in_delisting: bool,
    #[serde(default)]
    funding_rate: Option<String>,
    #[serde(default)]
    funding_next_apply: Option<i64>,
    #[serde(default)]
    index_price: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PerpTickerRest {
    contract: String,
    #[serde(default)]
    highest_bid: Option<String>,
    #[serde(default)]
    lowest_ask: Option<String>,
    #[serde(default)]
    volume_24h_quote: Option<String>,
    #[serde(default)]
    volume_24h_settle: Option<String>,
    #[serde(default)]
    funding_rate: Option<String>,
    #[serde(default)]
    index_price: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FundingHistoryRest {
    r: String,
    t: i64,
}

#[derive(Debug, Deserialize)]
struct WsEnvelope<T> {
    #[serde(default)]
    time: Option<i64>,
    #[serde(default)]
    time_ms: Option<i64>,
    #[serde(default)]
    event: Option<String>,
    result: T,
}

#[derive(Debug, Deserialize)]
struct SpotBookTickerWs {
    t: i64,
    s: String,
    b: String,
    a: String,
}

#[derive(Debug, Deserialize)]
struct SpotTickerWs {
    currency_pair: String,
    #[serde(default)]
    quote_volume: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PerpBookTickerWs {
    t: i64,
    s: String,
    b: String,
    a: String,
}

#[derive(Debug, Deserialize)]
struct PerpTickerWs {
    contract: String,
    #[serde(default)]
    funding_rate: Option<String>,
    #[serde(default)]
    index_price: Option<String>,
    #[serde(default)]
    volume_24h_quote: Option<String>,
    #[serde(default)]
    volume_24h_settle: Option<String>,
}

#[derive(Debug)]
struct ContractStateUpdate {
    market: MarketRef,
    funding_rate: Option<f64>,
    next_settle_ms: Option<i64>,
    index_price: Option<f64>,
}
