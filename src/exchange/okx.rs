use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
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

const REST_BASE: &str = "https://www.okx.com";
const PUBLIC_WS_URL: &str = "wss://ws.okx.com:8443/ws/v5/public";
const RECONNECT_DELAY: Duration = Duration::from_secs(5);
const WS_PING_INTERVAL: Duration = Duration::from_secs(25);
const FUNDING_SYNC_INTERVAL: Duration = Duration::from_secs(300);
const FUNDING_LOOKBACK_MS: i64 = 24 * 60 * 60 * 1000;
const WS_SYMBOL_CHUNK: usize = 100;
const FUNDING_HISTORY_LIMIT: usize = 100;

type SharedMarketMap = Arc<HashMap<String, MarketRef>>;
type SharedIndexMarketMap = Arc<HashMap<String, Vec<MarketRef>>>;

#[derive(Clone)]
pub struct OkxAdapter {
    client: Client,
}

pub fn build() -> Box<dyn ExchangeAdapter> {
    Box::new(OkxAdapter::new())
}

impl OkxAdapter {
    fn new() -> Self {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(120))
            .user_agent("perp-data-moin/0.1.0")
            .build()
            .expect("okx http client");

        Self { client }
    }

    async fn fetch_catalog(&self) -> Result<OkxCatalog, String> {
        let spot_instruments = fetch_okx_json_query::<Vec<InstrumentRest>, _>(
            &self.client,
            &format!("{REST_BASE}/api/v5/public/instruments"),
            &InstrumentsQuery { inst_type: "SPOT" },
        )
        .await?;
        let swap_instruments = fetch_okx_json_query::<Vec<InstrumentRest>, _>(
            &self.client,
            &format!("{REST_BASE}/api/v5/public/instruments"),
            &InstrumentsQuery { inst_type: "SWAP" },
        )
        .await?;

        let mut discovered = Vec::new();
        let mut spot_market_ids = Vec::new();
        let mut perp_market_ids = Vec::new();
        let mut spot_markets = HashMap::new();
        let mut perp_markets = HashMap::new();
        let mut index_ids = BTreeSet::new();
        let mut index_quotes = BTreeSet::new();
        let mut index_markets: HashMap<String, Vec<MarketRef>> = HashMap::new();

        for instrument in spot_instruments {
            if instrument.state != "live" {
                continue;
            }

            let Some(market) = build_spot_market_ref(&instrument) else {
                continue;
            };

            spot_market_ids.push(market.market_id.clone());
            discovered.push(to_discovered_market(&market));
            spot_markets.insert(market.market_id.clone(), market);
        }

        for instrument in swap_instruments {
            if instrument.state != "live" {
                continue;
            }

            let Some((base, quote)) = split_okx_symbol(&instrument.uly) else {
                continue;
            };

            let market = build_perp_market_ref(&instrument, base, quote);
            perp_market_ids.push(market.market_id.clone());
            discovered.push(to_discovered_market(&market));
            perp_markets.insert(market.market_id.clone(), market.clone());

            index_ids.insert(instrument.uly.clone());
            index_quotes.insert(market.instrument.quote.clone());
            index_markets
                .entry(instrument.uly)
                .or_default()
                .push(market);
        }

        Ok(OkxCatalog {
            discovered,
            spot_market_ids: Arc::new(spot_market_ids),
            perp_market_ids: Arc::new(perp_market_ids),
            index_ids: Arc::new(index_ids.into_iter().collect()),
            index_quotes: Arc::new(index_quotes.into_iter().collect()),
            spot_markets: Arc::new(spot_markets),
            perp_markets: Arc::new(perp_markets),
            index_markets: Arc::new(index_markets),
        })
    }
}

#[async_trait]
impl ExchangeAdapter for OkxAdapter {
    fn id(&self) -> ExchangeId {
        ExchangeId::Okx
    }

    async fn discover_markets(&self) -> Result<Vec<DiscoveredMarket>, String> {
        Ok(self.fetch_catalog().await?.discovered)
    }

    async fn spawn(&self, tx: EventSender) -> Result<Vec<JoinHandle<()>>, String> {
        let catalog = self.fetch_catalog().await?;

        if let Err(err) = seed_initial_state(&self.client, &catalog, &tx).await {
            warn!(exchange = %ExchangeId::Okx, error = %err, "okx rest seed failed");
            send_health(&tx, false, Some(format!("okx rest seed failed: {err}"))).await;
        }

        let mut handles = Vec::new();

        if !catalog.spot_markets.is_empty() {
            for chunk in catalog.spot_market_ids.chunks(WS_SYMBOL_CHUNK) {
                if chunk.is_empty() {
                    continue;
                }

                handles.push(tokio::spawn(run_ticker_stream(
                    build_ws_args("tickers", chunk),
                    catalog.spot_markets.clone(),
                    tx.clone(),
                    "spot ticker",
                )));
            }
        }

        if !catalog.perp_markets.is_empty() {
            for chunk in catalog.perp_market_ids.chunks(WS_SYMBOL_CHUNK) {
                if chunk.is_empty() {
                    continue;
                }

                handles.push(tokio::spawn(run_ticker_stream(
                    build_ws_args("tickers", chunk),
                    catalog.perp_markets.clone(),
                    tx.clone(),
                    "perp ticker",
                )));
                handles.push(tokio::spawn(run_funding_rate_stream(
                    build_ws_args("funding-rate", chunk),
                    catalog.perp_markets.clone(),
                    tx.clone(),
                )));
            }

            handles.push(tokio::spawn(run_funding_history_sync(
                self.client.clone(),
                catalog.perp_markets.clone(),
                tx.clone(),
            )));
        }

        if !catalog.index_ids.is_empty() {
            for chunk in catalog.index_ids.chunks(WS_SYMBOL_CHUNK) {
                if chunk.is_empty() {
                    continue;
                }

                handles.push(tokio::spawn(run_index_ticker_stream(
                    build_ws_args("index-tickers", chunk),
                    catalog.index_markets.clone(),
                    tx.clone(),
                )));
            }
        }

        Ok(handles)
    }
}

#[derive(Clone)]
struct OkxCatalog {
    discovered: Vec<DiscoveredMarket>,
    spot_market_ids: Arc<Vec<String>>,
    perp_market_ids: Arc<Vec<String>>,
    index_ids: Arc<Vec<String>>,
    index_quotes: Arc<Vec<String>>,
    spot_markets: SharedMarketMap,
    perp_markets: SharedMarketMap,
    index_markets: SharedIndexMarketMap,
}

fn build_spot_market_ref(instrument: &InstrumentRest) -> Option<MarketRef> {
    if instrument.base_ccy.is_empty() || instrument.quote_ccy.is_empty() {
        return None;
    }

    Some(MarketRef {
        exchange: ExchangeId::Okx,
        market_id: instrument.inst_id.clone(),
        raw_symbol: instrument.inst_id.clone(),
        instrument: InstrumentKey::new(
            instrument.base_ccy.to_ascii_uppercase(),
            instrument.quote_ccy.to_ascii_uppercase(),
            MarketKind::Spot,
        ),
    })
}

fn build_perp_market_ref(
    instrument: &InstrumentRest,
    base_asset: String,
    quote_asset: String,
) -> MarketRef {
    MarketRef {
        exchange: ExchangeId::Okx,
        market_id: instrument.inst_id.clone(),
        raw_symbol: instrument.inst_id.clone(),
        instrument: InstrumentKey::new(base_asset, quote_asset, MarketKind::Perp),
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

fn split_okx_symbol(symbol: &str) -> Option<(String, String)> {
    let mut tokens = symbol.split('-').filter(|value| !value.is_empty());
    let base = tokens.next()?.trim();
    let quote = tokens.next()?.trim();
    if base.is_empty() || quote.is_empty() {
        return None;
    }

    Some((base.to_ascii_uppercase(), quote.to_ascii_uppercase()))
}

fn build_ws_args(channel: &'static str, symbols: &[String]) -> Vec<WsArg> {
    symbols
        .iter()
        .map(|symbol| WsArg {
            channel,
            inst_id: symbol.clone(),
        })
        .collect()
}

async fn seed_initial_state(
    client: &Client,
    catalog: &OkxCatalog,
    tx: &EventSender,
) -> Result<(), String> {
    let mut errors = Vec::new();

    if !catalog.spot_markets.is_empty()
        && let Err(err) = seed_tickers(client, "SPOT", &catalog.spot_markets, tx).await
    {
        errors.push(err);
    }

    if !catalog.perp_markets.is_empty()
        && let Err(err) = seed_tickers(client, "SWAP", &catalog.perp_markets, tx).await
    {
        errors.push(err);
    }

    if !catalog.index_quotes.is_empty()
        && let Err(err) =
            seed_index_tickers(client, &catalog.index_markets, &catalog.index_quotes, tx).await
    {
        errors.push(err);
    }

    if errors.is_empty() {
        send_health(tx, true, Some("okx rest snapshot initialized".to_owned())).await;
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

async fn seed_tickers(
    client: &Client,
    inst_type: &'static str,
    markets: &SharedMarketMap,
    tx: &EventSender,
) -> Result<(), String> {
    let tickers = fetch_okx_json_query::<Vec<TickerRest>, _>(
        client,
        &format!("{REST_BASE}/api/v5/market/tickers"),
        &TickersQuery { inst_type },
    )
    .await?;

    for ticker in tickers {
        let Some(market) = markets.get(&ticker.inst_id) else {
            continue;
        };
        let ts_ms = ticker
            .ts
            .as_deref()
            .and_then(parse_i64)
            .unwrap_or_else(now_ms);

        if let (Some(bid), Some(ask)) = (
            ticker.bid_px.as_deref().and_then(parse_f64),
            ticker.ask_px.as_deref().and_then(parse_f64),
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

        if let Some(value) = parse_okx_volume_24h(
            ticker.last.as_deref(),
            ticker.vol_ccy_24h.as_deref(),
            market.instrument.market_kind,
        ) {
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

async fn seed_index_tickers(
    client: &Client,
    index_markets: &SharedIndexMarketMap,
    quotes: &[String],
    tx: &EventSender,
) -> Result<(), String> {
    let mut errors = Vec::new();

    for quote in quotes {
        match fetch_okx_json_query::<Vec<IndexTickerRest>, _>(
            client,
            &format!("{REST_BASE}/api/v5/market/index-tickers"),
            &IndexTickersQuery { quote_ccy: quote },
        )
        .await
        {
            Ok(updates) => {
                for update in updates {
                    let Some(price) = update.idx_px.as_deref().and_then(parse_f64) else {
                        continue;
                    };
                    let ts_ms = update
                        .ts
                        .as_deref()
                        .and_then(parse_i64)
                        .unwrap_or_else(now_ms);
                    broadcast_index_price(&update.inst_id, price, ts_ms, index_markets, tx).await;
                }
            }
            Err(err) => {
                errors.push(format!("{quote}: {err}"));
            }
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

async fn run_ticker_stream(
    args: Vec<WsArg>,
    markets: SharedMarketMap,
    tx: EventSender,
    stream_name: &'static str,
) {
    run_public_websocket_loop(args, tx, stream_name, move |payload, tx| {
        let markets = markets.clone();
        async move { handle_ticker_message(&payload, &markets, &tx).await }
    })
    .await;
}

async fn run_funding_rate_stream(args: Vec<WsArg>, markets: SharedMarketMap, tx: EventSender) {
    run_public_websocket_loop(args, tx, "funding rate", move |payload, tx| {
        let markets = markets.clone();
        async move { handle_funding_rate_message(&payload, &markets, &tx).await }
    })
    .await;
}

async fn run_index_ticker_stream(
    args: Vec<WsArg>,
    index_markets: SharedIndexMarketMap,
    tx: EventSender,
) {
    run_public_websocket_loop(args, tx, "index ticker", move |payload, tx| {
        let index_markets = index_markets.clone();
        async move { handle_index_ticker_message(&payload, &index_markets, &tx).await }
    })
    .await;
}

async fn run_public_websocket_loop<F, Fut>(
    args: Vec<WsArg>,
    tx: EventSender,
    stream_name: &'static str,
    mut handler: F,
) where
    F: FnMut(String, EventSender) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<(), String>> + Send,
{
    loop {
        match connect_async(PUBLIC_WS_URL).await {
            Ok((mut socket, _)) => {
                if let Err(err) = subscribe_topics(&mut socket, &args).await {
                    send_health(
                        &tx,
                        false,
                        Some(format!("okx {stream_name} subscribe failed: {err}")),
                    )
                    .await;
                    sleep(RECONNECT_DELAY).await;
                    continue;
                }

                send_health(&tx, true, Some(format!("okx {stream_name} connected"))).await;
                let mut ping = interval(WS_PING_INTERVAL);
                ping.set_missed_tick_behavior(MissedTickBehavior::Skip);

                loop {
                    tokio::select! {
                        _ = ping.tick() => {
                            if let Err(err) = socket.send(Message::Text("ping".into())).await {
                                send_health(
                                    &tx,
                                    false,
                                    Some(format!("okx {stream_name} ping failed: {err}")),
                                )
                                .await;
                                break;
                            }
                        }
                        message = socket.next() => {
                            match message {
                                Some(Ok(Message::Text(text))) => {
                                    if is_error_payload(&text) {
                                        send_health(
                                            &tx,
                                            false,
                                            Some(format!("okx {stream_name} error: {text}")),
                                        )
                                        .await;
                                        break;
                                    }
                                    if is_control_payload(&text) {
                                        continue;
                                    }
                                    if let Err(err) = handler(text.to_string(), tx.clone()).await {
                                        debug!(stream = stream_name, error = %err, "okx websocket payload skipped");
                                    }
                                }
                                Some(Ok(Message::Binary(binary))) => match std::str::from_utf8(&binary) {
                                    Ok(text) => {
                                        if is_error_payload(text) {
                                            send_health(
                                                &tx,
                                                false,
                                                Some(format!("okx {stream_name} error: {text}")),
                                            )
                                            .await;
                                            break;
                                        }
                                        if is_control_payload(text) {
                                            continue;
                                        }
                                        if let Err(err) = handler(text.to_owned(), tx.clone()).await {
                                            debug!(stream = stream_name, error = %err, "okx websocket payload skipped");
                                        }
                                    }
                                    Err(err) => {
                                        debug!(stream = stream_name, error = %err, "okx websocket payload was not utf8");
                                    }
                                },
                                Some(Ok(Message::Ping(payload))) => {
                                    if let Err(err) = socket.send(Message::Pong(payload)).await {
                                        send_health(
                                            &tx,
                                            false,
                                            Some(format!("okx {stream_name} pong failed: {err}")),
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
                                        Some(format!("okx {stream_name} closed: {frame:?}")),
                                    )
                                    .await;
                                    break;
                                }
                                Some(Err(err)) => {
                                    send_health(
                                        &tx,
                                        false,
                                        Some(format!("okx {stream_name} read failed: {err}")),
                                    )
                                    .await;
                                    break;
                                }
                                None => {
                                    send_health(
                                        &tx,
                                        false,
                                        Some(format!("okx {stream_name} disconnected")),
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
                    Some(format!("okx {stream_name} connect failed: {err}")),
                )
                .await;
            }
        }

        sleep(RECONNECT_DELAY).await;
    }
}

async fn subscribe_topics(
    socket: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    args: &[WsArg],
) -> Result<(), String> {
    let payload = serde_json::to_string(&WsSubscribeRequest {
        op: "subscribe",
        args,
    })
    .map_err(|err| format!("subscribe payload encode failed: {err}"))?;

    socket
        .send(Message::Text(payload.into()))
        .await
        .map_err(|err| format!("subscribe write failed: {err}"))
}

fn is_control_payload(payload: &str) -> bool {
    payload == "pong"
        || payload.contains("\"event\":\"subscribe\"")
        || payload.contains("\"event\":\"unsubscribe\"")
}

fn is_error_payload(payload: &str) -> bool {
    payload.contains("\"event\":\"error\"")
}

async fn handle_ticker_message(
    payload: &str,
    markets: &SharedMarketMap,
    tx: &EventSender,
) -> Result<(), String> {
    let message = serde_json::from_str::<WsMessage<TickerRest>>(payload)
        .map_err(|err| format!("okx ticker parse error: {err}"))?;

    for update in message.data {
        let Some(market) = markets.get(&update.inst_id) else {
            continue;
        };
        let ts_ms = update
            .ts
            .as_deref()
            .and_then(parse_i64)
            .unwrap_or_else(now_ms);

        if let (Some(bid), Some(ask)) = (
            update.bid_px.as_deref().and_then(parse_f64),
            update.ask_px.as_deref().and_then(parse_f64),
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

        if let Some(value) = parse_okx_volume_24h(
            update.last.as_deref(),
            update.vol_ccy_24h.as_deref(),
            market.instrument.market_kind,
        ) {
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

async fn handle_funding_rate_message(
    payload: &str,
    markets: &SharedMarketMap,
    tx: &EventSender,
) -> Result<(), String> {
    let message = serde_json::from_str::<WsMessage<FundingRateRest>>(payload)
        .map_err(|err| format!("okx funding rate parse error: {err}"))?;

    for update in message.data {
        let Some(market) = markets.get(&update.inst_id) else {
            continue;
        };
        let Some(rate) = update.funding_rate.as_deref().and_then(parse_f64) else {
            continue;
        };
        let ts_ms = update
            .ts
            .as_deref()
            .and_then(parse_i64)
            .unwrap_or_else(now_ms);

        let _ = tx
            .send(ExchangeEvent::FundingRate {
                market: market.clone(),
                rate,
                next_settle_ms: update.next_funding_time.as_deref().and_then(parse_i64),
                ts_ms,
            })
            .await;
    }

    Ok(())
}

async fn handle_index_ticker_message(
    payload: &str,
    index_markets: &SharedIndexMarketMap,
    tx: &EventSender,
) -> Result<(), String> {
    let message = serde_json::from_str::<WsMessage<IndexTickerRest>>(payload)
        .map_err(|err| format!("okx index ticker parse error: {err}"))?;

    for update in message.data {
        let Some(price) = update.idx_px.as_deref().and_then(parse_f64) else {
            continue;
        };
        let ts_ms = update
            .ts
            .as_deref()
            .and_then(parse_i64)
            .unwrap_or_else(now_ms);
        broadcast_index_price(&update.inst_id, price, ts_ms, index_markets, tx).await;
    }

    Ok(())
}

async fn broadcast_index_price(
    index_id: &str,
    price: f64,
    ts_ms: i64,
    index_markets: &SharedIndexMarketMap,
    tx: &EventSender,
) {
    let Some(markets) = index_markets.get(index_id) else {
        return;
    };

    for market in markets {
        let _ = tx
            .send(ExchangeEvent::IndexPrice {
                market: market.clone(),
                price,
                ts_ms,
            })
            .await;
    }
}

fn parse_okx_volume_24h(
    last: Option<&str>,
    vol_ccy_24h: Option<&str>,
    market_kind: MarketKind,
) -> Option<f64> {
    let volume = parse_f64(vol_ccy_24h?)?;
    if market_kind == MarketKind::Spot {
        Some(volume)
    } else {
        Some(volume * parse_f64(last?)?)
    }
}

async fn run_funding_history_sync(client: Client, markets: SharedMarketMap, tx: EventSender) {
    loop {
        match sync_funding_history(&client, &markets, &tx).await {
            Ok(()) => {
                send_health(&tx, true, Some("okx funding history synced".to_owned())).await;
            }
            Err(err) => {
                send_health(
                    &tx,
                    false,
                    Some(format!("okx funding history sync failed: {err}")),
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
    tx: &EventSender,
) -> Result<(), String> {
    let cutoff_ms = now_ms() - FUNDING_LOOKBACK_MS;
    let mut errors = Vec::new();

    for market in markets.values() {
        match fetch_funding_history(client, &market.market_id).await {
            Ok(records) => {
                for record in records.into_iter().rev() {
                    let Some(settle_ms) = record.funding_time.as_deref().and_then(parse_i64) else {
                        continue;
                    };
                    if settle_ms < cutoff_ms {
                        continue;
                    }
                    let Some(rate) = record
                        .realized_rate
                        .as_deref()
                        .and_then(parse_f64)
                        .or_else(|| record.funding_rate.as_deref().and_then(parse_f64))
                    else {
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

async fn fetch_funding_history(
    client: &Client,
    inst_id: &str,
) -> Result<Vec<FundingHistoryRest>, String> {
    fetch_okx_json_query::<Vec<FundingHistoryRest>, _>(
        client,
        &format!("{REST_BASE}/api/v5/public/funding-rate-history"),
        &FundingHistoryQuery {
            inst_id,
            limit: FUNDING_HISTORY_LIMIT,
        },
    )
    .await
}

async fn fetch_okx_json_query<T, Q>(client: &Client, url: &str, query: &Q) -> Result<T, String>
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
    let payload = response
        .json::<OkxResponse<T>>()
        .await
        .map_err(|err| format!("decode {url} failed: {err}"))?;

    if payload.code != "0" {
        return Err(format!(
            "request {url} returned code {}: {}",
            payload.code, payload.msg
        ));
    }

    Ok(payload.data)
}

fn parse_f64(value: &str) -> Option<f64> {
    value.parse::<f64>().ok()
}

fn parse_i64(value: &str) -> Option<i64> {
    value.parse::<i64>().ok()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis() as i64)
        .unwrap_or_default()
}

async fn send_health(tx: &EventSender, ok: bool, message: Option<String>) {
    let _ = tx
        .send(ExchangeEvent::Health {
            exchange: ExchangeId::Okx,
            ok,
            message,
            ts_ms: now_ms(),
        })
        .await;
}

#[derive(Serialize)]
struct InstrumentsQuery<'a> {
    #[serde(rename = "instType")]
    inst_type: &'a str,
}

#[derive(Serialize)]
struct TickersQuery<'a> {
    #[serde(rename = "instType")]
    inst_type: &'a str,
}

#[derive(Serialize)]
struct IndexTickersQuery<'a> {
    #[serde(rename = "quoteCcy")]
    quote_ccy: &'a str,
}

#[derive(Serialize)]
struct FundingHistoryQuery<'a> {
    #[serde(rename = "instId")]
    inst_id: &'a str,
    limit: usize,
}

#[derive(Serialize)]
struct WsSubscribeRequest<'a> {
    op: &'static str,
    args: &'a [WsArg],
}

#[derive(Clone, Serialize)]
struct WsArg {
    channel: &'static str,
    #[serde(rename = "instId")]
    inst_id: String,
}

#[derive(Deserialize)]
struct OkxResponse<T> {
    code: String,
    data: T,
    msg: String,
}

#[derive(Deserialize)]
struct WsMessage<T> {
    data: Vec<T>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct InstrumentRest {
    inst_id: String,
    uly: String,
    base_ccy: String,
    quote_ccy: String,
    state: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TickerRest {
    inst_id: String,
    last: Option<String>,
    bid_px: Option<String>,
    ask_px: Option<String>,
    vol_ccy_24h: Option<String>,
    ts: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FundingRateRest {
    inst_id: String,
    funding_rate: Option<String>,
    next_funding_time: Option<String>,
    ts: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FundingHistoryRest {
    funding_time: Option<String>,
    funding_rate: Option<String>,
    realized_rate: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct IndexTickerRest {
    inst_id: String,
    idx_px: Option<String>,
    ts: Option<String>,
}
