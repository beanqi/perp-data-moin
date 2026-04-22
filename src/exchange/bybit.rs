use std::collections::HashMap;
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

const REST_BASE: &str = "https://api.bybit.com";
const SPOT_WS_URL: &str = "wss://stream.bybit.com/v5/public/spot";
const LINEAR_WS_URL: &str = "wss://stream.bybit.com/v5/public/linear";
const RECONNECT_DELAY: Duration = Duration::from_secs(5);
const WS_PING_INTERVAL: Duration = Duration::from_secs(20);
const FUNDING_SYNC_INTERVAL: Duration = Duration::from_secs(300);
const FUNDING_LOOKBACK_MS: i64 = 24 * 60 * 60 * 1000;
const SPOT_WS_SYMBOL_CHUNK: usize = 10;
const LINEAR_WS_SYMBOL_CHUNK: usize = 100;
const LINEAR_INSTRUMENT_PAGE_LIMIT: usize = 1000;
const FUNDING_HISTORY_LIMIT: usize = 10;
type SharedMarketMap = Arc<HashMap<String, MarketRef>>;

#[derive(Clone)]
pub struct BybitAdapter {
    client: Client,
}

pub fn build() -> Box<dyn ExchangeAdapter> {
    Box::new(BybitAdapter::new())
}

impl BybitAdapter {
    fn new() -> Self {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(120))
            .user_agent("perp-data-moin/0.1.0")
            .build()
            .expect("bybit http client");

        Self { client }
    }

    async fn fetch_catalog(&self) -> Result<BybitCatalog, String> {
        let spot_info = fetch_bybit_json_query::<InstrumentList<SpotInstrumentRest>, _>(
            &self.client,
            &format!("{REST_BASE}/v5/market/instruments-info"),
            &InstrumentQuery {
                category: "spot",
                limit: None,
                cursor: None,
            },
        )
        .await?;

        let mut linear_info = Vec::new();
        let mut cursor = None::<String>;
        loop {
            let page = fetch_bybit_json_query::<InstrumentList<LinearInstrumentRest>, _>(
                &self.client,
                &format!("{REST_BASE}/v5/market/instruments-info"),
                &InstrumentQuery {
                    category: "linear",
                    limit: Some(LINEAR_INSTRUMENT_PAGE_LIMIT),
                    cursor: cursor.as_deref(),
                },
            )
            .await?;

            linear_info.extend(page.list);
            if page.next_page_cursor.is_empty() {
                break;
            }
            cursor = Some(page.next_page_cursor);
        }

        let mut discovered = Vec::new();
        let mut spot_symbols = Vec::new();
        let mut linear_symbols = Vec::new();
        let mut spot_markets = HashMap::new();
        let mut linear_markets = HashMap::new();

        for symbol in spot_info.list {
            if symbol.status != "Trading" {
                continue;
            }

            let market = build_market_ref(
                symbol.symbol.clone(),
                symbol.symbol,
                symbol.base_coin,
                symbol.quote_coin,
                MarketKind::Spot,
            );
            spot_symbols.push(market.market_id.clone());
            discovered.push(to_discovered_market(&market));
            spot_markets.insert(market.market_id.clone(), market);
        }

        for symbol in linear_info {
            if symbol.status != "Trading"
                || symbol.contract_type != "LinearPerpetual"
                || symbol.quote_coin != "USDT"
            {
                continue;
            }

            let market = build_market_ref(
                symbol.symbol.clone(),
                symbol.symbol,
                symbol.base_coin,
                symbol.quote_coin,
                MarketKind::Perp,
            );
            linear_symbols.push(market.market_id.clone());
            discovered.push(to_discovered_market(&market));
            linear_markets.insert(market.market_id.clone(), market);
        }

        Ok(BybitCatalog {
            discovered,
            spot_symbols: Arc::new(spot_symbols),
            linear_symbols: Arc::new(linear_symbols),
            spot_markets: Arc::new(spot_markets),
            linear_markets: Arc::new(linear_markets),
        })
    }
}

#[async_trait]
impl ExchangeAdapter for BybitAdapter {
    fn id(&self) -> ExchangeId {
        ExchangeId::Bybit
    }

    async fn discover_markets(&self) -> Result<Vec<DiscoveredMarket>, String> {
        Ok(self.fetch_catalog().await?.discovered)
    }

    async fn spawn(&self, tx: EventSender) -> Result<Vec<JoinHandle<()>>, String> {
        let catalog = self.fetch_catalog().await?;

        if let Err(err) = seed_initial_state(&self.client, &catalog, &tx).await {
            warn!(exchange = %ExchangeId::Bybit, error = %err, "bybit rest seed failed");
            send_health(&tx, false, Some(format!("bybit rest seed failed: {err}"))).await;
        }

        let mut handles = Vec::new();

        if !catalog.spot_markets.is_empty() {
            for chunk in catalog.spot_symbols.chunks(SPOT_WS_SYMBOL_CHUNK) {
                if chunk.is_empty() {
                    continue;
                }

                handles.push(tokio::spawn(run_spot_orderbook_stream(
                    build_spot_orderbook_topics(chunk),
                    catalog.spot_markets.clone(),
                    tx.clone(),
                )));
                handles.push(tokio::spawn(run_spot_ticker_stream(
                    build_ticker_topics(chunk),
                    catalog.spot_markets.clone(),
                    tx.clone(),
                )));
            }
        }

        if !catalog.linear_markets.is_empty() {
            for chunk in catalog.linear_symbols.chunks(LINEAR_WS_SYMBOL_CHUNK) {
                if chunk.is_empty() {
                    continue;
                }

                handles.push(tokio::spawn(run_linear_ticker_stream(
                    build_ticker_topics(chunk),
                    catalog.linear_markets.clone(),
                    tx.clone(),
                )));
            }

            handles.push(tokio::spawn(run_funding_history_sync(
                self.client.clone(),
                catalog.linear_markets,
                tx,
            )));
        }

        Ok(handles)
    }
}

#[derive(Clone)]
struct BybitCatalog {
    discovered: Vec<DiscoveredMarket>,
    spot_symbols: Arc<Vec<String>>,
    linear_symbols: Arc<Vec<String>>,
    spot_markets: SharedMarketMap,
    linear_markets: SharedMarketMap,
}

fn build_market_ref(
    market_id: String,
    raw_symbol: String,
    base_asset: String,
    quote_asset: String,
    market_kind: MarketKind,
) -> MarketRef {
    MarketRef {
        exchange: ExchangeId::Bybit,
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

fn build_spot_orderbook_topics(symbols: &[String]) -> Vec<String> {
    symbols
        .iter()
        .map(|symbol| format!("orderbook.1.{symbol}"))
        .collect()
}

fn build_ticker_topics(symbols: &[String]) -> Vec<String> {
    symbols
        .iter()
        .map(|symbol| format!("tickers.{symbol}"))
        .collect()
}

async fn seed_initial_state(
    client: &Client,
    catalog: &BybitCatalog,
    tx: &EventSender,
) -> Result<(), String> {
    let mut errors = Vec::new();

    if !catalog.spot_markets.is_empty()
        && let Err(err) = seed_spot_tickers(client, &catalog.spot_markets, tx).await
    {
        errors.push(err);
    }

    if !catalog.linear_markets.is_empty()
        && let Err(err) = seed_linear_tickers(client, &catalog.linear_markets, tx).await
    {
        errors.push(err);
    }

    if errors.is_empty() {
        send_health(tx, true, Some("bybit rest snapshot initialized".to_owned())).await;
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
    let result = fetch_bybit_json_query::<TickerList<SpotTickerRest>, _>(
        client,
        &format!("{REST_BASE}/v5/market/tickers"),
        &TickerQuery {
            category: "spot",
            symbol: None,
        },
    )
    .await?;

    for ticker in result.list {
        let Some(market) = markets.get(&ticker.symbol) else {
            continue;
        };

        if let (Some(bid), Some(ask)) = (
            ticker.bid1_price.as_deref().and_then(parse_f64),
            ticker.ask1_price.as_deref().and_then(parse_f64),
        ) {
            let _ = tx
                .send(ExchangeEvent::TopOfBook {
                    market: market.clone(),
                    bid,
                    ask,
                    ts_ms: now_ms(),
                })
                .await;
        }

        if let Some(value) = ticker.turnover_24h.as_deref().and_then(parse_f64) {
            let _ = tx
                .send(ExchangeEvent::Volume24h {
                    market: market.clone(),
                    value,
                    ts_ms: now_ms(),
                })
                .await;
        }
    }

    Ok(())
}

async fn seed_linear_tickers(
    client: &Client,
    markets: &SharedMarketMap,
    tx: &EventSender,
) -> Result<(), String> {
    let result = fetch_bybit_json_query::<TickerList<LinearTickerRest>, _>(
        client,
        &format!("{REST_BASE}/v5/market/tickers"),
        &TickerQuery {
            category: "linear",
            symbol: None,
        },
    )
    .await?;

    for ticker in result.list {
        let Some(market) = markets.get(&ticker.symbol) else {
            continue;
        };
        let ts_ms = now_ms();

        if let (Some(bid), Some(ask)) = (
            ticker.bid1_price.as_deref().and_then(parse_f64),
            ticker.ask1_price.as_deref().and_then(parse_f64),
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

        if let Some(value) = ticker.turnover_24h.as_deref().and_then(parse_f64) {
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

        if let Some(rate) = ticker.funding_rate.as_deref().and_then(parse_f64) {
            let _ = tx
                .send(ExchangeEvent::FundingRate {
                    market: market.clone(),
                    rate,
                    next_settle_ms: ticker.next_funding_time.as_deref().and_then(parse_i64),
                    ts_ms,
                })
                .await;
        }
    }

    Ok(())
}

async fn run_spot_orderbook_stream(args: Vec<String>, markets: SharedMarketMap, tx: EventSender) {
    run_public_websocket_loop(
        SPOT_WS_URL,
        args,
        tx,
        "spot orderbook",
        move |payload, tx| {
            let markets = markets.clone();
            async move { handle_spot_orderbook_message(&payload, &markets, &tx).await }
        },
    )
    .await;
}

async fn run_spot_ticker_stream(args: Vec<String>, markets: SharedMarketMap, tx: EventSender) {
    run_public_websocket_loop(SPOT_WS_URL, args, tx, "spot ticker", move |payload, tx| {
        let markets = markets.clone();
        async move { handle_spot_ticker_message(&payload, &markets, &tx).await }
    })
    .await;
}

async fn run_linear_ticker_stream(args: Vec<String>, markets: SharedMarketMap, tx: EventSender) {
    run_public_websocket_loop(
        LINEAR_WS_URL,
        args,
        tx,
        "linear ticker",
        move |payload, tx| {
            let markets = markets.clone();
            async move { handle_linear_ticker_message(&payload, &markets, &tx).await }
        },
    )
    .await;
}

async fn run_public_websocket_loop<F, Fut>(
    url: &'static str,
    args: Vec<String>,
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
                if let Err(err) = subscribe_topics(&mut socket, &args).await {
                    send_health(
                        &tx,
                        false,
                        Some(format!("bybit {stream_name} subscribe failed: {err}")),
                    )
                    .await;
                    sleep(RECONNECT_DELAY).await;
                    continue;
                }

                send_health(&tx, true, Some(format!("bybit {stream_name} connected"))).await;
                let mut ping = interval(WS_PING_INTERVAL);
                ping.set_missed_tick_behavior(MissedTickBehavior::Skip);

                loop {
                    tokio::select! {
                        _ = ping.tick() => {
                            if let Err(err) = socket.send(Message::Text(r#"{"op":"ping"}"#.into())).await {
                                send_health(
                                    &tx,
                                    false,
                                    Some(format!("bybit {stream_name} ping failed: {err}")),
                                )
                                .await;
                                break;
                            }
                        }
                        message = socket.next() => {
                            match message {
                                Some(Ok(Message::Text(text))) => {
                                    if is_control_payload(&text) {
                                        continue;
                                    }
                                    if let Err(err) = handler(text.to_string(), tx.clone()).await {
                                        debug!(stream = stream_name, error = %err, "bybit websocket payload skipped");
                                    }
                                }
                                Some(Ok(Message::Binary(binary))) => match std::str::from_utf8(&binary) {
                                    Ok(text) => {
                                        if is_control_payload(text) {
                                            continue;
                                        }
                                        if let Err(err) = handler(text.to_owned(), tx.clone()).await {
                                            debug!(stream = stream_name, error = %err, "bybit websocket payload skipped");
                                        }
                                    }
                                    Err(err) => {
                                        debug!(stream = stream_name, error = %err, "bybit websocket payload was not utf8");
                                    }
                                },
                                Some(Ok(Message::Ping(payload))) => {
                                    if let Err(err) = socket.send(Message::Pong(payload)).await {
                                        send_health(
                                            &tx,
                                            false,
                                            Some(format!("bybit {stream_name} pong failed: {err}")),
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
                                        Some(format!("bybit {stream_name} closed: {frame:?}")),
                                    )
                                    .await;
                                    break;
                                }
                                Some(Err(err)) => {
                                    send_health(
                                        &tx,
                                        false,
                                        Some(format!("bybit {stream_name} read failed: {err}")),
                                    )
                                    .await;
                                    break;
                                }
                                None => {
                                    send_health(
                                        &tx,
                                        false,
                                        Some(format!("bybit {stream_name} disconnected")),
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
                    Some(format!("bybit {stream_name} connect failed: {err}")),
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
    args: &[String],
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
    payload.contains("\"op\":\"ping\"")
        || payload.contains("\"op\":\"pong\"")
        || payload.contains("\"op\":\"subscribe\"")
        || payload.contains("\"success\":true")
}

async fn handle_spot_orderbook_message(
    payload: &str,
    markets: &SharedMarketMap,
    tx: &EventSender,
) -> Result<(), String> {
    let message = serde_json::from_str::<OrderbookWsMessage>(payload)
        .map_err(|err| format!("spot orderbook parse error: {err}"))?;
    let Some(market) = markets.get(&message.data.symbol) else {
        return Ok(());
    };
    let Some((bid, ask)) = parse_best_book(&message.data.bids, &message.data.asks) else {
        return Ok(());
    };

    let _ = tx
        .send(ExchangeEvent::TopOfBook {
            market: market.clone(),
            bid,
            ask,
            ts_ms: message.ts.unwrap_or_else(now_ms),
        })
        .await;
    Ok(())
}

async fn handle_spot_ticker_message(
    payload: &str,
    markets: &SharedMarketMap,
    tx: &EventSender,
) -> Result<(), String> {
    let message = serde_json::from_str::<SpotTickerWsMessage>(payload)
        .map_err(|err| format!("spot ticker parse error: {err}"))?;
    let Some(market) = markets.get(&message.data.symbol) else {
        return Ok(());
    };
    let Some(value) = message.data.turnover_24h.as_deref().and_then(parse_f64) else {
        return Ok(());
    };

    let _ = tx
        .send(ExchangeEvent::Volume24h {
            market: market.clone(),
            value,
            ts_ms: message.ts.unwrap_or_else(now_ms),
        })
        .await;
    Ok(())
}

async fn handle_linear_ticker_message(
    payload: &str,
    markets: &SharedMarketMap,
    tx: &EventSender,
) -> Result<(), String> {
    let message = serde_json::from_str::<LinearTickerWsMessage>(payload)
        .map_err(|err| format!("linear ticker parse error: {err}"))?;
    let Some(market) = markets.get(&message.data.symbol) else {
        return Ok(());
    };
    let ts_ms = message.ts.unwrap_or_else(now_ms);

    if let (Some(bid), Some(ask)) = (
        message.data.bid1_price.as_deref().and_then(parse_f64),
        message.data.ask1_price.as_deref().and_then(parse_f64),
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

    if let Some(value) = message.data.turnover_24h.as_deref().and_then(parse_f64) {
        let _ = tx
            .send(ExchangeEvent::Volume24h {
                market: market.clone(),
                value,
                ts_ms,
            })
            .await;
    }

    if let Some(price) = message.data.index_price.as_deref().and_then(parse_f64) {
        let _ = tx
            .send(ExchangeEvent::IndexPrice {
                market: market.clone(),
                price,
                ts_ms,
            })
            .await;
    }

    if let Some(rate) = message.data.funding_rate.as_deref().and_then(parse_f64) {
        let _ = tx
            .send(ExchangeEvent::FundingRate {
                market: market.clone(),
                rate,
                next_settle_ms: message
                    .data
                    .next_funding_time
                    .as_deref()
                    .and_then(parse_i64),
                ts_ms,
            })
            .await;
    }

    Ok(())
}

fn parse_best_book(bids: &[[String; 2]], asks: &[[String; 2]]) -> Option<(f64, f64)> {
    let bid = bids.first()?.first().and_then(|value| parse_f64(value))?;
    let ask = asks.first()?.first().and_then(|value| parse_f64(value))?;
    Some((bid, ask))
}

async fn run_funding_history_sync(client: Client, markets: SharedMarketMap, tx: EventSender) {
    loop {
        match sync_funding_history(&client, &markets, &tx).await {
            Ok(()) => {
                send_health(&tx, true, Some("bybit funding history synced".to_owned())).await;
            }
            Err(err) => {
                send_health(
                    &tx,
                    false,
                    Some(format!("bybit funding history sync failed: {err}")),
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
                    let Some(settle_ms) = parse_i64(&record.funding_rate_timestamp) else {
                        continue;
                    };
                    if settle_ms < cutoff_ms {
                        continue;
                    }
                    let Some(rate) = parse_f64(&record.funding_rate) else {
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
    symbol: &str,
) -> Result<Vec<FundingHistoryRest>, String> {
    let result = fetch_bybit_json_query::<FundingHistoryList, _>(
        client,
        &format!("{REST_BASE}/v5/market/funding/history"),
        &FundingHistoryQuery {
            category: "linear",
            symbol,
            limit: FUNDING_HISTORY_LIMIT,
        },
    )
    .await?;

    Ok(result.list)
}

async fn fetch_bybit_json_query<T, Q>(client: &Client, url: &str, query: &Q) -> Result<T, String>
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
        .json::<BybitResponse<T>>()
        .await
        .map_err(|err| format!("decode {url} failed: {err}"))?;

    if payload.ret_code != 0 {
        return Err(format!(
            "request {url} returned retCode {}: {}",
            payload.ret_code, payload.ret_msg
        ));
    }

    Ok(payload.result)
}

async fn send_health(tx: &EventSender, ok: bool, message: Option<String>) {
    let _ = tx
        .send(ExchangeEvent::Health {
            exchange: ExchangeId::Bybit,
            ok,
            message,
            ts_ms: now_ms(),
        })
        .await;
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

#[derive(Debug, Deserialize)]
struct BybitResponse<T> {
    #[serde(rename = "retCode")]
    ret_code: i32,
    #[serde(rename = "retMsg")]
    ret_msg: String,
    result: T,
}

#[derive(Debug, Deserialize)]
struct InstrumentList<T> {
    list: Vec<T>,
    #[serde(rename = "nextPageCursor", default)]
    next_page_cursor: String,
}

#[derive(Debug, Deserialize)]
struct TickerList<T> {
    list: Vec<T>,
}

#[derive(Debug, Deserialize)]
struct FundingHistoryList {
    list: Vec<FundingHistoryRest>,
}

#[derive(Debug, Serialize)]
struct InstrumentQuery<'a> {
    category: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cursor: Option<&'a str>,
}

#[derive(Debug, Serialize)]
struct TickerQuery<'a> {
    category: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    symbol: Option<&'a str>,
}

#[derive(Debug, Serialize)]
struct FundingHistoryQuery<'a> {
    category: &'a str,
    symbol: &'a str,
    limit: usize,
}

#[derive(Debug, Serialize)]
struct WsSubscribeRequest<'a> {
    op: &'a str,
    args: &'a [String],
}

#[derive(Debug, Deserialize)]
struct SpotInstrumentRest {
    symbol: String,
    status: String,
    #[serde(rename = "baseCoin")]
    base_coin: String,
    #[serde(rename = "quoteCoin")]
    quote_coin: String,
}

#[derive(Debug, Deserialize)]
struct LinearInstrumentRest {
    symbol: String,
    status: String,
    #[serde(rename = "contractType")]
    contract_type: String,
    #[serde(rename = "baseCoin")]
    base_coin: String,
    #[serde(rename = "quoteCoin")]
    quote_coin: String,
}

#[derive(Debug, Deserialize)]
struct SpotTickerRest {
    symbol: String,
    #[serde(rename = "bid1Price")]
    bid1_price: Option<String>,
    #[serde(rename = "ask1Price")]
    ask1_price: Option<String>,
    #[serde(rename = "turnover24h")]
    turnover_24h: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LinearTickerRest {
    symbol: String,
    #[serde(rename = "bid1Price")]
    bid1_price: Option<String>,
    #[serde(rename = "ask1Price")]
    ask1_price: Option<String>,
    #[serde(rename = "turnover24h")]
    turnover_24h: Option<String>,
    #[serde(rename = "indexPrice")]
    index_price: Option<String>,
    #[serde(rename = "fundingRate")]
    funding_rate: Option<String>,
    #[serde(rename = "nextFundingTime")]
    next_funding_time: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FundingHistoryRest {
    #[serde(rename = "fundingRate")]
    funding_rate: String,
    #[serde(rename = "fundingRateTimestamp")]
    funding_rate_timestamp: String,
}

#[derive(Debug, Deserialize)]
struct OrderbookWsMessage {
    ts: Option<i64>,
    data: OrderbookWsData,
}

#[derive(Debug, Deserialize)]
struct OrderbookWsData {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "b")]
    bids: Vec<[String; 2]>,
    #[serde(rename = "a")]
    asks: Vec<[String; 2]>,
}

#[derive(Debug, Deserialize)]
struct SpotTickerWsMessage {
    ts: Option<i64>,
    data: SpotTickerWsData,
}

#[derive(Debug, Deserialize)]
struct SpotTickerWsData {
    #[serde(rename = "symbol")]
    symbol: String,
    #[serde(rename = "turnover24h")]
    turnover_24h: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LinearTickerWsMessage {
    ts: Option<i64>,
    data: LinearTickerWsData,
}

#[derive(Debug, Deserialize)]
struct LinearTickerWsData {
    #[serde(rename = "symbol")]
    symbol: String,
    #[serde(rename = "bid1Price")]
    bid1_price: Option<String>,
    #[serde(rename = "ask1Price")]
    ask1_price: Option<String>,
    #[serde(rename = "turnover24h")]
    turnover_24h: Option<String>,
    #[serde(rename = "indexPrice")]
    index_price: Option<String>,
    #[serde(rename = "fundingRate")]
    funding_rate: Option<String>,
    #[serde(rename = "nextFundingTime")]
    next_funding_time: Option<String>,
}
