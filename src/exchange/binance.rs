use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, warn};

use crate::domain::{DiscoveredMarket, ExchangeId, InstrumentKey, MarketKind, MarketRef};
use crate::exchange::adapter::ExchangeAdapter;
use crate::exchange::event::{EventSender, ExchangeEvent};

const SPOT_REST_BASE: &str = "https://api.binance.com";
const SPOT_WS_BASE: &str = "wss://stream.binance.com:9443";
const PERP_REST_BASE: &str = "https://fapi.binance.com";
const PERP_WS_BASE: &str = "wss://fstream.binance.com";
const RECONNECT_DELAY: Duration = Duration::from_secs(5);
const FUNDING_SYNC_INTERVAL: Duration = Duration::from_secs(300);
const FUNDING_LOOKBACK_MS: i64 = 24 * 60 * 60 * 1000;
const BOOK_TICKER_STREAM_CHUNK: usize = 200;
const FUNDING_RATE_PAGE_LIMIT: usize = 1000;
type SharedMarketMap = Arc<HashMap<String, MarketRef>>;

#[derive(Clone)]
pub struct BinanceAdapter {
    client: Client,
}

pub fn build() -> Box<dyn ExchangeAdapter> {
    Box::new(BinanceAdapter::new())
}

impl BinanceAdapter {
    fn new() -> Self {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(120))
            .user_agent("perp-data-moin/0.1.0")
            .build()
            .expect("binance http client");

        Self { client }
    }

    async fn fetch_catalog(&self) -> Result<BinanceCatalog, String> {
        let spot_info = fetch_json_query::<SpotExchangeInfo, _>(
            &self.client,
            &format!("{SPOT_REST_BASE}/api/v3/exchangeInfo"),
            &SpotExchangeInfoQuery {
                permissions: "[\"SPOT\"]".to_owned(),
                symbol_status: "TRADING",
                show_permission_sets: false,
            },
        )
        .await?;
        let perp_info = fetch_json::<PerpExchangeInfo>(
            &self.client,
            &format!("{PERP_REST_BASE}/fapi/v1/exchangeInfo"),
        )
        .await?;

        let mut discovered = Vec::new();
        let mut spot_markets = HashMap::new();
        let mut perp_markets = HashMap::new();
        let mut spot_symbols = Vec::new();
        let mut perp_symbols = Vec::new();

        for symbol in spot_info.symbols {
            if symbol.status != "TRADING" || !symbol.is_spot_trading_allowed {
                continue;
            }

            let market = build_market_ref(
                symbol.symbol.clone(),
                symbol.symbol,
                symbol.base_asset,
                symbol.quote_asset,
                MarketKind::Spot,
            );
            spot_symbols.push(market.market_id.clone());
            discovered.push(to_discovered_market(&market));
            spot_markets.insert(market.market_id.clone(), market);
        }

        for symbol in perp_info.symbols {
            if symbol.status != "TRADING" || symbol.contract_type != "PERPETUAL" {
                continue;
            }

            let market = build_market_ref(
                symbol.symbol.clone(),
                symbol.symbol,
                symbol.base_asset,
                symbol.quote_asset,
                MarketKind::Perp,
            );
            perp_symbols.push(market.market_id.clone());
            discovered.push(to_discovered_market(&market));
            perp_markets.insert(market.market_id.clone(), market);
        }

        Ok(BinanceCatalog {
            discovered,
            spot_symbols: Arc::new(spot_symbols),
            perp_symbols: Arc::new(perp_symbols),
            spot_markets: Arc::new(spot_markets),
            perp_markets: Arc::new(perp_markets),
        })
    }
}

#[async_trait]
impl ExchangeAdapter for BinanceAdapter {
    fn id(&self) -> ExchangeId {
        ExchangeId::Binance
    }

    async fn discover_markets(&self) -> Result<Vec<DiscoveredMarket>, String> {
        Ok(self.fetch_catalog().await?.discovered)
    }

    async fn spawn(&self, tx: EventSender) -> Result<Vec<JoinHandle<()>>, String> {
        let catalog = self.fetch_catalog().await?;

        if let Err(err) = seed_initial_state(&self.client, &catalog, &tx).await {
            warn!(exchange = %ExchangeId::Binance, error = %err, "binance rest seed failed");
            send_health(&tx, false, Some(format!("binance rest seed failed: {err}"))).await;
        }

        let mut handles = Vec::new();

        for chunk in catalog.spot_symbols.chunks(BOOK_TICKER_STREAM_CHUNK) {
            if chunk.is_empty() {
                continue;
            }

            let streams = chunk
                .iter()
                .map(|symbol| format!("{}@bookTicker", symbol.to_ascii_lowercase()))
                .collect::<Vec<_>>()
                .join("/");
            let url = format!("{SPOT_WS_BASE}/stream?streams={streams}");
            handles.push(tokio::spawn(run_spot_book_ticker_stream(
                url,
                catalog.spot_markets.clone(),
                tx.clone(),
            )));
        }

        if !catalog.spot_markets.is_empty() {
            handles.push(tokio::spawn(run_spot_volume_stream(
                format!("{SPOT_WS_BASE}/ws/!miniTicker@arr"),
                catalog.spot_markets.clone(),
                tx.clone(),
            )));
        }

        if !catalog.perp_markets.is_empty() {
            for chunk in catalog.perp_symbols.chunks(BOOK_TICKER_STREAM_CHUNK) {
                if chunk.is_empty() {
                    continue;
                }

                let streams = chunk
                    .iter()
                    .map(|symbol| format!("{}@bookTicker", symbol.to_ascii_lowercase()))
                    .collect::<Vec<_>>()
                    .join("/");
                let url = format!("{PERP_WS_BASE}/stream?streams={streams}");
                handles.push(tokio::spawn(run_perp_book_ticker_stream(
                    url,
                    catalog.perp_markets.clone(),
                    tx.clone(),
                )));
            }
            handles.push(tokio::spawn(run_perp_mark_price_stream(
                format!("{PERP_WS_BASE}/ws/!markPrice@arr@1s"),
                catalog.perp_markets.clone(),
                tx.clone(),
            )));
            handles.push(tokio::spawn(run_perp_volume_stream(
                format!("{PERP_WS_BASE}/ws/!ticker@arr"),
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
struct BinanceCatalog {
    discovered: Vec<DiscoveredMarket>,
    spot_symbols: Arc<Vec<String>>,
    perp_symbols: Arc<Vec<String>>,
    spot_markets: SharedMarketMap,
    perp_markets: SharedMarketMap,
}

fn build_market_ref(
    market_id: String,
    raw_symbol: String,
    base_asset: String,
    quote_asset: String,
    market_kind: MarketKind,
) -> MarketRef {
    MarketRef {
        exchange: ExchangeId::Binance,
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
    catalog: &BinanceCatalog,
    tx: &EventSender,
) -> Result<(), String> {
    let mut errors = Vec::new();

    if !catalog.spot_markets.is_empty() {
        if let Err(err) = seed_spot_books(client, &catalog.spot_markets, tx).await {
            errors.push(err);
        }
        if let Err(err) = seed_spot_volume(client, &catalog.spot_markets, tx).await {
            errors.push(err);
        }
    }

    if !catalog.perp_markets.is_empty() {
        if let Err(err) = seed_perp_books(client, &catalog.perp_markets, tx).await {
            errors.push(err);
        }
        if let Err(err) = seed_perp_volume(client, &catalog.perp_markets, tx).await {
            errors.push(err);
        }
        if let Err(err) = seed_perp_mark_price(client, &catalog.perp_markets, tx).await {
            errors.push(err);
        }
    }

    if errors.is_empty() {
        send_health(
            tx,
            true,
            Some("binance rest snapshot initialized".to_owned()),
        )
        .await;
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

async fn seed_spot_books(
    client: &Client,
    markets: &SharedMarketMap,
    tx: &EventSender,
) -> Result<(), String> {
    let tickers = fetch_json::<Vec<BookTickerRest>>(
        client,
        &format!("{SPOT_REST_BASE}/api/v3/ticker/bookTicker"),
    )
    .await?;

    for ticker in tickers {
        let Some(market) = markets.get(&ticker.symbol) else {
            continue;
        };
        let (Some(bid), Some(ask)) = (parse_f64(&ticker.bid_price), parse_f64(&ticker.ask_price))
        else {
            continue;
        };

        let _ = tx
            .send(ExchangeEvent::TopOfBook {
                market: market.clone(),
                bid,
                ask,
                ts_ms: now_ms(),
            })
            .await;
    }

    Ok(())
}

async fn seed_spot_volume(
    client: &Client,
    markets: &SharedMarketMap,
    tx: &EventSender,
) -> Result<(), String> {
    let tickers =
        fetch_json::<Vec<Ticker24hRest>>(client, &format!("{SPOT_REST_BASE}/api/v3/ticker/24hr"))
            .await?;

    for ticker in tickers {
        let Some(market) = markets.get(&ticker.symbol) else {
            continue;
        };
        let Some(value) = parse_f64(&ticker.quote_volume) else {
            continue;
        };

        let _ = tx
            .send(ExchangeEvent::Volume24h {
                market: market.clone(),
                value,
                ts_ms: ticker.close_time,
            })
            .await;
    }

    Ok(())
}

async fn seed_perp_books(
    client: &Client,
    markets: &SharedMarketMap,
    tx: &EventSender,
) -> Result<(), String> {
    let tickers = fetch_json::<Vec<BookTickerRest>>(
        client,
        &format!("{PERP_REST_BASE}/fapi/v1/ticker/bookTicker"),
    )
    .await?;

    for ticker in tickers {
        let Some(market) = markets.get(&ticker.symbol) else {
            continue;
        };
        let (Some(bid), Some(ask)) = (parse_f64(&ticker.bid_price), parse_f64(&ticker.ask_price))
        else {
            continue;
        };

        let _ = tx
            .send(ExchangeEvent::TopOfBook {
                market: market.clone(),
                bid,
                ask,
                ts_ms: now_ms(),
            })
            .await;
    }

    Ok(())
}

async fn seed_perp_volume(
    client: &Client,
    markets: &SharedMarketMap,
    tx: &EventSender,
) -> Result<(), String> {
    let tickers =
        fetch_json::<Vec<Ticker24hRest>>(client, &format!("{PERP_REST_BASE}/fapi/v1/ticker/24hr"))
            .await?;

    for ticker in tickers {
        let Some(market) = markets.get(&ticker.symbol) else {
            continue;
        };
        let Some(value) = parse_f64(&ticker.quote_volume) else {
            continue;
        };

        let _ = tx
            .send(ExchangeEvent::Volume24h {
                market: market.clone(),
                value,
                ts_ms: ticker.close_time,
            })
            .await;
    }

    Ok(())
}

async fn seed_perp_mark_price(
    client: &Client,
    markets: &SharedMarketMap,
    tx: &EventSender,
) -> Result<(), String> {
    let updates = fetch_json::<Vec<PremiumIndexRest>>(
        client,
        &format!("{PERP_REST_BASE}/fapi/v1/premiumIndex"),
    )
    .await?;

    for update in updates {
        let Some(market) = markets.get(&update.symbol) else {
            continue;
        };
        let ts_ms = update.time.unwrap_or_else(now_ms);

        if let Some(price) = parse_f64(&update.index_price) {
            let _ = tx
                .send(ExchangeEvent::IndexPrice {
                    market: market.clone(),
                    price,
                    ts_ms,
                })
                .await;
        }

        if let Some(rate) = parse_f64(&update.last_funding_rate) {
            let _ = tx
                .send(ExchangeEvent::FundingRate {
                    market: market.clone(),
                    rate,
                    next_settle_ms: Some(update.next_funding_time),
                    ts_ms,
                })
                .await;
        }
    }

    Ok(())
}

async fn run_spot_book_ticker_stream(url: String, markets: SharedMarketMap, tx: EventSender) {
    run_text_websocket_loop(url, tx, "spot bookTicker", move |payload, tx| {
        let markets = markets.clone();
        async move { handle_spot_book_ticker_message(&payload, &markets, &tx).await }
    })
    .await;
}

async fn run_spot_volume_stream(url: String, markets: SharedMarketMap, tx: EventSender) {
    run_text_websocket_loop(url, tx, "spot miniTicker", move |payload, tx| {
        let markets = markets.clone();
        async move { handle_spot_volume_message(&payload, &markets, &tx).await }
    })
    .await;
}

async fn run_perp_book_ticker_stream(url: String, markets: SharedMarketMap, tx: EventSender) {
    run_text_websocket_loop(url, tx, "perp bookTicker", move |payload, tx| {
        let markets = markets.clone();
        async move { handle_perp_book_ticker_message(&payload, &markets, &tx).await }
    })
    .await;
}

async fn run_perp_mark_price_stream(url: String, markets: SharedMarketMap, tx: EventSender) {
    run_text_websocket_loop(url, tx, "perp markPrice", move |payload, tx| {
        let markets = markets.clone();
        async move { handle_perp_mark_price_message(&payload, &markets, &tx).await }
    })
    .await;
}

async fn run_perp_volume_stream(url: String, markets: SharedMarketMap, tx: EventSender) {
    run_text_websocket_loop(url, tx, "perp ticker", move |payload, tx| {
        let markets = markets.clone();
        async move { handle_perp_volume_message(&payload, &markets, &tx).await }
    })
    .await;
}

async fn run_text_websocket_loop<F, Fut>(
    url: String,
    tx: EventSender,
    stream_name: &'static str,
    mut handler: F,
) where
    F: FnMut(String, EventSender) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = Result<(), String>> + Send,
{
    loop {
        match connect_async(&url).await {
            Ok((mut socket, _)) => {
                send_health(&tx, true, Some(format!("binance {stream_name} connected"))).await;

                loop {
                    match socket.next().await {
                        Some(Ok(Message::Text(text))) => {
                            if let Err(err) = handler(text.to_string(), tx.clone()).await {
                                debug!(stream = stream_name, error = %err, "binance websocket payload skipped");
                            }
                        }
                        Some(Ok(Message::Binary(binary))) => match std::str::from_utf8(&binary) {
                            Ok(text) => {
                                if let Err(err) = handler(text.to_owned(), tx.clone()).await {
                                    debug!(stream = stream_name, error = %err, "binance websocket payload skipped");
                                }
                            }
                            Err(err) => {
                                debug!(stream = stream_name, error = %err, "binance websocket payload was not utf8");
                            }
                        },
                        Some(Ok(Message::Ping(payload))) => {
                            if let Err(err) = socket.send(Message::Pong(payload)).await {
                                send_health(
                                    &tx,
                                    false,
                                    Some(format!("binance {stream_name} pong failed: {err}")),
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
                                Some(format!("binance {stream_name} closed: {frame:?}")),
                            )
                            .await;
                            break;
                        }
                        Some(Err(err)) => {
                            send_health(
                                &tx,
                                false,
                                Some(format!("binance {stream_name} read failed: {err}")),
                            )
                            .await;
                            break;
                        }
                        None => {
                            send_health(
                                &tx,
                                false,
                                Some(format!("binance {stream_name} disconnected")),
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
                    Some(format!("binance {stream_name} connect failed: {err}")),
                )
                .await;
            }
        }

        sleep(RECONNECT_DELAY).await;
    }
}

async fn handle_spot_book_ticker_message(
    payload: &str,
    markets: &SharedMarketMap,
    tx: &EventSender,
) -> Result<(), String> {
    let wrapper = serde_json::from_str::<CombinedStream<BookTickerWs>>(payload)
        .map_err(|err| format!("spot book ticker parse error: {err}"))?;
    let Some(market) = markets.get(&wrapper.data.symbol) else {
        return Ok(());
    };
    let (Some(bid), Some(ask)) = (
        parse_f64(&wrapper.data.bid_price),
        parse_f64(&wrapper.data.ask_price),
    ) else {
        return Ok(());
    };

    let _ = tx
        .send(ExchangeEvent::TopOfBook {
            market: market.clone(),
            bid,
            ask,
            ts_ms: now_ms(),
        })
        .await;
    Ok(())
}

async fn handle_spot_volume_message(
    payload: &str,
    markets: &SharedMarketMap,
    tx: &EventSender,
) -> Result<(), String> {
    let updates = serde_json::from_str::<Vec<MiniTickerWs>>(payload)
        .map_err(|err| format!("spot mini ticker parse error: {err}"))?;

    for update in updates {
        let Some(market) = markets.get(&update.symbol) else {
            continue;
        };
        let Some(value) = parse_f64(&update.quote_volume) else {
            continue;
        };
        let ts_ms = update
            .event_time
            .or(update.close_time)
            .unwrap_or_else(now_ms);

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
    let update = serde_json::from_str::<BookTickerWs>(payload)
        .map_err(|err| format!("perp book ticker parse error: {err}"))?;
    let Some(market) = markets.get(&update.symbol) else {
        return Ok(());
    };
    let (Some(bid), Some(ask)) = (parse_f64(&update.bid_price), parse_f64(&update.ask_price))
    else {
        return Ok(());
    };

    let _ = tx
        .send(ExchangeEvent::TopOfBook {
            market: market.clone(),
            bid,
            ask,
            ts_ms: update.event_time.unwrap_or_else(now_ms),
        })
        .await;
    Ok(())
}

async fn handle_perp_mark_price_message(
    payload: &str,
    markets: &SharedMarketMap,
    tx: &EventSender,
) -> Result<(), String> {
    let updates = serde_json::from_str::<Vec<MarkPriceWs>>(payload)
        .map_err(|err| format!("perp mark price parse error: {err}"))?;

    for update in updates {
        let Some(market) = markets.get(&update.symbol) else {
            continue;
        };
        let ts_ms = update.event_time.unwrap_or_else(now_ms);

        if let Some(price) = parse_f64(&update.index_price) {
            let _ = tx
                .send(ExchangeEvent::IndexPrice {
                    market: market.clone(),
                    price,
                    ts_ms,
                })
                .await;
        }

        if let Some(rate) = parse_f64(&update.funding_rate) {
            let _ = tx
                .send(ExchangeEvent::FundingRate {
                    market: market.clone(),
                    rate,
                    next_settle_ms: Some(update.next_funding_time),
                    ts_ms,
                })
                .await;
        }
    }

    Ok(())
}

async fn handle_perp_volume_message(
    payload: &str,
    markets: &SharedMarketMap,
    tx: &EventSender,
) -> Result<(), String> {
    let updates = serde_json::from_str::<Vec<MiniTickerWs>>(payload)
        .map_err(|err| format!("perp ticker parse error: {err}"))?;

    for update in updates {
        let Some(market) = markets.get(&update.symbol) else {
            continue;
        };
        let Some(value) = parse_f64(&update.quote_volume) else {
            continue;
        };
        let ts_ms = update
            .event_time
            .or(update.close_time)
            .unwrap_or_else(now_ms);

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

async fn run_funding_history_sync(client: Client, markets: SharedMarketMap, tx: EventSender) {
    let mut cursor_ms = now_ms() - FUNDING_LOOKBACK_MS;

    loop {
        match sync_funding_history(&client, &markets, &tx, cursor_ms).await {
            Ok(next_cursor_ms) => {
                cursor_ms = next_cursor_ms;
                send_health(&tx, true, Some("binance funding history synced".to_owned())).await;
            }
            Err(err) => {
                send_health(
                    &tx,
                    false,
                    Some(format!("binance funding history sync failed: {err}")),
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
    start_time: i64,
) -> Result<i64, String> {
    let mut cursor_ms = start_time;

    loop {
        let records = fetch_funding_history_page(client, cursor_ms).await?;
        let page_len = records.len();
        if records.is_empty() {
            return Ok(cursor_ms);
        }

        let Some(last_time) = records.last().map(|record| record.funding_time) else {
            return Ok(cursor_ms);
        };

        for record in records {
            let Some(market) = markets.get(&record.symbol) else {
                continue;
            };
            let Some(rate) = parse_f64(&record.funding_rate) else {
                continue;
            };

            let _ = tx
                .send(ExchangeEvent::FundingSettlement {
                    market: market.clone(),
                    canonical_symbol: market.instrument.canonical_symbol.clone(),
                    rate,
                    settle_ms: record.funding_time,
                })
                .await;
        }

        let next_cursor_ms = last_time + 1;
        if next_cursor_ms <= cursor_ms || page_len < FUNDING_RATE_PAGE_LIMIT {
            return Ok(next_cursor_ms.max(cursor_ms));
        }
        cursor_ms = next_cursor_ms;
    }
}

async fn fetch_funding_history_page(
    client: &Client,
    start_time: i64,
) -> Result<Vec<FundingRateRest>, String> {
    fetch_json_query(
        client,
        &format!("{PERP_REST_BASE}/fapi/v1/fundingRate"),
        &FundingRateQuery {
            start_time,
            limit: FUNDING_RATE_PAGE_LIMIT,
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
            exchange: ExchangeId::Binance,
            ok,
            message,
            ts_ms: now_ms(),
        })
        .await;
}

fn parse_f64(value: &str) -> Option<f64> {
    value.parse::<f64>().ok()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis() as i64)
        .unwrap_or_default()
}

#[derive(Debug, Deserialize)]
struct SpotExchangeInfo {
    symbols: Vec<SpotSymbolInfo>,
}

#[derive(Debug, Serialize)]
struct SpotExchangeInfoQuery {
    permissions: String,
    #[serde(rename = "symbolStatus")]
    symbol_status: &'static str,
    #[serde(rename = "showPermissionSets")]
    show_permission_sets: bool,
}

#[derive(Debug, Deserialize)]
struct SpotSymbolInfo {
    symbol: String,
    status: String,
    #[serde(rename = "baseAsset")]
    base_asset: String,
    #[serde(rename = "quoteAsset")]
    quote_asset: String,
    #[serde(rename = "isSpotTradingAllowed")]
    is_spot_trading_allowed: bool,
}

#[derive(Debug, Deserialize)]
struct PerpExchangeInfo {
    symbols: Vec<PerpSymbolInfo>,
}

#[derive(Debug, Deserialize)]
struct PerpSymbolInfo {
    symbol: String,
    status: String,
    #[serde(rename = "contractType")]
    contract_type: String,
    #[serde(rename = "baseAsset")]
    base_asset: String,
    #[serde(rename = "quoteAsset")]
    quote_asset: String,
}

#[derive(Debug, Deserialize)]
struct BookTickerRest {
    symbol: String,
    #[serde(rename = "bidPrice")]
    bid_price: String,
    #[serde(rename = "askPrice")]
    ask_price: String,
}

#[derive(Debug, Deserialize)]
struct Ticker24hRest {
    symbol: String,
    #[serde(rename = "quoteVolume")]
    quote_volume: String,
    #[serde(rename = "closeTime")]
    close_time: i64,
}

#[derive(Debug, Deserialize)]
struct PremiumIndexRest {
    symbol: String,
    #[serde(rename = "indexPrice")]
    index_price: String,
    #[serde(rename = "lastFundingRate")]
    last_funding_rate: String,
    #[serde(rename = "nextFundingTime")]
    next_funding_time: i64,
    time: Option<i64>,
}

#[derive(Debug, Serialize)]
struct FundingRateQuery {
    #[serde(rename = "startTime")]
    start_time: i64,
    limit: usize,
}

#[derive(Debug, Deserialize)]
struct FundingRateRest {
    symbol: String,
    #[serde(rename = "fundingRate")]
    funding_rate: String,
    #[serde(rename = "fundingTime")]
    funding_time: i64,
}

#[derive(Debug, Deserialize)]
struct CombinedStream<T> {
    data: T,
}

#[derive(Debug, Deserialize)]
struct BookTickerWs {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "b")]
    bid_price: String,
    #[serde(rename = "a")]
    ask_price: String,
    #[serde(rename = "E")]
    event_time: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct MiniTickerWs {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "q")]
    quote_volume: String,
    #[serde(rename = "E")]
    event_time: Option<i64>,
    #[serde(rename = "C")]
    close_time: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct MarkPriceWs {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "i")]
    index_price: String,
    #[serde(rename = "r")]
    funding_rate: String,
    #[serde(rename = "T")]
    next_funding_time: i64,
    #[serde(rename = "E")]
    event_time: Option<i64>,
}
