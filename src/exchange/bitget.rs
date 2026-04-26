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

const REST_BASE: &str = "https://api.bitget.com";
const PUBLIC_WS_URL: &str = "wss://ws.bitget.com/v2/ws/public";
const REST_PRODUCT_TYPE_USDT_FUTURES: &str = "usdt-futures";
const WS_INST_TYPE_SPOT: &str = "SPOT";
const WS_INST_TYPE_USDT_FUTURES: &str = "USDT-FUTURES";
const RECONNECT_DELAY: Duration = Duration::from_secs(5);
const WS_PING_INTERVAL: Duration = Duration::from_secs(30);
const FUNDING_SYNC_INTERVAL: Duration = Duration::from_secs(300);
const FUNDING_LOOKBACK_MS: i64 = 24 * 60 * 60 * 1000;
const WS_SYMBOL_CHUNK: usize = 50;
const FUNDING_HISTORY_PAGE_SIZE: usize = 100;
type SharedMarketMap = Arc<HashMap<String, MarketRef>>;

#[derive(Clone)]
pub struct BitgetAdapter {
    client: Client,
}

pub fn build() -> Box<dyn ExchangeAdapter> {
    Box::new(BitgetAdapter::new())
}

impl BitgetAdapter {
    fn new() -> Self {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(120))
            .user_agent("perp-data-moin/0.1.0")
            .build()
            .expect("bitget http client");

        Self { client }
    }

    async fn fetch_catalog(&self) -> Result<BitgetCatalog, String> {
        let spot_symbols = fetch_bitget_json_query::<Vec<SpotSymbolRest>, _>(
            &self.client,
            &format!("{REST_BASE}/api/v2/spot/public/symbols"),
            &EmptyQuery,
        )
        .await?;
        let perp_symbols = fetch_bitget_json_query::<Vec<ContractConfigRest>, _>(
            &self.client,
            &format!("{REST_BASE}/api/v2/mix/market/contracts"),
            &ProductTypeQuery {
                product_type: REST_PRODUCT_TYPE_USDT_FUTURES,
            },
        )
        .await?;

        let mut discovered = Vec::new();
        let mut spot_market_ids = Vec::new();
        let mut perp_market_ids = Vec::new();
        let mut spot_markets = HashMap::new();
        let mut perp_markets = HashMap::new();

        for symbol in spot_symbols {
            if symbol.status != "online" {
                continue;
            }

            let market = build_market_ref(
                symbol.symbol.clone(),
                symbol.symbol,
                symbol.base_coin,
                symbol.quote_coin,
                MarketKind::Spot,
            );
            spot_market_ids.push(market.market_id.clone());
            discovered.push(to_discovered_market(&market));
            spot_markets.insert(market.market_id.clone(), market);
        }

        for symbol in perp_symbols {
            if symbol.symbol_type != "perpetual" || symbol.symbol_status != "normal" {
                continue;
            }

            let market = build_market_ref(
                symbol.symbol.clone(),
                symbol.symbol,
                symbol.base_coin,
                symbol.quote_coin,
                MarketKind::Perp,
            );
            perp_market_ids.push(market.market_id.clone());
            discovered.push(to_discovered_market(&market));
            perp_markets.insert(market.market_id.clone(), market);
        }

        Ok(BitgetCatalog {
            discovered,
            spot_symbols: Arc::new(spot_market_ids),
            perp_symbols: Arc::new(perp_market_ids),
            spot_markets: Arc::new(spot_markets),
            perp_markets: Arc::new(perp_markets),
        })
    }
}

#[async_trait]
impl ExchangeAdapter for BitgetAdapter {
    fn id(&self) -> ExchangeId {
        ExchangeId::Bitget
    }

    async fn discover_markets(&self) -> Result<Vec<DiscoveredMarket>, String> {
        Ok(self.fetch_catalog().await?.discovered)
    }

    async fn spawn(&self, tx: EventSender) -> Result<Vec<JoinHandle<()>>, String> {
        let catalog = self.fetch_catalog().await?;

        if let Err(err) = seed_initial_state(&self.client, &catalog, &tx).await {
            warn!(exchange = %ExchangeId::Bitget, error = %err, "bitget rest seed failed");
            send_health(&tx, false, Some(format!("bitget rest seed failed: {err}"))).await;
        }

        let mut handles = Vec::new();

        for chunk in catalog.spot_symbols.chunks(WS_SYMBOL_CHUNK) {
            if chunk.is_empty() {
                continue;
            }

            handles.push(tokio::spawn(run_spot_ticker_stream(
                build_ticker_args(WS_INST_TYPE_SPOT, chunk),
                catalog.spot_markets.clone(),
                tx.clone(),
            )));
        }

        if !catalog.perp_markets.is_empty() {
            for chunk in catalog.perp_symbols.chunks(WS_SYMBOL_CHUNK) {
                if chunk.is_empty() {
                    continue;
                }

                handles.push(tokio::spawn(run_perp_ticker_stream(
                    build_ticker_args(WS_INST_TYPE_USDT_FUTURES, chunk),
                    catalog.perp_markets.clone(),
                    tx.clone(),
                )));
            }

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
struct BitgetCatalog {
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
        exchange: ExchangeId::Bitget,
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

fn build_ticker_args(inst_type: &'static str, symbols: &[String]) -> Vec<WsArg> {
    symbols
        .iter()
        .map(|symbol| WsArg {
            inst_type,
            channel: "ticker",
            inst_id: symbol.clone(),
        })
        .collect()
}

async fn seed_initial_state(
    client: &Client,
    catalog: &BitgetCatalog,
    tx: &EventSender,
) -> Result<(), String> {
    let mut errors = Vec::new();

    if !catalog.spot_markets.is_empty()
        && let Err(err) = seed_spot_tickers(client, &catalog.spot_markets, tx).await
    {
        errors.push(err);
    }

    if !catalog.perp_markets.is_empty() {
        if let Err(err) = seed_perp_tickers(client, &catalog.perp_markets, tx).await {
            errors.push(err);
        }
        if let Err(err) = seed_current_funding(client, &catalog.perp_markets, tx).await {
            errors.push(err);
        }
    }

    if errors.is_empty() {
        send_health(
            tx,
            true,
            Some("bitget rest snapshot initialized".to_owned()),
        )
        .await;
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
    let tickers = fetch_bitget_json_query::<Vec<SpotTickerRest>, _>(
        client,
        &format!("{REST_BASE}/api/v2/spot/market/tickers"),
        &EmptyQuery,
    )
    .await?;

    for ticker in tickers {
        let Some(market) = markets.get(&ticker.symbol) else {
            continue;
        };
        let ts_ms = ticker
            .ts
            .as_deref()
            .and_then(parse_i64)
            .unwrap_or_else(now_ms);

        if let (Some(bid), Some(ask)) = (
            ticker.bid_pr.as_deref().and_then(parse_f64),
            ticker.ask_pr.as_deref().and_then(parse_f64),
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
    tx: &EventSender,
) -> Result<(), String> {
    let tickers = fetch_bitget_json_query::<Vec<PerpTickerRest>, _>(
        client,
        &format!("{REST_BASE}/api/v2/mix/market/tickers"),
        &ProductTypeQuery {
            product_type: REST_PRODUCT_TYPE_USDT_FUTURES,
        },
    )
    .await?;

    for ticker in tickers {
        let Some(market) = markets.get(&ticker.symbol) else {
            continue;
        };
        let ts_ms = ticker
            .ts
            .as_deref()
            .and_then(parse_i64)
            .unwrap_or_else(now_ms);

        if let (Some(bid), Some(ask)) = (
            ticker.bid_pr.as_deref().and_then(parse_f64),
            ticker.ask_pr.as_deref().and_then(parse_f64),
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
                    next_settle_ms: None,
                    ts_ms,
                })
                .await;
        }
    }

    Ok(())
}

async fn seed_current_funding(
    client: &Client,
    markets: &SharedMarketMap,
    tx: &EventSender,
) -> Result<(), String> {
    let rates = fetch_bitget_json_query::<Vec<CurrentFundingRateRest>, _>(
        client,
        &format!("{REST_BASE}/api/v2/mix/market/current-fund-rate"),
        &FundingRateQuery {
            product_type: REST_PRODUCT_TYPE_USDT_FUTURES,
            symbol: None,
        },
    )
    .await?;

    for rate in rates {
        let Some(market) = markets.get(&rate.symbol) else {
            continue;
        };
        let Some(value) = rate.funding_rate.as_deref().and_then(parse_f64) else {
            continue;
        };

        let _ = tx
            .send(ExchangeEvent::FundingRate {
                market: market.clone(),
                rate: value,
                next_settle_ms: rate.next_update.as_deref().and_then(parse_i64),
                ts_ms: now_ms(),
            })
            .await;
    }

    Ok(())
}

async fn run_spot_ticker_stream(args: Vec<WsArg>, markets: SharedMarketMap, tx: EventSender) {
    run_public_websocket_loop(args, tx, "spot ticker", move |payload, tx| {
        let markets = markets.clone();
        async move { handle_spot_ticker_message(&payload, &markets, &tx).await }
    })
    .await;
}

async fn run_perp_ticker_stream(args: Vec<WsArg>, markets: SharedMarketMap, tx: EventSender) {
    run_public_websocket_loop(args, tx, "perp ticker", move |payload, tx| {
        let markets = markets.clone();
        async move { handle_perp_ticker_message(&payload, &markets, &tx).await }
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
                        Some(format!("bitget {stream_name} subscribe failed: {err}")),
                    )
                    .await;
                    sleep(RECONNECT_DELAY).await;
                    continue;
                }

                send_health(&tx, true, Some(format!("bitget {stream_name} connected"))).await;
                let mut ping = interval(WS_PING_INTERVAL);
                ping.set_missed_tick_behavior(MissedTickBehavior::Skip);

                loop {
                    tokio::select! {
                        _ = ping.tick() => {
                            if let Err(err) = socket.send(Message::Text("ping".into())).await {
                                send_health(
                                    &tx,
                                    false,
                                    Some(format!("bitget {stream_name} ping failed: {err}")),
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
                                        debug!(stream = stream_name, error = %err, "bitget websocket payload skipped");
                                    }
                                }
                                Some(Ok(Message::Binary(binary))) => match std::str::from_utf8(&binary) {
                                    Ok(text) => {
                                        if is_control_payload(text) {
                                            continue;
                                        }
                                        if let Err(err) = handler(text.to_owned(), tx.clone()).await {
                                            debug!(stream = stream_name, error = %err, "bitget websocket payload skipped");
                                        }
                                    }
                                    Err(err) => {
                                        debug!(stream = stream_name, error = %err, "bitget websocket payload was not utf8");
                                    }
                                },
                                Some(Ok(Message::Ping(payload))) => {
                                    if let Err(err) = socket.send(Message::Pong(payload)).await {
                                        send_health(
                                            &tx,
                                            false,
                                            Some(format!("bitget {stream_name} pong failed: {err}")),
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
                                        Some(format!("bitget {stream_name} closed: {frame:?}")),
                                    )
                                    .await;
                                    break;
                                }
                                Some(Err(err)) => {
                                    send_health(
                                        &tx,
                                        false,
                                        Some(format!("bitget {stream_name} read failed: {err}")),
                                    )
                                    .await;
                                    break;
                                }
                                None => {
                                    send_health(
                                        &tx,
                                        false,
                                        Some(format!("bitget {stream_name} disconnected")),
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
                    Some(format!("bitget {stream_name} connect failed: {err}")),
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

async fn handle_spot_ticker_message(
    payload: &str,
    markets: &SharedMarketMap,
    tx: &EventSender,
) -> Result<(), String> {
    let message = serde_json::from_str::<SpotTickerWsMessage>(payload)
        .map_err(|err| format!("spot ticker parse error: {err}"))?;

    for update in message.data {
        let Some(inst_id) = ws_inst_id(&update.inst_id, &update.symbol) else {
            continue;
        };
        let Some(market) = markets.get(inst_id) else {
            continue;
        };
        let ts_ms = update
            .ts
            .as_deref()
            .and_then(parse_i64)
            .unwrap_or_else(now_ms);

        if let (Some(bid), Some(ask)) = (
            update.bid_pr.as_deref().and_then(parse_f64),
            update.ask_pr.as_deref().and_then(parse_f64),
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

        if let Some(value) = update.quote_volume.as_deref().and_then(parse_f64) {
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

async fn handle_perp_ticker_message(
    payload: &str,
    markets: &SharedMarketMap,
    tx: &EventSender,
) -> Result<(), String> {
    let message = serde_json::from_str::<PerpTickerWsMessage>(payload)
        .map_err(|err| format!("perp ticker parse error: {err}"))?;

    for update in message.data {
        let Some(inst_id) = ws_inst_id(&update.inst_id, &update.symbol) else {
            continue;
        };
        let Some(market) = markets.get(inst_id) else {
            continue;
        };
        let ts_ms = update
            .ts
            .as_deref()
            .and_then(parse_i64)
            .unwrap_or_else(now_ms);

        if let (Some(bid), Some(ask)) = (
            update.bid_pr.as_deref().and_then(parse_f64),
            update.ask_pr.as_deref().and_then(parse_f64),
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

        if let Some(value) = update.quote_volume.as_deref().and_then(parse_f64) {
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

        if let Some(rate) = update.funding_rate.as_deref().and_then(parse_f64) {
            let _ = tx
                .send(ExchangeEvent::FundingRate {
                    market: market.clone(),
                    rate,
                    next_settle_ms: update.next_funding_time.as_deref().and_then(parse_i64),
                    ts_ms,
                })
                .await;
        }
    }

    Ok(())
}

async fn run_funding_history_sync(client: Client, markets: SharedMarketMap, tx: EventSender) {
    loop {
        match sync_funding_history(&client, &markets, &tx).await {
            Ok(()) => {
                send_health(&tx, true, Some("bitget funding history synced".to_owned())).await;
            }
            Err(err) => {
                send_health(
                    &tx,
                    false,
                    Some(format!("bitget funding history sync failed: {err}")),
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
                    let Some(rate) = record.funding_rate.as_deref().and_then(parse_f64) else {
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
    fetch_bitget_json_query::<Vec<FundingHistoryRest>, _>(
        client,
        &format!("{REST_BASE}/api/v2/mix/market/history-fund-rate"),
        &FundingHistoryQuery {
            product_type: REST_PRODUCT_TYPE_USDT_FUTURES,
            symbol,
            page_size: FUNDING_HISTORY_PAGE_SIZE,
            page_no: 1,
        },
    )
    .await
}

async fn fetch_bitget_json_query<T, Q>(client: &Client, url: &str, query: &Q) -> Result<T, String>
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
        .json::<BitgetResponse<T>>()
        .await
        .map_err(|err| format!("decode {url} failed: {err}"))?;

    if payload.code != "00000" {
        return Err(format!(
            "request {url} returned code {}: {}",
            payload.code, payload.msg
        ));
    }

    Ok(payload.data)
}

async fn send_health(tx: &EventSender, ok: bool, message: Option<String>) {
    let _ = tx
        .send(ExchangeEvent::Health {
            exchange: ExchangeId::Bitget,
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

fn ws_inst_id<'a>(inst_id: &'a Option<String>, symbol: &'a Option<String>) -> Option<&'a str> {
    inst_id.as_deref().or(symbol.as_deref())
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis() as i64)
        .unwrap_or_default()
}

#[derive(Debug, Serialize)]
struct EmptyQuery;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProductTypeQuery<'a> {
    product_type: &'a str,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct FundingRateQuery<'a> {
    product_type: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    symbol: Option<&'a str>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct FundingHistoryQuery<'a> {
    product_type: &'a str,
    symbol: &'a str,
    page_size: usize,
    page_no: usize,
}

#[derive(Debug, Deserialize)]
struct BitgetResponse<T> {
    code: String,
    msg: String,
    data: T,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SpotSymbolRest {
    symbol: String,
    base_coin: String,
    quote_coin: String,
    status: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ContractConfigRest {
    symbol: String,
    base_coin: String,
    quote_coin: String,
    symbol_type: String,
    symbol_status: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SpotTickerRest {
    symbol: String,
    #[serde(default)]
    bid_pr: Option<String>,
    #[serde(default)]
    ask_pr: Option<String>,
    #[serde(default)]
    quote_volume: Option<String>,
    #[serde(default)]
    ts: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PerpTickerRest {
    symbol: String,
    #[serde(default)]
    bid_pr: Option<String>,
    #[serde(default)]
    ask_pr: Option<String>,
    #[serde(default)]
    quote_volume: Option<String>,
    #[serde(default)]
    index_price: Option<String>,
    #[serde(default)]
    funding_rate: Option<String>,
    #[serde(default)]
    ts: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CurrentFundingRateRest {
    symbol: String,
    #[serde(default)]
    funding_rate: Option<String>,
    #[serde(default)]
    next_update: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FundingHistoryRest {
    #[serde(default)]
    funding_rate: Option<String>,
    #[serde(default)]
    funding_time: Option<String>,
}

#[derive(Debug, Serialize)]
struct WsSubscribeRequest<'a> {
    op: &'static str,
    args: &'a [WsArg],
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct WsArg {
    inst_type: &'static str,
    channel: &'static str,
    inst_id: String,
}

#[derive(Debug, Deserialize)]
struct SpotTickerWsMessage {
    data: Vec<SpotTickerWsData>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SpotTickerWsData {
    #[serde(default)]
    inst_id: Option<String>,
    #[serde(default)]
    symbol: Option<String>,
    #[serde(default)]
    bid_pr: Option<String>,
    #[serde(default)]
    ask_pr: Option<String>,
    #[serde(default)]
    quote_volume: Option<String>,
    #[serde(default)]
    ts: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PerpTickerWsMessage {
    data: Vec<PerpTickerWsData>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PerpTickerWsData {
    #[serde(default)]
    inst_id: Option<String>,
    #[serde(default)]
    symbol: Option<String>,
    #[serde(default)]
    bid_pr: Option<String>,
    #[serde(default)]
    ask_pr: Option<String>,
    #[serde(default)]
    quote_volume: Option<String>,
    #[serde(default)]
    index_price: Option<String>,
    #[serde(default)]
    funding_rate: Option<String>,
    #[serde(default)]
    next_funding_time: Option<String>,
    #[serde(default)]
    ts: Option<String>,
}
