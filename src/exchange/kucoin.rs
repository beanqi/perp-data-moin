use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior, interval, sleep};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, warn};

use crate::domain::{DiscoveredMarket, ExchangeId, InstrumentKey, MarketKind, MarketRef};
use crate::exchange::adapter::ExchangeAdapter;
use crate::exchange::event::{EventSender, ExchangeEvent};
use crate::exchange::watchdog::{MARKET_DATA_IDLE_CHECK_INTERVAL, MARKET_DATA_IDLE_TIMEOUT};

const SPOT_REST_BASE: &str = "https://api.kucoin.com";
const PERP_REST_BASE: &str = "https://api-futures.kucoin.com";
const RECONNECT_DELAY: Duration = Duration::from_secs(5);
const FUNDING_SYNC_INTERVAL: Duration = Duration::from_secs(300);
const FUNDING_LOOKBACK_MS: i64 = 24 * 60 * 60 * 1000;
const PERP_WS_SYMBOL_CHUNK: usize = 100;

type SharedMarketMap = Arc<HashMap<String, MarketRef>>;
type SharedNextSettleMap = Arc<RwLock<HashMap<String, i64>>>;
type SharedStringList = Arc<Vec<String>>;
type KucoinSocket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

#[derive(Clone)]
pub struct KucoinAdapter {
    client: Client,
}

pub fn build() -> Box<dyn ExchangeAdapter> {
    Box::new(KucoinAdapter::new())
}

impl KucoinAdapter {
    fn new() -> Self {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(120))
            .user_agent("perp-data-moin/0.1.0")
            .build()
            .expect("kucoin http client");

        Self { client }
    }

    async fn fetch_catalog(&self) -> Result<KucoinCatalog, String> {
        let spot_symbols = fetch_kucoin_json::<Vec<SpotSymbolRest>>(
            &self.client,
            &format!("{SPOT_REST_BASE}/api/v2/symbols"),
        )
        .await?;
        let perp_contracts = fetch_kucoin_json::<Vec<PerpContractRest>>(
            &self.client,
            &format!("{PERP_REST_BASE}/api/v1/contracts/active"),
        )
        .await?;

        let mut discovered = Vec::new();
        let mut spot_markets = HashMap::new();
        let mut perp_markets = HashMap::new();
        let mut spot_market_categories = BTreeSet::new();
        let mut perp_symbols = Vec::new();
        let mut perp_next_settle_ms = HashMap::new();

        for symbol in spot_symbols {
            if !symbol.enable_trading {
                continue;
            }

            let market = build_market_ref(
                symbol.symbol.clone(),
                symbol.base_currency,
                symbol.quote_currency,
                MarketKind::Spot,
            );

            discovered.push(to_discovered_market(&market));
            spot_markets.insert(market.market_id.clone(), market);
            spot_market_categories.insert(symbol.market);
        }

        for contract in perp_contracts {
            if !is_active_perp_contract(&contract) {
                continue;
            }

            let market = build_market_ref(
                contract.symbol.clone(),
                contract.base_currency,
                contract.quote_currency,
                MarketKind::Perp,
            );

            if let Some(next_settle_ms) = contract.next_funding_rate_datetime {
                perp_next_settle_ms.insert(market.market_id.clone(), next_settle_ms);
            }

            perp_symbols.push(market.market_id.clone());
            discovered.push(to_discovered_market(&market));
            perp_markets.insert(market.market_id.clone(), market);
        }

        Ok(KucoinCatalog {
            discovered,
            spot_markets: Arc::new(spot_markets),
            perp_markets: Arc::new(perp_markets),
            spot_market_categories: Arc::new(spot_market_categories.into_iter().collect()),
            perp_symbols: Arc::new(perp_symbols),
            perp_next_settle_ms: Arc::new(RwLock::new(perp_next_settle_ms)),
        })
    }
}

#[async_trait]
impl ExchangeAdapter for KucoinAdapter {
    fn id(&self) -> ExchangeId {
        ExchangeId::Kucoin
    }

    async fn discover_markets(&self) -> Result<Vec<DiscoveredMarket>, String> {
        Ok(self.fetch_catalog().await?.discovered)
    }

    async fn spawn(&self, tx: EventSender) -> Result<Vec<JoinHandle<()>>, String> {
        let catalog = self.fetch_catalog().await?;

        if let Err(err) = seed_initial_state(&self.client, &catalog, &tx).await {
            warn!(exchange = %ExchangeId::Kucoin, error = %err, "kucoin rest seed failed");
            send_health(&tx, false, Some(format!("kucoin rest seed failed: {err}"))).await;
        }

        let mut handles = Vec::new();

        if !catalog.spot_markets.is_empty() {
            handles.push(tokio::spawn(run_spot_all_tickers_stream(
                self.client.clone(),
                catalog.spot_markets.clone(),
                tx.clone(),
            )));

            if !catalog.spot_market_categories.is_empty() {
                handles.push(tokio::spawn(run_spot_market_snapshot_stream(
                    self.client.clone(),
                    catalog.spot_market_categories.clone(),
                    catalog.spot_markets.clone(),
                    tx.clone(),
                )));
            }
        }

        if !catalog.perp_markets.is_empty() {
            for chunk in catalog.perp_symbols.chunks(PERP_WS_SYMBOL_CHUNK) {
                if chunk.is_empty() {
                    continue;
                }

                let symbols = Arc::new(chunk.to_vec());
                handles.push(tokio::spawn(run_perp_ticker_stream(
                    self.client.clone(),
                    symbols.clone(),
                    catalog.perp_markets.clone(),
                    tx.clone(),
                )));
                handles.push(tokio::spawn(run_perp_instrument_stream(
                    self.client.clone(),
                    symbols.clone(),
                    catalog.perp_markets.clone(),
                    catalog.perp_next_settle_ms.clone(),
                    tx.clone(),
                )));
                handles.push(tokio::spawn(run_perp_snapshot_stream(
                    self.client.clone(),
                    symbols,
                    catalog.perp_markets.clone(),
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
struct KucoinCatalog {
    discovered: Vec<DiscoveredMarket>,
    spot_markets: SharedMarketMap,
    perp_markets: SharedMarketMap,
    spot_market_categories: SharedStringList,
    perp_symbols: SharedStringList,
    perp_next_settle_ms: SharedNextSettleMap,
}

fn build_market_ref(
    market_id: String,
    base_asset: String,
    quote_asset: String,
    market_kind: MarketKind,
) -> MarketRef {
    let base_asset = normalize_asset(&base_asset);
    let quote_asset = normalize_asset(&quote_asset);

    MarketRef {
        exchange: ExchangeId::Kucoin,
        market_id,
        raw_symbol: format!("{base_asset}/{quote_asset}"),
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

fn normalize_asset(asset: &str) -> String {
    match asset.to_ascii_uppercase().as_str() {
        "XBT" => "BTC".to_owned(),
        other => other.to_owned(),
    }
}

fn is_active_perp_contract(contract: &PerpContractRest) -> bool {
    contract.status == "Open" && contract.expire_date.is_none()
}

async fn seed_initial_state(
    client: &Client,
    catalog: &KucoinCatalog,
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

        if let Err(err) = sync_contract_state(
            client,
            &catalog.perp_markets,
            &catalog.perp_next_settle_ms,
            tx,
        )
        .await
        {
            errors.push(err);
        }
    }

    if errors.is_empty() {
        send_health(
            tx,
            true,
            Some("kucoin rest snapshot initialized".to_owned()),
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
    let snapshot = fetch_kucoin_json::<SpotAllTickersRest>(
        client,
        &format!("{SPOT_REST_BASE}/api/v1/market/allTickers"),
    )
    .await?;

    for ticker in snapshot.ticker {
        let Some(market) = markets.get(&ticker.symbol) else {
            continue;
        };

        if let (Some(bid), Some(ask)) = (parse_f64(&ticker.buy), parse_f64(&ticker.sell)) {
            let _ = tx
                .send(ExchangeEvent::TopOfBook {
                    market: market.clone(),
                    bid,
                    ask,
                    ts_ms: snapshot.time,
                })
                .await;
        }

        if let Some(value) = parse_f64(&ticker.vol_value) {
            let _ = tx
                .send(ExchangeEvent::Volume24h {
                    market: market.clone(),
                    value,
                    ts_ms: snapshot.time,
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
    let tickers = fetch_kucoin_json::<Vec<PerpTickerRest>>(
        client,
        &format!("{PERP_REST_BASE}/api/v1/allTickers"),
    )
    .await?;

    for ticker in tickers {
        let Some(market) = markets.get(&ticker.symbol) else {
            continue;
        };
        let Some(bid) = parse_f64(&ticker.best_bid_price) else {
            continue;
        };
        let Some(ask) = parse_f64(&ticker.best_ask_price) else {
            continue;
        };

        let _ = tx
            .send(ExchangeEvent::TopOfBook {
                market: market.clone(),
                bid,
                ask,
                ts_ms: normalize_ts_ms(ticker.ts),
            })
            .await;
    }

    Ok(())
}

async fn run_spot_all_tickers_stream(client: Client, markets: SharedMarketMap, tx: EventSender) {
    loop {
        match connect_kucoin_socket(&client, WsMarket::Spot).await {
            Ok((mut socket, ping_interval)) => {
                let topics = vec!["/market/ticker:all".to_owned()];
                if let Err(err) = subscribe_topics(&mut socket, &topics).await {
                    send_health(
                        &tx,
                        false,
                        Some(format!("kucoin spot all-tickers subscribe failed: {err}")),
                    )
                    .await;
                    sleep(RECONNECT_DELAY).await;
                    continue;
                }

                send_health(
                    &tx,
                    true,
                    Some("kucoin spot all-tickers connected".to_owned()),
                )
                .await;

                let mut pinger = interval(ping_interval);
                pinger.set_missed_tick_behavior(MissedTickBehavior::Skip);
                pinger.tick().await;
                let mut idle_check = interval(MARKET_DATA_IDLE_CHECK_INTERVAL);
                idle_check.set_missed_tick_behavior(MissedTickBehavior::Skip);
                let mut last_data_at = Instant::now();

                loop {
                    tokio::select! {
                        _ = idle_check.tick() => {
                            if send_idle_reconnect(&tx, "spot all-tickers", last_data_at).await {
                                break;
                            }
                        }
                        _ = pinger.tick() => {
                            if let Err(err) = send_ping(&mut socket).await {
                                send_health(
                                    &tx,
                                    false,
                                    Some(format!("kucoin spot all-tickers ping failed: {err}")),
                                )
                                .await;
                                break;
                            }
                        }
                        message = socket.next() => {
                            match message {
                                Some(Ok(Message::Text(text))) => {
                                    let payload = text.as_ref();
                                    if is_control_payload(payload) {
                                        continue;
                                    }
                                    if is_error_payload(payload) {
                                        send_health(
                                            &tx,
                                            false,
                                            Some("kucoin spot all-tickers returned error payload".to_owned()),
                                        )
                                        .await;
                                        break;
                                    }
                                    match handle_spot_all_tickers_message(payload, &markets, &tx).await {
                                        Ok(()) => last_data_at = Instant::now(),
                                        Err(err) => {
                                            debug!(stream = "spot all-tickers", error = %err, "kucoin websocket payload skipped");
                                        }
                                    }
                                }
                                Some(Ok(Message::Binary(binary))) => match String::from_utf8(binary.to_vec()) {
                                    Ok(payload) => {
                                        if is_control_payload(&payload) {
                                            continue;
                                        }
                                        if is_error_payload(&payload) {
                                            send_health(
                                                &tx,
                                                false,
                                                Some("kucoin spot all-tickers returned error payload".to_owned()),
                                            )
                                            .await;
                                            break;
                                        }
                                        match handle_spot_all_tickers_message(&payload, &markets, &tx).await {
                                            Ok(()) => last_data_at = Instant::now(),
                                            Err(err) => {
                                                debug!(stream = "spot all-tickers", error = %err, "kucoin websocket payload skipped");
                                            }
                                        }
                                    }
                                    Err(err) => {
                                        debug!(stream = "spot all-tickers", error = %err, "kucoin websocket payload was not utf8");
                                    }
                                },
                                Some(Ok(Message::Ping(payload))) => {
                                    if let Err(err) = socket.send(Message::Pong(payload)).await {
                                        send_health(
                                            &tx,
                                            false,
                                            Some(format!("kucoin spot all-tickers pong failed: {err}")),
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
                                        Some(format!("kucoin spot all-tickers closed: {frame:?}")),
                                    )
                                    .await;
                                    break;
                                }
                                Some(Err(err)) => {
                                    send_health(
                                        &tx,
                                        false,
                                        Some(format!("kucoin spot all-tickers read failed: {err}")),
                                    )
                                    .await;
                                    break;
                                }
                                None => {
                                    send_health(
                                        &tx,
                                        false,
                                        Some("kucoin spot all-tickers disconnected".to_owned()),
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
                    Some(format!("kucoin spot all-tickers connect failed: {err}")),
                )
                .await;
            }
        }

        sleep(RECONNECT_DELAY).await;
    }
}

async fn run_spot_market_snapshot_stream(
    client: Client,
    market_categories: SharedStringList,
    markets: SharedMarketMap,
    tx: EventSender,
) {
    loop {
        match connect_kucoin_socket(&client, WsMarket::Spot).await {
            Ok((mut socket, ping_interval)) => {
                let topics = market_categories
                    .iter()
                    .map(|market| format!("/market/snapshot:{market}"))
                    .collect::<Vec<_>>();

                if let Err(err) = subscribe_topics(&mut socket, &topics).await {
                    send_health(
                        &tx,
                        false,
                        Some(format!(
                            "kucoin spot market-snapshot subscribe failed: {err}"
                        )),
                    )
                    .await;
                    sleep(RECONNECT_DELAY).await;
                    continue;
                }

                send_health(
                    &tx,
                    true,
                    Some("kucoin spot market-snapshot connected".to_owned()),
                )
                .await;

                let mut pinger = interval(ping_interval);
                pinger.set_missed_tick_behavior(MissedTickBehavior::Skip);
                pinger.tick().await;
                let mut idle_check = interval(MARKET_DATA_IDLE_CHECK_INTERVAL);
                idle_check.set_missed_tick_behavior(MissedTickBehavior::Skip);
                let mut last_data_at = Instant::now();

                loop {
                    tokio::select! {
                        _ = idle_check.tick() => {
                            if send_idle_reconnect(&tx, "spot market-snapshot", last_data_at).await {
                                break;
                            }
                        }
                        _ = pinger.tick() => {
                            if let Err(err) = send_ping(&mut socket).await {
                                send_health(
                                    &tx,
                                    false,
                                    Some(format!("kucoin spot market-snapshot ping failed: {err}")),
                                )
                                .await;
                                break;
                            }
                        }
                        message = socket.next() => {
                            match message {
                                Some(Ok(Message::Text(text))) => {
                                    let payload = text.as_ref();
                                    if is_control_payload(payload) {
                                        continue;
                                    }
                                    if is_error_payload(payload) {
                                        send_health(
                                            &tx,
                                            false,
                                            Some("kucoin spot market-snapshot returned error payload".to_owned()),
                                        )
                                        .await;
                                        break;
                                    }
                                    match handle_spot_market_snapshot_message(payload, &markets, &tx).await {
                                        Ok(()) => last_data_at = Instant::now(),
                                        Err(err) => {
                                            debug!(stream = "spot market-snapshot", error = %err, "kucoin websocket payload skipped");
                                        }
                                    }
                                }
                                Some(Ok(Message::Binary(binary))) => match String::from_utf8(binary.to_vec()) {
                                    Ok(payload) => {
                                        if is_control_payload(&payload) {
                                            continue;
                                        }
                                        if is_error_payload(&payload) {
                                            send_health(
                                                &tx,
                                                false,
                                                Some("kucoin spot market-snapshot returned error payload".to_owned()),
                                            )
                                            .await;
                                            break;
                                        }
                                        match handle_spot_market_snapshot_message(&payload, &markets, &tx).await {
                                            Ok(()) => last_data_at = Instant::now(),
                                            Err(err) => {
                                                debug!(stream = "spot market-snapshot", error = %err, "kucoin websocket payload skipped");
                                            }
                                        }
                                    }
                                    Err(err) => {
                                        debug!(stream = "spot market-snapshot", error = %err, "kucoin websocket payload was not utf8");
                                    }
                                },
                                Some(Ok(Message::Ping(payload))) => {
                                    if let Err(err) = socket.send(Message::Pong(payload)).await {
                                        send_health(
                                            &tx,
                                            false,
                                            Some(format!("kucoin spot market-snapshot pong failed: {err}")),
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
                                        Some(format!("kucoin spot market-snapshot closed: {frame:?}")),
                                    )
                                    .await;
                                    break;
                                }
                                Some(Err(err)) => {
                                    send_health(
                                        &tx,
                                        false,
                                        Some(format!("kucoin spot market-snapshot read failed: {err}")),
                                    )
                                    .await;
                                    break;
                                }
                                None => {
                                    send_health(
                                        &tx,
                                        false,
                                        Some("kucoin spot market-snapshot disconnected".to_owned()),
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
                    Some(format!("kucoin spot market-snapshot connect failed: {err}")),
                )
                .await;
            }
        }

        sleep(RECONNECT_DELAY).await;
    }
}

async fn run_perp_ticker_stream(
    client: Client,
    symbols: SharedStringList,
    markets: SharedMarketMap,
    tx: EventSender,
) {
    loop {
        match connect_kucoin_socket(&client, WsMarket::Perp).await {
            Ok((mut socket, ping_interval)) => {
                let topics = symbols
                    .iter()
                    .map(|symbol| format!("/contractMarket/tickerV2:{symbol}"))
                    .collect::<Vec<_>>();

                if let Err(err) = subscribe_topics(&mut socket, &topics).await {
                    send_health(
                        &tx,
                        false,
                        Some(format!("kucoin perp ticker subscribe failed: {err}")),
                    )
                    .await;
                    sleep(RECONNECT_DELAY).await;
                    continue;
                }

                send_health(&tx, true, Some("kucoin perp ticker connected".to_owned())).await;

                let mut pinger = interval(ping_interval);
                pinger.set_missed_tick_behavior(MissedTickBehavior::Skip);
                pinger.tick().await;
                let mut idle_check = interval(MARKET_DATA_IDLE_CHECK_INTERVAL);
                idle_check.set_missed_tick_behavior(MissedTickBehavior::Skip);
                let mut last_data_at = Instant::now();

                loop {
                    tokio::select! {
                        _ = idle_check.tick() => {
                            if send_idle_reconnect(&tx, "perp ticker", last_data_at).await {
                                break;
                            }
                        }
                        _ = pinger.tick() => {
                            if let Err(err) = send_ping(&mut socket).await {
                                send_health(
                                    &tx,
                                    false,
                                    Some(format!("kucoin perp ticker ping failed: {err}")),
                                )
                                .await;
                                break;
                            }
                        }
                        message = socket.next() => {
                            match message {
                                Some(Ok(Message::Text(text))) => {
                                    let payload = text.as_ref();
                                    if is_control_payload(payload) {
                                        continue;
                                    }
                                    if is_error_payload(payload) {
                                        send_health(
                                            &tx,
                                            false,
                                            Some("kucoin perp ticker returned error payload".to_owned()),
                                        )
                                        .await;
                                        break;
                                    }
                                    match handle_perp_ticker_message(payload, &markets, &tx).await {
                                        Ok(()) => last_data_at = Instant::now(),
                                        Err(err) => {
                                            debug!(stream = "perp ticker", error = %err, "kucoin websocket payload skipped");
                                        }
                                    }
                                }
                                Some(Ok(Message::Binary(binary))) => match String::from_utf8(binary.to_vec()) {
                                    Ok(payload) => {
                                        if is_control_payload(&payload) {
                                            continue;
                                        }
                                        if is_error_payload(&payload) {
                                            send_health(
                                                &tx,
                                                false,
                                                Some("kucoin perp ticker returned error payload".to_owned()),
                                            )
                                            .await;
                                            break;
                                        }
                                        match handle_perp_ticker_message(&payload, &markets, &tx).await {
                                            Ok(()) => last_data_at = Instant::now(),
                                            Err(err) => {
                                                debug!(stream = "perp ticker", error = %err, "kucoin websocket payload skipped");
                                            }
                                        }
                                    }
                                    Err(err) => {
                                        debug!(stream = "perp ticker", error = %err, "kucoin websocket payload was not utf8");
                                    }
                                },
                                Some(Ok(Message::Ping(payload))) => {
                                    if let Err(err) = socket.send(Message::Pong(payload)).await {
                                        send_health(
                                            &tx,
                                            false,
                                            Some(format!("kucoin perp ticker pong failed: {err}")),
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
                                        Some(format!("kucoin perp ticker closed: {frame:?}")),
                                    )
                                    .await;
                                    break;
                                }
                                Some(Err(err)) => {
                                    send_health(
                                        &tx,
                                        false,
                                        Some(format!("kucoin perp ticker read failed: {err}")),
                                    )
                                    .await;
                                    break;
                                }
                                None => {
                                    send_health(
                                        &tx,
                                        false,
                                        Some("kucoin perp ticker disconnected".to_owned()),
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
                    Some(format!("kucoin perp ticker connect failed: {err}")),
                )
                .await;
            }
        }

        sleep(RECONNECT_DELAY).await;
    }
}

async fn run_perp_instrument_stream(
    client: Client,
    symbols: SharedStringList,
    markets: SharedMarketMap,
    next_settles: SharedNextSettleMap,
    tx: EventSender,
) {
    loop {
        match connect_kucoin_socket(&client, WsMarket::Perp).await {
            Ok((mut socket, ping_interval)) => {
                let topics = symbols
                    .iter()
                    .map(|symbol| format!("/contract/instrument:{symbol}"))
                    .collect::<Vec<_>>();

                if let Err(err) = subscribe_topics(&mut socket, &topics).await {
                    send_health(
                        &tx,
                        false,
                        Some(format!("kucoin perp instrument subscribe failed: {err}")),
                    )
                    .await;
                    sleep(RECONNECT_DELAY).await;
                    continue;
                }

                send_health(
                    &tx,
                    true,
                    Some("kucoin perp instrument connected".to_owned()),
                )
                .await;

                let mut pinger = interval(ping_interval);
                pinger.set_missed_tick_behavior(MissedTickBehavior::Skip);
                pinger.tick().await;
                let mut idle_check = interval(MARKET_DATA_IDLE_CHECK_INTERVAL);
                idle_check.set_missed_tick_behavior(MissedTickBehavior::Skip);
                let mut last_data_at = Instant::now();

                loop {
                    tokio::select! {
                        _ = idle_check.tick() => {
                            if send_idle_reconnect(&tx, "perp instrument", last_data_at).await {
                                break;
                            }
                        }
                        _ = pinger.tick() => {
                            if let Err(err) = send_ping(&mut socket).await {
                                send_health(
                                    &tx,
                                    false,
                                    Some(format!("kucoin perp instrument ping failed: {err}")),
                                )
                                .await;
                                break;
                            }
                        }
                        message = socket.next() => {
                            match message {
                                Some(Ok(Message::Text(text))) => {
                                    let payload = text.as_ref();
                                    if is_control_payload(payload) {
                                        continue;
                                    }
                                    if is_error_payload(payload) {
                                        send_health(
                                            &tx,
                                            false,
                                            Some("kucoin perp instrument returned error payload".to_owned()),
                                        )
                                        .await;
                                        break;
                                    }
                                    match handle_perp_instrument_message(payload, &markets, &next_settles, &tx).await {
                                        Ok(()) => last_data_at = Instant::now(),
                                        Err(err) => {
                                            debug!(stream = "perp instrument", error = %err, "kucoin websocket payload skipped");
                                        }
                                    }
                                }
                                Some(Ok(Message::Binary(binary))) => match String::from_utf8(binary.to_vec()) {
                                    Ok(payload) => {
                                        if is_control_payload(&payload) {
                                            continue;
                                        }
                                        if is_error_payload(&payload) {
                                            send_health(
                                                &tx,
                                                false,
                                                Some("kucoin perp instrument returned error payload".to_owned()),
                                            )
                                            .await;
                                            break;
                                        }
                                        match handle_perp_instrument_message(&payload, &markets, &next_settles, &tx).await {
                                            Ok(()) => last_data_at = Instant::now(),
                                            Err(err) => {
                                                debug!(stream = "perp instrument", error = %err, "kucoin websocket payload skipped");
                                            }
                                        }
                                    }
                                    Err(err) => {
                                        debug!(stream = "perp instrument", error = %err, "kucoin websocket payload was not utf8");
                                    }
                                },
                                Some(Ok(Message::Ping(payload))) => {
                                    if let Err(err) = socket.send(Message::Pong(payload)).await {
                                        send_health(
                                            &tx,
                                            false,
                                            Some(format!("kucoin perp instrument pong failed: {err}")),
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
                                        Some(format!("kucoin perp instrument closed: {frame:?}")),
                                    )
                                    .await;
                                    break;
                                }
                                Some(Err(err)) => {
                                    send_health(
                                        &tx,
                                        false,
                                        Some(format!("kucoin perp instrument read failed: {err}")),
                                    )
                                    .await;
                                    break;
                                }
                                None => {
                                    send_health(
                                        &tx,
                                        false,
                                        Some("kucoin perp instrument disconnected".to_owned()),
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
                    Some(format!("kucoin perp instrument connect failed: {err}")),
                )
                .await;
            }
        }

        sleep(RECONNECT_DELAY).await;
    }
}

async fn run_perp_snapshot_stream(
    client: Client,
    symbols: SharedStringList,
    markets: SharedMarketMap,
    tx: EventSender,
) {
    loop {
        match connect_kucoin_socket(&client, WsMarket::Perp).await {
            Ok((mut socket, ping_interval)) => {
                let topics = symbols
                    .iter()
                    .map(|symbol| format!("/contractMarket/snapshot:{symbol}"))
                    .collect::<Vec<_>>();

                if let Err(err) = subscribe_topics(&mut socket, &topics).await {
                    send_health(
                        &tx,
                        false,
                        Some(format!("kucoin perp snapshot subscribe failed: {err}")),
                    )
                    .await;
                    sleep(RECONNECT_DELAY).await;
                    continue;
                }

                send_health(&tx, true, Some("kucoin perp snapshot connected".to_owned())).await;

                let mut pinger = interval(ping_interval);
                pinger.set_missed_tick_behavior(MissedTickBehavior::Skip);
                pinger.tick().await;
                let mut idle_check = interval(MARKET_DATA_IDLE_CHECK_INTERVAL);
                idle_check.set_missed_tick_behavior(MissedTickBehavior::Skip);
                let mut last_data_at = Instant::now();

                loop {
                    tokio::select! {
                        _ = idle_check.tick() => {
                            if send_idle_reconnect(&tx, "perp snapshot", last_data_at).await {
                                break;
                            }
                        }
                        _ = pinger.tick() => {
                            if let Err(err) = send_ping(&mut socket).await {
                                send_health(
                                    &tx,
                                    false,
                                    Some(format!("kucoin perp snapshot ping failed: {err}")),
                                )
                                .await;
                                break;
                            }
                        }
                        message = socket.next() => {
                            match message {
                                Some(Ok(Message::Text(text))) => {
                                    let payload = text.as_ref();
                                    if is_control_payload(payload) {
                                        continue;
                                    }
                                    if is_error_payload(payload) {
                                        send_health(
                                            &tx,
                                            false,
                                            Some("kucoin perp snapshot returned error payload".to_owned()),
                                        )
                                        .await;
                                        break;
                                    }
                                    match handle_perp_snapshot_message(payload, &markets, &tx).await {
                                        Ok(()) => last_data_at = Instant::now(),
                                        Err(err) => {
                                            debug!(stream = "perp snapshot", error = %err, "kucoin websocket payload skipped");
                                        }
                                    }
                                }
                                Some(Ok(Message::Binary(binary))) => match String::from_utf8(binary.to_vec()) {
                                    Ok(payload) => {
                                        if is_control_payload(&payload) {
                                            continue;
                                        }
                                        if is_error_payload(&payload) {
                                            send_health(
                                                &tx,
                                                false,
                                                Some("kucoin perp snapshot returned error payload".to_owned()),
                                            )
                                            .await;
                                            break;
                                        }
                                        match handle_perp_snapshot_message(&payload, &markets, &tx).await {
                                            Ok(()) => last_data_at = Instant::now(),
                                            Err(err) => {
                                                debug!(stream = "perp snapshot", error = %err, "kucoin websocket payload skipped");
                                            }
                                        }
                                    }
                                    Err(err) => {
                                        debug!(stream = "perp snapshot", error = %err, "kucoin websocket payload was not utf8");
                                    }
                                },
                                Some(Ok(Message::Ping(payload))) => {
                                    if let Err(err) = socket.send(Message::Pong(payload)).await {
                                        send_health(
                                            &tx,
                                            false,
                                            Some(format!("kucoin perp snapshot pong failed: {err}")),
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
                                        Some(format!("kucoin perp snapshot closed: {frame:?}")),
                                    )
                                    .await;
                                    break;
                                }
                                Some(Err(err)) => {
                                    send_health(
                                        &tx,
                                        false,
                                        Some(format!("kucoin perp snapshot read failed: {err}")),
                                    )
                                    .await;
                                    break;
                                }
                                None => {
                                    send_health(
                                        &tx,
                                        false,
                                        Some("kucoin perp snapshot disconnected".to_owned()),
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
                    Some(format!("kucoin perp snapshot connect failed: {err}")),
                )
                .await;
            }
        }

        sleep(RECONNECT_DELAY).await;
    }
}

async fn handle_spot_all_tickers_message(
    payload: &str,
    markets: &SharedMarketMap,
    tx: &EventSender,
) -> Result<(), String> {
    let message = serde_json::from_str::<SpotAllTickerWsMessage>(payload)
        .map_err(|err| format!("spot all-tickers parse error: {err}"))?;
    let Some(market) = markets.get(&message.subject) else {
        return Ok(());
    };
    let Some(bid) = parse_f64(&message.data.best_bid) else {
        return Ok(());
    };
    let Some(ask) = parse_f64(&message.data.best_ask) else {
        return Ok(());
    };

    let _ = tx
        .send(ExchangeEvent::TopOfBook {
            market: market.clone(),
            bid,
            ask,
            ts_ms: flex_ts_ms(message.data.time.as_ref()).unwrap_or_else(now_ms),
        })
        .await;
    Ok(())
}

async fn handle_spot_market_snapshot_message(
    payload: &str,
    markets: &SharedMarketMap,
    tx: &EventSender,
) -> Result<(), String> {
    let message = serde_json::from_str::<SpotMarketSnapshotWsMessage>(payload)
        .map_err(|err| format!("spot market-snapshot parse error: {err}"))?;
    let data = message.data.data;
    let Some(market) = markets.get(&data.symbol) else {
        return Ok(());
    };
    let ts_ms = flex_ts_ms(data.datetime.as_ref()).unwrap_or_else(now_ms);

    if let (Some(bid), Some(ask)) = (
        parse_f64_flex(data.buy.as_ref()),
        parse_f64_flex(data.sell.as_ref()),
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

    let volume = parse_f64_flex(data.vol_value.as_ref()).or_else(|| {
        data.market_change_24h
            .as_ref()
            .and_then(|change| parse_f64_flex(change.vol_value.as_ref()))
    });

    if let Some(value) = volume {
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

async fn handle_perp_ticker_message(
    payload: &str,
    markets: &SharedMarketMap,
    tx: &EventSender,
) -> Result<(), String> {
    let message = serde_json::from_str::<PerpTickerWsMessage>(payload)
        .map_err(|err| format!("perp ticker parse error: {err}"))?;
    let Some(market) = markets.get(&message.data.symbol) else {
        return Ok(());
    };
    let Some(bid) = parse_f64(&message.data.best_bid_price) else {
        return Ok(());
    };
    let Some(ask) = parse_f64(&message.data.best_ask_price) else {
        return Ok(());
    };

    let _ = tx
        .send(ExchangeEvent::TopOfBook {
            market: market.clone(),
            bid,
            ask,
            ts_ms: flex_ts_ms(message.data.ts.as_ref()).unwrap_or_else(now_ms),
        })
        .await;
    Ok(())
}

async fn handle_perp_instrument_message(
    payload: &str,
    markets: &SharedMarketMap,
    next_settles: &SharedNextSettleMap,
    tx: &EventSender,
) -> Result<(), String> {
    let message = serde_json::from_str::<PerpInstrumentWsMessage>(payload)
        .map_err(|err| format!("perp instrument parse error: {err}"))?;
    let Some(symbol) = extract_topic_symbol(&message.topic) else {
        return Ok(());
    };
    let Some(market) = markets.get(symbol) else {
        return Ok(());
    };
    let ts_ms = flex_ts_ms(message.data.ts.as_ref()).unwrap_or_else(now_ms);

    if let Some(price) = parse_f64_flex(message.data.index_price.as_ref()) {
        let _ = tx
            .send(ExchangeEvent::IndexPrice {
                market: market.clone(),
                price,
                ts_ms,
            })
            .await;
    }

    if let Some(rate) = parse_f64_flex(message.data.funding_rate.as_ref()) {
        let _ = tx
            .send(ExchangeEvent::FundingRate {
                market: market.clone(),
                rate,
                next_settle_ms: lookup_next_settle_ms(next_settles, &market.market_id),
                ts_ms,
            })
            .await;
    }

    Ok(())
}

async fn handle_perp_snapshot_message(
    payload: &str,
    markets: &SharedMarketMap,
    tx: &EventSender,
) -> Result<(), String> {
    let message = serde_json::from_str::<PerpSnapshotWsMessage>(payload)
        .map_err(|err| format!("perp snapshot parse error: {err}"))?;
    let Some(market) = markets.get(&message.data.symbol) else {
        return Ok(());
    };
    let Some(value) = parse_f64_flex(message.data.turnover.as_ref()) else {
        return Ok(());
    };

    let _ = tx
        .send(ExchangeEvent::Volume24h {
            market: market.clone(),
            value,
            ts_ms: flex_ts_ms(message.data.ts.as_ref()).unwrap_or_else(now_ms),
        })
        .await;
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
                send_health(&tx, true, Some("kucoin funding history synced".to_owned())).await;
            }
            Err(err) => {
                send_health(
                    &tx,
                    false,
                    Some(format!("kucoin funding history sync failed: {err}")),
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
        match fetch_funding_history(client, &market.market_id, cutoff_ms, now_ms()).await {
            Ok(records) => {
                for record in records.into_iter().rev() {
                    if record.timepoint < cutoff_ms {
                        continue;
                    }

                    let _ = tx
                        .send(ExchangeEvent::FundingSettlement {
                            market: market.clone(),
                            canonical_symbol: market.instrument.canonical_symbol.clone(),
                            rate: record.funding_rate,
                            settle_ms: record.timepoint,
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
    let contracts = fetch_kucoin_json::<Vec<PerpContractRest>>(
        client,
        &format!("{PERP_REST_BASE}/api/v1/contracts/active"),
    )
    .await?;
    let mut updates = Vec::new();

    for contract in contracts {
        if !is_active_perp_contract(&contract) {
            continue;
        }

        let Some(market) = markets.get(&contract.symbol) else {
            continue;
        };

        updates.push(ContractStateUpdate {
            market: market.clone(),
            volume_24h: contract.turnover_of_24h,
            index_price: contract.index_price,
            funding_rate: contract.funding_fee_rate,
            next_settle_ms: contract.next_funding_rate_datetime,
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
        if let Some(value) = update.volume_24h {
            let _ = tx
                .send(ExchangeEvent::Volume24h {
                    market: update.market.clone(),
                    value,
                    ts_ms,
                })
                .await;
        }

        if let Some(price) = update.index_price {
            let _ = tx
                .send(ExchangeEvent::IndexPrice {
                    market: update.market.clone(),
                    price,
                    ts_ms,
                })
                .await;
        }

        if let Some(rate) = update.funding_rate {
            let _ = tx
                .send(ExchangeEvent::FundingRate {
                    market: update.market,
                    rate,
                    next_settle_ms: update.next_settle_ms,
                    ts_ms,
                })
                .await;
        }
    }

    Ok(())
}

async fn fetch_funding_history(
    client: &Client,
    symbol: &str,
    from_ms: i64,
    to_ms: i64,
) -> Result<Vec<FundingHistoryRest>, String> {
    fetch_kucoin_json_query::<Vec<FundingHistoryRest>, _>(
        client,
        &format!("{PERP_REST_BASE}/api/v1/contract/funding-rates"),
        &FundingHistoryQuery {
            symbol,
            from_ms,
            to_ms,
        },
    )
    .await
}

async fn connect_kucoin_socket(
    client: &Client,
    market: WsMarket,
) -> Result<(KucoinSocket, Duration), String> {
    let bullet = post_kucoin_json::<BulletData>(client, market.bullet_url()).await?;
    let server = bullet.instance_servers.first().ok_or_else(|| {
        format!(
            "{} websocket token response missing instance server",
            market.label()
        )
    })?;
    let connect_id = format!("{}-{}", market.label(), now_ms());
    let url = format!(
        "{}?token={}&connectId={connect_id}",
        server.endpoint, bullet.token
    );
    let ping_interval_ms = (server.ping_interval.max(1_000) / 2).max(1_000) as u64;
    let (socket, _) = connect_async(url)
        .await
        .map_err(|err| format!("{} websocket connect failed: {err}", market.label()))?;

    Ok((socket, Duration::from_millis(ping_interval_ms)))
}

async fn subscribe_topics(socket: &mut KucoinSocket, topics: &[String]) -> Result<(), String> {
    for topic in topics {
        let payload = serde_json::to_string(&KucoinWsSubscribeRequest {
            id: now_ms().to_string(),
            kind: "subscribe",
            topic,
            response: true,
        })
        .map_err(|err| format!("subscribe payload encode failed: {err}"))?;

        socket
            .send(Message::Text(payload.into()))
            .await
            .map_err(|err| format!("subscribe write failed: {err}"))?;
    }

    Ok(())
}

async fn send_ping(socket: &mut KucoinSocket) -> Result<(), String> {
    let payload = serde_json::to_string(&KucoinWsPingRequest {
        id: now_ms().to_string(),
        kind: "ping",
    })
    .map_err(|err| format!("ping payload encode failed: {err}"))?;

    socket
        .send(Message::Text(payload.into()))
        .await
        .map_err(|err| format!("ping write failed: {err}"))
}

fn is_control_payload(payload: &str) -> bool {
    payload.contains("\"type\":\"welcome\"")
        || payload.contains("\"type\":\"ack\"")
        || payload.contains("\"type\":\"pong\"")
}

fn is_error_payload(payload: &str) -> bool {
    payload.contains("\"type\":\"error\"")
}

fn lookup_next_settle_ms(state: &SharedNextSettleMap, symbol: &str) -> Option<i64> {
    state
        .read()
        .ok()
        .and_then(|state| state.get(symbol).copied())
}

fn extract_topic_symbol(topic: &str) -> Option<&str> {
    topic.rsplit_once(':').map(|(_, symbol)| symbol)
}

async fn fetch_kucoin_json<T>(client: &Client, url: &str) -> Result<T, String>
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
    let payload = response
        .json::<KucoinResponse<T>>()
        .await
        .map_err(|err| format!("decode {url} failed: {err}"))?;

    if payload.code != "200000" {
        return Err(format!("request {url} returned code {}", payload.code));
    }

    Ok(payload.data)
}

async fn fetch_kucoin_json_query<T, Q>(client: &Client, url: &str, query: &Q) -> Result<T, String>
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
        .json::<KucoinResponse<T>>()
        .await
        .map_err(|err| format!("decode {url} failed: {err}"))?;

    if payload.code != "200000" {
        return Err(format!("request {url} returned code {}", payload.code));
    }

    Ok(payload.data)
}

async fn post_kucoin_json<T>(client: &Client, url: &str) -> Result<T, String>
where
    T: DeserializeOwned,
{
    let response = client
        .post(url)
        .send()
        .await
        .map_err(|err| format!("request {url} failed: {err}"))?;
    let response = response
        .error_for_status()
        .map_err(|err| format!("request {url} returned error: {err}"))?;
    let payload = response
        .json::<KucoinResponse<T>>()
        .await
        .map_err(|err| format!("decode {url} failed: {err}"))?;

    if payload.code != "200000" {
        return Err(format!("request {url} returned code {}", payload.code));
    }

    Ok(payload.data)
}

async fn send_health(tx: &EventSender, ok: bool, message: Option<String>) {
    let _ = tx
        .send(ExchangeEvent::Health {
            exchange: ExchangeId::Kucoin,
            ok,
            message,
            ts_ms: now_ms(),
        })
        .await;
}

async fn send_idle_reconnect(
    tx: &EventSender,
    stream_name: &'static str,
    last_data_at: Instant,
) -> bool {
    if last_data_at.elapsed() < MARKET_DATA_IDLE_TIMEOUT {
        return false;
    }

    send_health(
        tx,
        false,
        Some(format!(
            "kucoin {stream_name} idle for {}s; reconnecting",
            last_data_at.elapsed().as_secs()
        )),
    )
    .await;
    true
}

fn parse_f64(value: &str) -> Option<f64> {
    value.parse::<f64>().ok()
}

fn parse_i64(value: &str) -> Option<i64> {
    value.parse::<i64>().ok()
}

fn parse_f64_flex(value: Option<&FlexValue>) -> Option<f64> {
    match value? {
        FlexValue::String(value) => parse_f64(value),
        FlexValue::Integer(value) => Some(*value as f64),
        FlexValue::Float(value) => Some(*value),
    }
}

fn parse_i64_flex(value: Option<&FlexValue>) -> Option<i64> {
    match value? {
        FlexValue::String(value) => parse_i64(value),
        FlexValue::Integer(value) => Some(*value),
        FlexValue::Float(value) => Some(*value as i64),
    }
}

fn flex_ts_ms(value: Option<&FlexValue>) -> Option<i64> {
    parse_i64_flex(value).map(normalize_ts_ms)
}

fn normalize_ts_ms(raw: i64) -> i64 {
    if raw >= 10_000_000_000_000_000 {
        raw / 1_000_000
    } else if raw >= 10_000_000_000_000 {
        raw / 1_000
    } else if raw >= 1_000_000_000_000 {
        raw
    } else {
        raw * 1_000
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis() as i64)
        .unwrap_or_default()
}

#[derive(Debug, Deserialize)]
struct KucoinResponse<T> {
    code: String,
    data: T,
}

#[derive(Debug, Deserialize)]
struct SpotSymbolRest {
    symbol: String,
    #[serde(rename = "baseCurrency")]
    base_currency: String,
    #[serde(rename = "quoteCurrency")]
    quote_currency: String,
    market: String,
    #[serde(rename = "enableTrading")]
    enable_trading: bool,
}

#[derive(Debug, Deserialize)]
struct SpotAllTickersRest {
    time: i64,
    ticker: Vec<SpotTickerRest>,
}

#[derive(Debug, Deserialize)]
struct SpotTickerRest {
    symbol: String,
    buy: String,
    sell: String,
    #[serde(rename = "volValue")]
    vol_value: String,
}

#[derive(Debug, Deserialize)]
struct PerpContractRest {
    symbol: String,
    #[serde(rename = "baseCurrency")]
    base_currency: String,
    #[serde(rename = "quoteCurrency")]
    quote_currency: String,
    status: String,
    #[serde(rename = "expireDate")]
    expire_date: Option<i64>,
    #[serde(rename = "fundingFeeRate")]
    funding_fee_rate: Option<f64>,
    #[serde(rename = "nextFundingRateDateTime")]
    next_funding_rate_datetime: Option<i64>,
    #[serde(rename = "indexPrice")]
    index_price: Option<f64>,
    #[serde(rename = "turnoverOf24h")]
    turnover_of_24h: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct PerpTickerRest {
    symbol: String,
    #[serde(rename = "bestBidPrice")]
    best_bid_price: String,
    #[serde(rename = "bestAskPrice")]
    best_ask_price: String,
    ts: i64,
}

#[derive(Debug, Deserialize)]
struct FundingHistoryRest {
    #[serde(rename = "fundingRate")]
    funding_rate: f64,
    timepoint: i64,
}

#[derive(Debug, Serialize)]
struct FundingHistoryQuery<'a> {
    symbol: &'a str,
    #[serde(rename = "from")]
    from_ms: i64,
    #[serde(rename = "to")]
    to_ms: i64,
}

#[derive(Debug, Deserialize)]
struct BulletData {
    token: String,
    #[serde(rename = "instanceServers")]
    instance_servers: Vec<InstanceServer>,
}

#[derive(Debug, Deserialize)]
struct InstanceServer {
    endpoint: String,
    #[serde(rename = "pingInterval")]
    ping_interval: i64,
}

#[derive(Debug, Serialize)]
struct KucoinWsSubscribeRequest<'a> {
    id: String,
    #[serde(rename = "type")]
    kind: &'a str,
    topic: &'a str,
    response: bool,
}

#[derive(Debug, Serialize)]
struct KucoinWsPingRequest<'a> {
    id: String,
    #[serde(rename = "type")]
    kind: &'a str,
}

#[derive(Debug, Deserialize)]
struct SpotAllTickerWsMessage {
    subject: String,
    data: SpotAllTickerWsData,
}

#[derive(Debug, Deserialize)]
struct SpotAllTickerWsData {
    #[serde(rename = "bestBid")]
    best_bid: String,
    #[serde(rename = "bestAsk")]
    best_ask: String,
    time: Option<FlexValue>,
}

#[derive(Debug, Deserialize)]
struct SpotMarketSnapshotWsMessage {
    data: SpotMarketSnapshotEnvelopeData,
}

#[derive(Debug, Deserialize)]
struct SpotMarketSnapshotEnvelopeData {
    data: SpotMarketSnapshotData,
}

#[derive(Debug, Deserialize)]
struct SpotMarketSnapshotData {
    symbol: String,
    buy: Option<FlexValue>,
    sell: Option<FlexValue>,
    #[serde(rename = "volValue")]
    vol_value: Option<FlexValue>,
    datetime: Option<FlexValue>,
    #[serde(rename = "marketChange24h")]
    market_change_24h: Option<SpotMarketChange24h>,
}

#[derive(Debug, Deserialize)]
struct SpotMarketChange24h {
    #[serde(rename = "volValue")]
    vol_value: Option<FlexValue>,
}

#[derive(Debug, Deserialize)]
struct PerpTickerWsMessage {
    data: PerpTickerWsData,
}

#[derive(Debug, Deserialize)]
struct PerpTickerWsData {
    symbol: String,
    #[serde(rename = "bestBidPrice")]
    best_bid_price: String,
    #[serde(rename = "bestAskPrice")]
    best_ask_price: String,
    ts: Option<FlexValue>,
}

#[derive(Debug, Deserialize)]
struct PerpInstrumentWsMessage {
    topic: String,
    data: PerpInstrumentWsData,
}

#[derive(Debug, Deserialize)]
struct PerpInstrumentWsData {
    #[serde(rename = "indexPrice")]
    index_price: Option<FlexValue>,
    #[serde(rename = "fundingRate", alias = "fundingFeeRate")]
    funding_rate: Option<FlexValue>,
    #[serde(rename = "timestamp", alias = "ts")]
    ts: Option<FlexValue>,
}

#[derive(Debug, Deserialize)]
struct PerpSnapshotWsMessage {
    data: PerpSnapshotWsData,
}

#[derive(Debug, Deserialize)]
struct PerpSnapshotWsData {
    symbol: String,
    turnover: Option<FlexValue>,
    ts: Option<FlexValue>,
}

struct ContractStateUpdate {
    market: MarketRef,
    volume_24h: Option<f64>,
    index_price: Option<f64>,
    funding_rate: Option<f64>,
    next_settle_ms: Option<i64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum FlexValue {
    String(String),
    Integer(i64),
    Float(f64),
}

#[derive(Debug, Clone, Copy)]
enum WsMarket {
    Spot,
    Perp,
}

impl WsMarket {
    fn bullet_url(self) -> &'static str {
        match self {
            Self::Spot => "https://api.kucoin.com/api/v1/bullet-public",
            Self::Perp => "https://api-futures.kucoin.com/api/v1/bullet-public",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Spot => "spot",
            Self::Perp => "perp",
        }
    }
}
