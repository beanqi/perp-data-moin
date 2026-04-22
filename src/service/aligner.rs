use crate::domain::{DiscoveredMarket, InstrumentKey, MarketKind, MarketRef};

pub struct SymbolAligner {
    known_quotes: Vec<String>,
}

impl SymbolAligner {
    pub fn new() -> Self {
        let mut known_quotes = vec![
            "USDT".to_owned(),
            "USDC".to_owned(),
            "USD".to_owned(),
            "BTC".to_owned(),
            "ETH".to_owned(),
        ];
        known_quotes.sort_by_key(|value| std::cmp::Reverse(value.len()));

        Self { known_quotes }
    }

    pub fn align(&self, market: &DiscoveredMarket) -> Option<MarketRef> {
        let (base, quote) = self.parse_symbol(&market.raw_symbol, market.market_kind)?;
        Some(MarketRef {
            exchange: market.exchange,
            market_id: market.market_id.clone(),
            raw_symbol: market.raw_symbol.clone(),
            instrument: InstrumentKey::new(base, quote, market.market_kind),
        })
    }

    fn parse_symbol(&self, raw_symbol: &str, market_kind: MarketKind) -> Option<(String, String)> {
        let uppercase = raw_symbol.trim().to_uppercase();
        let replaced = uppercase.replace(['-', '_', ':'], "/");
        let mut tokens = replaced
            .split('/')
            .filter(|token| !token.is_empty())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();

        while matches!(
            tokens.last().map(|token| token.as_str()),
            Some("SPOT" | "SWAP" | "PERP" | "FUTURE" | "FUTURES")
        ) {
            tokens.pop();
        }

        if tokens.len() >= 2 {
            let base = tokens[0].to_owned();
            let quote = tokens[1].to_owned();
            return if base == quote || base.is_empty() || quote.is_empty() {
                None
            } else {
                Some((base, quote))
            };
        }

        let compact = uppercase
            .chars()
            .filter(|value| value.is_ascii_alphanumeric())
            .collect::<String>();

        self.parse_compact_symbol(&compact, market_kind)
    }

    fn parse_compact_symbol(
        &self,
        compact_symbol: &str,
        market_kind: MarketKind,
    ) -> Option<(String, String)> {
        if compact_symbol.is_empty() {
            return None;
        }

        if let Some(result) = self.try_known_quotes(compact_symbol) {
            return Some(result);
        }

        if market_kind == MarketKind::Perp && compact_symbol.ends_with('M') {
            return self.try_known_quotes(&compact_symbol[..compact_symbol.len() - 1]);
        }

        None
    }

    fn try_known_quotes(&self, symbol: &str) -> Option<(String, String)> {
        for quote in &self.known_quotes {
            if symbol.len() <= quote.len() || !symbol.ends_with(quote) {
                continue;
            }

            let base = symbol[..symbol.len() - quote.len()].to_owned();
            let quote = quote.to_owned();
            if !base.is_empty() && base != quote {
                return Some((base, quote));
            }
        }

        None
    }
}
