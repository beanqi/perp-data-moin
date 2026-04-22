use std::collections::BTreeMap;

use crate::domain::{MarketKind, MarketRef, MonitorPair, PairKind};

pub fn build_monitor_pairs(markets: &[MarketRef]) -> Vec<MonitorPair> {
    let mut grouped = BTreeMap::<String, Vec<MarketRef>>::new();
    for market in markets.iter().cloned() {
        grouped
            .entry(market.instrument.canonical_symbol.clone())
            .or_default()
            .push(market);
    }

    let mut pairs = Vec::new();
    for (symbol, legs) in grouped {
        let mut spots = Vec::new();
        let mut perps = Vec::new();

        for leg in legs {
            match leg.instrument.market_kind {
                MarketKind::Spot => spots.push(leg),
                MarketKind::Perp => perps.push(leg),
            }
        }

        for spot in &spots {
            for perp in &perps {
                pairs.push(MonitorPair::new(
                    symbol.clone(),
                    spot.clone(),
                    perp.clone(),
                    PairKind::SpotPerp,
                ));
            }
        }

        for left_idx in 0..perps.len() {
            for right_idx in (left_idx + 1)..perps.len() {
                let left = perps[left_idx].clone();
                let right = perps[right_idx].clone();
                pairs.push(MonitorPair::new(
                    symbol.clone(),
                    left,
                    right,
                    PairKind::PerpPerp,
                ));
            }
        }
    }

    pairs
}
