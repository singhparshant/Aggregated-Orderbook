use crate::modules::types::{AggregatedOrderBook, OrderBook, OrderBookUpdate, OrderLevel};
use std::collections::{BTreeMap, HashMap, HashSet};

const PRICE_SCALE: f64 = 1_000_000_000.0; // matches examples: 100.0 -> 100_000_000_000

impl AggregatedOrderBook {
    pub fn new() -> Self {
        Self {
            spread: 0.0,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_update_id: HashMap::new(),
        }
    }

    pub fn print_top10(&self) {
        // Top 10 bids (highest first)
        println!("Top 10 bids:");
        for (price_idx, exchange_map) in self.bids.iter().rev().take(10) {
            println!("{:#?} {:#?}", price_idx, exchange_map);
        }
        println!("Top 10 asks:");
        for (price_idx, exchange_map) in self.asks.iter().take(10) {
            println!("{:#?} {:#?}", price_idx, exchange_map);
        }
    }

    pub fn get_spread(&self) -> f64 {
        self.spread
    }

    pub fn get_bids(&self) -> BTreeMap<usize, HashMap<String, OrderLevel>> {
        self.bids.clone()
    }

    pub fn get_asks(&self) -> BTreeMap<usize, HashMap<String, OrderLevel>> {
        self.asks.clone()
    }

    pub fn get_last_update_id(&self) -> HashMap<String, u64> {
        self.last_update_id.clone()
    }

    pub fn merge_snapshots(&mut self, snapshots: Vec<OrderBook>) {
        for snapshot in snapshots {
            for level in snapshot.bids.iter() {
                Self::upsert_level(&mut self.bids, level);
            }
            for level in snapshot.asks.iter() {
                Self::upsert_level(&mut self.asks, level);
            }

            let mut seen: HashSet<&'static str> = HashSet::new();
            for ex in snapshot
                .bids
                .iter()
                .map(|l| l.exchange)
                .chain(snapshot.asks.iter().map(|l| l.exchange))
            {
                if seen.insert(ex) {
                    self.last_update_id
                        .insert(ex.to_lowercase(), snapshot.last_update_id);
                }
            }
            self.print_top10();
        }

        Self::prune_top_n(&mut self.bids, 10, true);
        Self::prune_top_n(&mut self.asks, 10, false);
        self.recompute_spread();
    }

    pub fn handle_update(&mut self, update: OrderBookUpdate) {
        self.last_update_id
            .insert(update.exchange.to_lowercase(), update.update_id);
        for level in update.bids.iter() {
            Self::upsert_level_optimized(&mut self.bids, level, true);
        }
        for level in update.asks.iter() {
            Self::upsert_level_optimized(&mut self.asks, level, false);
        }
        self.recompute_spread();
    }

    pub fn get_aggregated_orderbook(&self) -> Self {
        Self {
            spread: self.spread,
            bids: self.bids.clone(),
            asks: self.asks.clone(),
            last_update_id: self.last_update_id.clone(),
        }
    }

    #[inline]
    fn price_index(price: f64) -> usize {
        (price * PRICE_SCALE).round() as usize
    }

    fn upsert_level(map: &mut BTreeMap<usize, HashMap<String, OrderLevel>>, level: &OrderLevel) {
        let idx = Self::price_index(level.price);
        let exchange_key = level.exchange.to_lowercase();

        if level.amount == 0.0 {
            if let Some(bucket) = map.get_mut(&idx) {
                bucket.remove(&exchange_key);
                if bucket.is_empty() {
                    map.remove(&idx);
                }
            }
            return;
        }

        let bucket = map.entry(idx).or_insert_with(HashMap::new);
        bucket.insert(exchange_key, level.clone());
    }

    fn upsert_level_optimized(
        map: &mut BTreeMap<usize, HashMap<String, OrderLevel>>,
        level: &OrderLevel,
        is_bids: bool,
    ) {
        let idx = Self::price_index(level.price);
        let exchange_key = level.exchange.to_lowercase();

        if level.amount == 0.0 {
            if let Some(bucket) = map.get_mut(&idx) {
                bucket.remove(&exchange_key);
                if bucket.is_empty() {
                    map.remove(&idx);
                }
            }
            return;
        }

        // Check if price level already exists in top 10
        let exists_in_top_10 = if is_bids {
            // For bids, check if this price is in the top 10 (highest prices)
            map.keys().rev().take(10).any(|&key| key == idx)
        } else {
            // For asks, check if this price is in the top 10 (lowest prices)
            map.keys().take(10).any(|&key| key == idx)
        };

        if exists_in_top_10 {
            // Just update the existing level
            if let Some(bucket) = map.get_mut(&idx) {
                bucket.insert(exchange_key, level.clone());
            }
        } else {
            // Insert new level and check if we need to prune
            let bucket = map.entry(idx).or_insert_with(HashMap::new);
            bucket.insert(exchange_key, level.clone());

            // Only prune if we exceed 10 levels
            if map.len() > 10 {
                Self::prune_top_n(map, 10, is_bids);
            }
        }
    }

    fn prune_top_n(
        map: &mut BTreeMap<usize, HashMap<String, OrderLevel>>,
        n: usize,
        is_bids: bool,
    ) {
        if map.len() <= n {
            return;
        }
        if is_bids {
            let mut keys_to_remove = Vec::new();
            for (i, key) in map.keys().enumerate() {
                if i >= map.len() - n {
                    break;
                }
                keys_to_remove.push(*key);
            }
            for key in keys_to_remove {
                map.remove(&key);
            }
        } else {
            let mut keys_to_remove = Vec::new();
            for (i, key) in map.keys().enumerate() {
                if i >= n {
                    keys_to_remove.push(*key);
                }
            }
            for key in keys_to_remove {
                map.remove(&key);
            }
        }
    }

    fn recompute_spread(&mut self) {
        // A BTreeMap sorts keys from low to high.
        // To get the best bid (highest price), we reverse the iterator and take the first item.
        let best_bid = self.bids.keys().rev().next().copied();

        // To get the best ask (lowest price), we just take the first item.
        let best_ask = self.asks.keys().next().copied();

        self.spread = if let (Some(bid_price), Some(ask_price)) = (best_bid, best_ask) {
            (ask_price as f64 - bid_price as f64) / PRICE_SCALE
        } else {
            0.0
        };
    }
}
