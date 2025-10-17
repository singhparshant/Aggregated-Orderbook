use crate::modules::types::{AggregatedOrderBook, OrderBook, OrderBookUpdate, OrderLevel};
use std::collections::{BTreeMap, HashMap, HashSet};

const PRICE_SCALE: f64 = 1_000_000_000.0;

#[derive(Clone, Debug)]
pub struct Top10Snapshot {
    pub spread: f64,
    pub bids: Vec<OrderLevel>,
    pub asks: Vec<OrderLevel>,
}

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

        if let Err(e) = self.try_recompute_spread() {
            tracing::error!("Failed to recompute spread: {}", e);
        }
    }

    /// Handle update with robust error handling and retries
    pub fn handle_update(&mut self, update: OrderBookUpdate) -> Result<(), String> {
        let max_retries = 3;
        let mut retry_count = 0;

        while retry_count < max_retries {
            match self.try_apply_update(&update) {
                Ok(_) => {
                    tracing::debug!(
                        "Successfully applied update for {} (ID: {})",
                        update.exchange,
                        update.update_id
                    );
                    return Ok(());
                }
                Err(e) => {
                    retry_count += 1;
                    tracing::warn!(
                        "Failed to apply update for {} (ID: {}), attempt {}/{}: {}",
                        update.exchange,
                        update.update_id,
                        retry_count,
                        max_retries,
                        e
                    );

                    if retry_count < max_retries {
                        // Small delay before retry
                        std::thread::sleep(std::time::Duration::from_millis(2));
                    }
                }
            }
        }

        Err(format!(
            "Failed to apply update for {} (ID: {}) after {} retries",
            update.exchange, update.update_id, max_retries
        ))
    }

    /// Try to apply update with error handling
    fn try_apply_update(&mut self, update: &OrderBookUpdate) -> Result<(), String> {
        // Validate update data
        self.validate_update(update)?;

        // Update last update ID
        self.last_update_id
            .insert(update.exchange.to_lowercase(), update.update_id);

        // Debug: Log update details
        tracing::debug!(
            "Applying {} update (ID: {}): {} bids, {} asks",
            update.exchange,
            update.update_id,
            update.bids.len(),
            update.asks.len()
        );

        // Apply bids with error handling and detailed logging
        for level in update.bids.iter() {
            let old_count = self.bids.len();
            if let Err(e) = Self::try_upsert_level(&mut self.bids, level) {
                tracing::error!(
                    "Failed to upsert bid level: {} (price: {}, amount: {})",
                    e,
                    level.price,
                    level.amount
                );
                return Err(format!("Failed to upsert bid level: {}", e));
            }
            let new_count = self.bids.len();
            tracing::debug!(
                "Bid update: {} {} -> {} (price: {}, amount: {})",
                update.exchange,
                level.price,
                level.amount,
                old_count,
                new_count
            );
        }

        // Apply asks with error handling and detailed logging
        for level in update.asks.iter() {
            let old_count = self.asks.len();
            if let Err(e) = Self::try_upsert_level(&mut self.asks, level) {
                tracing::error!(
                    "Failed to upsert ask level: {} (price: {}, amount: {})",
                    e,
                    level.price,
                    level.amount
                );
                return Err(format!("Failed to upsert ask level: {}", e));
            }
            let new_count = self.asks.len();
            tracing::debug!(
                "Ask update: {} {} -> {} (price: {}, amount: {})",
                update.exchange,
                level.price,
                level.amount,
                old_count,
                new_count
            );
        }

        // Recompute spread with error handling
        if let Err(e) = self.try_recompute_spread() {
            return Err(format!("Failed to recompute spread: {}", e));
        }

        // Debug: Log final state
        tracing::debug!(
            "Update complete: {} total bids, {} total asks, spread: {}",
            self.bids.len(),
            self.asks.len(),
            self.spread
        );

        Ok(())
    }

    /// Validate update data
    fn validate_update(&self, update: &OrderBookUpdate) -> Result<(), String> {
        // Check for invalid prices
        for level in update.bids.iter().chain(update.asks.iter()) {
            if level.price < 0.0 {
                return Err(format!("Invalid price: {}", level.price));
            }
            if level.amount < 0.0 {
                return Err(format!("Invalid amount: {}", level.amount));
            }
        }

        // Validate update ID sequencing
        let exchange_key = update.exchange.to_lowercase();
        if let Some(&last_id) = self.last_update_id.get(&exchange_key) {
            match update.exchange {
                "binance" => {
                    // For Binance, the "u" value (final update ID) should be greater than our last update ID
                    if update.update_id <= last_id {
                        tracing::warn!(
                            "Binance update ID {} is not greater than last ID {}",
                            update.update_id,
                            last_id
                        );
                        return Ok(());
                    }
                }
                "bitstamp" => {
                    // For Bitstamp, the update ID should be greater than our last update ID
                    if update.update_id <= last_id {
                        tracing::warn!(
                            "Bitstamp update ID {} is not greater than last ID {}",
                            update.update_id,
                            last_id
                        );
                        return Ok(());
                    }
                }
                _ => {
                    // For other exchanges, just ensure it's greater
                    if update.update_id <= last_id {
                        return Ok(());
                    }
                }
            }
        }

        Ok(())
    }

    /// Try to upsert level with error handling
    fn try_upsert_level(
        map: &mut BTreeMap<usize, HashMap<String, OrderLevel>>,
        level: &OrderLevel,
    ) -> Result<(), String> {
        let idx = Self::price_index(level.price);
        let exchange_key = level.exchange.to_lowercase();

        if level.amount == 0.0 {
            // Remove level
            if let Some(bucket) = map.get_mut(&idx) {
                bucket.remove(&exchange_key);
                if bucket.is_empty() {
                    map.remove(&idx);
                }
            }
        } else {
            // Insert or update level
            let bucket = map.entry(idx).or_insert_with(HashMap::new);
            bucket.insert(exchange_key, level.clone());
        }

        Ok(())
    }

    /// Try to recompute spread with error handling
    fn try_recompute_spread(&mut self) -> Result<(), String> {
        let best_bid_idx = self.bids.keys().rev().next().copied().unwrap_or(0);
        let best_ask_idx = self.asks.keys().next().copied().unwrap_or(0);

        // Get the best bid price and exchange
        let best_bid_price = best_bid_idx as f64 / PRICE_SCALE;
        let best_bid_exchanges: Vec<String> = self
            .bids
            .get(&best_bid_idx)
            .map(|exchange_map| exchange_map.keys().cloned().collect())
            .unwrap_or_default();

        // Get the best ask price and exchange
        let best_ask_price = best_ask_idx as f64 / PRICE_SCALE;
        let best_ask_exchanges: Vec<String> = self
            .asks
            .get(&best_ask_idx)
            .map(|exchange_map| exchange_map.keys().cloned().collect())
            .unwrap_or_default();

        println!(
            "Best bid: {:.8} (exchanges: {:?})",
            best_bid_price, best_bid_exchanges
        );
        println!(
            "Best ask: {:.8} (exchanges: {:?})",
            best_ask_price, best_ask_exchanges
        );
        self.spread = (best_ask_idx as f64 - best_bid_idx as f64) / PRICE_SCALE;
        println!("Spread: {:.8}", self.spread);

        Ok(())
    }

    pub fn get_aggregated_orderbook(&self) -> Self {
        Self {
            spread: self.spread,
            bids: self.bids.clone(),
            asks: self.asks.clone(),
            last_update_id: self.last_update_id.clone(),
        }
    }

    pub fn get_top10_snapshot(&self) -> Top10Snapshot {
        // Write price levels and exchanges for bids to file
        // let mut output = String::new();
        // output.push_str("Bids (price level -> exchanges):\n");
        // for (price_idx, exchange_map) in self.bids.iter() {
        //     let exchanges: Vec<String> = exchange_map.keys().cloned().collect();
        //     output.push_str(&format!(
        //         "  Price Level {}: Exchanges {:?}\n",
        //         price_idx, exchanges
        //     ));
        // }

        // // Write price levels and exchanges for asks to file
        // output.push_str("Asks (price level -> exchanges):\n");
        // for (price_idx, exchange_map) in self.asks.iter() {
        //     let exchanges: Vec<String> = exchange_map.keys().cloned().collect();
        //     output.push_str(&format!(
        //         "  Price Level {}: Exchanges {:?}\n",
        //         price_idx, exchanges
        //     ));
        // }
        // output.push_str(&format!("Spread: {:#?}\n", self.spread));

        // Write to file
        // if let Err(e) = std::fs::write("out.txt", output) {
        //     eprintln!("Failed to write to out.txt: {}", e);
        // }

        // Get top 10 price levels for bids (highest prices first)
        let bid_levels: Vec<OrderLevel> = self
            .bids
            .iter()
            .rev()
            .take(10) // Take first 10 price levels
            .flat_map(|(_, exchange_map)| exchange_map.values().cloned())
            .collect();

        // Get top 10 price levels for asks (lowest prices first)
        let ask_levels: Vec<OrderLevel> = self
            .asks
            .iter()
            .take(10) // Take first 10 price levels
            .flat_map(|(_, exchange_map)| exchange_map.values().cloned())
            .collect();

        Top10Snapshot {
            spread: self.spread,
            bids: bid_levels,
            asks: ask_levels,
        }
    }

    // Removed invalid helper that attempted to return Vec<OrderLevel> into BTreeMap fields

    #[inline]
    fn price_index(price: f64) -> usize {
        // Use a more precise method to avoid precision loss
        // Convert to string with fixed precision, then parse back
        let scaled = (price * PRICE_SCALE).round();
        if scaled.is_finite() && scaled >= 0.0 {
            scaled as usize
        } else {
            // Fallback for edge cases
            (price * PRICE_SCALE).round() as usize
        }
    }

    // Insert or update a level in the orderbook. If the level amount is 0, remove the level.
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::types::{Exchange, OrderBook, OrderLevel};

    fn make_snapshot(exchange: Exchange) -> OrderBook {
        // Create 20 bid levels descending from 100.0, and 20 ask levels ascending from 100.5.
        // Prices are identical across exchanges so buckets should merge under the same price index.
        let bids: Vec<OrderLevel> = (0..20)
            .map(|i| OrderLevel {
                exchange: exchange.as_str(),
                price: 100.0 - (i as f64) * 0.01,
                amount: 1.0 + (i as f64) * 0.1,
            })
            .collect();
        let asks: Vec<OrderLevel> = (0..20)
            .map(|i| OrderLevel {
                exchange: exchange.as_str(),
                price: 100.5 + (i as f64) * 0.01,
                amount: 2.0 + (i as f64) * 0.05,
            })
            .collect();
        OrderBook {
            last_update_id: match exchange {
                Exchange::Binance => 111,
                Exchange::Bitstamp => 222,
            },
            bids,
            asks,
        }
    }

    #[test]
    fn merge_snapshots_keeps_all_levels_and_combines_exchanges() {
        let mut agg = AggregatedOrderBook::new();
        let binance = make_snapshot(Exchange::Binance);
        let bitstamp = make_snapshot(Exchange::Bitstamp);

        agg.merge_snapshots(vec![binance, bitstamp]);

        // Keep all levels (20 per side from each exchange)
        assert!(agg.bids.len() == 20);
        assert!(agg.asks.len() == 20);

        // Spread derived from best bid/ask indices
        let best_bid_idx = *agg.bids.keys().rev().next().expect("best bid idx");
        let best_ask_idx = *agg.asks.keys().next().expect("best ask idx");
        let expected_spread = (best_ask_idx as f64 - best_bid_idx as f64) / PRICE_SCALE;
        assert!((agg.spread - expected_spread).abs() < 1e-12);

        // Buckets at best levels include both exchanges
        let bid_bucket = agg.bids.get(&best_bid_idx).expect("bid bucket");
        assert!(bid_bucket.contains_key("binance"));
        assert!(bid_bucket.contains_key("bitstamp"));
        let ask_bucket = agg.asks.get(&best_ask_idx).expect("ask bucket");
        assert!(ask_bucket.contains_key("binance"));
        assert!(ask_bucket.contains_key("bitstamp"));

        // last_update_id per exchange set from snapshots
        let last_ids = agg.last_update_id;
        assert_eq!(last_ids.get("binance"), Some(&111));
        assert_eq!(last_ids.get("bitstamp"), Some(&222));
    }

    #[test]
    fn get_top10_methods_return_correct_levels() {
        let mut agg = AggregatedOrderBook::new();

        // Create a snapshot with 25 bid levels and 25 ask levels
        let mut bids = Vec::new();
        let mut asks = Vec::new();

        // Create 25 bid levels (prices 100.0 down to 99.76)
        for i in 0..25 {
            bids.push(OrderLevel {
                exchange: Exchange::Binance.as_str(),
                price: 100.0 - (i as f64) * 0.01,
                amount: 1.0 + (i as f64) * 0.1,
            });
        }

        // Create 25 ask levels (prices 100.5 up to 100.74)
        for i in 0..25 {
            asks.push(OrderLevel {
                exchange: Exchange::Binance.as_str(),
                price: 100.5 + (i as f64) * 0.01,
                amount: 2.0 + (i as f64) * 0.05,
            });
        }

        let snapshot = OrderBook {
            last_update_id: 111,
            bids,
            asks,
        };

        agg.merge_snapshots(vec![snapshot]);

        // Should keep all 25 levels each
        assert_eq!(agg.bids.len(), 25, "Bids should have all 25 levels");
        assert_eq!(agg.asks.len(), 25, "Asks should have all 25 levels");

        // Test get_top10_bids returns highest 10 prices
        let top10_bids = agg.get_top10_snapshot().bids;
        assert_eq!(
            top10_bids.len(),
            10,
            "get_top10_bids should return 10 levels"
        );

        // Verify the highest bid price is 100.0
        let highest_bid = top10_bids
            .iter()
            .max_by(|a, b| a.price.partial_cmp(&b.price).unwrap())
            .unwrap();
        assert_eq!(highest_bid.price, 100.0);

        // Test get_top10_asks returns lowest 10 prices
        let top10_asks = agg.get_top10_snapshot().asks;
        assert_eq!(
            top10_asks.len(),
            10,
            "get_top10_asks should return 10 levels"
        );

        // Verify the lowest ask price is 100.5
        let lowest_ask = top10_asks
            .iter()
            .min_by(|a, b| a.price.partial_cmp(&b.price).unwrap())
            .unwrap();
        assert_eq!(lowest_ask.price, 100.5);
    }
}
