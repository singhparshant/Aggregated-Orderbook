use crate::modules::types::{AggregatedOrderBook, OrderBook, OrderBookUpdate, OrderLevel};
use std::collections::{BTreeMap, HashMap};

impl AggregatedOrderBook {
    pub fn new() -> Self {
        Self {
            spread: 0.0,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_update_id: HashMap::new(),
        }
    }

    pub fn update(&mut self, update: OrderBookUpdate) {
        self.last_update_id
            .insert(update.exchange.to_string(), update.update_id);
        self.bids.insert(update.update_id, update.bids);
        self.asks.insert(update.update_id, update.asks);
    }

    pub fn get_spread(&self) -> f64 {
        self.spread
    }

    pub fn get_bids(&self) -> BTreeMap<u64, Vec<OrderLevel>> {
        self.bids.clone()
    }

    pub fn get_asks(&self) -> BTreeMap<u64, Vec<OrderLevel>> {
        self.asks.clone()
    }

    pub fn get_last_update_id(&self) -> HashMap<String, u64> {
        self.last_update_id.clone()
    }

    pub fn merge_snapshots(&mut self, snapshots: Vec<OrderBook>) {
        // not implemented
    }

    pub fn handle_update(&mut self, update: OrderBookUpdate) {
        // not implemented
    }

    pub fn get_aggregated_orderbook(&self) -> Self {
        Self {
            spread: self.spread,
            bids: self.bids.clone(),
            asks: self.asks.clone(),
            last_update_id: self.last_update_id.clone(),
        }
    }
}
