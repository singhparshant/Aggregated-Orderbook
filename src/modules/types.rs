use std::collections::{BTreeMap, HashMap};

#[derive(Clone, Debug, Default)]
pub struct OrderBook {
    pub last_update_id: u64,
    pub bids: Vec<OrderLevel>,
    pub asks: Vec<OrderLevel>,
}

#[derive(Clone, Debug)]
pub struct OrderLevel {
    pub exchange: &'static str,
    pub price: f64,
    pub amount: f64,
}

#[derive(Default, Debug)]
pub struct AggregatedOrderBook {
    pub spread: f64,
    pub bids: BTreeMap<u64, Vec<OrderLevel>>, // asc by price
    pub asks: BTreeMap<u64, Vec<OrderLevel>>, // asc by price
    pub last_update_id: HashMap<String, u64>,
}

#[derive(Default, Debug)]
pub struct OrderBookUpdate {
    pub exchange: &'static str,
    pub update_id: u64,
    pub bids: Vec<OrderLevel>,
    pub asks: Vec<OrderLevel>,
}
