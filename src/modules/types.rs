use serde_json::Value;
use std::collections::{BTreeMap, HashMap};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Exchange {
    Binance,
    Bitstamp,
}

impl Exchange {
    pub fn as_str(&self) -> &'static str {
        match self {
            Exchange::Binance => "binance",
            Exchange::Bitstamp => "bitstamp",
        }
    }
}

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
    pub bids: BTreeMap<usize, HashMap<String, OrderLevel>>, // price index -> { exchange -> level }
    pub asks: BTreeMap<usize, HashMap<String, OrderLevel>>, // price index -> { exchange -> level }
    pub last_update_id: HashMap<String, u64>,
}

#[derive(Default, Debug)]
pub struct OrderBookUpdate {
    pub exchange: &'static str,
    pub update_id: u64,
    pub bids: Vec<OrderLevel>,
    pub asks: Vec<OrderLevel>,
}

impl OrderBookUpdate {
    pub fn from_json(text: String) -> Self {
        let v: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
        if let Some(u) = Self::parse_binance_diff(&v) {
            return u;
        }
        if let Some(u) = Self::parse_binance_snapshot(&v) {
            return u;
        }
        if let Some(u) = Self::parse_bitstamp(&v) {
            return u;
        }
        Self {
            exchange: Exchange::Binance.as_str(),
            update_id: 0,
            bids: vec![],
            asks: vec![],
        }
    }

    pub fn from_binance_json(text: &str) -> Option<Self> {
        let v: Value = serde_json::from_str(text).ok()?;
        Self::parse_binance_diff(&v)
    }

    pub fn from_bitstamp_json(text: &str) -> Option<Self> {
        let v: Value = serde_json::from_str(text).ok()?;
        Self::parse_bitstamp(&v)
    }

    fn parse_binance_diff(v: &Value) -> Option<Self> {
        let bids = v.get("b")?.as_array()?;
        let asks = v.get("a")?.as_array()?;
        let update_id = v.get("u").and_then(|x| x.as_u64()).unwrap_or(0);
        let bids = bids
            .iter()
            .filter_map(|arr| {
                let price = arr.get(0).and_then(|x| x.as_str())?.parse::<f64>().ok()?;
                let amount = arr.get(1).and_then(|x| x.as_str())?.parse::<f64>().ok()?;
                Some(OrderLevel {
                    exchange: Exchange::Binance.as_str(),
                    price,
                    amount,
                })
            })
            .collect();
        let asks = asks
            .iter()
            .filter_map(|arr| {
                let price = arr.get(0).and_then(|x| x.as_str())?.parse::<f64>().ok()?;
                let amount = arr.get(1).and_then(|x| x.as_str())?.parse::<f64>().ok()?;
                Some(OrderLevel {
                    exchange: Exchange::Binance.as_str(),
                    price,
                    amount,
                })
            })
            .collect();
        Some(Self {
            exchange: Exchange::Binance.as_str(),
            update_id,
            bids,
            asks,
        })
    }

    fn parse_binance_snapshot(v: &Value) -> Option<Self> {
        let bids = v.get("bids")?.as_array()?;
        let asks = v.get("asks")?.as_array()?;
        let update_id = v.get("lastUpdateId").and_then(|x| x.as_u64()).unwrap_or(0);
        let bids = bids
            .iter()
            .filter_map(|arr| {
                let price = arr.get(0).and_then(|x| x.as_str())?.parse::<f64>().ok()?;
                let amount = arr.get(1).and_then(|x| x.as_str())?.parse::<f64>().ok()?;
                Some(OrderLevel {
                    exchange: Exchange::Binance.as_str(),
                    price,
                    amount,
                })
            })
            .collect();
        let asks = asks
            .iter()
            .filter_map(|arr| {
                let price = arr.get(0).and_then(|x| x.as_str())?.parse::<f64>().ok()?;
                let amount = arr.get(1).and_then(|x| x.as_str())?.parse::<f64>().ok()?;
                Some(OrderLevel {
                    exchange: Exchange::Binance.as_str(),
                    price,
                    amount,
                })
            })
            .collect();
        Some(Self {
            exchange: Exchange::Binance.as_str(),
            update_id,
            bids,
            asks,
        })
    }

    fn parse_bitstamp(v: &Value) -> Option<Self> {
        if v.get("event").and_then(|e| e.as_str())? != "data" {
            return None;
        }
        let data = v.get("data")?;
        let update_id = data
            .get("microtimestamp")
            .and_then(|x| x.as_str())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);
        let bids = data
            .get("bids")?
            .as_array()?
            .iter()
            .filter_map(|arr| {
                let price = arr.get(0).and_then(|x| x.as_str())?.parse::<f64>().ok()?;
                let amount = arr.get(1).and_then(|x| x.as_str())?.parse::<f64>().ok()?;
                Some(OrderLevel {
                    exchange: Exchange::Bitstamp.as_str(),
                    price,
                    amount,
                })
            })
            .collect();
        let asks = data
            .get("asks")?
            .as_array()?
            .iter()
            .filter_map(|arr| {
                let price = arr.get(0).and_then(|x| x.as_str())?.parse::<f64>().ok()?;
                let amount = arr.get(1).and_then(|x| x.as_str())?.parse::<f64>().ok()?;
                Some(OrderLevel {
                    exchange: Exchange::Bitstamp.as_str(),
                    price,
                    amount,
                })
            })
            .collect();
        Some(Self {
            exchange: Exchange::Bitstamp.as_str(),
            update_id,
            bids,
            asks,
        })
    }
}
