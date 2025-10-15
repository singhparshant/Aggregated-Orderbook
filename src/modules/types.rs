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
