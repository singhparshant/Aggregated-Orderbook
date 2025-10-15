#[derive(Default, Debug)]
struct AggregatedOrderBook {
    spread: f64,
    bids: BTreeMap<OrderedFloat<f64>, f64>, // asc by price
    asks: BTreeMap<OrderedFloat<f64>, f64>, // asc by price
    last_update_id: HashMap<String, u64>,
}
