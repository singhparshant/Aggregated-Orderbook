use keyrock_mm_rust_task::modules::types::{
    AggregatedOrderBook, Exchange, OrderBook, OrderBookUpdate, OrderLevel,
};

fn make_snapshot(exchange: Exchange) -> OrderBook {
    // 20 bids from 100.00 down by 0.01, 20 asks from 100.50 up by 0.01.
    let bids: Vec<OrderLevel> = (0..20)
        .map(|i| OrderLevel {
            exchange: exchange.as_str(),
            price: 100.00 - (i as f64) * 0.01,
            amount: 1.0 + i as f64 * 0.1,
        })
        .collect();
    let asks: Vec<OrderLevel> = (0..20)
        .map(|i| OrderLevel {
            exchange: exchange.as_str(),
            price: 100.50 + (i as f64) * 0.01,
            amount: 2.0 + i as f64 * 0.05,
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

fn build_book() -> AggregatedOrderBook {
    let mut agg = AggregatedOrderBook::new();
    let binance = make_snapshot(Exchange::Binance);
    let bitstamp = make_snapshot(Exchange::Bitstamp);
    agg.merge_snapshots(vec![binance, bitstamp]);
    agg
}

#[test]
fn merge_snapshots_keeps_all_levels_and_combines_exchanges() {
    let agg = build_book();

    assert_eq!(agg.bids.len(), 20);
    assert_eq!(agg.asks.len(), 20);

    // Buckets at best levels should include both exchanges (prices identical across exchanges)
    let best_bid_idx = *agg.bids.keys().rev().next().expect("best bid idx");
    let best_ask_idx = *agg.asks.keys().next().expect("best ask idx");
    let bid_bucket = agg.bids.get(&best_bid_idx).unwrap();
    let ask_bucket = agg.asks.get(&best_ask_idx).unwrap();
    assert!(bid_bucket.contains_key("binance"));
    assert!(bid_bucket.contains_key("bitstamp"));
    assert!(ask_bucket.contains_key("binance"));
    assert!(ask_bucket.contains_key("bitstamp"));

    // Spread sanity: derive from best prices inside the buckets (use stored f64s)
    let best_bid_price = bid_bucket.values().next().unwrap().price;
    let best_ask_price = ask_bucket.values().next().unwrap().price;
    let expected_spread = best_ask_price - best_bid_price;
    println!("expected_spread: {}", expected_spread);
    println!("agg.get_spread(): {}", agg.get_spread());
    assert!((agg.get_spread() - expected_spread).abs() < 1e-9);
}

#[test]
fn update_inserts_new_levels_and_keeps_all() {
    let mut agg = build_book();

    // Record previous counts
    let prev_bid_count = agg.bids.len();
    let prev_ask_count = agg.asks.len();
    let prev_best_bid_price = {
        let idx = *agg.bids.keys().rev().next().unwrap();
        agg.bids.get(&idx).unwrap().values().next().unwrap().price
    };
    let prev_best_ask_price = {
        let idx = *agg.asks.keys().next().unwrap();
        agg.asks.get(&idx).unwrap().values().next().unwrap().price
    };

    // 1) Insert a new top bid above current best → should become new best, size increases
    let new_top_bid_price = prev_best_bid_price + 0.05;
    let bid_update = OrderBookUpdate {
        exchange: Exchange::Binance.as_str(),
        update_id: 1000,
        bids: vec![OrderLevel {
            exchange: Exchange::Binance.as_str(),
            price: new_top_bid_price,
            amount: 3.14,
        }],
        asks: vec![],
    };
    agg.handle_update(bid_update);

    assert_eq!(agg.bids.len(), prev_bid_count + 1);
    // New best bid price present
    let best_bid_idx_after = *agg.bids.keys().rev().next().unwrap();
    let best_bid_bucket = agg.bids.get(&best_bid_idx_after).unwrap();
    let any_level = best_bid_bucket.values().next().unwrap();
    assert!((any_level.price - new_top_bid_price).abs() < 1e-12);

    // 2) Insert a new top ask below current best → should become new best ask, size increases
    let new_top_ask_price = prev_best_ask_price - 0.05;
    let ask_update = OrderBookUpdate {
        exchange: Exchange::Bitstamp.as_str(),
        update_id: 2000,
        bids: vec![],
        asks: vec![OrderLevel {
            exchange: Exchange::Bitstamp.as_str(),
            price: new_top_ask_price,
            amount: 1.11,
        }],
    };
    agg.handle_update(ask_update);

    assert_eq!(agg.asks.len(), prev_ask_count + 1);
    let best_ask_idx_after = *agg.asks.keys().next().unwrap();
    let best_ask_bucket = agg.asks.get(&best_ask_idx_after).unwrap();
    let any_ask = best_ask_bucket.values().next().unwrap();
    assert!((any_ask.price - new_top_ask_price).abs() < 1e-12);
}

#[test]
fn update_existing_amount_changes() {
    let mut agg = build_book();
    // Pick the best bid level
    let best_bid_idx = *agg.bids.keys().rev().next().unwrap();
    let old_bucket = agg.bids.get(&best_bid_idx).unwrap();
    let old_price = old_bucket.values().next().unwrap().price;

    // Change amount for Binance on this price
    let new_amount = 9.99;
    let upd = OrderBookUpdate {
        exchange: Exchange::Binance.as_str(),
        update_id: 3000,
        bids: vec![OrderLevel {
            exchange: Exchange::Binance.as_str(),
            price: old_price,
            amount: new_amount,
        }],
        asks: vec![],
    };
    agg.handle_update(upd);

    let bucket = agg.bids.get(&best_bid_idx).unwrap();
    let updated = bucket.get("binance").unwrap();
    assert!((updated.amount - new_amount).abs() < 1e-12);
}

#[test]
fn update_same_price_adds_second_exchange_and_creates_if_missing() {
    // Start from a single-exchange snapshot so we can add the other exchange at the same price
    let mut agg = AggregatedOrderBook::new();
    let only_binance = make_snapshot(Exchange::Binance);
    agg.merge_snapshots(vec![only_binance]);
    assert_eq!(agg.bids.len(), 20);
    assert_eq!(agg.asks.len(), 20);

    // Take best bid price and add Bitstamp level at the same price
    let best_bid_idx = *agg.bids.keys().rev().next().unwrap();
    let best_bid_price = agg
        .bids
        .get(&best_bid_idx)
        .unwrap()
        .values()
        .next()
        .unwrap()
        .price;
    let upd_same_price = OrderBookUpdate {
        exchange: Exchange::Bitstamp.as_str(),
        update_id: 4000,
        bids: vec![OrderLevel {
            exchange: Exchange::Bitstamp.as_str(),
            price: best_bid_price,
            amount: 7.77,
        }],
        asks: vec![],
    };
    agg.handle_update(upd_same_price);
    let bucket = agg.bids.get(&best_bid_idx).unwrap();
    assert!(bucket.contains_key("binance"));
    assert!(bucket.contains_key("bitstamp"));

    // Now add a brand new price within top-10 range for asks for both exchanges; should create bucket and hold both
    let best_ask_idx = *agg.asks.keys().next().unwrap();
    let new_ask_price = agg
        .asks
        .get(&best_ask_idx)
        .unwrap()
        .values()
        .next()
        .unwrap()
        .price
        - 0.02;
    let upd_ask_binance = OrderBookUpdate {
        exchange: Exchange::Binance.as_str(),
        update_id: 5000,
        bids: vec![],
        asks: vec![OrderLevel {
            exchange: Exchange::Binance.as_str(),
            price: new_ask_price,
            amount: 1.23,
        }],
    };
    let upd_ask_bitstamp = OrderBookUpdate {
        exchange: Exchange::Bitstamp.as_str(),
        update_id: 5001,
        bids: vec![],
        asks: vec![OrderLevel {
            exchange: Exchange::Bitstamp.as_str(),
            price: new_ask_price,
            amount: 4.56,
        }],
    };
    agg.handle_update(upd_ask_binance);
    agg.handle_update(upd_ask_bitstamp);

    // Verify the lowest ask price is the new one and has both exchanges
    let best_ask_idx_after = *agg.asks.keys().next().unwrap();
    let best_ask_bucket = agg.asks.get(&best_ask_idx_after).unwrap();
    let any_price = best_ask_bucket.values().next().unwrap().price;
    assert!((any_price - new_ask_price).abs() < 1e-12);
    assert!(best_ask_bucket.contains_key("binance"));
    assert!(best_ask_bucket.contains_key("bitstamp"));
}
