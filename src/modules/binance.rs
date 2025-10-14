use ordered_float::OrderedFloat;
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use futures_util::StreamExt;
use serde_json::Value;
use tokio::sync::Mutex;
use tokio::time::{Duration, sleep};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::{Message, Result};

#[derive(Clone, Debug)]
pub struct BinanceDiff {
    pub u_first: u64, // U
    pub u_last: u64,  // u
    pub bids: Vec<(f64, f64)>,
    pub asks: Vec<(f64, f64)>,
}

#[derive(Clone, Debug)]
pub struct BinanceSnapshot {
    pub last_update_id: u64,
    pub bids: Vec<(f64, f64)>,
    pub asks: Vec<(f64, f64)>,
}

#[derive(Default, Debug)]
struct BinanceBookState {
    bids: BTreeMap<OrderedFloat<f64>, f64>, // asc by price
    asks: BTreeMap<OrderedFloat<f64>, f64>, // asc by price
    last_update_id: u64,
}

#[derive(Clone, Debug, Default)]
pub struct OrderBook {
    pub bids: Vec<OrderLevel>,
    pub asks: Vec<OrderLevel>,
}

#[derive(Clone, Debug)]
pub struct OrderLevel {
    pub exchange: &'static str,
    pub price: f64,
    pub amount: f64,
}

impl BinanceBookState {
    fn from_snapshot(s: &BinanceSnapshot) -> Self {
        let mut bids = BTreeMap::new();
        let mut asks = BTreeMap::new();
        for (p, q) in &s.bids {
            if *q > 0.0 {
                bids.insert(OrderedFloat(*p), *q);
            }
        }
        for (p, q) in &s.asks {
            if *q > 0.0 {
                asks.insert(OrderedFloat(*p), *q);
            }
        }
        Self {
            bids,
            asks,
            last_update_id: s.last_update_id,
        }
    }

    // Returns false if out-of-sync (gap)
    fn apply_event(&mut self, ev: &BinanceDiff) -> bool {
        if ev.u_last <= self.last_update_id {
            return true;
        }
        if ev.u_first > self.last_update_id + 1 {
            return false;
        }
        for (p, q) in &ev.bids {
            let key = OrderedFloat(*p);
            if *q == 0.0 {
                self.bids.remove(&key);
            } else {
                self.bids.insert(key, *q);
            }
        }
        for (p, q) in &ev.asks {
            let key = OrderedFloat(*p);
            if *q == 0.0 {
                self.asks.remove(&key);
            } else {
                self.asks.insert(key, *q);
            }
        }
        self.last_update_id = ev.u_last;
        true
    }

    fn to_orderbook_top10(&self) -> OrderBook {
        let mut bids: Vec<OrderLevel> = Vec::with_capacity(10);
        for (&price, &qty) in self.bids.iter().rev().take(10) {
            bids.push(OrderLevel {
                exchange: "binance",
                price: price.0,
                amount: qty,
            });
        }
        let mut asks: Vec<OrderLevel> = Vec::with_capacity(10);
        for (&price, &qty) in self.asks.iter().take(10) {
            asks.push(OrderLevel {
                exchange: "binance",
                price: price.0,
                amount: qty,
            });
        }
        OrderBook { bids, asks }
    }
}

pub async fn run_binance(binance_book: Arc<Mutex<Option<OrderBook>>>) -> Result<()> {
    let symbol = "ethusdt"; // lowercase for WS; REST expects uppercase
    let ws_url = format!("wss://stream.binance.com:9443/ws/{}@depth@100ms", symbol);

    let (ws, _) = connect_async(&ws_url).await?;
    println!("binance: connected");
    let (_write, mut read) = ws.split();

    // WS reader task: parse diff events and push to buffer channel
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<BinanceDiff>();
    let reader_tx = tx.clone();
    tokio::spawn(async move {
        while let Some(msg) = read.next().await {
            let txt = match msg {
                Ok(Message::Text(t)) => t,
                Ok(Message::Binary(b)) => match String::from_utf8(b) {
                    Ok(t) => t,
                    Err(_) => continue,
                },
                Ok(Message::Ping(_)) | Ok(Message::Pong(_)) => continue,
                Ok(Message::Close(_)) => break,
                Ok(_) => continue,
                Err(_) => break,
            };
            if let Some(ev) = parse_binance_diff(&txt) {
                let _ = reader_tx.send(ev);
            }
        }
    });

    // Buffer initial events until we have the first U
    let mut buffer: VecDeque<BinanceDiff> = VecDeque::new();
    let first_u = loop {
        if let Some(ev) = rx.recv().await {
            buffer.push_back(ev.clone());
            println!("first_u: {:?}", ev.u_first);
            break ev.u_first;
        } else {
            return Ok(());
        }
    };

    // Fetch snapshot until lastUpdateId >= first U (Spot depth API)
    let snapshot_url = format!(
        "https://api.binance.com/api/v3/depth?symbol={}&limit=20",
        symbol.to_uppercase()
    );
    let snapshot: BinanceSnapshot = loop {
        match reqwest::get(&snapshot_url).await {
            Ok(resp) => match resp.text().await {
                Ok(body) => {
                    if let Some(snap) = parse_binance_snapshot(&body) {
                        if snap.last_update_id >= first_u {
                            break snap;
                        }
                    }
                }
                Err(e) => eprintln!("binance snapshot body error: {e}"),
            },
            Err(e) => eprintln!("binance snapshot http error: {e}"),
        }
        sleep(Duration::from_millis(400)).await;
    };

    let mut state = BinanceBookState::from_snapshot(&snapshot);

    // Discard buffered events with u <= snapshot.lastUpdateId
    buffer.retain(|ev| ev.u_last > snapshot.last_update_id);
    println!("buffer after retain: {:?}", buffer);
    println!("snapshot: {:?}", snapshot);

    // Keep reading until we have events and find overlap
    loop {
        if buffer.is_empty() {
            if let Some(ev) = rx.recv().await {
                buffer.push_back(ev);
            } else {
                eprintln!("binance: channel closed before overlap found");
                return Ok(());
            }
        }
        buffer.retain(|ev| ev.u_last > snapshot.last_update_id);
        if let Some(idx) = buffer.iter().position(|ev| {
            ev.u_first <= snapshot.last_update_id && ev.u_last >= snapshot.last_update_id
        }) {
            for _ in 0..idx {
                buffer.pop_front();
            }
            break;
        }
        if let Some(ev) = rx.recv().await {
            buffer.push_back(ev);
        } else {
            eprintln!("binance: channel closed before overlap found");
            return Ok(());
        }
    }
    println!("buffer after overlap: {:?}", buffer);

    // Apply buffered events in order: first bridging event uses relaxed rule, then strict
    let mut first_applied = false;
    while let Some(ev) = buffer.pop_front() {
        if !first_applied {
            if ev.u_last < snapshot.last_update_id {
                eprintln!("binance: bootstrap out-of-sync; restarting");
                return Ok(());
            }
            for (p, q) in &ev.bids {
                let key = OrderedFloat(*p);
                if *q == 0.0 {
                    state.bids.remove(&key);
                } else {
                    state.bids.insert(key, *q);
                }
            }
            for (p, q) in &ev.asks {
                let key = OrderedFloat(*p);
                if *q == 0.0 {
                    state.asks.remove(&key);
                } else {
                    state.asks.insert(key, *q);
                }
            }
            state.last_update_id = ev.u_last;
            first_applied = true;
        } else {
            if !state.apply_event(&ev) {
                eprintln!("binance: gap during buffered apply; restarting");
                return Ok(());
            }
        }
        let top = state.to_orderbook_top10();
        *binance_book.lock().await = Some(top.clone());
        print_book(&top);
    }

    // Publish initial top10
    let top = state.to_orderbook_top10();
    *binance_book.lock().await = Some(top.clone());
    print_book(&top);

    // Process subsequent events continuously
    while let Some(ev) = rx.recv().await {
        if !state.apply_event(&ev) {
            eprintln!("binance: gap detected; restarting sync");
            break;
        }
        let top = state.to_orderbook_top10();
        *binance_book.lock().await = Some(top.clone());
        print_book(&top);
    }

    Ok(())
}

pub fn parse_binance_book(txt: &str) -> Option<OrderBook> {
    let v: Value = serde_json::from_str(txt).ok()?;
    let mut bids: Vec<OrderLevel> = Vec::new();
    let mut asks: Vec<OrderLevel> = Vec::new();

    if let Some(b) = v.get("b").and_then(|x| x.as_array()) {
        for lvl in b.iter() {
            if let Some(arr) = lvl.as_array() {
                if arr.len() >= 2 {
                    let p = arr[0]
                        .as_str()
                        .and_then(|s| s.parse::<f64>().ok())
                        .or_else(|| arr[0].as_f64())
                        .unwrap_or(0.0);
                    let q = arr[1]
                        .as_str()
                        .and_then(|s| s.parse::<f64>().ok())
                        .or_else(|| arr[1].as_f64())
                        .unwrap_or(0.0);
                    if q > 0.0 {
                        bids.push(OrderLevel {
                            exchange: "binance",
                            price: p,
                            amount: q,
                        });
                    }
                }
            }
        }
    }

    if let Some(a) = v.get("a").and_then(|x| x.as_array()) {
        for lvl in a.iter() {
            if let Some(arr) = lvl.as_array() {
                if arr.len() >= 2 {
                    let p = arr[0]
                        .as_str()
                        .and_then(|s| s.parse::<f64>().ok())
                        .or_else(|| arr[0].as_f64())
                        .unwrap_or(0.0);
                    let q = arr[1]
                        .as_str()
                        .and_then(|s| s.parse::<f64>().ok())
                        .or_else(|| arr[1].as_f64())
                        .unwrap_or(0.0);
                    if q > 0.0 {
                        asks.push(OrderLevel {
                            exchange: "binance",
                            price: p,
                            amount: q,
                        });
                    }
                }
            }
        }
    }

    if bids.is_empty() && asks.is_empty() {
        None
    } else {
        bids.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap());
        asks.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap());
        Some(OrderBook { bids, asks })
    }
}

fn print_book(book: &OrderBook) {
    if let (Some(best_bid), Some(best_ask)) = (
        book.bids.first().map(|x| x.price),
        book.asks.first().map(|x| x.price),
    ) {
        let spread = best_ask - best_bid;
        let bids_json: Vec<_> = book.bids.iter().take(10).map(|lvl| {
            serde_json::json!({ "exchange": lvl.exchange, "price": lvl.price, "amount": lvl.amount })
        }).collect();
        let asks_json: Vec<_> = book.asks.iter().take(10).map(|lvl| {
            serde_json::json!({ "exchange": lvl.exchange, "price": lvl.price, "amount": lvl.amount })
        }).collect();
        println!(
            "{}",
            serde_json::json!({ "spread": spread, "bids": bids_json, "asks": asks_json })
        );
    }
}

pub fn merge_books(a: &OrderBook, b: &OrderBook) -> OrderBook {
    let mut bids = Vec::with_capacity(a.bids.len() + b.bids.len());
    bids.extend_from_slice(&a.bids);
    bids.extend_from_slice(&b.bids);
    bids.sort_by(|x, y| y.price.partial_cmp(&x.price).unwrap());

    let mut asks = Vec::with_capacity(a.asks.len() + b.asks.len());
    asks.extend_from_slice(&a.asks);
    asks.extend_from_slice(&b.asks);
    asks.sort_by(|x, y| x.price.partial_cmp(&y.price).unwrap());

    OrderBook { bids, asks }
}

fn parse_binance_diff(txt: &str) -> Option<BinanceDiff> {
    let v: Value = serde_json::from_str(txt).ok()?;
    let u_first = v.get("U")?.as_u64()?;
    let u_last = v.get("u")?.as_u64()?;
    let mut bids = Vec::new();
    let mut asks = Vec::new();
    if let Some(b) = v.get("b").and_then(|x| x.as_array()) {
        for lvl in b {
            if let Some(arr) = lvl.as_array() {
                if arr.len() >= 2 {
                    let p = arr[0]
                        .as_str()
                        .and_then(|s| s.parse::<f64>().ok())
                        .or_else(|| arr[0].as_f64())
                        .unwrap_or(0.0);
                    let q = arr[1]
                        .as_str()
                        .and_then(|s| s.parse::<f64>().ok())
                        .or_else(|| arr[1].as_f64())
                        .unwrap_or(0.0);
                    bids.push((p, q));
                }
            }
        }
    }
    if let Some(a) = v.get("a").and_then(|x| x.as_array()) {
        for lvl in a {
            if let Some(arr) = lvl.as_array() {
                if arr.len() >= 2 {
                    let p = arr[0]
                        .as_str()
                        .and_then(|s| s.parse::<f64>().ok())
                        .or_else(|| arr[0].as_f64())
                        .unwrap_or(0.0);
                    let q = arr[1]
                        .as_str()
                        .and_then(|s| s.parse::<f64>().ok())
                        .or_else(|| arr[1].as_f64())
                        .unwrap_or(0.0);
                    asks.push((p, q));
                }
            }
        }
    }
    Some(BinanceDiff {
        u_first,
        u_last,
        bids,
        asks,
    })
}

fn parse_binance_snapshot(txt: &str) -> Option<BinanceSnapshot> {
    let v: Value = serde_json::from_str(txt).ok()?;
    let last_update_id = v.get("lastUpdateId")?.as_u64()?;
    let mut bids = Vec::new();
    let mut asks = Vec::new();
    if let Some(b) = v.get("bids").and_then(|x| x.as_array()) {
        for lvl in b {
            if let Some(arr) = lvl.as_array() {
                if arr.len() >= 2 {
                    let p = arr[0]
                        .as_str()
                        .and_then(|s| s.parse::<f64>().ok())
                        .or_else(|| arr[0].as_f64())
                        .unwrap_or(0.0);
                    let q = arr[1]
                        .as_str()
                        .and_then(|s| s.parse::<f64>().ok())
                        .or_else(|| arr[1].as_f64())
                        .unwrap_or(0.0);
                    bids.push((p, q));
                }
            }
        }
    }
    if let Some(a) = v.get("asks").and_then(|x| x.as_array()) {
        for lvl in a {
            if let Some(arr) = lvl.as_array() {
                if arr.len() >= 2 {
                    let p = arr[0]
                        .as_str()
                        .and_then(|s| s.parse::<f64>().ok())
                        .or_else(|| arr[0].as_f64())
                        .unwrap_or(0.0);
                    let q = arr[1]
                        .as_str()
                        .and_then(|s| s.parse::<f64>().ok())
                        .or_else(|| arr[1].as_f64())
                        .unwrap_or(0.0);
                    asks.push((p, q));
                }
            }
        }
    }
    Some(BinanceSnapshot {
        last_update_id,
        bids,
        asks,
    })
}
