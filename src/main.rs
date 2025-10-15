use std::time::Duration;

use futures_util::StreamExt;
use futures_util::stream::select;
use tokio_tungstenite::tungstenite::Message;

use crate::modules::types::{AggregatedOrderBook, Exchange, OrderBookUpdate};

mod modules;

#[tokio::main]
async fn main() {
    let symbol = "ethusdt";
    // Streams
    let bitstamp_stream = modules::bitstamp::get_bitstamp_stream(symbol).await;
    let binance_stream = modules::binance::get_binance_stream(symbol).await;

    // Tag streams by source and combine
    let bitstamp_tagged = bitstamp_stream.map(|m| (Exchange::Bitstamp.as_str(), m));
    let binance_tagged = binance_stream.map(|m| (Exchange::Binance.as_str(), m));
    let mut combined = select(bitstamp_tagged, binance_tagged);

    // Fetch snapshots
    let bitstamp_snapshot = modules::bitstamp::get_bitstamp_snapshot(symbol).await;
    let binance_snapshot = modules::binance::get_binance_snapshot(symbol).await;

    // Merge into aggregated book
    let mut agg = AggregatedOrderBook::new();
    agg.merge_snapshots(vec![bitstamp_snapshot, binance_snapshot]);

    while let Some((source, msg_result)) = combined.next().await {
        match msg_result {
            Ok(msg) => match source {
                "bitstamp" => match msg {
                    Message::Text(text) => {
                        if let Some(update) = OrderBookUpdate::from_bitstamp_json(&text) {
                            agg.handle_update(update);
                        }
                        println!(
                            "Aggregated order book: {:#?}",
                            agg.get_aggregated_orderbook()
                        );
                    }
                    _ => {}
                },
                "binance" => match msg {
                    Message::Text(text) => {
                        if let Some(update) = OrderBookUpdate::from_binance_json(&text) {
                            agg.handle_update(update);
                        }
                        println!(
                            "Aggregated order book: {:#?}",
                            agg.get_aggregated_orderbook()
                        );
                    }
                    _ => {}
                },
                _ => {}
            },
            Err(e) => eprintln!("{} stream error: {}", source, e),
        }
    }
}
