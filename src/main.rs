use futures_util::StreamExt;
use futures_util::stream::select;
use std::sync::Arc;
use tokio::sync::Mutex;

mod modules;

#[tokio::main]
async fn main() {
    // Optionally fetch snapshots first
    let _bitstamp_snapshot = modules::bitstamp::get_bitstamp_snapshot("ethusdt").await;

    // Streams
    let bitstamp_stream = modules::bitstamp::get_bitstamp_stream("ethusdt").await;
    let binance_stream = modules::binance::get_binance_stream("ethusdt").await;

    // Tag streams by source and combine
    let bitstamp_tagged = bitstamp_stream.map(|m| ("bitstamp", m));
    let binance_tagged = binance_stream.map(|m| ("binance", m));
    let mut combined = select(bitstamp_tagged, binance_tagged);

    while let Some((source, msg_result)) = combined.next().await {
        match msg_result {
            Ok(msg) => {
                match source {
                    "bitstamp" => {
                        println!("bitstamp: {:?}", msg);
                        // TODO: parse Bitstamp diff here
                    }
                    "binance" => {
                        println!("binance: {:?}", msg);
                        // TODO: parse Binance depth update here
                    }
                    _ => {}
                }
            }
            Err(e) => eprintln!("{} stream error: {}", source, e),
        }
    }
}
