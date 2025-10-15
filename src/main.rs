use futures_util::StreamExt;
use futures_util::stream::select;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;

mod modules;

#[tokio::main]
async fn main() {
    let symbol = "ethusdt";
    // Streams
    let bitstamp_stream = modules::bitstamp::get_bitstamp_stream(symbol).await;
    let binance_stream = modules::binance::get_binance_stream(symbol).await;

    // Tag streams by source and combine
    let bitstamp_tagged = bitstamp_stream.map(|m| ("bitstamp", m));
    let binance_tagged = binance_stream.map(|m| ("binance", m));
    let mut combined = select(bitstamp_tagged, binance_tagged);

    // Fetch snapshots
    let _bitstamp_snapshot = modules::bitstamp::get_bitstamp_snapshot(symbol).await;
    let _binance_snapshot = modules::binance::get_binance_snapshot(symbol).await;

    while let Some((source, msg_result)) = combined.next().await {
        match msg_result {
            Ok(msg) => match source {
                "bitstamp" => match msg {
                    Message::Text(text) => {
                        println!("bitstamp: {:?}", text);
                    }
                    _ => {}
                },
                "binance" => match msg {
                    Message::Text(text) => {
                        println!("binance: {:?}", text);
                    }
                    _ => {}
                },
                _ => {}
            },
            Err(e) => eprintln!("{} stream error: {}", source, e),
        }
    }
}
