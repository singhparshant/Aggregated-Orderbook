use std::sync::Arc;

use futures_util::StreamExt;
use futures_util::stream::select;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;
use tonic::transport::Server;

use crate::grpc_service::create_grpc_server;
use crate::modules::types::{AggregatedOrderBook, Exchange, OrderBookUpdate};

mod grpc_service;
mod modules;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

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

    // Share the aggregated orderbook between threads
    let agg_shared = Arc::new(Mutex::new(agg));

    // Start gRPC server
    let agg_for_grpc = Arc::clone(&agg_shared);
    let grpc_server = tokio::spawn(async move {
        let addr = "127.0.0.1:5001".parse().unwrap();
        let service = create_grpc_server(agg_for_grpc);

        tracing::info!("gRPC server starting on {}", addr);
        Server::builder()
            .add_service(service)
            .serve(addr)
            .await
            .unwrap();
    });

    // Start WebSocket processing
    let agg_for_websocket = Arc::clone(&agg_shared);
    // while let Some(msg_result) = binance_stream.next().await {
    //     match msg_result {
    //         Ok(msg) => match msg {
    //             Message::Text(text) => {
    //                 println!("Received Binance updat11111e: {}", text);
    //                 if let Some(update) = OrderBookUpdate::from_binance_json(&text) {
    //                     tracing::info!(
    //                         "Received Binance update: {} bids, {} asks",
    //                         update.bids.len(),
    //                         update.asks.len()
    //                     );
    //                     // let mut agg = agg_for_websocket.lock().await;
    //                     // agg.handle_update(update);
    //                 }
    //             }
    //             _ => {}
    //         },
    //         Err(e) => eprintln!("Binance stream error: {}", e),
    //     }
    // }

    let websocket_task = tokio::spawn(async move {
        while let Some((source, msg_result)) = combined.next().await {
            match msg_result {
                Ok(msg) => match source {
                    "bitstamp" => match msg {
                        Message::Text(text) => {
                            if let Some(update) = OrderBookUpdate::from_bitstamp_json(&text) {
                                tracing::info!(
                                    "Received Bitstamp update: {} bids, {} asks",
                                    update.bids.len(),
                                    update.asks.len()
                                );
                                let mut agg = agg_for_websocket.lock().await;
                                agg.handle_update(update);
                            }
                        }
                        _ => {}
                    },
                    "binance" => match msg {
                        Message::Text(text) => {
                            // println!("Received Binance update: {}", text);
                            if let Some(update) = OrderBookUpdate::from_binance_json(&text) {
                                tracing::info!(
                                    "Received Binance update: {} bids, {} asks",
                                    update.bids.len(),
                                    update.asks.len()
                                );
                                let mut agg = agg_for_websocket.lock().await;
                                agg.handle_update(update);
                            }
                        }
                        _ => {}
                    },
                    _ => {}
                },
                Err(e) => eprintln!("{} stream error: {}", source, e),
            }
        }
    });

    // Wait for either task to complete
    tokio::select! {
        _ = grpc_server => {
            tracing::info!("gRPC server stopped");
        }
        _ = websocket_task => {
            tracing::info!("WebSocket processing stopped");
        }
    }

    Ok(())
}
