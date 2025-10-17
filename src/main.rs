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

    let symbol = "btcusdc";

    // Streams will be created in the websocket task loop for reconnection handling

    // Fetch snapshots
    let binance_snapshot = modules::binance::get_binance_snapshot(symbol).await;
    let bitstamp_snapshot = modules::bitstamp::get_bitstamp_snapshot(symbol).await;

    // Merge into aggregated book
    let mut agg = AggregatedOrderBook::new();
    agg.merge_snapshots(vec![bitstamp_snapshot, binance_snapshot]);

    // Share the aggregated orderbook between threads
    let agg_shared = Arc::new(Mutex::new(agg));

    // Start gRPC server
    let agg_for_grpc = Arc::clone(&agg_shared);
    let grpc_server = tokio::spawn(async move {
        let addr = "127.0.0.1:5002".parse().unwrap();
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

    // Listen to the combined stream and handle the updates
    let websocket_task = tokio::spawn(async move {
        loop {
            // Recreate streams on each iteration
            let bitstamp_stream = modules::bitstamp::get_bitstamp_stream(symbol).await;
            let binance_stream = modules::binance::get_binance_stream(symbol).await;

            // Tag streams by source and combine
            let bitstamp_tagged = bitstamp_stream.map(|m| (Exchange::Bitstamp.as_str(), m));
            let binance_tagged = binance_stream.map(|m| (Exchange::Binance.as_str(), m));
            let mut combined = select(bitstamp_tagged, binance_tagged);

            tracing::info!("Connected to exchanges");

            while let Some((source, msg_result)) = combined.next().await {
                match msg_result {
                    Ok(msg) => match source {
                        "bitstamp" => match msg {
                            Message::Text(text) => {
                                if let Some(update) = OrderBookUpdate::from_bitstamp_json(&text) {
                                    let start = std::time::Instant::now();
                                    tracing::info!(
                                        "Received Bitstamp update: {} bids, {} asks (ID: {})",
                                        update.bids.len(),
                                        update.asks.len(),
                                        update.update_id
                                    );
                                    let mut agg = agg_for_websocket.lock().await;
                                    match agg.handle_update(update) {
                                        Ok(_) => {
                                            let duration = start.elapsed();
                                            tracing::info!(
                                                "Bitstamp update took {}us to apply successfully",
                                                duration.as_micros()
                                            );
                                        }
                                        Err(e) => {
                                            let duration = start.elapsed();
                                            tracing::error!(
                                                "Bitstamp update failed after {}us: {}",
                                                duration.as_micros(),
                                                e
                                            );
                                        }
                                    }
                                }
                            }
                            Message::Ping(payload) => {
                                tracing::debug!("Received ping from Bitstamp, sending pong");
                                // Note: tungstenite handles pong automatically for ping frames
                            }
                            Message::Pong(_) => {
                                tracing::debug!("Received pong from Bitstamp");
                            }
                            Message::Close(_) => {
                                tracing::warn!("Bitstamp connection closed, will reconnect");
                                break; // Exit inner loop to reconnect
                            }
                            _ => {}
                        },
                        "binance" => match msg {
                            Message::Text(text) => {
                                if let Some(update) = OrderBookUpdate::from_binance_json(&text) {
                                    let start = std::time::Instant::now();
                                    tracing::info!(
                                        "Received Binance update: {} bids, {} asks (ID: {})",
                                        update.bids.len(),
                                        update.asks.len(),
                                        update.update_id
                                    );
                                    let mut agg = agg_for_websocket.lock().await;
                                    match agg.handle_update(update) {
                                        Ok(_) => {
                                            let duration = start.elapsed();
                                            tracing::info!(
                                                "Binance update took {}us to apply successfully",
                                                duration.as_micros()
                                            );
                                        }
                                        Err(e) => {
                                            let duration = start.elapsed();
                                            tracing::error!(
                                                "Binance update failed after {}us: {}",
                                                duration.as_micros(),
                                                e
                                            );
                                        }
                                    }
                                }
                            }
                            Message::Ping(payload) => {
                                tracing::debug!("Received ping from Binance, sending pong");
                            }
                            Message::Pong(_) => {
                                tracing::debug!("Received pong from Binance");
                            }
                            Message::Close(_) => {
                                tracing::warn!("Binance connection closed, will reconnect");
                                break; // Exit inner loop to reconnect
                            }
                            _ => {}
                        },
                        _ => {}
                    },
                    Err(e) => {
                        tracing::error!("{} stream error: {}, will reconnect", source, e);
                        break; // Exit inner loop to reconnect
                    }
                }
            }

            // Reconnection delay
            tracing::info!("Reconnecting to exchanges in 2 seconds...");
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

            // On disconnection, fetch new snapshots and merge them
            tracing::info!("Disconnected from exchanges, fetching new snapshots...");
            let binance_snapshot = modules::binance::get_binance_snapshot(symbol).await;
            let bitstamp_snapshot = modules::bitstamp::get_bitstamp_snapshot(symbol).await;

            let mut agg = agg_for_websocket.lock().await;
            agg.merge_snapshots(vec![bitstamp_snapshot, binance_snapshot]);
            tracing::info!("Merged new snapshots after disconnection");
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
