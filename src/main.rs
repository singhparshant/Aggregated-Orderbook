use std::sync::Arc;
use std::time::Instant;

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

    let symbol = "ethbtc";

    // Create empty aggregated orderbook initially
    let agg = AggregatedOrderBook::new();
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
            // Connect to streams first to avoid diff gap
            tracing::info!("Connecting to exchange streams...");
            let bitstamp_stream = modules::bitstamp::get_bitstamp_stream(symbol).await;
            let binance_stream = modules::binance::get_binance_stream(symbol).await;

            // Then fetch fresh snapshots concurrently and merge
            let snapshot_start = Instant::now();
            tracing::info!("Fetching fresh snapshots in parallel after connecting streams...");
            let (binance_snapshot, bitstamp_snapshot) = tokio::join!(
                modules::binance::get_binance_snapshot(symbol),
                modules::bitstamp::get_bitstamp_snapshot(symbol)
            );
            tracing::info!(
                "Snapshots fetched in parallel in {}ms",
                snapshot_start.elapsed().as_millis()
            );
            {
                let mut agg = agg_for_websocket.lock().await;
                agg.merge_snapshots(vec![bitstamp_snapshot, binance_snapshot]);
                tracing::info!("Snapshots merged into aggregated orderbook");
            }

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
                                    tracing::info!(
                                        "Received Bitstamp update: {:?} bids, {:?} asks (ID: {})",
                                        update.bids.len(),
                                        update.asks.len(),
                                        update.update_id
                                    );
                                    // tracing::info!("Received Bitstamp update: {:?}", update);
                                    let mut agg = agg_for_websocket.lock().await;
                                    let bitstamp_update_start = Instant::now();
                                    match agg.handle_update(update) {
                                        Ok(_) => {
                                            tracing::info!(
                                                "Bitstamp update took {}ms to apply successfully",
                                                bitstamp_update_start.elapsed().as_millis()
                                            );
                                        }
                                        Err(e) => {
                                            tracing::error!(
                                                "Bitstamp update failed after {}ms: {}",
                                                bitstamp_update_start.elapsed().as_millis(),
                                                e
                                            );
                                        }
                                    }
                                }
                            }
                            Message::Ping(_payload) => {
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
                                    tracing::info!(
                                        "Received Binance update: {:?} bids, {:?} asks (ID: {})",
                                        update.bids.len(),
                                        update.asks.len(),
                                        update.update_id
                                    );
                                    // tracing::info!("Received Binance update: {:?}", update);
                                    let mut agg = agg_for_websocket.lock().await;
                                    let binance_update_start = Instant::now();
                                    match agg.handle_update(update) {
                                        Ok(_) => {
                                            tracing::info!(
                                                "Binance update took {}ms to apply successfully",
                                                binance_update_start.elapsed().as_millis()
                                            );
                                        }
                                        Err(e) => {
                                            tracing::error!(
                                                "Binance update failed after {}ms: {}",
                                                binance_update_start.elapsed().as_millis(),
                                                e
                                            );
                                        }
                                    }
                                }
                            }
                            Message::Ping(_payload) => {
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

            // On disconnection, fetch new snapshots in parallel and merge them
            tracing::info!("Disconnected from exchanges, fetching fresh snapshots in parallel...");
            let start = std::time::Instant::now();

            // Fetch both snapshots concurrently
            let (binance_snapshot, bitstamp_snapshot) = tokio::join!(
                modules::binance::get_binance_snapshot(symbol),
                modules::bitstamp::get_bitstamp_snapshot(symbol)
            );

            let snapshot_duration = start.elapsed();
            tracing::info!(
                "Reconnection snapshots fetched in parallel in {}ms",
                snapshot_duration.as_millis()
            );

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
