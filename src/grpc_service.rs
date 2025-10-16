use crate::modules::types::AggregatedOrderBook;
use async_stream::try_stream;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

// Include the generated gRPC code
pub mod orderbook {
    tonic::include_proto!("orderbook");
}

use orderbook::orderbook_aggregator_server::{OrderbookAggregator, OrderbookAggregatorServer};
use orderbook::{Empty, Level, Summary};

pub struct OrderbookAggregatorService {
    pub aggregated_orderbook: Arc<Mutex<AggregatedOrderBook>>,
}

impl OrderbookAggregatorService {
    pub fn new(aggregated_orderbook: Arc<Mutex<AggregatedOrderBook>>) -> Self {
        Self {
            aggregated_orderbook,
        }
    }
}

#[tonic::async_trait]
impl OrderbookAggregator for OrderbookAggregatorService {
    // Not exactly sure what this is for or what it does, but it's required by the tonic library
    type BookSummaryStream =
        std::pin::Pin<Box<dyn futures::Stream<Item = Result<Summary, Status>> + Send + 'static>>;

    async fn book_summary(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::BookSummaryStream>, Status> {
        let agg_shared = Arc::clone(&self.aggregated_orderbook);

        let stream = try_stream! {
            loop {
                let agg = agg_shared.lock().await;
                let summary = agg.get_aggregated_orderbook();

                // Convert to gRPC format - limit to exactly 10 levels each
                let mut bids = Vec::new();
                let mut asks = Vec::new();

                // Convert bids (highest prices first) - take only top 10
                for (_price_idx, exchange_map) in summary.bids.iter().rev().take(10) {
                    for (exchange, level) in exchange_map {
                        bids.push(Level {
                            exchange: exchange.clone(),
                            price: level.price,
                            amount: level.amount,
                        });
                    }
                }

                // Convert asks (lowest prices first) - take only top 10
                for (_price_idx, exchange_map) in summary.asks.iter().take(10) {
                    for (exchange, level) in exchange_map {
                        asks.push(Level {
                            exchange: exchange.clone(),
                            price: level.price,
                            amount: level.amount,
                        });
                    }
                }

                let summary = Summary {
                    spread: summary.spread,
                    asks,
                    bids,
                };

                tracing::debug!("Sending summary: {} bids, {} asks, spread: {:.4}",
                    summary.bids.len(), summary.asks.len(), summary.spread);

                yield summary;

                // Release the lock and wait
                drop(agg);
                tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
            }
        };

        Ok(Response::new(Box::pin(stream)))
    }
}

pub fn create_grpc_server(
    aggregated_orderbook: Arc<Mutex<AggregatedOrderBook>>,
) -> OrderbookAggregatorServer<OrderbookAggregatorService> {
    let service = OrderbookAggregatorService::new(aggregated_orderbook);
    OrderbookAggregatorServer::new(service)
}
