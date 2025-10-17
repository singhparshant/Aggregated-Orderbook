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

                // Get top 10 levels from the aggregated orderbook
                let top10_bids = agg.get_top10_bids();
                let top10_asks = agg.get_top10_asks();
                let spread = agg.get_spread();

                // Convert to gRPC format
                let bids: Vec<Level> = top10_bids.into_iter().map(|level| Level {
                    exchange: level.exchange.to_string(),
                    price: level.price,
                    amount: level.amount,
                }).collect();

                let asks: Vec<Level> = top10_asks.into_iter().map(|level| Level {
                    exchange: level.exchange.to_string(),
                    price: level.price,
                    amount: level.amount,
                }).collect();

                let summary = Summary {
                    spread,
                    asks,
                    bids,
                };

                tracing::debug!("Sending summary: {} bids, {} asks, spread: {:.4}",
                    summary.bids.len(), summary.asks.len(), summary.spread);

                yield summary;

                // Release the lock and wait
                drop(agg);

                // Sleep for 1 second
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
