use futures_util::StreamExt;
use tonic::Request;
use tonic::transport::Channel;

// Include the generated gRPC code
pub mod orderbook {
    tonic::include_proto!("orderbook");
}

use orderbook::Empty;
use orderbook::orderbook_aggregator_client::OrderbookAggregatorClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Connect to the gRPC server
    let channel = Channel::from_static("http://127.0.0.1:5002")
        .connect()
        .await?;
    let mut client = OrderbookAggregatorClient::new(channel);

    println!("Connected to gRPC server. Starting to receive orderbook updates...");

    // Create an empty request
    let request = Request::new(Empty {});

    // Call the streaming RPC
    let mut stream = client.book_summary(request).await?.into_inner();

    while let Some(result) = stream.next().await {
        match result {
            Ok(summary) => {
                // Print in JSON-like format
                println!("{{");
                println!("  \"spread\": {:.2},", summary.spread);

                println!("  \"asks\": [");
                for (i, ask) in summary.asks.iter().rev().enumerate() {
                    println!(
                        "    {{ \"exchange\": \"{}\", \"price\": {:.2}, \"amount\": {:.4} }}{}",
                        ask.exchange,
                        ask.price,
                        ask.amount,
                        if i < summary.asks.len() - 1 { "," } else { "" }
                    );
                }
                println!("  ],");

                println!("  \"bids\": [");
                for (i, bid) in summary.bids.iter().enumerate() {
                    println!(
                        "    {{ \"exchange\": \"{}\", \"price\": {:.2}, \"amount\": {:.4} }}{}",
                        bid.exchange,
                        bid.price,
                        bid.amount,
                        if i < summary.bids.len() - 1 { "," } else { "" }
                    );
                }
                println!("  ]");
                println!("}}");
                println!();
            }
            Err(e) => {
                eprintln!("Error receiving update: {}", e);
                break;
            }
        }
    }

    println!("Client disconnected");
    Ok(())
}
