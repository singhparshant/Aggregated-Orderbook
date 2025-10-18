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
                // Clear screen and move cursor to top
                print!("\x1B[2J\x1B[1;1H");

                // Header
                println!("╔══════════════════════════════════════════════════════════════╗");
                println!("║                    ORDERBOOK AGGREGATOR                     ║");
                println!("╚══════════════════════════════════════════════════════════════╝");
                println!();

                // Spread
                println!("📊 Spread: {:.8}", summary.spread);
                println!();

                // Asks (Sell orders)
                println!("🔴 ASKS (Sell Orders)");
                println!("┌─────────────┬──────────────┬──────────────┐");
                println!("│ Exchange    │ Price        │ Quantity     │");
                println!("├─────────────┼──────────────┼──────────────┤");
                for ask in &summary.asks {
                    println!(
                        "│ {:<11} │ {:<12.8} │ {:<12.8} │",
                        ask.exchange, ask.price, ask.amount
                    );
                }
                println!("└─────────────┴──────────────┴──────────────┘");
                println!();

                // Bids (Buy orders)
                println!("🟢 BIDS (Buy Orders)");
                println!("┌─────────────┬──────────────┬──────────────┐");
                println!("│ Exchange    │ Price        │ Quantity     │");
                println!("├─────────────┼──────────────┼──────────────┤");
                for bid in &summary.bids {
                    println!(
                        "│ {:<11} │ {:<12.8} │ {:<12.8} │",
                        bid.exchange, bid.price, bid.amount
                    );
                }
                println!("└─────────────┴──────────────┴──────────────┘");
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
