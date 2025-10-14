use std::sync::Arc;
use tokio::sync::Mutex;

mod modules;

#[tokio::main]
async fn main() {
    let binance_book: Arc<Mutex<Option<modules::binance::OrderBook>>> = Arc::new(Mutex::new(None));
    let _bitstamp_book: Arc<Mutex<Option<modules::binance::OrderBook>>> =
        Arc::new(Mutex::new(None));

    let b1 = binance_book.clone();
    // let binance_task = tokio::spawn(async move {
    //     if let Err(e) = modules::binance::run_binance(b1).await {
    //         eprintln!("binance error: {e}");
    //     }
    // });
    // let _ = binance_task.await;

    // Placeholder for other exchange integration
    // let b2 = _bitstamp_book.clone();
    // let bitstamp_task = tokio::spawn(async move {
    //     if let Err(e) = run_bitstamp(b2).await {
    //         eprintln!("bitstamp error: {e}");
    //     }
    // });
    // let _ = bitstamp_task.await;
}
