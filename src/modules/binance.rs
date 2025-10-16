use crate::modules::types::Exchange;
use futures_util::StreamExt;
use futures_util::stream::SplitStream;
use tokio::net::TcpStream;

use crate::modules::types::{OrderBook, OrderLevel};
use serde_json::Value;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

pub async fn get_binance_snapshot(symbol: &str) -> OrderBook {
    let url = format!(
        "https://api.binance.com/api/v3/depth?symbol={}&limit=1000",
        symbol.to_uppercase()
    );
    let response = reqwest::get(url).await.unwrap();
    let mut bids = vec![];
    let mut asks = vec![];
    let body = response.text().await.unwrap();
    let data: Value = serde_json::from_str(&body).unwrap();
    let last_update_id = data["lastUpdateId"].as_u64().unwrap();
    let bidsJsonArray = data["bids"].as_array().unwrap();
    for bid in bidsJsonArray {
        bids.push(OrderLevel {
            exchange: Exchange::Binance.as_str(),
            price: bid[0].as_str().unwrap().parse::<f64>().unwrap(),
            amount: bid[1].as_str().unwrap().parse::<f64>().unwrap(),
        });
    }
    let asksJsonArray = data["asks"].as_array().unwrap();
    for ask in asksJsonArray {
        asks.push(OrderLevel {
            exchange: Exchange::Binance.as_str(),
            price: ask[0].as_str().unwrap().parse::<f64>().unwrap(),
            amount: ask[1].as_str().unwrap().parse::<f64>().unwrap(),
        });
    }
    OrderBook {
        last_update_id,
        bids,
        asks,
    }
}

pub async fn get_binance_stream(
    symbol: &str,
) -> SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>> {
    let url = format!("wss://stream.binance.com:9443/ws/{}@depth@1000ms", symbol);
    let (ws_stream, _) = connect_async(url).await.unwrap();
    let (_, read) = ws_stream.split();
    read
}
