use crate::modules::types::Exchange;
use futures_util::SinkExt;
use futures_util::StreamExt;
use futures_util::stream::{SplitSink, SplitStream};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message};

use crate::modules::types::{OrderBook, OrderLevel};

pub async fn get_bitstamp_snapshot(symbol: &str) -> OrderBook {
    let url = format!(
        "https://www.bitstamp.net/api/v2/order_book/{}/",
        symbol.to_lowercase()
    );
    let response = reqwest::get(url).await.unwrap();
    let body = response.text().await.unwrap();
    let data: Value = serde_json::from_str(&body).unwrap();
    let last_update_id = data["microtimestamp"]
        .as_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();
    let bids = data["bids"].as_array().unwrap();
    let asks = data["asks"].as_array().unwrap();
    let bids = bids
        .iter()
        .map(|bid| OrderLevel {
            exchange: Exchange::Bitstamp.as_str(),
            price: bid[0].as_str().unwrap().parse::<f64>().unwrap(),
            amount: bid[1].as_str().unwrap().parse::<f64>().unwrap(),
        })
        .collect();
    let asks = asks
        .iter()
        .map(|ask| OrderLevel {
            exchange: Exchange::Bitstamp.as_str(),
            price: ask[0].as_str().unwrap().parse::<f64>().unwrap(),
            amount: ask[1].as_str().unwrap().parse::<f64>().unwrap(),
        })
        .collect();
    OrderBook {
        last_update_id,
        bids,
        asks,
    }
}

pub async fn get_bitstamp_stream(
    symbol: &str,
) -> (
    SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>,
    SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
) {
    let ws_url_bitstamp = "wss://ws.bitstamp.net".to_string();
    let (mut ws_stream_bitstamp, _) = connect_async(&ws_url_bitstamp).await.unwrap();
    let subscribe_msg = serde_json::json!({
        "event": "bts:subscribe",
        "data": {
            "channel": format!("diff_order_book_{}", symbol)
        }
    });
    let res = ws_stream_bitstamp
        .send(Message::Text(subscribe_msg.to_string().into()))
        .await;
    if res.is_err() {
        eprintln!("error sending subscribe message: {}", res.err().unwrap());
    }
    let (write_stream, read_stream) = ws_stream_bitstamp.split();
    (write_stream, read_stream)
}
