Keyrock MM Rust Task

What this does (high level)
- Connects to Binance and Bitstamp order book feeds for a single symbol (currently `ethusdt`).
- Aggregates price levels into a unified book (per price bucket → per-exchange levels).
- Keeps the top 10 bids (highest) and top 10 asks (lowest) up to date.
- Serves a streaming gRPC endpoint that pushes `Summary` messages (spread, bids, asks).

Prereqs
- Rust toolchain (stable). No manual proto step needed; Cargo runs `build.rs` which invokes `tonic-build`.

Build
- Compile both binaries:
  - `cargo build`  (or run steps below which also build)

Run the server (gRPC producer)
- Starts WebSocket consumers, aggregates the book, and serves gRPC on `127.0.0.1:5002`.
- Command:
  - `cargo run --bin keyrock_mm_rust_task`

Run the client (gRPC consumer)
- Connects to `127.0.0.1:5002`, subscribes to `BookSummary`, prints streamed summaries.
- In a separate shell after the server is running:
  - `cargo run --bin client`

Notes
- Proto is at `protos/orderbook.proto`. `build.rs` compiles it automatically on build.
- The server method is `book_summary` (from `rpc BookSummary(...)`).
- Change the trading pair by editing `symbol` in `src/main.rs`.