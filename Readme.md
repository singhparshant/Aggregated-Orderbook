# Keyrock MM Rust Task

## Overview
Real-time order book aggregation system that combines data from Binance and Bitstamp exchanges, maintaining a unified order book and serving it via gRPC streaming.

## Approach - Step by Step

### 1. **WebSocket Connection Strategy**
- Connect to both Binance and Bitstamp WebSocket streams **first**
- Then fetch fresh snapshots from both exchanges
- This prevents missing updates between snapshot fetch and stream connection

### 2. **Data Structure Design**
- **BTreeMap** with scaled price levels as keys
- **Value**: HashMap<exchange, OrderLevel> for each price bucket
- **Why BTreeMap**: Keeps price levels naturally ordered (crucial for bid/ask ordering)
- **Why HashMap inside**: Allows multiple exchanges at the same price level

### 3. **Snapshot Merging**
- Fetch initial snapshots from both exchanges
- Merge into aggregated order book
- Start processing real-time updates from streams

### 4. **Concurrency Control**
- **Read locks (RwLock)**: Multiple gRPC clients can read simultaneously
- **Write locks (RwLock)**: Exclusive access for WebSocket updates
- **Lock scope**: Minimized to prevent blocking

### 5. **Disconnection Handling**
- On any stream disconnection → restart from scratch
- Fetch fresh snapshots again
- Reconnect to both streams
- Ensures data consistency after reconnection

### 6. **Update Processing**
- Apply real-time updates to aggregated book
- Validate update IDs to prevent out-of-order updates
- Early return on stale updates (no retries/sleeps in hot path)

## Architecture

```
WebSocket Streams → Snapshot Fetch → Merge → Real-time Updates
       ↓                    ↓           ↓           ↓
   [Binance]           [Snapshot]   [Aggregated]  [gRPC Stream]
   [Bitstamp]          [Snapshot]   [OrderBook]   [to Clients]
```

## Data Flow

1. **Connect** to WebSocket streams (both exchanges)
2. **Fetch** fresh snapshots in parallel
3. **Merge** snapshots into aggregated order book
4. **Process** real-time updates as they arrive
5. **Serve** top 10 bids/asks via gRPC streaming
6. **Handle** disconnections by restarting the entire flow

## Prerequisites
- Rust toolchain (stable)
- No manual proto step needed; Cargo runs `build.rs` which invokes `tonic-build`

## Build & Run

### Build
```bash
cargo build
```

### Run Server (gRPC producer)
```bash
cargo run --bin keyrock_mm_rust_task
```
- Starts WebSocket consumers, aggregates the book
- Serves gRPC on `127.0.0.1:5002`

### Run Client (gRPC consumer)
```bash
cargo run --bin client
```
- Connects to `127.0.0.1:5002`
- Subscribes to `BookSummary`, prints streamed summaries

## Potential Improvements

### Memory Efficiency
- **Current**: Maintains full order book, returns top 10 levels
- **Improvement**: Consider using Vector instead of HashMap for 2 exchanges (minor optimization)
- **Trade-off**: HashMap provides O(1) exchange lookup vs Vector O(n) but with only 2 exchanges, difference is negligible

### Production Readiness
- **Error Handling**: More robust error recovery strategies
- **Rate Limiting**: Handle high-frequency updates more efficiently  
- **Monitoring**: Add metrics for update latency, connection health
- **Configuration**: Make exchange endpoints and symbols configurable
- **Testing**: Add comprehensive unit and integration tests
- **Logging**: Structured logging with different levels
- **Graceful Shutdown**: Proper cleanup on termination signals 