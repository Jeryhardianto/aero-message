# AeroMessage

A chat server in Rust. Clients connect over WebSocket and join a named room, and every message goes to everyone in that room. Members see each other join and leave, and a newcomer receives the last 50 messages. Everything lives in memory: there is no database and no login.

Built with Axum 0.8 and Tokio, tested with Rust 1.98.

## Run it

```sh
cargo run --release
```

The server listens on port 3000 on all interfaces. To chat, paste this into a browser console:

```js
const ws = new WebSocket("ws://localhost:3000/chat/rust?username=budi");
ws.onmessage = (e) => console.log(JSON.parse(e.data));
ws.onopen = () => ws.send("halo");
```

The console shows your own `join` event, then your message coming back. A second tab with another username sees the same conversation. The server logs every join and leave, plus a metrics line every 5 seconds:

```
INFO aero_message: listening on ws://0.0.0.0:3000/chat/{room}
INFO aero_message::room: joined room="rust" username="budi"
INFO aero_message: metrics rooms=1 connections=1 msg_per_sec=0.2
INFO aero_message::room: left room="rust" username="budi"
```

## Protocol

Connect with `GET /chat/{room}?username={name}` as a WebSocket upgrade. Each text frame a client sends is one chat message; binary frames are ignored. The server sends JSON:

```json
{"type":"message","username":"budi","text":"halo"}
{"type":"join","username":"ani"}
{"type":"leave","username":"ani"}
```

Senders get their own messages back, so a client can render the conversation straight from the stream. A new connection first receives up to 50 recent messages, then its own `join`. Everything after that is live.

| Case | Result |
|---|---|
| Missing username, or a room or username that is empty, over 32 characters, padded with spaces, or contains control characters | Closed with code 1008 and the reason |
| Username already taken in that room | Closed with code 1008, reason `username already taken` |
| Message over 4 KiB | Connection dropped, the room sees `leave` |
| Client more than 128 events behind | Connection dropped, the room sees `leave` |
| Last member leaves | Room and its history deleted |

## How it works

```
src/room.rs     room state: join, say, leave, stats, with unit tests
src/lib.rs      router and WebSocket handler
src/main.rs     listener on :3000, logging, metrics task
tests/chat.rs   integration tests with real WebSocket clients
bench/load.mjs  load test
```

All rooms live in one map behind a lock:

```rust
pub type SharedState = Arc<RwLock<HashMap<String, ChatRoom>>>;

pub struct ChatRoom {
    tx: broadcast::Sender<Utf8Bytes>, // one channel per room
    users: HashSet<String>,           // names taken in this room
    history: VecDeque<Utf8Bytes>,     // last 50 messages, already serialized
}
```

Each connection goes through this sequence:

1. `room::join` checks both names. Under the write lock it claims the username (creating the room if needed), subscribes to the room's channel, announces the join and copies the history. It returns a `Member` value that stands for the seat.
2. The handler sends the history, then loops on `tokio::select!` over the socket and the room's receiver. Text from the client goes to `Member::say`; events from the room go to the client.
3. When the loop ends, for any reason, the `Member` is dropped. Its `Drop` impl frees the name, announces the leave and deletes the room if nobody is left.

## Design decisions

- Each room has one `broadcast` channel, and only that room's members subscribe to it. A message is serialized to JSON once and sent once, whatever the room size.
- The channel carries `Utf8Bytes`, the reference-counted string type inside Axum's `Message::Text`. A broadcast to N members clones a pointer N times rather than allocating N strings.
- `say` appends to the history and broadcasts under the same lock that `join` holds while it copies the history and subscribes. A newcomer therefore sees every message exactly once, either in the history it received or live.
- Leaving happens in `Drop`, so a close frame, a network error, an early return and a panic in the handler all run the same cleanup.
- Each connection is a single task running a `select!` loop over the socket and the room. With no separate reader and writer tasks, nothing needs aborting when one side ends.
- The loop keeps reading after the client's close frame, because tungstenite sends the close reply on the next read. A loop that stops at the close frame drops the connection before the reply goes out, and browsers then report code 1006 instead of a clean close.
- Refusals happen after the upgrade, as close code 1008 with a reason. A browser cannot read the HTTP status of a failed handshake, so an HTTP 409 would reach a web page as an unexplained error.
- A client more than 128 events behind is disconnected. Skipping ahead would leave gaps in its conversation that it cannot detect; after reconnecting, it gets the last 50 messages.
- The frame size limit equals the 4 KiB message limit, so an oversized frame is rejected from its header before tungstenite buffers it (its default frame limit is 16 MiB).

## Tests

```sh
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

`cargo test` runs 7 unit tests in `src/room.rs` and 6 integration tests in `tests/chat.rs`. The unit tests cover name rules, per-room uniqueness, join and leave events, broadcast scope, the history cap, room cleanup and the metrics counter. The integration tests start the server on a random port and talk to it with real WebSocket clients (tokio-tungstenite): chat between members, room isolation, history for newcomers, refusals with code 1008, oversized messages and the close handshake. Each test has been seen to fail, either before its feature existed or against a deliberately broken version.

## Performance

`bench/load.mjs` opens 2,000 WebSocket connections (20 rooms of 100 clients), waits until each client has seen its own join, then has every client send one message. That is 2,000 messages and 200,000 deliveries.

Four runs on an Apple M1 Pro (8 cores, 16 GB), release build, with the load script on the same machine:

| Step | Time |
|---|---|
| 2,000 clients connected and joined | 501 to 646 ms |
| 200,000 deliveries | 357 to 455 ms |
| Connections dropped | 0 |

Client and server share the CPU here, so treat these numbers as a smoke test, not a benchmark. To reproduce, raise the open-file limit in both shells, since the defaults on macOS and Linux are below 2,000:

```sh
# shell 1
ulimit -n 4096
cargo run --release

# shell 2 (Node 22+)
ulimit -n 4096
node bench/load.mjs    # optional args: rooms per_room
```

## Limitations

- Everything is in memory in one process. A restart loses all rooms and history, and two instances behind a load balancer would split rooms between them.
- There is no authentication: any client can take any free username.
- There is no per-client rate limit.
- The server does not ping clients. A client whose network disappears without closing the connection stays in its room until a write to it fails, which never happens while the room stays quiet.
- One lock covers all rooms, and each message holds it for a history push and a channel send. If busy rooms start to contend, the next step is a lock per room.

## License

MIT, see [LICENSE](LICENSE).
