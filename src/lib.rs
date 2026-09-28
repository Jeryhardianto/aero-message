//! Chat rooms over WebSocket.
//!
//! Connect to `/chat/{room}?username={name}` and send plain text frames.
//! Everyone in the room, the sender included, receives JSON events:
//!
//! ```text
//! {"type":"message","username":"budi","text":"halo"}
//! {"type":"join","username":"budi"}
//! {"type":"leave","username":"budi"}
//! ```
//!
//! A new connection first gets up to 50 recent messages, then its own `join`.
//! History lives in memory and goes away when the room empties. A taken or
//! invalid name is refused with close code 1008 and a reason; a message over
//! 4 KiB, or falling 128 events behind, drops the connection.

pub mod room;

use axum::{
    extract::{
        ws::{close_code, CloseFrame, Message, WebSocket, WebSocketUpgrade},
        Path, Query, State,
    },
    response::Response,
    routing::get,
    Router,
};
use serde::Deserialize;

use room::SharedState;

/// Anything bigger closes the sender's connection unread.
const MAX_MESSAGE_BYTES: usize = 4 * 1024;

pub fn app(state: SharedState) -> Router {
    Router::new()
        .route("/chat/{room}", get(chat))
        .with_state(state)
}

#[derive(Deserialize)]
struct Params {
    #[serde(default)]
    username: String,
}

async fn chat(
    ws: WebSocketUpgrade,
    Path(room): Path<String>,
    Query(Params { username }): Query<Params>,
    State(state): State<SharedState>,
) -> Response {
    ws.max_frame_size(MAX_MESSAGE_BYTES)
        .max_message_size(MAX_MESSAGE_BYTES)
        .on_upgrade(move |socket| handle_socket(socket, state, room, username))
}

async fn handle_socket(mut socket: WebSocket, state: SharedState, room: String, username: String) {
    let (member, mut events, history) = match room::join(&state, &room, &username) {
        Ok(joined) => joined,
        Err(reason) => {
            let close = CloseFrame {
                code: close_code::POLICY,
                reason: reason.into(),
            };
            let _ = socket.send(Message::Close(Some(close))).await;
            return;
        }
    };
    for event in history {
        if socket.send(Message::Text(event)).await.is_err() {
            return;
        }
    }
    loop {
        tokio::select! {
            incoming = socket.recv() => match incoming {
                Some(Ok(Message::Text(text))) => member.say(text.as_str()),
                Some(Ok(_)) => {}
                Some(Err(_)) | None => break,
            },
            event = events.recv() => {
                let Ok(event) = event else { break };
                if socket.send(Message::Text(event)).await.is_err() {
                    break;
                }
            }
        }
    }
}
