//! In-memory chat rooms shared by every connection.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use axum::extract::ws::Utf8Bytes;
use serde::Serialize;
use tokio::sync::broadcast;

/// Every live room, keyed by name.
pub type SharedState = Arc<RwLock<HashMap<String, ChatRoom>>>;

/// A client this many events behind is too slow and gets disconnected.
/// A power of two, because tokio rounds the capacity up to one anyway.
const CHANNEL_CAPACITY: usize = 128;
const MAX_NAME_CHARS: usize = 32;
const HISTORY_LEN: usize = 50;

/// Chat messages sent since start, for the throughput metric.
pub static MESSAGES: AtomicU64 = AtomicU64::new(0);

pub struct ChatRoom {
    tx: broadcast::Sender<Utf8Bytes>,
    users: HashSet<String>,
    history: VecDeque<Utf8Bytes>,
}

impl Default for ChatRoom {
    fn default() -> Self {
        Self {
            tx: broadcast::channel(CHANNEL_CAPACITY).0,
            users: HashSet::new(),
            history: VecDeque::new(),
        }
    }
}

/// What clients receive, as JSON: `{"type": "join", "username": "budi"}`.
#[derive(Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum Event<'a> {
    Join { username: &'a str },
    Leave { username: &'a str },
    Message { username: &'a str, text: &'a str },
}

impl Event<'_> {
    fn to_json(&self) -> Utf8Bytes {
        serde_json::to_string(self)
            .expect("events serialize")
            .into()
    }
}

/// A user's seat in a room. Dropping it leaves the room.
pub struct Member {
    state: SharedState,
    room: String,
    username: String,
}

/// Takes `username` in `room`, creating the room if needed. Returns the seat,
/// a subscription to the room's events and the room's recent messages.
pub fn join(
    state: &SharedState,
    room: &str,
    username: &str,
) -> Result<(Member, broadcast::Receiver<Utf8Bytes>, Vec<Utf8Bytes>), &'static str> {
    if !valid_name(room) || !valid_name(username) {
        return Err("room and username must be 1-32 characters, no control characters or surrounding spaces");
    }
    let joined = Event::Join { username }.to_json();
    let mut rooms = state.write().unwrap();
    let chat = rooms.entry(room.to_owned()).or_default();
    if !chat.users.insert(username.to_owned()) {
        return Err("username already taken");
    }
    let rx = chat.tx.subscribe();
    let _ = chat.tx.send(joined);
    let history = chat.history.iter().cloned().collect();
    drop(rooms);

    tracing::info!(room, username, "joined");
    let member = Member {
        state: state.clone(),
        room: room.to_owned(),
        username: username.to_owned(),
    };
    Ok((member, rx, history))
}

impl Member {
    /// Sends a chat message to everyone in the room, the sender included.
    pub fn say(&self, text: &str) {
        let event = Event::Message {
            username: &self.username,
            text,
        }
        .to_json();
        let mut rooms = self.state.write().unwrap();
        if let Some(chat) = rooms.get_mut(&self.room) {
            if chat.history.len() == HISTORY_LEN {
                chat.history.pop_front();
            }
            chat.history.push_back(event.clone());
            let _ = chat.tx.send(event);
            MESSAGES.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Leaving runs on drop, so every way a connection can end frees the name.
impl Drop for Member {
    fn drop(&mut self) {
        let left = Event::Leave {
            username: &self.username,
        }
        .to_json();
        let mut rooms = self.state.write().unwrap();
        if let Some(chat) = rooms.get_mut(&self.room) {
            chat.users.remove(&self.username);
            if chat.users.is_empty() {
                rooms.remove(&self.room);
            } else {
                let _ = chat.tx.send(left);
            }
        }
        drop(rooms);

        let (room, username) = (self.room.as_str(), self.username.as_str());
        tracing::info!(room, username, "left");
    }
}

/// `(rooms, connections)` currently open.
pub fn stats(state: &SharedState) -> (usize, usize) {
    let rooms = state.read().unwrap();
    (
        rooms.len(),
        rooms.values().map(|chat| chat.users.len()).sum(),
    )
}

fn valid_name(name: &str) -> bool {
    name == name.trim()
        && !name.chars().any(char::is_control)
        && (1..=MAX_NAME_CHARS).contains(&name.chars().count())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    #[test]
    fn username_is_unique_per_room() {
        let state = SharedState::default();
        let _budi = join(&state, "rust", "budi").unwrap();

        assert!(join(&state, "rust", "budi").is_err());
        assert!(join(&state, "go", "budi").is_ok());
    }

    #[test]
    fn rejects_invalid_names() {
        let state = SharedState::default();
        let long = "x".repeat(33);
        for (room, name) in [
            ("rust", ""),
            ("rust", "   "),
            ("rust", " budi"),
            ("rust", long.as_str()),
            ("rust", "bu\ndi"),
            ("", "budi"),
            (long.as_str(), "budi"),
            ("ru\u{1b}st", "budi"),
        ] {
            assert!(
                join(&state, room, name).is_err(),
                "accepted {room:?}/{name:?}"
            );
        }

        let max = "é".repeat(32);
        assert!(join(&state, &max, &max).is_ok());
    }

    #[test]
    fn join_is_announced_to_the_room() {
        let state = SharedState::default();
        let (_budi, mut budi_rx, _) = join(&state, "rust", "budi").unwrap();
        let _ani = join(&state, "rust", "ani").unwrap();

        assert_eq!(
            drain(&mut budi_rx),
            [
                json!({"type": "join", "username": "budi"}),
                json!({"type": "join", "username": "ani"}),
            ]
        );
    }

    #[test]
    fn messages_reach_everyone_in_the_room_only() {
        let state = SharedState::default();
        let (budi, mut budi_rx, _) = join(&state, "rust", "budi").unwrap();
        let (_ani, mut ani_rx, _) = join(&state, "rust", "ani").unwrap();
        let (_eko, mut eko_rx, _) = join(&state, "go", "eko").unwrap();
        for rx in [&mut budi_rx, &mut ani_rx, &mut eko_rx] {
            drain(rx);
        }

        budi.say("halo");

        let halo = [json!({"type": "message", "username": "budi", "text": "halo"})];
        assert_eq!(drain(&mut budi_rx), halo);
        assert_eq!(drain(&mut ani_rx), halo);
        assert!(drain(&mut eko_rx).is_empty());
    }

    #[test]
    fn joiners_get_the_last_50_messages() {
        let state = SharedState::default();
        let (budi, _, _) = join(&state, "rust", "budi").unwrap();
        for i in 1..=55 {
            budi.say(&format!("m{i}"));
        }

        let (_ani, _, history) = join(&state, "rust", "ani").unwrap();

        let expected: Vec<Value> = (6..=55)
            .map(|i| json!({"type": "message", "username": "budi", "text": format!("m{i}")}))
            .collect();
        assert_eq!(history.iter().map(parse).collect::<Vec<_>>(), expected);
    }

    #[test]
    fn leaving_is_announced_and_empty_rooms_are_removed() {
        let state = SharedState::default();
        let (budi, mut budi_rx, _) = join(&state, "rust", "budi").unwrap();
        let ani = join(&state, "rust", "ani").unwrap();
        drain(&mut budi_rx);

        drop(ani);
        assert_eq!(
            drain(&mut budi_rx),
            [json!({"type": "leave", "username": "ani"})]
        );
        assert_eq!(stats(&state), (1, 1));

        drop(budi);
        assert_eq!(stats(&state), (0, 0));
    }

    #[test]
    fn messages_are_counted_for_metrics() {
        let state = SharedState::default();
        let (budi, _, _) = join(&state, "rust", "budi").unwrap();
        let before = MESSAGES.load(Ordering::Relaxed);

        budi.say("halo");

        assert!(MESSAGES.load(Ordering::Relaxed) > before);
    }

    /// Every event already waiting in `rx`, parsed.
    fn drain(rx: &mut broadcast::Receiver<Utf8Bytes>) -> Vec<Value> {
        std::iter::from_fn(|| rx.try_recv().ok())
            .map(|event| parse(&event))
            .collect()
    }

    fn parse(event: &Utf8Bytes) -> Value {
        serde_json::from_str(event.as_str()).unwrap()
    }
}
