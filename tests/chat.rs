//! Real WebSocket clients against a server on a random local port.

use std::net::SocketAddr;
use std::time::Duration;

use aero_message::{app, room::SharedState};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

type Client = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[tokio::test]
async fn members_see_each_other_join_chat_and_leave() {
    let addr = start_server().await;
    let mut budi = connect(addr, "rust", "budi").await;
    assert_eq!(
        next_event(&mut budi).await,
        json!({"type": "join", "username": "budi"})
    );

    let mut ani = connect(addr, "rust", "ani").await;
    assert_eq!(
        next_event(&mut budi).await,
        json!({"type": "join", "username": "ani"})
    );

    ani.send(Message::text("halo")).await.unwrap();
    assert_eq!(
        next_event(&mut budi).await,
        json!({"type": "message", "username": "ani", "text": "halo"})
    );

    ani.close(None).await.unwrap();
    assert_eq!(
        next_event(&mut budi).await,
        json!({"type": "leave", "username": "ani"})
    );
}

#[tokio::test]
async fn closing_client_gets_the_close_reply() {
    let addr = start_server().await;
    let mut budi = connect(addr, "rust", "budi").await;
    next_event(&mut budi).await;

    budi.close(None).await.unwrap();
    assert!(matches!(next_frame(&mut budi).await, Message::Close(_)));
}

#[tokio::test]
async fn rooms_do_not_hear_each_other() {
    let addr = start_server().await;
    let mut budi = connect(addr, "rust", "budi").await;
    let mut eko = connect(addr, "go", "eko").await;
    assert_eq!(next_event(&mut budi).await["username"], "budi");
    assert_eq!(next_event(&mut eko).await["username"], "eko");

    budi.send(Message::text("halo")).await.unwrap();
    assert_eq!(next_event(&mut budi).await["text"], "halo");

    eko.send(Message::text("ping")).await.unwrap();
    assert_eq!(
        next_event(&mut eko).await,
        json!({"type": "message", "username": "eko", "text": "ping"})
    );
}

#[tokio::test]
async fn late_joiners_get_recent_messages_before_their_join() {
    let addr = start_server().await;
    let mut budi = connect(addr, "rust", "budi").await;
    next_event(&mut budi).await;
    for text in ["satu", "dua"] {
        budi.send(Message::text(text)).await.unwrap();
        next_event(&mut budi).await;
    }

    let mut ani = connect(addr, "rust", "ani").await;
    assert_eq!(
        next_event(&mut ani).await,
        json!({"type": "message", "username": "budi", "text": "satu"})
    );
    assert_eq!(
        next_event(&mut ani).await,
        json!({"type": "message", "username": "budi", "text": "dua"})
    );
    assert_eq!(
        next_event(&mut ani).await,
        json!({"type": "join", "username": "ani"})
    );
}

#[tokio::test]
async fn refused_joins_are_closed_with_policy_code() {
    let addr = start_server().await;
    let mut budi = connect(addr, "rust", "budi").await;
    next_event(&mut budi).await;

    let taken = format!("ws://{addr}/chat/rust?username=budi");
    let missing = format!("ws://{addr}/chat/rust");
    for url in [taken, missing] {
        let (mut client, _) = connect_async(url.as_str()).await.unwrap();
        match next_frame(&mut client).await {
            Message::Close(Some(frame)) => assert_eq!(u16::from(frame.code), 1008, "{url}"),
            other => panic!("{url}: expected a close frame, got {other:?}"),
        }
    }
}

#[tokio::test]
async fn oversized_messages_are_dropped_with_their_sender() {
    let addr = start_server().await;
    let mut budi = connect(addr, "rust", "budi").await;
    next_event(&mut budi).await;
    let mut ani = connect(addr, "rust", "ani").await;
    next_event(&mut budi).await;

    ani.send(Message::text("x".repeat(64 * 1024)))
        .await
        .unwrap();
    assert_eq!(
        next_event(&mut budi).await,
        json!({"type": "leave", "username": "ani"})
    );
}

async fn start_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app(SharedState::default()))
            .await
            .unwrap()
    });
    addr
}

async fn connect(addr: SocketAddr, room: &str, username: &str) -> Client {
    let url = format!("ws://{addr}/chat/{room}?username={username}");
    connect_async(url).await.unwrap().0
}

/// The next frame, which must be a JSON event.
async fn next_event(client: &mut Client) -> Value {
    match next_frame(client).await {
        Message::Text(text) => serde_json::from_str(text.as_str()).unwrap(),
        other => panic!("expected an event, got {other:?}"),
    }
}

async fn next_frame(client: &mut Client) -> Message {
    tokio::time::timeout(Duration::from_secs(5), client.next())
        .await
        .expect("no frame within 5s")
        .expect("connection ended")
        .unwrap()
}
