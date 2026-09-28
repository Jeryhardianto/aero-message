use std::sync::atomic::Ordering;
use std::time::Duration;

use aero_message::{app, room};
use tokio::net::TcpListener;

const METRICS_EVERY: Duration = Duration::from_secs(5);

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt::init();
    let state = room::SharedState::default();
    tokio::spawn(log_metrics(state.clone()));

    let listener = TcpListener::bind("0.0.0.0:3000").await?;
    tracing::info!("listening on ws://{}/chat/{{room}}", listener.local_addr()?);
    axum::serve(listener, app(state)).await
}

/// Logs open rooms, connections and message throughput every few seconds.
async fn log_metrics(state: room::SharedState) {
    let mut tick = tokio::time::interval(METRICS_EVERY);
    let mut last = 0;
    loop {
        tick.tick().await;
        let (rooms, connections) = room::stats(&state);
        let total = room::MESSAGES.load(Ordering::Relaxed);
        let msg_per_sec = (total - last) as f64 / METRICS_EVERY.as_secs_f64();
        last = total;
        tracing::info!(rooms, connections, msg_per_sec, "metrics");
    }
}
