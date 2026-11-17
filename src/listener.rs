//! The `apalis::job::insert` notifications as a stream that survives a lost connection.
//!
//! `PgListener::into_stream` ends at its first error, and a Postgres restart produces one
//! (the server terminates the session, and the immediate reconnect fails while it is still
//! starting). The fetchers then depended on the poll fallback alone. This stream drops the
//! listener on any error and opens a new one, retrying with a capped backoff, and ends only
//! when the pool is closed.

use std::time::Duration;

use apalis_core::timer::sleep;
use futures::{Stream, StreamExt, stream};
use sqlx::{PgPool, postgres::PgListener};

use crate::InsertEvent;

/// The channel the insert trigger notifies on.
pub(crate) const INSERT_CHANNEL: &str = "apalis::job::insert";

/// The first wait before reconnecting after a failed attempt.
const MIN_RECONNECT_DELAY: Duration = Duration::from_millis(100);
/// The longest wait between reconnection attempts.
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(5);

struct State {
    pool: PgPool,
    listener: Option<PgListener>,
    delay: Duration,
}

async fn connect(pool: &PgPool) -> Result<PgListener, sqlx::Error> {
    let mut listener = PgListener::connect_with(pool).await?;
    listener.listen(INSERT_CHANNEL).await?;
    Ok(listener)
}

/// Every insert event on `pool`'s database, reconnecting as long as the pool is open.
/// Notifications sent while it is disconnected are lost; the poll fallback picks up those jobs.
pub(crate) fn insert_events(pool: PgPool) -> impl Stream<Item = InsertEvent> + Send + Unpin {
    let state = State {
        pool,
        listener: None,
        delay: MIN_RECONNECT_DELAY,
    };
    stream::unfold(state, |mut state| async move {
        loop {
            if state.pool.is_closed() {
                return None;
            }
            let listener = match state.listener.as_mut() {
                Some(listener) => listener,
                None => match connect(&state.pool).await {
                    Ok(listener) => {
                        state.delay = MIN_RECONNECT_DELAY;
                        state.listener.insert(listener)
                    }
                    Err(_) => {
                        sleep(state.delay).await;
                        state.delay = (state.delay * 2).min(MAX_RECONNECT_DELAY);
                        continue;
                    }
                },
            };
            match listener.recv().await {
                Ok(notification) => {
                    if let Ok(event) = serde_json::from_str::<InsertEvent>(notification.payload()) {
                        return Some((event, state));
                    }
                }
                // The connection is gone (or unusable): open a new one.
                Err(_) => state.listener = None,
            }
        }
    })
    .boxed()
}
