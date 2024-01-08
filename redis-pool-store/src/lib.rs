use async_trait::async_trait;
pub use redis;
use redis::aio::ConnectionManager;
use std::fmt::{Debug, Formatter};
use time::OffsetDateTime;
use tower_sessions_core::session::Record;
use tower_sessions_core::{session::Id, session_store, SessionStore};

#[derive(Debug, thiserror::Error)]
pub enum RedisStoreError {
    #[error(transparent)]
    Redis(#[from] redis::RedisError),

    #[error(transparent)]
    Decode(#[from] rmp_serde::decode::Error),

    #[error(transparent)]
    Encode(#[from] rmp_serde::encode::Error),
}

impl From<RedisStoreError> for session_store::Error {
    fn from(err: RedisStoreError) -> Self {
        match err {
            RedisStoreError::Redis(inner) => session_store::Error::Backend(inner.to_string()),
            RedisStoreError::Decode(inner) => session_store::Error::Decode(inner.to_string()),
            RedisStoreError::Encode(inner) => session_store::Error::Encode(inner.to_string()),
        }
    }
}

/// A Redis session store.
#[derive(Clone)]
pub struct RedisPoolStore {
    client: ConnectionManager,
}

impl Debug for RedisPoolStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "tower-sessions client with Redis ConnectionManager")
    }
}

impl RedisPoolStore {
    /// Create a new Redis store with the provided client.
    pub fn new(client: ConnectionManager) -> Self {
        Self { client }
    }
}

#[async_trait]
impl SessionStore for RedisPoolStore {
    async fn save(&self, record: &Record) -> session_store::Result<()> {
        let expire = OffsetDateTime::unix_timestamp(record.expiry_date);
        let mut con = self.client.clone();
        redis::cmd("SET")
            .arg(format!("tower_session:{}", record.id))
            .arg(rmp_serde::to_vec(&record).map_err(RedisStoreError::Encode)?)
            .arg("EXAT") // EXAT: set expiry timestamp
            .arg(expire as usize)
            .query_async(&mut con)
            .await
            .map_err(RedisStoreError::Redis)?;
        Ok(())
    }

    async fn load(&self, session_id: &Id) -> session_store::Result<Option<Record>> {
        let mut con = self.client.clone();
        let data: Option<Vec<u8>> = redis::cmd("GET")
            .arg(format!("tower_session:{}", session_id))
            .query_async(&mut con)
            .await
            .map_err(RedisStoreError::Redis)?;
        if let Some(data) = data {
            Ok(Some(
                rmp_serde::from_slice(&data).map_err(RedisStoreError::Decode)?,
            ))
        } else {
            Ok(None)
        }
    }

    async fn delete(&self, session_id: &Id) -> session_store::Result<()> {
        let mut con = self.client.clone();
        redis::cmd("DEL")
            .arg(format!("tower_session:{}", session_id))
            .query_async(&mut con)
            .await
            .map_err(RedisStoreError::Redis)?;
        Ok(())
    }
}
