use thiserror::Error;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("failed to deserialize data: {0}")]
    DeserializeData(serde_json::Error),
    #[error("stream name is empty")]
    EmptyStreamName,

    // Database errors
    #[cfg(feature = "database")]
    #[error(transparent)]
    Database(#[from] sqlx::Error),
    #[cfg(feature = "database")]
    #[error("failed to decode: expected {expected}")]
    Decode { expected: &'static str },
    #[cfg(feature = "database")]
    #[error("failed to deserialize metadata: {0}")]
    DeserializeMetadata(serde_json::Error),
}
