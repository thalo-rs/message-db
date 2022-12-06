#[cfg(feature = "database")]
pub mod client;
#[cfg(feature = "database")]
pub mod consumer;
mod error;
pub mod message;
pub mod stream_name;

pub use error::Error;

pub type Result<T, E = Error> = std::result::Result<T, E>;
