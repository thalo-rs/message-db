//! # Message DB
//!
//! **Rust Client for the Microservice Native Event Store and Message Store for
//! Postgres**
//!
//! A fully-featured event store and message store implemented in PostgreSQL for
//! Pub/Sub, Event Sourcing, Messaging, and Evented Microservices applications.
//!
//! For more information, see the Message DB project page.
//! <https://github.com/message-db/message-db>
//!
//! # Example
//!
//! ```ignore
//! use message_db::database::{MessageStore, WriteMessageOpts};
//! use serde_json::json;
//!
//! // Connect to MessageDb
//! let message_store = MessageStore::connect("postgres://postgres:password@localhost:5432/postgres").await?;
//!
//! // Get last stream message
//! let last_message = MessageStore::get_last_stream_message(&message_store, "account-123", None).await?;
//!
//! // Write message
//! let last_message = MessageStore::write_message(
//!     &message_store,
//!     "account-123",
//!     "AccountOpened",
//!     &json!({ "initial_balance": 0 }),
//!     &WriteMessageOpts::default(),
//! ).await?;
//! ```

#![warn(missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

#[cfg(feature = "database")]
pub mod database;
pub mod message;
pub mod stream_name;

mod error;

pub use error::*;
