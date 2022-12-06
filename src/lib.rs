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

#![warn(missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

#[cfg(feature = "database")]
pub mod database;
pub mod message;
pub mod stream_name;

mod error;

pub use error::*;
