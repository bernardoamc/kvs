#![deny(missing_docs)]
//! An in-memory key-value store with a serialized log file as the source of truth.
//! The log is formatted as JSON due to its simplicity and easy debuggability.

pub use error::{KvsError, Result};
pub use kv::KvStore;

mod error;
mod kv;