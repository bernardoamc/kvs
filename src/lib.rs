#![deny(missing_docs)]
//! An in-memory key-value store with a serialized log file as the source of truth.
//! The log is formatted as JSON due to its simplicity and easy debuggability.

#[macro_use]
extern crate log;

mod client;
mod engines;
mod error;
mod protocol;
mod server;

pub use client::KvsClient;
pub use engines::{KvStore, KvsEngine};
pub use error::{KvsError, Result};
pub use server::KvsServer;
