use std::io;
use std::result;

/// The error type for our key value store.
#[derive(Debug)]
pub enum KvsError {
    /// Triggered when the provided key cannot be found.
    KeyNotFound,
    /// Triggered when the provided command cannot be found.
    /// It might indicate a corrupted log or a program bug.
    UnexpectedCommand,
    /// Triggered when an IO error occurs.
    Io(io::Error),
    /// Triggered when serializing/deserializing fails.
    Serde(serde_json::Error),
}

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> KvsError {
        KvsError::Io(err)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(err: serde_json::Error) -> KvsError {
        KvsError::Serde(err)
    }
}

/// The result type for our key value store
pub type Result<T> = result::Result<T, KvsError>;