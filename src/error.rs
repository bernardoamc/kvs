use failure::Fail;
use std::io;
use std::result;

/// The error type for our key value store.
#[derive(Fail, Debug)]
pub enum KvsError {
    /// Triggered when the provided key cannot be found.
    #[fail(display = "Key not found error")]
    KeyNotFound,
    /// Triggered when the provided command cannot be found.
    /// It might indicate a corrupted log or a program bug.
    #[fail(display = "UnexpectedCommand error")]
    UnexpectedCommand,
    /// Triggered when an IO error occurs.
    #[fail(display = "IO error: {}", _0)]
    Io(io::Error),
    /// Triggered when serializing/deserializing fails.
    #[fail(display = "serde_json error: {}", _0)]
    Serde(serde_json::Error),
    /// Error with a string message.
    #[fail(display = "{}", _0)]
    MessageError(String),
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
