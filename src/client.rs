use crate::{KvsError, Result};

use crate::protocol::{GetResponse, Protocol, RemoveResponse, SetResponse};
use serde::Deserialize;
use serde_json::de::IoRead;
use serde_json::Deserializer;
use std::io::{BufReader, BufWriter, Write};
use std::net::{SocketAddr, TcpStream};

/// The client of our key-value that connects to `KvsServer`.
pub struct KvsClient {
    reader: Deserializer<IoRead<BufReader<TcpStream>>>,
    writer: BufWriter<TcpStream>,
}

impl KvsClient {
    /// Open the connection with the server and returns a KvsClient struct.
    pub fn connect(addr: SocketAddr) -> Result<Self> {
        let reader = TcpStream::connect(addr)?;
        let writer = reader.try_clone()?;

        Ok(KvsClient {
            reader: Deserializer::from_reader(BufReader::new(reader)),
            writer: BufWriter::new(writer),
        })
    }

    /// Sends a GET request and parses the response.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        serde_json::to_writer(&mut self.writer, &Protocol::Get { key })?;
        self.writer.flush()?;

        // https://docs.serde.rs/serde/trait.Deserialize.html#tymethod.deserialize
        match GetResponse::deserialize(&mut self.reader)? {
            GetResponse::Ok(value) => Ok(value),
            GetResponse::Err(e) => Err(KvsError::MessageError(e)),
        }
    }

    /// Sends a SET request and parses the response.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        serde_json::to_writer(&mut self.writer, &Protocol::Set { key, value })?;
        self.writer.flush()?;

        // https://docs.serde.rs/serde/trait.Deserialize.html#tymethod.deserialize
        match SetResponse::deserialize(&mut self.reader)? {
            SetResponse::Ok(_) => Ok(()),
            SetResponse::Err(e) => Err(KvsError::MessageError(e)),
        }
    }

    /// Sends a REMOVE request and parses the response.
    pub fn remove(&mut self, key: String) -> Result<()> {
        serde_json::to_writer(&mut self.writer, &Protocol::Remove { key })?;
        self.writer.flush()?;

        // https://docs.serde.rs/serde/trait.Deserialize.html#tymethod.deserialize
        match RemoveResponse::deserialize(&mut self.reader)? {
            RemoveResponse::Ok(_) => Ok(()),
            RemoveResponse::Err(e) => Err(KvsError::MessageError(e)),
        }
    }
}
