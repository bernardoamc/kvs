use crate::{KvsEngine, Result};

use serde_json::Deserializer;
use std::io::{BufReader, BufWriter, Write};
use std::net::SocketAddr;
use std::net::{TcpListener, TcpStream};

use crate::protocol::{GetResponse, Protocol, RemoveResponse, SetResponse};

/// The server of our key-value store tied to a storage engine.
pub struct KvsServer<E: KvsEngine> {
    engine: E,
}

impl<E: KvsEngine> KvsServer<E> {
    /// Creates a `KvsServer` tied to a storage engine.
    pub fn new(engine: E) -> Self {
        KvsServer { engine }
    }

    /// Runs our KvsServer bound to the specified IP address.
    /// The server will be listening to incoming messages.
    pub fn run(mut self, addr: SocketAddr) -> Result<()> {
        let listener = TcpListener::bind(addr)?;
        info!("KvsServer listening in {}", addr);

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    if let Err(e) = self.handle_connection(stream) {
                        error!("Failed to handle connection: {}", e)
                    }
                }
                Err(e) => error!("Failed to establish connection: {}", e),
            }
        }

        Ok(())
    }

    fn handle_connection(&mut self, stream: TcpStream) -> Result<()> {
        let reader = BufReader::new(&stream);
        let mut writer = BufWriter::new(&stream);
        let peer_addr = stream.peer_addr()?;

        // https://docs.serde.rs/serde_json/de/struct.Deserializer.html#method.from_reader
        // https://doc.rust-lang.org/nightly/std/net/struct.TcpStream.html#impl-Read
        let commands = Deserializer::from_reader(reader).into_iter::<Protocol>();

        for command in commands {
            let command = command?;

            match command {
                Protocol::Get { key } => {
                    let response = match self.engine.get(key) {
                        Ok(value) => GetResponse::Ok(value),
                        Err(e) => GetResponse::Err(format!("{}", e)),
                    };

                    serde_json::to_writer(&mut writer, &response)?;
                    writer.flush()?;
                    debug!("GetResponse sent to {}: {:?}", peer_addr, response);
                }
                Protocol::Set { key, value } => {
                    let response = match self.engine.set(key, value) {
                        Ok(()) => SetResponse::Ok(()),
                        Err(e) => SetResponse::Err(format!("{}", e)),
                    };

                    serde_json::to_writer(&mut writer, &response)?;
                    writer.flush()?;
                    debug!("SetResponse sent to {}: {:?}", peer_addr, response);
                }
                Protocol::Remove { key } => {
                    let response = match self.engine.remove(key) {
                        Ok(()) => RemoveResponse::Ok(()),
                        Err(e) => RemoveResponse::Err(format!("{}", e)),
                    };

                    serde_json::to_writer(&mut writer, &response)?;
                    writer.flush()?;
                    debug!("RemoveResponse sent to {}: {:?}", peer_addr, response);
                }
            }
        }

        Ok(())
    }
}
