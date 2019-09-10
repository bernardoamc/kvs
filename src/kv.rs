use crate::{KvsError, Result};

use std::io::prelude::*;
use std::io::{BufReader, SeekFrom};
use std::fs::{File, OpenOptions};
use std::collections::BTreeMap;
use std::path::{PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, value: String},
    Remove { key: String }
}

#[derive(Debug)]
pub struct CommandMetadata {
    position: u64,
    length: u64,
}

/// A struct representing our key-value store mechanism.
pub struct KvStore {
    path: PathBuf,
    writer: File,
    map: BTreeMap<String, CommandMetadata>,
}

impl KvStore {
    /// Initializes our key-value store with an empty state.
    ///
    /// ```
    /// use kvs::KvStore;
    ///
    /// let store = KvStore::new(path, log_file, hash_map);
    /// ```
    pub fn new(path: PathBuf, writer: File, map: BTreeMap<String, CommandMetadata>) -> Self {
        KvStore {
            path,
            writer,
            map,
        }
    }

    /// Opens the log file and reconstructs the key/value store in memory.
    /// Keys are stored in a HashMap pointing to positions in the log file.
    ///
    /// ```
    /// use kvs::KvStore;
    ///
    /// let store = KvStore::open(dir_path);
    /// ```
    pub fn open(dir_path: impl Into<PathBuf>) -> Result<KvStore> {
        let file_path = dir_path.into().join("log_file.log");

        let map: BTreeMap<String, CommandMetadata> = match File::open(&file_path) {
            Ok(read_log) => load(read_log)?,
            _ => BTreeMap::new(),
        };

        let log_file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .read(true)
            .open(file_path.to_owned())?;

        let store = KvStore::new(file_path.to_owned(), log_file, map);
        Ok(store)
    }

    /// Inserts a String `value` associated with a String `key`.
    ///
    /// # Arguments
    ///
    /// * `key` - A String that will be associated with the value
    /// * `value` - The String value
    ///
    /// ```
    /// use kvs::KvStore;
    ///
    /// let mut store = KvStore::open(dir_path);
    /// store.set("foo".to_owned(), "bar".to_owned());
    /// ```
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::Set { key: key.to_owned(), value: value.to_owned() };
        let pos = self.writer.seek(SeekFrom::End(0))?;
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;
        let new_pos = self.writer.seek(SeekFrom::End(0))?;

        self.map.insert(key, CommandMetadata { position: pos, length: (new_pos - pos)});
        Ok(())
    }

    /// Gets a String `value` associated with a String `key` when that `key` exists.
    ///
    /// # Arguments
    ///
    /// * `key` - A String from which the associated value will be fetched
    ///
    /// ```
    /// use kvs::KvStore;
    ///
    /// let mut store = KvStore::open(dir_path);;
    /// store.set("foo".to_owned(), "bar".to_owned());
    /// println!("{:?}", store.get("foo".to_owned()));
    /// ```
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        match self.map.get(&key) {
            Some(metadata) => {
                dbg!(metadata);
                dbg!(read_n(&mut self.writer, metadata.position, metadata.length)?);

                if let Command::Set { value, .. } = read_n(&mut self.writer, metadata.position, metadata.length)? {
                    Ok(Some(value))
                } else {
                    Err(KvsError::UnexpectedCommand)
                }
            },
            None => Ok(None),
        }
    }

    /// Removes a `key` and its associated value from our key-value store.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to be removed from the store
    ///
    /// ```
    /// use kvs::KvStore;
    ///
    /// let mut store = KvStore::open(dir_path);;
    /// store.set("foo".to_owned(), "bar".to_owned());
    /// store.remove("foo".to_owned());
    /// ```
    pub fn remove(&mut self, key: String) -> Result<()> {
        match self.map.remove(&key) {
            Some(_) => {
                let cmd = Command::Remove { key: key.to_owned() };
                serde_json::to_writer(&mut self.writer, &cmd)?;
                Ok(())
            },
            None => Err(KvsError::KeyNotFound)
        }
    }
}

fn load(log: File) -> Result<BTreeMap<String, CommandMetadata>> {
    let mut map: BTreeMap<String, CommandMetadata> = BTreeMap::new();
    let mut reader = BufReader::new(log);
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();

    while let Some(command) = stream.next() {
        let next_pos = stream.byte_offset() as u64;

        match command? {
            Command::Set {key, ..} => {
                map.insert(key, CommandMetadata { position: pos, length: (next_pos - pos) });
            },
            Command::Remove {key} => {
                map.remove(&key);
            }
        }

        pos = next_pos;
    }

    Ok(map)
}

fn read_n<R: Read + Seek>(mut reader: R, position: u64, bytes_to_read: u64) -> Result<Command> {
    reader.seek(SeekFrom::Start(position))?;
    let mut chunk = reader.take(bytes_to_read);
    let command = serde_json::from_reader(&mut chunk)?;

    Ok(command)
}