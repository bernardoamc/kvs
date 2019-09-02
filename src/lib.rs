/// An in-memory key-value store.
/// 
/// The log is formatted as JSON due to its simplicity and
/// easy debuggability.

use std::fs::{File, OpenOptions};
use std::collections::HashMap;
use std::result;
use std::path::{PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

use std::io;
use std::io::{BufReader};

#[derive(Debug)]
pub enum KvsError {
    KeyNotFound,
    Io(io::Error),
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

pub type Result<T> = result::Result<T, KvsError>;

/// A struct representing our key-value store mechanism.
pub struct KvStore {
    path: PathBuf,
    writer: File,
    map: HashMap<String, String>,
}

impl KvStore {
    /// Initializes our key-value store with an empty state.
    ///
    /// ```
    /// use kvs::KvStore;
    ///
    /// let store = KvStore::new(path, log_file, hash_map);
    /// ```
    pub fn new(path: PathBuf, writer: File, map: HashMap<String, String>) -> Self {
        KvStore {
            path,
            writer,
            map,
        }
    }

    pub fn open(dir_path: impl Into<PathBuf>) -> Result<KvStore> {
        let file_path = dir_path.into().join("log_file.log");
        
        let map: HashMap<String, String> = match File::open(&file_path) {
            Ok(read_log) => load(read_log)?,
            _ => HashMap::new(),
        };

        let log_file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
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
        serde_json::to_writer(&mut self.writer, &cmd)?;

        self.map.insert(key, value);
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
        Ok(self.map.get(&key).cloned())
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

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, value: String},
    Remove { key: String }
}

fn load(log: File) -> Result<HashMap<String, String>> {
    let mut map: HashMap<String, String> = HashMap::new();
    let reader = BufReader::new(log);
    let mut commands = Deserializer::from_reader(reader).into_iter::<Command>();

    while let Some(command) = commands.next() {
        match command? {
            Command::Set {key, value} => {
                map.insert(key, value);
            },
            Command::Remove {key} => {
                map.remove(&key);
            }
        }
    }

    Ok(map)
}