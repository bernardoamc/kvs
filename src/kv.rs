use crate::{KvsError, Result};

use std::io::prelude::*;
use std::io::{BufWriter, BufReader, SeekFrom};
use std::fs::{File, OpenOptions};
use std::collections::{BTreeMap, HashMap};
use std::path::{PathBuf};
use std::ffi::OsStr;

use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, value: String},
    Remove { key: String }
}

#[derive(Debug)]
pub struct CommandMetadata {
    file_index: u64,
    position: u64,
    length: u64,
}

/// A struct representing our key-value store mechanism.
pub struct KvStore {
    readers: HashMap<u64, BufReader<File>>,
    writer: BufWriter<File>,
    map: BTreeMap<String, CommandMetadata>,
    current_index: u64,
}

impl KvStore {
    /// Initializes our key-value store with an empty state.
    ///
    /// ```
    /// use kvs::KvStore;
    ///
    /// let store = KvStore::new(path, log_file, hash_map);
    /// ```
    pub fn new(
        readers: HashMap<u64, BufReader<File>>,
        writer: BufWriter<File>,
        map: BTreeMap<String, CommandMetadata>,
        current_index: u64
    ) -> Self {
        KvStore {
            readers,
            writer,
            map,
            current_index,
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
        let dir_path = dir_path.into();
        let mut readers: HashMap<u64, BufReader<File>> = HashMap::new();
        let mut map: BTreeMap<String, CommandMetadata> = BTreeMap::new();

        let file_indexes = fetch_file_indexes(dir_path.to_owned())?;
        load_files(dir_path.to_owned(), &file_indexes, &mut readers, &mut map)?;

        let new_index = file_indexes.last().unwrap_or(&0) + 1;
        let writer_path = dir_path.to_owned().join(format!("{}.log", new_index));

        let writer = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(&writer_path)?;

        readers.insert(new_index, BufReader::new(File::open(&writer_path)?));
        let store = KvStore::new(readers, BufWriter::new(writer), map, new_index);
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

        self.map.insert(key, CommandMetadata { file_index: self.current_index, position: pos, length: (new_pos - pos)});
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
                if let Some(mut reader) = self.readers.get_mut(&metadata.file_index) {
                    if let Command::Set { value, .. } = read_n(&mut reader, metadata)? {
                        Ok(Some(value))
                    } else {
                        Err(KvsError::UnexpectedCommand)
                    }
                } else {
                    return Err(KvsError::UnexpectedCommand);
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

fn fetch_file_indexes(dir_path: impl Into<PathBuf>) -> Result<Vec<u64>> {
    let mut indexes: Vec<u64> = std::fs::read_dir(dir_path.into())?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path
                .file_name()
                .and_then(OsStr::to_str)
                .map(|p| p.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();

    indexes.sort_unstable();
    Ok(indexes)
}

fn load_files(
    dir_path: impl Into<PathBuf>,
    file_indexes: &Vec<u64>,
    readers: &mut HashMap<u64, BufReader<File>>,
    map: &mut BTreeMap<String, CommandMetadata>
) -> Result<()> {
    let dir_path = dir_path.into();

    for file_index in file_indexes {
        let file_path = dir_path.join(format!("{}.log", file_index));
        let reader = OpenOptions::new().read(true).open(file_path)?;
        let mut buffer = BufReader::new(reader);

        load(file_index.to_owned(), &mut buffer, map)?;
        readers.insert(file_index.to_owned(), buffer);
    }

    Ok(())
}

fn load(file_index: u64, reader: &mut BufReader<File>, map: &mut BTreeMap<String, CommandMetadata>) -> Result<()> {
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();

    while let Some(command) = stream.next() {
        let next_pos = stream.byte_offset() as u64;

        match command? {
            Command::Set {key, ..} => {
                map.insert(key, CommandMetadata { file_index: file_index, position: pos, length: (next_pos - pos) });
            },
            Command::Remove {key} => {
                map.remove(&key);
            }
        }

        pos = next_pos;
    }

    Ok(())
}

fn read_n<R: Read + Seek>(mut reader: R, metadata: &CommandMetadata) -> Result<Command> {
    reader.seek(SeekFrom::Start(metadata.position))?;
    let mut chunk = reader.take(metadata.length);
    let command = serde_json::from_reader(&mut chunk)?;

    Ok(command)
}