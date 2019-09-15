use crate::{KvsError, Result};

use std::io::prelude::*;
use std::io::{BufWriter, BufReader, SeekFrom};
use std::fs::{File, OpenOptions};
use std::collections::{BTreeMap, HashMap};
use std::path::{PathBuf};
use std::ffi::OsStr;

use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;

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
    path: PathBuf,
    readers: HashMap<u64, BufReader<File>>,
    writer: BufWriter<File>,
    map: BTreeMap<String, CommandMetadata>,
    current_index: u64,
    umcompacted_bytes: u64,
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
        path: PathBuf,
        readers: HashMap<u64, BufReader<File>>,
        writer: BufWriter<File>,
        map: BTreeMap<String, CommandMetadata>,
        current_index: u64,
        umcompacted_bytes: u64,
    ) -> Self {
        KvStore {
            path,
            readers,
            writer,
            map,
            current_index,
            umcompacted_bytes,
        }
    }

    /// Opens each log file and reconstructs the key/value store in memory.
    /// Keys are stored in a BTreeMap pointing to positions in their respective log file.
    ///
    /// A new log file is always generated in this step to serve as the writer file.
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
        let total_umcompacted_bytes = load_files(dir_path.to_owned(), &file_indexes, &mut readers, &mut map)?;

        let new_index = file_indexes.last().unwrap_or(&0) + 1;
        let writer_path = dir_path.to_owned().join(format!("{}.log", new_index));

        let writer = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(&writer_path)?;

        readers.insert(new_index, BufReader::new(File::open(&writer_path)?));
        let store = KvStore::new(dir_path, readers, BufWriter::new(writer), map, new_index, total_umcompacted_bytes);
        Ok(store)
    }

    /// Serializes a Command::Set and appends it to the writer log file.
    /// Once this operation is sucessful inserts the value and metadata to our BTreeMap.
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

    /// Fetches the serialized command associated with the `key` from a log file,
    /// unserializes it and returns the associated value.
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
        let metadata = match self.map.get(&key) {
            Some(metadata) => metadata,
            None => return Ok(None),
        };

        let mut reader = self.readers
            .get_mut(&metadata.file_index)
            .ok_or(KvsError::UnexpectedCommand)?;

        if let Command::Set { value, .. } = read_command(&mut reader, metadata)? {
            Ok(Some(value))
        } else {
            Err(KvsError::UnexpectedCommand)
        }
    }

    /// Removes a `key` and its associated metadata from our BTreeMap and
    /// writes a serialized Command::Remove to our writer log file.
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
        self.map.remove(&key).ok_or(KvsError::KeyNotFound)?;

        let cmd = Command::Remove { key: key.to_owned() };
        serde_json::to_writer(&mut self.writer, &cmd)?;
        Ok(())
    }

    /// Compacts log files once the total amount of umcompacted bytes surpasses the
    /// COMPACTION_THRESHOLD.
    ///
    /// ```
    /// use kvs::KvStore;
    ///
    /// let mut store = KvStore::open(dir_path);;
    /// store.compact();
    /// ```
    pub fn compact(&mut self) -> Result<()> {
        if self.umcompacted_bytes <= COMPACTION_THRESHOLD {
            return Ok(())
        }

        let mut writer_pos: u64 = 0;

        for cmd_metadata in self.map.values_mut() {
            let reader = self.readers
                .get_mut(&cmd_metadata.file_index)
                .ok_or(KvsError::UnexpectedCommand)?;

            reader.seek(SeekFrom::Start(cmd_metadata.position))?;
            let mut chunk = reader.take(cmd_metadata.length);
            let len = std::io::copy(&mut chunk, &mut self.writer)?;
            *cmd_metadata = CommandMetadata { file_index: self.current_index, position: writer_pos, length: len };
            writer_pos += len;
        }

        self.writer.flush()?;
        let stale_log_indexes: Vec<u64> = self.readers.keys().filter(|key| **key < self.current_index).cloned().collect();

        for stale_log_index in stale_log_indexes {
            self.readers.remove(&stale_log_index);
            let stale_path = self.path.to_owned().join(format!("{}.log", stale_log_index));
            std::fs::remove_file(stale_path)?;
        }

        self.umcompacted_bytes = 0;
        Ok(())
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
) -> Result<u64> {
    let dir_path = dir_path.into();
    let mut total_umcompacted_bytes: u64 = 0;

    for file_index in file_indexes {
        let file_path = dir_path.join(format!("{}.log", file_index));
        let reader = OpenOptions::new().read(true).open(file_path)?;
        let mut buffer = BufReader::new(reader);

        total_umcompacted_bytes += load_file(file_index.to_owned(), &mut buffer, map)?;
        readers.insert(file_index.to_owned(), buffer);
    }

    Ok(total_umcompacted_bytes)
}

fn load_file(file_index: u64, reader: &mut BufReader<File>, map: &mut BTreeMap<String, CommandMetadata>) -> Result<u64> {
    let mut pos = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<Command>();
    let mut umcompacted_bytes: u64 = 0;

    while let Some(command_result) = stream.next() {
        let next_pos = stream.byte_offset() as u64;
        let command = command_result?;

        umcompacted_bytes += load_command(map, command, file_index, pos, next_pos);
        pos = next_pos;
    }

    Ok(umcompacted_bytes)
}

/// Load command into our BTreeMap and return the length of superseeded commands
fn load_command(map: &mut BTreeMap<String, CommandMetadata>, command: Command, file_index: u64, pos: u64, next_pos: u64) -> u64 {
    let old_metadata = match command {
        Command::Set {key, ..} => {
            map.insert(key, CommandMetadata { file_index: file_index, position: pos, length: (next_pos - pos) })
        },
        Command::Remove {key} => {
            map.remove(&key)
        }
    };

    match old_metadata {
        Some(metadata) => metadata.length,
        None => 0,
    }
}

fn read_command<R: Read + Seek>(mut reader: R, metadata: &CommandMetadata) -> Result<Command> {
    reader.seek(SeekFrom::Start(metadata.position))?;
    let mut chunk = reader.take(metadata.length);
    let command = serde_json::from_reader(&mut chunk)?;

    Ok(command)
}