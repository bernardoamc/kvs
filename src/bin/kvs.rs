extern crate structopt;
use structopt::StructOpt;
use std::process::exit;
use std::env::current_dir;

use kvs::{KvStore, KvsError};

#[derive(Debug, StructOpt)]
#[structopt()]
enum Config {
    #[structopt(name = "set")]
    /// Associates the specified key with the specified value (set <KEY> <VALUE>)
    Set {
        #[structopt(name = "KEY")]
        key: String,
        #[structopt(name = "VALUE")]
        value: String,
    },
    #[structopt(name = "rm")]
    /// Removes the specified key and associated value (rm <KEY>)
    Rm {
        #[structopt(name = "KEY")]
        key: String,
    },
    #[structopt(name = "get")]
    /// Gets the value associated with a key (get <KEY>)
    Get {
        #[structopt(name = "KEY")]
        key: String,
    }
}

fn main() {
    let config = Config::from_args();
    let current_dir = current_dir().unwrap();

    let mut store = match KvStore::open(current_dir) {
        Ok(store) => store,
        Err(KvsError::Io(e)) => {
            println!("{}, Unable to load store!", e);
            exit(1);
        },
        _ => {
            println!("Another error");
            exit(1);
        }
    };

    match config {
        Config::Get { key } => { 
            if let Ok(Some(value)) = store.get(key) {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        Config::Set { key, value } => { store.set(key, value).unwrap(); }
        Config::Rm { key } => {
            if let Ok(()) = store.remove(key) {
                {}
            } else {
                println!("Key not found");
                exit(1);
            }
        }
    }
}
