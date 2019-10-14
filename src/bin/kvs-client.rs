extern crate structopt;
use std::process::exit;
use structopt::StructOpt;

use kvs::{KvsClient, Result};
use std::net::SocketAddr;

#[derive(Debug, StructOpt)]
#[structopt()]
enum CommandOption {
    #[structopt(name = "set")]
    /// Associates the specified key with the specified value (set <KEY> <VALUE>)
    Set {
        #[structopt(name = "KEY")]
        key: String,
        #[structopt(name = "VALUE")]
        value: String,
        #[structopt(
            long,
            help = "Sets the server address",
            value_name = "IP:PORT",
            default_value = "127.0.0.1:4000",
            parse(try_from_str)
        )]
        addr: SocketAddr,
    },
    #[structopt(name = "rm")]
    /// Removes the specified key and associated value (rm <KEY>)
    Rm {
        #[structopt(name = "KEY")]
        key: String,
        #[structopt(
            long,
            help = "Sets the server address",
            value_name = "IP:PORT",
            default_value = "127.0.0.1:4000",
            parse(try_from_str)
        )]
        addr: SocketAddr,
    },
    #[structopt(name = "get")]
    /// Gets the value associated with a key (get <KEY>)
    Get {
        #[structopt(name = "KEY")]
        key: String,
        #[structopt(
            long,
            help = "Sets the server address",
            value_name = "IP:PORT",
            default_value = "127.0.0.1:4000",
            parse(try_from_str)
        )]
        addr: SocketAddr,
    },
}

fn main() {
    let command_option = CommandOption::from_args();
    if let Err(e) = run(command_option) {
        eprintln!("{}", e);
        exit(1);
    }
}

fn run(command_option: CommandOption) -> Result<()> {
    match command_option {
        CommandOption::Get { key, addr } => {
            let mut client = KvsClient::connect(addr)?;

            if let Some(value) = client.get(key)? {
                println!("{}", value);
            } else {
                println!("Key not found");
            }
        }
        CommandOption::Set { key, value, addr } => {
            let mut client = KvsClient::connect(addr)?;
            client.set(key, value)?;
        }
        CommandOption::Rm { key, addr } => {
            let mut client = KvsClient::connect(addr)?;
            client.remove(key)?;
        }
    }

    Ok(())
}
