#[macro_use]
extern crate log;
extern crate structopt;

use clap::arg_enum;
use env_logger::Env;
use kvs::{KvStore, KvsServer, Result};
use std::env::current_dir;
use std::net::SocketAddr;
use std::process::exit;
use structopt::StructOpt;

arg_enum! {
    #[derive(Copy, Clone, PartialEq, Debug)]
    enum Engine {
        Kvs,
    }
}

#[derive(Debug, StructOpt)]
#[structopt(name = "kvs-server")]
struct ServerOption {
    #[structopt(
        long,
        help = "Sets the listening address",
        value_name = "IP:PORT",
        default_value = "127.0.0.1:4000",
        parse(try_from_str)
    )]
    addr: SocketAddr,
    #[structopt(
        long,
        help = "Sets the storage engine",
        value_name = "ENGINE-NAME",
        raw(possible_values = "&Engine::variants()"),
        case_insensitive = true
    )]
    engine: Engine,
}

fn main() {
    env_logger::from_env(Env::default().default_filter_or("info")).init();
    let server_option = ServerOption::from_args();
    info!("Staring kvs-server with: {:?}", server_option);

    if let Err(e) = run(server_option) {
        error!("{}", e);
        exit(1);
    }
}

fn run(options: ServerOption) -> Result<()> {
    let kvs_engine = KvStore::open(current_dir()?)?;

    info!("kvs-server {}", env!("CARGO_PKG_VERSION"));
    info!("Storage engine: Kvs");

    let server = KvsServer::new(kvs_engine);
    server.run(options.addr)
}
