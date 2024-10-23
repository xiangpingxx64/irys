mod app_state;
mod config;
mod database;
mod partitions;
mod tables;
mod vdf;
mod block_producer;

use clap::Parser;
use config::get_data_dir;
use database::open_or_create_db;
use irys_types::H256;
use partitions::{get_partitions, mine_partition};
use std::{str::FromStr, sync::mpsc};
use vdf::run_vdf;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Name of the person to greet
    #[arg(short, long, default_value = "./database")]
    database: String,
}

fn main() -> eyre::Result<()> {
    let args = Args::parse();

    let db_path = get_data_dir();

    let db = open_or_create_db(&args.database)?;

    let mut part_channels = Vec::new();

    for part in get_partitions() {
        let (tx, rx) = mpsc::channel();
        part_channels.push(tx.clone());
        std::thread::spawn(move || mine_partition(part, rx));
    }

    let (new_seed_tx, new_seed_rx) = mpsc::channel::<H256>();

    std::thread::spawn(move || run_vdf(H256::random(), new_seed_rx, part_channels));

    reth_node_bridge::run_node(new_seed_tx).unwrap();

    Ok(())
}
