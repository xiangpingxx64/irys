mod app_state;
mod config;
mod database;
mod partitions;
mod vdf;

use api_server::*;
use clap::Parser;
use database::open_or_create_db;
use partitions::{get_partitions, mine_partition, Partition};
use std::{str::FromStr, sync::mpsc};
use vdf::run_vdf;
use irys_types::H256;

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

    let db = open_or_create_db(&args.database)?;

    let mut part_channels = Vec::new();

    for part in get_partitions() {
        let (tx, rx) = mpsc::channel();
        part_channels.push(tx.clone());
        std::thread::spawn(move || mine_partition(part, rx));
    }

    let (new_seed_tx, new_seed_rx) = mpsc::channel();

    let part_channels_cloned = part_channels.clone();
    std::thread::spawn(move || run_vdf(H256::random(), new_seed_rx, part_channels_cloned));

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    dbg!(&part_channels);

    for c in &part_channels {
        c.send(H256::zero());
    }

    let _ = runtime.block_on(async { 
        dbg!(api_server::run_server().await)
     });

    Ok(())
}
