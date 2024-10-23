mod block_producer;
mod config;
mod database;
mod partitions;
mod tables;
mod vdf;

use actix::Actor;
use block_producer::BlockProducerActor;
use clap::Parser;
use config::get_data_dir;
use database::open_or_create_db;
use irys_types::{app_state::AppState, H256};
use partitions::{get_partitions, mine_partition, PartitionMiningActor};
use std::{
    str::FromStr,
    sync::{mpsc, Arc},
};
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

    let block_producer_actor = BlockProducerActor {};

    let block_producer_addr = block_producer_actor.start();

    let mut part_actors = Vec::new();

    for part in get_partitions() {
        let partition_mining_actor = PartitionMiningActor::new(part, block_producer_addr.clone());
        part_actors.push(partition_mining_actor.start());
    }

    let (new_seed_tx, new_seed_rx) = mpsc::channel::<H256>();

    std::thread::spawn(move || run_vdf(H256::random(), new_seed_rx, part_actors));

    let app_state = AppState {};

    let global_app_state = Arc::new(app_state);

    irys_reth_node_bridge::run_node(new_seed_tx).unwrap();

    Ok(())
}
