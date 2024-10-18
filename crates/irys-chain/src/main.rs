mod app_state;
mod database;
mod partitions;
mod vdf;

use api_server::*;
use clap::Parser;
use database::open_or_create_db;
use partitions::{get_partitions, mine_partition};
use std::sync::mpsc;
use vdf::run_vdf;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Name of the person to greet
    #[arg(short, long, default_value = "./database")]
    database: String,
}

fn main() {
    let args = Args::parse();

    open_or_create_db(&args.database);

    let mut part_channels = Vec::new();

    for part in get_partitions() {
        let (tx, rx) = mpsc::channel();
        part_channels.push(tx);
        std::thread::spawn(move || mine_partition(rx));
    }

    std::thread::spawn(move || run_vdf(part_channels));

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let _ = runtime.block_on(async { api_server::run_server().await });
}
