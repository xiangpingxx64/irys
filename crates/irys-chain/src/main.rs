use database::open_or_create_db;
use partitions::{get_partitions, mine_partition};
use vdf::run_vdf;
use std::sync::mpsc;

mod vdf;
mod partitions;
mod app_state;
mod database;

fn main() {

    open_or_create_db();
    
    
    let mut part_channels = Vec::new();

    for part in get_partitions() {
        let (tx, rx) = mpsc::channel();
        part_channels.push(tx);
        std::thread::spawn(move || mine_partition(rx));
    }

    std::thread::spawn(move || run_vdf(part_channels));

    println!("Hello, world!");
}
