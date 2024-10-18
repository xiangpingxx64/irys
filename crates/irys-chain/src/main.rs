use partitions::{get_partitions, mine_partition};
use vdf::run_vdf;
use std::sync::mpsc;

mod vdf;
mod partitions;

fn main() {
    let mut part_channels = Vec::new();

    for part in get_partitions() {
        let (tx, rx) = mpsc::channel();
        part_channels.push(tx);
        std::thread::spawn(move || mine_partition(rx));
    }


    std::thread::spawn(move || run_vdf(part_channels));

    println!("Hello, world!");
}
