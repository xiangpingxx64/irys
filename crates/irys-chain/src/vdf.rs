use std::sync::mpsc::Sender;
use sha2::{Digest, Sha256};

pub fn run_vdf(partition_channels: Vec<Sender<String>>) {
    let mut hasher = Sha256::new();
    hasher.update(b"hello world");
    let result = hasher.finalize().to_vec();

    for c in partition_channels {
        c.send(String::from(result));
    }
}