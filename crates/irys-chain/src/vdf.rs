use std::{io::Write, sync::mpsc::{Receiver, Sender}};
use sha2::{Digest, Sha256};
use irys_types::{H256, HASHES_PER_CHECKPOINT, NUM_CHECKPOINTS_IN_VDF_STEP, VDF_SHA_1S};



pub fn run_vdf(seed: H256, new_seed_listener: Receiver<H256>, partition_channels: Vec<Sender<H256>>) {
    let mut hasher = Sha256::new();

    let mut hash: H256 = H256::from_slice(seed.as_bytes());

    loop {
        let mut checkpoints: Vec<H256> = Vec::with_capacity(NUM_CHECKPOINTS_IN_VDF_STEP);
        for i in 0..VDF_SHA_1S {
            hasher.update(hash);
            let hash_result= hasher.finalize_reset().to_vec();
            hash = H256::from_slice(&hash_result);
            if (i+1) % HASHES_PER_CHECKPOINT == 0 {
                // write checkpoint
                checkpoints.push(hash);
            }
        }

        for c in &partition_channels {
            dbg!("Sending hash {} to all partitions", hash);
            c.send(hash);
        }

        if let Ok(h) = new_seed_listener.try_recv() {
            hash = h;
        }
    }
    
}