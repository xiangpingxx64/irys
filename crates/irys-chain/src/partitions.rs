use std::{ops::Index, sync::mpsc::Receiver};

use irys_types::{CHUNK_SIZE, H256, NUM_OF_CHUNKS_IN_PARTITION, RECALL_RANGE_CHUNK_COUNTER, U256};
use rand::{seq::SliceRandom, RngCore};
use sha2::{Digest, Sha256};

pub struct Partition {
    id: u64,
    mining_addr: H256
}

impl Default for Partition {
    fn default() -> Self {
        Self { id: 0, mining_addr: H256::random() }
    }
}

pub fn get_partitions() -> Vec<Partition> {
    vec![Partition::default(), Partition::default()]
}

pub struct SolutionContext {
    partition_id: u64,
    chunk_index: u64,
    mining_address: H256
}

pub fn mine_partition(partition: Partition, seed_receiver_channel: Receiver<H256>) {
    // Random difficulty
    let mut difficulty = U256::from_little_endian(H256::random().as_bytes());
    dbg!("Difficulty at start is {}", difficulty);
    loop {
        let mining_hash = match seed_receiver_channel.recv() {
            Ok(h) => h,
            Err(e) => panic!("Mine partition channel dead {}", e),
        };

        // Startup message to ignore
        if mining_hash == H256::zero() {
            continue;
        }

        // TODO: Use a real rpng
        let mining_hash_chunk_index = mining_hash.to_low_u64_le();

        // Starting chunk index within partition
        let mining_hash_number = NUM_OF_CHUNKS_IN_PARTITION % mining_hash_chunk_index;


        // Create a contiguous piece of memory on the heap where chunks can be written into
        let mut chunks_buffer: Vec<[u8; CHUNK_SIZE as usize]> = Vec::with_capacity((RECALL_RANGE_CHUNK_COUNTER * CHUNK_SIZE) as usize);


        // TODO: read chunks. For now creates random
        for _ in 0..RECALL_RANGE_CHUNK_COUNTER {
            let mut data = [0u8; CHUNK_SIZE as usize];
            rand::thread_rng().fill_bytes(&mut data);
            chunks_buffer.push(data);
        }

        let mut hasher = Sha256::new();
        for i in 0..RECALL_RANGE_CHUNK_COUNTER {
            let chunk: &[u8] = &chunks_buffer[i as usize];

            hasher.update(chunk);
            let hash = hasher.finalize_reset().to_vec();

            // TODO: check if difficulty higher now. Will look in DB for latest difficulty info and update difficulty
        
            let solution_number = hash_to_number(&hash);
            dbg!("Solution attempt is {}", solution_number);
            if solution_number >= U256::from(difficulty) {
                dbg!("SOLUTION FOUND!!!!!!!!!");
                let solution = SolutionContext {
                    partition_id: partition.id,
                    // TODO: Fix
                    chunk_index: 0,
                    mining_address: partition.mining_addr,
                };
                // TODO: Send info to block builder code

                // TODO: Let all partitions know to stop mining

                // Once solution is sent stop mining and let all other partitions know
                break;
            }
        }
    }
}


fn hash_to_number(hash: &[u8]) -> U256 {
    U256::from_little_endian(hash)
}