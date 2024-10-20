use std::{ops::Index, sync::mpsc::Receiver};

use irys_types::{CHUNK_SIZE, H256, NUM_OF_CHUNKS_IN_PARTITION};
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
    let mut difficulty = 0;
    loop {
        let mining_hash = match seed_receiver_channel.recv() {
            Ok(h) => h,
            Err(_) => panic!("Mine partition channel dead"),
        };

        // TODO: Use a real rpng
        let mining_hash_chunk_index = mining_hash.to_low_u64_le();

        let mining_hash_number = NUM_OF_CHUNKS_IN_PARTITION % mining_hash_chunk_index;


        let chunks_buffer: Vec<u8> = Vec::with_capacity(NUM_OF_CHUNKS_IN_PARTITION * CHUNK_SIZE);

        // TODO: read chunks

        let hasher = Sha256::new();
        for i in 0..(NUM_OF_CHUNKS_IN_PARTITION*CHUNK_SIZE) {
            let x = chunks_buffer[i..i+CHUNK_SIZE];

            hasher.update(hash);
            let hash = hasher.finalize_reset().as_slice();

            // TODO: check if difficulty higher now. Will look in DB for latest difficulty info and update difficulty
        
            if hash_to_number(hash) >= difficulty {
                let solution = SolutionContext {
                    partition_id: partition.id,
                    chunk_index: todo!(),
                    mining_address: partition.mining_addr,
                };
                // TODO: Send info to block builder code

                // TODO: Let all partitions know to stop mining

                // Once solution is sent stop mining and let all other partitions know
                break;
            }

            *i = *i + CHUNK_SIZE;
        }
    }
}


fn hash_to_number(hash: &[u8]) -> u64 {
    u64::from_le_bytes(hash)
}