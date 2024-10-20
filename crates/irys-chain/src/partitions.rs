use std::{ops::Index, sync::mpsc::Receiver};

use irys_types::{CHUNK_SIZE, H256, NUM_OF_CHUNKS_IN_PARTITION, U256};
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


        let chunks_buffer: Vec<u8> = Vec::with_capacity((NUM_OF_CHUNKS_IN_PARTITION * CHUNK_SIZE) as usize);

        // TODO: read chunks

        let mut hasher = Sha256::new();
        for i in (0..(NUM_OF_CHUNKS_IN_PARTITION*CHUNK_SIZE)).step_by(CHUNK_SIZE as usize) {
            let chunk: &[u8] = &chunks_buffer[(i as usize)..(i+CHUNK_SIZE) as usize];

            hasher.update(chunk);
            let hash = hasher.finalize_reset().to_vec();

            // TODO: check if difficulty higher now. Will look in DB for latest difficulty info and update difficulty
        
            if hash_to_number(&hash) >= U256::from(difficulty) {
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
        }
    }
}


fn hash_to_number(hash: &[u8]) -> U256 {
    U256::from_little_endian(hash)
}