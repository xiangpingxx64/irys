pub const BLOCK_TIME: u64 = 30_000;

pub const CHUNK_SIZE: u64 = 256 * 1024;

pub const NUM_OF_CHUNKS_IN_PARTITION: u64 = 10;

pub const PARTITION_SIZE: u64 = CHUNK_SIZE * NUM_OF_CHUNKS_IN_PARTITION;

pub const RECALL_RANGE_CHUNK_COUNTER: u64 = 2;

// Reset the nonce limiter (vdf) once every 1200 steps/seconds or every ~20 min
pub const NONCE_LIMITER_RESET_FREQUENCY: usize = 10 * 120;

// 25 checkpoints 40 ms each = 1000 ms
pub const NUM_CHECKPOINTS_IN_VDF_STEP: usize = 25;

// Typical ryzen 5900X iterations for 1 sec
// pub const VDF_SHA_1S: u64 = 15_000_000;
pub const VDF_SHA_1S: u64 = 100_000;

pub const HASHES_PER_CHECKPOINT: u64 = VDF_SHA_1S / NUM_CHECKPOINTS_IN_VDF_STEP as u64;
