use actix::{Actor, Context};
use color_eyre::eyre::eyre;
use eyre::{Error, Result};
use irys_types::{
    Address, IrysBlockHeader, CAPACITY_SCALAR, H256, NUM_BLOCKS_IN_EPOCH,
    NUM_REPLICAS_PER_LEDGER_INDEX, PARTITION_SIZE, U256,
};
use openssl::sha;
use std::ops::{Index, IndexMut};
/// Temporary struct tracking partition assignments to miners - will be moved to database
#[derive(Debug)]
pub struct PartitionAssignment {
    /// Hash of the partition
    pub partition_id: H256,
    /// Address of the miner it is assigned to
    pub miner_address: Address,
    /// If assigned to a ledger, the ledger number
    pub ledger_num: Option<u64>,
    /// If assigned to a ledger, the index in the ledger
    pub ledger_index: Option<u64>,
}

/// Represents different ledger types in the system with their corresponding
/// numeric identifiers.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Ledger {
    /// Publish (0): Used for publishing transactions to the permanent ledger
    Publish = 0,
    /// Submit (1): Used for submitting transactions to the network
    Submit = 1,
}
/// Temporary struct tracking partition IDs per ledger slot (will move to db)
#[derive(Debug)]
pub struct LedgerSlot {
    /// A list of partition ids assigned to replicate this ledger index
    pub partitions: Vec<H256>,
}
/// Provides a way to enforce accessing ledgers by their [Ledger] enum.
#[derive(Debug)]
pub struct Ledgers {
    /// Each ledger is a list of [LedgerSlot]s, and there are multiple ledgers
    pub ledgers: Vec<Vec<LedgerSlot>>,
}

impl Index<Ledger> for Ledgers {
    type Output = Vec<LedgerSlot>;
    fn index(&self, ledger: Ledger) -> &Self::Output {
        &self.ledgers[ledger as usize]
    }
}

impl IndexMut<Ledger> for Ledgers {
    fn index_mut(&mut self, ledger: Ledger) -> &mut Self::Output {
        &mut self.ledgers[ledger as usize]
    }
}

/// Temporarily track all of the ledger definitions inside the epoch service actor
#[derive(Debug)]
pub struct EpochServiceActor {
    /// The previous epoch hash is used in the current epochs tasks as a reliable source of random
    pub previous_epoch_hash: H256,
    /// The most recently activated partition hash
    pub latest_partition_hash: H256,
    /// List of all currently supported ledgers, each one a Vec of LedgerSlots
    pub ledgers: Ledgers,
    /// Which partitions are assigned to what miner addresses
    pub assigned_partitions: Vec<PartitionAssignment>,
    /// The partition ids (hashes) of each active partition
    pub active_partitions: Vec<H256>,
}

impl Actor for EpochServiceActor {
    type Context = Context<Self>;
}

impl EpochServiceActor {
    /// Create a new instance of the epoch service actor
    pub fn new(last_epoch_hash: H256) -> Self {
        Self {
            previous_epoch_hash: last_epoch_hash,
            ledgers: Ledgers {
                ledgers: vec![Vec::new(), Vec::new()],
            },
            assigned_partitions: Vec::new(),
            active_partitions: Vec::new(),
            latest_partition_hash: H256::zero(),
        }
    }

    /// Computes active capacity partitions available for pledges based on
    /// data partitions and scaling factor
    pub fn get_num_capacity_partitions(num_data_partitions: u64, capacity_scalar: u64) -> u64 {
        // Convert total_data_partitions to f64 for the logarithmic and multiplication operations
        let data_partitions_f64 = num_data_partitions as f64;
        let capacity_scalar_f64 = capacity_scalar as f64;

        // Submit and Publish ledgers both need at least one ledger index worth of data partitions
        let minimum_partition_count = (NUM_REPLICAS_PER_LEDGER_INDEX * 2) as f64;

        // Project capacity partitions based on data partitions in use
        let num_capacity_partitions = (data_partitions_f64 + minimum_partition_count)
            .log10()
            .ceil()
            * capacity_scalar_f64;

        num_capacity_partitions.ceil() as u64
    }

    /// Initialize genesis state by generating initial partition hashes if none exist
    pub fn try_genesis_init(&mut self, new_epoch_block: &IrysBlockHeader) -> Result<H256> {
        if self.active_partitions.is_empty() {
            // Initialize the genesis quantity of capacity partitions to activate
            let num_partitions = Self::get_num_capacity_partitions(0, CAPACITY_SCALAR);
            let mut prev_partition_hash = self.latest_partition_hash;
            for _i in 0..num_partitions {
                let next_part_hash = H256(hash_sha256(&prev_partition_hash.0)?);
                self.active_partitions.push(next_part_hash);
                prev_partition_hash = next_part_hash;
            }
            self.latest_partition_hash = prev_partition_hash;
        }
        Ok(self.latest_partition_hash)
    }

    /// TODO: we probably want some kind of type state pattern for ledgers
    /// where they can be term or perm and make it impossible to expire
    /// aan index in a perm ledger, while making it possible to read the
    /// term of a term ledger in blocks or epochs
    pub fn expire_partition_slots(&self, ledger: Ledger) {
        let submit_ledger = &self.ledgers[ledger];
        if submit_ledger.is_empty() {
            return;
        }

        // Can't expire partitions from the publish ledger
        if ledger == Ledger::Publish {
            return;
        }

        // Find index of last expired partition slot
        let expire_index = submit_ledger
            .iter()
            .enumerate()
            .find(|(_, ledger)| {
                // Find the block height of the last chunk in the ledger index
                // Compare it with the ledgers term, if its older expire the index
                // Loop though all it's partitions and make them capacity again
                false
            })
            .map(|(index, _)| index as u64);
    }

    pub fn calculate_needed_partition_slots(
        &self,
        new_epoch_block: &IrysBlockHeader,
        ledger: Ledger,
    ) -> u64 {
        let max_capacity = U256::from(self.ledgers[ledger].len() as u64 * PARTITION_SIZE);
        let ledger_size = new_epoch_block.ledgers[ledger as usize].ledger_size;

        // Add capacity slots if ledger usage exceeds 50% of partition size from max capacity
        let add_capacity_threshold = U256::from(max_capacity - PARTITION_SIZE / 2);
        let mut slots_to_add: u64 = 0;
        if ledger_size >= add_capacity_threshold {
            // Calculate needed slots + 1 buffer slot for safety margin
            let size_delta = ledger_size - max_capacity;
            slots_to_add = 1 + (size_delta.as_u64() / PARTITION_SIZE); // Add 1 for safety buffer
        }

        // Compute Data uploaded to the ledger last epoch
        // TODO: need a block index to do this
        if new_epoch_block.height >= NUM_BLOCKS_IN_EPOCH {
            // let last_epoch_block =
            //     block_index.get(new_new_epoch_block.height - NUM_BLOCKS_IN_EPOCH);
            // let data_added: u64 = ledger_size - last_epoch_block.ledger_size;
            // slots_to_add += Math::ceil(data_added / PARTITION_SIZE)
        }

        slots_to_add
    }

    pub fn allocate_ledger_slots(&mut self, ledger: Ledger, part_slots: u64) {
        // Add ledger slots sufficient to handle the same amount of data next epoch
        for _i in 0..part_slots {
            self.ledgers[ledger].push(LedgerSlot {
                partitions: Vec::new(),
            });
        }

        // Randomly (and deterministically) assign capacity partitions to ledger slots that need them
        // TODO: have this random assignment be weighted - possibly by blocks produced
    }

    pub fn perform_epoch_tasks(&mut self, new_epoch_block: IrysBlockHeader) -> Result<()> {
        // Validate this is an epoch block height
        if new_epoch_block.height % NUM_BLOCKS_IN_EPOCH != 0 {
            return Err(eyre!("This is not an epoch block"));
        }

        // TODO: Check to see if any partitions need to be removed from a ledger (pledging/staking rules)

        // Check to see if its time to initialize the ledgers
        self.try_genesis_init(&new_epoch_block)?;

        // Expire any LedgerSlots (and their partitions) from the submit ledger
        self.expire_partition_slots(Ledger::Submit);

        // Increase submit ledger capacity
        let part_slots = self.calculate_needed_partition_slots(&new_epoch_block, Ledger::Submit);
        self.allocate_ledger_slots(Ledger::Submit, part_slots);

        // Increase Publish ledger capacity
        let part_slots = self.calculate_needed_partition_slots(&new_epoch_block, Ledger::Publish);
        self.allocate_ledger_slots(Ledger::Publish, part_slots);

        // Project new capacity partitions based on active data partitions
        Ok(())
    }
}

/// SHA256 hash the message parameter
pub fn hash_sha256(message: &[u8]) -> Result<[u8; 32], Error> {
    let mut hasher = sha::Sha256::new();
    hasher.update(message);
    let result = hasher.finish();
    Ok(result)
}
