use actix::{Actor, Context};
use irys_types::{Address, IrysBlockHeader, H256, NUM_REPLICAS_PER_LEDGER_INDEX};

/// Temporary struct tracking partition IDs per ledger index (will move to db)
#[derive(Debug)]
pub struct LedgerIndex {
    /// A list of partition ids assigned to replicate this ledger index
    pub partitions: Vec<H256>,
}

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

/// Temporarily track all of the ledger definitions inside the epoch service actor
#[derive(Debug)]
pub struct EpochServiceActor {
    /// The previous epoch hash is used in the current epochs tasks as a reliable source of random
    pub last_epoch_hash: H256,
    /// The LedgerIndexes that contain the partitions of the Submit ledger
    pub submit_ledger: Vec<LedgerIndex>,
    /// The ledgerIndexes that contain the partitions of the Publish Ledger
    pub publish_ledger: Vec<LedgerIndex>,
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
            last_epoch_hash,
            submit_ledger: Vec::new(),
            publish_ledger: Vec::new(),
            assigned_partitions: Vec::new(),
            active_partitions: Vec::new(),
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

    pub fn perform_epoch_tasks(new_epoch_block: IrysBlockHeader) {
        // Expire any ledger indexes (and their partitions) from the submit ledger

        // Compute Data uploaded to submit ledger last epoch

        // Calculate Num ledger indexes to add to submit ledger

        // Assign partitions to the submit ledger

        // Increase Publish ledger capacity to include entire submit ledger capacity

        // Assign partitions to the publish ledger

        // Project new capacity partitions based on active data partitions
    }
}
