use irys_types::H256;
#[derive(Debug)]
// State types
pub struct Permanent;
#[derive(Debug)]
pub struct Term {
    epoch_duration: u64,
}

// Common trait for ledger behaviors shared between both ledger types
pub trait DataLedger {}
impl DataLedger for Permanent {}
impl DataLedger for Term {}

#[derive(Debug)]
pub struct PartitionSlot {
    pub partitions: Vec<H256>,
}

// Base ledger structure with phantom state type
#[derive(Debug)]
pub struct Ledger<S: DataLedger> {
    slots: Vec<PartitionSlot>,
    state: S,
}

// Shared behavior for both ledger types
impl<S: DataLedger> Ledger<S> {
    pub fn allocate_partition_slots(&mut self, slots: u64) {
        for _ in 0..slots {
            self.slots.push(PartitionSlot {
                partitions: Vec::new(),
                // TODO: assign partitions out of capacity
            });
        }
    }

    pub fn slot_count(&self) -> usize {
        self.slots.len()
    }
}

// Permanent ledger specific behavior
impl Ledger<Permanent> {
    pub fn new() -> Self {
        Self {
            slots: Vec::new(),
            state: Permanent,
        }
    }
}

// Term ledger specific behavior
impl Ledger<Term> {
    pub fn new(epoch_length: u64) -> Self {
        Self {
            slots: Vec::new(),
            state: Term {
                epoch_duration: epoch_length,
            },
        }
    }

    pub fn epoch_duration(&self) -> u64 {
        self.state.epoch_duration
    }

    pub fn expire_old_slots(&mut self, current_height: u64) -> Vec<PartitionSlot> {
        let mut expired = Vec::new();
        // Can now use self.state.epoch_length for expiry logic
        expired
    }
}
