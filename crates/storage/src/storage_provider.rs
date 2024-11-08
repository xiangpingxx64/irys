use crate::partition_provider::PartitionStorageProvider;
use eyre::eyre;
use irys_types::{block_production::PartitionId, ChunkBin, ChunkState, Interval, IntervalState};
use std::{collections::HashMap, sync::Arc};

#[derive(Debug, Clone)]
/// Storage provider struct - top level structure, used to interact with other storage components
pub struct StorageProvider {
    /// Map of partition IDs to storage actors
    partition_providers: Arc<HashMap<PartitionId, PartitionStorageProvider>>,
}

impl StorageProvider {
    /// create a new storage provider
    pub fn new(
        partition_providers: Option<HashMap<PartitionId, PartitionStorageProvider>>,
    ) -> Self {
        return Self {
            partition_providers: Arc::new(partition_providers.unwrap_or(HashMap::new())),
        };
    }

    // /// add a storage provider
    // pub fn add_provider(&mut self, partition_id: PartitionId, storage: PartitionStorageProvider) {
    //     self.partition_providers.insert(partition_id, storage);
    // }

    /// read an interval of chunks from a partition
    pub fn read_chunks(
        &self,
        partition_id: PartitionId,
        read_interval: Interval<u32>,
        expected_state: Option<ChunkState>,
    ) -> eyre::Result<Vec<ChunkBin>> {
        let part_provider = self.get_part_storage_provider(partition_id)?;
        part_provider.read_chunks(read_interval, expected_state)
    }

    /// get the partition storage provider associated with a partition_id
    pub fn get_part_storage_provider(
        &self,
        partition_id: PartitionId,
    ) -> eyre::Result<PartitionStorageProvider> {
        Ok(self
            .partition_providers
            .get(&partition_id)
            .ok_or(eyre!(
                "Can't find storage provider for partition {:?}",
                &partition_id
            ))?
            .clone())
    }

    /// write a vec of chunks to an interval in a partition
    pub fn write_chunks(
        &self,
        partition_id: PartitionId,
        write_interval: Interval<u32>,
        chunks: Vec<ChunkBin>,
        expected_state: Option<ChunkState>,
        new_state: IntervalState,
    ) -> eyre::Result<()> {
        let part_provider = self.get_part_storage_provider(partition_id)?;
        part_provider.write_chunks(chunks, write_interval, expected_state, new_state)
    }
}
