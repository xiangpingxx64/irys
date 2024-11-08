//! Crate dedicated to the IrysNodeConfig to avoid depdendency cycles
use std::{
    path::{absolute, PathBuf},
    str::FromStr as _,
};

use irys_storage::{ii, partition_provider::PartitionStorageProvider};
use irys_types::{
    block_production::Partition, PartitionStorageProviderConfig, StorageModuleConfig, CHUNK_SIZE,
};

// TODO: convert this into a set of clap args
#[derive(Debug, Clone)]
pub struct IrysNodeConfig {
    /// Node ID/instance number: used for testing
    pub node_id: u32,
    /// configuration of partitions and their associated storage providers
    /// TODO: rework
    pub sm_partition_config: Vec<(Partition, PartitionStorageProvider)>,
    /// base data directory, i.e `./.tmp`
    /// should not be used directly, instead use the appropriate methods, i.e `instance_directory`
    pub base_directory: PathBuf,
}

impl Default for IrysNodeConfig {
    fn default() -> Self {
        let base_dir = absolute(PathBuf::from_str("./.tmp").unwrap()).unwrap();
        Self {
            node_id: 0,
            sm_partition_config: get_default_partitions_and_storage_providers(
                base_dir.join("storage_modules"),
            )
            .unwrap(),
            base_directory: base_dir,
        }
    }
}
impl IrysNodeConfig {
    pub fn instance_directory(&self) -> PathBuf {
        self.base_directory.join(self.node_id.to_string())
    }
    pub fn storage_module_dir(&self) -> PathBuf {
        self.instance_directory().join("storage_modules")
    }
    pub fn reth_data_dir(&self) -> PathBuf {
        self.instance_directory().join("reth")
    }
    pub fn reth_log_dir(&self) -> PathBuf {
        self.reth_data_dir().join("logs")
    }
}

pub fn get_default_partitions_and_storage_providers(
    storage_module_dir: PathBuf,
) -> eyre::Result<Vec<(Partition, PartitionStorageProvider)>> {
    Ok(vec![
        (
            Partition::default(),
            PartitionStorageProvider::from_config(PartitionStorageProviderConfig {
                sm_paths_offsets: vec![
                    (
                        ii(0, 3),
                        StorageModuleConfig {
                            directory_path: storage_module_dir.join("/p1/sm1"),
                            size_bytes: 10 * CHUNK_SIZE,
                        },
                    ),
                    (
                        ii(4, 10),
                        StorageModuleConfig {
                            directory_path: storage_module_dir.join("/p1/sm2"),
                            size_bytes: 10 * CHUNK_SIZE,
                        },
                    ),
                ],
            })?,
        ),
        (
            Partition::default(),
            PartitionStorageProvider::from_config(PartitionStorageProviderConfig {
                sm_paths_offsets: vec![
                    (
                        ii(0, 5),
                        StorageModuleConfig {
                            directory_path: storage_module_dir.join("/p2/sm1"),
                            size_bytes: 10 * CHUNK_SIZE,
                        },
                    ),
                    (
                        ii(6, 10),
                        StorageModuleConfig {
                            directory_path: storage_module_dir.join("/p2/sm2"),
                            size_bytes: 10 * CHUNK_SIZE,
                        },
                    ),
                ],
            })?,
        ),
    ])
}
