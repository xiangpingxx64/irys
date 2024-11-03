use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct StorageModuleConfig {
    path: PathBuf,
    size: u128,
}
