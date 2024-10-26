use serde::{Deserialize, Serialize};

use crate::{Base64, H256};

#[derive(Serialize, Deserialize, Debug, Default, PartialEq)]
pub struct Chunk {
    pub data_root: H256,
    pub data_size: u64,
    pub data_path: Base64,
    pub bytes: Base64,
    pub offset: usize,
}
