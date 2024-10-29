use arbitrary::Arbitrary;
use irys_types::{Base64, Compact, H256};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, Default, PartialEq, Serialize, Deserialize, Arbitrary, Compact)]
pub struct CachedDataRoot {
    /// Unlike a unix timestamp which stores the number of seconds since
    /// UNIX_EPOCH, this timestamp stores the number of milliseconds. Similar
    /// to javascript timestamps.
    pub timestamp: u128,

    /// Total size (in bytes) of the data represented by the data_root
    pub data_size: u64,

    /// The set of all tx.ids' that contain this data_root
    pub txid_set: Vec<H256>,
}

#[derive(Clone, Debug, Eq, Default, PartialEq, Serialize, Deserialize, Arbitrary, Compact)]
pub struct CachedChunk {
    pub chunk: Option<Base64>,
    pub data_path: Base64,
}
