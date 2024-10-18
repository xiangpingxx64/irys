//! Contains a common set of types used across all of the `irys-chain` modules.
//!
//! This module implements a single location where these types are managed,
//! making them easy to reference and maintain.

use base58::ToBase58;
use eyre::Error;
use fixed_hash::construct_fixed_hash;
use serde::{
    de::{self, Error as _},
    Deserialize, Deserializer, Serialize, Serializer,
};
use std::{ops::Index, slice::SliceIndex, str::FromStr};
use uint::construct_uint;

pub mod block_header;
pub mod consensus;
pub mod serialization_types;

pub use block_header::*;
pub use serialization_types::*;

//==============================================================================
// U256 Type
//------------------------------------------------------------------------------
construct_uint! {
    /// 256-bit unsigned integer.
    pub struct U256(4);
}

//==============================================================================
// H256 Type
//------------------------------------------------------------------------------
construct_fixed_hash! {
    /// A 256-bit hash type (32 bytes)
    pub struct H256(32);
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn test_irys_block_header_serialization() {
        let mut txids = H256List::new();

        // Create a sample IrysBlockHeader object with mock data
        let header = IrysBlockHeader {
            diff: U256::from(1000),
            cumulative_diff: U256::from(5000),
            last_retarget: 1622543200,
            solution_hash: H256::zero(),
            previous_solution_hash: H256::zero(),
            chunk_hash: H256::zero(),
            height: 42,
            block_hash: H256::zero(),
            previous_block_hash: H256::zero(),
            previous_cumulative_diff: U256::from(4000),
            poa: PoaData {
                option: "default".to_string(),
                tx_path: Base64::from_str("").unwrap(),
                data_path: Base64::from_str("").unwrap(),
                chunk: Base64::from_str("").unwrap(),
            },
            reward_address: H256::zero(),
            reward_key: Base64::from_str("").unwrap(),
            signature: Base64::from_str("").unwrap(),
            timestamp: 1622543200,
            ledgers: vec![TransactionLedger {
                tx_root: H256::zero(),
                txids: txids,
                ledger_size: U256::from(100),
                expires: Some(1622543200),
            }],
        };

        // Serialize the header to a JSON string
        let serialized = serde_json::to_string(&header).unwrap();
        println!("{}", serialized);

        // Deserialize back to IrysBlockHeader struct
        let deserialized: IrysBlockHeader = serde_json::from_str(&serialized).unwrap();

        // Assert that the deserialized object is equal to the original
        assert_eq!(header, deserialized);
    }
}
