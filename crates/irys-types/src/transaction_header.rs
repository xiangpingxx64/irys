use reth_codecs::Compact;
use serde::{Deserialize, Serialize};

use alloy_primitives::Signature;

use crate::{IrysSignature, H256};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Compact)]
/// Stores deserialized fields from a JSON formatted Irys transaction header.
/// We include the Irys prefix to differentiate from EVM transactions.
pub struct IrysTransactionHeader {
    /// A SHA-256 hash of the transaction signature.
    pub id: H256,

    /// block_hash of a recent (last 50) blocks or the a recent transaction id
    /// from the signer. Multiple transactions can share the same anchor.
    pub anchor: H256,

    /// The ecdsa/secp256k1 public key of the transaction signer
    pub signer: H256, // TODO: use a serialize type appropriate to these keys

    /// The merkle root of the transactions data chunks
    pub data_root: H256,

    /// Size of the transaction data in bytes
    pub data_size: u64,

    /// Funds the storage of the transaction data during the storage term
    pub term_fee: u64,

    /// Funds the storage of the transaction for the next 200+ years
    pub perm_fee: Option<u64>,

    /// Destination ledger for the transaction, default is 0 - Permanent Ledger
    pub ledger_num: Option<u64>,

    /// Bundles are critical for how data items are indexed and settled, different
    /// bundle formats enable different levels of indexing and verification.
    pub bundle_format: u64,

    /// Indicating the type of transaction, pledge, data, schema, etc.
    pub tx_type: u64,

    /// Transaction signature bytes
    pub signature: IrysSignature,
}

impl Default for IrysTransactionHeader {
    fn default() -> Self {
        IrysTransactionHeader {
            id: H256::zero(),
            anchor: H256::zero(),
            signer: H256::zero(),
            data_root: H256::zero(),
            data_size: 0,
            term_fee: 0,
            perm_fee: None,
            ledger_num: None,
            bundle_format: 0,
            tx_type: 0,
            signature: IrysSignature {
                reth_signature: Signature::test_signature(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    // #[test]
    // fn test_irys_transaction_header_serde() {
    //     // Create a sample IrysTransactionHeader
    //     let original_header = IrysTransactionHeader {
    //         id: H256::from([0u8; 32]),
    //         anchor: H256::from([1u8; 32]),
    //         signer: H256::from([2u8; 32]),
    //         data_root: H256::from([3u8; 32]),
    //         data_size: 1024,
    //         term_fee: 100,
    //         perm_fee: Some(200),
    //         ledger_num: Some(1),
    //         bundle_format: 0,
    //         tx_type: 1,
    //         signature: IrysSignature {
    //             reth_signature: Signature::test_signature(),
    //         },
    //     };

    //     // Serialize the IrysTransactionHeader to JSON
    //     let serialized = serde_json::to_string(&original_header).expect("Failed to serialize");

    //     println!("\n{}", serialized);

    //     // Deserialize the JSON back to IrysTransactionHeader
    //     let deserialized: IrysTransactionHeader =
    //         serde_json::from_str(&serialized).expect("Failed to deserialize");

    //     // Ensure the deserialized struct matches the original
    //     assert_eq!(original_header, deserialized);
    // }
}
