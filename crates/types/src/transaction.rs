use crate::{address_base58_stringify, Address, Arbitrary, Compact, Signature};
use alloy_rlp::{Encodable, RlpDecodable, RlpEncodable};
use serde::{Deserialize, Serialize};

use crate::{
    merkle::{Node, Proof},
    Base64, IrysSignature, H256,
};

#[derive(
    Clone,
    Debug,
    Eq,
    Serialize,
    Deserialize,
    PartialEq,
    Arbitrary,
    Compact,
    RlpEncodable,
    RlpDecodable,
)]
#[rlp(trailing)]
/// Stores deserialized fields from a JSON formatted Irys transaction header.
/// We include the Irys prefix to differentiate from EVM transactions.
pub struct IrysTransactionHeader {
    /// A SHA-256 hash of the transaction signature.
    pub id: H256,

    /// block_hash of a recent (last 50) blocks or the a recent transaction id
    /// from the signer. Multiple transactions can share the same anchor.
    pub anchor: H256,

    /// The ecdsa/secp256k1 public key of the transaction signer
    #[serde(default, with = "address_base58_stringify")]
    pub signer: Address,

    /// The merkle root of the transactions data chunks
    pub data_root: H256,

    /// Size of the transaction data in bytes
    pub data_size: u64,

    /// Funds the storage of the transaction data during the storage term
    pub term_fee: u64,

    /// Bundles are critical for how data items are indexed and settled, different
    /// bundle formats enable different levels of indexing and verification.
    pub bundle_format: u64,

    /// Indicating the type of transaction, pledge, data, schema, etc.
    pub tx_type: u64,

    /// Transaction signature bytes
    pub signature: IrysSignature,

    /// Funds the storage of the transaction for the next 200+ years
    pub perm_fee: Option<u64>,

    /// Destination ledger for the transaction, default is 0 - Permanent Ledger
    pub ledger_num: Option<u64>,
}

impl IrysTransactionHeader {
    /// When signing a transaction it's required to form the prehash from the
    /// RPL encoded header fields __excluding__ the signature field and optional
    /// fields if they are Option::None
    pub fn encode_fields(&self, out: &mut dyn alloy_rlp::BufMut) {
        self.id.encode(out);
        self.anchor.encode(out);
        self.signer.encode(out);
        self.data_root.encode(out);
        self.data_size.encode(out);
        self.term_fee.encode(out);
        self.bundle_format.encode(out);
        self.tx_type.encode(out);

        // Encode the optional fields if they are provided
        if let Some(perm_fee) = self.perm_fee {
            perm_fee.encode(out);
        }

        if let Some(ledger_num) = self.ledger_num {
            ledger_num.encode(out);
        }
    }

    /// When RLP encoding a transaction for singing we include an extra byte
    /// for tx type as a way to simplify parsing/decoding of RLP encoded headers
    /// in the future.
    /// (EIP-1559 added this to ethereum tx encoding as a proof point of its
    /// usefulness)
    pub fn encode_for_signing(&self, out: &mut dyn alloy_rlp::BufMut) {
        out.put_u8(self.tx_type as u8);
        self.encode_fields(out)
    }
}

/// Wrapper for the underlying IrysTransactionHeader fields, this wrapper
/// contains the data/chunk/proof info that is necessary for clients to seed
/// a transactions data to the network.
#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq)]
pub struct IrysTransaction {
    pub header: IrysTransactionHeader,
    pub data: Base64,
    #[serde(skip)]
    pub chunks: Vec<Node>,
    #[serde(skip)]
    pub proofs: Vec<Proof>,
}

impl Default for IrysTransactionHeader {
    fn default() -> Self {
        IrysTransactionHeader {
            id: H256::zero(),
            anchor: H256::zero(),
            signer: Address::default(),
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

    #[test]
    fn test_irys_transaction_header_serde() {
        // Create a sample IrysTransactionHeader
        let original_header = IrysTransactionHeader {
            id: H256::from([0u8; 32]),
            anchor: H256::from([1u8; 32]),
            signer: Address::default(),
            data_root: H256::from([3u8; 32]),
            data_size: 1024,
            term_fee: 100,
            perm_fee: Some(200),
            ledger_num: Some(1),
            bundle_format: 0,
            tx_type: 1,
            signature: IrysSignature {
                reth_signature: Signature::test_signature(),
            },
        };

        // Serialize the IrysTransactionHeader to JSON
        let serialized = serde_json::to_string(&original_header).expect("Failed to serialize");

        //println!("\n{}\n", serialized);

        // Deserialize the JSON back to IrysTransactionHeader
        let deserialized: IrysTransactionHeader =
            serde_json::from_str(&serialized).expect("Failed to deserialize");

        // println!("\n original_header: {:?}\n", original_header);
        // println!("\n deserialized: {:?}\n", deserialized);

        // Ensure the deserialized struct matches the original
        assert_eq!(original_header, deserialized);
    }
}
