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

pub mod consensus;
pub mod decode;
use self::decode::DecodeHash;

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
/// Stores deserialized fields from a JSON formatted Irys block header.
pub struct IrysBlockHeader {
    /// Difficulty threshold used to produce the current block.
    pub diff: U256,

    /// The sum of the average number of hashes computed by the network to
    /// produce the past blocks including this one.
    pub cumulative_diff: U256,

    /// Unix timestamp of the last difficulty adjustment
    pub last_retarget: u64,

    /// The solution hash for the block
    pub solution_hash: H256,

    /// The solution hash of the previous block in the chain.
    pub previous_solution_hash: H256,

    /// `SHA-256` hash of the PoA chunk (unencoded) bytes.
    pub chunk_hash: H256,

    /// The block height.
    pub height: u64,

    /// The block identifier.
    pub block_hash: H256,

    // Previous block identifier.
    pub previous_block_hash: H256,
    pub previous_cumulative_diff: U256,

    /// The recall chunk proof
    pub poa: PoaData,

    /// Address of the miner claiming the block reward, also used in validation
    /// of the poa chunks as the packing key.
    pub reward_address: H256,

    /// {KeyType, PubKey} - the public key the block was signed with. The only
    // supported KeyType is currently {rsa, 65537}.
    pub reward_key: Base64,

    /// The block signature
    pub signature: Base64,

    /// timestamp of when the block was discovered/produced
    pub timestamp: u64,

    /// A list of transaction ledgers, one for each active data ledger
    /// Maintains the block->tx_root->data_root relationship for each block
    /// and ledger.
    pub ledgers: Vec<TransactionLedger>,
}

#[derive(Default, Clone, Debug, PartialEq, Serialize, Deserialize)]
/// Stores deserialized fields from a `poa` (Proof of Access) JSON
pub struct PoaData {
    pub option: String,
    pub tx_path: Base64,
    pub data_path: Base64,
    pub chunk: Base64,
}

#[derive(Default, Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TransactionLedger {
    pub tx_root: H256,
    /// List of transaction ids included in the block
    pub txids: H256List,
    pub ledger_size: U256,
    pub expires: Option<u64>,
}

/// Stores the `nonce_limiter_info` in the [`ArweaveBlockHeader`]
#[derive(Clone, Debug, Default, Deserialize)]
pub struct NonceLimiterInfo {
    /// The output of the latest step - the source of the entropy for the mining nonces.
    pub output: H256,
    /// The global sequence number of the nonce limiter step at which the block was found.
    pub global_step_number: u64,
    /// The hash of the latest block mined below the current reset line.
    pub seed: H256,
    /// The hash of the latest block mined below the future reset line.
    pub next_seed: H256,
    /// The output of the latest step of the previous block
    pub prev_output: H256,
    /// VDF_CHECKPOINT_COUNT_IN_STEP checkpoints from the most recent step in the nonce limiter process.
    pub last_step_checkpoints: H256List,
    /// A list of the output of each step of the nonce limiting process. Note: each step
    /// has VDF_CHECKPOINT_COUNT_IN_STEP checkpoints, the last of which is that step's output.
    /// This field would be more accurately named "steps" as checkpoints are between steps.
    pub checkpoints: H256List,
    /// The number of SHA2-256 iterations in a single VDF checkpoint. The protocol aims to keep the
    /// checkpoint calculation time to around 40ms by varying this parameter. Note: there are
    /// 25 checkpoints in a single VDF step - so the protocol aims to keep the step calculation at
    /// 1 second by varying this parameter.
    #[serde(default, with = "option_u64_stringify")]
    pub vdf_difficulty: Option<u64>,
    /// The VDF difficulty scheduled for to be applied after the next VDF reset line.
    #[serde(default, with = "option_u64_stringify")]
    pub next_vdf_difficulty: Option<u64>,
}

//==============================================================================
// Option<u64>
//------------------------------------------------------------------------------
/// where u64 is represented as a string in the json
mod option_u64_stringify {
    use serde::{self, Deserialize, Deserializer, Serializer};
    use serde_json::Value;

    #[allow(dead_code)]
    pub fn serialize<S>(value: &Option<u64>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(number) => serializer.serialize_str(&number.to_string()),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let opt_val: Option<Value> = Option::deserialize(deserializer)?;

        match opt_val {
            Some(Value::String(s)) => s.parse::<u64>().map(Some).map_err(serde::de::Error::custom),
            Some(_) => Err(serde::de::Error::custom("Invalid type")),
            None => Ok(None),
        }
    }
}

//==============================================================================
// Base64 Type
//------------------------------------------------------------------------------
/// A struct of [`Vec<u8>`] used for all `base64_url` encoded fields

#[derive(Default, Debug, Clone, PartialEq)]
pub struct Base64(pub Vec<u8>);

impl std::fmt::Display for Base64 {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let string = base64_url::encode(&self.0);
        write!(f, "{}", string)
    }
}

/// Converts a base64url encoded string to a Base64 struct.
impl FromStr for Base64 {
    type Err = base64_url::base64::DecodeError;
    fn from_str(str: &str) -> Result<Self, base64_url::base64::DecodeError> {
        let result = base64_url::decode(str)?;
        Ok(Self(result))
    }
}

impl Base64 {
    pub fn from_utf8_str(str: &str) -> Result<Self, Error> {
        Ok(Self(str.as_bytes().to_vec()))
    }
    pub fn to_utf8_string(&self) -> Result<String, Error> {
        Ok(String::from_utf8(self.0.clone())?)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn as_slice(&self) -> &[u8] {
        self.0.as_slice()
    }

    pub fn split_at(&self, mid: usize) -> (&[u8], &[u8]) {
        self.0.split_at(mid)
    }
}

impl Serialize for Base64 {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(&format!("{}", &self))
    }
}

impl<'de> Deserialize<'de> for Base64 {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Vis;
        impl serde::de::Visitor<'_> for Vis {
            type Value = Base64;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a base64 string")
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                base64_url::decode(v)
                    .map(Base64)
                    .map_err(|_| de::Error::custom("failed to decode base64 string"))
            }
        }
        deserializer.deserialize_str(Vis)
    }
}

//==============================================================================
// U256 Type
//------------------------------------------------------------------------------
construct_uint! {
    /// 256-bit unsigned integer.
    pub struct U256(4);
}

/// Implement Serialize for U256
impl Serialize for U256 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.to_string().as_str())
    }
}

/// Implement Deserialize for U256
impl<'de> Deserialize<'de> for U256 {
    fn deserialize<D>(deserializer: D) -> Result<U256, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = Deserialize::deserialize(deserializer)?;
        U256::from_dec_str(&s).map_err(serde::de::Error::custom)
    }
}

//==============================================================================
// H256 Type
//------------------------------------------------------------------------------
construct_fixed_hash! {
    /// A 256-bit hash type (32 bytes)
    pub struct H256(32);
}

impl H256 {
    pub fn to_vec(self) -> Vec<u8> {
        self.0.to_vec()
    }
}

// Implement Serialize for H256
impl Serialize for H256 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_bytes().to_base58().as_ref())
    }
}

// Implement Deserialize for H256
impl<'de> Deserialize<'de> for H256 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s: String = Deserialize::deserialize(deserializer)?;
        DecodeHash::from(&s).map_err(D::Error::custom)
    }
}

//==============================================================================
// H256List Type
//------------------------------------------------------------------------------
/// A struct of [`Vec<H256>`] used for lists of [`Base64`] encoded hashes
#[derive(Debug, Default, Clone, PartialEq)]
pub struct H256List(pub Vec<H256>);

impl H256List {
    // Constructor for an empty H256List
    pub fn new() -> Self {
        H256List(Vec::new())
    }

    // Constructor for an initialized H256List
    pub fn with_capacity(capacity: usize) -> Self {
        H256List(Vec::with_capacity(capacity))
    }

    pub fn push(&mut self, value: H256) {
        self.0.push(value)
    }

    pub fn reverse(&mut self) {
        self.0.reverse()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, H256> {
        self.0.iter()
    }

    pub fn get(&self, index: usize) -> Option<&<usize as SliceIndex<[H256]>>::Output> {
        self.0.get(index)
    }
}

impl Index<usize> for H256List {
    type Output = H256;

    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

impl PartialEq<Vec<H256>> for H256List {
    fn eq(&self, other: &Vec<H256>) -> bool {
        &self.0 == other
    }
}

impl PartialEq<H256List> for Vec<H256> {
    fn eq(&self, other: &H256List) -> bool {
        self == &other.0
    }
}

// Implement Serialize for H256 base64url encoded Array
impl Serialize for H256List {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Serialize self.0 (Vec<Base64>) directly
        self.0.serialize(serializer)
    }
}

// Implement Deserialize for H256 base64url encoded Array
impl<'de> Deserialize<'de> for H256List {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Deserialize a Vec<Base64> and then wrap it in Base64Array
        Vec::<H256>::deserialize(deserializer).map(H256List)
    }
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
