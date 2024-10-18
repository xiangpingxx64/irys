use crate::H256;

/// Traits to decode base64_url encoded hashes into their corresponding types
pub trait DecodeHash: Sized {
	fn from(base58_string: &str) -> Result<Self, String>;
	fn empty() -> Self;
}

impl DecodeHash for H256 {
	fn from(base58_string: &str) -> Result<Self, String> {
		// TODO: 
		base58::FromBase58::from_base58(base58_string)
				.map_err(|e| format!("Failed to decode from base58 {:?}", e))
				.map(|bytes| H256::from_slice(bytes.as_slice()))
	}

	fn empty() -> Self {
			H256::zero()
	}
}