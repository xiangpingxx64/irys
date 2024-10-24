use irys_types::{
    merkle::{generate_data_root, generate_leaves, resolve_proofs},
    Address, Base64, IrysTransaction, IrysTransactionHeader, H256,
};

use eyre::Result;
use rand::rngs::OsRng;

use k256::ecdsa::SigningKey;
use reth_primitives::revm_primitives::keccak256;

pub struct Irys {
    pub signer: SigningKey,
}

impl Irys {
    pub fn random_signer() -> Self {
        Irys {
            signer: k256::ecdsa::SigningKey::random(&mut OsRng),
        }
    }

    /// Creates a transaction from a data buffer, optional anchor hash for the
    /// transaction is supported. The txid will not be set until the transaction
    /// is signed with [sign_transaction]
    pub async fn create_transaction(
        &self,
        data: Vec<u8>,
        anchor: Option<H256>, //TODO!: more parameters as they are implemented
    ) -> Result<IrysTransaction> {
        let mut transaction = self.merklize(data)?;
        transaction.header.signer = Address::from_public_key(self.signer.verifying_key());

        // TODO: These should be calculated from some pricing params passed in
        // as a parameter
        transaction.header.perm_fee = Some(1);
        transaction.header.term_fee = 1;

        // Fetch and set last_tx if not provided (primarily for testing).
        let anchor = if let Some(anchor) = anchor {
            anchor
        } else {
            // TODO: Retrieve an acceptable block_hash anchor
            H256::default()
        };
        transaction.header.anchor = anchor;

        Ok(transaction)
    }

    /// signs and sets signature and id.
    pub fn sign_transaction(&self, mut transaction: IrysTransaction) -> Result<IrysTransaction> {
        // Encode the header data
        let mut encoded_rlp = Vec::new();
        transaction.header.encode_for_signing(&mut encoded_rlp);

        let prehash = keccak256(&encoded_rlp);
        let signature = self.signer.sign_prehash_recoverable(prehash.as_slice())?;

        transaction.header.signature.reth_signature = signature.into();
        let id: [u8; 32] = keccak256(signature.0.to_bytes()).into();
        transaction.header.id = H256::from(id);
        Ok(transaction)
    }

    /// Builds a merkle tree, with a root, including all the proofs for each
    /// chunk.
    fn merklize(&self, data: Vec<u8>) -> Result<IrysTransaction> {
        let mut chunks = generate_leaves(data.clone())?;
        let root = generate_data_root(chunks.clone())?;
        let data_root = H256(root.id.clone());
        let mut proofs = resolve_proofs(root, None)?;

        // Discard the last chunk & proof if it's zero length.
        let last_chunk = chunks.last().unwrap();
        if last_chunk.max_byte_range == last_chunk.min_byte_range {
            chunks.pop();
            proofs.pop();
        }

        Ok(IrysTransaction {
            header: IrysTransactionHeader {
                data_size: data.len() as u64,
                data_root,
                ..Default::default()
            },
            data: Base64(data),
            chunks,
            proofs,
            ..Default::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use irys_types::merkle::MAX_CHUNK_SIZE;
    use rand::Rng;

    use super::Irys;

    #[tokio::test]
    async fn create_and_sign_transaction() {
        // Create 2.5 chunks worth of data *  fill the data with random bytes
        let data_size = (MAX_CHUNK_SIZE as f64 * 2.5).round() as usize;
        let mut data_bytes = vec![0u8; data_size];
        rand::thread_rng().fill(&mut data_bytes[..]);

        // Create a new Irys API instance
        let irys = Irys::random_signer();

        // Create a transaction from the random bytes
        let mut tx = irys.create_transaction(data_bytes, None).await.unwrap();

        // Sign the transaction
        tx = irys.sign_transaction(tx).unwrap();

        assert_eq!(tx.chunks.len(), 3);

        for chunk in &tx.chunks {
            println!(
                "min: {}, max: {}",
                chunk.min_byte_range, chunk.max_byte_range
            );
        }

        // Make sure the size of the last chunk is just whatever is left over
        // after chunking the rest of the data at MAX_CHUNK_SIZE intervals.
        let last_chunk = tx.chunks.last().unwrap();
        assert_eq!(
            data_size % MAX_CHUNK_SIZE,
            last_chunk.max_byte_range - last_chunk.min_byte_range
        );
    }
}
