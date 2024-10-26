use actix::{Actor, Context, Handler, Message};
use database::db_cache::CachedDataRoot;
use irys_types::{
    chunk::Chunk, hash_sha256, validate_path, IrysTransactionHeader, Node, Proof, CHUNK_SIZE, H256,
};
use reth_db::DatabaseEnv;
use std::{path, sync::Arc};

/// The Mempool oversees pending transactions and validation of incoming tx.
#[derive(Debug)]
pub struct MempoolActor {
    db: Arc<DatabaseEnv>,
    confirmed_tx: Vec<IrysTransactionHeader>,
    invalid_tx: Vec<H256>,
}

impl Actor for MempoolActor {
    type Context = Context<Self>;
}

impl MempoolActor {
    /// Create a new instance of the mempool actor passing in a reference
    /// counted reference to a DatabaseEnv
    pub fn new(db: Arc<DatabaseEnv>) -> Self {
        Self {
            db,
            confirmed_tx: Vec::new(),
            invalid_tx: Vec::new(),
        }
    }
}

/// Message for when a new TX is discovered by the node, either though
/// synchronization with peers, or by a user posting the tx.
#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct TxIngressMessage(pub IrysTransactionHeader);

impl TxIngressMessage {
    fn into_inner(self) -> IrysTransactionHeader {
        self.0
    }
}

/// Message for when a new chunk is discovered by the node, either though
/// synchronization with peers, or by a user posting the chunk.
#[derive(Message, Debug)]
#[rtype(result = "()")]
pub struct ChunkIngressMessage(pub Chunk);

impl ChunkIngressMessage {
    fn into_inner(self) -> Chunk {
        self.0
    }
}

impl Handler<TxIngressMessage> for MempoolActor {
    type Result = ();

    fn handle(&mut self, tx_msg: TxIngressMessage, _ctx: &mut Context<Self>) -> Self::Result {
        let tx = &tx_msg.0;

        // Early out if we already know about this transaction
        if self.invalid_tx.contains(&tx.id) || self.confirmed_tx.contains(&tx) {
            return;
        }

        // Validate the transaction signature
        if tx.is_signature_valid() {
            self.confirmed_tx.push(tx.clone());
        } else {
            self.invalid_tx.push(tx.id);
        }

        // TODO: Check if the signer has funds to post the tx

        // Cache the data_root in the database
        let _ = database::cache_data_root(&self.db, &tx);
    }
}

impl Handler<ChunkIngressMessage> for MempoolActor {
    type Result = ();

    fn handle(&mut self, chunk_msg: ChunkIngressMessage, _ctx: &mut Context<Self>) -> Self::Result {
        let chunk = chunk_msg.0;
        // Check to see if we have a cached data_root for this chunk
        let result = database::cached_data_root_by_data_root(&self.db, chunk.data_root);

        // Early out if the data_root wasn't already added to the cache by a
        // confirmed transaction.
        if let Err(_) = result {
            return ();
        }

        // Next validate the data_path/proof for the chunk, linking
        // data_root->chunk_hash
        let root_hash = chunk.data_root.0;
        let target_offset = chunk.offset as u128;
        let path_buff = &chunk.data_path;

        let path_result = match validate_path(root_hash, path_buff, target_offset) {
            Ok(result) => result,
            Err(_) => {
                // TODO: Log about invalid chunk proof
                return;
            }
        };

        // Check that the leaf hash on the data_path matches the chunk_hash
        if path_result.leaf_hash == hash_sha256(&chunk.bytes.0).unwrap() {
            // Finally write the chunk to CachedChunks
            let _ = database::cache_chunk(&self.db, chunk);
        }
    }
}
