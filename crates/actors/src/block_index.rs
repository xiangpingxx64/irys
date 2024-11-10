use std::sync::{Arc, RwLock};

use actix::prelude::*;
use database::{BlockIndex, BlockIndexItem, Initialized, Ledger, LedgerIndexItem};
use irys_types::{IrysBlockHeader, IrysTransactionHeader, CHUNK_SIZE, H256};

use crate::block_producer::BlockConfirmedMessage;

/// The Mempool oversees pending transactions and validation of incoming tx.
#[derive(Debug)]
pub struct BlockIndexActor {
    block_index: Arc<RwLock<BlockIndex<Initialized>>>,
}

impl Actor for BlockIndexActor {
    type Context = Context<Self>;
}

impl BlockIndexActor {
    /// Create a new instance of the mempool actor passing in a reference
    /// counted reference to a DatabaseEnv
    pub fn new(block_index: Arc<RwLock<BlockIndex<Initialized>>>) -> Self {
        Self { block_index }
    }

    /// Adds a finalized block to the index
    fn add_finalized_block(
        &mut self,
        irys_block_header: &Arc<IrysBlockHeader>,
        data_txs: &Arc<Vec<IrysTransactionHeader>>,
    ) {
        // Calculate storage requirements for new transactions
        let mut total_chunks = 0;
        for data_tx in data_txs.iter() {
            // Convert data size to required number of 256KB chunks, rounding up
            let num_chunks = (data_tx.data_size + CHUNK_SIZE - 1) / CHUNK_SIZE;
            total_chunks += num_chunks;
        }

        // Calculate total bytes needed in submit ledger
        let bytes_added = total_chunks * CHUNK_SIZE;
        // Get read lock once and keep it for multiple operations
        let index = self.block_index.read().unwrap();
        // MAGIC: block index includes the genesis block (0th block, would make index size 1, so we minus 1)
        let last_height = (index.num_blocks() - 1) as usize;

        // Use same lock to get previous block
        let prev_block = index.get_item(last_height).unwrap();
        let submit_ledger_size = prev_block.ledgers[Ledger::Submit as usize].ledger_size;
        let new_submit_ledger_size = submit_ledger_size + u128::from(bytes_added);

        // Create a new BlockIndexItem
        let block_index_item = BlockIndexItem {
            block_hash: irys_block_header.block_hash,
            num_ledgers: 2,
            ledgers: vec![
                LedgerIndexItem {
                    ledger_size: prev_block.ledgers[0].ledger_size,
                    tx_root: H256::zero(), // Until we actually compute tx_roots
                },
                LedgerIndexItem {
                    ledger_size: new_submit_ledger_size,
                    tx_root: H256::zero(), // Until we actually compute tx_roots
                },
            ],
        };

        self.block_index
            .write()
            .unwrap()
            .push_item(&block_index_item);
    }
}

impl Handler<BlockConfirmedMessage> for BlockIndexActor {
    type Result = ();

    // TODO: We are treating confirmed blocks like finalized blocks here when we
    // can receive multiple confirmed blocks on different forks we'll have to
    // listen to a FinalizedBlockMessage
    fn handle(&mut self, msg: BlockConfirmedMessage, _ctx: &mut Context<Self>) -> Self::Result {
        // Access the block header through msg.0
        let irys_block_header = &msg.0;
        let data_txs = &msg.1;

        // Do something with the block
        println!("Block height: {}", irys_block_header.height);

        self.add_finalized_block(&irys_block_header, &data_txs);

        // No return value needed since result type is ()
    }
}

/// Returns the current block height in the index
#[derive(Message, Clone, Debug)]
#[rtype(result = "u64")]
pub struct GetBlockHeightMessage {}

impl Handler<GetBlockHeightMessage> for BlockIndexActor {
    type Result = u64;

    fn handle(&mut self, msg: GetBlockHeightMessage, ctx: &mut Self::Context) -> Self::Result {
        let _ = ctx;
        let _ = msg;
        self.block_index.read().unwrap().num_blocks()
    }
}
