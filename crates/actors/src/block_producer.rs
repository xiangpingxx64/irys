use std::{
    str::FromStr, sync::{atomic::AtomicU64, Arc, Mutex, RwLock}, time::{SystemTime, UNIX_EPOCH}
};

use actix::{Actor, Addr, AsyncContext, Context, Handler, Message, ResponseFuture};
use alloy_rpc_types_engine::{ExecutionPayloadEnvelopeV1Irys, PayloadAttributes};
use irys_primitives::{DataShadow, IrysTxId, ShadowTx, ShadowTxType, Shadows};
// use irys_primitives::PayloadAttributes;
use irys_reth_node_bridge::{adapter::node::RethNodeContext, node::RethNodeProvider};
use irys_types::{
    app_state::DatabaseProvider, block_production::SolutionContext, Address, Base64, H256List, IrysBlockHeader, IrysSignature, IrysTransactionHeader, PoaData, Signature, TransactionLedger, H256, U256
};
use reth::{payload::EthPayloadBuilderAttributes, primitives::SealedBlock, revm::primitives::B256};
use reth_db::DatabaseEnv;

use crate::{
    block_index::{BlockIndexActor, GetBlockHeightMessage},
    mempool::{GetBestMempoolTxs, MempoolActor, RemoveConfirmedTxs},
};

pub struct BlockProducerActor {
    pub db: DatabaseProvider,
    pub mempool_addr: Addr<MempoolActor>,
    pub block_index_addr: Addr<BlockIndexActor>,
    pub last_height: Arc<RwLock<u64>>,
    pub reth_provider: RethNodeProvider,
}

impl BlockProducerActor {
    pub fn new(
        db: DatabaseProvider,
        mempool_addr: Addr<MempoolActor>,
        block_index_addr: Addr<BlockIndexActor>,
        reth_provider: RethNodeProvider,
    ) -> Self {
        Self {
            last_height: Arc::new(RwLock::new(get_latest_height_from_db(&db))),
            db,
            mempool_addr,
            block_index_addr,
            reth_provider,
        }
    }
}

// TODO: Implement real query
fn get_latest_height_from_db(db: &DatabaseEnv) -> u64 {
    0
}

impl Actor for BlockProducerActor {
    type Context = Context<Self>;
}

impl Handler<SolutionContext> for BlockProducerActor {
    type Result = ResponseFuture<Option<(Arc<IrysBlockHeader>, ExecutionPayloadEnvelopeV1Irys)>>;

    fn handle(&mut self, msg: SolutionContext, ctx: &mut Self::Context) -> Self::Result {
        dbg!("BlockProducerActor solution received {}", &msg);

        let mempool_addr = self.mempool_addr.clone();
        let block_index_addr = self.block_index_addr.clone();
        let current_height = *self.last_height.read().unwrap();
        let arc_rwlock = self.last_height.clone();
        let reth = self.reth_provider.clone();
        let db = self.db.clone();
        let self_addr = ctx.address();

        Box::pin(async move {
            // Acquire lock and check that the height hasn't changed identifying a race condition

            // TEMP: This demonstrates how to get the block height from the block_index_actor
            let bh = block_index_addr
                .send(GetBlockHeightMessage {})
                .await
                .unwrap();
            println!("block_height: {:?}", bh);

            let mut write_current_height = arc_rwlock.write().unwrap();
            if current_height != *write_current_height {
                return None;
            };

            let data_txs: Vec<IrysTransactionHeader> =
                mempool_addr.send(GetBestMempoolTxs).await.unwrap();

            let data_tx_ids = data_txs.iter().map(|h| h.id.clone()).collect::<Vec<H256>>();

            let mut irys_block = IrysBlockHeader {
                diff: U256::from(1000),
                cumulative_diff: U256::from(5000),
                last_retarget: 1622543200,
                solution_hash: H256::zero(),
                previous_solution_hash: H256::zero(),
                last_epoch_hash: H256::random(),
                chunk_hash: H256::zero(),
                height: current_height + 1,
                block_hash: H256::zero(),
                previous_block_hash: H256::zero(),
                previous_cumulative_diff: U256::from(4000),
                poa: PoaData {
                    tx_path: Base64::from_str("").unwrap(),
                    data_path: Base64::from_str("").unwrap(),
                    chunk: Base64::from_str("").unwrap(),
                },
                reward_address: Address::ZERO,
                reward_key: Base64::from_str("").unwrap(),
                signature: IrysSignature {
                    reth_signature: Signature::test_signature(),
                },
                timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64,
                ledgers: vec![
                    // Permanent Publish Ledger
                    TransactionLedger {
                        tx_root: H256::zero(),
                        txids: H256List(data_tx_ids.clone()),
                        ledger_size: U256::from(0),
                        expires: None,
                    },
                    // Term Submit Ledger
                    TransactionLedger {
                        tx_root: H256::zero(),
                        txids: H256List::new(),
                        ledger_size: U256::from(0),
                        expires: Some(1622543200),
                    },
                ],
                evm_block_hash: B256::ZERO,
            };

            // RethNodeContext is a type-aware wrapper that lets us interact with the reth node
            let context = RethNodeContext::new(reth.into()).await.unwrap();


            let shadows = Shadows::new(
                data_txs
                    .iter()
                    .map(|header| ShadowTx {
                        tx_id: IrysTxId::from_slice(header.id.as_bytes()),
                        fee: irys_primitives::U256::from(header.term_fee + header.perm_fee.unwrap_or(0)),
                        address: header.signer,
                        tx: ShadowTxType::Data(DataShadow {
                            fee: irys_primitives::U256::from(header.term_fee + header.perm_fee.unwrap_or(0)),
                        }),
                    })
                    .collect(),
            );

            // create a new reth payload

            // generate payload attributes
            // TODO: we need previous block metadata to fill in parent & prev_randao
            let payload_attrs = PayloadAttributes {
                timestamp: irys_block.timestamp, // tie timestamp together
                prev_randao: B256::ZERO,
                suggested_fee_recipient: irys_block.reward_address,
                withdrawals: None,
                parent_beacon_block_root: None,
                shadows: Some(shadows),
            };

            let exec_payload = context
                .engine_api
                .build_payload_v1_irys(B256::ZERO, payload_attrs)
                .await
                .unwrap();

            // we can examine the execution status of generated shadow txs
            // let shadow_receipts = exec_payload.shadow_receipts;

            let v1_payload = exec_payload
                .clone()
                .execution_payload
                .payload_inner
                .payload_inner
                .payload_inner;
            // TODO @JesseTheRobot create a deref(?) trait so this isn't as bad
            let block_hash = v1_payload.block_hash;

            irys_block.evm_block_hash = block_hash;

            // TODO: Irys block header building logic

            /*

            // Calculate storage requirements for new transactions
            let mut total_chunks = 0;
            for data_tx in data_txs {
                // Convert data size to required number of 256KB chunks, rounding up
                let num_chunks = (data_tx.data_size + CHUNK_SIZE - 1) / CHUNK_SIZE;
                total_chunks += num_chunks;
            }

            // Calculate total bytes needed in submit ledger
            let bytes_added = total_chunks * CHUNK_SIZE;

            // Update submit ledger size with new data
            let prev_block = self.block_index.get_item(self.last_height).unwrap();
            let submit_ledger_size = prev_block.ledgers[Ledger::Submit as usize].ledger_size;
            let new_submit_ledger_size = submit_ledger_size + u128::from(bytes_added);

            */

            // TODO: Commit block

            let _ = mempool_addr
                .send(RemoveConfirmedTxs(data_tx_ids))
                .await
                .unwrap();

            // let final_block = IrysBlockHeader {
            //     block_hash: todo!(),
            //     diff: todo!(),
            //     cumulative_diff: todo!(),
            //     last_retarget: todo!(),
            //     solution_hash: todo!(),
            //     previous_solution_hash: todo!(),
            //     chunk_hash: todo!(),
            //     height: get_current_block_height() + 1,
            //     previous_block_hash: todo!(),
            //     previous_cumulative_diff: todo!(),
            //     poa: todo!(),
            //     reward_address: todo!(),
            //     reward_key: todo!(),
            //     signature: todo!(),
            //     timestamp: get_current_timestamp(),
            //     ledgers: todo!(),
            // };

            // TODO: Commit block to DB and send to networking layer

            // make the built evm payload canonical
            context
                .engine_api
                .update_forkchoice(v1_payload.parent_hash, v1_payload.block_hash)
                .await
                .unwrap();

            database::insert_block(&db, &irys_block).unwrap();

            let block = Arc::new(irys_block);
            let txs = Arc::new(data_txs);
            let block_confirm_message = BlockConfirmedMessage(Arc::clone(&block), Arc::clone(&txs));

            // We can clone messages because it only contains references to the data
            self_addr.do_send(block_confirm_message.clone());
            block_index_addr.do_send(block_confirm_message.clone());

            *write_current_height += 1;
            Some((block.clone(), exec_payload))
        })
    }
}

fn get_current_timestamp() -> u64 {
    let start = SystemTime::now();
    start.duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
}

fn get_current_block_height() -> u64 {
    0
}

#[derive(Message, Clone)]
#[rtype(result = "()")]
pub struct BlockConfirmedMessage(
    pub Arc<IrysBlockHeader>,
    pub Arc<Vec<IrysTransactionHeader>>,
);

impl Handler<BlockConfirmedMessage> for BlockProducerActor {
    type Result = ();
    fn handle(&mut self, msg: BlockConfirmedMessage, _ctx: &mut Context<Self>) -> Self::Result {
        // Access the block header through msg.0
        let block = &msg.0;
        let data_tx = &msg.1;

        // Do something with the block
        dbg!("Block height: {} num tx: {}", block.height, data_tx.len());

        // No return value needed since result type is ()
    }
}
