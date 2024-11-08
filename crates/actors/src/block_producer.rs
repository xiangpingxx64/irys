use std::{
    sync::{atomic::AtomicU64, Arc, Mutex, RwLock},
    time::{SystemTime, UNIX_EPOCH},
};

use actix::{Actor, Addr, Context, Handler, ResponseFuture};
use alloy_rpc_types_engine::PayloadAttributes;
// use irys_primitives::PayloadAttributes;
use irys_reth_node_bridge::{adapter::node::RethNodeContext, node::RethNodeProvider};
use irys_types::{
    app_state::DatabaseProvider, block_production::SolutionContext, Address, IrysBlockHeader,
};
use reth::{payload::EthPayloadBuilderAttributes, primitives::SealedBlock, revm::primitives::B256};
use reth_db::DatabaseEnv;

use crate::mempool::{GetBestMempoolTxs, MempoolActor};

pub struct BlockProducerActor {
    pub db: DatabaseProvider,
    pub mempool_addr: Addr<MempoolActor>,
    pub last_height: Arc<RwLock<u64>>,
    pub reth_provider: RethNodeProvider,
}

impl BlockProducerActor {
    pub fn new(
        db: DatabaseProvider,
        mempool_addr: Addr<MempoolActor>,
        reth_provider: RethNodeProvider,
    ) -> Self {
        Self {
            last_height: Arc::new(RwLock::new(get_latest_height_from_db(&db))),
            db,
            mempool_addr,
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
    type Result = ResponseFuture<()>;

    fn handle(&mut self, msg: SolutionContext, ctx: &mut Self::Context) -> Self::Result {
        let mempool_addr = self.mempool_addr.clone();
        let current_height = *self.last_height.read().unwrap();
        let arc_rwlock = self.last_height.clone();
        let reth = self.reth_provider.clone();

        Box::pin(async move {
            // Acquire lock and check that the height hasn't changed identifying a race condition
            let mut write_current_height = arc_rwlock.write().unwrap();
            if current_height != *write_current_height {
                return ();
            };

            let r = mempool_addr.send(GetBestMempoolTxs).await.unwrap();

            let mut irys_block = IrysBlockHeader::new();

            // RethNodeContext is a type-aware wrapper that lets us interact with the reth node
            let context = RethNodeContext::new(reth.0.as_ref().clone()).await.unwrap();

            // create a new reth payload

            // generate payload attributes
            // TODO: we need previous block metadata to fill in parent & prev_randao
            let payload_attrs = PayloadAttributes {
                timestamp: irys_block.timestamp, // tie timestamp together
                prev_randao: B256::ZERO,
                suggested_fee_recipient: irys_block.reward_address,
                withdrawals: None,
                parent_beacon_block_root: None,
                shadows: None,
            };

            let exec_payload = context
                .engine_api
                .build_payload_v1_irys(B256::ZERO, payload_attrs)
                .await
                .unwrap();

            // TODO @JesseTheRobot create a deref(?) trait so this isn't as bad
            let block_hash = exec_payload
                .execution_payload
                .payload_inner
                .payload_inner
                .payload_inner
                .block_hash;

            irys_block.evm_block_hash = block_hash;

            // TODO: Irys block header building logic

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

            *write_current_height += 1;
            ()
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
