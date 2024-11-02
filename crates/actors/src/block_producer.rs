use std::{sync::Arc, time::{SystemTime, UNIX_EPOCH}};

use actix::{Actor, Addr, Context, Handler, ResponseFuture};
use irys_types::{block_production::SolutionContext, IrysBlockHeader};
use reth_db::DatabaseEnv;

use crate::mempool::{GetBestMempoolTxs, MempoolActor};

pub struct BlockProducerActor {
    pub db: Arc<DatabaseEnv>,
    pub mempool_addr: Addr<MempoolActor>
}

impl Actor for BlockProducerActor {
    type Context = Context<Self>;
}

impl Handler<SolutionContext> for BlockProducerActor {
    type Result = ResponseFuture<()>;

    fn handle(&mut self, msg: SolutionContext, ctx: &mut Self::Context) -> Self::Result {
        let mempool_addr = self.mempool_addr.clone();
        Box::pin(async move {
            let r = mempool_addr.send(GetBestMempoolTxs).await.unwrap();

            let final_block = IrysBlockHeader {
                block_hash: todo!(),
                diff: todo!(),
                cumulative_diff: todo!(),
                last_retarget: todo!(),
                solution_hash: todo!(),
                previous_solution_hash: todo!(),
                chunk_hash: todo!(),
                height: todo!(),
                previous_block_hash: todo!(),
                previous_cumulative_diff: todo!(),
                poa: todo!(),
                reward_address: todo!(),
                reward_key: todo!(),
                signature: todo!(),
                timestamp: get_current_timestamp(),
                ledgers: todo!(),
            };

            // TODO: Commit block to DB and send to networking layer


            ()
        })
    }
}

fn get_current_timestamp() -> u64 {
    let start = SystemTime::now();
    start
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}