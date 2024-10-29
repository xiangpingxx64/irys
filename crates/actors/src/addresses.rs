use actix::Addr;

use crate::{
    block_producer::BlockProducerActor, mempool::MempoolActor, mining::PartitionMiningActor,
    packing::PackingActor,
};

#[derive(Clone)]
pub struct ActorAddresses {
    pub partitions: Vec<Addr<PartitionMiningActor>>,
    pub block_producer: Addr<BlockProducerActor>,
    pub packing: Addr<PackingActor>,
    // pub mempool: Addr<MempoolActor>,
}
