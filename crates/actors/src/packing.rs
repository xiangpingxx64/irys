use std::{
    collections::VecDeque,
    fs,
    io::{Seek, Write},
    os::unix::fs::FileExt,
    sync::{Arc, RwLock},
};

use actix::{Actor, Context, Handler, Message};
use irys_primitives::IrysTxId;
use irys_storage::StorageProvider;
use irys_types::{Address, ChunkState, Interval, U256};
use packing::capacity_pack_range_with_data;
use tokio::runtime::Handle;

#[derive(Message, Clone)]
#[rtype("()")]
struct PackingRequestRange {
    pub partition_id: IrysTxId,
    pub chunk_interval: Interval<u32>
}

type AtomicChunkRange = Arc<RwLock<VecDeque<PackingRequestRange>>>;

pub struct PackingActor {
    storage_provider: StorageProvider,
    runtime_handle: Handle,
    chunks: AtomicChunkRange,
}

const CHUNK_POLL_TIME_MS: u64 = 1_000;

impl PackingActor {
    pub fn new(handle: Handle, storage_provider: StorageProvider) -> Self {
        Self {
            runtime_handle: handle,
            storage_provider,
            chunks: Arc::new(RwLock::new(VecDeque::with_capacity(1000))),
        }
    }

    async fn poll_chunks(chunks: AtomicChunkRange, storage_provider: &StorageProvider) {
        // Loop which runs all jobs every 1 second (defined in CHUNK_POLL_TIME_MS)
        loop {
            if let Some(next_range) = chunks.read().unwrap().front() {
                let PartitionInfo {
                    filename,
                    mining_addr,
                } = get_partition_info(next_range.partition_id);

                let f = fs::File::open(filename.clone()).unwrap();

                let mut data_in_range = match fs::read(filename) {
                    Ok(r) => r,
                    Err(_) => continue,
                };

                // TODO: Pack range
                let range = match capacity_pack_range_with_data(
                    data_in_range,
                    mining_addr,
                    next_range.chunk_interval.start() as u64,
                    next_range.partition_id,
                    None,
                ) {
                    Ok(r) => r,
                    Err(_) => continue,
                };

                // TODO: Write to disk correctly
                let _ = storage_provider.write_chunks(next_range.partition_id, next_range.chunk_interval, range, None, ChunkState::Packed).unwrap();

                // Remove from queue once complete
                let _ = chunks.write().unwrap().pop_front();
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(CHUNK_POLL_TIME_MS)).await;
        }
    }
}

struct PartitionInfo {
    mining_addr: Address,
    filename: String,
}

fn get_partition_info(id: IrysTxId) -> PartitionInfo {
    PartitionInfo {
        mining_addr: Address::random(),
        filename: "".to_string(),
    }
}

impl Actor for PackingActor {
    type Context = Context<Self>;

    fn start(self) -> actix::Addr<Self> {
        // Create packing worker that runs every
        self.runtime_handle
            .spawn(Self::poll_chunks(self.chunks.clone(), &self.storage_provider));

        Context::new().run(self)
    }

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.set_mailbox_capacity(1000000);
    }
}

impl Handler<PackingRequestRange> for PackingActor {
    type Result = ();

    fn handle(&mut self, msg: PackingRequestRange, ctx: &mut Self::Context) -> Self::Result {
        self.chunks.write().unwrap().push_back(msg);
    }
}
