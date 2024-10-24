use std::{collections::VecDeque, sync::{Arc, RwLock}};

use actix::{Actor, Context, Handler, Message};
use irys_types::U256;
use tokio::runtime::Handle;

#[derive(Message, Clone, Copy)]
#[rtype("()")]
struct PackingRequestRange((U256, U256));

type WrappedChunkRange = Arc<RwLock<VecDeque<PackingRequestRange>>>;

pub struct PackingActor {
    runtime_handle: Handle, 
    chunks: WrappedChunkRange,
}

const CHUNK_POLL_TIME_MS: u64 = 1_000;

impl PackingActor {
    pub fn new(handle: Handle) -> Self {
        Self {
            runtime_handle: handle,
            chunks: Arc::new(RwLock::new(VecDeque::with_capacity(1000)))
        }
    }

    // Gets next range in queue and derefs (copies) as it's a cheap operation
    fn get_next_range(&self) -> PackingRequestRange {
        *self.chunks.read().unwrap().front().unwrap()
    }

    fn remove_next_range(&mut self) {
        (*self.chunks).write().unwrap().pop_front();
    }

    async fn poll_chunks(chunks: WrappedChunkRange) {
        // Loop which runs all jobs every 1 second (defined in CHUNK_POLL_TIME_MS)
        loop {
            if let Some(next_range) = chunks.read().unwrap().front() {
                // TODO: Pack range 

                // Remove from queue once complete
                let _ = chunks.write().unwrap().pop_front();
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(CHUNK_POLL_TIME_MS)).await;
        }
    }
}

impl Actor for PackingActor {
    type Context = Context<Self>;

    fn start(self) -> actix::Addr<Self> {
        // Create packing worker that runs every
        self.runtime_handle.spawn(Self::poll_chunks(self.chunks.clone()));
        
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