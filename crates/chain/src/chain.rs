use crate::partitions::{get_partitions_and_storage_providers, mine_partition};
use ::database::{config::get_data_dir, open_or_create_db};
use actix::{Actor, Addr, Arbiter, System};
use actors::{
    block_producer::BlockProducerActor,
    chunk_storage::ChunkStorageActor,
    mempool::{self, MempoolActor},
    mining::PartitionMiningActor,
    packing::PackingActor,
    ActorAddresses,
};
use clap::Parser;
use irys_api_server::run_server;
pub use irys_reth_node_bridge::{
    chainspec,
    node::{RethNode, RethNodeAddOns, RethNodeExitHandle, RethNodeProvider},
    IrysChainSpecBuilder,
};
use irys_storage::{partition_provider::PartitionStorageProvider, StorageProvider};
use irys_types::{
    app_state::{AppState, DatabaseProvider},
    block_production::{Partition, PartitionId},
    H256,
};
use reth::{
    builder::{FullNode, NodeHandle},
    chainspec::ChainSpec,
    core::{exit::NodeExitFuture, irys_ext::NodeExitReason},
    tasks::{TaskExecutor, TaskManager},
    CliContext,
};
use reth_cli_runner::{run_to_completion_or_panic, run_until_ctrl_c, AsyncCliRunner};
use reth_db::{database, DatabaseEnv};
use std::{
    collections::HashMap,
    fs::canonicalize,
    future::IntoFuture,
    path::{absolute, PathBuf},
    str::FromStr,
    sync::{mpsc, Arc},
    time::Duration,
};

use futures::FutureExt;
use tokio::{
    runtime::Handle,
    sync::oneshot::{self, Sender},
    time::sleep,
};

use crate::vdf::run_vdf;
use tracing::{debug, error, span, trace, Level};

pub struct IrysNodeConfig {
    pub sm_partition_config: Vec<(Partition, PartitionStorageProvider)>,
}

impl Default for IrysNodeConfig {
    fn default() -> Self {
        Self {
            sm_partition_config: get_partitions_and_storage_providers().unwrap(),
        }
    }
}

pub async fn start_for_testing(config: IrysNodeConfig) -> eyre::Result<IrysNodeCtx> {
    start_irys_node(config).await
}

#[derive(Debug, Clone)]
pub struct IrysNodeCtx {
    pub reth_handle: RethNodeProvider,
    pub storage_provider: StorageProvider,
    pub actor_addresses: ActorAddresses,
    pub db: DatabaseProvider,
}

pub async fn start_irys_node(
    node_config: IrysNodeConfig,
) -> eyre::Result<IrysNodeCtx> {
    let (actor_addr_channel_sender, actor_addr_channel_receiver) =
        oneshot::channel::<ActorAddresses>();

    let (reth_handle_sender, reth_handle_receiver) =
        oneshot::channel::<FullNode<RethNode, RethNodeAddOns>>();
    let (irys_node_handle_sender, irys_node_handle_receiver) = oneshot::channel::<IrysNodeCtx>();
    // Spawn thread and runtime for actors
    std::thread::spawn(move || {
        let rt = actix_rt::Runtime::new().unwrap();
        rt.block_on(async move {
            // the RethNodeHandle doesn't *need* to be Arc, but it will reduce the copy cost
            let reth_node = RethNodeProvider(Arc::new(reth_handle_receiver.await.unwrap()));
            let db = DatabaseProvider(reth_node.provider.database.db.clone());

            let mempool_actor = MempoolActor::new(db.clone());
            let mempool_actor_addr = mempool_actor.start();

            let mut part_actors = Vec::new();

            let block_producer_actor =
                BlockProducerActor::new(db.clone(), mempool_actor_addr.clone(), reth_node.clone());
            let block_producer_addr = block_producer_actor.start();

            let mut partition_storage_providers =
                HashMap::<PartitionId, PartitionStorageProvider>::new();

            for (part, storage_provider) in node_config.sm_partition_config {
                partition_storage_providers.insert(part.id, storage_provider.clone());

                let partition_mining_actor =
                    PartitionMiningActor::new(part, block_producer_addr.clone(), storage_provider);
                part_actors.push(partition_mining_actor.start());
            }

            let storage_provider = StorageProvider::new(Some(partition_storage_providers));

            let (new_seed_tx, new_seed_rx) = mpsc::channel::<H256>();

            let part_actors_clone = part_actors.clone();
            std::thread::spawn(move || run_vdf(H256::random(), new_seed_rx, part_actors));

            let packing_actor_addr =
                PackingActor::new(Handle::current(), storage_provider.clone()).start();

            let actor_addresses = ActorAddresses {
                partitions: part_actors_clone,
                block_producer: block_producer_addr,
                packing: packing_actor_addr,
                mempool: mempool_actor_addr,
            };

            let _ = actor_addr_channel_sender.send(actor_addresses.clone());

            let _ = irys_node_handle_sender.send(IrysNodeCtx {
                storage_provider,
                actor_addresses: actor_addresses.clone(),
                reth_handle: reth_node,
                db: db.clone(),
            });

            run_server(actor_addresses).await;
        });
    });

    let builder = IrysChainSpecBuilder::mainnet();
    let reth_chainspec = builder.reth_builder.build();

    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(move || {
            let tokio_runtime = /* Handle::current(); */ tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();
            let mut task_manager = TaskManager::new(tokio_runtime.handle().clone());
            let exec: reth::tasks::TaskExecutor = task_manager.executor();

            tokio_runtime.block_on(run_to_completion_or_panic(
                &mut task_manager,
                run_until_ctrl_c(start_reth_node(exec, reth_chainspec, reth_handle_sender)),
            )).unwrap();

           
        })?;

    // send over the reth handle, wait for the full handle
    return Ok(irys_node_handle_receiver.await?);
}

async fn start_reth_node(
    exec: TaskExecutor,
    chainspec: ChainSpec,
    sender: oneshot::Sender<FullNode<RethNode, RethNodeAddOns>>,
) -> eyre::Result<NodeExitReason> {

    let pb = absolute(PathBuf::from_str("./.reth").unwrap()).unwrap();
    let node_handle = irys_reth_node_bridge::run_node(Arc::new(chainspec), exec, pb).await?;
    let r = sender
        .send(node_handle.node.clone())
        .expect("unable to send reth node handle");
    let exit_reason = node_handle.node_exit_future.await?;
    Ok(exit_reason)
}
