mod irys;
mod partitions;
mod vdf;

use actix::{Actor, Addr, Arbiter, System};
use actors::{
    block_producer::BlockProducerActor, mining::PartitionMiningActor, packing::PackingActor,
};
use clap::Parser;
use irys_reth_node_bridge::{chainspec, IrysChainSpecBuilder};
use irys_types::{app_state::AppState, H256};
use partitions::{get_partitions, mine_partition};
use reth::{chainspec::ChainSpec, core::irys_ext::NodeExitReason, CliContext};
use reth_cli_runner::{run_to_completion_or_panic, run_until_ctrl_c, AsyncCliRunner};
use reth_db::database;
use std::{
    str::FromStr,
    sync::{mpsc, Arc},
    time::Duration,
};
use tokio::{runtime::Handle, sync::oneshot};

use vdf::run_vdf;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Name of the person to greet
    #[arg(short, long, default_value = "./database")]
    database: String,
}

use tracing::{debug, error, trace};

struct ActorAddresses {
    partitions: Vec<Addr<PartitionMiningActor>>,
    block_producer: Addr<BlockProducerActor>,
    packing: Addr<PackingActor>,
}

fn main() -> eyre::Result<()> {
    let (actor_addr_channel_sender, actor_addr_channel_receiver) =
        oneshot::channel::<ActorAddresses>();
    // Spawn thread and runtime for actors
    std::thread::spawn(move || {
        let rt = actix_rt::Runtime::new().unwrap();
        rt.block_on(async move {
            let block_producer_actor = BlockProducerActor {};

            let block_producer_addr = block_producer_actor.start();

            let mut part_actors = Vec::new();

            for part in get_partitions() {
                let partition_mining_actor =
                    PartitionMiningActor::new(part, block_producer_addr.clone());
                part_actors.push(partition_mining_actor.start());
            }

            let (new_seed_tx, new_seed_rx) = mpsc::channel::<H256>();

            let part_actors_clone = part_actors.clone();
            std::thread::spawn(move || run_vdf(H256::random(), new_seed_rx, part_actors));

            let packing_actor_addr = PackingActor::new(Handle::current()).start();
            let _ = actor_addr_channel_sender.send(ActorAddresses {
                partitions: part_actors_clone,
                block_producer: block_producer_addr,
                packing: packing_actor_addr,
            });
        });
    });

    let actor_addresses = actor_addr_channel_receiver.blocking_recv().unwrap();

    let builder = IrysChainSpecBuilder::mainnet();
    let reth_chainspec = builder.reth_builder.build();

    // TODO @JesseTheRobot - make sure logging is initialized before we get here as this uses logging macros
    // use the existing reth code to handle blocking & graceful shutdown
    let AsyncCliRunner {
        context,
        mut task_manager,
        tokio_runtime,
    } = AsyncCliRunner::new()?;
    // Executes the command until it finished or ctrl-c was fired
    let command_res = tokio_runtime.block_on(run_to_completion_or_panic(
        &mut task_manager,
        run_until_ctrl_c(main2(context, reth_chainspec)),
    ));

    if command_res.is_err() {
        error!(target: "reth::cli", "shutting down due to error");
    } else {
        debug!(target: "reth::cli", "shutting down gracefully");
        // after the command has finished or exit signal was received we shutdown the task
        // manager which fires the shutdown signal to all tasks spawned via the task
        // executor and awaiting on tasks spawned with graceful shutdown
        task_manager.graceful_shutdown_with_timeout(Duration::from_secs(5));
    }

    // `drop(tokio_runtime)` would block the current thread until its pools
    // (including blocking pool) are shutdown. Since we want to exit as soon as possible, drop
    // it on a separate thread and wait for up to 5 seconds for this operation to
    // complete.
    let (tx, rx) = mpsc::channel();
    std::thread::Builder::new()
        .name("tokio-runtime-shutdown".to_string())
        .spawn(move || {
            drop(tokio_runtime);
            let _ = tx.send(());
        })
        .unwrap();

    let _ = rx.recv_timeout(Duration::from_secs(5)).inspect_err(|err| {
        debug!(target: "reth::cli", %err, "tokio runtime shutdown timed out");
    });

    // command_res
    Ok(())
}

async fn main2(ctx: CliContext, chainspec: ChainSpec) -> eyre::Result<NodeExitReason> {
    let args = Args::parse();

    // let db_path = get_data_dir();

    // let db = open_or_create_db(&args.database)?;

    let handle = Handle::current();

    // let (actor_addr_channel_sender, actor_addr_channel_receiver) = oneshot::channel();

    let app_state: AppState = AppState {};

    let global_app_state = Arc::new(app_state);

    let node_handle = irys_reth_node_bridge::run_node(
        std::sync::mpsc::channel().0,
        Arc::new(chainspec),
        ctx.task_executor,
    )
    .await?;

    // TODO @JesseTheRobot - make this dump genesis (or keep it internal to reth?)
    let exit_reason = node_handle.node_exit_future.await?;
    Ok(exit_reason)
}
