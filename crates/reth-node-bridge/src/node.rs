use core::fmt;
use std::{fs::File, future::Future, io::Write, path::PathBuf, sync::{mpsc::Sender, Arc}, time::Duration};

use api_server::run_server;
use irys_types::H256;
use reth::{chainspec::EthereumChainSpecParser, cli::{Cli, Commands}, core::irys_ext::{NodeExitReason, ReloadPayload}, prometheus_exporter::install_prometheus_recorder, version, CliContext, CliRunner};
use reth_chainspec::{EthChainSpec, EthereumHardforks};
use reth_cli::chainspec::ChainSpecParser;
use reth_cli_commands::{node::NoArgs, NodeCommand};
use reth_db::{init_db, DatabaseEnv};
use reth_node_builder::{NodeBuilder, NodeConfig, WithLaunchContext};
use tokio::time::sleep;
use tracing::{info};
use clap::Parser;
use reth_node_ethereum::{node::EthereumAddOns, EthereumNode};

use crate::rpc::{AccountStateExt, AccountStateExtApiServer};


// use crate::node_launcher::CustomNodeLauncher;

#[macro_export]
macro_rules! vec_of_strings {
    ($($x:expr),*) => (vec![$($x.to_string()),*]);
}

#[global_allocator]
static ALLOC: reth_cli_util::allocator::Allocator = reth_cli_util::allocator::new_allocator();

pub fn run_node(new_seed_channel: Sender<H256>) -> eyre::Result<()> {
    let mut os_args: Vec<String> = std::env::args().collect();
    let bp = os_args.remove(0);
    // let mut args = vec_of_strings![
    //     "node",
    //     "-vvvvvv",
    //     "--disable-discovery",
    //     "--http",
    //     "--http.api",
    //     "debug,rpc,reth,eth"
    // ];

    // let mut args = vec_of_strings![
    //     "node",
    //     "-vvvvv",
    //     "--disable-discovery",
    //     "--http",
    //     "--http.api",
    //     "debug,rpc,reth,eth",
    //     "--datadir",
    //     "/workspaces/irys/.reth",
    //     "--log.file.directory",
    //     "/workspaces/irys/.reth/logs",
    //     "--log.file.format",
    //     "json",
    //     "--log.stdout.format",
    //     "json",
    //     "--log.file.filter",
    //     "trace"
    // ];

    let mut args = match true /* <- true if running manually :) */ {
        true => vec_of_strings![
            "node",
            "-vvvvv",
            "--instance", 
            "1",
            "--disable-discovery",
            "--http",
            "--http.api",
            "debug,rpc,reth,eth",
            "--datadir",
            "./.reth",
            "--log.file.directory",
            "./.reth/logs",
            "--log.file.format",
            "json",
            "--log.stdout.format",
            "json",
            "--log.stdout.filter",
            "trace",
            "--log.file.filter",
            "trace"
        ],
        false => vec_of_strings![
            "node",
            "-vvvvvv",
            "--disable-discovery",
            "--http",
            "--http.api",
            "debug,rpc,reth,eth"
        ],
    };

    args.insert(0, bp.to_string());
    args.append(&mut os_args);
    // dbg!(&args);
    info!("Running with args: {:#?}", &args);
    // loop is flawed, retains too much global set-once state
    let cli = Cli::<EthereumChainSpecParser, NoArgs>::parse_from(args.clone());
    let _guard = cli.logs.init_tracing()?;

    loop {
        let runner = CliRunner::default();

        // this loop allows us to 'reload' the reth node with a new config very quickly, without having to actually restart the entire process
        // mainly used to provide and then restart with a new genesis block.
        // TODO: extract run_command_until_exit to re-use async context to prevent global thread pool error on reload

        let exit_reason = runner.run_command_until_exit(|ctx| run_custom_node(ctx, cli.clone(), 
        |builder, engine_args| async move {
            // if engine_args.experimental {
            //     warn!(target: "reth::cli", "Experimental engine is default now, and the --engine.experimental flag is deprecated. To enable the legacy functionality, use --engine.legacy.");
            // }

            // let use_legacy_engine = engine_args.legacy;
            // match use_legacy_engine {
            //     false => {
                    // let engine_tree_config = TreeConfig::default()
                    //     .with_persistence_threshold(engine_args.persistence_threshold)
                    //     .with_memory_block_buffer_target(engine_args.memory_block_buffer_target);
                    let handle = builder
                        .with_types::<EthereumNode>()
                        // .with_types_and_provider::<EthereumNode, BlockchainProvider2<_>>()
                        // .with_types_and_provider::<EthereumNode, BlockchainProvider2<_>>()
                        // .with_types_and_provider::<EthereumNode, BlockchainProvider<NodeTypesWithDBAdapter<EthereumNode, Arc<DatabaseEnv>>>>()
                        .with_components(EthereumNode::components())
                        .with_add_ons(EthereumAddOns::default())
                        .extend_rpc_modules(move |ctx| {
                            
                    //         let pool = ctx.pool().clone();
                    // let p = ctx.provider();
                    // let ext = TxpoolExt { pool };

                    // // now we merge our extension namespace into all configured transports
                    // ctx.modules.merge_configured(ext.into_rpc())?;
                            
                            
                            let provider = ctx.provider().clone();

                            let irys_ext = ctx.node().components.irys_ext.clone();
                            let network = ctx.network().clone();

                            ctx.node().task_executor.spawn(run_server());

                            let ext = AccountStateExt {
                                provider,
                                irys_ext,
                                network,
                            };
                            let rpc = ext.into_rpc();
                            ctx.modules.merge_configured(rpc)?;



                            println!("extension added!");
                            Ok(())
                        })
                        // .launch_with_fn(|builder| {
                        //     let launcher = EngineNodeLauncher::new(
                        //         builder.task_executor().clone(),
                        //         builder.config().datadir(),
                        //         engine_tree_config,
                        //     );
                        //     builder.launch_with(launcher)
                        // })
                        .launch()
                        .await?;
                    
                    let exit_reason = handle.node_exit_future.await?;
                // }
            //     true => {
            //         info!(target: "reth::cli", "Running with legacy engine");
            //         let handle = builder.launch_node(EthereumNode::default()).await?;
            //         handle.node_exit_future.await
            //     }
            // }

                    if true
                    /* launched.node.config.dev.dev */
                    {
                        match exit_reason.clone() {
                            NodeExitReason::Normal => (),
                            NodeExitReason::Reload(payload) => match payload {
                                ReloadPayload::ReloadConfig(chain_spec) => {
                                    // delay here so the genesis submission RPC reponse is able to make it back before the server dies
                                
                                    let ser = serde_json::to_string_pretty(&chain_spec)?;
                                    let pb =
                                        PathBuf::from(handle.node.data_dir.data_dir().join("dev_genesis.json"));
                                    // remove_file(&pb)?;
                                    let mut f = File::create(&pb)?;
                                    f.write_all(ser.as_bytes())?;
                                    info!("Written dev_genesis.json");
                                    sleep(Duration::from_millis(500)).await;
                                }
                            },
                        }
                    }

                    Ok(NodeExitReason::Normal)
        }
     ))?;
    }
    Ok(())
}


async fn run_custom_node<Ext, C, L, Fut>(ctx: CliContext, cli: Cli<C, Ext>, launcher: L) -> eyre::Result<NodeExitReason>
where
    C: ChainSpecParser<ChainSpec: EthChainSpec + EthereumHardforks>,
    Ext: clap::Args + Clone + fmt::Debug,
    L: Fn(WithLaunchContext<NodeBuilder<Arc<DatabaseEnv>, C::ChainSpec>>, Ext) -> Fut,
    Fut: Future<Output = eyre::Result<NodeExitReason>>,
{
    // from reth/bin/reth/src/commands/node/mod.rs:137
    let matched_cmd = match cli.command {
        Commands::Node(command) => Some(command),
        _ => None,
    };

    let node_command = *matched_cmd.expect("unable to get cmd_cfg");

    tracing::info!(target: "reth::cli", version = ?version::SHORT_VERSION, "Starting reth");

        let NodeCommand {
            datadir,
            config,
            chain,
            metrics,
            instance,
            with_unused_ports,
            network,
            rpc,
            txpool,
            builder,
            debug,
            db,
            dev,
            pruning,
            ext,
        } = node_command;


        // let chain_spec =
        // get_chain_spec_with_path(vec![], &datadir.datadir.unwrap_or_default().as_ref(), dev.dev);

        // set up node config
        let mut node_config = NodeConfig
         {
            datadir,
            config,
            // chain: Arc::new(chain_spec),
            chain,
            metrics,
            instance,
            network,
            rpc,
            txpool,
            builder,
            debug,
            db,
            dev,
            pruning,
        };

        // Register the prometheus recorder before creating the database,
        // because database init needs it to register metrics.
        let _ = install_prometheus_recorder();
        
        let data_dir = node_config.datadir();
        let db_path = data_dir.db();

        tracing::info!(target: "reth::cli", path = ?db_path, "Opening database");
        let database = Arc::new(init_db(db_path.clone(), db.database_args())?.with_metrics());


        // node_config = node_config.with_chain(chain_spec);
        if with_unused_ports {
            node_config = node_config.with_unused_ports();
        }

        let builder = NodeBuilder::new(node_config)
            .with_database(database)
            .with_launch_context(ctx.task_executor);

        launcher(builder, ext).await?;
        Ok(NodeExitReason::Normal)


}

