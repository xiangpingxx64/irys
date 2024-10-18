use crate::{chain::get_chain_spec, node_launcher::CustomNodeLauncher};
use clap::Parser;
use jsonrpsee::{core::RpcResult, proc_macros::rpc};
use reth::api::FullNodeTypesAdapter;
use reth::builder::components::{Components, ComponentsBuilder};
use reth::builder::{
    NodeAdapter, NodeBuilder, NodeBuilderWithComponents, NodeHandle, WithLaunchContext,
};
use reth::tasks::TaskManager;
use reth::transaction_pool::blobstore::DiskFileBlobStore;
use reth::transaction_pool::{
    CoinbaseTipOrdering, EthPooledTransaction, EthTransactionValidator, Pool,
    TransactionValidationTaskExecutor,
};
use reth::{
    args::RpcServerArgs,
    builder::NodeConfig,
    cli::{Cli, Commands},
    commands::node::NodeCommand,
    CliContext, CliRunner,
};
use reth_cli_runner::AsyncCliRunner;
use reth_db::transaction::DbTxMut;
use reth_db::DatabaseEnv;
use reth_db::{database::Database, tables};
use reth_db::{init_db, transaction::DbTx};
use reth_node_ethereum::node::{
    EthereumExecutorBuilder, EthereumNetworkBuilder, EthereumPayloadBuilder, EthereumPoolBuilder,
};
use reth_node_ethereum::{EthEvmConfig, EthExecutorProvider, EthereumNode};
use reth_primitives::{Address, U256};
use reth_provider::providers::BlockchainProvider;
use std::fmt;
use std::sync::Arc;
use tokio::runtime::Handle;
// We use jemalloc for performance reasons.
#[cfg(all(feature = "jemalloc", unix))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg_attr(not(test), rpc(server, namespace = "accountStateExt"))]
#[cfg_attr(test, rpc(server, client, namespace = "accountStateExt"))]
pub trait AccountStateExtApi {
    #[method(name = "updateBasicAccount")]
    fn update_basic_account(&self, address: Address, new_balance: U256) -> RpcResult<bool>;
    #[method(name = "ping")]
    fn ping(&self) -> RpcResult<String>;
}

pub struct AccountStateExt<Provider> {
    provider: Provider,
}

impl<Provider> AccountStateExtApiServer for AccountStateExt<Provider>
where
    Provider: Database + Send + Sync + 'static,
{
    fn update_basic_account(&self, address: Address, new_balance: U256) -> RpcResult<bool> {
        let _ = self.provider.update(|tx| {
            // TODO: use result type
            // also support deltas, issue there was converting an I256 -> U256, look at the LIMBS for I256, manually mut and reconstruct into a U256?
            let mut account = tx
                .get::<tables::PlainAccountState>(address)
                .expect("unable to get current state")
                .ok_or("No existing account state")
                .unwrap();

            account.balance = new_balance;
            tx.put::<tables::PlainAccountState>(address, account)
                .expect("error updating account");
        });

        Ok(true)
    }
    fn ping(&self) -> RpcResult<String> {
        Ok("pong".to_string())
    }
}

#[derive(Debug, Clone, Copy, Default, clap::Args)]
pub struct RethCliIrysExt {
    /// CLI flag to enable the txpool extension namespace
    #[arg(long, default_value_t = true)]
    pub enable_irys_ext: bool,
}
#[macro_export]
macro_rules! vec_of_strings {
    ($($x:expr),*) => (vec![$($x.to_string()),*]);
}

pub fn main() -> eyre::Result<()> {
    let runner = CliRunner::default();
    let mut os_args: Vec<String> = std::env::args().collect();
    let mut to_add = vec_of_strings![
        // "-vvvvv",
        "--disable-discovery"
    ];
    os_args.append(&mut to_add);
    let cli = Cli::<RethCliIrysExt>::parse_from(os_args);

    runner.run_command_until_exit(|ctx| run_custom_node(ctx, cli))
}

pub fn get_custom_node<Ext>(
    ctx: CliContext,
    cli: Cli<Ext>,
) -> Result<
    WithLaunchContext<
        NodeBuilderWithComponents<
            FullNodeTypesAdapter<
                EthereumNode,
                Arc<DatabaseEnv>,
                BlockchainProvider<Arc<DatabaseEnv>>,
            >,
            ComponentsBuilder<
                FullNodeTypesAdapter<
                    EthereumNode,
                    Arc<DatabaseEnv>,
                    BlockchainProvider<Arc<DatabaseEnv>>,
                >,
                EthereumPoolBuilder,
                EthereumPayloadBuilder,
                EthereumNetworkBuilder,
                EthereumExecutorBuilder,
            >,
        >,
    >,
    eyre::Error,
>
where
    Ext: clap::Args + fmt::Debug,
{
    // from reth/bin/reth/src/commands/node/mod.rs:137
    let matched_cmd = match cli.command {
        Commands::Node(command) => Some(command),
        _ => None,
    };

    let cmd_cfg = matched_cmd.expect("unable to get cmd_cfg");

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
    } = cmd_cfg;

    let mut node_config = NodeConfig {
        config,
        chain,
        metrics,
        instance,
        network: network.clone(),
        rpc,
        txpool,
        builder,
        debug,
        db,
        dev,
        pruning,
    };

    // must configure logging before tracing init

    let logs = cli.logs;
    // logs.log_file_directory
    // tracing & metric guards
    let _guard = logs.init_tracing()?;
    let _ = node_config.install_prometheus_recorder()?;

    let data_dir = datadir.unwrap_or_chain_default(node_config.chain.chain);
    let db_path = data_dir.db();

    tracing::info!(target: "reth::cli", path = ?db_path, "Opening database");
    let database =
        Arc::new(init_db(db_path.clone(), node_config.db.database_args())?.with_metrics());

    // if with_unused_ports {
    //     node_config = node_config.with_unused_ports();
    // }

    let chain_spec = get_chain_spec();

    let mut rpc_config = RpcServerArgs::default().with_http();
    rpc_config.ipcdisable = true;
    // rpc

    // let node_config = NodeConfig::test()
    //     .dev()
    //     .with_rpc(rpc_config)
    //     .with_network(network)
    //     .with_chain(chain_spec);

    node_config = node_config.with_chain(chain_spec).with_rpc(rpc_config);

    let node = EthereumNode::default();

    Ok(NodeBuilder::new(node_config)
        // .testing_node(ctx.task_executor)
        .with_database(database)
        .with_launch_context(ctx.task_executor, data_dir)
        .node(node)
        .extend_rpc_modules(move |ctx| {
            let provider = ctx.provider().database.clone();
            let db = provider.into_db();
            let ext = AccountStateExt { provider: db };
            ctx.modules.merge_configured(ext.into_rpc())?;
            println!("extension added!");
            Ok(())
        }))
    // .launch()
    // .await?;

    // don't use default launcher

    // let NodeBuilderWithComponents {
    //     adapter: NodeTypesAdapter { database },
    //     components_builder,
    //     add_ons:
    //         NodeAddOns {
    //             hooks,
    //             rpc,
    //             exexs: installed_exex,
    //         },
    //     config,
    // } = node;

    // infinite await
    // launched.node_exit_future.await
}

async fn run_custom_node<Ext>(ctx: CliContext, cli: Cli<Ext>) -> eyre::Result<()>
where
    Ext: clap::Args + fmt::Debug,
{
    let node = get_custom_node(ctx, cli)?;
    let launcher = CustomNodeLauncher::new(node.task_executor, node.data_dir);
    let launched = node.builder.launch_with(launcher).await?;
    launched.node_exit_future.await?;
    Ok(())
}

pub async fn get_dev_node() -> eyre::Result<
    NodeHandle<
        NodeAdapter<
            FullNodeTypesAdapter<
                EthereumNode,
                Arc<DatabaseEnv>,
                BlockchainProvider<Arc<DatabaseEnv>>,
            >,
            Components<
                FullNodeTypesAdapter<
                    EthereumNode,
                    Arc<DatabaseEnv>,
                    BlockchainProvider<Arc<DatabaseEnv>>,
                >,
                Pool<
                    TransactionValidationTaskExecutor<
                        EthTransactionValidator<
                            BlockchainProvider<Arc<DatabaseEnv>>,
                            EthPooledTransaction,
                        >,
                    >,
                    CoinbaseTipOrdering<EthPooledTransaction>,
                    DiskFileBlobStore,
                >,
                EthEvmConfig,
                EthExecutorProvider,
            >,
        >,
    >,
> {
    // don't use AsyncCliRunner here as it spawns it's own nested runtime, but get a handle to the current runtime and use that.
    // let AsyncCliRunner { context, .. } = AsyncCliRunner::new()?;
    let tokio_handle = Handle::try_current()?;
    let task_manager = TaskManager::new(tokio_handle.clone());
    let task_executor = task_manager.executor();
    let context = CliContext { task_executor };
    // trick reth
    let args = vec_of_strings!["reth", "node", "--dev"];
    let cli = Cli::<RethCliIrysExt>::parse_from(args);
    let node = get_custom_node(context, cli)?;
    let launcher = CustomNodeLauncher::new(node.task_executor, node.data_dir);
    let launched = node.builder.launch_with(launcher).await?;
    Ok(launched)
}
