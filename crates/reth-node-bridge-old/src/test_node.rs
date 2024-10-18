use crate::chain::{get_chain_spec, get_chain_spec_with_path};
use crate::custom_rpc::{AccountStateExt, AccountStateExtApiServer};

use reth::args::{ColorMode, DiscoveryArgs, NetworkArgs};
use reth::builder::{NodeBuilder, NodeHandle};

use reth::tasks::TaskManager;
use reth::{args::RpcServerArgs, builder::NodeConfig};

use reth_e2e_test_utils::node::NodeTestContext;
use reth_node_core::irys_ext::{NodeExitReason, ReloadPayload};
use reth_node_ethereum::EthereumNode;
use reth_tracing::{LayerInfo, LogFormat, RethTracer, Tracer as _};

use std::fs::File;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::time::sleep;
use tracing::{trace, Level};

pub async fn run_custom_node_test() -> eyre::Result<()> {
    loop {
        let exit_reason = run_custom_node_test_inner().await?;
        match exit_reason {
            NodeExitReason::Normal => break,
            NodeExitReason::Reload(_) => {
                trace!("reloading node!")
            }
        }
    }
    Ok(())
}

async fn run_custom_node_test_inner() -> eyre::Result<NodeExitReason> {
    // logs/tracing config
    let mut tracer = RethTracer::new();
    let stdout = LayerInfo::new(
        LogFormat::Json,
        Level::TRACE.to_string(),
        "".to_string(),
        Some(ColorMode::Auto.to_string()),
    );
    tracer = tracer.with_stdout(stdout);
    let _guard = tracer.init()?;
    // let node = get_custom_node(ctx, cli)?;
    // let launcher = CustomNodeLauncher::new(node.task_executor, node.data_dir);
    // let launched = node.builder.launch_with(launcher).await?;
    // let chain_spec = get_chain_spec(vec![]);
    let chain_spec = get_chain_spec_with_path(vec![], Path::new("/tmp"), true);

    let network_config = NetworkArgs {
        discovery: DiscoveryArgs {
            disable_discovery: true,
            ..DiscoveryArgs::default()
        },
        ..NetworkArgs::default()
    };
    let mut node_config = NodeConfig::test()
        .with_chain(chain_spec.clone())
        .with_network(network_config.clone())
        // .with_network(network_config.clone())
        // .with_unused_ports()
        .with_rpc(RpcServerArgs::default().with_http());

    // if is_dev {
    //     node_config = node_config.dev();
    // }

    // let span = span!(Level::INFO, "node");
    let tasks = TaskManager::current();
    let exec = tasks.executor();

    // let _enter = span.enter();
    let NodeHandle {
        node,
        node_exit_future,
    } = NodeBuilder::new(node_config.clone())
        .testing_node_2(exec.clone())
        .node(EthereumNode::default())
        .extend_rpc_modules(move |ctx| {
            let provider = ctx.provider().clone();
            let irys_ext = ctx.node().components.irys_ext.clone();
            let ext = AccountStateExt {
                provider,
                irys_ext,
                network: ctx.network().clone(),
            };
            ctx.modules.merge_configured(ext.into_rpc())?;
            println!("extension added!");
            Ok(())
        })
        .launch()
        .await?;
    let is_dev = node.config.dev.dev.clone();

    let node_test_context = NodeTestContext::new(node);

    let exit_reason = node_exit_future.await?;

    if is_dev {
        match exit_reason.clone() {
            NodeExitReason::Normal => (),
            NodeExitReason::Reload(payload) => match payload {
                ReloadPayload::ReloadConfig(chain_spec) => {
                    // delay here so the genesis submission RPC reponse is able to make it back before the server dies

                    let ser = serde_json::to_string_pretty(&chain_spec)?;
                    let pb = PathBuf::from(
                        "/tmp/dev_genesis.json", /* node.data_dir.data_dir().join("dev_genesis.json") */
                    );
                    // remove_file(&pb)?;
                    let mut f = File::create(&pb)?;
                    f.write_all(ser.as_bytes())?;
                    sleep(Duration::from_millis(100)).await;
                }
            },
        }
    }
    Ok(exit_reason)
}
