use std::{
    fs::{remove_dir_all, File},
    io::Write as _,
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use alloy_core::primitives::U256;
use irys_reth_node_bridge::{rpc::AccountStateExtApiClient, GenesisInfo, IrysChainSpecBuilder};
use irys_types::Address;
use reth::{
    chainspec::ChainSpec,
    core::irys_ext::{NodeExitReason, ReloadPayload},
    tasks::{TaskExecutor, TaskManager},
};
use reth_db::mdbx::ffi;
use reth_primitives::{Genesis, GenesisAccount};
use tokio::{runtime::Handle, time::sleep};
use tracing::{error, info};

#[tokio::test]
pub async fn basic_genesis_test() -> eyre::Result<()> {
    // TODO: eventually we'll want to produce a nice testing API like ext/reth/crates/e2e-test-utils/src/lib.rs
    // but for now due to runtime weirdness we just yoink the parts of the reth API we need out
    remove_dir_all("../../.reth")?;
    let builder = IrysChainSpecBuilder::mainnet();
    let mut chainspec = builder.reth_builder.build();

    // TODO @JesseTheRobot - make sure logging is initialized before we get here as this uses logging macros
    // use the existing reth code to handle blocking & graceful shutdown
    // let AsyncCliRunner {
    //     context,
    //     mut task_manager,
    //     tokio_runtime,
    // } = AsyncCliRunner::new()?;

    let addr1 = Address::random();
    let balance1 = U256::from(1000000000);
    let alloc = vec![(
        addr1,
        GenesisAccount {
            balance: balance1,
            ..Default::default()
        },
    )];

    let genesis_info = GenesisInfo {
        accounts: alloc.clone(),
        shadows: None,
        timestamp: 0,
    };

    let new_genesis = genesis(genesis_info.clone(), chainspec.clone()).await?;

    sleep(Duration::from_millis(500)).await;

    let handle = Handle::current();
    let task_manager = TaskManager::new(handle);
    let task_executor = task_manager.executor();
    chainspec.genesis = new_genesis;
    // re-create the node
    // drop(node_handle.node);

    dbg!("new node");
    let node_handle =
        irys_reth_node_bridge::run_node(Arc::new(chainspec.clone()), task_executor).await?;

    let http_rpc_client = node_handle.node.rpc_server_handle().http_client().unwrap();
    let res2 = http_rpc_client.get_account(addr1, None).await?;
    // dbg!(res2);
    assert!(res2.is_some_and(|a| a.balance == balance1));
    Ok(())
}

async fn genesis(genesis_info: GenesisInfo, chainspec: ChainSpec) -> eyre::Result<Genesis> {
    let handle = Handle::current();
    let task_manager = TaskManager::new(handle);
    let task_executor = task_manager.executor();

    let node_handle =
        irys_reth_node_bridge::run_node(Arc::new(chainspec.clone()), task_executor.clone()).await?;

    // let http_rpc_client = node_handle.node.rpc_server_handle().http_client().unwrap();
    // http_rpc_client.build_new_payload_irys(parent, payload_attributes);
    // submit genesis
    // let ts: u64 = std::time::SystemTime::now()
    //     .duration_since(std::time::UNIX_EPOCH)
    //     .unwrap()
    //     .as_millis()
    //     .into();

    let res = node_handle
        .node
        .rpc_server_handle()
        .http_client()
        .unwrap()
        .add_genesis_block(genesis_info)
        .await?;

    let exit_reason: NodeExitReason = node_handle.node_exit_future.await?;

    match exit_reason.clone() {
        NodeExitReason::Normal => Ok(chainspec.genesis.clone()),
        NodeExitReason::Reload(payload) => match payload {
            ReloadPayload::ReloadConfig(chain_spec) => {
                dbg!("reloading!");
                error!("RELOADING2");

                // delay here so the genesis submission RPC reponse is able to make it back before the server dies
                let ser = serde_json::to_string_pretty(&chain_spec.genesis)?;
                let pb = PathBuf::from(
                    node_handle
                        .node
                        .data_dir
                        .data_dir()
                        .join("dev_genesis.json"),
                );
                // remove_file(&pb)?;
                let mut f = File::create(&pb)?;
                f.write_all(ser.as_bytes())?;
                info!("Written dev_genesis.json");
                // node_handle.node.rpc_server_handles.rpc.clone().stop()?;
                // node_handle.node.rpc_server_handles.auth.clone().stop()?;
                // node_handle.node.rpc_server_handle().clone().stop()?;

                // // drop(node_handle.node.provider.database.db.inner.inner.env);

                // manually close the DB - if you don't do this, you'll get an error code (11)
                unsafe {
                    ffi::mdbx_env_close_ex(
                        node_handle.node.provider.database.db.inner.inner.env,
                        false,
                    );
                }
                drop(node_handle.node);

                // chainspec.genesis = chain_spec.genesis;
                // drop(node_handle.node);
                // sleep(Duration::from_millis(500)).await;
                return Ok(chain_spec.genesis.clone());
            }
        },
    }
}
