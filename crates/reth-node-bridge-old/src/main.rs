use reth_node_bridge::custom_rpc;
// use reth_node_bridge::scratch;
use reth_node_bridge::test_node;
// todo: redenominate gwei etc
// #[tokio::main]
pub fn main() -> eyre::Result<()> {
    custom_rpc::main()?;
    // test_node::run_custom_node_test().await?;
    // scratch::main()?;
    Ok(())
}
