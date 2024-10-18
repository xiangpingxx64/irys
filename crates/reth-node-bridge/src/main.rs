use reth_node_bridge::custom_rpc;

pub fn main() -> eyre::Result<()> {
    custom_rpc::main()?;
    Ok(())
}
