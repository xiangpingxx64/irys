use chain::chain::{start_for_testing, IrysNodeConfig};
use irys_types::{block_production::SolutionContext, Address};

#[tokio::test]
async fn test_blockprod() -> eyre::Result<()> {
    // note: this isn't an actual fully fledged test, as we lack the required infrastructure for that right now.
    // instead it's just a small function that prompts the block producer to produce a block
    // TODO: Make this a full test
    let node = start_for_testing(IrysNodeConfig {
        ..Default::default()
    })
    .await?;

    node.actor_addresses
        .block_producer
        .send(SolutionContext {
            partition_id: 0,
            chunk_index: 0,
            mining_address: Address::random(),
        })
        .await?;

    Ok(())
}
