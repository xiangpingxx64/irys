use reth_payload_builder::EthPayloadBuilderAttributes;
use revm_primitives::{commitment::IrysTxId, B256};
use revm_primitives::{
    irys::payload::PayloadAttributes as EthPayloadAttributes,
    shadow::{DataShadow, ShadowTx},
    U256,
};
use revm_primitives::{shadow::ShadowTxType, Address};

#[tokio::main]
pub async fn run_node_test() -> () {
    node_test().await.expect("exited with error variant");
}

async fn get_dev_node_2() {}

async fn node_test() -> eyre::Result<()> {
    // // let launched = get_dev_node().await?;
    // let test_wallet = Wallet::default().with_chain_id(Chain::from(4096).id());
    // // let t = test_wallet.inner.address()
    // let chain_spec = get_chain_spec(vec![test_wallet.inner.address()], ChainPath{ "/"});
    // let network_config = NetworkArgs {
    //     discovery: DiscoveryArgs {
    //         disable_discovery: true,
    //         ..DiscoveryArgs::default()
    //     },
    //     ..NetworkArgs::default()
    // };
    // let node_config = NodeConfig::test()
    //     .with_chain(chain_spec.clone())
    //     .with_network(network_config.clone())
    //     .with_unused_ports()
    //     .with_rpc(RpcServerArgs::default().with_unused_ports().with_http());
    // // .dev();

    // // if is_dev {
    // //     node_config = node_config.dev();
    // // }

    // let tasks = TaskManager::current();
    // let exec = tasks.executor();

    // let span = span!(Level::TRACE, "node");
    // let _enter = span.enter();
    // let NodeHandle {
    //     node,
    //     node_exit_future: _,
    // } = NodeBuilder::new(node_config.clone())
    //     .testing_node(exec.clone())
    //     .node::<EthereumNode>(Default::default())
    //     .launch()
    //     .await?;

    // // let chain_spec = launched.node.chain_spec().clone();
    // // wrap node in test utils
    // let mut test_ctx = NodeTestContext::new(node).await?;

    // let raw_tx =
    //     TransactionTestContext::transfer_tx_bytes(chain_spec.chain().into(), test_wallet.inner)
    //         .await;

    // let raw_tx = TransactionTestContext::transfer_tx_bytes(1, test_wallet.inner).await;
    // let attr = CustomPayloadAttributes {
    // inner: EthPayloadAttributes {
    // let attr = EthPayloadAttributes {
    //     timestamp: 0 as u64,
    //     prev_randao: B256::ZERO,
    //     suggested_fee_recipient: Address::default(),
    //     withdrawals: None,
    //     parent_beacon_block_root: None,
    //     shadows: None,
    // };
    // let payload_attr = EthPayloadBuilderAttributes::new(B256::ZERO, attr);
    // };
    // let pa = CustomPayloadBuilderAttributes::try_new(Default::default(), attr).unwrap();

    // let eth_attr = test_ctx
    //     .payload
    //     .payload_builder
    //     .new_payload(payload_attr)
    //     .await?;
    // let (payload, eth_attr) = test_ctx
    //     .new_payload(|timestamp| {
    //         let attr = EthPayloadAttributes {
    //             timestamp,
    //             prev_randao: B256::ZERO,
    //             suggested_fee_recipient: Address::default(),
    //             withdrawals: None,
    //             parent_beacon_block_root: None,
    //             shadows: None,
    //         };
    //         let payload_attr = EthPayloadBuilderAttributes::new(B256::ZERO, attr);
    //         return payload_attr;
    //     })
    //     .await?;

    // let block_hash = test_ctx
    //     .engine_api
    //     .submit_payload(
    //         payload.clone(),
    //         eth_attr.clone(),
    //         PayloadStatusEnum::Valid,
    //         vec![],
    //     )
    //     .await?;

    // test_ctx
    //     .engine_api
    //     .update_forkchoice(block_hash, block_hash)
    //     .await?;

    // build payload

    // test_ctx.engine_api.submit_payload(
    //     payload,
    //     payload_builder_attributes,
    //     PayloadStatusEnum::Valid,
    //     versioned_hashes,
    // );
    // test_ctx.

    // let node = setup::<EthereumNode>(1, chain_spec, true);

    // let tx_hash = test_ctx.rpc.inject_tx(raw_tx).await?;
    // let (payload, _) = test_ctx.advance_block(vec![], payload_gen).await?;
    // // test_ctx.advance(length, tx_generator, attributes_generator)
    // dbg!(payload);

    // let block_hash = payload.block().hash();
    // let block_number = payload.block().number;

    Ok(())
}

fn payload_gen(ts: u64) -> EthPayloadBuilderAttributes {
    // do not mess with these fields, they will cause issues!
    let attr = EthPayloadAttributes {
        timestamp: ts,
        prev_randao: B256::ZERO,
        suggested_fee_recipient: Address::ZERO,
        withdrawals: None,              /* Some(vec![]) */
        parent_beacon_block_root: /* None */ Some(B256::ZERO),
        shadows: Some(vec![ShadowTx {tx_id: IrysTxId::random(), address: Address::random(), tx: ShadowTxType::Data(DataShadow { fee: U256::from(999999)}), fee: U256::from(999999)}].into()),
    };
    let payload_attr = EthPayloadBuilderAttributes::new(B256::ZERO, attr);
    return payload_attr;
}

mod tests {
    use crate::node_block_test::node_test;

    #[tokio::test]
    async fn test() {
        match node_test().await {
            Ok(v) => {
                dbg!(v);
            }
            Err(e) => {
                dbg!(e);
                dbg!("test");
            }
        }
        // dbg!(res);
    }
}
