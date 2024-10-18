use reth::rpc::eth::EthApiSpec;
use reth_node_ethereum::EthereumNode;
use reth_primitives::{Chain, PayloadAttributes};
use reth_rpc_types::BlockId;
use reth_tracing::{LayerInfo, LogFormat, RethTracer, Tracer};
use revm_primitives::{
    commitment::{Commitment, CommitmentStatus, CommitmentType, IrysBlockHash, IrysTxId, Stake},
    shadow::{ShadowTx, ShadowTxType, Shadows, TransferShadow},
    DestHash, GenesisAccount, LastTx,
};
use std::sync::Arc;

#[tokio::test]
async fn can_sync() -> eyre::Result<()> {
    // let x = serde_json::to_string(&LastTx::TxId(IrysTxId::random()));
    // dbg!(x);

    // // "{"TxId":"0x76e8d0be5ed672bc466b07cbd7af68a77f70890ec671e4b07e4e1f207306018c"}"

    // let v = serde_json::from_str::<LastTx>(
    //     "{\"TxId\":\"0x76e8d0be5ed672bc466b07cbd7af68a77f70890ec671e4b07e4e1f207306018c\"}",
    // );
    // dbg!(v);

    // let x = serde_json::to_string(&LastTx::BlockHash(IrysBlockHash::random()));
    // // "{"BlockHash":"0xc344a24ca77fe96cefee152bc16ee084915624d2e0b57d1b217c84b008de23aed5b1776f4bfb32ae4c511c665e9d2821"}"
    // dbg!(x);

    // let v = serde_json::from_str::<LastTx>("{\"BlockHash\":\"0xc344a24ca77fe96cefee152bc16ee084915624d2e0b57d1b217c84b008de23aed5b1776f4bfb32ae4c511c665e9d2821\"}");
    // dbg!(v);

    // let v = serde_json::from_str::<BlockId>("{\"blockNumber\": \"0x1\"}").unwrap();
    // dbg!(&v.is_number());
    // dbg!(v);

    // let v = serde_json::from_str::<BlockId>("{\"blockNumber\": \"latest\"}").unwrap();
    // let x = v.is_latest();
    // dbg!(v);

    // let v = serde_json::to_string(&BlockId::latest());
    // dbg!(v);

    // let v = serde_json::to_string(&BlockId::number(12));
    // dbg!(v);
    // let v = serde_json::to_string(&BlockId::hash(B256::random()));
    // dbg!(v);

    let x = vec![(
        Address::random(),
        GenesisAccount {
            nonce: Some(0),
            balance: U256::from(69696),
            code: None,
            storage: None,
            private_key: None,
            commitments: Some(
                vec![Commitment {
                    tx_id: IrysTxId::random(),
                    quantity: U256::from(69696),
                    dest_hash: DestHash::PartitionHash(IrysTxId::random()),
                    status: CommitmentStatus::Active,
                    height: 0,
                    tx_type: CommitmentType::Pledge,
                }]
                .into(),
            ),
            stake: Some(Stake {
                tx_id: IrysTxId::random(),
                quantity: U256::from(69696),
                status: CommitmentStatus::Active,
                height: 0,
            }),
            mining_permission: true,
            last_tx: None,
        },
    )];

    let v = serde_json::to_string(&x);
    dbg!(v);

    // reth_tracing::init_test_tracing();

    let tracer = RethTracer::new().with_stdout(LayerInfo::new(
        LogFormat::Json,
        LevelFilter::DEBUG.to_string(),
        "debug".to_string(),
        None,
    ));
    let _ = tracer.init()?;

    // let bytes = hex!("f90288f90218a0fe21bb173f43067a9f90cfc59bbb6830a7a2929b5de4a61f372a9db28e87f9aea01dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347940000000000000000000000000000000000000000a061effbbcca94f0d3e02e5bd22e986ad57142acabf0cb3d129a6ad8d0f8752e94a0d911c25e97e27898680d242b7780b6faef30995c355a2d5de92e6b9a7212ad3aa0056b23fbba480696b65fe5a59b8f2148a1299103c4f57df839233af2cf4ca2d2b90100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000008003834c4b408252081e80a00000000000000000000000000000000000000000000000000000000000000000880000000000000000842806be9da056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421f869f86702842806be9e82520894658bdf435d810c91414ec09147daa6db624063798203e880820a95a040ce7918eeb045ebf8c8b1887ca139d076bda00fa828a07881d442a72626c42da0156576a68e456e295e4c9cf67cf9f53151f329438916e0f24fc69d6bbb7fbacfc0c0");
    // let bytes_buf = &mut bytes.as_ref();
    // let block = Block::decode(bytes_buf).unwrap();
    // let mut encoded_buf = Vec::new();
    // block.encode(&mut encoded_buf);
    // assert_eq!(bytes[..], encoded_buf);

    let test_wallet = Wallet::default().with_chain_id(Chain::from(4096).id());

    let (mut nodes, _tasks, wallet) = setup::<EthereumNode>(
        2,
        Arc::new(
            // ChainSpecBuilder::default()
            //     .chain(MAINNET.chain)
            //     .genesis(
            //         serde_json::from_str(include_str!(
            //             "../../reth/crates/ethereum/node/tests/assets/genesis.json"
            //         ))
            //         .unwrap(),
            //     )
            //     .cancun_activated()
            //     .build(),
            get_chain_spec(vec![test_wallet.inner.address()]),
        ),
        false,
    )
    .await?;
    let raw_tx = TransactionTestContext::transfer_tx_bytes(4096, wallet.inner).await;
    let mut second_node = nodes.pop().unwrap();
    let mut first_node = nodes.pop().unwrap();

    // Make the first node advance
    let tx_hash = first_node.rpc.inject_tx(raw_tx).await?;
    // let mut attrs;

    // make the node advance
    let (payload, attrs) = first_node
        .advance_block(vec![], |ts| {
            // let mut pa = eth_payload_attributes(ts);

            let attributes = PayloadAttributes {
                timestamp: ts,
                prev_randao: B256::ZERO,
                suggested_fee_recipient: Address::ZERO,
                withdrawals: Some(vec![]),
                parent_beacon_block_root: Some(B256::ZERO),
                shadows: Some(Shadows::new(vec![ShadowTx {
                    tx_id: IrysTxId::random(),
                    address: test_wallet.inner.address(),
                    tx: ShadowTxType::Transfer(TransferShadow {
                        to: Address::random(),
                        amount: U256::from(99999999),
                    }),
                    fee: U256::from(1000),
                }])),
            };
            // pa.shadows = Some(Shadows::new(vec![ShadowTx {
            //     tx_id: TxId::random(),
            //     address: Address::random(),
            //     tx: ShadowTxType::Transfer(TransferShadow {
            //         to: Address::random(),
            //         amount: U256::from(99999999),
            //     }),
            // }]));
            let mut attrs2 = EthPayloadBuilderAttributes::new(B256::ZERO, attributes.clone());
            attrs2.parent_beacon_block_root = None;
            attrs2
        })
        .await?;

    let block_hash = payload.block().hash();
    let block_number = payload.block().number;

    // assert the block has been committed to the blockchain
    first_node
        .assert_new_block(tx_hash, block_hash, block_number)
        .await?;

    let recovered_attrs = PayloadAttributes {
        timestamp: attrs.timestamp,
        suggested_fee_recipient: attrs.suggested_fee_recipient,
        prev_randao: attrs.prev_randao,
        withdrawals: Some(attrs.withdrawals.to_vec()),
        parent_beacon_block_root: attrs.parent_beacon_block_root,
        shadows: Some(attrs.shadows.clone()),
    };
    let bal = first_node
        .rpc
        .get_balance(test_wallet.inner.address(), None)
        .await;
    dbg!(bal);
    // let bal = first_node
    //     .rpc
    //     .inner
    //     .eth
    //     .expect("e")
    //     .api
    //     .balance(test_wallet.inner.address(), Some(BlockId::latest()));

    let res = serde_json::to_string(&recovered_attrs);
    dbg!(res);

    // second_node
    //     .engine_api
    //     .add_shadows(block_hash, attrs.shadows.clone())
    //     .await?;

    // let shadows2 = second_node
    //     .inner
    //     .db
    //     .pending_shadows(reth_primitives::BlockHashOrNumber::Hash(block_hash))?;
    // dbg!(shadows2);
    // only send forkchoice update to second node
    // first_node
    //     .rpc
    //     .inner
    second_node
        .engine_api
        .update_forkchoice_payload_attr(block_hash, block_hash, Some(recovered_attrs))
        .await?;

    // expect second node advanced via p2p gossip
    second_node.assert_new_block(tx_hash, block_hash, 1).await?;
    // first_node.rpc.inner.eth_api().
    dbg!("done!");
    Ok(())
}

use reth_e2e_test_utils::{
    setup, transaction::TransactionTestContext, wallet::Wallet, NodeHelperType,
};
use reth_node_core::primitives::U256;
use reth_payload_builder::EthPayloadBuilderAttributes;
use reth_primitives::{Address, B256};
use tracing::level_filters::LevelFilter;

use crate::chain::{get_chain_spec, get_chain_spec_with_path};

/// Ethereum Node Helper type
pub type EthNode = NodeHelperType<EthereumNode>;

/// Helper function to create a new eth payload attributes
pub fn eth_payload_attributes(timestamp: u64) -> EthPayloadBuilderAttributes {
    let attributes = PayloadAttributes {
        timestamp,
        prev_randao: B256::ZERO,
        suggested_fee_recipient: Address::ZERO,
        withdrawals: Some(vec![]),
        parent_beacon_block_root: Some(B256::ZERO),
        shadows: Some(Shadows::new(vec![ShadowTx {
            tx_id: IrysTxId::random(),
            address: Address::random(),
            tx: ShadowTxType::Transfer(TransferShadow {
                to: Address::random(),
                amount: U256::from(99999999),
            }),
            fee: U256::from(999999),
        }])),
    };
    EthPayloadBuilderAttributes::new(B256::ZERO, attributes)
}
