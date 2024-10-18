#[cfg(test)]
mod tests {

    use crate::custom_rpc::{get_dev_node, GenesisInfo};
    use alloy_rlp::{Decodable, Encodable};
    use alloy_rpc_types_engine::ForkchoiceState;
    use reth_e2e_test_utils::wallet::Wallet;
    use reth_primitives::{
        revm_primitives::{
            commitment::IrysTxId,
            shadow::{ShadowTx, ShadowTxType, Shadows, TransferShadow},
        },
        Account, Address, Header, SealedBlock, Withdrawals, B256, U256,
    };
    use revm_primitives::{
        commitment::{Commitment, CommitmentStatus, CommitmentType, Commitments, IrysBlockHash},
        shadow::{BlockRewardShadow, DiffShadow},
        DestHash, GenesisAccount, LastTx, NewAccountState, WrappedCommitments,
    };
    #[test]
    fn test() {
        // dbg!("rust tests");
        // let chain_spec = get_chain_spec(vec![]);
        // dbg!(chain_spec);
        // ()
    }
    #[tokio::test]
    async fn block_test() {
        let mut node = get_dev_node().await.expect("unable to build dev node");
        let block = SealedBlock {
            header: Header {
                number: 0,
                difficulty: U256::from(1),
                ..Default::default()
            }
            .seal(B256::ZERO),
            body: vec![],
            ommers: vec![],
            withdrawals: Some(Withdrawals::default()),
            shadows: Some(Shadows::default()),
        };
        dbg!(block);
        let _ = node.node_exit_future.await;
        ()
        // node.node.provider.tree.get_state()
        // // add this to the exposed tree
        // let res = node
        //     .node
        //     .provider.database.provider_rw().unwrap().tx_ref().put::<tables::CanonicalHeaders>(1, B256::new([100; 32]))
    }
    #[test]
    fn enc_dec_test() {
        let shadow1 = ShadowTx {
            tx_id: IrysTxId::random(),
            address: Address::random(),
            tx: ShadowTxType::Transfer(TransferShadow {
                to: Address::random(),
                amount: U256::from(69420),
            }),
            fee: U256::from(1000),
        };
        let shadows = Shadows::new(vec![shadow1]);
        let mut buf = vec![];
        let enc = shadows.encode(&mut buf);
        dbg!(&buf);
        let slice = &mut buf.as_slice();
        match Shadows::decode(slice) {
            Ok(d) => {
                d;
                ()
            }
            Err(e) => {
                dbg!(e);
                ()
            }
        };
        // dbg!(&dec);

        ()
    }

    #[test]
    fn tx_id_test() {
        // Dib4n6GtpN1mpHcsorKE7cz3EGfqWKpcse69bNKUkSwh
        let d = vec![
            188, 243, 146, 231, 237, 164, 140, 61, 213, 21, 220, 10, 4, 194, 159, 53, 204, 81, 154,
            71, 85, 10, 85, 79, 194, 210, 8, 66, 91, 5, 165, 208,
        ];
        dbg!(d.len());
        let bs58Address = bs58::decode("AmDEHYirZKemURMzegAtpRxpoZ2MFyrRp318cJHYsAhD")
            .into_vec()
            .expect("unable to decode bs58 address");
        dbg!(&bs58Address);
        let b2: [u8; 20] = bs58Address
            .as_slice()
            .try_into()
            .expect("unable to convert to u8;32");
        let addr = Address::from(b2);
        let irysTxId = "47RVkQ1w9kztRVACxJyz8MRSFakztWAPEL9iUCd8Twm8";
        let txBuf: [u8; 32] = bs58::decode(irysTxId)
            .into_vec()
            .expect("unable to decode tx id")
            .try_into()
            .expect("unable to decode tx id");

        let txid = IrysTxId::from(txBuf);

        dbg!(addr);
        dbg!(txid);
    }
    #[test]
    fn account_test() {
        let acc = Account {
            nonce: 0,
            balance: U256::from(100),
            commitments: None,
            stake: None,
            last_tx: Some(LastTx::BlockHash(IrysBlockHash::random())),
            bytecode_hash: None,
            mining_permission: false,
        };
        let v = serde_json::to_string_pretty(&acc);
        dbg!(v);
    }

    #[test]
    fn fcu_test() {
        let f = ForkchoiceState {
            head_block_hash: B256::random(),
            safe_block_hash: B256::random(),
            finalized_block_hash: B256::random(),
        };
        let e = serde_json::to_string(&f);
        dbg!(e);
    }

    #[test]
    fn fcu_dec_test() {
        let S = "{\"safeBlockHash\":\"0x5d6b5d5f90437cb15ca711bafaf4a12af4033bc980cb64c3a9c839e9e1c03fe3\",\"headBlockHash\":\"0x0b15aa3cc88ad84dffe6179baa2bf75c1ecf9c3dbaa276524cc8b590ec88b92e\",\"finalizedBlockHash\":\"0x5d6b5d5f90437cb15ca711bafaf4a12af4033bc980cb64c3a9c839e9e1c03fe3\"}";
        let FCS = serde_json::from_str::<ForkchoiceState>(&S);
        dbg!(FCS);
        let x: Option<String> = None;
        let XS = serde_json::to_string(&x);
        dbg!(XS);
    }

    #[test]
    fn shadow_enc_test_2() {
        let shadow = ShadowTx {
            tx_id: IrysTxId::random(),
            fee: U256::from(0),
            address: Address::random(),
            tx: ShadowTxType::BlockReward(BlockRewardShadow {
                reward: U256::from(10000),
            }),
        };
        let enc = serde_json::to_string(&shadow);
        dbg!(enc);
    }
    #[test]
    fn pk() {
        let wallet = Wallet::default().with_chain_id(4096);
        let w = wallet.inner.into_signer().to_bytes();
        dbg!(w);
    }
    #[test]
    fn genesis_info() {
        let info = GenesisInfo {
            accounts: vec![(Address::random(), GenesisAccount::default())],
            shadows: Some(Shadows::new(vec![])),
            timestamp: 100,
        };
        let info2 = GenesisInfo {
            accounts: vec![(Address::random(), GenesisAccount::default())],
            shadows: None,
            timestamp: 100,
        };
        let x = (serde_json::to_string(&info), serde_json::to_string(&info2));
        dbg!(x);
    }
    #[test]
    fn shadow_enc_test_3() {
        let shadow = ShadowTx {
            tx_id: IrysTxId::random(),
            fee: U256::from(0),
            address: Address::random(),
            tx: ShadowTxType::Diff(DiffShadow {
                new_state: NewAccountState {
                    balance: Some(U256::from(10000)),
                    ..Default::default()
                },
            }),
        };
        let enc = serde_json::to_string(&shadow);
        dbg!(enc);
    }
    #[test]
    fn shadow_enc_test_4() {
        let com = Commitment {
            tx_id: IrysTxId::random(),
            quantity: U256::from(100),
            dest_hash: Some(DestHash::PartitionHash(IrysTxId::random())),
            height: 1,
            status: CommitmentStatus::Active,
            tx_type: CommitmentType::Pledge,
        };
        let enc_com = serde_json::to_string(&com);
        dbg!(enc_com);

        let mut buf = vec![];
        com.encode(&mut buf);
        // dbg!(&buf);
        let slice = &mut buf.as_slice();
        dbg!(com);
        let decoded = Commitment::decode(slice).expect("dec err");
        dbg!(decoded);

        let shadow = ShadowTx {
            tx_id: IrysTxId::ZERO,
            fee: U256::from(0),
            address: Address::random(),
            tx: ShadowTxType::Diff(DiffShadow {
                new_state: NewAccountState {
                    balance: Some(U256::from(10000)),
                    commitments: Some(WrappedCommitments(Some(Commitments(vec![com])))),
                    ..Default::default()
                },
            }),
        };
        let enc = serde_json::to_string(&shadow);
        dbg!(enc);
    }
}
