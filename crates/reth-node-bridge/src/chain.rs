use std::{collections::HashMap, fs::File, io::Write};

use reth_primitives::{hex, Chain, ChainSpec, ChainSpecBuilder, Genesis, GenesisAccount, U256};
use serde_json::json;

pub fn get_chain_spec() -> ChainSpec {
    let chain = Chain::from_id(4096);

    let genesis_val = json!(
        {

            "nonce": "0x42",
            "timestamp": "0x0",
            "extraData": "0x5343",
            "gasLimit": "0x1388",
            "difficulty": "0x400000000",
            "mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "coinbase": "0x0000000000000000000000000000000000000000",
            "alloc": {
                "0x6Be02d1d3665660d22FF9624b7BE0551ee1Ac91b": {
                    "balance": "0x4a47e3c12448f4ad000000"
                }
            },
            "number": "0x0",
            "gasUsed": "0x0",
            "parentHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "config": {
                "ethash": {},
                "chainId": chain.id(),
                "homesteadBlock": 0,
                "eip150Block": 0,
                "eip155Block": 0,
                "eip158Block": 0,
                "byzantiumBlock": 0,
                "constantinopleBlock": 0,
                "petersburgBlock": 0,
                "istanbulBlock": 0,
                "berlinBlock": 0,
                "londonBlock": 0,
                "terminalTotalDifficulty": 0,
                "terminalTotalDifficultyPassed": true,
                "shanghaiTime": 0
            }
        }
    );

    // genesis_val.pointer_mut("/config/chainId")
    // genesis_val["alloc"].as_object_mut()

    let mut genesis: Genesis = serde_json::from_str(&genesis_val.to_string()).unwrap();
    genesis = genesis.extend_accounts(HashMap::from([(
        hex!("6Be02d1d3665660d22FF9624b7BE0551ee1Ac91b").into(),
        GenesisAccount::default().with_balance(U256::from(33)),
    )]));

    let chain_spec = ChainSpecBuilder::default()
        .chain(chain)
        .genesis(genesis)
        // hardfork activation
        .paris_activated()
        .build();
    let ser = serde_json::to_string(&chain_spec).expect("unable to serialize");
    // write out full chainspec to JSON
    // let mut f = File::create("./chainspec_1.json").expect("fcreate");
    // f.write_all(ser.as_bytes()).expect("write");

    // dbg!(&chain);
    // dbg!(&chain_spec);
    return chain_spec;
}
