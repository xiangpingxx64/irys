use std::{
    collections::BTreeMap,
    fs::{self, remove_file, File},
    io::Write as _,
    path::{Path, PathBuf},
};

use irys_primitives::{Genesis, GenesisAccount};
use reth::dirs::{ChainPath, DataDirPath};

use reth_chainspec::{Chain, ChainSpec, ChainSpecBuilder};
use revm_primitives::Address;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tracing::{debug, info};

#[derive(Serialize, Deserialize, Clone, Debug)]
struct GenesisAlloc(
    Vec<(Address, GenesisAccount)>, /* BTreeMap<Address, GenesisAccount> */
);

fn get_genesis_json() -> Value {
    json!(
        {

            "nonce": "0x0",
            "timestamp": "0x0",
            "extraData": "0x00",
            "gasLimit": "0x1c9c380",
            "difficulty": "0x0",
            "mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "coinbase": "0x0000000000000000000000000000000000000000",
            "alloc": {
                "0x6Be02d1d3665660d22FF9624b7BE0551ee1Ac91b": {
                    "balance": "0x4a47e3c12448f4ad000000"
                },
                "0x99b560ec252a3f7dd27820cb6e8a7bf82ba96c2e": {
                    // df705c63446378c2865d7f35498e7a828e822454b93b3eda4a60789239965361
                     "balance": "0x4a47e3c12448f4ad000000"
                }
            },
            "number": "0x0",
            "parentHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "config": {
                "chainId": 4096,
                "homesteadBlock": 0,
                "daoForkSupport": true,
                "eip150Block": 0,
                "eip155Block": 0,
                "eip158Block": 0,
                "byzantiumBlock": 0,
                "constantinopleBlock": 0,
                "petersburgBlock": 0,
                "istanbulBlock": 0,
                "muirGlacierBlock": 0,
                "berlinBlock": 0,
                "londonBlock": 0,
                "arrowGlacierBlock": 0,
                "grayGlacierBlock": 0,
                "shanghaiTime": 0,
                "cancunTime": 0,
                "terminalTotalDifficulty": "0x0",
                "terminalTotalDifficultyPassed": true
            }
        }
    )
}

pub fn get_chain_spec(wallets: Vec<Address>) -> ChainSpec {
    let chain = Chain::from_id(4096);

    // let mut genesis_accounts: Vec<(Address, GenesisAccount)> = wallets
    //     .clone()
    //     .iter_mut()
    //     .map(|w| {
    //         (
    //             *w,
    //             GenesisAccount::default()
    //                 .with_balance(U256::from(9999999999999999999999999999 as u128)),
    //         )
    //     })
    //     .collect();

    let mut genesis: Genesis = serde_json::from_str(&get_genesis_json().to_string()).unwrap();

    // genesis = genesis.extend_accounts(genesis_accounts);

    let chain_spec = ChainSpecBuilder::default()
        .chain(chain)
        .genesis(genesis)
        // hardfork activation
        .cancun_activated()
        .build();
    // let ser = serde_json::to_string(&chain_spec).expect("unable to serialize");
    // // write out full chainspec to JSON
    // let mut f = File::create("./chainspec_1.json").expect("fcreate");
    // f.write_all(ser.as_bytes()).expect("write");

    return chain_spec;
}

pub fn get_chain_spec_with_path(wallets: Vec<Address>, data_dir: &Path, is_dev: bool) -> ChainSpec {
    info!("Running in dev mode? {}", &is_dev);
    if true {
        let path = PathBuf::from(data_dir.join("dev_genesis.json"));
        let v = path.try_exists();
        debug!("attempting to load dev_genesis.json from {:#?}", &path);

        if v.is_ok_and(|v| v) {
            debug!("loading dev_genesis.json from {:#?}", &path);

            let chain_spec: ChainSpec = serde_json::from_str(
                &fs::read_to_string(path.clone()).expect("unable to read genesis chain spec file"),
            )
            .unwrap();
            // remove_file(path);
            let hash = chain_spec.genesis_hash();
            debug!("Loaded dev_genesis, hash: {}", &hash);
            return chain_spec;
        } else {
            debug!(
                "unable to load dev_genesis.json from {:#?} - file doesn't exist",
                &path
            );
        }
    }

    let chain = Chain::from_id(4096);

    // reserve addresses here
    // let mut genesis_accounts: Vec<(Address, GenesisAccount)> = wallets
    //     .clone()
    //     .iter_mut()
    //     .map(|w| {
    //         (
    //             *w,
    //             GenesisAccount::default()
    //                 .with_balance(U256::from(9999999999999999999999999999 as u128)),
    //         )
    //     })
    //     .collect();

    let mut genesis: Genesis = serde_json::from_str(&get_genesis_json().to_string()).unwrap();

    // genesis = genesis.extend_accounts(genesis_accounts);

    let chain_spec = ChainSpecBuilder::default()
        .chain(chain)
        .genesis(genesis)
        // hardfork activation
        .cancun_activated()
        .build();

    return chain_spec;
}
