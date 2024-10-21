use std::{
    fs::{self, File},
    io::Write as _,
    path::{Path, PathBuf},
};

use irys_primitives::Genesis;
use irys_types::EVM_CHAIN_ID;

use reth_chainspec::{Chain, ChainSpec, ChainSpecBuilder};
use tracing::{debug, info};

/// Helper function to load the dev genesis from disk.
/// This overrides the genesis element of the og_chainspec if is_dev is true and the file <RETH_DATA_DIR>/dev_genesis.json exists
pub fn get_chain_spec_with_path(
    mut og_chainspec: ChainSpec,
    data_dir: &Path,
    is_dev: bool,
) -> ChainSpec {
    info!("Running in dev mode? {}", &is_dev);
    if !is_dev {
        return og_chainspec;
    }

    let path = PathBuf::from(data_dir.join("dev_genesis.json"));
    let v = path.try_exists();
    debug!("attempting to load dev_genesis.json from {:#?}", &path);
    if v.is_ok_and(|v| v) {
        debug!("loading dev_genesis.json from {:#?}", &path);

        let genesis: Genesis = serde_json::from_str(
            &fs::read_to_string(path.clone()).expect("unable to read genesis file"),
        )
        .unwrap();
        // remove_file(path);

        og_chainspec.genesis = genesis;
        // reset associated OnceCells
        debug!(
            "Loaded dev_genesis, hash: {:?}",
            &og_chainspec.genesis_hash()
        );
        og_chainspec.genesis_hash = Default::default();
        og_chainspec.genesis_header = Default::default();
        // note: due to OnceCell it's important we use the method accessor, as it initializes it
        debug!(
            "Loaded dev_genesis, hash: {:?}",
            &og_chainspec.genesis_hash()
        );
        return og_chainspec;
    } else {
        debug!(
            "unable to load dev_genesis.json from {:#?} - file doesn't exist",
            &path
        );
        og_chainspec
    }
}

pub fn get_base_dev_chainspec() -> ChainSpecBuilder {
    ChainSpecBuilder::default()
        .chain(Chain::from_id(EVM_CHAIN_ID))
        .genesis(Default::default())
        // hardfork activation - this will activate all preceding hardforks
        .cancun_activated()
}

// TODO @JesseTheRobot - make these proper tests
#[test]
fn test_cs() {
    let csb = get_base_dev_chainspec();
    let chainspec = csb.build();
    let chainspec2 = get_chain_spec_with_path(chainspec, Path::new("../../.reth/"), true);
    dbg!(chainspec2);
}

#[test]
fn w_gen() {
    let chainspec = get_base_dev_chainspec().build();
    let ser = serde_json::to_string_pretty(chainspec.genesis()).expect("err");
    let pb = std::path::absolute(PathBuf::from(
        Path::new("../../.reth/").join("dev_genesis.json"),
    ))
    .expect("pb err");
    dbg!(&pb);
    // remove_file(&pb)?;
    let mut f = File::create(&pb).expect("err");
    f.write_all(ser.as_bytes()).expect("err");
    dbg!("written");
}
