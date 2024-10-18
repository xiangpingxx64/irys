use crate::{chain::get_chain_spec_with_path, node_launcher::CustomNodeLauncher};
use alloy_signer_wallet::LocalWallet;
use clap::Parser;
use jsonrpsee::{core::RpcResult, proc_macros::rpc};
use jsonrpsee_core::async_trait;
use jsonrpsee_types::ErrorObjectOwned;
use reth::api::FullNodeTypesAdapter;
use reth::builder::components::{Components, ComponentsBuilder};
use reth::builder::{
    NodeAdapter, NodeBuilder, NodeBuilderWithComponents, NodeHandle, WithLaunchContext,
};
use reth::commands::node::NoArgs;
use reth::network::NetworkHandle;
use reth::tasks::TaskManager;
use reth::transaction_pool::blobstore::DiskFileBlobStore;
use reth::transaction_pool::{
    CoinbaseTipOrdering, EthPooledTransaction, EthTransactionValidator, Pool,
    TransactionValidationTaskExecutor,
};
use reth::{
    builder::NodeConfig,
    cli::{Cli, Commands},
    commands::node::NodeCommand,
    CliContext, CliRunner,
};
use reth_db::database::Database;
use reth_db::static_file::iter_static_files;
use reth_db::table::Table;
use reth_db::transaction::DbTxMut;
use reth_db::{init_db, transaction::DbTx};
use reth_db::{DatabaseEnv, TableViewer, Tables};

use reth_e2e_test_utils::transaction::{tx, TransactionTestContext};
use reth_interfaces::provider::ProviderResult;
use reth_primitives::Bytes;

use alloy_eips::eip2718::Encodable2718;
use reth_node_core::irys_ext::{IrysExtWrapped, NodeExitReason, ReloadPayload};
use reth_node_ethereum::node::{
    EthereumExecutorBuilder, EthereumNetworkBuilder, EthereumPoolBuilder,
};
use reth_node_ethereum::{EthEvmConfig, EthExecutorProvider, EthereumNode};
use reth_payload_builder::database::CachedReads;
use reth_primitives::static_file::find_fixed_range;
use reth_primitives::ShadowReceipt;
use reth_primitives::{Account, Address, ChainSpec, StaticFileSegment};
use reth_provider::providers::BlockchainProvider;
use reth_provider::{
    BlockIdReader, ChainSpecProvider, FullBundleStateDataProvider, ProviderError, StateProvider,
    StateProviderFactory, StaticFileProviderFactory,
};
use reth_revm::database::StateProviderDatabase;
use reth_revm::state_change::{apply_shadow, simulate_apply_shadow_thin};
use reth_rpc_types::{BlockId, PeerId};
use revm::db::{CacheDB, State};
use revm::{DatabaseCommit, JournaledState};
use revm_primitives::hex::ToHexExt;
use revm_primitives::ruint::Uint;
use revm_primitives::shadow::{ShadowTx, Shadows};
use revm_primitives::{AccountInfo, Bytecode, FixedBytes, GenesisAccount, HashSet, SpecId, B256};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::fs::File;
use std::io::Write as _;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use strum::IntoEnumIterator;
use tokio::runtime::Handle;
use tokio::time::sleep;
use tracing::{debug, error, info, trace};

// We use jemalloc for performance reasons.
#[cfg(all(feature = "jemalloc", unix))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg_attr(not(test), rpc(server, namespace = "irys"))]
#[cfg_attr(test, rpc(server, client, namespace = "irys"))]

pub trait AccountStateExtApi {
    // #[method(name = "updateBasicAccount")]
    // fn update_basic_account(&self, address: Address, new_balance: U256) -> RpcResult<bool>;
    #[method(name = "getAccount")]
    fn get_account(
        &self,
        address: Address,
        block_hash: Option<BlockId>,
    ) -> RpcResult<Option<Account>>;

    #[method(name = "getAccounts")]
    fn get_accounts(
        &self,
        addresses: Vec<Address>,
        block_id: Option<BlockId>,
    ) -> RpcResult<HashMap<Address, Option<Account>>>;

    #[method(name = "getAccount2")]
    fn get_account2(
        &self,
        address: Address,
        block_hash: Option<BlockId>,
    ) -> RpcResult<Option<Account>>;

    #[method(name = "ping")]
    fn ping(&self) -> RpcResult<String>;

    #[method(name = "genesisBlock")]
    fn add_genesis_block(
        &self,
        // accounts: Vec<(Address, GenesisAccount)>,
        // shadows: Option<Shadows>,
        info: GenesisInfo,
    ) -> RpcResult<ChainSpec>;

    #[method(name = "testApplyShadow")]
    fn test_apply_shadow(&self, parent: BlockId, shadow: ShadowTx) -> RpcResult<ShadowReceipt>;

    #[method(name = "addPeer")]
    fn add_peer(&self, peer_id: PeerId, addr: String) -> RpcResult<()>;

    #[method(name = "removePeer")]
    fn remove_peer(&self, peer_id: PeerId) -> RpcResult<()>;

    #[method(name = "getPeerId")]
    fn get_peer_id(&self) -> RpcResult<PeerId>;

    #[method(name = "createEthTx")]
    async fn create_eth_tx(&self, private_key: B256) -> RpcResult<Bytes>;

    #[method(name = "toEthAddress")]
    fn to_address(&self, private_key: B256) -> RpcResult<Address>;

    #[method(name = "getAccounts2")]
    fn get_accounts2(
        &self,
        addresses: Vec<Address>,
        block_id: Option<BlockId>,
    ) -> RpcResult<HashMap<Address, Option<Account>>>;
}

pub struct AccountStateExt {
    pub provider: BlockchainProvider<Arc<DatabaseEnv>>,
    pub irys_ext: IrysExtWrapped,
    pub network: NetworkHandle,
}

pub struct ClearViewer<'a, DB: Database> {
    db: &'a DB,
}

impl<DB: Database> TableViewer<()> for ClearViewer<'_, DB> {
    type Error = eyre::Report;

    fn view<T: Table>(&self) -> Result<(), Self::Error> {
        let tx = self.db.tx_mut()?;
        tx.clear::<T>()?;
        tx.commit()?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GenesisInfo {
    pub accounts: Vec<(Address, GenesisAccount)>,
    pub shadows: Option<Shadows>,
    pub timestamp: u64,
}

pub enum Either<L, R> {
    Left(L),
    Right(R),
}

#[async_trait]
impl AccountStateExtApiServer for AccountStateExt {
    fn add_peer(&self, peer_id: PeerId, addr: String) -> RpcResult<()> {
        dbg!(&addr);
        let socket_addr = SocketAddr::from_str(addr.as_str()).map_err(|e| {
            ErrorObjectOwned::owned::<String>(
                -32073, // TOOD @JesseTheRobot add a RPC error registry so we don't overlap with error codes/numbers
                "unable to convert address into SocketAddr",
                Some(e.to_string()),
            )
        })?;
        // TODO @JesseTheRobot - these don't confirm if the operation was successful
        self.network.peers_handle().add_peer(peer_id, socket_addr);
        Ok(())
    }

    fn remove_peer(&self, peer_id: PeerId) -> RpcResult<()> {
        self.network.peers_handle().remove_peer(peer_id);
        Ok(())
    }

    fn get_peer_id(&self) -> RpcResult<PeerId> {
        Ok(self.network.peer_id().clone())
    }

    fn get_account(
        &self,
        address: Address,
        block_id: Option<BlockId>,
    ) -> RpcResult<Option<Account>> {
        // get provider for latest state
        let state = match block_id {
            Some(block_id) => match block_id {
                BlockId::Number(n) => self.provider.state_by_block_number_or_tag(n),
                BlockId::Hash(h) => self.provider.history_by_block_hash(h.into()),
            },
            None => self.provider.latest(),
        }
        .map_err(|e| {
            ErrorObjectOwned::owned::<String>(
                -32071,
                "error getting state provider",
                Some(e.to_string()),
            )
        })?;
        // TODO: replace with proper errors/error codes
        let r2 = state.basic_account(address).map_err(|e| {
            ErrorObjectOwned::owned::<String>(
                -32072,
                "error getting account info",
                Some(e.to_string()),
            )
        });

        return r2;
    }

    fn get_account2(
        &self,
        address: Address,
        block_id: Option<BlockId>,
    ) -> RpcResult<Option<Account>> {
        // get provider for latest state
        let state_provider: Either<Box<dyn StateProvider>, Box<dyn FullBundleStateDataProvider>> =
            match block_id {
                Some(block_id) => match block_id {
                    BlockId::Number(n) => self
                        .provider
                        .state_by_block_number_or_tag(n)
                        .map(|p| Either::Left(p)),
                    BlockId::Hash(h) => match self.provider.history_by_block_hash(h.into()) {
                        Ok(s) => Ok(Either::Left(s)),
                        Err(_) => self
                            .provider
                            .tree
                            .pending_state_provider(h.into())
                            .map(|p| Either::Right(p)),
                    },
                },
                None => self.provider.latest().map(|p| Either::Left(p)),
            }
            .map_err(|e: ProviderError| {
                ErrorObjectOwned::owned::<String>(
                    -32071,
                    "error getting state provider",
                    Some(e.to_string()),
                )
            })?;

        let acc = match state_provider {
            Either::Left(ref provider) => provider.basic_account(address.clone()).map_err(|e| {
                ErrorObjectOwned::owned::<String>(
                    -32072,
                    "error getting account info",
                    Some(e.to_string()),
                )
            })?,
            Either::Right(ref provider) => provider.state().account(&address).flatten(),
        };

        return Ok(acc);
    }

    fn get_accounts(
        &self,
        addresses: Vec<Address>,
        block_id: Option<BlockId>,
    ) -> RpcResult<HashMap<Address, Option<Account>>> {
        // get provider for latest state
        let state = match block_id {
            Some(block_id) => match block_id {
                BlockId::Number(n) => self.provider.state_by_block_number_or_tag(n),
                BlockId::Hash(h) => self.provider.history_by_block_hash(h.into()),
            },
            None => self.provider.latest(),
        }
        .map_err(|e| {
            ErrorObjectOwned::owned::<String>(
                -32071,
                "error getting state provider",
                Some(e.to_string()),
            )
        })?;
        let mut hm = HashMap::new();
        for address in addresses.iter() {
            hm.insert(
                address.clone(),
                state.basic_account(address.clone()).map_err(|e| {
                    ErrorObjectOwned::owned::<String>(
                        -32072,
                        "error getting account info",
                        Some(e.to_string()),
                    )
                })?,
            );
        }

        Ok(hm)
    }

    fn get_accounts2(
        &self,
        addresses: Vec<Address>,
        block_id: Option<BlockId>,
    ) -> RpcResult<HashMap<Address, Option<Account>>> {
        // get state provider
        let state_provider: Either<Box<dyn StateProvider>, Box<dyn FullBundleStateDataProvider>> =
            match block_id {
                Some(block_id) => match block_id {
                    BlockId::Number(n) => self
                        .provider
                        .state_by_block_number_or_tag(n)
                        .map(|p| Either::Left(p)),
                    BlockId::Hash(h) => match self.provider.history_by_block_hash(h.into()) {
                        Ok(s) => Ok(Either::Left(s)),
                        Err(_) => self
                            .provider
                            .tree
                            .pending_state_provider(h.into())
                            .map(|p| Either::Right(p)),
                    },
                },
                None => self.provider.latest().map(|p| Either::Left(p)),
            }
            .map_err(|e: ProviderError| {
                ErrorObjectOwned::owned::<String>(
                    -32071,
                    "error getting state provider",
                    Some(e.to_string()),
                )
            })?;

        let mut hm = HashMap::new();
        for address in addresses.iter() {
            hm.insert(
                address.clone(),
                match state_provider {
                    Either::Left(ref provider) => {
                        provider.basic_account(address.clone()).map_err(|e| {
                            ErrorObjectOwned::owned::<String>(
                                -32072,
                                "error getting account info",
                                Some(e.to_string()),
                            )
                        })?
                    }
                    Either::Right(ref provider) => {
                        provider.state().account(address).flatten() /* .ok_or_else(|| {
                                                                        ErrorObjectOwned::owned::<String>(
                                                                            -32072,
                                                                            "error getting account info from pending state provider",
                                                                            Some(address.to_string()),
                                                                        )
                                                                    })? */
                    }
                },
            );
        }

        Ok(hm)
    }

    fn ping(&self) -> RpcResult<String> {
        Ok("pong".to_string())
    }

    fn add_genesis_block(&self, info: GenesisInfo) -> RpcResult<ChainSpec> {
        let GenesisInfo {
            accounts,
            shadows,
            timestamp,
        } = info;
        // clean database & static files
        // TODO: gate this fn so that it only works/shows up in dev mode
        let static_file_provider = self.provider.static_file_provider();
        for segment in StaticFileSegment::iter() {
            let static_files =
                iter_static_files(static_file_provider.directory()).map_err(|e| {
                    ErrorObjectOwned::owned::<String>(
                        -32073,
                        "error getting database provider",
                        Some(e.to_string()),
                    )
                })?;

            if let Some(segment_static_files) = static_files.get(&segment) {
                for (block_range, _) in segment_static_files {
                    static_file_provider
                        .delete_jar(segment, find_fixed_range(block_range.start()))
                        .map_err(|e| {
                            ErrorObjectOwned::owned::<String>(
                                -32073,
                                "error getting database provider",
                                Some(e.to_string()),
                            )
                        })?;
                }
            }
        }

        for table in Tables::ALL {
            table
                .view(&ClearViewer {
                    db: self.provider.database.db_ref(),
                })
                .map_err(|e| {
                    ErrorObjectOwned::owned::<String>(
                        -32073,
                        "error getting database provider",
                        Some(e.to_string()),
                    )
                })?;
        }

        // write the new genesis json, boot from it.
        // code taken from reth/crates/storage/db-common/src/init.rs:init_genesis
        let mut chain = (*self.provider.chain_spec()).clone();
        chain.genesis.timestamp = timestamp;

        // StateProviderDatabase implements the required DatabaseRef trait
        let db = StateProviderDatabase::new(self.provider.latest().unwrap());
        let mut cache_db = CacheDB::new(db);
        let mut journaled_state = JournaledState::new(SpecId::LATEST, HashSet::new());

        // TODO: inhereting alloc from the loaded chain requires that we add reth state resets between runs in a single erlang shell
        // otherwise the 'new' genesis alloc will have different values
        // see the commented out reth_node ! {'EXIT', self(), normal} call in ar_weave:init/3
        // let mut genesis_accounts: BTreeMap<Address, GenesisAccount> = chain.genesis.alloc.clone();

        let mut genesis_accounts: BTreeMap<Address, GenesisAccount> = BTreeMap::new();
        accounts.iter().for_each(|(k, v)| {
            genesis_accounts.insert(k.clone(), v.clone());
        });

        // load genesis accounts into cache (memory) db, then apply shadows
        // in future we can extend this to evaluate EVM txs for the genesis block, if needed.
        for (address, account) in genesis_accounts
            .iter()
            .map(|(addr, acc)| (addr.clone(), acc.clone()))
        {
            let bytecode = Bytecode::new_raw(account.code.unwrap_or_default());
            cache_db.insert_account_info(
                address,
                AccountInfo {
                    balance: account.balance,
                    nonce: account.nonce.unwrap_or(0),
                    code_hash: bytecode.clone().hash_slow(),
                    code: Some(bytecode),
                    stake: account.stake,
                    commitments: account.commitments,
                    last_tx: account.last_tx,
                    mining_permission: account.mining_permission,
                },
            );
            for (slot, value) in account.storage.unwrap_or_default() {
                let res = cache_db.insert_account_storage(address, slot.into(), value.into());
            }
        }
        // apply all shadows
        for shadow in shadows.unwrap_or(Shadows::new(vec![])) {
            let res = apply_shadow(shadow, &mut journaled_state, &mut cache_db);
        }

        // commit state changes
        let (state, _logs) = journaled_state.finalize();
        debug!("Evaluated genesis with state: {:#?}", &state);
        cache_db.commit(state);

        let mut after_btree: BTreeMap<Address, GenesisAccount> = BTreeMap::new();

        cache_db.accounts.iter().for_each(|(addr, acc)| {
            let info = &acc.info;
            let genesis_account = GenesisAccount {
                nonce: Some(info.nonce),
                balance: info.balance,
                code: info.code.clone().map(|c| c.bytes()),
                storage: {
                    let mut btree: BTreeMap<FixedBytes<32>, FixedBytes<32>> = BTreeMap::new();
                    acc.storage.iter().for_each(|(k, v)| {
                        // todo: fix this, not even sure why this is required.
                        let nk = <Uint<256, 4> as Into<FixedBytes<32>>>::into(*k);
                        let nv = <Uint<256, 4> as Into<FixedBytes<32>>>::into(*v);
                        btree.insert(nk, nv);
                    });
                    Some(btree)
                },
                private_key: genesis_accounts.get(addr).unwrap().private_key,
                commitments: info.commitments.clone(),
                stake: info.stake,
                mining_permission: info.mining_permission,
                last_tx: info.last_tx,
            };
            after_btree.insert(addr.clone(), genesis_account);
        });

        // overwrite alloc
        chain.genesis.alloc = after_btree;

        // trigger reload
        // todo: redo this Arc<Mutex<...>> nonsense, ideally with a oneshot channel
        let v = self.irys_ext.0.lock().map_err(|e| {
            ErrorObjectOwned::owned::<String>(
                -32080,
                "error locking reload channel",
                Some(e.to_string()),
            )
        })?;

        let hash = chain.genesis_hash();
        let header = serde_json::to_string(&chain.sealed_genesis_header())
            .expect("Unable to serialize genesis header");
        error!("Header: {}", &header);
        error!("Written genesis block (hash: {}), reloading...", &hash);

        // WARNING: RACE CONDITION
        // the reload operation doesn't wait for the reponse response of this method/RPC call to finish, it *does* have a 500ms delay, but that might not be
        // sufficient in certain conditions
        match &v.reload {
            Some(tx) => {
                let _res = tx.send(ReloadPayload::ReloadConfig(chain.clone()));
            }
            None => (),
        }

        Ok(chain.clone())
    }

    fn test_apply_shadow(&self, parent: BlockId, shadow: ShadowTx) -> RpcResult<ShadowReceipt> {
        let block_hash = &self
            .provider
            .block_hash_for_id(parent)
            .map_err(|e| {
                ErrorObjectOwned::owned::<String>(
                    -32090,
                    "error acquiring database state for block",
                    Some(e.to_string()),
                )
            })?
            .ok_or::<ErrorObjectOwned>(ErrorObjectOwned::owned::<String>(
                -32091,
                "Invalid block ID",
                None,
            ))?;

        let state_provider = &self
            .provider
            .state_by_block_hash(*block_hash)
            .map_err(|e| {
                ErrorObjectOwned::owned::<String>(
                    -32090,
                    "error acquiring database state for block",
                    Some(e.to_string()),
                )
            })?;
        let state = StateProviderDatabase::new(state_provider);
        let mut cached_reads = CachedReads::default();
        let mut db = State::builder()
            .with_database_ref(cached_reads.as_db(state))
            .with_bundle_update()
            .build();
        let mut journaled_state = JournaledState::new(SpecId::LATEST, HashSet::new());
        // let res = apply_shadow(shadow, &mut journaled_state, &mut db);
        let res = simulate_apply_shadow_thin(shadow, &mut journaled_state, &mut db);
        return Ok(res.map_err(|e| {
            ErrorObjectOwned::owned::<String>(-32091, "error executing shadow", Some(e.to_string()))
        })?);
        // OLD - used to build a full EVM env (lol) to execute the shadow //

        // let block_hash = parent
        //     .as_block_hash()
        //     .expect("unable to get blockId as hash");
        // let state_provider = &self.provider.state_by_block_hash(block_hash)?;
        // let state = StateProviderDatabase::new(state_provider);
        // let mut cached_reads = CachedReads::default();
        // let mut block_env = BlockEnv::default();
        // let mut cfg = CfgEnvWithHandlerCfg::default()
        //     & self
        //         .provider
        //         .fill_block_env_at(&mut block_env, BlockHashOrNumber::Hash(block_hash));
        // // &self
        // //     .provider
        // //     .fill_cfg_env_at(cfg, BlockHashOrNumber::Hash(block_hash), evm_config);

        // let hash = self
        //     .convert_number(at)?
        //     .ok_or(ProviderError::HeaderNotFound(at))?;
        // let header = self
        //     .header(&hash)?
        //     .ok_or(ProviderError::HeaderNotFound(at))?;
        // let total_difficulty = &self
        //     .provider
        //     .header_td_by_number(header.number)?
        //     .ok_or_else(|| ProviderError::HeaderNotFound(header.number.into()))?;
        // EvmConfig::fill_cfg_env(cfg, &self.chain_spec, header, total_difficulty);

        // let mut db = State::builder()
        //     .with_database_ref(cached_reads.as_db(state))
        //     .with_bundle_update()
        //     .build();
        // let mut evm = revm::Evm::builder()
        //     .with_db(&mut db)
        //     .with_env_with_handler_cfg(EnvWithHandlerCfg::new_with_cfg_env(
        //         initialized_cfg.clone(),
        //         block_env.clone(),
        //         // tx_env_with_recovered(&tx),
        //         Default::default(),
        //     ))
        //     .build();
    }

    async fn create_eth_tx(&self, private_key: B256) -> RpcResult<Bytes> {
        // create a new tx
        // let wallet = LocalWallet::from(private_key);
        let wallet = LocalWallet::from_bytes(&private_key).map_err(|e| {
            ErrorObjectOwned::owned::<String>(-32091, "error executing shadow", Some(e.to_string()))
        })?;
        debug!("Creating tx with owner: {}", wallet.address());

        let account = self
            .get_account(wallet.address(), Some(BlockId::latest()))?
            .ok_or(ErrorObjectOwned::owned::<String>(
                -32091,
                "error executing shadow",
                None,
            ))?;
        let tx = tx(4096, None, account.nonce);

        let signed = TransactionTestContext::sign_tx(wallet, tx).await;
        let raw_tx: Bytes = signed.encoded_2718().into();
        return Ok(raw_tx);
    }

    fn to_address(&self, private_key: B256) -> RpcResult<Address> {
        dbg!(&private_key.encode_hex());
        let wallet = LocalWallet::from_bytes(&private_key).map_err(|e| {
            ErrorObjectOwned::owned::<String>(
                -32091,
                "error resolving address for wallet",
                Some(e.to_string()),
            )
        })?;
        return Ok(wallet.address());
    }
}

// #[derive(Debug, Clone, Copy, Default, clap::Args)]
// pub struct RethCliIrysExt {
//     #[arg(long, default_value_t = true)]
//     pub enable_irys_ext: bool,
// }

#[macro_export]
macro_rules! vec_of_strings {
    ($($x:expr),*) => (vec![$($x.to_string()),*]);
}

pub fn main() -> eyre::Result<()> {
    let mut os_args: Vec<String> = std::env::args().collect();
    let bp = os_args.remove(0);
    // let mut args = vec_of_strings![
    //     "node",
    //     "-vvvvvv",
    //     "--disable-discovery",
    //     "--http",
    //     "--http.api",
    //     "debug,rpc,reth,eth"
    // ];

    // let mut args = vec_of_strings![
    //     "node",
    //     "-vvvvv",
    //     "--disable-discovery",
    //     "--http",
    //     "--http.api",
    //     "debug,rpc,reth,eth",
    //     "--datadir",
    //     "/workspaces/irys/.reth",
    //     "--log.file.directory",
    //     "/workspaces/irys/.reth/logs",
    //     "--log.file.format",
    //     "json",
    //     "--log.stdout.format",
    //     "json",
    //     "--log.file.filter",
    //     "trace"
    // ];

    let mut args = match false /* <- true if running manually :) */ {
        true => vec_of_strings![
            "node",
            "-vvvvv",
            "--instance", 
            "1",
            "--disable-discovery",
            "--http",
            "--http.api",
            "debug,rpc,reth,eth",
            "--datadir",
            "/workspaces/irys/.reth",
            "--log.file.directory",
            "/workspaces/irys/.reth/logs",
            "--log.file.format",
            "json",
            "--log.stdout.format",
            "json",
            "--log.stdout.filter",
            "trace",
            "--log.file.filter",
            "trace"
        ],
        false => vec_of_strings![
            "node",
            "-vvvvvv",
            "--disable-discovery",
            "--http",
            "--http.api",
            "debug,rpc,reth,eth"
        ],
    };

    args.insert(0, bp.to_string());
    args.append(&mut os_args);
    // dbg!(&args);
    info!("Running with args: {:#?}", &args);
    // loop is flawed, retains too much global set-once state
    let cli = Cli::<NoArgs>::parse_from(args.clone());
    let _guard = cli.logs.init_tracing()?;
    loop {
        let runner = CliRunner::default();

        // this loop allows us to 'reload' the reth node with a new config very quickly, without having to actually restart the entire process
        // mainly used to provide and then restart with a new genesis block.
        // TODO: extract run_command_until_exit to re-use async context to prevent global thread pool error on reload

        let exit_reason = runner.run_command_until_exit(|ctx| run_custom_node(ctx, cli.clone()))?;
        match exit_reason {
            NodeExitReason::Normal => break,
            NodeExitReason::Reload(_) => {
                trace!("reloading node!");
            } // NodeExitReason::Reload(payload) => match payload {
              //     ReloadPayload::ReloadConfig(chain_spec) => {
              //         // sleep(Duration::from_millis(500)).await;
              //         let ser = serde_json::to_string_pretty(&chain_spec)?;
              //         let pb =
              //             PathBuf::from(launched.node.data_dir.data_dir().join("dev_genesis.json"));
              //         let mut f = File::create(pb)?;
              //         f.write_all(ser.as_bytes())?;
              //     }
              // },
        }
    }
    Ok(())
}

pub fn get_custom_node<Ext>(
    ctx: CliContext,
    node_command: NodeCommand<Ext>,
) -> Result<
    WithLaunchContext<
        NodeBuilderWithComponents<
            FullNodeTypesAdapter<
                EthereumNode,
                Arc<DatabaseEnv>,
                BlockchainProvider<Arc<DatabaseEnv>>,
            >,
            ComponentsBuilder<
                FullNodeTypesAdapter<
                    EthereumNode,
                    Arc<DatabaseEnv>,
                    BlockchainProvider<Arc<DatabaseEnv>>,
                >,
                EthereumPoolBuilder,
                reth_node_ethereum::node::EthereumPayloadBuilder,
                EthereumNetworkBuilder,
                EthereumExecutorBuilder,
            >,
        >,
    >,
    eyre::Error,
>
where
    Ext: clap::Args + Clone + fmt::Debug,
{
    // if with_unused_ports {
    //     node_config = node_config.with_unused_ports();
    // }

    let NodeCommand {
        datadir,
        config,
        chain,
        metrics,
        instance,
        with_unused_ports,
        network,
        rpc,
        txpool,
        builder,
        debug,
        db,
        dev,
        pruning,
        ext,
    } = node_command;

    let mut node_config = NodeConfig {
        config,
        chain,
        metrics,
        instance,
        network: network.clone(),
        rpc: rpc.clone(),
        txpool,
        builder,
        debug,
        db,
        dev,
        pruning,
    };

    let chain_spec =
        get_chain_spec_with_path(vec![], &datadir.unwrap_or_default().as_ref(), dev.dev);

    node_config = node_config.with_chain(chain_spec);

    let data_dir = datadir.unwrap_or_chain_default(node_config.chain.chain);
    // let mut rpc_config = RpcServerArgs::default().with_http();
    // rpc_config.ipcdisable = true;

    let db_path = data_dir.db();

    tracing::info!(target: "reth::cli", path = ?db_path, "Opening database");
    let database =
        Arc::new(init_db(db_path.clone(), node_config.db.database_args())?.with_metrics());

    // let node_config = NodeConfig::test()
    //     .dev()
    //     .with_rpc(rpc_config)
    //     .with_network(network)
    //     .with_chain(chain_spec);

    node_config =/*  node_config.with_chain(chain_spec) */node_config.with_rpc(rpc.clone());

    let node = EthereumNode::default();
    // let components = node
    //     .components_builder()
    //     .payload(CustomPayloadBuilder::default());
    // components.payload();

    Ok(NodeBuilder::new(node_config)
        // .testing_node(ctx.task_executor)
        .with_database(database)
        .with_launch_context(ctx.task_executor, data_dir)
        // .with_components(node)
        .node(node)
        // .with_types::<EthereumNode>()
        // .with_components(EthereumNode::components().payload(CustomPayloadBuilder::default()))
        // .with_components(EthereumNode::components().payload(CustomPayloadBuilder::default()))
        .extend_rpc_modules(move |ctx| {
            // let node = ctx.node().clone();
            let provider = ctx.provider().clone();

            let irys_ext = ctx.node().components.irys_ext.clone();
            let network = ctx.network().clone();
            // dbg!(serde_json::to_string(network.peer_id()).expect("unable to serialize addr"))
            // let rpc= ctx.
            // provider.database.db.begin_rw_txn()
            let ext = AccountStateExt {
                provider,
                irys_ext,
                network,
            };
            ctx.modules.merge_configured(ext.into_rpc())?;
            println!("extension added!");
            Ok(())
        }))
}

async fn run_custom_node<Ext>(ctx: CliContext, cli: Cli<Ext>) -> eyre::Result<NodeExitReason>
where
    Ext: clap::Args + Clone + fmt::Debug,
{
    // from reth/bin/reth/src/commands/node/mod.rs:137
    let matched_cmd = match cli.command {
        Commands::Node(command) => Some(command),
        _ => None,
    };

    let node_command = matched_cmd.expect("unable to get cmd_cfg");

    let NodeCommand {
        config,
        chain,
        metrics,
        instance,
        network,
        rpc,
        txpool,
        builder,
        debug,
        db,
        dev,
        pruning,
        ..
    } = node_command.clone();

    let node_config = NodeConfig {
        config,
        chain,
        metrics,
        instance,
        network: network.clone(),
        rpc: rpc.clone(),
        txpool,
        builder,
        debug,
        db,
        dev,
        pruning,
    };

    // tracing & metric guards

    let _prom_recorder = node_config.install_prometheus_recorder()?;
    // must configure logging before tracing init
    let node = get_custom_node(ctx, node_command)?;
    let launcher = CustomNodeLauncher::new(node.task_executor, node.data_dir);
    let launched = node.builder.launch_with(launcher).await?;
    let exit_reason = launched.node_exit_future.await?;
    // TODO: fix
    if true
    /* launched.node.config.dev.dev */
    {
        match exit_reason.clone() {
            NodeExitReason::Normal => (),
            NodeExitReason::Reload(payload) => match payload {
                ReloadPayload::ReloadConfig(chain_spec) => {
                    // delay here so the genesis submission RPC reponse is able to make it back before the server dies

                    let ser = serde_json::to_string_pretty(&chain_spec)?;
                    let pb =
                        PathBuf::from(launched.node.data_dir.data_dir().join("dev_genesis.json"));
                    // remove_file(&pb)?;
                    let mut f = File::create(&pb)?;
                    f.write_all(ser.as_bytes())?;
                    info!("Written dev_genesis.json");
                    sleep(Duration::from_millis(500)).await;
                }
            },
        }
    }

    Ok(exit_reason)
}

pub async fn get_dev_node() -> eyre::Result<
    NodeHandle<
        NodeAdapter<
            FullNodeTypesAdapter<
                EthereumNode,
                Arc<DatabaseEnv>,
                BlockchainProvider<Arc<DatabaseEnv>>,
            >,
            Components<
                FullNodeTypesAdapter<
                    EthereumNode,
                    Arc<DatabaseEnv>,
                    BlockchainProvider<Arc<DatabaseEnv>>,
                >,
                Pool<
                    TransactionValidationTaskExecutor<
                        EthTransactionValidator<
                            BlockchainProvider<Arc<DatabaseEnv>>,
                            EthPooledTransaction,
                        >,
                    >,
                    CoinbaseTipOrdering<EthPooledTransaction>,
                    DiskFileBlobStore,
                >,
                EthEvmConfig,
                EthExecutorProvider,
            >,
        >,
    >,
> {
    // don't use AsyncCliRunner here as it spawns it's own nested runtime, but get a handle to the current runtime and use that.
    // let AsyncCliRunner { context, .. } = AsyncCliRunner::new()?;
    let tokio_handle = Handle::try_current()?;
    let task_manager = TaskManager::new(tokio_handle.clone());
    let task_executor = task_manager.executor();
    let context = CliContext { task_executor };
    // trick reth
    let args = vec_of_strings!["reth", "node", "--dev", "-vvvvv"];
    let cli = Cli::<NoArgs>::parse_from(args);
    let matched_cmd = match cli.command {
        Commands::Node(command) => Some(command),
        _ => None,
    };

    let node_command = matched_cmd.expect("unable to get cmd_cfg");
    let node = get_custom_node(context, node_command)?;
    let launcher = CustomNodeLauncher::new(node.task_executor, node.data_dir);

    let launched = node.builder.launch_with(launcher).await?;
    Ok(launched)
}
