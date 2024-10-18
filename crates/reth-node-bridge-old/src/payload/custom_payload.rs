//! This example shows how to implement a custom [EngineTypes].
//!
//! The [EngineTypes] trait can be implemented to configure the engine to work with custom types,
//! as long as those types implement certain traits.
//!
//! Custom payload attributes can be supported by implementing two main traits:
//!
//! [PayloadAttributes] can be implemented for payload attributes types that are used as
//! arguments to the `engine_forkchoiceUpdated` method. This type should be used to define and
//! _spawn_ payload jobs.
//!
//! [PayloadBuilderAttributes] can be implemented for payload attributes types that _describe_
//! running payload jobs.
//!
//! Once traits are implemented and custom types are defined, the [EngineTypes] trait can be
//! implemented:

use reth::{
    builder::{
        components::{ComponentsBuilder, PayloadServiceBuilder},
        node::NodeTypes,
        BuilderContext, FullNodeTypes, Node, NodeBuilder, PayloadBuilderConfig,
    },
    primitives::revm_primitives::{BlockEnv, CfgEnvWithHandlerCfg},
    providers::{CanonStateSubscriptions, StateProviderFactory},
    revm::database::StateProviderDatabase,
    tasks::TaskManager,
    transaction_pool::TransactionPool,
};
use reth_basic_payload_builder::{
    BasicPayloadJobGenerator, BasicPayloadJobGeneratorConfig, BuildArguments, BuildOutcome,
    PayloadBuilder, PayloadConfig,
};
use reth_node_api::{
    validate_version_specific_fields, EngineApiMessageVersion, EngineObjectValidationError,
    EngineTypes, PayloadAttributes, PayloadBuilderAttributes, PayloadOrAttributes,
};
use reth_node_core::{args::RpcServerArgs, node_config::NodeConfig};
use reth_node_ethereum::{
    node::{EthereumExecutorBuilder, EthereumNetworkBuilder, EthereumPoolBuilder},
    EthEngineTypes,
};
use reth_payload_builder::{
    error::PayloadBuilderError, EthBuiltPayload, EthPayloadBuilderAttributes, PayloadBuilderHandle,
    PayloadBuilderService,
};
use reth_primitives::{
    constants::EMPTY_SHADOWS_ROOT, proofs::calculate_shadows_root, Address, Chain, ChainSpec,
    Genesis, Header, PayloadAttributes as EthPayloadAttributes, Withdrawals, B256,
};
use reth_revm::state_change::apply_block_shadows;
use reth_rpc_types::{engine::PayloadId, irys_payload::ExecutionPayloadEnvelopeV1Irys, Withdrawal};
use reth_tracing::{RethTracer, Tracer};
use revm::State;
use serde::{Deserialize, Serialize};
use std::convert::Infallible;
use thiserror::Error;

use reth_basic_payload_builder::{
    commit_withdrawals, is_better_payload, pre_block_beacon_root_contract_call, WithdrawalsOutcome,
};
use reth_primitives::{
    constants::{eip4844::MAX_DATA_GAS_PER_BLOCK, BEACON_NONCE},
    eip4844::calculate_excess_blob_gas,
    proofs,
    revm::env::tx_env_with_recovered,
    Block, IntoRecoveredTransaction, Receipt, Receipts, EMPTY_OMMER_ROOT_HASH, U256,
};
use reth_provider::BundleStateWithReceipts;
use reth_transaction_pool::BestTransactionsAttributes;
use revm::{
    db::states::bundle_state::BundleRetention,
    primitives::{EVMError, EnvWithHandlerCfg, InvalidTransaction, ResultAndState},
    DatabaseCommit,
};
use tracing::{debug, trace};

use crate::payload::generator::EmptyBlockPayloadJobGenerator;

/// A custom payload attributes type.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CustomPayloadAttributes {
    /// An inner payload type
    #[serde(flatten)]
    pub inner: EthPayloadAttributes,
}

/// Custom error type used in payload attributes validation
#[derive(Debug, Error)]
pub enum CustomError {
    #[error("Custom field is not zero")]
    CustomFieldIsNotZero,
}

impl PayloadAttributes for CustomPayloadAttributes {
    fn timestamp(&self) -> u64 {
        self.inner.timestamp()
    }

    fn withdrawals(&self) -> Option<&Vec<Withdrawal>> {
        self.inner.withdrawals()
    }

    fn parent_beacon_block_root(&self) -> Option<B256> {
        self.inner.parent_beacon_block_root()
    }

    fn ensure_well_formed_attributes(
        &self,
        chain_spec: &ChainSpec,
        version: EngineApiMessageVersion,
    ) -> Result<(), EngineObjectValidationError> {
        validate_version_specific_fields(chain_spec, version, self.into())?;

        // // custom validation logic - ensure that the custom field is not zero
        // if self.custom == 0 {
        //     return Err(EngineObjectValidationError::invalid_params(
        //         CustomError::CustomFieldIsNotZero,
        //     ));
        // }

        Ok(())
    }
}

/// New type around the payload builder attributes type
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CustomPayloadBuilderAttributes(EthPayloadBuilderAttributes);

impl PayloadBuilderAttributes for CustomPayloadBuilderAttributes {
    type RpcPayloadAttributes = CustomPayloadAttributes;
    type Error = Infallible;

    fn try_new(parent: B256, attributes: CustomPayloadAttributes) -> Result<Self, Infallible> {
        Ok(Self(EthPayloadBuilderAttributes::new(
            parent,
            attributes.inner,
        )))
    }

    fn payload_id(&self) -> PayloadId {
        self.0.id
    }

    fn parent(&self) -> B256 {
        self.0.parent
    }

    fn timestamp(&self) -> u64 {
        self.0.timestamp
    }

    fn parent_beacon_block_root(&self) -> Option<B256> {
        self.0.parent_beacon_block_root
    }

    fn suggested_fee_recipient(&self) -> Address {
        self.0.suggested_fee_recipient
    }

    fn prev_randao(&self) -> B256 {
        self.0.prev_randao
    }

    fn withdrawals(&self) -> &Withdrawals {
        &self.0.withdrawals
    }

    fn cfg_and_block_env(
        &self,
        chain_spec: &ChainSpec,
        parent: &Header,
    ) -> (CfgEnvWithHandlerCfg, BlockEnv) {
        self.0.cfg_and_block_env(chain_spec, parent)
    }
}

/// Custom engine types - uses a custom payload attributes RPC type, but uses the default
/// payload builder attributes type.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[non_exhaustive]
pub struct CustomEngineTypes;

impl EngineTypes for CustomEngineTypes {
    type PayloadAttributes = CustomPayloadAttributes;
    type PayloadBuilderAttributes = CustomPayloadBuilderAttributes;
    type BuiltPayload = EthBuiltPayload;
    // type ExecutionPayloadV1 = ExecutionPayloadV1;
    // type ExecutionPayloadV2 = ExecutionPayloadEnvelopeV2;
    // type ExecutionPayloadV3 = ExecutionPayloadEnvelopeV3;
    type ExecutionPayloadEnvelopeV1Irys = ExecutionPayloadEnvelopeV1Irys;

    fn validate_version_specific_fields(
        chain_spec: &ChainSpec,
        version: EngineApiMessageVersion,
        payload_or_attrs: PayloadOrAttributes<'_, CustomPayloadAttributes>,
    ) -> Result<(), EngineObjectValidationError> {
        validate_version_specific_fields(chain_spec, version, payload_or_attrs)
    }
}

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
struct MyCustomNode;

/// Configure the node types
impl NodeTypes for MyCustomNode {
    type Primitives = ();
    // use the custom engine types
    type Engine = CustomEngineTypes;
}

/// Implement the Node trait for the custom node
///
/// This provides a preset configuration for the node
impl<N> Node<N> for MyCustomNode
where
    N: FullNodeTypes<Engine = CustomEngineTypes>,
{
    type ComponentsBuilder = ComponentsBuilder<
        N,
        EthereumPoolBuilder,
        CustomPayloadServiceBuilder,
        EthereumNetworkBuilder,
        EthereumExecutorBuilder,
    >;

    fn components_builder(self) -> Self::ComponentsBuilder {
        ComponentsBuilder::default()
            .node_types::<N>()
            .pool(EthereumPoolBuilder::default())
            .payload(CustomPayloadServiceBuilder::default())
            .network(EthereumNetworkBuilder::default())
            .executor(EthereumExecutorBuilder::default())
    }
}

/// A custom payload service builder that supports the custom engine types
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct CustomPayloadServiceBuilder;

impl<Node, Pool> PayloadServiceBuilder<Node, Pool> for CustomPayloadServiceBuilder
where
    Node: FullNodeTypes<Engine = CustomEngineTypes>,
    Pool: TransactionPool + Unpin + 'static,
{
    async fn spawn_payload_service(
        self,
        ctx: &BuilderContext<Node>,
        pool: Pool,
    ) -> eyre::Result<PayloadBuilderHandle<Node::Engine>> {
        let payload_builder = CustomPayloadBuilder::default();
        let conf = ctx.payload_builder_config();

        let payload_job_config = BasicPayloadJobGeneratorConfig::default()
            .interval(conf.interval())
            .deadline(conf.deadline())
            .max_payload_tasks(conf.max_payload_tasks())
            .extradata(conf.extradata_bytes());

        let payload_generator = BasicPayloadJobGenerator::with_builder(
            ctx.provider().clone(),
            pool,
            ctx.task_executor().clone(),
            payload_job_config,
            ctx.chain_spec(),
            payload_builder,
        );
        let (payload_service, payload_builder) =
            PayloadBuilderService::new(payload_generator, ctx.provider().canonical_state_stream());

        ctx.task_executor()
            .spawn_critical("payload builder service", Box::pin(payload_service));

        Ok(payload_builder)
    }
}

/// The type responsible for building custom payloads
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct CustomPayloadBuilder;

impl<Pool, Client> PayloadBuilder<Pool, Client> for CustomPayloadBuilder
where
    Client: StateProviderFactory,
    Pool: TransactionPool,
{
    type Attributes = CustomPayloadBuilderAttributes;
    type BuiltPayload = EthBuiltPayload;

    fn try_build(
        &self,
        args: BuildArguments<Pool, Client, Self::Attributes, Self::BuiltPayload>,
    ) -> Result<BuildOutcome<Self::BuiltPayload>, PayloadBuilderError> {
        // let BuildArguments {
        //     client,
        //     pool,
        //     cached_reads,
        //     config,
        //     cancel,
        //     best_payload,
        // } = args;

        // let PayloadConfig {
        //     initialized_block_env,
        //     initialized_cfg,
        //     parent_block,
        //     extra_data,
        //     attributes,
        //     chain_spec,
        // } = config;
        // let atrributes = attributes.0;

        // This reuses the default EthereumPayloadBuilder to build the payload
        // but any custom logic can be implemented here
        // reth_ethereum_payload_builder::EthereumPayloadBuilder::default().try_build(BuildArguments {
        //     client,
        //     pool,
        //     cached_reads,
        //     config: PayloadConfig {
        //         initialized_block_env,
        //         initialized_cfg,
        //         parent_block,
        //         extra_data,
        //         attributes: attributes.0,
        //         chain_spec,
        //     },
        //     cancel,
        //     best_payload,
        // })

        // let BuildArguments {
        //     client,
        //     pool,
        //     mut cached_reads,
        //     config,
        //     cancel,
        //     best_payload,
        // } = args;

        let BuildArguments {
            client,
            pool,
            mut cached_reads,
            config,
            cancel,
            best_payload,
        } = args;

        let state_provider = client.state_by_block_hash(config.parent_block.hash())?;
        let state = StateProviderDatabase::new(state_provider);
        let mut db = State::builder()
            .with_database_ref(cached_reads.as_db(state))
            .with_bundle_update()
            .build();
        let extra_data = config.extra_data();

        let PayloadConfig {
            initialized_block_env,
            initialized_cfg,
            parent_block,
            extra_data,
            attributes,
            chain_spec,
        } = config;
        let attribute = attributes.0;

        debug!(target: "payload_builder", id=%attribute.id, parent_hash = ?parent_block.hash(), parent_number = parent_block.number, "building new payload");
        let mut cumulative_gas_used = 0;
        let mut sum_blob_gas_used = 0;
        let block_gas_limit: u64 = initialized_block_env
            .gas_limit
            .try_into()
            .unwrap_or(u64::MAX);
        let base_fee = initialized_block_env.basefee.to::<u64>();

        let mut executed_txs = Vec::new();

        let mut best_txs = pool.best_transactions_with_attributes(BestTransactionsAttributes::new(
            base_fee,
            initialized_block_env
                .get_blob_gasprice()
                .map(|gasprice| gasprice as u64),
        ));

        let mut total_fees = U256::ZERO;

        let block_number = initialized_block_env.number.to::<u64>();

        // // apply eip-4788 pre block contract call
        // pre_block_beacon_root_contract_call(
        //     &mut db,
        //     &chain_spec,
        //     block_number,
        //     &initialized_cfg,
        //     &initialized_block_env,
        //     &attribute,
        // )?;

        let mut receipts = Vec::new();

        let mut evm = revm::Evm::builder()
            .with_db(&mut db)
            .with_env_with_handler_cfg(EnvWithHandlerCfg::new_with_cfg_env(
                initialized_cfg.clone(),
                initialized_block_env.clone(),
                // tx_env_with_recovered(&tx),
                Default::default(),
            ))
            .build();

        let shadow_exec =
            apply_block_shadows(Some(&attribute.shadows), &mut evm).expect("shadow exec failed :c");
        // commit changes
        let ss = evm.context.evm.inner.journaled_state.state.clone();
        evm.db_mut().commit(ss);
        // drop handle on db
        drop(evm);

        while let Some(pool_tx) = best_txs.next() {
            // ensure we still have capacity for this transaction
            if cumulative_gas_used + pool_tx.gas_limit() > block_gas_limit {
                // we can't fit this transaction into the block, so we need to mark it as invalid
                // which also removes all dependent transaction from the iterator before we can
                // continue
                best_txs.mark_invalid(&pool_tx);
                continue;
            }

            // check if the job was cancelled, if so we can exit early
            if cancel.is_cancelled() {
                return Ok(BuildOutcome::Cancelled);
            }

            // convert tx to a signed transaction
            let tx = pool_tx.to_recovered_transaction();

            // There's only limited amount of blob space available per block, so we need to check if
            // the EIP-4844 can still fit in the block
            if let Some(blob_tx) = tx.transaction.as_eip4844() {
                let tx_blob_gas = blob_tx.blob_gas();
                if sum_blob_gas_used + tx_blob_gas > MAX_DATA_GAS_PER_BLOCK {
                    // we can't fit this _blob_ transaction into the block, so we mark it as
                    // invalid, which removes its dependent transactions from
                    // the iterator. This is similar to the gas limit condition
                    // for regular transactions above.
                    trace!(target: "payload_builder", tx=?tx.hash, ?sum_blob_gas_used, ?tx_blob_gas, "skipping blob transaction because it would exceed the max data gas per block");
                    best_txs.mark_invalid(&pool_tx);
                    continue;
                }
            }

            // Configure the environment for the block.
            let mut evm = revm::Evm::builder()
                .with_db(&mut db)
                .with_env_with_handler_cfg(EnvWithHandlerCfg::new_with_cfg_env(
                    initialized_cfg.clone(),
                    initialized_block_env.clone(),
                    tx_env_with_recovered(&tx),
                ))
                .build();

            let ResultAndState { result, state } = match evm.transact() {
                Ok(res) => res,
                Err(err) => {
                    match err {
                        EVMError::Transaction(err) => {
                            if matches!(err, InvalidTransaction::NonceTooLow { .. }) {
                                // if the nonce is too low, we can skip this transaction
                                trace!(target: "payload_builder", %err, ?tx, "skipping nonce too low transaction");
                            } else {
                                // if the transaction is invalid, we can skip it and all of its
                                // descendants
                                trace!(target: "payload_builder", %err, ?tx, "skipping invalid transaction and its descendants");
                                best_txs.mark_invalid(&pool_tx);
                            }

                            continue;
                        }
                        err => {
                            // this is an error that we should treat as fatal for this attempt
                            return Err(PayloadBuilderError::EvmExecutionError(err));
                        }
                    }
                }
            };
            // drop evm so db is released.
            drop(evm);
            // commit changes
            db.commit(state);

            // add to the total blob gas used if the transaction successfully executed
            if let Some(blob_tx) = tx.transaction.as_eip4844() {
                let tx_blob_gas = blob_tx.blob_gas();
                sum_blob_gas_used += tx_blob_gas;

                // if we've reached the max data gas per block, we can skip blob txs entirely
                if sum_blob_gas_used == MAX_DATA_GAS_PER_BLOCK {
                    best_txs.skip_blobs();
                }
            }

            let gas_used = result.gas_used();

            // add gas used by the transaction to cumulative gas used, before creating the receipt
            cumulative_gas_used += gas_used;

            // Push transaction changeset and calculate header bloom filter for receipt.
            #[allow(clippy::needless_update)] // side-effect of optimism fields
            receipts.push(Some(Receipt {
                tx_type: tx.tx_type(),
                success: result.is_success(),
                cumulative_gas_used,
                logs: result.into_logs().into_iter().map(Into::into).collect(),
                ..Default::default()
            }));

            // update add to total fees
            let miner_fee = tx
                .effective_tip_per_gas(Some(base_fee))
                .expect("fee is always valid; execution succeeded");
            total_fees += U256::from(miner_fee) * U256::from(gas_used);

            // append transaction to the list of executed transactions
            executed_txs.push(tx.into_signed());
        }

        // check if we have a better block
        if !is_better_payload(best_payload.as_ref(), total_fees) {
            // can skip building the block
            return Ok(BuildOutcome::Aborted {
                fees: total_fees,
                cached_reads,
            });
        }

        let WithdrawalsOutcome {
            withdrawals_root,
            withdrawals,
        } = commit_withdrawals(
            &mut db,
            &chain_spec,
            attribute.timestamp,
            attribute.withdrawals,
        )?;

        // merge all transitions into bundle state, this would apply the withdrawal balance changes
        // and 4788 contract call
        db.merge_transitions(BundleRetention::PlainState);

        let bundle = BundleStateWithReceipts::new(
            db.take_bundle(),
            Receipts::from_vec(vec![receipts]),
            block_number,
        );
        let receipts_root = bundle
            .receipts_root_slow(block_number)
            .expect("Number is in range");
        let logs_bloom = bundle
            .block_logs_bloom(block_number)
            .expect("Number is in range");

        let shadows_root = calculate_shadows_root(&attribute.shadows);
        // calculate the state root
        let state_root = {
            let state_provider = db.database.0.inner.borrow_mut();
            state_provider.db.state_root(bundle.state())?
        };

        // create the block header
        let transactions_root = proofs::calculate_transaction_root(&executed_txs);

        // initialize empty blob sidecars at first. If cancun is active then this will
        let mut blob_sidecars = Vec::new();
        let mut excess_blob_gas = None;
        let mut blob_gas_used = None;

        // only determine cancun fields when active
        if chain_spec.is_cancun_active_at_timestamp(attribute.timestamp) {
            // grab the blob sidecars from the executed txs
            blob_sidecars = pool.get_all_blobs_exact(
                executed_txs
                    .iter()
                    .filter(|tx| tx.is_eip4844())
                    .map(|tx| tx.hash)
                    .collect(),
            )?;

            excess_blob_gas = if chain_spec.is_cancun_active_at_timestamp(parent_block.timestamp) {
                let parent_excess_blob_gas = parent_block.excess_blob_gas.unwrap_or_default();
                let parent_blob_gas_used = parent_block.blob_gas_used.unwrap_or_default();
                Some(calculate_excess_blob_gas(
                    parent_excess_blob_gas,
                    parent_blob_gas_used,
                ))
            } else {
                // for the first post-fork block, both parent.blob_gas_used and
                // parent.excess_blob_gas are evaluated as 0
                Some(calculate_excess_blob_gas(0, 0))
            };

            blob_gas_used = Some(sum_blob_gas_used);
        }

        let header = Header {
            parent_hash: parent_block.hash(),
            ommers_hash: EMPTY_OMMER_ROOT_HASH,
            shadows_root,
            beneficiary: initialized_block_env.coinbase,
            state_root,
            transactions_root,
            receipts_root,
            withdrawals_root,
            logs_bloom,
            timestamp: attribute.timestamp,
            mix_hash: attribute.prev_randao,
            nonce: BEACON_NONCE,
            base_fee_per_gas: Some(base_fee),
            number: parent_block.number + 1,
            gas_limit: block_gas_limit,
            difficulty: U256::ZERO,
            gas_used: cumulative_gas_used,
            extra_data,
            parent_beacon_block_root: attribute.parent_beacon_block_root,
            blob_gas_used,
            excess_blob_gas,
        };

        // seal the block
        let block = Block {
            header,
            body: executed_txs,
            ommers: vec![],
            withdrawals,
            shadows: None,
        };

        let sealed_block = block.seal_slow();
        debug!(target: "payload_builder", ?sealed_block, "sealed built block");

        let mut payload = EthBuiltPayload::new(attribute.id, sealed_block, total_fees, false);

        // extend the payload with the blob sidecars from the executed txs
        payload.extend_sidecars(blob_sidecars);

        Ok(BuildOutcome::Better {
            payload,
            cached_reads,
        })
    }

    fn build_empty_payload(
        client: &Client,
        config: PayloadConfig<Self::Attributes>,
    ) -> Result<Self::BuiltPayload, PayloadBuilderError> {
        let PayloadConfig {
            initialized_block_env,
            initialized_cfg,
            parent_block,
            extra_data,
            attributes,
            chain_spec,
        } = config;
        <reth_ethereum_payload_builder::EthereumPayloadBuilder  as PayloadBuilder<Pool,Client>>  ::build_empty_payload(client,
                                                                                                                       PayloadConfig { initialized_block_env, initialized_cfg, parent_block, extra_data, attributes: attributes.0, chain_spec }
        )
    }
}

impl<Node, Pool> PayloadServiceBuilder<Node, Pool> for CustomPayloadBuilder
where
    Node: FullNodeTypes<Engine = EthEngineTypes>,
    Pool: TransactionPool + Unpin + 'static,
{
    async fn spawn_payload_service(
        self,
        ctx: &BuilderContext<Node>,
        pool: Pool,
    ) -> eyre::Result<PayloadBuilderHandle<Node::Engine>> {
        tracing::info!("Spawning a custom payload builder");
        let conf = ctx.payload_builder_config();

        let payload_job_config = BasicPayloadJobGeneratorConfig::default()
            .interval(conf.interval())
            .deadline(conf.deadline())
            .max_payload_tasks(conf.max_payload_tasks())
            .extradata(conf.extradata_bytes());

        let payload_generator = EmptyBlockPayloadJobGenerator::with_builder(
            ctx.provider().clone(),
            pool,
            ctx.task_executor().clone(),
            payload_job_config,
            ctx.chain_spec().clone(),
            reth_ethereum_payload_builder::EthereumPayloadBuilder::default(),
        );

        let (payload_service, payload_builder) =
            PayloadBuilderService::new(payload_generator, ctx.provider().canonical_state_stream());

        ctx.task_executor()
            .spawn_critical("custom payload builder service", Box::pin(payload_service));

        Ok(payload_builder)
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let _guard = RethTracer::new().init()?;

    let tasks = TaskManager::current();

    // create optimism genesis with canyon at block 2
    let spec = ChainSpec::builder()
        .chain(Chain::mainnet())
        .genesis(Genesis::default())
        .london_activated()
        .paris_activated()
        .shanghai_activated()
        .build();

    // create node config
    let node_config = NodeConfig::test()
        .with_rpc(RpcServerArgs::default().with_http())
        .with_chain(spec);

    let handle = NodeBuilder::new(node_config)
        .testing_node(tasks.executor())
        .launch_node(MyCustomNode::default())
        .await
        .unwrap();

    println!("Node started");

    handle.node_exit_future.await?;
    Ok(())
}
