use reth_config::config::{PruneConfig, StageConfig};
use reth_db::database::Database;
use reth_evm::execute::BlockExecutorProvider;
use reth_exex::ExExManagerHandle;
use reth_provider::ProviderFactory;
use reth_stages::{
    sets::OfflineStages,
    stages::{ExecutionStage, FinishStage},
    Pipeline, StageSet,
};
use reth_static_file::StaticFileProducer;

pub async fn build_custom_pipeline<DB, Executor>(
    provider_factory: ProviderFactory<DB>,
    stage_config: &StageConfig,
    metrics_tx: reth_stages::MetricEventsSender,
    prune_config: Option<PruneConfig>,
    static_file_producer: StaticFileProducer<DB>,
    executor: Executor,
    exex_manager_handle: ExExManagerHandle,
) -> eyre::Result<Pipeline<DB>>
where
    DB: Database + Clone + 'static,
    Executor: BlockExecutorProvider,
{
    let builder = Pipeline::builder();

    let prune_modes = prune_config.map(|prune| prune.segments).unwrap_or_default();

    let pipeline = builder
        .with_metrics_tx(metrics_tx.clone())
        .add_stages(
            OfflineStages::new(executor.clone(), stage_config.clone(), prune_modes.clone()).set(
                ExecutionStage::new(
                    executor,
                    stage_config.execution.into(),
                    stage_config.execution_external_clean_threshold(),
                    prune_modes,
                    exex_manager_handle,
                )
                .with_metrics_tx(metrics_tx),
            ),
        )
        .add_stage(FinishStage)
        .build(provider_factory, static_file_producer);
    Ok(pipeline)
}

// #[derive(Debug, Default)]
// #[non_exhaustive]
// pub struct CustomOfflineStages<EF> {
//     /// Executor factory needs for execution stage
//     pub executor_factory: EF,
//     /// Configuration for each stage in the pipeline
//     stages_config: StageConfig,
//     /// Prune configuration for every segment that can be pruned
//     prune_modes: PruneModes,
// }

// // #[derive(Debug)]
// // pub struct CustomOnlineStages<Provider, H, B> {
// //     /// Sync gap provider for the headers stage.
// //     provider: Provider,
// //     /// The sync mode for the headers stage.
// //     // header_mode: HeaderSyncMode,
// //     /// The consensus engine used to validate incoming data.
// //     // consensus: Arc<dyn Consensus>,
// //     /// The block header downloader
// //     // header_downloader: H,
// //     /// The block body downloader
// //     // body_downloader: B,
// //     /// Configuration for each stage in the pipeline
// //     stages_config: StageConfig,
// // }

// impl<EF> CustomOfflineStages<EF> {
//     /// Create a new set of offline stages with default values.
//     pub fn new(executor_factory: EF, stages_config: StageConfig, prune_modes: PruneModes) -> Self {
//         Self { executor_factory, stages_config, prune_modes }
//     }
// }

// impl<E, DB> StageSet<DB> for CustomOfflineStages<E>
// where
//     E: BlockExecutorProvider,
//     DB: Database,
// {
//     fn builder(self) -> StageSetBuilder<DB> {
//         CustomExecutionStages::new(
//             self.executor_factory,
//             self.stages_config.clone(),
//             self.prune_modes.clone(),
//         )
//         .builder()
//         .add_set(HashingStages { stages_config: self.stages_config.clone() })
//         .add_set(HistoryIndexingStages {
//             stages_config: self.stages_config.clone(),
//             prune_modes: self.prune_modes,
//         })
//     }
// }

// /// A set containing all stages that are required to execute pre-existing block data.
// #[derive(Debug)]
// #[non_exhaustive]
// pub struct CustomExecutionStages<E> {
//     /// Executor factory that will create executors.
//     executor_factory: E,
//     /// Configuration for each stage in the pipeline
//     stages_config: StageConfig,
//     /// Prune configuration for every segment that can be pruned
//     prune_modes: PruneModes,
// }

// impl<E> CustomExecutionStages<E> {
//     /// Create a new set of execution stages with default values.
//     pub fn new(executor_factory: E, stages_config: StageConfig, prune_modes: PruneModes) -> Self {
//         Self { executor_factory, stages_config, prune_modes }
//     }
// }

// impl<E, DB> StageSet<DB> for CustomExecutionStages<E>
// where
//     DB: Database,
//     E: BlockExecutorProvider,
// {
//     fn builder(self) -> StageSetBuilder<DB> {
//         StageSetBuilder::default()
//             .add_stage(SenderRecoveryStage::new(self.stages_config.sender_recovery))
//             .add_stage(ExecutionStage::from_config(
//                 self.executor_factory,
//                 self.stages_config.execution,
//                 self.stages_config.execution_external_clean_threshold(),
//                 self.prune_modes,
//             ))
//     }
// }

// /// A set containing all stages that hash account state.
// #[derive(Debug, Default)]
// #[non_exhaustive]
// pub struct HashingStages {
//     /// Configuration for each stage in the pipeline
//     stages_config: StageConfig,
// }

// impl<DB: Database> StageSet<DB> for HashingStages {
//     fn builder(self) -> StageSetBuilder<DB> {
//         StageSetBuilder::default()
//             .add_stage(MerkleStage::default_unwind())
//             .add_stage(AccountHashingStage::new(
//                 self.stages_config.account_hashing,
//                 self.stages_config.etl.clone(),
//             ))
//             .add_stage(StorageHashingStage::new(
//                 self.stages_config.storage_hashing,
//                 self.stages_config.etl.clone(),
//             ))
//             .add_stage(MerkleStage::new_execution(self.stages_config.merkle.clean_threshold))
//     }
// }

// /// A set containing all stages that do additional indexing for historical state.
// #[derive(Debug, Default)]
// #[non_exhaustive]
// pub struct HistoryIndexingStages {
//     /// Configuration for each stage in the pipeline
//     stages_config: StageConfig,
//     /// Prune configuration for every segment that can be pruned
//     prune_modes: PruneModes,
// }

// impl<DB: Database> StageSet<DB> for HistoryIndexingStages {
//     fn builder(self) -> StageSetBuilder<DB> {
//         StageSetBuilder::default()
//             .add_stage(TransactionLookupStage::new(
//                 self.stages_config.transaction_lookup,
//                 self.stages_config.etl.clone(),
//                 self.prune_modes.transaction_lookup,
//             ))
//             .add_stage(IndexStorageHistoryStage::new(
//                 self.stages_config.index_storage_history,
//                 self.stages_config.etl.clone(),
//                 self.prune_modes.account_history,
//             ))
//             .add_stage(IndexAccountHistoryStage::new(
//                 self.stages_config.index_account_history,
//                 self.stages_config.etl.clone(),
//                 self.prune_modes.storage_history,
//             ))
//     }
// }
