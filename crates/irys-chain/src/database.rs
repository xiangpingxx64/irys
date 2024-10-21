use crate::{
    config::get_data_dir,
    tables::{IrysBlockHeaders, Tables},
};
use irys_types::{IrysBlockHeader, H256};
use reth::prometheus_exporter::install_prometheus_recorder;
use reth_db::transaction::DbTxMut;
use reth_db::{
    create_db as reth_create_db,
    mdbx::{DatabaseArguments, DatabaseFlags, MaxReadTransactionDuration},
    ClientVersion, Database, DatabaseEnv, DatabaseError,
};
use reth_db::{transaction::DbTx, TableType};
use reth_primitives::revm_primitives::B256;

/// Opens up an existing database or creates a new one at the specified path. Creates tables if
/// necessary. Read/Write mode.
pub fn open_or_create_db(cli_args: &str) -> eyre::Result<DatabaseEnv> {
    let args = DatabaseArguments::new(ClientVersion::default())
        .with_max_read_transaction_duration(Some(MaxReadTransactionDuration::Unbounded));

    // Register the prometheus recorder before creating the database,
    // because database init needs it to register metrics.
    let _ = install_prometheus_recorder();

    let db_path = get_data_dir();
    let db = reth_create_db(db_path.clone(), args)?.with_metrics_and_tables(Tables::ALL);

    let block_header = IrysBlockHeader::new();
    put_block(&db, &block_header).unwrap();

    Ok(db)
}

pub fn put_block(db: &DatabaseEnv, block: &IrysBlockHeader) -> Result<(), DatabaseError> {
    let value = block;
    let key: B256 = B256::from(value.block_hash.0);

    let tx = db.tx_mut().expect("create a mutable tx");
    tx.put::<IrysBlockHeaders>(key, value.clone().into())?;
    tx.commit()?;
    Ok(())
}
