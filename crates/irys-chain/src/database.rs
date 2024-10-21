use crate::{
    config::get_data_dir,
    tables::{IrysBlockHeaders, TableType, Tables},
};
use irys_types::{IrysBlockHeader, H256};
use reth::prometheus_exporter::install_prometheus_recorder;
use reth_db::transaction::DbTx;
use reth_db::transaction::DbTxMut;
use reth_db::{
    create_db as reth_create_db,
    mdbx::{DatabaseArguments, DatabaseFlags, MaxReadTransactionDuration},
    ClientVersion, Database, DatabaseEnv, DatabaseError,
};
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
    let db = reth_create_db(db_path.clone(), args)?; //.with_metrics();

    let tx = db
        .begin_rw_txn()
        .map_err(|e| DatabaseError::InitTx(e.into()))?;

    let flags = DatabaseFlags::default();

    for table in Tables::ALL {
        let flags = match table.table_type() {
            TableType::Table => DatabaseFlags::default(),
            TableType::DupSort => DatabaseFlags::DUP_SORT,
        };

        tx.create_db(Some(table.name()), flags)
            .map_err(|e| DatabaseError::CreateTable(e.into()))?;
    }

    tx.commit().map_err(|e| DatabaseError::Commit(e.into()))?;

    let block_header = IrysBlockHeader::new();

    let value = IrysBlockHeader::new();
    let key: B256 = B256::from(value.block_hash.0);

    let tx = db.tx_mut().expect("create a mutable tx");
    tx.put::<IrysBlockHeaders>(key, value.clone().into())
        .expect("expected to put");
    tx.commit().expect("expected to commit");

    Ok(db)
}
