use crate::config::get_data_dir;
use reth_db::{
    init_db,
    mdbx::{DatabaseArguments, MaxReadTransactionDuration},
    ClientVersion, DatabaseEnv,
};

pub fn create_db(args: DatabaseArguments) -> eyre::Result<DatabaseEnv> {
    let db_path = get_data_dir();
    let database = init_db(db_path.clone(), args)?.with_metrics();
    Ok(database)
}

/// Opens up an existing database or creates a new one at the specified path. Creates tables if
/// necessary. Read/Write mode.
pub fn open_or_create_db(cli_args: &str) -> eyre::Result<DatabaseEnv> {
    let args = DatabaseArguments::new(ClientVersion::default())
        .with_max_read_transaction_duration(Some(MaxReadTransactionDuration::Unbounded));

    let db = create_db(args)?;
    db.create_tables()?;
    Ok(db)
}
