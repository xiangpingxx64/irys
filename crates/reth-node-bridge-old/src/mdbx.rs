// ===========================================
// Old MDBX code used for the initial NIF, left here for future reference
// ===========================================

use reth_db::create_db;
use reth_db::database::Database;
use reth_db::transaction::{DbTx, DbTxMut};
use reth_db::{
    mdbx::{Result as MDBXResult, TableObject, WriteFlags},
    DatabaseEnv, DatabaseError, PlainAccountState,
};
use reth_primitives::{Account, Address};
use std::{env::VarError, path::Path};

pub fn main() -> eyre::Result<()> {
    // Opens a RO handle to the database file.
    // TODO: Should be able to do `ProviderFactory::new_with_db_path_ro(...)` instead of
    //  doing in 2 steps.

    // let tx = db
    //     .begin_ro_txn()
    //     .map_err(|e| DatabaseError::InitTx(e.into()))?;
    // tx.get(dbi, key)
    // // let db = open_db()?;
    // db.create_tables().expect("unable to create DB tables");
    // Instantiate a provider factory for Ethereum mainnet using the provided DB.
    // TODO: Should the DB version include the spec so that you do not need to specify it here?
    // let spec = ChainSpecBuilder::mainnet().build();
    // let factory = ProviderFactory::new(db, spec.into(), db_path.join("static_files"))?;

    // // This call opens a RO transaction on the database. To write to the DB you'd need to call
    // // the `provider_rw` function and look for the `Writer` variants of the traits.
    // let provider = factory.provider()?;
    // let tx = provider.tx_mut()
    dbg!("running");
    let db = open_db()?;
    // let tx = db.begin_ro_txn().expect("failed to start RW tx");
    // dbg!("started RW tx");
    let dbi = get_dbi(&db);
    dbg!(dbi);

    put(&db, dbi, [69], "test", WriteFlags::default());

    // let key1 = Address::with_last_byte(1);
    // let key2 = Address::with_last_byte(2);
    // let key3 = Address::with_last_byte(3);

    // let mut cursor = tx.cursor_write::<PlainAccountState>().unwrap();

    dbg!("put");

    let tx = db.begin_ro_txn().expect("failed to start RW tx");
    let read_res: Option<Vec<u8>> = tx
        .get::<Vec<u8>>(dbi, &[69])
        .expect("Failed to read from database");
    dbg!("read");

    dbg!(read_res);
    // provider.basic_account("hello world");
    // // Run basic queries against the DB
    // let block_num = 100;
    // header_provider_example(&provider, block_num)?;
    // block_provider_example(&provider, block_num)?;
    // txs_provider_example(&provider)?;
    // receipts_provider_example(&provider)?;

    // // Closes the RO transaction opened in the `factory.provider()` call. This is optional and
    // // would happen anyway at the end of the function scope.
    // drop(provider);

    // // Run the example against latest state
    // state_provider_example(factory.latest()?)?;

    // // Run it with historical state
    // state_provider_example(factory.history_by_block_number(block_num)?)?;

    Ok(())
}

pub fn open_db() -> Result<DatabaseEnv, eyre::Error> {
    let db_path = std::env::var("RETH_DB_PATH").or::<VarError>(Ok("reth-db".to_string()))?;
    let db_path = Path::new(&db_path);
    // Ok(init_db(db_path.join("db").as_path(), Default::default())?)
    Ok(create_db(db_path.join("db").as_path(), Default::default())?)
}
// from reth/crates/storage/db/src/implementation/mdbx/mod.rs

// const ERROR_DB_CREATION: &str = "Not able to create the mdbx file.";
const ERROR_OPEN_DB: &str = "Unable to open database.";
const ERROR_PUT: &str = "Not able to insert value into table.";
// const ERROR_APPEND: &str = "Not able to append the value to the table.";
// const ERROR_UPSERT: &str = "Not able to upsert the value to the table.";
// const ERROR_GET: &str = "Not able to get value from table.";
// const ERROR_DEL: &str = "Not able to delete from table.";
// const ERROR_COMMIT: &str = "Not able to commit transaction.";
// const ERROR_RETURN_VALUE: &str = "Mismatching result.";
const ERROR_INIT_TX: &str = "Failed to create a MDBX transaction.";
// const ERROR_ETH_ADDRESS: &str = "Invalid address.";

pub fn put(
    db: &DatabaseEnv,
    dbi: u32, /* ffi::MDBX_dbi */
    key: impl AsRef<[u8]>,
    value: impl AsRef<[u8]>,
    flags: WriteFlags,
) {
    let tx = db.begin_rw_txn().expect(ERROR_INIT_TX);
    // let dbi = tx.open_db(None);
    // // db.tx_mut()
    tx.put(dbi, key, value, flags).expect(ERROR_PUT);
    tx.commit();
}

pub fn get_dbi(db: &DatabaseEnv) -> u32 {
    let tx = db.begin_ro_txn().expect(ERROR_INIT_TX);
    let default_handle = tx.open_db(None).expect(ERROR_OPEN_DB);
    default_handle.dbi()
}

pub fn get<Key>(
    db: DatabaseEnv,
    dbi: u32, /* ffi::MDBX_dbi */
    key: &[u8],
) -> MDBXResult<Option<Key>>
where
    Key: TableObject,
{
    let tx = db.begin_ro_txn().expect(ERROR_INIT_TX);
    tx.get::<Key>(dbi, key)
}

pub fn get_account(db: DatabaseEnv, account: Address) -> Result<Option<Account>, DatabaseError> {
    let tx = db.tx().expect(ERROR_INIT_TX);
    tx.get::<PlainAccountState>(account)
}

pub fn update_account(
    db: DatabaseEnv,
    address: Address,
    state: Account,
) -> Result<(), DatabaseError> {
    let tx = db.tx_mut().expect(ERROR_INIT_TX);
    tx.put::<PlainAccountState>(address, state)
}

// /// The `HeaderProvider` allows querying the headers-related tables.
