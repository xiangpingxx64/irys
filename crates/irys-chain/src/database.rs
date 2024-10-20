use crate::config::get_data_dir;
use irys_types::{IrysBlockHeader, H256};
use reth_codecs::Compact;
use reth_db::{
    create_db as reth_create_db,
    mdbx::{DatabaseArguments, DatabaseFlags, MaxReadTransactionDuration},
    table::Table,
    ClientVersion, Database, DatabaseEnv, DatabaseError,
};
use reth_primitives::revm_primitives::B256;
use serde::{Deserialize, Serialize};

pub fn create_db(args: DatabaseArguments) -> eyre::Result<DatabaseEnv> {
    let db_path = get_data_dir();
    let database = reth_create_db(db_path.clone(), args)?.with_metrics();
    Ok(database)
}

/// Opens up an existing database or creates a new one at the specified path. Creates tables if
/// necessary. Read/Write mode.
pub fn open_or_create_db(cli_args: &str) -> eyre::Result<DatabaseEnv> {
    let args = DatabaseArguments::new(ClientVersion::default())
        .with_max_read_transaction_duration(Some(MaxReadTransactionDuration::Unbounded));

    let db = create_db(args)?;

    let tx = db
        .begin_rw_txn()
        .map_err(|e| DatabaseError::InitTx(e.into()))?;

    let flags = DatabaseFlags::default();

    tx.create_db(Some("IrysBlockHeaders"), flags)
        .map_err(|e| DatabaseError::CreateTable(e.into()))?;

    tx.commit().map_err(|e| DatabaseError::Commit(e.into()))?;

    let block_header = IrysBlockHeader::new();

    record_block_header(&db, block_header);

    Ok(db)
}

/// Adds wrapper structs for some primitive types so they can use `StructFlags` from Compact, when
/// used as pure table values.
macro_rules! add_wrapper_struct {
    ($(($name:tt, $wrapper:tt)),+) => {
        $(
            /// Wrapper struct so it can use StructFlags from Compact, when used as pure table values.
            #[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, Compact)]
            #[derive(arbitrary::Arbitrary)]
            //#[add_arbitrary_tests(compact)]
            pub struct $wrapper(pub $name);

            impl From<$name> for $wrapper {
                fn from(value: $name) -> Self {
                    $wrapper(value)
                }
            }

            impl From<$wrapper> for $name {
                fn from(value: $wrapper) -> Self {
                    value.0
                }
            }

            impl std::ops::Deref for $wrapper {
                type Target = $name;

                fn deref(&self) -> &Self::Target {
                    &self.0
                }
            }

        )+
    };
}

add_wrapper_struct!((IrysBlockHeader, CompactIrysBlockHeader));

// Define the IrysBlockHeaders table manually
#[derive(Clone, Debug, PartialEq)]
pub struct IrysBlockHeaders;

// impl Table for IrysBlockHeaders {
//     const NAME: &'static str = "IrysBlockHeaders";

//     type Key = B256; // The key is H256
//     type Value = CompactIrysBlockHeader; // The value is BlockHeader
// }

pub fn record_block_header(
    db: &DatabaseEnv,
    block_header: IrysBlockHeader,
) -> Result<(), DatabaseError> {
    // let tx = db.tx_mut()?;

    // let mut block_header_cursor = tx.cursor_write::<IrysBlockHeaders>()?;

    // block_header_cursor.upsert(
    //     block_header.block_hash,
    //     CompactIrysBlockHeader::from(block_header),
    // )?;
    // tx.commit()?;

    Ok(())
}
