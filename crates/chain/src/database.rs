use std::path::Path;

use crate::tables::{IrysBlockHeaders, IrysTxHeaders, Tables};
use irys_types::{IrysBlockHeader, IrysTransactionHeader, H256};
use reth::prometheus_exporter::install_prometheus_recorder;
use reth_db::transaction::DbTx;
use reth_db::transaction::DbTxMut;
use reth_db::{
    create_db as reth_create_db,
    mdbx::{DatabaseArguments, MaxReadTransactionDuration},
    ClientVersion, Database, DatabaseEnv, DatabaseError,
};

const ERROR_GET: &str = "Not able to get value from table.";
const ERROR_PUT: &str = "Not able to insert value into table.";

/// Opens up an existing database or creates a new one at the specified path. Creates tables if
/// necessary. Read/Write mode.
pub fn open_or_create_db<P: AsRef<Path>>(path: P) -> eyre::Result<DatabaseEnv> {
    let args = DatabaseArguments::new(ClientVersion::default())
        .with_max_read_transaction_duration(Some(MaxReadTransactionDuration::Unbounded));

    // Register the prometheus recorder before creating the database,
    // because database init needs it to register metrics.
    let _ = install_prometheus_recorder();
    let db = reth_create_db(path, args)?.with_metrics_and_tables(Tables::ALL);

    Ok(db)
}

pub fn insert_block(db: &DatabaseEnv, block: &IrysBlockHeader) -> Result<(), DatabaseError> {
    let value = block;
    let key = value.block_hash;

    db.update(|tx| {
        tx.put::<IrysBlockHeaders>(key, value.clone().into())
            .expect(ERROR_PUT)
    })
}

pub fn block_by_hash(
    db: &DatabaseEnv,
    block_hash: H256,
) -> Result<Option<IrysBlockHeader>, DatabaseError> {
    let key = block_hash;

    let result = db.view(|tx| tx.get::<IrysBlockHeaders>(key).expect(ERROR_GET))?;
    Ok(Some(IrysBlockHeader::from(result.unwrap())))
}

pub fn insert_tx(db: &DatabaseEnv, tx: &IrysTransactionHeader) -> Result<(), DatabaseError> {
    let key = tx.id;
    let value = tx;

    db.update(|tx| {
        tx.put::<IrysTxHeaders>(key, value.clone().into())
            .expect(ERROR_PUT)
    })
}

pub fn tx_by_txid(
    db: &DatabaseEnv,
    txid: &H256,
) -> Result<Option<IrysTransactionHeader>, DatabaseError> {
    let key = txid;
    let result = db.view(|tx| tx.get::<IrysTxHeaders>(*key).expect(ERROR_GET))?;
    Ok(Some(IrysTransactionHeader::from(result.unwrap())))
}

#[cfg(test)]
mod tests {
    use assert_matches::assert_matches;
    use irys_types::{IrysBlockHeader, IrysTransactionHeader};
    use tempfile::tempdir;

    use crate::{
        config::get_data_dir,
        database::{block_by_hash, insert_tx, tx_by_txid},
    };

    use super::{insert_block, open_or_create_db};

    #[test]
    fn insert_and_get_tests() {
        //let path = tempdir().unwrap();
        let path = get_data_dir();
        println!("TempDir: {:?}", path);

        let mut tx = IrysTransactionHeader::default();
        tx.id.0[0] = 2;
        let db = open_or_create_db(path).unwrap();

        // // Write a Tx
        // {
        //     let result = insert_tx(&db, &tx);
        //     println!("result: {:?}", result);
        //     assert_matches!(result, Ok(_));
        // }

        // // Read a Tx
        // {
        //     let result = tx_by_txid(&db, &tx.id);
        //     assert_eq!(result, Ok(Some(tx)));
        //     println!("result: {:?}", result.unwrap().unwrap());
        // }

        let mut block_header = IrysBlockHeader::new();
        block_header.block_hash.0[0] = 1;

        // Write a Block
        {
            let result = insert_block(&db, &block_header);
            println!("result: {:?}", result);
            assert_matches!(result, Ok(_));
        }

        // Read a Block
        {
            let result = block_by_hash(&db, block_header.block_hash);
            assert_eq!(result, Ok(Some(block_header)));
            println!("result: {:?}", result.unwrap().unwrap());
        }
    }

    // #[test]
    // fn insert_and_get_a_block() {
    //     //let path = tempdir().unwrap();
    //     let path = get_data_dir();
    //     println!("TempDir: {:?}", path);

    //     let mut block_header = IrysBlockHeader::new();
    //     block_header.block_hash.0[0] = 1;
    //     let db = open_or_create_db(path).unwrap();

    //     // Write a Block
    //     {
    //         let result = insert_block(&db, &block_header);
    //         println!("result: {:?}", result);
    //         assert_matches!(result, Ok(_));
    //     }

    //     // Read a Block
    //     {
    //         let result = block_by_hash(&db, block_header.block_hash);
    //         assert_eq!(result, Ok(Some(block_header)));
    //         println!("result: {:?}", result.unwrap().unwrap());
    //     }
    // }

    // #[test]
    // fn insert_and_get_tx() {
    //     //let path = tempdir().unwrap();
    //     let path = get_data_dir();
    //     println!("TempDir: {:?}", path);

    //     let mut tx = IrysTransactionHeader::default();
    //     tx.id.0[0] = 2;
    //     let db = open_or_create_db(path).unwrap();

    //     // Write a Tx
    //     {
    //         let result = insert_tx(&db, &tx);
    //         println!("result: {:?}", result);
    //         assert_matches!(result, Ok(_));
    //     }

    //     // Read a Tx
    //     {
    //         let result = tx_by_txid(&db, &tx.id);
    //         assert_eq!(result, Ok(Some(tx)));
    //         println!("result: {:?}", result.unwrap().unwrap());
    //     }
    // }
}
