use futures_core::future::LocalBoxFuture;

use crate::{Sqlite, SqliteConnection};
use sqlx_core::error::Error;
use sqlx_core::transaction::TransactionManager;

/// Implementation of [`TransactionManager`] for SQLite.
pub struct SqliteTransactionManager;

impl TransactionManager for SqliteTransactionManager {
    type Database = Sqlite;

    fn begin(conn: &mut SqliteConnection) -> LocalBoxFuture<'_, Result<(), Error>> {
        Box::pin(conn.worker.begin())
    }

    fn commit(conn: &mut SqliteConnection) -> LocalBoxFuture<'_, Result<(), Error>> {
        Box::pin(conn.worker.commit())
    }

    fn rollback(conn: &mut SqliteConnection) -> LocalBoxFuture<'_, Result<(), Error>> {
        Box::pin(conn.worker.rollback())
    }

    fn start_rollback(conn: &mut SqliteConnection) {
        conn.worker.start_rollback().ok();
    }
}
