use crate::any::{Any, AnyArguments, AnyQueryResult, AnyRow, AnyStatement, AnyTypeInfo};
use crate::describe::Describe;
use either::Either;
use futures_core::future::LocalBoxFuture;
use futures_core::stream::LocalBoxStream;
use std::fmt::Debug;

pub trait AnyConnectionBackend: std::any::Any + Debug + 'static {
    /// The backend name.
    fn name(&self) -> &str;

    /// Explicitly close this database connection.
    ///
    /// This method is **not required** for safe and consistent operation. However, it is
    /// recommended to call it instead of letting a connection `drop` as the database backend
    /// will be faster at cleaning up resources.
    fn close(self: Box<Self>) -> LocalBoxFuture<'static, crate::Result<()>>;

    /// Immediately close the connection without sending a graceful shutdown.
    ///
    /// This should still at least send a TCP `FIN` frame to let the server know we're dying.
    #[doc(hidden)]
    fn close_hard(self: Box<Self>) -> LocalBoxFuture<'static, crate::Result<()>>;

    /// Checks if a connection to the database is still valid.
    fn ping(&mut self) -> LocalBoxFuture<'_, crate::Result<()>>;

    /// Begin a new transaction or establish a savepoint within the active transaction.
    fn begin(&mut self) -> LocalBoxFuture<'_, crate::Result<()>>;

    fn commit(&mut self) -> LocalBoxFuture<'_, crate::Result<()>>;

    fn rollback(&mut self) -> LocalBoxFuture<'_, crate::Result<()>>;

    fn start_rollback(&mut self);

    /// The number of statements currently cached in the connection.
    fn cached_statements_size(&self) -> usize {
        0
    }

    /// Removes all statements from the cache, closing them on the server if
    /// needed.
    fn clear_cached_statements(&mut self) -> LocalBoxFuture<'_, crate::Result<()>> {
        Box::pin(async move { Ok(()) })
    }

    /// Forward to [`Connection::shrink_buffers()`].
    ///
    /// [`Connection::shrink_buffers()`]: method@crate::connection::Connection::shrink_buffers
    fn shrink_buffers(&mut self);

    #[doc(hidden)]
    fn flush(&mut self) -> LocalBoxFuture<'_, crate::Result<()>>;

    #[doc(hidden)]
    fn should_flush(&self) -> bool;

    #[cfg(feature = "migrate")]
    fn as_migrate(&mut self) -> crate::Result<&mut (dyn crate::migrate::Migrate + 'static)> {
        Err(crate::Error::Configuration(
            format!(
                "{} driver does not support migrations or `migrate` feature was not enabled",
                self.name()
            )
            .into(),
        ))
    }

    fn fetch_many<'q>(
        &'q mut self,
        query: &'q str,
        persistent: bool,
        arguments: Option<AnyArguments<'q>>,
    ) -> LocalBoxStream<'q, crate::Result<Either<AnyQueryResult, AnyRow>>>;

    fn fetch_optional<'q>(
        &'q mut self,
        query: &'q str,
        persistent: bool,
        arguments: Option<AnyArguments<'q>>,
    ) -> LocalBoxFuture<'q, crate::Result<Option<AnyRow>>>;

    fn prepare_with<'c, 'q: 'c>(
        &'c mut self,
        sql: &'q str,
        parameters: &[AnyTypeInfo],
    ) -> LocalBoxFuture<'c, crate::Result<AnyStatement<'q>>>;

    fn describe<'q>(&'q mut self, sql: &'q str)
        -> LocalBoxFuture<'q, crate::Result<Describe<Any>>>;
}
