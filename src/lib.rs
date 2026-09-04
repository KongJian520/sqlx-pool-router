//! # sqlx_pool_router
//!
//! A lightweight library for routing database operations to different SQLx PostgreSQL connection pools
//! based on whether they're read or write operations.
//!
//! This enables load distribution by routing read-heavy operations to read replicas while ensuring
//! write operations always go to the primary database.
//!
//! ## Features
//!
//! - **Zero-cost routing**: Trait-based design; a pool handle is one `Arc` clone
//! - **Type-safe routing**: Compile-time guarantees for read/write pool separation
//! - **Runtime pool replacement**: [`DbPools::replace`] atomically swaps the active pools
//!   and every existing clone of the `DbPools` observes the swap on its next call
//! - **Backward compatible**: `PgPool` implements `PoolProvider` for seamless integration
//! - **Flexible**: Use single pool or separate primary/replica pools
//! - **Test helpers**: [`TestDbPools`] for testing with `#[sqlx::test]`
//!
//! ## Quick Start
//!
//! ### Single Pool (Development)
//!
//! ```rust,no_run
//! use sqlx::PgPool;
//! use sqlx_pool_router::PoolProvider;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let pool = PgPool::connect("postgresql://localhost/mydb").await?;
//!
//! // PgPool implements PoolProvider automatically
//! let result: (i32,) = sqlx::query_as("SELECT 1")
//!     .fetch_one(pool.read())
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Read/Write Separation (Production)
//!
//! ```rust,no_run
//! use sqlx::postgres::PgPoolOptions;
//! use sqlx_pool_router::{DbPools, PoolProvider};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let primary = PgPoolOptions::new()
//!     .max_connections(5)
//!     .connect("postgresql://primary-host/mydb")
//!     .await?;
//!
//! let replica = PgPoolOptions::new()
//!     .max_connections(10)
//!     .connect("postgresql://replica-host/mydb")
//!     .await?;
//!
//! let pools = DbPools::with_replica(primary, replica);
//!
//! // Reads go to replica
//! let users: Vec<(i32, String)> = sqlx::query_as("SELECT id, name FROM users")
//!     .fetch_all(pools.read())
//!     .await?;
//!
//! // Writes go to primary
//! sqlx::query("INSERT INTO users (name) VALUES ($1)")
//!     .bind("Alice")
//!     .execute(pools.write())
//!     .await?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Replacing pools at runtime
//!
//! sqlx pools are fixed-size once built. When the right size changes at runtime
//! (for example when a per-role connection budget is re-divided across a changing
//! number of replicas) build a new pool and swap it in. Every component holding a
//! clone of the `DbPools` — request handlers, background daemons — picks up the
//! new pool on its next `.read()` / `.write()` call without being restarted.
//!
//! ```rust,no_run
//! use sqlx::postgres::PgPoolOptions;
//! use sqlx_pool_router::{DbPools, PoolProvider};
//!
//! # async fn example(pools: DbPools) -> Result<(), Box<dyn std::error::Error>> {
//! let smaller = PgPoolOptions::new()
//!     .max_connections(5)
//!     .connect_lazy("postgresql://primary-host/mydb")?;
//!
//! let (old_primary, old_replica) = pools.replace(smaller, None);
//!
//! // Drain the previous pools in the background: `close()` closes idle
//! // connections now and the checked-out ones as they are returned, so
//! // in-flight work on the old pool completes untouched.
//! tokio::spawn(async move {
//!     old_primary.close().await;
//!     if let Some(replica) = old_replica {
//!         replica.close().await;
//!     }
//! });
//! # Ok(())
//! # }
//! ```
//!
//! ### Generic Code
//!
//! ```rust,no_run
//! use sqlx_pool_router::PoolProvider;
//!
//! async fn get_user_count<P: PoolProvider>(pools: &P) -> Result<i64, sqlx::Error> {
//!     sqlx::query_scalar("SELECT COUNT(*) FROM users")
//!         .fetch_one(pools.read())
//!         .await
//! }
//! ```
//!
//! ### Testing with Read-Only Enforcement
//!
//! ```rust,no_run
//! use sqlx::PgPool;
//! use sqlx_pool_router::{TestDbPools, PoolProvider};
//!
//! #[sqlx::test]
//! async fn test_read_write_routing(pool: PgPool) {
//!     let pools = TestDbPools::new(pool).await.unwrap();
//!
//!     // A fresh #[sqlx::test] database is empty: create a regular table first
//!     // (through the write pool — TEMP tables are per-connection, so a pooled
//!     // insert could land on a connection that cannot see one).
//!     sqlx::query("CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT)")
//!         .execute(pools.write())
//!         .await
//!         .unwrap();

//!     // This works - writes go to write pool
//!     sqlx::query("INSERT INTO users (name) VALUES ('Alice')")
//!         .execute(pools.write())
//!         .await
//!         .unwrap();
//!
//!     // This FAILS - read pool rejects writes
//!     let result = sqlx::query("INSERT INTO users (name) VALUES ('Bob')")
//!         .execute(pools.read())
//!         .await;
//!     assert!(result.is_err());
//! }
//! ```

use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

use arc_swap::ArcSwap;
use either::Either;
use futures_util::future::BoxFuture;
use futures_util::stream::BoxStream;
use futures_util::TryStreamExt;
use sqlx::postgres::PgPool;
use sqlx::{Database, Error as SqlxError, Execute, Executor, Postgres, SqlStr};

/// A cheap, owned handle to a `PgPool`.
///
/// Returned by [`PoolProvider::read`] and [`PoolProvider::write`]. It is one
/// `Arc` clone of the pool that was active at the moment of the call, so it
/// stays valid even if the provider swaps its pools afterwards ([`DbPools::replace`]).
///
/// It dereferences to [`PgPool`] (so `.acquire()`, `.begin()`,
/// `.connect_options()` etc. work directly) and implements
/// [`sqlx::Executor`] by value, so it can be passed straight to
/// `fetch_one` / `execute` / `fetch_all` exactly like `&PgPool`.
///
/// Do **not** store a `PoolHandle` long-term: it pins the pool it was taken
/// from. Store the provider and call `.read()` / `.write()` per operation.
#[derive(Clone)]
pub struct PoolHandle(PgPool);

impl PoolHandle {
    /// Unwrap into the underlying `PgPool`.
    pub fn into_inner(self) -> PgPool {
        self.0
    }
}

impl From<PgPool> for PoolHandle {
    fn from(pool: PgPool) -> Self {
        Self(pool)
    }
}

impl From<PoolHandle> for PgPool {
    fn from(handle: PoolHandle) -> Self {
        handle.0
    }
}

impl Deref for PoolHandle {
    type Target = PgPool;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AsRef<PgPool> for PoolHandle {
    fn as_ref(&self) -> &PgPool {
        &self.0
    }
}

impl fmt::Debug for PoolHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("PoolHandle").field(&self.0).finish()
    }
}

/// `PoolHandle` executes exactly like `&PgPool`: acquire a connection, run the
/// query, return the connection. Because `PgPool::acquire` produces a `'static`
/// future, the handle can be moved into the returned future.
impl<'p> Executor<'p> for PoolHandle {
    type Database = Postgres;

    fn fetch_many<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxStream<
        'e,
        Result<
            Either<<Self::Database as Database>::QueryResult, <Self::Database as Database>::Row>,
            SqlxError,
        >,
    >
    where
        E: 'q + Execute<'q, Self::Database>,
    {
        let pool = self.0;
        Box::pin(async_stream::try_stream! {
            let mut connection = pool.acquire().await?;
            let mut stream = connection.fetch_many(query);
            while let Some(item) = stream.try_next().await? {
                yield item;
            }
        })
    }

    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxFuture<'e, Result<Option<<Self::Database as Database>::Row>, SqlxError>>
    where
        E: 'q + Execute<'q, Self::Database>,
    {
        let pool = self.0;
        Box::pin(async move {
            let mut connection = pool.acquire().await?;
            connection.fetch_optional(query).await
        })
    }

    fn prepare_with<'e>(
        self,
        sql: SqlStr,
        parameters: &'e [<Self::Database as Database>::TypeInfo],
    ) -> BoxFuture<'e, Result<<Self::Database as Database>::Statement, SqlxError>>
    where
        'p: 'e,
    {
        let pool = self.0;
        Box::pin(async move {
            let mut connection = pool.acquire().await?;
            connection.prepare_with(sql, parameters).await
        })
    }

}

/// `&PoolHandle` executes too, so `.execute(&handle)` works wherever code
/// previously wrote `.execute(&pool)` with a `PgPool` binding.
impl<'p> Executor<'p> for &'_ PoolHandle {
    type Database = Postgres;

    fn fetch_many<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxStream<
        'e,
        Result<
            Either<<Self::Database as Database>::QueryResult, <Self::Database as Database>::Row>,
            SqlxError,
        >,
    >
    where
        E: 'q + Execute<'q, Self::Database>,
    {
        self.clone().fetch_many(query)
    }

    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxFuture<'e, Result<Option<<Self::Database as Database>::Row>, SqlxError>>
    where
        E: 'q + Execute<'q, Self::Database>,
    {
        self.clone().fetch_optional(query)
    }

    fn prepare_with<'e>(
        self,
        sql: SqlStr,
        parameters: &'e [<Self::Database as Database>::TypeInfo],
    ) -> BoxFuture<'e, Result<<Self::Database as Database>::Statement, SqlxError>>
    where
        'p: 'e,
    {
        self.clone().prepare_with(sql, parameters)
    }

}

/// Trait for providing database connection pools with read/write routing.
///
/// Implement this trait to customize pool selection logic. The default
/// implementation [`DbPools`] routes reads to a replica (if configured)
/// and writes to the primary.
///
/// Both methods return an owned [`PoolHandle`] — a cheap `Arc` clone of the
/// pool that is active *right now*. Providers may replace their pools at
/// runtime, so fetch a handle per operation rather than caching one.
///
/// # Example
///
/// ```rust
/// use sqlx::PgPool;
/// use sqlx_pool_router::{PoolHandle, PoolProvider};
///
/// #[derive(Clone)]
/// struct MyPools {
///     primary: PgPool,
///     replica: Option<PgPool>,
/// }
///
/// impl PoolProvider for MyPools {
///     fn read(&self) -> PoolHandle {
///         self.replica.as_ref().unwrap_or(&self.primary).clone().into()
///     }
///
///     fn write(&self) -> PoolHandle {
///         self.primary.clone().into()
///     }
/// }
/// ```
pub trait PoolProvider: Clone + Send + Sync + 'static {
    /// Get a pool for read operations.
    ///
    /// May return a read replica for load distribution, or fall back to
    /// the primary pool if no replica is configured.
    fn read(&self) -> PoolHandle;

    /// Get a pool for write operations.
    ///
    /// Should always return the primary pool to ensure ACID guarantees
    /// and read-after-write consistency.
    fn write(&self) -> PoolHandle;
}

/// The pools a [`DbPools`] is currently routing to.
#[derive(Debug)]
struct PoolSet {
    primary: PgPool,
    replica: Option<PgPool>,
}

/// Database pool abstraction supporting read replicas and runtime replacement.
///
/// Wraps primary and optional replica pools, providing explicit read/write
/// routing. Cloning a `DbPools` is cheap and every clone shares the same
/// underlying pool set: after [`replace`](Self::replace), all clones route to
/// the new pools.
///
/// # Examples
///
/// ## Single Pool Configuration
///
/// ```rust,no_run
/// use sqlx::PgPool;
/// use sqlx_pool_router::DbPools;
///
/// # async fn example() -> Result<(), sqlx::Error> {
/// let pool = PgPool::connect("postgresql://localhost/db").await?;
/// let pools = DbPools::new(pool);
///
/// // Both read() and write() return the same pool
/// assert!(!pools.has_replica());
/// # Ok(())
/// # }
/// ```
///
/// ## Primary/Replica Configuration
///
/// ```rust,no_run
/// use sqlx::postgres::PgPoolOptions;
/// use sqlx_pool_router::DbPools;
///
/// # async fn example() -> Result<(), sqlx::Error> {
/// let primary = PgPoolOptions::new()
///     .max_connections(5)
///     .connect("postgresql://primary/db")
///     .await?;
///
/// let replica = PgPoolOptions::new()
///     .max_connections(10)
///     .connect("postgresql://replica/db")
///     .await?;
///
/// let pools = DbPools::with_replica(primary, replica);
/// assert!(pools.has_replica());
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct DbPools {
    inner: Arc<ArcSwap<PoolSet>>,
}

impl DbPools {
    /// Create a new DbPools with only a primary pool.
    ///
    /// This is useful for development or when you don't have a read replica configured.
    /// All read and write operations will route to the primary pool.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use sqlx::PgPool;
    /// use sqlx_pool_router::DbPools;
    ///
    /// # async fn example() -> Result<(), sqlx::Error> {
    /// let pool = PgPool::connect("postgresql://localhost/db").await?;
    /// let pools = DbPools::new(pool);
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(primary: PgPool) -> Self {
        Self::from_set(PoolSet {
            primary,
            replica: None,
        })
    }

    /// Create a new DbPools with primary and replica pools.
    ///
    /// Read operations will route to the replica pool for load distribution,
    /// while write operations always use the primary pool.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use sqlx::postgres::PgPoolOptions;
    /// use sqlx_pool_router::DbPools;
    ///
    /// # async fn example() -> Result<(), sqlx::Error> {
    /// let primary = PgPoolOptions::new()
    ///     .max_connections(5)
    ///     .connect("postgresql://primary/db")
    ///     .await?;
    ///
    /// let replica = PgPoolOptions::new()
    ///     .max_connections(10)
    ///     .connect("postgresql://replica/db")
    ///     .await?;
    ///
    /// let pools = DbPools::with_replica(primary, replica);
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_replica(primary: PgPool, replica: PgPool) -> Self {
        Self::from_set(PoolSet {
            primary,
            replica: Some(replica),
        })
    }

    fn from_set(set: PoolSet) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(set)),
        }
    }

    /// Atomically replace the active pools.
    ///
    /// Returns the previous `(primary, replica)` so the caller can drain them
    /// (typically `old.close().await` on a background task — `close()` shuts
    /// idle connections immediately and checked-out ones as they are returned,
    /// so in-flight work completes untouched).
    ///
    /// Every clone of this `DbPools` routes to the new pools from its next
    /// `.read()` / `.write()` call. [`PoolHandle`]s obtained *before* the
    /// swap keep pointing at the old pools.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use sqlx::postgres::PgPoolOptions;
    /// use sqlx_pool_router::DbPools;
    ///
    /// # async fn example(pools: DbPools) -> Result<(), sqlx::Error> {
    /// let resized = PgPoolOptions::new()
    ///     .max_connections(8)
    ///     .connect_lazy("postgresql://primary/db")?;
    /// let (old_primary, old_replica) = pools.replace(resized, None);
    /// tokio::spawn(async move {
    ///     old_primary.close().await;
    ///     if let Some(r) = old_replica { r.close().await; }
    /// });
    /// # Ok(())
    /// # }
    /// ```
    pub fn replace(&self, primary: PgPool, replica: Option<PgPool>) -> (PgPool, Option<PgPool>) {
        let old = self.inner.swap(Arc::new(PoolSet { primary, replica }));
        (old.primary.clone(), old.replica.clone())
    }

    /// Check if a replica pool is configured.
    ///
    /// Returns `true` if a replica pool was provided via [`with_replica`](Self::with_replica)
    /// or the last [`replace`](Self::replace).
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use sqlx::PgPool;
    /// use sqlx_pool_router::DbPools;
    ///
    /// # async fn example() -> Result<(), sqlx::Error> {
    /// let pool = PgPool::connect("postgresql://localhost/db").await?;
    /// let pools = DbPools::new(pool);
    /// assert!(!pools.has_replica());
    /// # Ok(())
    /// # }
    /// ```
    pub fn has_replica(&self) -> bool {
        self.inner.load().replica.is_some()
    }

    /// Close all database connections.
    ///
    /// Closes both primary and replica pools (if configured).
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use sqlx::PgPool;
    /// use sqlx_pool_router::DbPools;
    ///
    /// # async fn example() -> Result<(), sqlx::Error> {
    /// let pool = PgPool::connect("postgresql://localhost/db").await?;
    /// let pools = DbPools::new(pool);
    /// pools.close().await;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn close(&self) {
        let set = self.inner.load_full();
        set.primary.close().await;
        if let Some(replica) = &set.replica {
            replica.close().await;
        }
    }

    /// Inherent alias of [`PoolProvider::read`], usable without importing the trait.
    pub fn read(&self) -> PoolHandle {
        PoolProvider::read(self)
    }

    /// Inherent alias of [`PoolProvider::write`], usable without importing the trait.
    pub fn write(&self) -> PoolHandle {
        PoolProvider::write(self)
    }
}

impl PoolProvider for DbPools {
    fn read(&self) -> PoolHandle {
        let set = self.inner.load();
        PoolHandle(set.replica.as_ref().unwrap_or(&set.primary).clone())
    }

    fn write(&self) -> PoolHandle {
        PoolHandle(self.inner.load().primary.clone())
    }
}

/// `&DbPools` executes directly against the **primary** pool that is active
/// at call time, so a long-lived component can hold a `DbPools` (instead of a
/// pinned `PgPool`) and still write `query.execute(&self.pools)`. This is the
/// replacement for the removed `Deref<Target = PgPool>`; it never routes to
/// the replica — use `.read()` for that.
impl<'p> Executor<'p> for &'_ DbPools {
    type Database = Postgres;

    fn fetch_many<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxStream<
        'e,
        Result<
            Either<<Self::Database as Database>::QueryResult, <Self::Database as Database>::Row>,
            SqlxError,
        >,
    >
    where
        E: 'q + Execute<'q, Self::Database>,
    {
        self.write().fetch_many(query)
    }

    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxFuture<'e, Result<Option<<Self::Database as Database>::Row>, SqlxError>>
    where
        E: 'q + Execute<'q, Self::Database>,
    {
        self.write().fetch_optional(query)
    }

    fn prepare_with<'e>(
        self,
        sql: SqlStr,
        parameters: &'e [<Self::Database as Database>::TypeInfo],
    ) -> BoxFuture<'e, Result<<Self::Database as Database>::Statement, SqlxError>>
    where
        'p: 'e,
    {
        self.write().prepare_with(sql, parameters)
    }

}

/// Implement PoolProvider for PgPool for backward compatibility.
///
/// This allows existing code using `PgPool` directly to work with generic
/// code that accepts `impl PoolProvider` without any changes.
///
/// # Example
///
/// ```rust,no_run
/// use sqlx::PgPool;
/// use sqlx_pool_router::PoolProvider;
///
/// async fn query_user<P: PoolProvider>(pools: &P, id: i64) -> Result<String, sqlx::Error> {
///     sqlx::query_scalar("SELECT name FROM users WHERE id = $1")
///         .bind(id)
///         .fetch_one(pools.read())
///         .await
/// }
///
/// # async fn example() -> Result<(), sqlx::Error> {
/// let pool = PgPool::connect("postgresql://localhost/db").await?;
///
/// // Works with PgPool directly
/// let name = query_user(&pool, 1).await?;
/// # Ok(())
/// # }
/// ```
impl PoolProvider for PgPool {
    fn read(&self) -> PoolHandle {
        PoolHandle(self.clone())
    }

    fn write(&self) -> PoolHandle {
        PoolHandle(self.clone())
    }
}

/// Object-safe view of a [`PoolProvider`], for storing a provider without
/// naming its concrete type.
///
/// Implemented for every `PoolProvider`; you normally use it through
/// [`DynPools`] rather than directly.
pub trait PoolSource: Send + Sync + 'static {
    /// See [`PoolProvider::read`].
    fn read_pool(&self) -> PoolHandle;
    /// See [`PoolProvider::write`].
    fn write_pool(&self) -> PoolHandle;
}

impl<P: PoolProvider> PoolSource for P {
    fn read_pool(&self) -> PoolHandle {
        self.read()
    }

    fn write_pool(&self) -> PoolHandle {
        self.write()
    }
}

/// A type-erased, cheaply clonable [`PoolProvider`].
///
/// Long-lived components (middleware state, background tasks, caches) should
/// hold one of these instead of a pinned `PgPool`: every `.read()` /
/// `.write()` goes back to the underlying provider, so a runtime pool swap
/// ([`DbPools::replace`]) reaches them without a restart, and generic code
/// (`AppState<P: PoolProvider>`) can hand out a `DynPools` without making the
/// holder generic too.
///
/// # Example
///
/// ```rust
/// use sqlx_pool_router::{DynPools, PoolProvider};
///
/// struct Cache {
///     pools: DynPools,
/// }
///
/// impl Cache {
///     fn new(pools: impl PoolProvider) -> Self {
///         Self { pools: DynPools::new(pools) }
///     }
///
///     async fn lookup(&self) -> Result<i64, sqlx::Error> {
///         sqlx::query_scalar("SELECT 1").fetch_one(self.pools.read()).await
///     }
/// }
/// ```
#[derive(Clone)]
pub struct DynPools(Arc<dyn PoolSource>);

impl DynPools {
    /// Erase `provider`'s type. Wrapping a `DynPools` returns it unchanged
    /// (no double indirection).
    pub fn new<P: PoolProvider>(provider: P) -> Self {
        let any: &dyn std::any::Any = &provider;
        if let Some(existing) = any.downcast_ref::<DynPools>() {
            return existing.clone();
        }
        Self(Arc::new(provider))
    }

    /// Inherent alias of [`PoolProvider::read`], usable without importing the trait.
    pub fn read(&self) -> PoolHandle {
        self.0.read_pool()
    }

    /// Inherent alias of [`PoolProvider::write`], usable without importing the trait.
    pub fn write(&self) -> PoolHandle {
        self.0.write_pool()
    }
}

impl fmt::Debug for DynPools {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("DynPools")
    }
}

impl PoolProvider for DynPools {
    fn read(&self) -> PoolHandle {
        self.0.read_pool()
    }

    fn write(&self) -> PoolHandle {
        self.0.write_pool()
    }
}

/// `&DynPools` executes against the **primary** pool active at call time,
/// mirroring `&DbPools`.
impl<'p> Executor<'p> for &'_ DynPools {
    type Database = Postgres;

    fn fetch_many<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxStream<
        'e,
        Result<
            Either<<Self::Database as Database>::QueryResult, <Self::Database as Database>::Row>,
            SqlxError,
        >,
    >
    where
        E: 'q + Execute<'q, Self::Database>,
    {
        self.write().fetch_many(query)
    }

    fn fetch_optional<'e, 'q: 'e, E>(
        self,
        query: E,
    ) -> BoxFuture<'e, Result<Option<<Self::Database as Database>::Row>, SqlxError>>
    where
        E: 'q + Execute<'q, Self::Database>,
    {
        self.write().fetch_optional(query)
    }

    fn prepare_with<'e>(
        self,
        sql: SqlStr,
        parameters: &'e [<Self::Database as Database>::TypeInfo],
    ) -> BoxFuture<'e, Result<<Self::Database as Database>::Statement, SqlxError>>
    where
        'p: 'e,
    {
        self.write().prepare_with(sql, parameters)
    }

}

/// Test pool provider with read-only replica enforcement.
///
/// This creates two separate connection pools from the same database:
/// - Primary pool for writes (normal permissions)
/// - Replica pool for reads (enforces `default_transaction_read_only = on`)
///
/// This ensures tests catch bugs where write operations are incorrectly
/// routed through `.read()`. PostgreSQL will reject writes with:
/// "cannot execute INSERT/UPDATE/DELETE in a read-only transaction"
///
/// # Usage with `#[sqlx::test]`
///
/// ```rust,no_run
/// use sqlx::PgPool;
/// use sqlx_pool_router::{TestDbPools, PoolProvider};
///
/// #[sqlx::test]
/// async fn test_read_write_routing(pool: PgPool) {
///     let pools = TestDbPools::new(pool).await.unwrap();
///
///     // Write operations work on .write()
///     sqlx::query("CREATE TABLE users (id INT)")
///         .execute(pools.write())
///         .await
///         .expect("Write pool should allow writes");
///
///     // Write operations FAIL on .read()
///     let result = sqlx::query("INSERT INTO users VALUES (1)")
///         .execute(pools.read())
///         .await;
///     assert!(result.is_err(), "Read pool should reject writes");
///
///     // Read operations work on .read()
///     let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM users")
///         .fetch_one(pools.read())
///         .await
///         .expect("Read pool should allow reads");
/// }
/// ```
///
/// # Why This Matters
///
/// Without this test helper, you might accidentally route write operations through
/// `.read()` and not catch the bug until production when you have an actual replica
/// with replication lag. This helper makes the bug obvious immediately in tests.
///
/// # Example
///
/// ```rust,no_run
/// use sqlx::PgPool;
/// use sqlx_pool_router::{TestDbPools, PoolProvider};
///
/// struct Repository<P: PoolProvider> {
///     pools: P,
/// }
///
/// impl<P: PoolProvider> Repository<P> {
///     async fn get_user(&self, id: i64) -> Result<String, sqlx::Error> {
///         sqlx::query_scalar("SELECT name FROM users WHERE id = $1")
///             .bind(id)
///             .fetch_one(self.pools.read())
///             .await
///     }
///
///     async fn create_user(&self, name: &str) -> Result<i64, sqlx::Error> {
///         sqlx::query_scalar("INSERT INTO users (name) VALUES ($1) RETURNING id")
///             .bind(name)
///             .fetch_one(self.pools.write())
///             .await
///     }
/// }
///
/// #[sqlx::test]
/// async fn test_repository_routing(pool: PgPool) {
///     let pools = TestDbPools::new(pool).await.unwrap();
///     let repo = Repository { pools };
///
///     // Test will fail if create_user incorrectly uses .read()
///     sqlx::query("CREATE TABLE users (id SERIAL PRIMARY KEY, name TEXT)")
///         .execute(repo.pools.write())
///         .await
///         .unwrap();
///
///     let user_id = repo.create_user("Alice").await.unwrap();
///     let name = repo.get_user(user_id).await.unwrap();
///     assert_eq!(name, "Alice");
/// }
/// ```
#[derive(Clone, Debug)]
pub struct TestDbPools {
    primary: PgPool,
    replica: PgPool,
}

impl TestDbPools {
    /// Create test pools from a single database pool.
    ///
    /// This creates:
    /// - A primary pool (clone of input) for writes
    /// - A replica pool (new connection) configured as read-only
    ///
    /// The replica pool enforces `default_transaction_read_only = on`,
    /// so any write operations will fail with a PostgreSQL error.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use sqlx::PgPool;
    /// use sqlx_pool_router::TestDbPools;
    ///
    /// # async fn example(pool: PgPool) -> Result<(), sqlx::Error> {
    /// let pools = TestDbPools::new(pool).await?;
    ///
    /// // Now you have pools that enforce read/write separation
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new(pool: PgPool) -> Result<Self, sqlx::Error> {
        use sqlx::postgres::PgPoolOptions;

        let primary = pool.clone();

        // Create a separate pool with read-only enforcement
        let replica = PgPoolOptions::new()
            .max_connections(pool.options().get_max_connections())
            .after_connect(|conn, _meta| {
                Box::pin(async move {
                    // Set all transactions to read-only by default
                    sqlx::query("SET default_transaction_read_only = on")
                        .execute(&mut *conn)
                        .await?;
                    Ok(())
                })
            })
            .connect_with(pool.connect_options().as_ref().clone())
            .await?;

        Ok(Self { primary, replica })
    }
}

impl PoolProvider for TestDbPools {
    fn read(&self) -> PoolHandle {
        PoolHandle(self.replica.clone())
    }

    fn write(&self) -> PoolHandle {
        PoolHandle(self.primary.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::postgres::PgPoolOptions;

    /// Helper to create a test database and return its pool and name
    async fn create_test_db(admin_pool: &PgPool, suffix: &str) -> (PgPool, String) {
        let db_name = format!("test_dbpools_{}", suffix);

        // Clean up if exists
        sqlx::query(&format!(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{}'",
            db_name
        ))
        .execute(admin_pool)
        .await
        .ok();
        sqlx::query(&format!("DROP DATABASE IF EXISTS {}", db_name))
            .execute(admin_pool)
            .await
            .unwrap();

        // Create fresh database
        sqlx::query(&format!("CREATE DATABASE {}", db_name))
            .execute(admin_pool)
            .await
            .unwrap();

        // Connect to it
        let url = build_test_url(&db_name);
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .unwrap();

        // Create a marker table to identify which database we're connected to
        sqlx::query("CREATE TABLE db_marker (name TEXT)")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(&format!("INSERT INTO db_marker VALUES ('{}')", db_name))
            .execute(&pool)
            .await
            .unwrap();

        (pool, db_name)
    }

    async fn drop_test_db(admin_pool: &PgPool, db_name: &str) {
        sqlx::query(&format!(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{}'",
            db_name
        ))
        .execute(admin_pool)
        .await
        .ok();
        sqlx::query(&format!("DROP DATABASE IF EXISTS {}", db_name))
            .execute(admin_pool)
            .await
            .ok();
    }

    fn build_test_url(database: &str) -> String {
        if let Ok(base_url) = std::env::var("DATABASE_URL") {
            if let Ok(mut url) = url::Url::parse(&base_url) {
                url.set_path(&format!("/{}", database));
                return url.to_string();
            }
        }
        format!("postgres://postgres:password@localhost:5432/{}", database)
    }

    /// Identity of the pool behind a handle: two handles point at the same
    /// pool iff they share the same `Arc<PgConnectOptions>` allocation.
    fn same_pool(a: &PgPool, b: &PgPool) -> bool {
        Arc::ptr_eq(&a.connect_options(), &b.connect_options())
    }

    #[sqlx::test]
    async fn test_dbpools_without_replica(pool: PgPool) {
        let db_pools = DbPools::new(pool.clone());

        // Without replica, read() should return primary
        assert!(!db_pools.has_replica());

        // Both read and write should work
        let read_result: (i32,) = sqlx::query_as("SELECT 1")
            .fetch_one(db_pools.read())
            .await
            .unwrap();
        assert_eq!(read_result.0, 1);

        let write_result: (i32,) = sqlx::query_as("SELECT 2")
            .fetch_one(db_pools.write())
            .await
            .unwrap();
        assert_eq!(write_result.0, 2);

        // A handle derefs to PgPool, so `&*handle` is a `&PgPool`
        let handle = db_pools.write();
        let deref_result: (i32,) = sqlx::query_as("SELECT 3")
            .fetch_one(&*handle)
            .await
            .unwrap();
        assert_eq!(deref_result.0, 3);
    }

    #[sqlx::test]
    async fn test_dbpools_with_replica_routes_correctly(_pool: PgPool) {
        // Create admin connection to postgres database
        let admin_url = build_test_url("postgres");
        let admin_pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&admin_url)
            .await
            .unwrap();

        // Create two separate databases to simulate primary and replica
        let (primary_pool, primary_name) = create_test_db(&admin_pool, "primary").await;
        let (replica_pool, replica_name) = create_test_db(&admin_pool, "replica").await;

        let db_pools = DbPools::with_replica(primary_pool.clone(), replica_pool.clone());
        assert!(db_pools.has_replica());

        // read() should return replica
        let read_marker: (String,) = sqlx::query_as("SELECT name FROM db_marker")
            .fetch_one(db_pools.read())
            .await
            .unwrap();
        assert_eq!(
            read_marker.0, replica_name,
            "read() should route to replica"
        );

        // write() should return primary
        let write_marker: (String,) = sqlx::query_as("SELECT name FROM db_marker")
            .fetch_one(db_pools.write())
            .await
            .unwrap();
        assert_eq!(
            write_marker.0, primary_name,
            "write() should route to primary"
        );

        // Cleanup
        primary_pool.close().await;
        replica_pool.close().await;
        drop_test_db(&admin_pool, &primary_name).await;
        drop_test_db(&admin_pool, &replica_name).await;
    }

    #[sqlx::test]
    async fn replace_swaps_pools_for_existing_clones(pool: PgPool) {
        let pool_a = pool;
        let pool_b = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(pool_a.connect_options().as_ref().clone())
            .await
            .unwrap();
        let pools = DbPools::new(pool_a.clone());
        // Simulates a daemon that captured the provider at boot.
        let held_clone = pools.clone();
        // A handle taken BEFORE the swap keeps pointing at the old pool.
        let pre_swap_handle = held_clone.write();

        let (old_primary, old_replica) = pools.replace(pool_b.clone(), None);

        assert!(same_pool(&old_primary, &pool_a), "old primary handed back");
        assert!(old_replica.is_none());
        assert!(
            same_pool(&held_clone.write(), &pool_b),
            "clone routes to new pool"
        );
        assert!(
            same_pool(&held_clone.read(), &pool_b),
            "no replica: read follows primary"
        );
        assert!(
            same_pool(&pre_swap_handle, &pool_a),
            "pre-swap handle pinned to old pool"
        );
        assert!(!pools.has_replica());

        // The new pool is usable through every clone, by handle or directly.
        let via_clone: (i32,) = sqlx::query_as("SELECT 4")
            .fetch_one(held_clone.write())
            .await
            .unwrap();
        assert_eq!(via_clone.0, 4);
        let direct: (i32,) = sqlx::query_as("SELECT 44")
            .fetch_one(&held_clone)
            .await
            .unwrap();
        assert_eq!(direct.0, 44);

        // Draining the old pool does not affect the new one.
        old_primary.close().await;
        let still_ok: (i32,) = sqlx::query_as("SELECT 5")
            .fetch_one(pools.read())
            .await
            .unwrap();
        assert_eq!(still_ok.0, 5);
    }

    #[sqlx::test]
    async fn replace_can_add_and_drop_replica(pool: PgPool) {
        let make = || async {
            PgPoolOptions::new()
                .max_connections(1)
                .connect_with(pool.connect_options().as_ref().clone())
                .await
                .unwrap()
        };
        let pools = DbPools::new(pool.clone());
        assert!(!pools.has_replica());

        let replica = make().await;
        pools.replace(pool.clone(), Some(replica.clone()));
        assert!(pools.has_replica());
        assert!(
            same_pool(&pools.read(), &replica),
            "read routes to new replica"
        );

        let (_, dropped) = pools.replace(pool.clone(), None);
        assert!(same_pool(dropped.as_ref().unwrap(), &replica));
        assert!(!pools.has_replica());
    }

    #[sqlx::test]
    async fn dyn_pools_follow_the_underlying_provider(pool: PgPool) {
        let pool_b = PgPoolOptions::new()
            .max_connections(1)
            .connect_with(pool.connect_options().as_ref().clone())
            .await
            .unwrap();
        let pools = DbPools::new(pool.clone());
        let erased = DynPools::new(pools.clone());
        assert!(same_pool(&erased.write(), &pool));

        pools.replace(pool_b.clone(), None);
        assert!(
            same_pool(&erased.write(), &pool_b),
            "erased view sees the swap"
        );
        assert!(same_pool(&erased.read(), &pool_b));

        let via_ref: (i32,) = sqlx::query_as("SELECT 6").fetch_one(&erased).await.unwrap();
        assert_eq!(via_ref.0, 6);

        // Re-wrapping does not add indirection.
        let rewrapped = DynPools::new(erased.clone());
        assert!(Arc::ptr_eq(&rewrapped.0, &erased.0));

        // A bare PgPool can be erased too (test harnesses).
        let from_pg = DynPools::new(pool.clone());
        assert!(same_pool(&from_pg.read(), &pool));
    }

    #[sqlx::test]
    async fn test_dbpools_close(pool: PgPool) {
        let db_pools = DbPools::new(pool);

        // Close should not panic
        db_pools.close().await;
    }

    #[sqlx::test]
    async fn test_pgpool_implements_pool_provider(pool: PgPool) {
        // PgPool should implement PoolProvider, routing both ways to itself
        assert!(same_pool(&pool.read(), &pool));
        assert!(same_pool(&pool.write(), &pool));

        // Should be able to use it the same way
        let result: (i32,) = sqlx::query_as("SELECT 1")
            .fetch_one(pool.read())
            .await
            .unwrap();
        assert_eq!(result.0, 1);
    }

    #[sqlx::test]
    async fn pool_handle_executes_every_executor_path(pool: PgPool) {
        let handle = pool.write();

        // fetch_optional / fetch_one path
        let one: (i32,) = sqlx::query_as("SELECT 7")
            .fetch_one(handle.clone())
            .await
            .unwrap();
        assert_eq!(one.0, 7);

        // fetch_many path (fetch_all streams)
        let many: Vec<(i32,)> = sqlx::query_as("SELECT * FROM generate_series(1, 3)")
            .fetch_all(handle.clone())
            .await
            .unwrap();
        assert_eq!(many.len(), 3);

        // execute path, by value and by reference
        let done = sqlx::query("SELECT 1")
            .execute(handle.clone())
            .await
            .unwrap();
        assert_eq!(done.rows_affected(), 1);
        let done = sqlx::query("SELECT 1").execute(&handle).await.unwrap();
        assert_eq!(done.rows_affected(), 1);

        // prepare path
        use sqlx::Statement as _;
        let stmt = sqlx::Executor::prepare(handle.clone(), "SELECT $1::int")
            .await
            .unwrap();
        let prepared: (i32,) = stmt.query_as().bind(9).fetch_one(handle).await.unwrap();
        assert_eq!(prepared.0, 9);
    }

    #[sqlx::test]
    async fn test_testdbpools_read_pool_rejects_writes(pool: PgPool) {
        let pools = TestDbPools::new(pool).await.unwrap();

        // Write operations should work on the write pool
        sqlx::query("CREATE TEMP TABLE test_write (id INT)")
            .execute(pools.write())
            .await
            .expect("Write pool should allow CREATE TABLE");

        // Write operations should FAIL on the read pool
        let result = sqlx::query("CREATE TEMP TABLE test_read_reject (id INT)")
            .execute(pools.read())
            .await;

        assert!(result.is_err(), "Read pool should reject CREATE TABLE");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("read-only") || err.contains("cannot execute"),
            "Error should mention read-only restriction, got: {}",
            err
        );
    }

    #[sqlx::test]
    async fn test_testdbpools_read_pool_allows_selects(pool: PgPool) {
        let pools = TestDbPools::new(pool).await.unwrap();

        // Read operations should work on the read pool
        let result: (i32,) = sqlx::query_as("SELECT 1 + 1 as sum")
            .fetch_one(pools.read())
            .await
            .expect("Read pool should allow SELECT");

        assert_eq!(result.0, 2, "Should compute 1 + 1 = 2");
    }
}
