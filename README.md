# sqlx-pool-router

[![Crates.io](https://img.shields.io/crates/v/sqlx-pool-router.svg)](https://crates.io/crates/sqlx-pool-router)
[![Documentation](https://docs.rs/sqlx-pool-router/badge.svg)](https://docs.rs/sqlx-pool-router)
[![License](https://img.shields.io/crates/l/sqlx-pool-router.svg)](https://github.com/doublewordai/sqlx-pool-router#license)

A lightweight Rust library for routing database operations to different SQLx PostgreSQL connection pools based on whether they're read or write operations.

This enables load distribution by routing read-heavy operations to read replicas while ensuring write operations always go to the primary database.

## Features

- **Zero-cost abstraction**: Trait-based design with no runtime overhead
- **Type-safe routing**: Compile-time guarantees for read/write pool separation
- **Backward compatible**: `PgPool` implements `PoolProvider` for seamless integration
- **Flexible**: Use single pool or separate primary/replica pools
- **Well-tested**: Comprehensive test suite with replica routing verification

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
sqlx-pool-router = "0.1"
sqlx = { version = "0.8", features = ["postgres", "runtime-tokio"] }
```

## Quick Start

### Single Pool (Development)

```rust
use sqlx::PgPool;
use sqlx-pool-router::PoolProvider;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = PgPool::connect("postgresql://localhost/mydb").await?;

    // PgPool implements PoolProvider automatically
    let result: (i32,) = sqlx::query_as("SELECT 1")
        .fetch_one(pool.read())
        .await?;

    Ok(())
}
```

### Read/Write Separation (Production)

```rust
use sqlx::postgres::PgPoolOptions;
use sqlx-pool-router::{DbPools, PoolProvider};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let primary = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgresql://primary-host/mydb")
        .await?;

    let replica = PgPoolOptions::new()
        .max_connections(10)  // More connections for read-heavy workload
        .connect("postgresql://replica-host/mydb")
        .await?;

    let pools = DbPools::with_replica(primary, replica);

    // Reads go to replica
    let users: Vec<(i32, String)> = sqlx::query_as("SELECT id, name FROM users")
        .fetch_all(pools.read())
        .await?;

    // Writes go to primary
    sqlx::query("INSERT INTO users (name) VALUES ($1)")
        .bind("Alice")
        .execute(pools.write())
        .await?;

    Ok(())
}
```

## Replacing pools at runtime

sqlx pools are fixed-size once built. When the right size changes while the
process is running — e.g. a per-role connection budget re-divided across a
changing number of replicas — build a new pool and swap it in:

```rust
let (old_primary, old_replica) = pools.replace(new_primary, None);
tokio::spawn(async move {
    old_primary.close().await; // idle now, checked-out as they return
    if let Some(r) = old_replica { r.close().await; }
});
```

Every clone of the `DbPools` routes to the new pool from its next `.read()` /
`.write()`. `.read()`/`.write()` return an owned `PoolHandle` (one `Arc` clone
of the pool active at that moment) that derefs to `PgPool` and executes like
`&PgPool`; don't cache it. Long-lived components that can't name the provider
type (middleware state, caches, background tasks) can hold a type-erased
`DynPools` instead of a pinned `PgPool`.

## Testing with `TestDbPools`

The crate includes a `TestDbPools` helper for use with `#[sqlx::test]` that enforces read/write separation in your tests:

```rust
use sqlx::PgPool;
use sqlx-pool-router::{TestDbPools, PoolProvider};

#[sqlx::test]
async fn test_repository(pool: PgPool) {
    // TestDbPools creates a read-only replica from the same database
    let pools = TestDbPools::new(pool).await.unwrap();

    // A fresh #[sqlx::test] database is empty: create the table through the
    // write pool first (a regular table, not TEMP — TEMP tables are
    // per-connection and a pooled query may not see one).
    sqlx::query("CREATE TABLE users (id INT, name TEXT)")
        .execute(pools.write())
        .await
        .unwrap();

    // Writes through .write() work fine
    sqlx::query("INSERT INTO users (name) VALUES ('Alice')")
        .execute(pools.write())
        .await
        .unwrap();

    // Writes through .read() FAIL with a read-only error - catches bugs immediately!
    let result = sqlx::query("INSERT INTO users (name) VALUES ('Bob')")
        .execute(pools.read())
        .await;
    assert!(result.is_err());
}
```

**Why use `TestDbPools`?**

- Catches routing bugs immediately in tests
- No need for an actual replica database in test environment
- Enforces `default_transaction_read_only = on` on the read pool
- PostgreSQL will reject any write operations on `.read()`

## Generic Programming

Make your types generic over `PoolProvider` to support both single and multi-pool configurations:

```rust
use sqlx-pool-router::PoolProvider;

struct Repository<P: PoolProvider> {
    pools: P,
}

impl<P: PoolProvider> Repository<P> {
    async fn get_user(&self, id: i64) -> Result<String, sqlx::Error> {
        // Read from replica
        sqlx::query_scalar("SELECT name FROM users WHERE id = $1")
            .bind(id)
            .fetch_one(self.pools.read())
            .await
    }

    async fn create_user(&self, name: &str) -> Result<i64, sqlx::Error> {
        // Write to primary
        sqlx::query_scalar("INSERT INTO users (name) VALUES ($1) RETURNING id")
            .bind(name)
            .fetch_one(self.pools.write())
            .await
    }
}

// Works with both PgPool and DbPools!
let repo_single = Repository { pools: single_pool };
let repo_multi = Repository { pools: db_pools };
```

## When to Use Each Method

### `.read()` - For Read Operations

Use for queries that:
- Don't modify data (SELECT without FOR UPDATE)
- Can tolerate slight staleness (eventual consistency)
- Benefit from load distribution

Examples: user listings, analytics, dashboards, search

### `.write()` - For Write Operations

Use for operations that:
- Modify data (INSERT, UPDATE, DELETE)
- Require transactions
- Need locking reads (SELECT FOR UPDATE)
- Require read-after-write consistency

Examples: creating records, updates, deletes, transactions

## Architecture

```text
┌─────────────┐
│   DbPools   │
└──────┬──────┘
       │
  ┌────┴────┐
  ↓         ↓
┌─────┐  ┌─────────┐
│Primary│  │ Replica │ (optional)
└─────┘  └─────────┘
```

## Real-World Use Cases

This library is used in production by:
- [outlet-postgres](https://github.com/doublewordai/outlet-postgres) - HTTP request/response logging middleware
- [fusillade](https://github.com/doublewordai/fusillade) - LLM request batching daemon
- [dwctl](https://github.com/doublewordai/control-layer) - Observability and analytics platform

## License

This project is licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Running Tests

The test suite requires a PostgreSQL database:

```bash
# Start PostgreSQL (using Docker)
docker run -d \
  -p 5432:5432 \
  -e POSTGRES_PASSWORD=password \
  -e POSTGRES_DB=test \
  --name sqlx-pool-router-test-db \
  postgres:16

# Set the DATABASE_URL (use 'postgres' database for sqlx::test to create isolated test DBs)
export DATABASE_URL=postgresql://postgres:password@localhost:5432/postgres

# Run tests
cargo test --all-features

# Clean up
docker stop sqlx-pool-router-test-db && docker rm sqlx-pool-router-test-db
```

**Note:** The tests use `#[sqlx::test]` which automatically creates isolated test databases for each test, so you don't need to worry about test pollution.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

### Commit Convention

This project uses [Conventional Commits](https://www.conventionalcommits.org/). Please format your commits as:

- `feat:` New features
- `fix:` Bug fixes
- `docs:` Documentation changes
- `test:` Test additions or modifications
- `refactor:` Code refactoring
- `perf:` Performance improvements
- `chore:` Build process or tooling changes

Example: `feat: add support for connection timeout configuration`
