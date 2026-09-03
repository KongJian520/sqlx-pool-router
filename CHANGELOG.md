# Changelog

## [1.0.0](https://github.com/doublewordai/sqlx-pool-router/compare/sqlx-pool-router-v0.2.0...sqlx-pool-router-v1.0.0) (2026-09-03)


### ⚠ BREAKING CHANGES

* PoolProvider::read/write return PoolHandle instead of &PgPool; Deref<Target = PgPool> for DbPools is removed. Migration: `&pools.read()` where a &PgPool parameter is expected, `.into_inner()` where an owned PgPool is stored.
* PoolProvider::read/write return PoolHandle instead of &PgPool; Deref<Target = PgPool> for DbPools is removed.
* add generic PoolProvider trait with sqlx-tracing support

### Features

* add generic PoolProvider trait with sqlx-tracing support ([bfb9553](https://github.com/doublewordai/sqlx-pool-router/commit/bfb95536267570975e38e37c7c81a370220d9b3b))
* inherent read/write on DbPools and DynPools ([79a2d71](https://github.com/doublewordai/sqlx-pool-router/commit/79a2d716b7c704d95b7804dbfa73e4fd7e7f3d1b))
* owned pool handles and runtime-swappable DbPools ([dbc16b5](https://github.com/doublewordai/sqlx-pool-router/commit/dbc16b5ee0864aff772d70459a3fd07d67acc393))
* owned pool handles and runtime-swappable DbPools ([0675ccb](https://github.com/doublewordai/sqlx-pool-router/commit/0675ccbecabd521c3f68ada61acab96f99a9546f))

## [0.2.0](https://github.com/doublewordai/janus/compare/sqlx-pool-router-v0.1.0...sqlx-pool-router-v0.2.0) (2026-01-22)


### Features

* initial release of sqlx-pool-router ([e7100c1](https://github.com/doublewordai/janus/commit/e7100c18a076a4a67dff34dd23c19a7552b90a57))
