# Cassandra / ScyllaDB driver for Toasty — research digest

> Persistent record of the Phase-1 codebase exploration for the Cassandra /
> ScyllaDB driver. Kept here so the same exploration does not need to be
> dispatched again.
>
> Repo: `toasty` (Rust ORM). Date: 2026-10-28.

---

## 0. Bottom line

Cassandra/Scylla must be integrated as a **non-SQL (key-value) driver**, exactly like
DynamoDB, not as a SQL driver:

- `Capability::sql = None` makes the planner emit `GetByKey`, `QueryPk`, `Scan`,
  `UpdateByKey`, `DeleteByKey`, `FindPkByIndex`, `Upsert`, `Insert`.
- `sql = Some(Dialect)` would route through `toasty-sql`, and the planner would emit
  joins (via includes), CTEs (`#[version]` OCC), `RETURNING`, `ORDER BY`, and
  `IN (subquery)` — none of which CQL supports.
- Relations are already join-free for non-SQL: the engine issues one batched child
  query per relation level and merges in memory (`NestedMerge`).

The gaps to accept/handle:

| Area | Status for a non-SQL driver |
|---|---|
| Transactions | Planner never wraps plans in tx (`use_transactions = capability.sql()`). Explicit `db.transaction()` still reaches the driver → return `unsupported_feature` (DynamoDB does this). |
| `count()` | Rejected for non-SQL (`plan/statement.rs:902-910`). |
| Raw SQL | Rejected for non-SQL (`engine.rs:108-121`). |
| `#[version]` OCC | Compiled to CTE or read-modify-write, both `QuerySql`-only → needs CQL LWT support or rejection. |
| Multi-step `via` includes | `todo!()` panic for non-SQL (`lower/include.rs:506-512`, `lower/via_join.rs:1-8`) → should become `unsupported_feature`. |
| Unique indexes | Cassandra has no unique constraint; DynamoDB emulates with extra tables. For Cassandra, reject at schema/`push_schema` time. |

---

## 1. Driver contract

### `Driver` — `crates/toasty-core/src/driver.rs:126-153`

```rust
#[async_trait]
pub trait Driver: Debug + Send + Sync + 'static {
    fn url(&self) -> Cow<'_, str>;
    fn capability(&self) -> &'static Capability;
    async fn connect(&self, cx: &ConnectContext) -> crate::Result<Box<dyn Connection>>;
    fn max_connections(&self) -> Option<usize> { None }
    fn generate_migration(&self, schema_diff: &diff::Schema<'_>) -> Migration;
    async fn reset_db(&self) -> crate::Result<()>;
}
```

- No associated types; `Result` is `toasty_core::Result`.
- `ConnectContext { query_log: QueryLogConfig }` (`driver.rs:96-103`).
- `generate_migration` is synchronous and returns `Migration` (SQL-only enum today,
  `crates/toasty-core/src/schema/db/migration.rs:15-44`: `Migration::Sql(String)`).

### `Connection` — `crates/toasty-core/src/driver.rs:171-260`

```rust
#[async_trait]
pub trait Connection: Debug + Send + 'static {
    async fn exec(&mut self, schema: &Arc<Schema>, plan: Operation) -> crate::Result<ExecResponse>;
    fn is_valid(&self) -> bool { true }
    async fn ping(&mut self) -> crate::Result<()> { Ok(()) }
    async fn push_schema(&mut self, schema: &Schema) -> crate::Result<()>;
    async fn applied_migrations(&mut self) -> crate::Result<Vec<AppliedMigration>>;
    async fn apply_migration(&mut self, id: u64, name: &str, migration: &Migration) -> crate::Result<()>;
}
```

- `Connection` is `Send + 'static` but **not** `Sync`; one connection per worker task.
- `push_schema` is where `CREATE TABLE` / index DDL happens (driver-specific).
- Error contract (`driver.rs:31-56`): connection faults must be `Error::connection_lost`,
  write conflicts `Error::serialization_failure`, read-only writes
  `Error::read_only_transaction`, otherwise `Error::driver_operation_failed`.

### `Operation` — `crates/toasty-core/src/driver/operation.rs:69-107`

```rust
pub enum Operation {
    Insert(Insert), DeleteByKey(DeleteByKey), FindPkByIndex(FindPkByIndex),
    GetByKey(GetByKey), QueryPk(QueryPk), QuerySql(QuerySql), RawSql(RawSql),
    Transaction(Transaction), UpdateByKey(UpdateByKey), Upsert(Upsert), Scan(Scan),
}
```

| Variant | Payload (file under `driver/operation/`) |
|---|---|
| `Insert` | `{ stmt: stmt::Statement, params: Vec<TypedValue>, ret: Option<Vec<stmt::Type>> }` — sent to **every** driver (`insert.rs`) |
| `GetByKey` | `{ table, select: Vec<ColumnId>, keys: Vec<stmt::Value> }` (`get_by_key.rs`) |
| `QueryPk` | `{ table, index: Option<IndexId>, select, pk_filter: Expr, filter: Option<Expr>, limit: Option<Pagination>, order: Option<Direction> }` (`query_pk.rs`) |
| `Scan` | `{ table, columns: Vec<usize> /* positions */, filter, limit }` (`scan.rs`) |
| `UpdateByKey` | `{ table, keys, assignments: Assignments, filter, condition, returning: Option<Vec<ColumnId>> }` (`update_by_key.rs`) |
| `DeleteByKey` | `{ table, keys, filter, condition }` (`delete_by_key.rs`) |
| `FindPkByIndex` | `{ table, index: IndexId, filter }` (`find_pk_by_index.rs`) |
| `Upsert` | `{ stmt: stmt::Insert, params, ret }`, non-SQL upserts only (`upsert.rs`) |
| `QuerySql` / `RawSql` | SQL drivers only (`query_sql.rs`, `raw_sql.rs`) |
| `Transaction` | `Start{isolation, read_only, mode}`, `Commit`, `Rollback`, `Savepoint`, ... (`transaction.rs`) |
| `Pagination` | `Cursor { page_size, after }` \| `Offset { limit, offset }` (`pagination.rs`) |
| `TypedValue` | `{ value: stmt::Value, ty: db::Type }` (`typed_value.rs`) |

### Responses — `crates/toasty-core/src/driver/response.rs:19-43`

```rust
pub struct ExecResponse {
    pub values: Rows,
    pub next_cursor: Option<Box<stmt::Value>>,
    pub prev_cursor: Option<Box<stmt::Value>>,
}
pub enum Rows { Count(u64), Value(stmt::Value), Stream(stmt::ValueStream) }
```

The engine normalizes `Rows::Value(List)` / `Rows::Stream` (`engine/exec.rs:136-142`).

---

## 2. `Capability` — `crates/toasty-core/src/driver/capability.rs`

Plain struct; drivers return a `&'static Capability` constant. Existing constants:
`SQLITE` (`:660`), `POSTGRESQL` (`:761`), `MYSQL` (`:832`), `MARIADB` (`:892`),
`TURSO` (`:921`), **`DYNAMODB` (`:928-1021`, the non-SQL template)**.
`validate()` at `:554-613`, called from `Db::builder().build()`
(`crates/toasty/src/db/builder.rs:291-295`).

### Fields that matter for Cassandra

| Field | Cassandra value | Why |
|---|---|---|
| `driver_name` | `"cassandra"` | diagnostics |
| `sql` | `None` | force KV pipeline |
| `sql_placeholder` | `None` | invariant: `sql.is_some() == sql_placeholder.is_some()` |
| `storage_types` | custom | UUID/Timestamp/Blob/Text/collections |
| `schema_mutations` | `SchemaMutations { alter_column_type: false, alter_column_properties_atomic: false }` | CQL cannot change column types in place |
| `auto_increment` | `false` | no sequences; keys must be supplied |
| `max_identifier_length` | `Some(48)` | Cassandra name limit |
| `native_*` (varchar/json/timestamp/date/time/datetime/cidr/inet/macaddr/decimal/enum) | mostly `false` or true per mapping | see §6 |
| `native_starts_with` | `false` | CQL has no prefix operator |
| `native_like` / `native_ilike` | `false` / `false` | |
| `scan` | `true` | `Scan` fallback; `false` would require a covering index for every query |
| `scan_supports_sort` | `false` | CQL `ORDER BY` only on clustering columns |
| `index_or_predicate` | `false` | OR rewritten to `ANY(MAP(...))` fan-out |
| `backward_pagination` | `false` | no `prev_cursor` |
| `transaction_lock_mode` | `false` | no `FOR UPDATE` modes |
| `bool_key_type` | `true` (allowed) or `false` | bool is a legal CQL key type |
| `primary_key_ne_predicate` | `false` | `!=` on partition key unsupported |
| `upsert_primary_key` | `true` (CQL `INSERT` is an upsert) | |
| `upsert_unique` | `false` | no unique constraints |
| `upsert_branch_assignments`, `upsert_targeted_ignore` | `false` | lightweight `IF NOT EXISTS` only |
| `bind_list_param`, `predicate_match_any` | `false` | |
| `native_array` | `false` unless templated lists are modelled | |
| `vec_scalar` | `true` | CQL `list<scalar>` |
| `unique_list_index` | `false` | |
| `document_collections` | `true` if maps/UDTs used | |
| `vec_remove` / `vec_pop` / `vec_remove_at` | `false` | no in-place atomic list edits |
| `test_connection_pool` | `false` | |

Other capability facts:
- `Dialect` is closed: `Sqlite, Postgresql, Mysql, MariaDb`
  (`crates/toasty-core/src/driver/dialect.rs:19-32`). No Cassandra variant needed
  for the recommended design.
- `schema.db::IndexColumn.scope: IndexScope::{Partition, Local}`
  (`crates/toasty-core/src/schema/db/index.rs:104-154`) maps directly onto
  Cassandra partition keys vs clustering keys. DynamoDB consumes it in
  `crates/toasty-driver-dynamodb/src/op/create_table.rs:83-95`.
- Capability gates live in: schema build (`toasty-core/src/schema/builder.rs:106-332`),
  type verification (`schema/db/ty.rs:361-400`), app model verification
  (`schema/app/model.rs:219-238`, `via` requires SQL), `engine/verify.rs`,
  `engine/lower.rs`, `engine/index.rs`, `engine/plan/statement.rs`.

---

## 3. Existing driver implementations (patterns to copy)

### DynamoDB — `crates/toasty-driver-dynamodb/` (closest template)

Files: `src/lib.rs`, `src/op.rs` + `src/op/{get_by_key,query_pk,scan,insert,update_by_key,delete_by_key,find_pk_by_index,upsert,create_table}.rs`, `src/type.rs`, `src/value.rs`.

- `DynamoDb { url: String, client: Client }` (`lib.rs:56-60`); `new(url, client)`,
  async `from_env(url)` validating the `dynamodb` scheme (`:68-94`).
- `impl Driver` `:97-156`: `capability()` → `&Capability::DYNAMODB`; `connect()`
  clones the SDK client and copies `cx.query_log`; `generate_migration()` →
  `unimplemented!`; `reset_db()` lists/deletes tables.
- `Connection { client, query_log }`; `exec()` wraps `exec2` in
  `QueryLog::operation(...)` (`:191-203`).
- `push_schema` iterates `schema.db.tables` and calls `create_table` (`:205-211`);
  `applied_migrations` / `apply_migration` are `todo!` (`:213-226`).
- Dispatch (`:229-252`): all KV ops; `QuerySql` → `todo!`; `RawSql` and
  `Transaction` → `Err(unsupported_feature(...))`.
- `create_table` (`op/create_table.rs:7-183`): 1–2 PK columns, GSIs from
  `IndexScope`, separate tables for unique indexes.
- Pagination cursors are a flat `Record([name, value, ...])`
  (`serialize_ddb_cursor` / `deserialize_ddb_cursor`, `lib.rs:289-320`).
- Value conversion is type-directed (`src/value.rs:28-185`).

### SQLite — `crates/toasty-driver-sqlite/`

- `Sqlite { source, busy_timeout, pragmas }`, `in_memory()` / `new(url)` / `open()`.
- `impl Driver` `:259-320`: `capability()`, `connect()` sets pragmas and
  `max_connections() = Some(1)` for in-memory, `generate_migration()` via
  `sql::MigrationStatement::from_diff(...)` + `Serializer::sqlite`, `reset_db()`
  deletes the file.
- `Connection::exec` `:431-492` handles `Insert`/`QuerySql`/`RawSql`/`Transaction`,
  others `todo!()`. `push_schema` `:494-501` + `create_table` `:590-615`.
  Migrations via `__toasty_migrations` table.

### PostgreSQL — `crates/toasty-driver-postgresql/`

- Error classification: SQLSTATE `40001` → serialization failure, `25006` →
  read-only, no `DbError` → connection lost (`lib.rs:60-70`).
- `Connection::exec` `:490-541` rejects `TransactionMode::Immediate|Exclusive`;
  `push_schema` creates enum types then tables/indices (`:543-570`);
  `is_valid()` / `ping()` at `:660-674`.

### MySQL / MariaDB — `crates/toasty-driver-mysql/`

- One crate, two driver types: `MySQL` and `MariaDB` (`src/mariadb.rs`), MariaDB
  reuses the MySQL driver with `Capability::MARIADB` + `Dialect::MariaDb`
  (`lib.rs:112-156`). This is the precedent for "one crate, two vendor entry points".

### Turso — `crates/toasty-driver-turso/`

- Reuses the SQLite serializer; feature-gated `sync` / `serverless`.

---

## 4. URL dispatch and Cargo wiring

### Dispatch — `crates/toasty/src/db/connect.rs:43-130`

```rust
let scheme = ConnectionUrl::parse(url)?.scheme().to_ascii_lowercase();
let driver: Box<dyn Driver> = match scheme.as_str() {
    #[cfg(feature = "dynamodb")]
    "dynamodb" => Box::new(toasty_driver_dynamodb::DynamoDb::from_env(...).await?),
    #[cfg(not(feature = "dynamodb"))]
    "dynamodb" => return Err(Error::unsupported_feature("`dynamodb` feature not enabled")),
    ...
    scheme => return Err(Error::unsupported_feature(format!("unsupported database scheme `{scheme}`"))),
};
```

Doc table of scheme→driver at `connect.rs:28-37`; feature list also in
`crates/toasty/src/lib.rs:93-103`.

### Workspace — root `Cargo.toml`

- `members` includes `crates/toasty-driver-*` (lines 10–15); drivers excluded from
  `default-members` (lines 36–59).
- `[workspace.dependencies]` driver entries at lines 82–91.
- `release-plz.toml:17-63` lists each crate with `version_group = "toasty"`.

### `crates/toasty/Cargo.toml`

- Features `dynamodb`, `mysql`, `mariadb`, `postgresql`, `sqlite`, `turso`
  (lines 16–28); optional driver deps at 67–72.
- Type features (`rust_decimal`, `bigdecimal`, `jiff`, `net`) forward to every
  driver with `?/feature` syntax (lines 29–59).

### `tests/Cargo.toml`

- Per-driver feature mapping (lines 14–41), e.g.
  `dynamodb = ["toasty/dynamodb", "dep:toasty-driver-dynamodb", "dep:aws-config", "dep:aws-sdk-dynamodb"]`.
- Optional driver deps 61–72; `build.rs` generates guide doc tests.

---

## 5. Test harness

### `Setup` trait — `crates/toasty-driver-integration-suite/src/setup.rs:1-10`

```rust
#[async_trait]
pub trait Setup: Send + Sync + 'static {
    fn driver(&self) -> Box<dyn Driver>;
    async fn delete_table(&self, name: &str);
}
```

- No `url()` / `create_db()`; the URL is baked into the driver instance.
- `Test::try_setup_db_with` calls `db.push_schema()` then records table names
  (`test.rs:58-93`); after each test it calls `setup.delete_table(prefix+name)`.
- Table prefix: `test_{process_id}_{test_counter}_`
  (`crates/toasty-driver-integration-suite/src/isolate.rs:26-29`).
- Driver is wrapped in `InstrumentedDriver` (`instrumented_driver.rs:126-175`),
  which records ops and supports fault injection.

### Per-driver test binary — `tests/tests/<driver>.rs`

```rust
#![cfg(feature = "cassandra")]

#[async_trait::async_trait]
impl toasty_driver_integration_suite::Setup for CassandraSetup {
    fn driver(&self) -> Box<dyn toasty_core::driver::Driver> { ... }
    async fn delete_table(&self, name: &str) { ... }
}

toasty_driver_integration_suite::generate_driver_tests!(CassandraSetup::new(),
    sql: false,
    auto_increment: false,
    /* ...one flag per unsupported capability... */
);
```

- `generate_driver_tests!` is produced by
  `generate_test_registry!("src/tests")` (`integration-suite/src/lib.rs:34-36`);
  it scans `src/tests/*.rs` for `#[driver_test]`.
- Flags omitted from the invocation default to **true**
  (`test_variants.rs:100-124`). Only explicitly-`false` flags filter tests.
- Aliases: `bigdecimal` → `bigdecimal_implemented`, `decimal` →
  `decimal_arbitrary_precision` + `native_decimal`
  (`test_registry.rs:296-329`).
- Every capability referenced in some `requires(...)` is compile-time
  validated, and a runtime `validate_driver_capabilities` test asserts the
  driver's actual `Capability` matches the flags
  (`test_registry.rs:202-330`).
- Test flags (not capabilities): `cte_unreferenced` only (`parse.rs:38-64`).
- `#[driver_test(...)]` supports `id(ID)`, `matrix(...)`, `requires(...)`,
  `serial`, `scenario(...)`; **no** `only(...)` (`parse.rs:295-438`).
- `id(ID)` expands to `id_u64` (gated on `auto_increment`) and `id_uuid`
  (`parse.rs:540-583`).
- Module list must be updated in `integration-suite/src/tests.rs` when adding a
  suite test (129 modules today).

Reference files: `tests/tests/sqlite.rs:1-48`, `tests/tests/dynamodb.rs:1-103`,
`tests/tests/postgresql.rs:8-58`.

### CI — `.github/workflows/ci.yml`

Jobs: `check` (workspace all-features), `clippy`, `check-wasm`, `feature-combinations`,
`msrv`, `test-sqlite` (143), `test-mysql` (167), `test-mariadb` (200),
`test-postgresql` (231), `test-postgresql-uds` (274), `test-dynamodb` (300),
`test-turso` (319), `test-all-os` (340).

DynamoDB job starts `amazon/dynamodb-local` on 8000 and runs
`cargo test --no-default-features --features dynamodb -- --test-threads 1`.

`compose.yaml` services: `dynamodb`, `mysql`, `postgresql`, `postgresql-tls`.
No Cassandra/Scylla service yet.

Env conventions: `TOASTY_TEST_POSTGRES_URL`, `TOASTY_TEST_MYSQL_URL`,
`TOASTY_TEST_MARIADB_URL`, `AWS_ENDPOINT_URL_DYNAMODB`.

---

## 6. Engine behaviour a driver author must know

### Pipeline — `crates/toasty/src/engine.rs:81-105`

```
stmt -> normalize -> verify -> lower (HIR) -> plan (MIR) -> exec
```

### The two planning paths — `engine/plan/statement.rs:897-926`

```rust
if select_items.contains(&CountStar) && !capability.sql() {
    return Err(unsupported_feature("... does not support count() queries"));
}
...
} else if stmt.is_insert() { self.plan_insert(stmt) }
else if capability.sql() { self.plan_data_loading_sql(stmt) }
else { self.plan_data_loading_nosql(stmt) }
```

Non-SQL loading (`:1576-1605`) consults `plan_index_path` and emits
`QueryPk` / `GetByKey` / `FindPkByIndex` / `Scan` / `UpdateByKey` /
`DeleteByKey`.

### Transaction gating — `engine/plan.rs:47-53`

```rust
let (steps, needs_transaction) = ExecPlanner {
    ... use_transactions: self.capability().sql(),
}.plan_execution();
```

`needs_transaction` also requires >1 DB op (`plan/execution.rs:63-71`).
So a non-SQL driver receives no plan-level transactions. Explicit
`Db::transaction()` still sends `Operation::Transaction`
(`crates/toasty/src/db/tx.rs:132-263`) → reject with `unsupported_feature`.

### Driver call sites (`engine/exec/`)

| MIR op | File | Driver op |
|---|---|---|
| `ExecStatement` | `exec_statement.rs:106-140` | `Insert` if `stmt.is_insert()`, else `QuerySql` |
| `Scan` | `scan.rs:23-37` | `Scan` (columns as positional indices) |
| `QueryPk` | `query_pk.rs:45-61` | `QueryPk` (one call per split filter) |
| `GetByKey` | `get_by_key.rs:29-35` | `GetByKey` |
| `DeleteByKey` | `delete_by_key.rs:26-34` | `DeleteByKey` |
| `UpdateByKey` | `update_by_key.rs:66-75` | `UpdateByKey` |
| `FindPkByIndex` | `find_pk_by_index.rs:29-38` | `FindPkByIndex` |
| `Upsert` | `upsert.rs:21-28` | `Upsert` |

- In-memory post-processing exists: `Operation::Filter` (`exec/filter.rs`),
  `Eval` (`exec/eval.rs`), `NestedMerge` (`exec/nested_merge.rs`). KV drivers may
  return coarse rows and let the engine filter.
- `prepare_for_driver` (`legalize.rs:41-49`) extracts bind params **only** for SQL;
  KV drivers read `stmt::Value` directly from the statement.

### Relations

`lower/include.rs:487-598` synthesizes one child query per relation and attaches it
via `Expr::Arg`; `plan/nested_merge.rs:65-113` batch-loads and merges in memory.
There is no join op in the driver interface. Exception: multi-step `via` relations
are lowered to a SQL JOIN chain and are `todo!()` for non-SQL
(`lower/include.rs:506-512`).

### `toasty-sql`

- `Serializer` chooses dialect for quoting/placeholders
  (`serializer/dialect.rs:71-78`, `serializer/ident.rs:5-19`): `?`, `?N`, `$N`.
- DDL generation: `MigrationStatement::from_diff` (`migration.rs:73-239`);
  `stmt::CreateTable::create_table` (`stmt/create_table.rs:24-41`);
  `CreateIndex::create_index` (`stmt/create_index.rs:26-44`).
- Not reusable for CQL without a new `Dialect` and auditing every dialect match;
  recommended design does **not** use it for DML.

---

## 7. Schema and DDL for Cassandra

- `push_schema` must emit, per `schema.db.tables` entry:
  `CREATE TABLE [IF NOT EXISTS] ks.name (... , PRIMARY KEY ((partition_cols), clustering_cols))`.
- Primary key composition: `Table.primary_key.columns` (ordered) plus
  `IndexColumn.scope` (`Partition` vs `Local`) — the same data DynamoDB uses.
  `PrimaryKey` type at `crates/toasty-core/src/schema/db/pk.rs:24-30`.
- Secondary indexes: `CREATE INDEX` (or Scylla/SAI) per non-PK `Index`;
  Cassandra indexes are not unique → reject `index.unique`.
- Migrations: `generate_migration` can return `Migration::new_sql_with_breakpoints`
  (CQL is SQL-ish for DDL), or be `unimplemented!()` like DynamoDB initially.
- `reset_db`: drop keyspace / truncate tables.
- `Table`/`Column`/`Index` shapes: `crates/toasty-core/src/schema/db/{table.rs,column.rs,index.rs}`.
- `db::Type` variants: `Boolean, Integer(u8), UnsignedInteger(u8), Float(u8), Text,
  VarChar(u64), Uuid, Numeric, Blob, Binary, Timestamp, Date, Time, DateTime, Cidr,
  Inet, MacAddr, MacAddr8, Enum, List, Document, Json, Jsonb, Custom` 
  (`crates/toasty-core/src/schema/db/ty.rs:64-148`).
- `Column { id, name, ty: stmt::Type, storage_ty: db::Type, nullable, primary_key,
  auto_increment, versionable }` (`db/column.rs:32-62`).

### Suggested CQL type mapping

| `db::Type` | CQL |
|---|---|
| `Boolean` | `boolean` |
| `Integer(1/2/4/8)` | `tinyint` / `smallint` / `int` / `bigint` |
| `UnsignedInteger(_)` | next-wider signed (`smallint` / `int` / `bigint` / `varint`?) |
| `Float(4/8)` | `float` / `double` |
| `Text` | `text` |
| `Uuid` | `uuid` |
| `Blob` / `Binary` | `blob` |
| `Timestamp` | `timestamp` |
| `Date` | `date` |
| `Time` | `time` |
| `DateTime` | `timestamp` |
| `Numeric` | `decimal` |
| `List(inner)` | `list<inner>` |
| `Document { .. }` / `Object` | `map<text, text>` or `blob` (JSON) |

### `stmt::Value` variants — `crates/toasty-core/src/stmt/value.rs:36-147`

`Bool, I8..I64, U8..U64, F32, F64, SparseRecord, Null, Record, Object, List,
String, Bytes, Uuid`, plus feature-gated `Decimal, BigDecimal, Timestamp, Zoned,
Date, Time, DateTime, Cidr, Inet, MacAddr, MacAddr8`.

Result decoding is type-directed by `ret: Option<Vec<stmt::Type>>`;
KV ops carry `ColumnId` sets and the driver resolves `column.ty` from `schema.db`.
DynamoDB's `value.rs` is the template for a typed `to_cql` / `from_cql` layer.

---

## 8. Examples

- Examples live in `examples/<name>/`, are workspace members (root `Cargo.toml:21-28`),
  `publish = false`.
- Convention: crate-level doc comment, `#[derive(toasty::Model)]`, `#[tokio::main]`,
  read `TOASTY_CONNECTION_URL` (default `sqlite::memory:`), then
  `Db::builder().models(toasty::models!(crate::*)).connect(&url).await?`,
  `db.push_schema().await?`, then CRUD demo.
- `examples/quickstart-blog/src/main.rs:1-9` is the minimal template.
- CI runs examples per backend, e.g.
  `cargo run -p example-quickstart-blog --features postgresql` (ci.yml).
- Index of examples in `examples/README.md:10-18`.

---

## 9. Questions still open (resolve before implementing)

1. **Client crate**: `scylla` (async, Scylla-first but speaks CQL to Cassandra;
   supports shard-aware pool, prepared statements) vs `cdrs-tokio` vs `cassandra-cpp`/`kdb`.
   Recommend `scylla` async with `scylla::client::session::Session` and prepared statements.
2. **Keyspace strategy**: connection URL form (`cassandra://host:9042/keyspace`),
   create keyspace in `push_schema` (replication factor config?), or require it to exist.
3. **Scan semantics**: toasty's `Scan` means full-table scan. CQL requires
   `ALLOW FILTERING` for arbitrary predicates. Options: (a) implement `Scan` with
   `ALLOW FILTERING`, `scan: true`; (b) `scan: false` and force index coverage
   (harsher but safe). Recommend (a) plus `scan_supports_sort: false`.
4. **Updates**: `UpdateByKey`/`DeleteByKey` include `filter` + `condition`. CQL
   cannot filter in `UPDATE`/`DELETE` beyond key columns. Options: read-modify-write
   in the driver, or reject non-key filters with `unsupported_feature`.
5. **`Upsert`**: CQL `INSERT` is naturally an upsert; `IF NOT EXISTS` covers
   `UpsertAction::Ignore`. `UpsertAction::Update` needs `UPDATE ... IF` or a
   read-modify-write. Decide how much to support.
6. **`#[version]` OCC**: engine only supports it via SQL CTE/RMW. Either add an
   LWT path in the engine (`UPDATE ... IF version = ?`) or document as unsupported.
7. **`Row`/record shape**: `GetByKey` keys may be a scalar or `Value::Record` in PK
   order — driver must normalize.
8. **Collections**: `List`/`Object` documents — decide between native
   `list`/`map` and JSON-in-text (DynamoDB uses native). Affects `vec_*` and
   `document_collections` flags.
9. **Prepared-statement caching**: DynamoDB has no equivalent; SQL drivers cache.
   Decide per client crate.
10. **Naming**: crate `toasty-driver-cassandra` with a `Scylla`/`Cassandra` alias?
    URL schemes `cassandra://` and `scylla://`.

---

## 10. Points to verify by reading before coding

- `crates/toasty-core/src/driver/capability.rs` — exact `DYNAMODB` constant and
  `Capability::validate()` invariants.
- `crates/toasty-driver-dynamodb/src/op/create_table.rs` — partition/clustering key
  derivation, index handling.
- `crates/toasty-driver-dynamodb/src/lib.rs:229-252` — dispatch and error style.
- `crates/toasty/src/db/connect.rs:43-130` — scheme arms.
- `tests/tests/dynamodb.rs:54-103` — capability flag list to mirror.
- `crates/toasty/src/engine/plan/statement.rs:1576-1605` — what queries become for
  non-SQL.
- `crates/toasty-core/src/schema/db/index.rs:104-173` — `IndexScope` semantics.
