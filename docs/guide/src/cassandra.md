# Cassandra and ScyllaDB

Toasty's Cassandra driver speaks CQL through the official [ScyllaDB Rust
driver][scylla-driver]. Although that driver is optimized for ScyllaDB, it is
also compatible with Apache Cassandra, so one Toasty driver — and one URL
scheme family — covers both servers.

Cassandra is a wide-column store: it has no joins, no interactive
transactions, and a deliberately restricted `WHERE` clause. Toasty maps those
differences onto its key-value driver path instead of pretending they do not
exist. Familiar model code — [`create!`](./creating-records.md),
[`find_by_pk`](./querying-records.md),
[`filter_by_<indexed>`](./querying-records.md#filtering-by-indexed-fields),
[`update()`](./updating-records.md), [`delete()`](./deleting-records.md), and
[associations](./relationships.md) — still works, with the limits below.

[scylla-driver]: https://github.com/scylladb/scylla-rust-driver

## Enabling the driver

Add the `cassandra` feature to Toasty in `Cargo.toml`:

```toml
[dependencies]
toasty = { version = "{{toasty_version}}", features = ["cassandra"] }
```

Then pass a `cassandra://` (or `scylla://`) URL to `Db::builder`. The path
component names the keyspace:

```rust,ignore
let db = toasty::Db::builder()
    .models(toasty::models!(crate::*))
    .connect("cassandra://127.0.0.1:9042/my_keyspace")
    .await?;
```

Username and password are part of the URL:
`cassandra://user:password@host:9042/keyspace`. Supported query parameters:

| Parameter | Default | Meaning |
|---|---|---|
| `replication_factor` | `1` | Keyspace replication factor created by `push_schema` |
| `replication_class` | `SimpleStrategy` | `SimpleStrategy` or `NetworkTopologyStrategy` |
| `local_datacenter` | — | Datacenter name for `NetworkTopologyStrategy` |
| `page_size` | `1024` | Server paging size for cursor pagination |
| `connection_timeout_ms` | driver default | Connection timeout |
| `contact_point` | — | Additional host, repeatable |

`push_schema` creates the keyspace with `CREATE KEYSPACE IF NOT EXISTS` and
then creates every table and single-column secondary index. To use an existing
keyspace, create it yourself and grant the connection permission to create
tables in it.

The connection pool described in [Database Setup](./database-setup.md) does
not apply: the Rust driver owns its session and connections, so
`max_pool_size` and the related knobs have no effect.

## Keys and partitioning

Cassandra tables are defined by a partition key and, optionally, clustering
columns. Toasty exposes that split through the `#[key]` attribute:

```rust,ignore
#[derive(Debug, toasty::Model)]
#[key(partition = author_id, local = id)]
struct Post {
    #[auto]
    id: uuid::Uuid,
    author_id: uuid::Uuid,
    title: String,
}
```

This becomes `PRIMARY KEY ((author_id), id)`: rows are partitioned by
`author_id` and ordered by `id` within a partition. A plain `#[key]` on one
field is a partition-only key. Composite partition keys use
`partition = [a, b]`.

UUIDs are generated on the client, so `#[auto]` on a `uuid::Uuid` works without
a server sequence. `#[auto]` on an integer does not: Cassandra has no
auto-increment columns, and the schema build rejects it.

## Type mapping

| Rust / Toasty type | CQL type |
|---|---|
| `bool` | `boolean` |
| `i8` / `i16` / `i32` / `i64` | `tinyint` / `smallint` / `int` / `bigint` |
| `u8` / `u16` / `u32` | the next wider signed integer |
| `u64` | `bigint`, limited to `i64::MAX` |
| `f32` / `f64` | `float` / `double` |
| `String` | `text` |
| `uuid::Uuid` | `uuid` |
| `Vec<u8>` | `blob` |
| `Vec<scalar>` | `list<…>` |
| timestamps, dates, times, decimals, network types | `text` |
| `#[document]` / embedded structs | JSON in a `text` column |
| enums | `text` |

Temporal values use the same fixed-precision text encoding as the other
non-native backends. Cassandra's native `timestamp` is millisecond precision,
which would silently truncate Toasty's nanosecond values.

## Supported queries

- Full primary-key reads and writes (`get_by_…`).
- Partition-key queries with clustering-column filters and ordering.
- Full-table scans, with `ALLOW FILTERING` when the predicate is not covered by
  the key.
- `LIMIT` and offset pagination. Cursor pagination uses Cassandra's opaque
  paging state; it is forward-only, so `prev()` is unavailable.
- `Vec<scalar>` literals, `CONTAINS`, list append (`push`/`extend`), and
  remove-by-value.
- `#[version]` optimistic concurrency through CQL lightweight transactions
  (`UPDATE … IF version = ?`).
- Upserts by primary key: `INSERT … IF NOT EXISTS` for ignore, and a
  read-then-write update for the update branch.

## Unsupported or restricted

- **No transactions.** `db.transaction()` returns `unsupported_feature`.
  Individual writes are atomic, but a multi-statement operation is not.
- **No joins.** Associations are loaded with separate batched queries and
  merged in the client, which is how Toasty handles every key-value backend.
  Multi-step `via` relations are rejected.
- **No unique constraints.** Cassandra cannot enforce them. `#[unique]` is
  accepted as schema metadata (so `filter_by_<field>` methods are still
  generated), but no enforcing index is created. The test suite's uniqueness
  tests are skipped for this driver.
- **No `count()` and no raw CQL.** Both are rejected for key-value drivers.
- **`LIKE` is text-only.** `.starts_with()` becomes `LIKE 'prefix%'` and needs
  `ALLOW FILTERING` unless the column is part of the key. `.ilike()` is not
  supported.
- **No `RETURNING`.** Values are either known to the client or must be read
  back.
- **Secondary indexes are single-column and non-unique.** Composite or
  collection indexes are skipped; those queries fall back to
  `ALLOW FILTERING`.
- **Secondary-index `ORDER BY` is not supported.** Ordering is available on
  clustering columns of a single partition.
- **Updates and deletes with a non-key filter** are executed as read, filter,
  write. This is not atomic with concurrent writers; use `#[version]` when that
  matters.
- **No positional list operations.** `pop()` and remove-at are rejected;
  remove-by-value is supported.
- **Schema changes are limited.** Cassandra can add and drop columns but cannot
  change a column's type, and primary keys cannot be altered. Migrations are
  not implemented yet; recreate the table when the key changes.

## Local development

The Toasty repository's `compose.yaml` starts a single-node ScyllaDB:

```bash
docker compose up -d scylla
cargo test -p tests --no-default-features --features cassandra -- --test-threads 1
cargo run -p example-cassandra-blog --no-default-features --features cassandra
```

To run the same tests against Apache Cassandra, replace the `scylla` service
with the commented `cassandra` service in `compose.yaml`.
