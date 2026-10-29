#![warn(missing_docs)]

//! Toasty driver for [Apache Cassandra](https://cassandra.apache.org/) and
//! [ScyllaDB](https://www.scylladb.com/).
//!
//! The driver speaks CQL through the
//! [`scylla`](https://docs.rs/scylla) Rust driver, which is optimized for
//! ScyllaDB but also compatible with Apache Cassandra.
//!
//! # Examples
//!
//! ```ignore
//! # async fn example() -> toasty_core::Result<()> {
//! use toasty_driver_cassandra::Cassandra;
//!
//! let db = toasty::Db::builder()
//!     .models(toasty::models!(crate::*))
//!     .connect("cassandra://127.0.0.1:9042/toasty")
//!     .await?;
//! # Ok(())
//! # }
//! ```

mod cql;
mod expr;
mod op;
mod schema;
mod value;

use async_trait::async_trait;
use scylla::{
    client::{session::Session, session_builder::SessionBuilder},
    errors::{ExecutionError, PrepareError, RequestAttemptError},
    statement::prepared::PreparedStatement,
};
use std::{
    borrow::Cow,
    collections::HashMap,
    fmt,
    sync::{Arc, Mutex, OnceLock},
};
use toasty_core::{
    Error, Result, Schema,
    driver::{
        Capability, ConnectContext, Driver, ExecResponse, QueryLogConfig, log::QueryLog,
        operation::Operation,
    },
    schema::{
        db::{AppliedMigration, Migration},
        diff,
    },
};
use tokio::sync::OnceCell;

/// Connection settings parsed from a `cassandra://` or `scylla://` URL.
#[derive(Clone)]
pub(crate) struct Config {
    pub(crate) keyspace: String,
    pub(crate) replication_factor: u16,
    pub(crate) network_topology: bool,
    pub(crate) local_datacenter: Option<String>,
    pub(crate) contact_points: Vec<String>,
    pub(crate) username: Option<String>,
    pub(crate) password: Option<String>,
    pub(crate) page_size: i32,
    pub(crate) connection_timeout: Option<std::time::Duration>,
}

impl fmt::Debug for Config {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Config")
            .field("keyspace", &self.keyspace)
            .field("replication_factor", &self.replication_factor)
            .field("network_topology", &self.network_topology)
            .field("local_datacenter", &self.local_datacenter)
            .field("contact_points", &self.contact_points)
            .field("username", &self.username)
            .field("password", &self.password.as_ref().map(|_| "***"))
            .field("page_size", &self.page_size)
            .field("connection_timeout", &self.connection_timeout)
            .finish()
    }
}

impl Config {
    #[cfg(test)]
    pub(crate) fn test_default() -> Self {
        Self {
            keyspace: "toasty_test".into(),
            replication_factor: 1,
            network_topology: false,
            local_datacenter: None,
            contact_points: vec!["127.0.0.1:9042".into()],
            username: None,
            password: None,
            page_size: 1024,
            connection_timeout: None,
        }
    }
}

/// A shared, lazily initialized session for one connection URL.
type SessionCell = Arc<OnceCell<Arc<Session>>>;

/// A Cassandra / ScyllaDB [`Driver`] backed by the `scylla` Rust driver.
///
/// One driver instance serves both Apache Cassandra and ScyllaDB; the URL
/// scheme may be `cassandra://` or `scylla://`.
#[derive(Clone)]
pub struct Cassandra {
    url: String,
    config: Arc<Config>,
    session: SessionCell,
}

impl fmt::Debug for Cassandra {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Cassandra")
            .field("url", &self.url)
            .field("config", &self.config)
            .finish()
    }
}

impl Cassandra {
    /// Creates a driver from a connection URL.
    ///
    /// The URL takes the form
    /// `cassandra://[user[:password]@]host[:port]/keyspace[?key=value...]`.
    /// The `scylla` scheme is accepted as an alias.
    pub fn new(url: impl Into<String>) -> Result<Self> {
        let url = url.into();
        let parsed = toasty_core::driver::ConnectionUrl::parse(&url)?;

        if !parsed.has_scheme("cassandra") && !parsed.has_scheme("scylla") {
            return Err(Error::invalid_connection_url(format!(
                "expected a `cassandra://` or `scylla://` URL, got `{url}`"
            )));
        }

        let host = parsed.host()?.unwrap_or("127.0.0.1");
        let port = parsed.port()?.unwrap_or(9042);
        let node = if host.contains(':') {
            format!("[{host}]:{port}")
        } else {
            format!("{host}:{port}")
        };

        let keyspace = parsed.decoded_path()?.trim_matches('/').to_string();

        if keyspace.is_empty() {
            return Err(Error::invalid_connection_url(
                "Cassandra connection URL must name a keyspace",
            ));
        }

        let username = parsed.username()?.map(Cow::into_owned);
        let password = parsed
            .password()
            .map(|password| String::from_utf8_lossy(&password).into_owned());

        if username.is_some() != password.is_some() {
            return Err(Error::invalid_connection_url(
                "Cassandra connection URL must provide both a username and a password",
            ));
        }

        let mut config = Config {
            keyspace,
            replication_factor: 1,
            network_topology: false,
            local_datacenter: None,
            contact_points: vec![node],
            username,
            password,
            page_size: 1024,
            connection_timeout: None,
        };

        for (key, value) in parsed.query_pairs() {
            match key.as_ref() {
                "replication_factor" => {
                    config.replication_factor = value.parse().map_err(|_| {
                        Error::invalid_connection_url(
                            "`replication_factor` must be a positive integer",
                        )
                    })?;
                }
                "replication_class" => match value.as_ref() {
                    "SimpleStrategy" => config.network_topology = false,
                    "NetworkTopologyStrategy" => config.network_topology = true,
                    other => {
                        return Err(Error::invalid_connection_url(format!(
                            "unsupported `replication_class` `{other}`"
                        )));
                    }
                },
                "local_datacenter" => config.local_datacenter = Some(value.into_owned()),
                "page_size" => {
                    config.page_size = value.parse().map_err(|_| {
                        Error::invalid_connection_url("`page_size` must be an integer")
                    })?;
                }
                "connection_timeout_ms" => {
                    let millis: u64 = value.parse().map_err(|_| {
                        Error::invalid_connection_url("`connection_timeout_ms` must be an integer")
                    })?;
                    config.connection_timeout = Some(std::time::Duration::from_millis(millis));
                }
                "contact_point" => config.contact_points.push(value.into_owned()),
                other => {
                    return Err(Error::invalid_connection_url(format!(
                        "unsupported Cassandra URL parameter `{other}`"
                    )));
                }
            }
        }

        let session = session_cell(&url);

        Ok(Self {
            url: parsed.redact_password().into_owned(),
            config: Arc::new(config),
            session,
        })
    }

    /// The keyspace this driver connects to.
    pub fn keyspace(&self) -> &str {
        &self.config.keyspace
    }

    /// Creates a driver that uses an already-established session.
    ///
    /// This is useful for test harnesses that own the runtime the session was
    /// created on, or that share one session across many driver instances.
    pub fn with_session(url: impl Into<String>, session: Arc<Session>) -> Result<Self> {
        let mut driver = Self::new(url)?;
        let cell = Arc::new(OnceCell::new());
        let _ = cell.set(session);
        driver.session = cell;
        Ok(driver)
    }

    /// Returns the shared session, connecting on first use.
    ///
    /// This is primarily useful for test harnesses that need to issue cleanup
    /// statements through the same session.
    pub async fn session(&self) -> Result<Arc<Session>> {
        self.session
            .get_or_try_init(|| async { self.build_session().await })
            .await
            .cloned()
    }

    async fn build_session(&self) -> Result<Arc<Session>> {
        let mut builder = SessionBuilder::new();

        for contact_point in &self.config.contact_points {
            builder = builder.known_node(contact_point);
        }

        if let (Some(username), Some(password)) = (&self.config.username, &self.config.password) {
            builder = builder.user(username, password);
        }

        if let Some(timeout) = self.config.connection_timeout {
            builder = builder.connection_timeout(timeout);
        }

        // The session spawns upkeep tasks on the runtime that builds it. Build
        // it on a process-lifetime runtime so short-lived caller runtimes
        // (notably one-per-test runtimes) cannot tear the session down.
        let session = runtime()
            .spawn(async move { builder.build().await })
            .await
            .map_err(Error::driver_operation_failed)?
            .map_err(Error::driver_operation_failed)?;

        Ok(Arc::new(session))
    }
}

/// The runtime that owns the shared Cassandra sessions.
///
/// A dedicated thread keeps this runtime alive for the process lifetime.
fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<&'static tokio::runtime::Runtime> = OnceLock::new();

    RUNTIME.get_or_init(|| {
        let (tx, rx) = std::sync::mpsc::channel();

        std::thread::Builder::new()
            .name("toasty-cassandra-runtime".into())
            .spawn(move || {
                let runtime: &'static tokio::runtime::Runtime = Box::leak(Box::new(
                    tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("failed to build the Cassandra runtime"),
                ));

                tx.send(runtime).expect("runtime receiver dropped");
                runtime.block_on(std::future::pending::<()>());
            })
            .expect("failed to spawn the Cassandra runtime thread");

        rx.recv().expect("failed to start the Cassandra runtime")
    })
}

fn session_cell(url: &str) -> SessionCell {
    static SESSIONS: OnceLock<Mutex<HashMap<String, SessionCell>>> = OnceLock::new();

    let sessions = SESSIONS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut sessions = sessions
        .lock()
        .expect("Cassandra session registry poisoned");

    sessions
        .entry(url.to_owned())
        .or_insert_with(|| Arc::new(OnceCell::new()))
        .clone()
}

pub(crate) fn capability() -> &'static Capability {
    &Capability::CASSANDRA
}

#[async_trait]
impl Driver for Cassandra {
    fn url(&self) -> Cow<'_, str> {
        Cow::Borrowed(&self.url)
    }

    fn capability(&self) -> &'static Capability {
        capability()
    }

    async fn connect(
        &self,
        cx: &ConnectContext,
    ) -> Result<Box<dyn toasty_core::driver::Connection>> {
        let session = self.session().await?;

        Ok(Box::new(Connection {
            session,
            config: self.config.clone(),
            query_log: cx.query_log,
            prepared: HashMap::new(),
        }))
    }

    fn generate_migration(&self, _schema_diff: &diff::Schema<'_>) -> Migration {
        unimplemented!(
            "Cassandra migrations are not yet supported; apply CQL manually or use `push_schema`"
        )
    }

    async fn reset_db(&self) -> Result<()> {
        let session = self.session().await?;

        session
            .query_unpaged(schema::drop_keyspace(&self.config), &[])
            .await
            .map_err(map_execution_error)?;

        session
            .query_unpaged(schema::create_keyspace(&self.config), &[])
            .await
            .map_err(map_execution_error)?;

        session
            .await_schema_agreement()
            .await
            .map_err(Error::driver_operation_failed)?;

        Ok(())
    }
}

/// An open connection to Cassandra / ScyllaDB.
pub struct Connection {
    session: Arc<Session>,
    config: Arc<Config>,
    query_log: QueryLogConfig,
    prepared: HashMap<String, PreparedStatement>,
}

impl fmt::Debug for Connection {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Connection")
            .field("keyspace", &self.config.keyspace)
            .finish()
    }
}

#[async_trait]
impl toasty_core::driver::Connection for Connection {
    async fn exec(&mut self, schema: &Arc<Schema>, op: Operation) -> Result<ExecResponse> {
        let log = QueryLog::operation(
            &self.query_log,
            "cassandra",
            op.name(),
            op_table_name(schema, &op),
        );

        let result = self.exec2(schema, op).await;
        log.finish(&result);
        result
    }

    async fn ping(&mut self) -> Result<()> {
        self.session
            .query_unpaged("SELECT release_version FROM system.local", &[])
            .await
            .map_err(map_execution_error)?;
        Ok(())
    }

    async fn push_schema(&mut self, schema: &Schema) -> Result<()> {
        schema::validate(&schema.db, &self.config)?;

        self.execute_ddl(&schema::create_keyspace(&self.config))
            .await?;
        self.session
            .await_schema_agreement()
            .await
            .map_err(Error::driver_operation_failed)?;

        for table in &schema.db.tables {
            tracing::debug!(table = %table.name, "creating Cassandra table");
            self.execute_ddl(&schema::create_table(&self.config, table)?)
                .await?;
        }

        for table in &schema.db.tables {
            for index in &table.indices {
                if let Some(cql) = schema::create_index(&self.config, table, index)? {
                    tracing::debug!(index = %index.name, "creating Cassandra index");
                    self.execute_ddl(&cql).await?;
                }
            }
        }

        self.session
            .await_schema_agreement()
            .await
            .map_err(Error::driver_operation_failed)?;

        Ok(())
    }

    async fn applied_migrations(&mut self) -> Result<Vec<AppliedMigration>> {
        Err(Error::unsupported_feature(
            "Cassandra migrations are not yet supported",
        ))
    }

    async fn apply_migration(
        &mut self,
        _id: u64,
        _name: &str,
        _migration: &Migration,
    ) -> Result<()> {
        Err(Error::unsupported_feature(
            "Cassandra migrations are not yet supported",
        ))
    }
}

impl Connection {
    async fn exec2(&mut self, schema: &Arc<Schema>, op: Operation) -> Result<ExecResponse> {
        match op {
            Operation::GetByKey(op) => self.exec_get_by_key(schema, op).await,
            Operation::QueryPk(op) => self.exec_query_pk(schema, op).await,
            Operation::Scan(op) => self.exec_scan(schema, op).await,
            Operation::Insert(op) => self.exec_insert(&schema.db, op).await,
            Operation::UpdateByKey(op) => self.exec_update_by_key(&schema.db, op).await,
            Operation::DeleteByKey(op) => self.exec_delete_by_key(&schema.db, op).await,
            Operation::Upsert(op) => self.exec_upsert(&schema.db, op).await,
            Operation::FindPkByIndex(op) => self.exec_find_pk_by_index(schema, op).await,
            Operation::QuerySql(_) => Err(Error::unsupported_feature(
                "QuerySql is only supported by SQL drivers",
            )),
            Operation::RawSql(_) => Err(Error::unsupported_feature(
                "raw SQL is only supported by SQL drivers; the Cassandra driver does not expose raw CQL",
            )),
            Operation::Transaction(_) => Err(Error::unsupported_feature(
                "transactions are not supported by the Cassandra driver",
            )),
        }
    }
}

fn op_table_name<'a>(schema: &'a Schema, op: &Operation) -> Option<&'a str> {
    let table = match op {
        Operation::GetByKey(op) => op.table,
        Operation::QueryPk(op) => op.table,
        Operation::DeleteByKey(op) => op.table,
        Operation::UpdateByKey(op) => op.table,
        Operation::Upsert(op) => op.stmt.target.as_table_unwrap().table,
        Operation::FindPkByIndex(op) => op.table,
        Operation::Scan(op) => op.table,
        _ => return None,
    };

    Some(&schema.db.table(table).name)
}

pub(crate) fn map_execution_error(error: ExecutionError) -> Error {
    match &error {
        ExecutionError::ConnectionPoolError(_)
        | ExecutionError::LastAttemptError(RequestAttemptError::BrokenConnectionError(_)) => {
            Error::connection_lost(error)
        }
        _ => Error::driver_operation_failed(error),
    }
}

pub(crate) fn map_prepare_error(error: PrepareError) -> Error {
    Error::driver_operation_failed(error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dedicated_runtime_executes_spawned_tasks() {
        let value = runtime().spawn(async { 42 }).await.unwrap();
        assert_eq!(value, 42);
    }

    #[test]
    fn parses_cassandra_and_scylla_urls() {
        let cassandra = Cassandra::new("cassandra://user:pass@example.com:9042/ks").unwrap();
        assert_eq!(cassandra.keyspace(), "ks");
        assert!(!cassandra.url().contains("pass"));

        let scylla = Cassandra::new("scylla://127.0.0.1/toasty?replication_factor=3").unwrap();
        assert_eq!(scylla.keyspace(), "toasty");
        assert_eq!(scylla.config.replication_factor, 3);
    }

    #[test]
    fn rejects_urls_without_a_keyspace() {
        assert!(Cassandra::new("cassandra://127.0.0.1:9042").is_err());
    }
}
