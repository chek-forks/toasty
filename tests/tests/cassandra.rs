#![cfg(feature = "cassandra")]

use std::sync::OnceLock;

use toasty_core::driver::Driver;
use toasty_driver_cassandra::Cassandra;

const DEFAULT_URL: &str = "cassandra://127.0.0.1:9042/toasty_test";

fn test_url() -> String {
    std::env::var("TOASTY_TEST_CASSANDRA_URL").unwrap_or_else(|_| DEFAULT_URL.to_string())
}

fn driver() -> Cassandra {
    static DRIVER: OnceLock<Cassandra> = OnceLock::new();

    DRIVER
        .get_or_init(|| Cassandra::new(test_url()).expect("invalid TOASTY_TEST_CASSANDRA_URL"))
        .clone()
}

struct CassandraSetup;

impl CassandraSetup {
    fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl toasty_driver_integration_suite::Setup for CassandraSetup {
    fn driver(&self) -> Box<dyn Driver> {
        Box::new(driver())
    }

    async fn delete_table(&self, name: &str) {
        let driver = driver();
        let Ok(session) = driver.session().await else {
            // Cleanup must not mask the test's own failure.
            return;
        };

        let keyspace = driver.keyspace();
        let name = name.replace('"', "\"\"");
        let cql = format!("DROP TABLE IF EXISTS \"{keyspace}\".\"{name}\"");

        if let Err(error) = session.query_unpaged(cql, &[]).await {
            eprintln!("warning: failed to drop Cassandra table `{name}`: {error}");
        }
    }
}

toasty_driver_integration_suite::generate_driver_tests!(
    CassandraSetup::new(),
    sql: false,
    auto_increment: false,
    bigdecimal_implemented: false,
    decimal_arbitrary_precision: false,
    native_decimal: false,
    native_varchar: false,
    native_json: false,
    native_jsonb: false,
    native_ilike: false,
    native_timestamp: false,
    native_date: false,
    native_time: false,
    native_datetime: false,
    native_cidr: false,
    native_inet: false,
    native_macaddr: false,
    native_macaddr8: false,
    native_array: true,
    native_enum: false,
    upsert_primary_key: true,
    upsert_unique: false,
    upsert_branch_assignments: false,
    upsert_targeted_ignore: true,
    unique_index: false,
    unique_list_index: false,
    vec_scalar: true,
    vec_remove: true,
    vec_pop: false,
    vec_remove_at: false,
    document_collections: true,
    backward_pagination: false,
    test_connection_pool: false,
    transaction_lock_mode: false,
    returning_from_insert: false,
    cte_unreferenced: false,
    dynamodb_index_limits: false,
    dynamodb_key_semantics: false,
    scan: true,
    index_or_predicate: false,
    scan_supports_sort: false,
);
