use toasty_core::{
    Error, Result,
    schema::db::{self, Column, Index, IndexOp, Table},
    stmt,
};

use crate::Config;

/// Returns the CQL statement that creates the configured keyspace.
pub(crate) fn create_keyspace(config: &Config) -> String {
    let keyspace = quote(&config.keyspace);

    let replication = if config.network_topology {
        let datacenter = config.local_datacenter.as_deref().unwrap_or("datacenter1");
        let datacenter = datacenter.replace('\'', "''");
        format!(
            "{{'class': 'NetworkTopologyStrategy', '{}': {}}}",
            datacenter, config.replication_factor
        )
    } else {
        format!(
            "{{'class': 'SimpleStrategy', 'replication_factor': {}}}",
            config.replication_factor
        )
    };

    format!("CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH replication = {replication}")
}

/// Returns the CQL statement that drops the configured keyspace.
pub(crate) fn drop_keyspace(config: &Config) -> String {
    format!("DROP KEYSPACE IF EXISTS {}", quote(&config.keyspace))
}

/// Validates the parts of a schema that Cassandra cannot represent.
pub(crate) fn validate(schema: &db::Schema, config: &Config) -> Result<()> {
    check_identifier(&config.keyspace)?;

    for table in &schema.tables {
        check_identifier(&table.name)?;

        for column in &table.columns {
            check_identifier(&column.name)?;

            if column.auto_increment {
                return Err(Error::invalid_schema(format!(
                    "Cassandra has no auto-increment columns; table `{}` column `{}` is marked auto-increment",
                    table.name, column.name
                )));
            }
        }

        for index in &table.indices {
            check_identifier(&index.name)?;

            if index.unique && !index.primary_key {
                // Cassandra has no unique secondary indexes. Keep the column
                // metadata (the engine still generates `filter_by_*` methods)
                // but do not create an index; `Capability::unique_index` is
                // false so tests that require enforcement are skipped.
                tracing::warn!(
                    table = %table.name,
                    index = %index.name,
                    "Cassandra cannot enforce unique indexes; skipping index creation"
                );
            }
        }

        let (partition, clustering) = primary_key_parts(table)?;
        if partition.is_empty() {
            return Err(Error::invalid_schema(format!(
                "Cassandra table `{}` must have at least one partition-key column",
                table.name
            )));
        }

        for column in partition.iter().chain(clustering.iter()) {
            if column.nullable {
                return Err(Error::invalid_schema(format!(
                    "Cassandra primary-key column `{}.{}` cannot be nullable",
                    table.name, column.name
                )));
            }

            if matches!(
                column.storage_ty,
                db::Type::List(_) | db::Type::Document { .. }
            ) {
                return Err(Error::invalid_schema(format!(
                    "Cassandra primary-key column `{}.{}` cannot be a collection or document",
                    table.name, column.name
                )));
            }
        }
    }

    Ok(())
}

/// Builds `CREATE TABLE IF NOT EXISTS ...` for one table.
pub(crate) fn create_table(config: &Config, table: &Table) -> Result<String> {
    let (partition, clustering) = primary_key_parts(table)?;

    let mut definitions = Vec::with_capacity(table.columns.len() + 1);

    for column in &table.columns {
        definitions.push(format!(
            "{} {}",
            quote(&column.name),
            cql_type(&column.storage_ty)?
        ));
    }

    let mut key_parts = Vec::with_capacity(1 + clustering.len());
    if partition.len() == 1 {
        key_parts.push(quote(&partition[0].name));
    } else {
        key_parts.push(format!(
            "({})",
            partition
                .iter()
                .map(|column| quote(&column.name))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    key_parts.extend(clustering.iter().map(|column| quote(&column.name)));
    definitions.push(format!("PRIMARY KEY ({})", key_parts.join(", ")));

    let mut cql = format!(
        "CREATE TABLE IF NOT EXISTS {} ({})",
        qualified(config, &table.name),
        definitions.join(", ")
    );

    if !clustering.is_empty() {
        let pk_index = &table.indices[table.primary_key.index.index];
        let order = clustering
            .iter()
            .map(|column| {
                let direction = pk_index
                    .columns
                    .iter()
                    .find(|index_column| index_column.column == column.id)
                    .map(|index_column| index_column.op);

                let direction = match direction {
                    Some(IndexOp::Sort(stmt::Direction::Desc)) => "DESC",
                    _ => "ASC",
                };

                format!("{} {direction}", quote(&column.name))
            })
            .collect::<Vec<_>>();

        cql.push_str(&format!(" WITH CLUSTERING ORDER BY ({})", order.join(", ")));
    }

    Ok(cql)
}

/// Builds `CREATE INDEX IF NOT EXISTS ...` for one index, or `None` when
/// Cassandra cannot represent the index.
pub(crate) fn create_index(
    config: &Config,
    table: &Table,
    index: &Index,
) -> Result<Option<String>> {
    if index.primary_key {
        return Ok(None);
    }

    if index.unique {
        // Cassandra has no unique secondary indexes; the index is accepted as
        // schema metadata but not materialized.
        return Ok(None);
    }

    // Cassandra's built-in secondary indexes cover exactly one column.
    let [index_column] = index.columns.as_slice() else {
        return Ok(None);
    };

    let column = table.column(index_column.column);

    if column.primary_key
        || matches!(
            column.storage_ty,
            db::Type::List(_) | db::Type::Document { .. }
        )
    {
        return Ok(None);
    }

    let name = if index.name.is_empty() {
        format!("{}_{}_idx", table.name, column.name)
    } else {
        index.name.clone()
    };
    check_identifier(&name)?;

    Ok(Some(format!(
        "CREATE INDEX IF NOT EXISTS {} ON {} ({})",
        quote(&name),
        qualified(config, &table.name),
        quote(&column.name)
    )))
}

/// Returns the quoted, keyspace-qualified name of a table.
pub(crate) fn qualified(config: &Config, table: &str) -> String {
    format!("{}.{}", quote(&config.keyspace), quote(table))
}

fn primary_key_parts(table: &Table) -> Result<(Vec<&Column>, Vec<&Column>)> {
    let index = table
        .indices
        .get(table.primary_key.index.index)
        .filter(|index| index.primary_key);

    let Some(index) = index else {
        // The primary-key index should always be present; fall back to the
        // ordered primary-key columns as a partition key rather than failing.
        return Ok((table.primary_key_columns().collect(), Vec::new()));
    };

    let partition = index
        .columns
        .iter()
        .filter(|index_column| index_column.scope.is_partition())
        .map(|index_column| table.column(index_column.column))
        .collect::<Vec<_>>();

    let clustering = index
        .columns
        .iter()
        .filter(|index_column| index_column.scope.is_local())
        .map(|index_column| table.column(index_column.column))
        .collect::<Vec<_>>();

    if partition.is_empty() {
        // A simple `#[key]` may not carry an explicit scope; treat the
        // primary-key columns as the partition key in declaration order.
        return Ok((table.primary_key_columns().collect(), Vec::new()));
    }

    Ok((partition, clustering))
}

fn cql_type(ty: &db::Type) -> Result<String> {
    Ok(match ty {
        db::Type::Boolean => "boolean".into(),
        db::Type::Integer(1) => "tinyint".into(),
        db::Type::Integer(2) => "smallint".into(),
        db::Type::Integer(3..=4) => "int".into(),
        db::Type::Integer(_) => "bigint".into(),
        db::Type::UnsignedInteger(1) => "smallint".into(),
        db::Type::UnsignedInteger(2) => "int".into(),
        db::Type::UnsignedInteger(_) => "bigint".into(),
        db::Type::Float(1..=4) => "float".into(),
        db::Type::Float(_) => "double".into(),
        db::Type::Text | db::Type::VarChar(_) => "text".into(),
        db::Type::Uuid => "uuid".into(),
        db::Type::Blob | db::Type::Binary(_) => "blob".into(),
        db::Type::Timestamp(_) => "timestamp".into(),
        db::Type::Date
        | db::Type::Time(_)
        | db::Type::DateTime(_)
        | db::Type::Numeric(_)
        | db::Type::Cidr
        | db::Type::Inet
        | db::Type::MacAddr
        | db::Type::MacAddr8
        | db::Type::Json
        | db::Type::Jsonb
        | db::Type::Document { .. }
        | db::Type::Enum(_) => "text".into(),
        db::Type::List(elem) => format!("list<{}>", cql_type(elem)?),
        db::Type::Custom(name) => {
            return Err(Error::unsupported_feature(format!(
                "Cassandra driver does not support custom column type `{name}`"
            )));
        }
    })
}

fn check_identifier(name: &str) -> Result<()> {
    if name.chars().count() > 48 {
        return Err(Error::invalid_schema(format!(
            "Cassandra identifiers are limited to 48 characters; `{name}` is {}",
            name.chars().count()
        )));
    }

    Ok(())
}

/// Quotes a CQL identifier.
pub(crate) fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;
    use toasty_core::schema::db::{
        ColumnId, IndexColumn, IndexId, IndexScope, PrimaryKey, TableId,
    };

    fn config() -> Config {
        Config {
            keyspace: "ks".into(),
            replication_factor: 1,
            network_topology: false,
            local_datacenter: None,
            ..Config::test_default()
        }
    }

    fn column(index: usize, name: &str, ty: stmt::Type) -> Column {
        Column {
            id: ColumnId {
                table: TableId(0),
                index,
            },
            name: name.into(),
            ty: ty.clone(),
            storage_ty: db::Type::from_app(&ty, None, &crate::capability().storage_types).unwrap(),
            nullable: false,
            primary_key: true,
            auto_increment: false,
            versionable: false,
        }
    }

    fn table(partition: bool) -> Table {
        let columns = vec![
            column(0, "tenant", stmt::Type::Uuid),
            column(1, "id", stmt::Type::Uuid),
            column(2, "name", stmt::Type::String),
        ];

        let scope = if partition {
            IndexScope::Partition
        } else {
            IndexScope::Local
        };

        Table {
            id: TableId(0),
            name: "users".into(),
            columns,
            primary_key: PrimaryKey {
                columns: vec![
                    ColumnId {
                        table: TableId(0),
                        index: 0,
                    },
                    ColumnId {
                        table: TableId(0),
                        index: 1,
                    },
                ],
                index: IndexId {
                    table: TableId(0),
                    index: 0,
                },
            },
            indices: vec![Index {
                id: IndexId {
                    table: TableId(0),
                    index: 0,
                },
                name: "users_pk".into(),
                on: TableId(0),
                columns: vec![
                    IndexColumn {
                        column: ColumnId {
                            table: TableId(0),
                            index: 0,
                        },
                        op: IndexOp::Eq,
                        scope,
                    },
                    IndexColumn {
                        column: ColumnId {
                            table: TableId(0),
                            index: 1,
                        },
                        op: IndexOp::Sort(stmt::Direction::Desc),
                        scope: IndexScope::Local,
                    },
                ],
                unique: false,
                primary_key: true,
            }],
        }
    }

    #[test]
    fn creates_keyspace() {
        assert_eq!(
            create_keyspace(&config()),
            "CREATE KEYSPACE IF NOT EXISTS \"ks\" WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
        );
    }

    #[test]
    fn renders_timestamp_columns_natively() {
        assert_eq!(cql_type(&db::Type::Timestamp(3)).unwrap(), "timestamp");
    }

    #[test]
    fn creates_partition_and_clustering_key() {
        let cql = create_table(&config(), &table(true)).unwrap();
        assert!(cql.contains("PRIMARY KEY (\"tenant\", \"id\")"), "{cql}");
        assert!(cql.contains("CLUSTERING ORDER BY (\"id\" DESC)"), "{cql}");
    }

    #[test]
    fn falls_back_to_primary_key_as_partition_key() {
        let cql = create_table(&config(), &table(false)).unwrap();
        assert!(cql.contains("PRIMARY KEY ((\"tenant\", \"id\"))"), "{cql}");
    }

    #[test]
    fn skips_unique_indexes() {
        let mut table = table(true);
        let mut index = table.indices[0].clone();
        index.primary_key = false;
        index.unique = true;
        index.name = "users_by_tenant".into();
        table.indices.push(index);
        let schema = db::Schema {
            tables: vec![table.clone()],
        };

        assert!(validate(&schema, &config()).is_ok());
        assert!(
            create_index(&config(), &table, &table.indices[1])
                .unwrap()
                .is_none()
        );
    }
}
