use toasty_core::{
    Error, Result,
    driver::{ExecResponse, operation},
    schema::db,
    stmt,
};

use crate::{Connection, cql, op::ttl_bind, schema, value};

impl Connection {
    pub(crate) async fn exec_insert(
        &mut self,
        schema: &db::Schema,
        op: operation::Insert,
    ) -> Result<ExecResponse> {
        let insert = op.stmt.into_insert_unwrap();
        let ttl = insert.ttl;

        if insert.returning.is_some() {
            return Err(Error::unsupported_feature(
                "Cassandra does not support RETURNING on INSERT",
            ));
        }

        let target = insert.target.as_table_unwrap();
        let table = schema.table(target.table);

        let stmt::ExprSet::Values(values) = &insert.source.body else {
            return Err(Error::invalid_statement(
                "Cassandra inserts require a VALUES source",
            ));
        };

        let columns = target
            .columns
            .iter()
            .map(|column| schema.column(*column))
            .collect::<Vec<_>>();

        let column_list = cql::column_list(&columns);
        let placeholders = vec!["?"; columns.len()].join(", ");
        let versioned = table.columns.iter().any(|column| column.versionable);
        let cql = insert_cql(
            &schema::qualified(&self.config, &table.name),
            &column_list,
            &placeholders,
            versioned,
            ttl.is_some(),
        );

        let ttl_bind = ttl_bind(ttl)?;

        let mut count = 0_u64;

        for row in &values.rows {
            let mut binds = Vec::with_capacity(columns.len() + usize::from(ttl_bind.is_some()));

            for (position, column) in columns.iter().enumerate() {
                let value = row_value(row, position)?;
                binds.push(value::encode_for_column(column, value)?);
            }

            if let Some(bind) = &ttl_bind {
                binds.push(Some(bind.clone()));
            }

            if versioned {
                if !self.execute_lwt(&cql, &binds).await? {
                    return Err(Error::condition_failed(
                        "Cassandra insert conflicts with an existing primary key",
                    ));
                }
            } else {
                self.query_all(&cql, &binds).await?;
            }
            count += 1;
        }

        Ok(ExecResponse::count(count))
    }
}

fn row_value(row: &stmt::Expr, position: usize) -> Result<&stmt::Value> {
    match row.entry(position) {
        Some(stmt::Entry::Value(value)) | Some(stmt::Entry::Expr(stmt::Expr::Value(value))) => {
            Ok(value)
        }
        _ => Err(Error::invalid_statement(format!(
            "Cassandra insert row entry did not lower to a literal at position {position}: {row:#?}"
        ))),
    }
}

/// Renders an `INSERT`. `versioned` adds the `IF NOT EXISTS` clause and
/// `has_ttl` adds `USING TTL ?`; CQL places the TTL clause after
/// `IF NOT EXISTS`, so its bind is last.
pub(super) fn insert_cql(
    qualified: &str,
    column_list: &str,
    placeholders: &str,
    versioned: bool,
    has_ttl: bool,
) -> String {
    format!(
        "INSERT INTO {qualified} ({column_list}) VALUES ({placeholders}){}{}",
        if versioned { " IF NOT EXISTS" } else { "" },
        if has_ttl { " USING TTL ?" } else { "" }
    )
}

#[cfg(test)]
mod tests {
    use super::insert_cql;

    #[test]
    fn insert_cql_without_ttl_is_unchanged() {
        assert_eq!(
            insert_cql("ks.things", "\"id\"", "?", false, false),
            "INSERT INTO ks.things (\"id\") VALUES (?)"
        );
        assert_eq!(
            insert_cql("ks.things", "\"id\"", "?", true, false),
            "INSERT INTO ks.things (\"id\") VALUES (?) IF NOT EXISTS"
        );
    }

    #[test]
    fn insert_cql_places_ttl_after_if_not_exists() {
        assert_eq!(
            insert_cql("ks.things", "\"id\"", "?", false, true),
            "INSERT INTO ks.things (\"id\") VALUES (?) USING TTL ?"
        );
        assert_eq!(
            insert_cql("ks.things", "\"id\"", "?", true, true),
            "INSERT INTO ks.things (\"id\") VALUES (?) IF NOT EXISTS USING TTL ?"
        );
    }
}
