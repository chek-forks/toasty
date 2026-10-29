use toasty_core::{
    Error, Result,
    driver::{ExecResponse, operation},
    schema::db,
    stmt,
};

use crate::{Connection, cql, schema, value};

impl Connection {
    pub(crate) async fn exec_insert(
        &mut self,
        schema: &db::Schema,
        op: operation::Insert,
    ) -> Result<ExecResponse> {
        let insert = op.stmt.into_insert_unwrap();

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
        let cql = format!(
            "INSERT INTO {} ({column_list}) VALUES ({placeholders}){}",
            schema::qualified(&self.config, &table.name),
            if versioned { " IF NOT EXISTS" } else { "" }
        );

        let mut count = 0_u64;

        for row in &values.rows {
            let mut binds = Vec::with_capacity(columns.len());

            for (position, column) in columns.iter().enumerate() {
                let value = row_value(row, position)?;
                binds.push(value::encode_for_column(column, value)?);
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
