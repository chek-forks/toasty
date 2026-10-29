use toasty_core::{
    Error, Result,
    driver::{ExecResponse, operation},
    schema::db,
    stmt::{self, UpsertAction, UpsertTarget, Value, ValueRecord},
};

use crate::{
    Connection, cql,
    op::{apply_assignments_with_incoming, apply_defaults, project, returning_columns},
    schema, value,
};

impl Connection {
    pub(crate) async fn exec_upsert(
        &mut self,
        schema: &db::Schema,
        op: operation::Upsert,
    ) -> Result<ExecResponse> {
        let insert = op.stmt;
        let target = insert.target.as_table_unwrap();
        let table = schema.table(target.table);
        let upsert = insert
            .upsert
            .as_ref()
            .expect("upsert operation without a clause");

        let UpsertTarget::Columns(conflict_columns) = &upsert.target else {
            return Err(Error::invalid_statement(
                "Cassandra upsert target was not lowered to columns",
            ));
        };

        let primary_key = table
            .primary_key_columns()
            .map(|column| column.id)
            .collect::<Vec<_>>();
        if conflict_columns != &primary_key {
            return Err(Error::unsupported_feature(
                "Cassandra upserts may only target the primary key",
            ));
        }

        let stmt::ExprSet::Values(values) = &insert.source.body else {
            return Err(Error::invalid_statement(
                "Cassandra upsert requires a VALUES source",
            ));
        };
        let [row] = values.rows.as_slice() else {
            return Err(Error::invalid_statement(
                "Cassandra upsert requires exactly one row",
            ));
        };

        let mut base = ValueRecord::from_vec(vec![Value::Null; table.columns.len()]);
        for (position, column_id) in target.columns.iter().enumerate() {
            let column = schema.column(*column_id);
            base.fields[column.id.index] = row_value(row, position)?.clone();
        }

        let returning = returning_columns(table, insert.returning.as_ref());

        match upsert.action {
            UpsertAction::Ignore => {
                let applied = self.insert_if_not_exists(table, &base).await?;

                if !applied {
                    return Ok(ExecResponse::empty_value_stream());
                }

                Ok(match &returning {
                    Some(columns) => cql::stream(vec![Value::Record(project(&base, columns))]),
                    None => ExecResponse::count(1),
                })
            }
            UpsertAction::Update => {
                for _ in 0..3 {
                    if let Some(existing) = self
                        .read_row_by_key(table, &primary_key_value(table, &base)?)
                        .await?
                    {
                        let updated = apply_assignments_with_incoming(
                            &existing,
                            &upsert.shared,
                            table,
                            &base,
                        )?;
                        let updated = apply_assignments_with_incoming(
                            &updated,
                            &upsert.update,
                            table,
                            &base,
                        )?;

                        let columns = cql::all_columns(table);
                        if self
                            .write_row(
                                table,
                                &primary_key_value(table, &updated)?,
                                &updated,
                                &columns,
                                None,
                                true,
                            )
                            .await?
                        {
                            return Ok(match &returning {
                                Some(columns) => {
                                    cql::stream(vec![Value::Record(project(&updated, columns))])
                                }
                                None => ExecResponse::count(1),
                            });
                        }

                        continue;
                    }

                    let created = apply_defaults(&base, &upsert.defaults, table)?;
                    // The engine normalizes the create branch (including
                    // shared assignments and defaults) into the insert source,
                    // so only explicit `on_create` overrides remain here.
                    let created =
                        apply_assignments_with_incoming(&created, &upsert.create, table, &base)?;

                    if self.insert_if_not_exists(table, &created).await? {
                        return Ok(match &returning {
                            Some(columns) => {
                                cql::stream(vec![Value::Record(project(&created, columns))])
                            }
                            None => ExecResponse::count(1),
                        });
                    }
                }

                Err(Error::condition_failed(
                    "Cassandra upsert could not resolve a concurrent write after 3 attempts",
                ))
            }
        }
    }

    async fn insert_if_not_exists(
        &mut self,
        table: &db::Table,
        record: &ValueRecord,
    ) -> Result<bool> {
        // Include every non-null column so values added by `#[default]` /
        // `on_create` assignments are written too. Primary-key columns are
        // always present.
        let columns = table
            .columns
            .iter()
            .filter(|column| {
                !record.fields[column.id.index].is_null()
                    || table
                        .primary_key
                        .columns
                        .iter()
                        .any(|pk| pk.index == column.id.index)
            })
            .collect::<Vec<_>>();
        let names = cql::column_list(&columns);
        let placeholders = vec!["?"; columns.len()].join(", ");

        let mut binds = Vec::with_capacity(columns.len());
        for column in &columns {
            binds.push(value::encode_for_column(
                column,
                &record.fields[column.id.index],
            )?);
        }

        let cql = format!(
            "INSERT INTO {} ({names}) VALUES ({placeholders}) IF NOT EXISTS",
            schema::qualified(&self.config, &table.name)
        );

        self.execute_lwt(&cql, &binds).await
    }
}

fn primary_key_value(table: &db::Table, record: &ValueRecord) -> Result<Value> {
    let fields = table
        .primary_key_columns()
        .map(|column| record.fields[column.id.index].clone())
        .collect::<Vec<_>>();

    for field in &fields {
        if field.is_null() {
            return Err(Error::invalid_statement(
                "Cassandra upsert is missing a primary-key value",
            ));
        }
    }

    Ok(Value::Record(ValueRecord::from_vec(fields)))
}

fn row_value(row: &stmt::Expr, position: usize) -> Result<&Value> {
    match row.entry(position) {
        Some(stmt::Entry::Value(value)) | Some(stmt::Entry::Expr(stmt::Expr::Value(value))) => {
            Ok(value)
        }
        _ => Err(Error::invalid_statement(format!(
            "Cassandra upsert row entry did not lower to a literal at position {position}: {row:#?}"
        ))),
    }
}
