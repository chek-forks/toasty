use toasty_core::{
    Error, Result,
    driver::{ExecResponse, operation},
    schema::db,
    stmt::{self, Assignment},
};

use crate::{
    Connection, cql,
    op::{apply_assignments, project},
    schema, value,
};

impl Connection {
    pub(crate) async fn exec_update_by_key(
        &mut self,
        schema: &db::Schema,
        op: operation::UpdateByKey,
    ) -> Result<ExecResponse> {
        let table = schema.table(op.table);
        let returning = op.returning.as_ref().map(|columns| {
            columns
                .iter()
                .map(|column| schema.column(*column))
                .collect::<Vec<_>>()
        });

        let simple = op.filter.is_none()
            && op.condition.is_none()
            && returning.is_none()
            && op
                .assignments
                .iter()
                .all(|(_, assignment)| matches!(assignment, Assignment::Set(_)));

        let mut count = 0_u64;
        let mut returned = Vec::new();

        for key in &op.keys {
            if simple {
                if self.fast_update(table, key, &op.assignments).await? {
                    count += 1;
                }
                continue;
            }

            let Some(record) = self.read_row_by_key(table, key).await? else {
                if op.condition.is_some() {
                    return Err(Error::record_not_found("conditional update matched no row"));
                }
                continue;
            };

            if let Some(filter) = &op.filter
                && !crate::expr::matches(filter, &record)
            {
                continue;
            }

            let updated = apply_assignments(&record, &op.assignments, table)?;
            let columns = cql::all_columns(table);

            let applied = self
                .write_row(table, key, &updated, &columns, op.condition.as_ref(), false)
                .await?;

            if !applied {
                return Err(Error::condition_failed(
                    "Cassandra lightweight transaction failed",
                ));
            }

            count += 1;

            if let Some(returning) = &returning {
                returned.push(stmt::Value::Record(project(&updated, returning)));
            }
        }

        if returning.is_some() {
            Ok(ExecResponse {
                values: toasty_core::driver::Rows::Stream(stmt::ValueStream::from_vec(returned)),
                next_cursor: None,
                prev_cursor: None,
            })
        } else {
            Ok(ExecResponse::count(count))
        }
    }

    async fn fast_update(
        &mut self,
        table: &db::Table,
        key: &stmt::Value,
        assignments: &stmt::Assignments,
    ) -> Result<bool> {
        let mut sets = Vec::new();
        let mut binds = Vec::new();

        for (projection, assignment) in assignments.iter() {
            let Assignment::Set(expr) = assignment else {
                return Ok(false);
            };
            let column = table.resolve(projection);
            if table
                .primary_key
                .columns
                .iter()
                .any(|pk| pk.index == column.id.index)
            {
                return Ok(false);
            }
            let value = expr.eval_const()?;
            sets.push(format!("{} = ?", cql::quote(&column.name)));
            binds.push(value::encode_for_column(column, &value)?);
        }

        if sets.is_empty() {
            return Ok(false);
        }

        let cql = format!(
            "UPDATE {} SET {} WHERE {}",
            schema::qualified(&self.config, &table.name),
            sets.join(", "),
            cql::pk_where(table)
        );
        binds.extend(value::key_binds(table, key)?);

        self.query_all(&cql, &binds).await?;
        Ok(true)
    }
}
