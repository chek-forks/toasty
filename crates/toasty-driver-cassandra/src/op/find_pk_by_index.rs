use std::sync::Arc;
use toasty_core::{
    Result, Schema,
    driver::{ExecResponse, operation},
    stmt,
};

use crate::{Connection, cql, expr, schema, value};

impl Connection {
    pub(crate) async fn exec_find_pk_by_index(
        &mut self,
        schema: &Arc<Schema>,
        op: operation::FindPkByIndex,
    ) -> Result<ExecResponse> {
        let table = schema.db.table(op.table);
        let pk = table.primary_key_columns().collect::<Vec<_>>();
        let select = cql::column_list(&pk);
        let from = schema::qualified(&self.config, &table.name);

        let records = match expr::render(table, &op.filter)? {
            Some(rendered) => {
                let mut cql = format!("SELECT {select} FROM {from}");
                if !rendered.cql.is_empty() {
                    cql.push_str(" WHERE ");
                    cql.push_str(&rendered.cql);
                    cql.push_str(" ALLOW FILTERING");
                }

                let rows = self.query_all(&cql, &rendered.binds).await?;
                rows.iter()
                    .map(|row| value::decode_row(row, &pk))
                    .collect::<Result<Vec<_>>>()?
            }
            None => {
                // The filter cannot be expressed in CQL. Fetch the full table
                // and evaluate it in Rust; this keeps mutation key sets exact.
                let all = cql::all_columns(table);
                let cql = format!("SELECT {} FROM {from}", cql::column_list(&all));
                let rows = self.query_all(&cql, &[]).await?;
                let rows = rows
                    .iter()
                    .map(|row| value::decode_row(row, &all))
                    .collect::<Result<Vec<_>>>()?;

                rows.into_iter()
                    .filter(|record| expr::matches(&op.filter, record))
                    .map(|record| value::project(&record, &pk))
                    .collect()
            }
        };

        Ok(cql::stream(
            records.into_iter().map(stmt::Value::Record).collect(),
        ))
    }
}
