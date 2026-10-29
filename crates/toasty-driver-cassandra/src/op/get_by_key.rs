use std::sync::Arc;
use toasty_core::{
    Result, Schema,
    driver::{ExecResponse, operation},
};

use crate::{Connection, cql, schema, value};

impl Connection {
    pub(crate) async fn exec_get_by_key(
        &mut self,
        schema: &Arc<Schema>,
        op: operation::GetByKey,
    ) -> Result<ExecResponse> {
        let table = schema.db.table(op.table);

        if op.keys.is_empty() {
            return Ok(ExecResponse::empty_value_stream());
        }

        let columns = cql::select_columns(&schema.db, table, &op.select);
        let empty_projection = op.select.is_empty();
        let select = cql::column_list(&columns);
        let from = schema::qualified(&self.config, &table.name);
        let predicate = cql::pk_where(table);
        let cql = format!("SELECT {select} FROM {from} WHERE {predicate}");

        let mut rows = Vec::with_capacity(op.keys.len());

        for key in &op.keys {
            let binds = value::key_binds(table, key)?;
            let raw = self.query_all(&cql, &binds).await?;

            if empty_projection {
                for _ in raw {
                    rows.push(toasty_core::stmt::Value::Record(
                        toasty_core::stmt::ValueRecord::from_vec(Vec::new()),
                    ));
                }
            } else {
                rows.extend(cql::decode_rows(&raw, &columns)?);
            }
        }

        Ok(cql::stream(rows))
    }
}
