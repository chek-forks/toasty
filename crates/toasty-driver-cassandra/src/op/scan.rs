use std::sync::Arc;
use toasty_core::{
    Result, Schema,
    driver::{ExecResponse, operation},
    stmt,
    stmt::Value,
};

use crate::{Connection, cql, expr, op::apply_pagination, schema, value};

impl Connection {
    pub(crate) async fn exec_scan(
        &mut self,
        schema: &Arc<Schema>,
        op: operation::Scan,
    ) -> Result<ExecResponse> {
        let table = schema.db.table(op.table);
        let all = cql::all_columns(table);

        let select = if op.columns.is_empty() {
            all.clone()
        } else {
            op.columns
                .iter()
                .map(|index| &table.columns[*index])
                .collect::<Vec<_>>()
        };

        let rendered = match &op.filter {
            Some(filter) => expr::render(table, filter)?,
            None => Some(expr::Rendered::default()),
        };

        let pushdown = rendered.is_some();
        let scan_columns = if pushdown {
            select.clone()
        } else {
            all.clone()
        };

        let from = schema::qualified(&self.config, &table.name);
        let mut cql = format!("SELECT {} FROM {from}", cql::column_list(&scan_columns));
        let mut binds = Vec::new();
        let mut allow_filtering = false;

        if let Some(rendered) = &rendered
            && !rendered.cql.is_empty()
        {
            cql.push_str(" WHERE ");
            cql.push_str(&rendered.cql);
            allow_filtering = true;
            binds = rendered.binds.clone();
        }

        let (mut records, next) = self
            .load_records(&cql, binds, &scan_columns, &op.limit, allow_filtering)
            .await?;

        if !pushdown {
            if let Some(filter) = &op.filter {
                records.retain(|record| expr::matches(filter, record));
            }

            records = records
                .into_iter()
                .map(|record| value::project(&record, &select))
                .collect();
        }

        let (records, next) = apply_pagination(records, &op.limit, next);

        Ok(ExecResponse {
            values: toasty_core::driver::Rows::Stream(stmt::ValueStream::from_vec(
                records.into_iter().map(Value::Record).collect(),
            )),
            next_cursor: next,
            prev_cursor: None,
        })
    }
}
