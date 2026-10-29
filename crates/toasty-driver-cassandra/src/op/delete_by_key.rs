use toasty_core::{
    Error, Result,
    driver::{ExecResponse, operation},
    schema::db,
};

use crate::{Connection, cql, expr, schema, value};

impl Connection {
    pub(crate) async fn exec_delete_by_key(
        &mut self,
        schema: &db::Schema,
        op: operation::DeleteByKey,
    ) -> Result<ExecResponse> {
        let table = schema.table(op.table);

        let mut count = 0_u64;

        for key in &op.keys {
            if op.filter.is_some() || op.condition.is_some() {
                let Some(record) = self.read_row_by_key(table, key).await? else {
                    if op.condition.is_some() {
                        return Err(Error::record_not_found("conditional delete matched no row"));
                    }
                    continue;
                };

                if let Some(filter) = &op.filter
                    && !expr::matches(filter, &record)
                {
                    continue;
                }
            }

            let mut cql = format!(
                "DELETE FROM {} WHERE {}",
                schema::qualified(&self.config, &table.name),
                cql::pk_where(table)
            );
            let mut binds = value::key_binds(table, key)?;

            if let Some(condition) = &op.condition {
                let rendered = expr::render(table, condition)?.ok_or_else(|| {
                    Error::unsupported_feature(
                        "Cassandra lightweight transactions cannot express this condition",
                    )
                })?;
                cql.push_str(" IF ");
                cql.push_str(&rendered.cql);
                binds.extend(rendered.binds);

                if !self.execute_lwt(&cql, &binds).await? {
                    return Err(Error::condition_failed(
                        "Cassandra lightweight transaction failed",
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
