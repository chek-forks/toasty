use std::sync::Arc;
use toasty_core::{
    Result, Schema,
    driver::{
        ExecResponse,
        operation::{self, Pagination},
    },
    stmt::{self, Expr, Value},
};

use crate::{
    Connection, cql, expr,
    op::{cursor_state, next_cursor, order_columns, sort_records},
    schema, value,
};

impl Connection {
    pub(crate) async fn exec_query_pk(
        &mut self,
        schema: &Arc<Schema>,
        op: operation::QueryPk,
    ) -> Result<ExecResponse> {
        let table = schema.db.table(op.table);
        let select = cql::select_columns(&schema.db, table, &op.select);
        let all = cql::all_columns(table);

        let pk_rendered = expr::render(table, &op.pk_filter)?;
        let filter_rendered = match &op.filter {
            Some(filter) => expr::render(table, filter)?,
            None => Some(expr::Rendered::default()),
        };

        let pushdown = pk_rendered.is_some() && filter_rendered.is_some();

        // CQL can order by clustering columns only when every partition-key
        // column is restricted to a single value.
        let can_push_order = op.order.is_some()
            && op.index.is_none()
            && expr::full_partition_restriction(table, &op.pk_filter);
        let needs_client_sort = op.order.is_some() && !can_push_order;

        let from = schema::qualified(&self.config, &table.name);

        let mut where_parts = Vec::new();
        let mut binds = Vec::new();
        let mut client_filter = None;
        let scan_columns;

        if pushdown {
            // A client-side sort needs the ordering columns in the result even
            // when the caller only selected a subset.
            scan_columns = if needs_client_sort {
                all.clone()
            } else {
                select.clone()
            };

            if let Some(rendered) = &pk_rendered
                && !rendered.cql.is_empty()
            {
                where_parts.push(rendered.cql.clone());
                binds.extend(rendered.binds.clone());
            }

            if let Some(rendered) = &filter_rendered
                && !rendered.cql.is_empty()
            {
                where_parts.push(rendered.cql.clone());
                binds.extend(rendered.binds.clone());
            }
        } else {
            scan_columns = all.clone();
            client_filter = Some(match &op.filter {
                Some(filter) => Expr::and(op.pk_filter.clone(), filter.clone()),
                None => op.pk_filter.clone(),
            });
        }

        let mut select_cql = format!("SELECT {} FROM {from}", cql::column_list(&scan_columns));

        if !where_parts.is_empty() {
            select_cql.push_str(" WHERE ");
            select_cql.push_str(&where_parts.join(" AND "));
        }

        let allow_filtering = !where_parts.is_empty();

        if can_push_order {
            let direction = match op.order {
                Some(stmt::Direction::Desc) => "DESC",
                _ => "ASC",
            };
            let order = order_columns(&schema.db, table, None)
                .iter()
                .map(|column| format!("{} {direction}", cql::quote(&column.name)))
                .collect::<Vec<_>>();

            if !order.is_empty() {
                select_cql.push_str(" ORDER BY ");
                select_cql.push_str(&order.join(", "));
            }
        }

        let (mut records, next) = self
            .load_records(
                &select_cql,
                binds,
                &scan_columns,
                &op.limit,
                allow_filtering,
            )
            .await?;

        if let Some(filter) = &client_filter {
            records.retain(|record| expr::matches(filter, record));
        }

        if needs_client_sort {
            let direction = op.order.unwrap_or(stmt::Direction::Asc);
            let columns = order_columns(&schema.db, table, op.index);
            sort_records(
                &mut records,
                &columns,
                matches!(direction, stmt::Direction::Desc),
            );
        }

        if client_filter.is_some() || needs_client_sort {
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

    /// Loads records, optionally following server paging state for a cursor.
    pub(crate) async fn load_records(
        &mut self,
        select_cql: &str,
        binds: Vec<Option<scylla::value::CqlValue>>,
        columns: &[&toasty_core::schema::db::Column],
        pagination: &Option<Pagination>,
        allow_filtering: bool,
    ) -> Result<(Vec<stmt::ValueRecord>, Option<Box<Value>>)> {
        match pagination {
            Some(Pagination::Cursor { page_size, after }) => {
                let cql = finish_query(select_cql, allow_filtering);
                let (rows, state) = self
                    .query_page(
                        &cql,
                        &binds,
                        (*page_size).try_into().unwrap_or(i32::MAX),
                        cursor_state(after),
                    )
                    .await?;

                let records = rows
                    .iter()
                    .map(|row| value::decode_row(row, columns))
                    .collect::<Result<Vec<_>>>()?;

                Ok((records, next_cursor(state)))
            }
            Some(Pagination::Offset { limit, offset }) => {
                let fetch = offset.unwrap_or(0).saturating_add(*limit);
                let cql = finish_query(&format!("{select_cql} LIMIT {fetch}"), allow_filtering);

                let rows = self.query_all(&cql, &binds).await?;
                let records = rows
                    .iter()
                    .map(|row| value::decode_row(row, columns))
                    .collect::<Result<Vec<_>>>()?;

                Ok((
                    records
                        .into_iter()
                        .skip(offset.unwrap_or(0) as usize)
                        .collect(),
                    None,
                ))
            }
            None => {
                let cql = finish_query(select_cql, allow_filtering);
                let rows = self.query_all(&cql, &binds).await?;
                let records = rows
                    .iter()
                    .map(|row| value::decode_row(row, columns))
                    .collect::<Result<Vec<_>>>()?;
                Ok((records, None))
            }
        }
    }
}

/// Appends `ALLOW FILTERING`, which CQL requires to be the final clause.
fn finish_query(cql: &str, allow_filtering: bool) -> String {
    if allow_filtering {
        format!("{cql} ALLOW FILTERING")
    } else {
        cql.to_owned()
    }
}

pub(crate) fn apply_pagination(
    records: Vec<stmt::ValueRecord>,
    pagination: &Option<Pagination>,
    next: Option<Box<Value>>,
) -> (Vec<stmt::ValueRecord>, Option<Box<Value>>) {
    match pagination {
        Some(Pagination::Cursor { page_size, .. }) => (
            records.into_iter().take(*page_size as usize).collect(),
            next,
        ),
        Some(Pagination::Offset { limit, .. }) => {
            (records.into_iter().take(*limit as usize).collect(), None)
        }
        None => (records, None),
    }
}
