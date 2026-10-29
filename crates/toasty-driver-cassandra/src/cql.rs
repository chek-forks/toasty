use scylla::{
    response::{PagingState, PagingStateResponse},
    statement::prepared::PreparedStatement,
    value::{CqlValue, Row},
};
use toasty_core::{
    Error, Result,
    schema::db::{self, Column, ColumnId},
};

use crate::{Connection, value};

impl Connection {
    pub(crate) async fn prepare(&mut self, cql: &str) -> Result<PreparedStatement> {
        if let Some(prepared) = self.prepared.get(cql) {
            return Ok(prepared.clone());
        }

        let prepared = self
            .session
            .prepare(cql)
            .await
            .map_err(crate::map_prepare_error)?;

        self.prepared.insert(cql.to_owned(), prepared.clone());
        Ok(prepared)
    }

    pub(crate) async fn query_all(
        &mut self,
        cql: &str,
        binds: &[Option<CqlValue>],
    ) -> Result<Vec<Row>> {
        tracing::trace!(cql, "cassandra query");
        debug_cql(cql, binds);
        let prepared = self.prepare(cql).await?;

        let result = self
            .session
            .execute_unpaged(&prepared, binds)
            .await
            .map_err(crate::map_execution_error)?;

        collect_rows(result)
    }

    pub(crate) async fn query_page(
        &mut self,
        cql: &str,
        binds: &[Option<CqlValue>],
        page_size: i32,
        state: PagingState,
    ) -> Result<(Vec<Row>, PagingStateResponse)> {
        tracing::trace!(cql, "cassandra paged query");
        debug_cql(cql, binds);
        let mut prepared = self.prepare(cql).await?;
        prepared.set_page_size(page_size);

        let (result, state) = self
            .session
            .execute_single_page(&prepared, binds, state)
            .await
            .map_err(crate::map_execution_error)?;

        Ok((collect_rows(result)?, state))
    }

    pub(crate) async fn execute_lwt(
        &mut self,
        cql: &str,
        binds: &[Option<CqlValue>],
    ) -> Result<bool> {
        let rows = self.query_all(cql, binds).await?;

        let row = rows.first().ok_or_else(|| {
            Error::invalid_result("Cassandra lightweight transaction returned no rows")
        })?;

        match row.columns.first() {
            Some(Some(CqlValue::Boolean(applied))) => Ok(*applied),
            _ => Err(Error::invalid_result(
                "Cassandra lightweight transaction result is missing the [applied] column",
            )),
        }
    }

    pub(crate) async fn execute_ddl(&mut self, cql: &str) -> Result<()> {
        tracing::trace!(cql, "cassandra ddl");
        debug_cql(cql, &[]);
        self.session
            .query_unpaged(cql, &[])
            .await
            .map_err(crate::map_execution_error)?;
        // DDL invalidates prepared statements for the changed schema.
        self.prepared.clear();
        Ok(())
    }
}

/// Prints the CQL and binds when `TOASTY_CASSANDRA_DEBUG_CQL` is set.
fn debug_cql(cql: &str, binds: &[Option<CqlValue>]) {
    if std::env::var_os("TOASTY_CASSANDRA_DEBUG_CQL").is_some() {
        eprintln!("CQL: {cql}");
        eprintln!("BINDS: {binds:?}");
    }
}

fn collect_rows(result: scylla::response::query_result::QueryResult) -> Result<Vec<Row>> {
    if !result.is_rows() {
        // Writes (INSERT/UPDATE/DELETE) and DDL return no rows.
        return Ok(Vec::new());
    }

    let rows = result
        .into_rows_result()
        .map_err(|error| Error::invalid_result(error.to_string()))?;

    rows.rows::<Row>()
        .map_err(|error| Error::invalid_result(error.to_string()))?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| Error::invalid_result(error.to_string()))
}

/// The columns a query should select, falling back to the primary key when the
/// engine did not request a projection.
pub(crate) fn select_columns<'a>(
    schema: &'a db::Schema,
    table: &'a db::Table,
    select: &[ColumnId],
) -> Vec<&'a Column> {
    if select.is_empty() {
        table.primary_key_columns().collect()
    } else {
        select.iter().map(|id| schema.column(*id)).collect()
    }
}

/// Every column in table order.
pub(crate) fn all_columns(table: &db::Table) -> Vec<&Column> {
    table.columns.iter().collect()
}

/// Renders the `SELECT` projection.
pub(crate) fn column_list(columns: &[&Column]) -> String {
    columns
        .iter()
        .map(|column| quote(&column.name))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Renders the primary-key `WHERE` clause with one bind placeholder per key
/// column.
pub(crate) fn pk_where(table: &db::Table) -> String {
    table
        .primary_key_columns()
        .map(|column| format!("{} = ?", quote(&column.name)))
        .collect::<Vec<_>>()
        .join(" AND ")
}

/// Decodes rows into records in the given column order.
pub(crate) fn decode_rows(
    rows: &[Row],
    columns: &[&Column],
) -> Result<Vec<toasty_core::stmt::Value>> {
    rows.iter()
        .map(|row| value::decode_row(row, columns))
        .map(|row| row.map(toasty_core::stmt::Value::Record))
        .collect()
}

pub(crate) fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Builds a value stream response.
pub(crate) fn stream(rows: Vec<toasty_core::stmt::Value>) -> toasty_core::driver::ExecResponse {
    toasty_core::driver::ExecResponse {
        values: toasty_core::driver::Rows::Stream(toasty_core::stmt::ValueStream::from_vec(rows)),
        next_cursor: None,
        prev_cursor: None,
    }
}
