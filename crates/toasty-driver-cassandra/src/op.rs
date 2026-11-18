mod delete_by_key;
mod find_pk_by_index;
mod get_by_key;
mod insert;
mod query_pk;
mod scan;
mod update_by_key;
mod upsert;

pub(crate) use query_pk::apply_pagination;

use scylla::value::CqlValue;
use toasty_core::{
    Error, Result,
    schema::db::{self, Column, Table},
    stmt::{self, Assignment, Expr, Value, ValueRecord},
};

use crate::{Connection, cql, expr, schema, value};

/// Encodes a per-write TTL (seconds) as the CQL `int` bind its `USING TTL ?`
/// clause expects.
pub(crate) fn ttl_bind(ttl: Option<i64>) -> Result<Option<CqlValue>> {
    match ttl {
        Some(ttl) => {
            let ttl = i32::try_from(ttl).map_err(|_| {
                Error::invalid_statement(format!(
                    "CQL time-to-live {ttl} seconds does not fit in a 32-bit integer"
                ))
            })?;
            Ok(Some(CqlValue::Int(ttl)))
        }
        None => Ok(None),
    }
}

impl Connection {
    /// Reads one row by its full primary key, selecting every column.
    pub(crate) async fn read_row_by_key(
        &mut self,
        table: &Table,
        key: &Value,
    ) -> Result<Option<ValueRecord>> {
        let columns = cql::all_columns(table);
        let cql = format!(
            "SELECT {} FROM {} WHERE {}",
            cql::column_list(&columns),
            schema::qualified(&self.config, &table.name),
            cql::pk_where(table)
        );
        let binds = value::key_binds(table, key)?;
        let rows = self.query_all(&cql, &binds).await?;

        rows.first()
            .map(|row| value::decode_row(row, &columns))
            .transpose()
    }

    /// Builds and executes a full-row `UPDATE` for the given key.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn write_row(
        &mut self,
        table: &Table,
        key: &Value,
        record: &ValueRecord,
        columns: &[&Column],
        condition: Option<&stmt::Expr>,
        if_exists: bool,
        ttl: Option<i64>,
    ) -> Result<bool> {
        let (sets, mut binds) = write_row_assignments(table, record, columns, ttl)?;

        if sets.is_empty() {
            return Ok(true);
        }

        let mut cql = update_cql(
            &schema::qualified(&self.config, &table.name),
            &sets.join(", "),
            &cql::pk_where(table),
            ttl.is_some(),
        );
        binds.extend(value::key_binds(table, key)?);

        if let Some(condition) = condition {
            let rendered = expr::render(table, condition)?.ok_or_else(|| {
                Error::unsupported_feature(
                    "Cassandra lightweight transactions cannot express this condition",
                )
            })?;
            if rendered.cql.is_empty() {
                return Err(Error::invalid_statement(
                    "Cassandra lightweight transaction condition is empty",
                ));
            }
            cql.push_str(" IF ");
            cql.push_str(&rendered.cql);
            binds.extend(rendered.binds);
            return self.execute_lwt(&cql, &binds).await;
        }

        if if_exists {
            cql.push_str(" IF EXISTS");
            return self.execute_lwt(&cql, &binds).await;
        }

        self.query_all(&cql, &binds).await?;
        Ok(true)
    }
}

/// Builds the `SET` list and its binds for a full-row update.
///
/// The TTL bind, when present, comes first because `USING TTL ?` precedes
/// `SET`; getting this order wrong binds the TTL to the wrong column.
pub(crate) fn write_row_assignments(
    table: &Table,
    record: &ValueRecord,
    columns: &[&Column],
    ttl: Option<i64>,
) -> Result<(Vec<String>, Vec<Option<CqlValue>>)> {
    let mut sets = Vec::with_capacity(columns.len());
    let mut binds = Vec::with_capacity(columns.len() + 1);

    if let Some(bind) = ttl_bind(ttl)? {
        binds.push(Some(bind));
    }

    for column in columns {
        if table
            .primary_key
            .columns
            .iter()
            .any(|pk| pk.index == column.id.index)
        {
            continue;
        }
        sets.push(format!("{} = ?", cql::quote(&column.name)));
        binds.push(value::encode_for_column(
            column,
            &record.fields[column.id.index],
        )?);
    }

    Ok((sets, binds))
}

/// Renders an `UPDATE` statement. `has_ttl` adds the `USING TTL ?` clause
/// between the table name and `SET`.
pub(crate) fn update_cql(qualified: &str, sets: &str, where_clause: &str, has_ttl: bool) -> String {
    format!(
        "UPDATE {qualified}{} SET {sets} WHERE {where_clause}",
        if has_ttl { " USING TTL ?" } else { "" }
    )
}

/// Applies an assignment list to a row, substituting `Expr::Incoming`
/// references with the proposed upsert row.
pub(crate) fn apply_assignments_with_incoming(
    record: &ValueRecord,
    assignments: &stmt::Assignments,
    table: &Table,
    proposed: &ValueRecord,
) -> Result<ValueRecord> {
    let mut record = record.clone();

    for (projection, assignment) in assignments.iter() {
        let mut assignment = assignment.clone();
        rewrite_incoming(&mut assignment, proposed);
        apply_one(&mut record, table, projection, &assignment)?;
    }

    Ok(record)
}

/// Applies `#[default]` assignments only to columns that are still null, so an
/// explicit create-branch value in the insert source wins over the default.
pub(crate) fn apply_defaults(
    record: &ValueRecord,
    assignments: &stmt::Assignments,
    table: &Table,
) -> Result<ValueRecord> {
    let mut record = record.clone();

    for (projection, assignment) in assignments.iter() {
        let column = table.resolve(projection);
        if record.fields[column.id.index].is_null() {
            apply_one(&mut record, table, projection, assignment)?;
        }
    }

    Ok(record)
}

/// Applies an assignment list to a row.
pub(crate) fn apply_assignments(
    record: &ValueRecord,
    assignments: &stmt::Assignments,
    table: &Table,
) -> Result<ValueRecord> {
    let mut record = record.clone();

    for (projection, assignment) in assignments.iter() {
        apply_one(&mut record, table, projection, assignment)?;
    }

    Ok(record)
}

/// Applies one assignment to a row in place.
pub(crate) fn apply_one(
    record: &mut ValueRecord,
    table: &Table,
    projection: &stmt::Projection,
    assignment: &Assignment,
) -> Result<()> {
    let column = table.resolve(projection);
    let next = apply_assignment(record, column, assignment)?;
    record.fields[column.id.index] = next;
    Ok(())
}

/// Rewrites `Expr::Incoming` references in an assignment to the proposed row.
pub(crate) fn rewrite_incoming(assignment: &mut Assignment, proposed: &ValueRecord) {
    match assignment {
        Assignment::Set(expr)
        | Assignment::Insert(expr)
        | Assignment::Remove(expr)
        | Assignment::Append(expr)
        | Assignment::RemoveAt(expr)
        | Assignment::Add(expr)
        | Assignment::Subtract(expr) => rewrite_incoming_expr(expr, proposed),
        Assignment::Pop => {}
        Assignment::Batch(assignments) => {
            for assignment in assignments {
                rewrite_incoming(assignment, proposed);
            }
        }
    }
}

fn rewrite_incoming_expr(expr: &mut Expr, proposed: &ValueRecord) {
    struct Substitute<'a>(&'a ValueRecord);

    impl stmt::VisitMut for Substitute<'_> {
        fn visit_expr_mut(&mut self, expr: &mut Expr) {
            if matches!(expr, Expr::Incoming(_)) {
                *expr = Expr::Value(Value::Record(self.0.clone()));
            } else {
                stmt::visit_mut::visit_expr_mut(self, expr);
            }
        }
    }

    stmt::visit_mut::visit_expr_mut(&mut Substitute(proposed), expr);
}

fn apply_assignment(
    record: &ValueRecord,
    column: &Column,
    assignment: &Assignment,
) -> Result<Value> {
    let current = &record.fields[column.id.index];
    apply_assignment_to(record, column, current, assignment)
}

fn apply_assignment_to(
    record: &ValueRecord,
    column: &Column,
    current: &Value,
    assignment: &Assignment,
) -> Result<Value> {
    Ok(match assignment {
        Assignment::Set(expr) => expr::eval(expr, record)?,
        Assignment::Add(expr) => {
            let delta = expr::eval(expr, record)?;
            arithmetic(column, current, &delta, true)?
        }
        Assignment::Subtract(expr) => {
            let delta = expr::eval(expr, record)?;
            arithmetic(column, current, &delta, false)?
        }
        Assignment::Append(expr) | Assignment::Insert(expr) => {
            let addition = expr::eval(expr, record)?;
            append(column, current, &addition)?
        }
        Assignment::Remove(expr) => {
            let removal = expr::eval(expr, record)?;
            remove(column, current, &removal)?
        }
        Assignment::Pop | Assignment::RemoveAt(_) => {
            return Err(Error::unsupported_feature(
                "Cassandra lists do not support positional pop or remove-at",
            ));
        }
        Assignment::Batch(assignments) => {
            let mut current = current.clone();
            for assignment in assignments {
                current = apply_assignment_to(record, column, &current, assignment)?;
            }
            current
        }
    })
}

fn arithmetic(column: &Column, current: &Value, delta: &Value, add: bool) -> Result<Value> {
    let delta = if value::same_shape(&column.ty, delta) {
        delta.clone()
    } else {
        column.ty.cast(&(), delta.clone())?
    };

    fn signed(current: i64, delta: i64, add: bool) -> std::result::Result<i64, ()> {
        if add {
            current.checked_add(delta).ok_or(())
        } else {
            current.checked_sub(delta).ok_or(())
        }
    }

    let result = match (current, &delta) {
        (Value::I8(a), Value::I8(b)) => Value::I8(
            signed(i64::from(*a), i64::from(*b), add)
                .map_err(|_| Error::invalid_statement("integer overflow"))? as i8,
        ),
        (Value::I16(a), Value::I16(b)) => Value::I16(
            signed(i64::from(*a), i64::from(*b), add)
                .map_err(|_| Error::invalid_statement("integer overflow"))? as i16,
        ),
        (Value::I32(a), Value::I32(b)) => Value::I32(
            signed(i64::from(*a), i64::from(*b), add)
                .map_err(|_| Error::invalid_statement("integer overflow"))? as i32,
        ),
        (Value::I64(a), Value::I64(b)) => Value::I64(
            signed(*a, *b, add).map_err(|_| Error::invalid_statement("integer overflow"))?,
        ),
        (Value::U8(a), Value::U8(b)) => Value::U8(
            signed(i64::from(*a), i64::from(*b), add)
                .map_err(|_| Error::invalid_statement("integer overflow"))? as u8,
        ),
        (Value::U16(a), Value::U16(b)) => Value::U16(
            signed(i64::from(*a), i64::from(*b), add)
                .map_err(|_| Error::invalid_statement("integer overflow"))? as u16,
        ),
        (Value::U32(a), Value::U32(b)) => Value::U32(
            signed(i64::from(*a), i64::from(*b), add)
                .map_err(|_| Error::invalid_statement("integer overflow"))? as u32,
        ),
        (Value::U64(a), Value::U64(b)) => {
            let a = i64::try_from(*a).map_err(|_| Error::invalid_statement("integer overflow"))?;
            let b = i64::try_from(*b).map_err(|_| Error::invalid_statement("integer overflow"))?;
            Value::U64(
                signed(a, b, add).map_err(|_| Error::invalid_statement("integer overflow"))? as u64,
            )
        }
        (Value::F32(a), Value::F32(b)) => Value::F32(if add { a + b } else { a - b }),
        (Value::F64(a), Value::F64(b)) => Value::F64(if add { a + b } else { a - b }),
        _ => {
            return Err(Error::unsupported_feature(format!(
                "Cassandra cannot apply arithmetic to column `{}`",
                column.name
            )));
        }
    };

    Ok(result)
}

fn append(column: &Column, current: &Value, addition: &Value) -> Result<Value> {
    let mut items = match current {
        Value::Null => Vec::new(),
        Value::List(items) => items.clone(),
        other => {
            return Err(Error::unsupported_feature(format!(
                "Cassandra cannot append to column `{}` containing {other:?}",
                column.name
            )));
        }
    };

    match addition {
        Value::List(added) => items.extend(added.iter().cloned()),
        other => items.push(other.clone()),
    }

    Ok(Value::List(items))
}

fn remove(column: &Column, current: &Value, removal: &Value) -> Result<Value> {
    let Value::List(current) = current else {
        return Err(Error::unsupported_feature(format!(
            "Cassandra cannot remove elements from column `{}`",
            column.name
        )));
    };

    let removals: Vec<&Value> = match removal {
        Value::List(items) => items.iter().collect(),
        other => vec![other],
    };

    Ok(Value::List(
        current
            .iter()
            .filter(|item| !removals.contains(item))
            .cloned()
            .collect(),
    ))
}

/// Extracts the columns named by an upsert/update `RETURNING` projection.
pub(crate) fn returning_columns<'a>(
    table: &'a Table,
    returning: Option<&stmt::Returning>,
) -> Option<Vec<&'a Column>> {
    let returning = returning?;
    let expr = returning
        .as_project()
        .expect("lowered returning projection");
    let record = expr.as_record().expect("returning must be a record");

    Some(
        record
            .fields
            .iter()
            .map(|expr| {
                let Expr::Reference(reference) = expr else {
                    panic!("returning item is not a column reference: {expr:#?}")
                };
                let column = reference.as_expr_column_unwrap();
                &table.columns[column.column]
            })
            .collect(),
    )
}

/// Projects a row to the requested columns.
pub(crate) fn project(record: &ValueRecord, columns: &[&Column]) -> ValueRecord {
    value::project(record, columns)
}

/// Sorts records by the given columns in the requested direction.
pub(crate) fn sort_records(records: &mut [ValueRecord], columns: &[&Column], descending: bool) {
    records.sort_by(|a, b| {
        for column in columns {
            let left = &a.fields[column.id.index];
            let right = &b.fields[column.id.index];
            let ordering = left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal);
            let ordering = if descending {
                ordering.reverse()
            } else {
                ordering
            };
            if ordering != std::cmp::Ordering::Equal {
                return ordering;
            }
        }
        std::cmp::Ordering::Equal
    });
}

/// Returns the columns that define the requested ordering.
pub(crate) fn order_columns<'a>(
    schema: &'a db::Schema,
    table: &'a Table,
    index: Option<db::IndexId>,
) -> Vec<&'a Column> {
    if let Some(index) = index {
        return schema
            .index(index)
            .columns
            .iter()
            .map(|index_column| schema.column(index_column.column))
            .collect();
    }

    let pk_index = &table.indices[table.primary_key.index.index];
    let clustering = pk_index
        .columns
        .iter()
        .filter(|index_column| index_column.scope.is_local())
        .map(|index_column| table.column(index_column.column))
        .collect::<Vec<_>>();

    if clustering.is_empty() {
        table.primary_key_columns().collect()
    } else {
        clustering
    }
}

pub(crate) fn cursor_state(after: &Option<Value>) -> scylla::response::PagingState {
    match after {
        Some(Value::Bytes(bytes)) => {
            scylla::response::PagingState::new_from_raw_bytes(bytes.clone())
        }
        _ => scylla::response::PagingState::start(),
    }
}

pub(crate) fn next_cursor(state: scylla::response::PagingStateResponse) -> Option<Box<Value>> {
    match state.into_paging_control_flow() {
        std::ops::ControlFlow::Break(()) => None,
        std::ops::ControlFlow::Continue(state) => state
            .as_bytes_slice()
            .map(|bytes| Box::new(Value::Bytes(bytes.to_vec()))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use toasty_core::{
        schema::db::{ColumnId, IndexId, PrimaryKey, TableId},
        stmt::Type,
    };

    fn column(index: usize, name: &str, primary_key: bool) -> Column {
        Column {
            id: ColumnId {
                table: TableId(0),
                index,
            },
            name: name.to_owned(),
            ty: Type::I64,
            storage_ty: db::Type::Integer(8),
            nullable: false,
            primary_key,
            auto_increment: false,
            versionable: false,
        }
    }

    fn table() -> Table {
        Table {
            id: TableId(0),
            name: "things".to_owned(),
            columns: vec![column(0, "id", true), column(1, "value", false)],
            primary_key: PrimaryKey {
                columns: vec![ColumnId {
                    table: TableId(0),
                    index: 0,
                }],
                index: IndexId {
                    table: TableId(0),
                    index: 0,
                },
            },
            indices: vec![],
        }
    }

    #[test]
    fn ttl_bind_encodes_seconds_as_cql_int() {
        assert_eq!(ttl_bind(None).unwrap(), None);
        assert_eq!(ttl_bind(Some(3_600)).unwrap(), Some(CqlValue::Int(3_600)));
    }

    #[test]
    fn ttl_bind_rejects_out_of_range() {
        assert!(ttl_bind(Some(i64::from(i32::MAX) + 1)).is_err());
    }

    #[test]
    fn update_cql_places_ttl_between_table_and_set() {
        assert_eq!(
            update_cql("ks.things", "\"value\" = ?", "\"id\" = ?", true),
            "UPDATE ks.things USING TTL ? SET \"value\" = ? WHERE \"id\" = ?"
        );
        assert_eq!(
            update_cql("ks.things", "\"value\" = ?", "\"id\" = ?", false),
            "UPDATE ks.things SET \"value\" = ? WHERE \"id\" = ?"
        );
    }

    #[test]
    fn write_row_assignments_binds_ttl_first() {
        let table = table();
        let columns: Vec<&Column> = table.columns.iter().collect();
        let record = ValueRecord::from_vec(vec![Value::I64(1), Value::I64(7)]);

        let (sets, binds) = write_row_assignments(&table, &record, &columns, Some(60)).unwrap();
        assert_eq!(sets, vec!["\"value\" = ?".to_owned()]);
        assert_eq!(
            binds,
            vec![Some(CqlValue::Int(60)), Some(CqlValue::BigInt(7))]
        );

        let (_, binds) = write_row_assignments(&table, &record, &columns, None).unwrap();
        assert_eq!(binds, vec![Some(CqlValue::BigInt(7))]);
    }
}
