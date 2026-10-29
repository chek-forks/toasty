use scylla::value::CqlValue;
use toasty_core::{
    Error, Result,
    schema::db::{self, Column},
    stmt::{self, BinaryOp, Expr, ExprReference, Input, Projection, Value, ValueRecord},
};

use crate::value;

/// A rendered CQL predicate and its bind values.
#[derive(Debug, Default)]
pub(crate) struct Rendered {
    pub(crate) cql: String,
    pub(crate) binds: Vec<Option<CqlValue>>,
}

/// Renders an expression as a CQL `WHERE` predicate.
///
/// Returns `Ok(None)` when the expression cannot be pushed down; the caller
/// should fetch rows and evaluate the predicate in Rust instead.
pub(crate) fn render(table: &db::Table, expr: &Expr) -> Result<Option<Rendered>> {
    render_inner(table, expr)
}

fn render_inner(table: &db::Table, expr: &Expr) -> Result<Option<Rendered>> {
    match expr {
        Expr::Value(Value::Bool(true)) => Ok(Some(Rendered::default())),
        Expr::Value(Value::Bool(false)) => Ok(None),

        Expr::And(and) => {
            let mut cql = String::new();
            let mut binds = Vec::new();

            for operand in &and.operands {
                let Some(rendered) = render_inner(table, operand)? else {
                    return Ok(None);
                };

                if rendered.cql.is_empty() {
                    binds.extend(rendered.binds);
                    continue;
                }

                if !cql.is_empty() {
                    cql.push_str(" AND ");
                }
                cql.push_str(&rendered.cql);
                binds.extend(rendered.binds);
            }

            Ok(Some(Rendered { cql, binds }))
        }

        Expr::BinaryOp(op) => {
            let (column, value_expr) = match (column_of(table, &op.lhs), column_of(table, &op.rhs))
            {
                (Some(column), None) => (column, &op.rhs),
                (None, Some(column)) => (column, &op.lhs),
                _ => return Ok(None),
            };

            let operator = match op.op {
                BinaryOp::Eq => "=",
                BinaryOp::Gt => ">",
                BinaryOp::Ge => ">=",
                BinaryOp::Lt => "<",
                BinaryOp::Le => "<=",
                // ScyllaDB rejects `!=` in `WHERE` even with ALLOW FILTERING,
                // so evaluate it in the client.
                BinaryOp::Ne | BinaryOp::Add | BinaryOp::Sub => return Ok(None),
            };

            let Some(value) = const_value(value_expr) else {
                return Ok(None);
            };

            if value.is_null() {
                // CQL does not accept a null bind in `WHERE`; evaluate the
                // predicate client-side instead.
                return Ok(None);
            }

            Ok(Some(Rendered {
                cql: format!("{} {operator} ?", quote(&column.name)),
                binds: vec![value::encode_for_column(column, &value)?],
            }))
        }

        Expr::Between(between) => {
            let Some(column) = column_of(table, &between.expr) else {
                return Ok(None);
            };
            let (Some(low), Some(high)) = (const_value(&between.low), const_value(&between.high))
            else {
                return Ok(None);
            };
            if low.is_null() || high.is_null() {
                return Ok(None);
            }

            Ok(Some(Rendered {
                cql: format!(
                    "{} >= ? AND {} <= ?",
                    quote(&column.name),
                    quote(&column.name)
                ),
                binds: vec![
                    value::encode_for_column(column, &low)?,
                    value::encode_for_column(column, &high)?,
                ],
            }))
        }

        Expr::InList(in_list) => {
            let Some(column) = column_of(table, &in_list.expr) else {
                return Ok(None);
            };
            let Some(Value::List(items)) = const_value(&in_list.list) else {
                return Ok(None);
            };
            if items.is_empty() || items.iter().any(|item| item.is_null()) {
                return Ok(None);
            }

            let placeholders = vec!["?"; items.len()].join(", ");
            let mut binds = Vec::with_capacity(items.len());
            for item in &items {
                binds.push(value::encode_for_column(column, item)?);
            }

            Ok(Some(Rendered {
                cql: format!("{} IN ({placeholders})", quote(&column.name)),
                binds,
            }))
        }

        Expr::StartsWith(starts_with) => {
            let Some(column) = column_of(table, &starts_with.expr) else {
                return Ok(None);
            };
            let Some(Value::String(prefix)) = const_value(&starts_with.prefix) else {
                return Ok(None);
            };
            if prefix.contains('%') || prefix.contains('_') {
                // CQL LIKE has no ESCAPE clause; fall back to a client-side
                // filter rather than matching incorrectly.
                return Ok(None);
            }

            Ok(Some(Rendered {
                cql: format!("{} LIKE ?", quote(&column.name)),
                binds: vec![Some(CqlValue::Text(format!("{prefix}%")))],
            }))
        }

        Expr::Like(like) => {
            if like.case_insensitive || like.escape.is_some() {
                return Ok(None);
            }
            let Some(column) = column_of(table, &like.expr) else {
                return Ok(None);
            };
            let Some(Value::String(pattern)) = const_value(&like.pattern) else {
                return Ok(None);
            };

            Ok(Some(Rendered {
                cql: format!("{} LIKE ?", quote(&column.name)),
                binds: vec![Some(CqlValue::Text(pattern))],
            }))
        }

        Expr::AnyOp(any) => {
            let Some(column) = column_of(table, &any.lhs) else {
                return Ok(None);
            };

            match &column.storage_ty {
                db::Type::List(_) | db::Type::Document { .. } => {
                    if any.op != BinaryOp::Eq {
                        return Ok(None);
                    }
                    let Some(value) = const_value(&any.rhs) else {
                        return Ok(None);
                    };
                    Ok(Some(Rendered {
                        cql: format!("{} CONTAINS ?", quote(&column.name)),
                        binds: vec![value::encode_for_column(column, &value)?],
                    }))
                }
                _ => {
                    if any.op != BinaryOp::Eq {
                        return Ok(None);
                    }
                    let Some(Value::List(items)) = const_value(&any.rhs) else {
                        return Ok(None);
                    };
                    if items.is_empty() {
                        return Ok(None);
                    }

                    let placeholders = vec!["?"; items.len()].join(", ");
                    let mut binds = Vec::with_capacity(items.len());
                    for item in &items {
                        binds.push(value::encode_for_column(column, item)?);
                    }

                    Ok(Some(Rendered {
                        cql: format!("{} IN ({placeholders})", quote(&column.name)),
                        binds,
                    }))
                }
            }
        }

        Expr::Cast(cast) => {
            if let Some(column) = column_of(table, &cast.expr) {
                // Boolean keys are stored as CQL `boolean`; rendering the
                // inner reference is enough.
                if matches!(cast.ty, stmt::Type::Bool) {
                    return Ok(Some(Rendered {
                        cql: quote(&column.name),
                        binds: vec![],
                    }));
                }
                return Ok(None);
            }

            Ok(None)
        }

        _ => Ok(None),
    }
}

fn column_of<'a>(table: &'a db::Table, expr: &Expr) -> Option<&'a Column> {
    match expr {
        Expr::Reference(ExprReference::Column(column)) => Some(&table.columns[column.column]),
        Expr::Cast(cast) => column_of(table, &cast.expr),
        _ => None,
    }
}

fn const_value(expr: &Expr) -> Option<Value> {
    expr.eval_const().ok()
}

fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Evaluates a filter expression against a fully decoded row.
pub(crate) fn matches(expr: &Expr, record: &ValueRecord) -> bool {
    eval_bool(expr, record).unwrap_or(false)
}

fn eval_bool(expr: &Expr, record: &ValueRecord) -> Result<bool> {
    Ok(match expr {
        Expr::And(and) => {
            for operand in &and.operands {
                if !eval_bool(operand, record)? {
                    return Ok(false);
                }
            }
            true
        }
        Expr::Or(or) => {
            for operand in &or.operands {
                if eval_bool(operand, record)? {
                    return Ok(true);
                }
            }
            false
        }
        Expr::Not(not) => !eval_bool(&not.expr, record)?,
        Expr::Value(Value::Bool(value)) => *value,
        Expr::BinaryOp(op) => {
            let lhs = eval_value(&op.lhs, record)?;
            let rhs = eval_value(&op.rhs, record)?;

            if lhs.is_null() || rhs.is_null() {
                // Cassandra has no SQL three-valued logic for these key-value
                // filters: a missing value is simply "not equal" to a present
                // one. This is what the engine expects for negated membership.
                return Ok(match op.op {
                    BinaryOp::Ne => lhs.is_null() != rhs.is_null(),
                    _ => false,
                });
            }

            match op.op {
                BinaryOp::Eq => lhs == rhs,
                BinaryOp::Ne => lhs != rhs,
                BinaryOp::Gt => lhs > rhs,
                BinaryOp::Ge => lhs >= rhs,
                BinaryOp::Lt => lhs < rhs,
                BinaryOp::Le => lhs <= rhs,
                BinaryOp::Add | BinaryOp::Sub => {
                    return Err(Error::unsupported_feature(
                        "Cassandra cannot evaluate arithmetic in a filter",
                    ));
                }
            }
        }
        Expr::Between(between) => {
            let value = eval_value(&between.expr, record)?;
            let low = eval_value(&between.low, record)?;
            let high = eval_value(&between.high, record)?;
            !value.is_null() && !low.is_null() && !high.is_null() && value >= low && value <= high
        }
        Expr::InList(in_list) => {
            let value = eval_value(&in_list.expr, record)?;
            let list = eval_value(&in_list.list, record)?;
            match list {
                Value::List(items) => !value.is_null() && items.contains(&value),
                _ => false,
            }
        }
        Expr::IsNull(is_null) => {
            let value = eval_value(&is_null.expr, record)?;
            value.is_null() != is_null.negated
        }
        Expr::StartsWith(starts_with) => {
            let value = eval_value(&starts_with.expr, record)?;
            let prefix = eval_value(&starts_with.prefix, record)?;
            match (value, prefix) {
                (Value::String(value), Value::String(prefix)) => value.starts_with(&prefix),
                _ => false,
            }
        }
        Expr::Like(like) => {
            let value = eval_value(&like.expr, record)?;
            let pattern = eval_value(&like.pattern, record)?;
            match (value, pattern) {
                (Value::String(value), Value::String(pattern)) => {
                    let (value, pattern) = if like.case_insensitive {
                        (value.to_lowercase(), pattern.to_lowercase())
                    } else {
                        (value, pattern)
                    };
                    let value = value.chars().collect::<Vec<_>>();
                    let pattern = pattern.chars().collect::<Vec<_>>();
                    like_match(&value, &pattern, like.escape)
                }
                _ => false,
            }
        }
        Expr::AnyOp(any) => {
            let lhs = eval_value(&any.lhs, record)?;
            let rhs = eval_value(&any.rhs, record)?;

            if lhs.is_null() {
                return Ok(false);
            }

            match (&lhs, &rhs) {
                (Value::List(items), value) => items.contains(value),
                (value, Value::List(items)) => items.contains(value),
                _ => false,
            }
        }
        _ => expr.eval_bool(RecordInput(record))?,
    })
}

fn eval_value(expr: &Expr, record: &ValueRecord) -> Result<Value> {
    Ok(match expr {
        Expr::Value(value) => value.clone(),
        Expr::Reference(ExprReference::Column(column)) => record.fields[column.column].clone(),
        Expr::Length(length) => {
            let value = eval_value(&length.expr, record)?;
            let len = match value {
                Value::List(items) => items.len(),
                Value::String(value) => value.chars().count(),
                Value::Bytes(value) => value.len(),
                Value::Null => 0,
                other => {
                    return Err(Error::unsupported_feature(format!(
                        "Cassandra cannot evaluate length of {other:?}"
                    )));
                }
            };
            Value::I64(len as i64)
        }
        Expr::Cast(cast) => {
            let value = eval_value(&cast.expr, record)?;
            cast.ty.cast(&(), value)?
        }
        _ => expr.eval(RecordInput(record))?,
    })
}

fn like_match(value: &[char], pattern: &[char], escape: Option<char>) -> bool {
    if pattern.is_empty() {
        return value.is_empty();
    }

    if let Some(escape) = escape
        && pattern[0] == escape
        && pattern.len() > 1
    {
        return !value.is_empty()
            && value[0] == pattern[1]
            && like_match(&value[1..], &pattern[2..], Some(escape));
    }

    match pattern[0] {
        '%' => {
            like_match(value, &pattern[1..], escape)
                || (!value.is_empty() && like_match(&value[1..], pattern, escape))
        }
        '_' => !value.is_empty() && like_match(&value[1..], &pattern[1..], escape),
        ch => !value.is_empty() && value[0] == ch && like_match(&value[1..], &pattern[1..], escape),
    }
}

/// Evaluates an expression against a fully decoded row.
pub(crate) fn eval(expr: &Expr, record: &ValueRecord) -> Result<Value> {
    expr.eval(RecordInput(record))
}

struct RecordInput<'a>(&'a ValueRecord);

impl Input for RecordInput<'_> {
    fn resolve_ref(
        &mut self,
        expr_reference: &ExprReference,
        projection: &Projection,
    ) -> Option<Expr> {
        match expr_reference {
            ExprReference::Column(column) => {
                Some(self.0.fields[column.column].entry(projection).to_expr())
            }
            _ => None,
        }
    }
}

/// Returns `true` when every partition-key column is restricted to a single
/// value by the predicate.
pub(crate) fn full_partition_restriction(table: &db::Table, expr: &Expr) -> bool {
    let partition = {
        let index = &table.indices[table.primary_key.index.index];
        let columns = index
            .columns
            .iter()
            .filter(|index_column| index_column.scope.is_partition())
            .map(|index_column| table.column(index_column.column))
            .collect::<Vec<_>>();

        if columns.is_empty() {
            table.primary_key_columns().collect()
        } else {
            columns
        }
    };

    partition
        .iter()
        .all(|column| has_equality(table, expr, column))
}

fn has_equality(table: &db::Table, expr: &Expr, column: &Column) -> bool {
    match expr {
        Expr::And(and) => and
            .operands
            .iter()
            .any(|operand| has_equality(table, operand, column)),
        Expr::BinaryOp(op) if op.op == BinaryOp::Eq => {
            let lhs_is_column = column_of(table, &op.lhs).map(|c| c.id == column.id) == Some(true);
            let rhs_is_column = column_of(table, &op.rhs).map(|c| c.id == column.id) == Some(true);

            (lhs_is_column && const_value(&op.rhs).is_some())
                || (rhs_is_column && const_value(&op.lhs).is_some())
        }
        Expr::InList(in_list) => {
            column_of(table, &in_list.expr).map(|c| c.id == column.id) == Some(true)
                && matches!(const_value(&in_list.list), Some(Value::List(_)))
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use toasty_core::schema::db::{ColumnId, TableId};

    fn column(index: usize, name: &str) -> Column {
        Column {
            id: ColumnId {
                table: TableId(0),
                index,
            },
            name: name.into(),
            ty: stmt::Type::String,
            storage_ty: db::Type::Text,
            nullable: true,
            primary_key: false,
            auto_increment: false,
            versionable: false,
        }
    }

    fn table() -> db::Table {
        db::Table {
            id: TableId(0),
            name: "users".into(),
            columns: vec![column(0, "name"), column(1, "age")],
            primary_key: db::PrimaryKey {
                columns: vec![ColumnId {
                    table: TableId(0),
                    index: 0,
                }],
                index: db::IndexId {
                    table: TableId(0),
                    index: 0,
                },
            },
            indices: vec![],
        }
    }

    fn column_ref(index: usize) -> Expr {
        Expr::Reference(ExprReference::Column(stmt::ExprColumn {
            nesting: 0,
            table: 0,
            column: index,
        }))
    }

    #[test]
    fn renders_equality() {
        let expr = Expr::eq(column_ref(0), Expr::Value(Value::String("ada".into())));
        let rendered = render(&table(), &expr).unwrap().unwrap();
        assert_eq!(rendered.cql, "\"name\" = ?");
        assert_eq!(rendered.binds.len(), 1);
    }

    #[test]
    fn renders_starts_with_as_like() {
        let expr = Expr::starts_with(column_ref(0), Expr::Value(Value::String("ad".into())));
        let rendered = render(&table(), &expr).unwrap().unwrap();
        assert_eq!(rendered.cql, "\"name\" LIKE ?");
        assert_eq!(rendered.binds[0], Some(CqlValue::Text("ad%".into())));
    }

    #[test]
    fn leaves_disjunction_to_the_client() {
        let lhs = Expr::eq(column_ref(0), Expr::Value(Value::String("a".into())));
        let rhs = Expr::eq(column_ref(0), Expr::Value(Value::String("b".into())));
        let expr = Expr::or(lhs, rhs);
        assert!(render(&table(), &expr).unwrap().is_none());
    }
}
