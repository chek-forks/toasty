use scylla::value::{CqlValue, Row};
use toasty_core::{
    Error, Result,
    schema::db::Column,
    stmt::{self, Value, ValueObject, ValueRecord},
};

/// Encodes a value for the given column, narrowing it to the column's
/// application type first.
pub(crate) fn encode_for_column(column: &Column, value: &Value) -> Result<Option<CqlValue>> {
    if value.is_null() {
        return Ok(None);
    }

    // Documents (embedded structs and `Vec<embed>`) are stored as JSON text;
    // their application type is `Object` / `List(Object)`, not a CQL
    // collection.
    if matches!(
        column.storage_ty,
        toasty_core::schema::db::Type::Document { .. }
    ) || (matches!(column.storage_ty, toasty_core::schema::db::Type::Text)
        && matches!(value, Value::Object(_) | Value::List(_)))
    {
        let json = value_to_json(value)?;
        return Ok(Some(CqlValue::Text(
            serde_json::to_string(&json)
                .map_err(|error| Error::invalid_statement(error.to_string()))?,
        )));
    }

    let target = match &column.storage_ty {
        // Text-backed columns keep the application type so the value codec can
        // choose the canonical text form (temporals, decimals, …).
        toasty_core::schema::db::Type::Text
        | toasty_core::schema::db::Type::VarChar(_)
        | toasty_core::schema::db::Type::Document { .. } => column.ty.clone(),
        // Explicit `#[column(type = ...)]` numeric overrides need a cast to the
        // storage width; `bridge_type` only covers integer widening.
        toasty_core::schema::db::Type::Float(1..=4) => stmt::Type::F32,
        toasty_core::schema::db::Type::Float(_) => stmt::Type::F64,
        toasty_core::schema::db::Type::Integer(1) => stmt::Type::I8,
        toasty_core::schema::db::Type::Integer(2) => stmt::Type::I16,
        toasty_core::schema::db::Type::Integer(3..=4) => stmt::Type::I32,
        toasty_core::schema::db::Type::Integer(_) => stmt::Type::I64,
        toasty_core::schema::db::Type::UnsignedInteger(1) => stmt::Type::U8,
        toasty_core::schema::db::Type::UnsignedInteger(2) => stmt::Type::U16,
        toasty_core::schema::db::Type::UnsignedInteger(3..=4) => stmt::Type::U32,
        toasty_core::schema::db::Type::UnsignedInteger(_) => stmt::Type::U64,
        storage_ty => storage_ty.bridge_type(&column.ty),
    };

    let value = if same_shape(&target, value) {
        value.clone()
    } else {
        target.cast(&(), value.clone())?
    };

    encode(&value)
}

/// Encodes a Toasty value as a CQL value. `Null` maps to `None`.
#[allow(unreachable_patterns)]
pub(crate) fn encode(value: &Value) -> Result<Option<CqlValue>> {
    let value = match value {
        Value::Null => return Ok(None),
        Value::Bool(v) => CqlValue::Boolean(*v),
        Value::I8(v) => CqlValue::TinyInt(*v),
        Value::I16(v) => CqlValue::SmallInt(*v),
        Value::I32(v) => CqlValue::Int(*v),
        Value::I64(v) => CqlValue::BigInt(*v),
        Value::U8(v) => CqlValue::SmallInt(i16::from(*v)),
        Value::U16(v) => CqlValue::Int(i32::from(*v)),
        Value::U32(v) => CqlValue::BigInt(i64::from(*v)),
        Value::U64(v) => {
            let v = i64::try_from(*v).map_err(|_| {
                Error::invalid_statement(format!(
                    "Cassandra cannot store unsigned integer {v}: it exceeds i64::MAX"
                ))
            })?;
            CqlValue::BigInt(v)
        }
        Value::F32(v) => CqlValue::Float(*v),
        Value::F64(v) => CqlValue::Double(*v),
        Value::String(v) => CqlValue::Text(v.clone()),
        Value::Bytes(v) => CqlValue::Blob(v.clone()),
        Value::Uuid(v) => CqlValue::Uuid(*v),
        Value::List(items) => CqlValue::List(
            items
                .iter()
                .map(|item| {
                    encode(item)?.ok_or_else(|| {
                        Error::invalid_statement("CQL collections cannot contain null elements")
                    })
                })
                .collect::<Result<Vec<_>>>()?,
        ),
        Value::Object(object) => CqlValue::Text(
            serde_json::to_string(&object_to_json(object)?)
                .map_err(|error| Error::invalid_statement(error.to_string()))?,
        ),
        Value::Record(_) => {
            return Err(Error::invalid_statement(
                "cannot bind a record as a single CQL value",
            ));
        }
        Value::SparseRecord(_) => {
            return Err(Error::invalid_statement(
                "cannot bind a sparse record as a CQL value",
            ));
        }
        other => match other.document_storage_text() {
            Some(text) => CqlValue::Text(text.to_string()),
            None => {
                return Err(Error::invalid_statement(format!(
                    "value {other:?} has no Cassandra representation"
                )));
            }
        },
    };

    Ok(Some(value))
}

/// Decodes a CQL value into a Toasty value of the requested application type.
pub(crate) fn decode(ty: &stmt::Type, value: Option<&CqlValue>) -> Result<Value> {
    let Some(value) = value else {
        // Cassandra does not distinguish an empty collection from a null one;
        // a `Vec<T>` field reads back as an empty list.
        if matches!(ty, stmt::Type::List(_)) {
            return Ok(Value::List(Vec::new()));
        }
        return Ok(Value::Null);
    };

    if matches!(ty, stmt::Type::Object) {
        return match value {
            CqlValue::Text(text) | CqlValue::Ascii(text) => {
                let json: serde_json::Value = serde_json::from_str(text)
                    .map_err(|error| Error::invalid_result(error.to_string()))?;
                json_to_value(&json)
            }
            CqlValue::Map(entries) => {
                let mut fields = Vec::with_capacity(entries.len());
                for (key, value) in entries {
                    let key = match key {
                        CqlValue::Text(key) | CqlValue::Ascii(key) => key.clone(),
                        other => {
                            return Err(Error::invalid_result(format!(
                                "document map key is not text: {other:?}"
                            )));
                        }
                    };
                    fields.push((key, decode(&stmt::Type::Unknown, Some(value))?));
                }
                Ok(Value::Object(ValueObject::from_vec(fields)))
            }
            other => Err(Error::invalid_result(format!(
                "expected text document, got {other:?}"
            ))),
        };
    }

    if let stmt::Type::List(elem) = ty {
        if matches!(**elem, stmt::Type::Object)
            && let CqlValue::Text(text) | CqlValue::Ascii(text) = value
        {
            let json: serde_json::Value = serde_json::from_str(text)
                .map_err(|error| Error::invalid_result(error.to_string()))?;
            return json_to_value(&json);
        }

        return match value {
            CqlValue::List(items) | CqlValue::Set(items) | CqlValue::Vector(items) => {
                Ok(Value::List(
                    items
                        .iter()
                        .map(|item| decode(elem, Some(item)))
                        .collect::<Result<Vec<_>>>()?,
                ))
            }
            // ScyllaDB reports an empty collection as `Empty`; keep it a list.
            CqlValue::Empty => Ok(Value::List(Vec::new())),
            CqlValue::Tuple(_) => Ok(Value::Null),
            other => Err(Error::invalid_result(format!(
                "expected a CQL list, got {other:?}"
            ))),
        };
    }

    let base = match value {
        CqlValue::Boolean(v) => Value::Bool(*v),
        CqlValue::TinyInt(v) => Value::I8(*v),
        CqlValue::SmallInt(v) => Value::I16(*v),
        CqlValue::Int(v) => Value::I32(*v),
        CqlValue::BigInt(v) => Value::I64(*v),
        CqlValue::Counter(v) => Value::I64(v.0),
        CqlValue::Float(v) => Value::F32(*v),
        CqlValue::Double(v) => Value::F64(*v),
        CqlValue::Text(v) | CqlValue::Ascii(v) => Value::String(v.clone()),
        CqlValue::Blob(v) => Value::Bytes(v.clone()),
        CqlValue::Uuid(v) => Value::Uuid(*v),
        CqlValue::Timeuuid(v) => Value::Uuid(uuid::Uuid::from_u128(v.as_u128())),
        CqlValue::Empty => return Ok(Value::Null),
        other => {
            return Err(Error::invalid_result(format!(
                "unsupported CQL value for column type {ty:?}: {other:?}"
            )));
        }
    };

    if same_shape(ty, &base) {
        Ok(base)
    } else {
        ty.cast(&(), base)
    }
}

/// Returns `true` when the value already has the requested type, avoiding a
/// no-op cast (some identity casts are not implemented in `toasty-core`).
pub(crate) fn same_shape(ty: &stmt::Type, value: &Value) -> bool {
    matches!(
        (ty, value),
        (stmt::Type::Unknown, _)
            | (stmt::Type::Bool, Value::Bool(_))
            | (stmt::Type::I8, Value::I8(_))
            | (stmt::Type::I16, Value::I16(_))
            | (stmt::Type::I32, Value::I32(_))
            | (stmt::Type::I64, Value::I64(_))
            | (stmt::Type::U8, Value::U8(_))
            | (stmt::Type::U16, Value::U16(_))
            | (stmt::Type::U32, Value::U32(_))
            | (stmt::Type::U64, Value::U64(_))
            | (stmt::Type::F32, Value::F32(_))
            | (stmt::Type::F64, Value::F64(_))
            | (stmt::Type::String, Value::String(_))
            | (stmt::Type::Bytes, Value::Bytes(_))
            | (stmt::Type::Uuid, Value::Uuid(_))
            | (stmt::Type::List(_), Value::List(_))
            | (stmt::Type::Object, Value::Object(_))
    )
}

/// Decodes a row into a record containing the requested columns, in order.
pub(crate) fn decode_row(row: &Row, columns: &[&Column]) -> Result<ValueRecord> {
    if row.columns.len() != columns.len() {
        return Err(Error::invalid_result(format!(
            "expected {} columns, got {}",
            columns.len(),
            row.columns.len()
        )));
    }

    let mut fields = Vec::with_capacity(columns.len());

    for (column, value) in columns.iter().zip(row.columns.iter()) {
        fields.push(decode(&column.ty, value.as_ref())?);
    }

    Ok(ValueRecord::from_vec(fields))
}

fn object_to_json(object: &ValueObject) -> Result<serde_json::Value> {
    let mut map = serde_json::Map::with_capacity(object.entries.len());

    for (key, value) in object.iter() {
        map.insert(key.clone(), value_to_json(value)?);
    }

    Ok(serde_json::Value::Object(map))
}

#[allow(unreachable_patterns)]
fn value_to_json(value: &Value) -> Result<serde_json::Value> {
    Ok(match value {
        Value::Null => serde_json::Value::Null,
        Value::Bool(v) => serde_json::Value::Bool(*v),
        Value::I8(v) => (*v).into(),
        Value::I16(v) => (*v).into(),
        Value::I32(v) => (*v).into(),
        Value::I64(v) => (*v).into(),
        Value::U8(v) => (*v).into(),
        Value::U16(v) => (*v).into(),
        Value::U32(v) => (*v).into(),
        Value::U64(v) => (*v).into(),
        Value::F32(v) => serde_json::Number::from_f64(f64::from(*v))
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::F64(v) => serde_json::Number::from_f64(*v)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::String(v) => serde_json::Value::String(v.clone()),
        Value::Uuid(v) => serde_json::Value::String(v.to_string()),
        Value::List(items) => serde_json::Value::Array(
            items
                .iter()
                .map(value_to_json)
                .collect::<Result<Vec<_>>>()?,
        ),
        Value::Object(object) => object_to_json(object)?,
        Value::Record(_) => {
            return Err(Error::invalid_statement(
                "cannot encode a record inside a document",
            ));
        }
        Value::SparseRecord(_) => {
            return Err(Error::invalid_statement(
                "cannot encode a sparse record inside a document",
            ));
        }
        other => match other.document_storage_text() {
            Some(text) => serde_json::Value::String(text.to_string()),
            None => {
                return Err(Error::invalid_statement(format!(
                    "value {other:?} has no document representation"
                )));
            }
        },
    })
}

fn json_to_value(value: &serde_json::Value) -> Result<Value> {
    Ok(match value {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(v) => Value::Bool(*v),
        serde_json::Value::Number(v) => {
            if let Some(v) = v.as_i64() {
                Value::I64(v)
            } else if let Some(v) = v.as_u64() {
                Value::U64(v)
            } else if let Some(v) = v.as_f64() {
                Value::F64(v)
            } else {
                return Err(Error::invalid_result(format!(
                    "unsupported JSON number: {v}"
                )));
            }
        }
        serde_json::Value::String(v) => Value::String(v.clone()),
        serde_json::Value::Array(items) => Value::List(
            items
                .iter()
                .map(json_to_value)
                .collect::<Result<Vec<_>>>()?,
        ),
        serde_json::Value::Object(map) => {
            let mut entries = Vec::with_capacity(map.len());
            for (key, value) in map {
                entries.push((key.clone(), json_to_value(value)?));
            }
            Value::Object(ValueObject::from_vec(entries))
        }
    })
}

/// Builds a record from the values a key operation selected.
pub(crate) fn project(record: &ValueRecord, columns: &[&Column]) -> ValueRecord {
    ValueRecord::from_vec(
        columns
            .iter()
            .map(|column| record.fields[column.id.index].clone())
            .collect(),
    )
}

/// Builds the bind values for a primary-key value.
pub(crate) fn key_binds(
    table: &toasty_core::schema::db::Table,
    key: &Value,
) -> Result<Vec<Option<CqlValue>>> {
    let pk: Vec<&Column> = table.primary_key_columns().collect();

    let values: Vec<&Value> = match key {
        Value::Record(record) => {
            if record.fields.len() != pk.len() {
                return Err(Error::invalid_statement(format!(
                    "key has {} fields but table `{}` has {} primary-key columns",
                    record.fields.len(),
                    table.name,
                    pk.len()
                )));
            }
            record.fields.iter().collect()
        }
        value => {
            if pk.len() != 1 {
                return Err(Error::invalid_statement(format!(
                    "key for table `{}` must be a record of {} values",
                    table.name,
                    pk.len()
                )));
            }
            vec![value]
        }
    };

    pk.iter()
        .zip(values)
        .map(|(column, value)| encode_for_column(column, value))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use toasty_core::schema::db::{Column, ColumnId, TableId};
    use toasty_core::stmt::Type;

    fn column(ty: Type) -> Column {
        Column {
            id: ColumnId {
                table: TableId(0),
                index: 0,
            },
            name: "c".into(),
            ty: ty.clone(),
            storage_ty: toasty_core::schema::db::Type::Text,
            nullable: true,
            primary_key: false,
            auto_increment: false,
            versionable: false,
        }
    }

    #[test]
    fn narrows_integers_to_the_column_type() {
        let column = column(Type::I16);
        let encoded = encode_for_column(&column, &Value::I64(7)).unwrap();
        assert_eq!(encoded, Some(CqlValue::SmallInt(7)));
    }

    #[test]
    fn uuid_round_trips() {
        let uuid = uuid::Uuid::now_v7();
        let encoded = encode(&Value::Uuid(uuid)).unwrap().unwrap();
        assert_eq!(
            decode(&Type::Uuid, Some(&encoded)).unwrap(),
            Value::Uuid(uuid)
        );
    }

    #[test]
    fn list_round_trips() {
        let value = Value::List(vec![Value::I32(1), Value::I32(2)]);
        let encoded = encode(&value).unwrap().unwrap();
        assert_eq!(
            decode(&Type::List(Box::new(Type::I32)), Some(&encoded)).unwrap(),
            value
        );
    }

    #[test]
    fn document_round_trips_through_json() {
        let value = Value::Object(ValueObject::from_vec(vec![
            ("name".into(), Value::String("ada".into())),
            ("age".into(), Value::I64(36)),
        ]));
        let encoded = encode(&value).unwrap().unwrap();
        assert_eq!(decode(&Type::Object, Some(&encoded)).unwrap(), value);
    }
}
