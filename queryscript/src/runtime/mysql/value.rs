use std::any::Any;
use std::sync::Arc;

use crate::types::{
    record::VecRow, value::ArrowRecordBatch, Field, Record, RecordBatch, Relation, Value,
};
use mysql_async::{
    prelude::{ConvIr, FromRow, FromValue},
    FromRowError, FromValueError, Row as MRow, Value as MValue,
};

impl Into<MValue> for Value {
    fn into(self) -> MValue {
        todo!()
    }
}

pub struct ValueIr {
    source_value: MValue,
    target_value: Value,
}

impl ConvIr<Value> for ValueIr {
    fn new(v: MValue) -> Result<ValueIr, FromValueError> {
        let target_value = match &v {
            MValue::NULL => Value::Null,
            MValue::Bytes(bytes) => Value::Binary(bytes.clone()),
            MValue::Int(i) => Value::Int64(*i),
            MValue::UInt(u) => Value::UInt64(*u),
            MValue::Float(f32) => Value::Float32(*f32),
            MValue::Double(f64) => Value::Float64(*f64),
            MValue::Date(..) | MValue::Time(..) => return Err(FromValueError(v)),
        };

        Ok(ValueIr {
            source_value: v,
            target_value,
        })
    }
    fn commit(self) -> Value {
        self.target_value
    }
    fn rollback(self) -> MValue {
        self.source_value
    }
}

impl FromValue for Value {
    type Intermediate = ValueIr;
}

#[derive(Debug, Clone)]
pub struct MySQLRow(pub Vec<Value>);

impl FromRow for MySQLRow {
    fn from_row_opt(mut row: MRow) -> Result<Self, FromRowError> {
        match (0..row.len())
            .map(|i| row.take_opt(i).unwrap())
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(values) => Ok(MySQLRow(values)),
            Err(_) => Err(FromRowError(row)),
        }
    }
}

#[derive(Debug)]
pub struct MySQLRelation {
    pub rows: Vec<Arc<dyn Record>>,
    pub schema: Arc<Vec<Field>>,
}

impl Relation for MySQLRelation {
    fn schema(&self) -> Vec<Field> {
        self.schema.as_ref().clone()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn num_batches(&self) -> usize {
        1
    }
    fn batch(&self, _index: usize) -> &dyn RecordBatch {
        self
    }

    fn try_cast(
        &self,
        target_schema: &Vec<Field>,
    ) -> crate::types::error::Result<Arc<dyn Relation>> {
        Ok(Arc::new(MySQLRelation {
            rows: self.rows.clone(),
            schema: Arc::new(target_schema.clone()),
        }))
    }
}

impl RecordBatch for MySQLRelation {
    fn schema(&self) -> Vec<Field> {
        self.schema.as_ref().clone()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn records(&self) -> Vec<Arc<dyn Record>> {
        self.rows.clone()
    }

    fn as_arrow_recordbatch(&self) -> &ArrowRecordBatch {
        todo!()
    }
}
