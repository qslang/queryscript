use datafusion::arrow::array::{
    as_boolean_array, as_null_array, as_primitive_array, PrimitiveArray,
};
use datafusion::arrow::datatypes::ArrowPrimitiveType;

use super::record::VecRow;
use super::types::{try_arrow_fields_to_fields, Field, Type};
use super::value::*;

impl Relation for Vec<ArrowRecordBatch> {
    fn schema(&self) -> Vec<Field> {
        if self.len() == 0 {
            panic!("Empty vector of record batches. Cannot derive schema.");
        }

        self.batch(0).schema()
    }

    fn num_batches(&self) -> usize {
        self.len()
    }

    fn batch(&self, index: usize) -> &dyn RecordBatch {
        &self[index]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_arrow_recordbatch(self: Arc<Self>) -> Arc<Vec<ArrowRecordBatch>> {
        self
    }
}

impl RecordBatch for ArrowRecordBatch {
    fn schema(&self) -> Vec<Field> {
        try_arrow_fields_to_fields(&self.schema().fields).expect("Failed to convert relation type")
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn records(&self) -> Vec<Arc<dyn Record>> {
        let schema = Arc::new(RecordBatch::schema(self));
        let mut columns: Vec<_> = self
            .columns()
            .iter()
            .map(|col| col.as_vec().into_iter())
            .collect();
        (0..(self.num_rows()))
            .map(|_| {
                VecRow::new(
                    schema.clone(),
                    columns.iter_mut().map(|col| col.next().unwrap()).collect(),
                )
            })
            .collect()
    }
}

impl<T: ArrowArray + 'static> List for T {
    fn data_type(&self) -> Type {
        self.data_type()
            .try_into()
            .expect("Arrow array type convert to qvm")
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_vec(&self) -> Vec<Value> {
        use super::ArrowDataType::*;
        use datafusion::arrow::datatypes::*;
        let vec_list: VecList = match T::data_type(self) {
            Null => VecList(
                (0..(as_null_array(self).data().len()))
                    .map(|_| Value::Null)
                    .collect(),
            ),

            // These are special cases
            Boolean => VecList(
                as_boolean_array(self)
                    .iter()
                    .map(|x| match x {
                        Some(b) => Value::Boolean(b),
                        None => Value::Null,
                    })
                    .collect(),
            ),
            // These can just use the primitive array to convert
            Int8 => as_primitive_array::<Int8Type>(self).into(),
            Int16 => as_primitive_array::<Int16Type>(self).into(),
            Int32 => as_primitive_array::<Int32Type>(self).into(),
            Int64 => as_primitive_array::<Int64Type>(self).into(),
            UInt8 => as_primitive_array::<UInt8Type>(self).into(),
            UInt16 => as_primitive_array::<UInt16Type>(self).into(),
            UInt32 => as_primitive_array::<UInt32Type>(self).into(),
            UInt64 => as_primitive_array::<UInt64Type>(self).into(),
            Float16 => as_primitive_array::<Float16Type>(self).into(),
            Float32 => as_primitive_array::<Float32Type>(self).into(),
            Float64 => as_primitive_array::<Float64Type>(self).into(),

            /*
            Timestamp(u, s) => Type::Atom(AtomicType::Timestamp(u.clone(), s.clone())),
            Date32 => Type::Atom(AtomicType::Date32),
            Date64 => Type::Atom(AtomicType::Date64),
            Time64(u) => Type::Atom(AtomicType::Time64(u.clone())),
            Interval(u) => Type::Atom(AtomicType::Interval(u.clone())),
            Binary => Type::Atom(AtomicType::Binary),
            FixedSizeBinary(l) => Type::Atom(AtomicType::FixedSizeBinary(*l)),
            LargeBinary => Type::Atom(AtomicType::LargeBinary),
            Utf8 => Type::Atom(AtomicType::Utf8),
            LargeUtf8 => Type::Atom(AtomicType::LargeUtf8),
            Decimal128(p, s) => Type::Atom(AtomicType::Decimal128(*p, *s)),
            Decimal256(p, s) => Type::Atom(AtomicType::Decimal256(*p, *s)),
            List(f) | LargeList(f) | FixedSizeList(f, _) => {
                Type::List(Box::new(f.data_type().try_into()?))
            }
            Struct(fields) => fields.try_into()?,
            Union(..) | Dictionary(..) | Map(..) | Time32(..) | Duration(..) => {
                return ts_unimplemented!("type {:?}", &t)
            }
            */
            t => panic!("unsupported array type {:?}", &t),
        };
        vec_list.0
    }
}

struct VecList(Vec<Value>);

impl<T: ArrowPrimitiveType> From<&PrimitiveArray<T>> for VecList
where
    Value: From<T::Native>,
{
    fn from(array: &PrimitiveArray<T>) -> Self {
        VecList(
            array
                .iter()
                .map(|x| match x {
                    None => Value::Null,
                    Some(x) => x.into(),
                })
                .collect(),
        )
    }
}
