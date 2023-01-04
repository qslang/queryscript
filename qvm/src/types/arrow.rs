use arrow::{
    array::{
        Date32Array, Date64Array, FixedSizeBinaryArray, GenericBinaryArray, IntervalDayTimeArray,
        IntervalMonthDayNanoArray, IntervalYearMonthArray, LargeStringArray, PrimitiveArray,
        StringArray, Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray,
        Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray,
    },
    datatypes::ArrowPrimitiveType,
};
use arrow_schema::SchemaRef as ArrowSchemaRef;

use super::list::VecList;
use super::record::VecRow;
use super::types::{try_arrow_fields_to_fields, Field, Type};
use super::value::*;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ArrowRecordBatchRelation {
    schema: ArrowSchemaRef,
    batches: Arc<Vec<ArrowRecordBatch>>,
}

impl ArrowRecordBatchRelation {
    pub fn new(schema: ArrowSchemaRef, batches: Arc<Vec<ArrowRecordBatch>>) -> Arc<dyn Relation> {
        Arc::new(Self { schema, batches })
    }
}

impl Relation for ArrowRecordBatchRelation {
    fn schema(&self) -> Vec<Field> {
        let type_: Type = self.schema.as_ref().try_into().unwrap();
        match type_ {
            Type::Record(fields) => fields,
            _ => panic!("Expected record type"),
        }
    }

    fn num_batches(&self) -> usize {
        self.batches.len()
    }

    fn batch(&self, index: usize) -> &dyn RecordBatch {
        &self.batches[index]
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_arrow_recordbatch(self: Arc<Self>) -> Arc<Vec<ArrowRecordBatch>> {
        self.batches.clone()
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

macro_rules! downcast_to_vec {
    ($val:expr, $array_ty:ty) => {
        T::as_any($val)
            .downcast_ref::<$array_ty>()
            .unwrap()
            .to_vec()
    };
}

impl<'a, T: ArrowArray> List for &'a T {
    fn data_type(&self) -> Type {
        T::data_type(self)
            .try_into()
            .expect("Arrow array type convert to qvm")
    }

    fn as_any(&self) -> &dyn Any {
        T::as_any(self)
    }

    fn as_vec(&self) -> Vec<Value> {
        use super::ArrowDataType::*;
        use arrow::array::*;
        use arrow::datatypes::*;
        let vec_list: VecWrapper = match T::data_type(self) {
            Null => VecWrapper(
                (0..(as_null_array(self).data().len()))
                    .map(|_| Value::Null)
                    .collect(),
            ),

            // These are special cases
            Boolean => VecWrapper(as_boolean_array(self).iter().map(|x| x.into()).collect()),
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

            Utf8 => as_string_array(self).to_vec(),
            LargeUtf8 => as_largestring_array(self).to_vec(),
            Binary => as_generic_binary_array::<i32>(self).to_vec(),
            LargeBinary => as_generic_binary_array::<i64>(self).to_vec(),
            FixedSizeBinary(_) => T::as_any(self)
                .downcast_ref::<FixedSizeBinaryArray>()
                .unwrap()
                .to_vec(),

            Timestamp(u, _) => match u {
                TimeUnit::Second => downcast_to_vec!(self, TimestampSecondArray),
                TimeUnit::Millisecond => downcast_to_vec!(self, TimestampMillisecondArray),
                TimeUnit::Microsecond => downcast_to_vec!(self, TimestampMicrosecondArray),
                TimeUnit::Nanosecond => downcast_to_vec!(self, TimestampNanosecondArray),
            },
            Date32 => downcast_to_vec!(self, Date32Array),
            Date64 => downcast_to_vec!(self, Date64Array),
            Time64(u) => match u {
                TimeUnit::Second => downcast_to_vec!(self, Time32SecondArray),
                TimeUnit::Millisecond => downcast_to_vec!(self, Time32MillisecondArray),
                TimeUnit::Microsecond => downcast_to_vec!(self, Time64MicrosecondArray),
                TimeUnit::Nanosecond => downcast_to_vec!(self, Time64NanosecondArray),
            },
            Interval(u) => match u {
                IntervalUnit::YearMonth => downcast_to_vec!(self, IntervalYearMonthArray),
                IntervalUnit::DayTime => downcast_to_vec!(self, IntervalDayTimeArray),
                IntervalUnit::MonthDayNano => downcast_to_vec!(self, IntervalMonthDayNanoArray),
            },
            Decimal128(..) => as_primitive_array::<Decimal128Type>(self).into(),
            Decimal256(..) => as_primitive_array::<Decimal256Type>(self).into(),
            List(field_type) => {
                let dt: Arc<Type> = Arc::new(field_type.data_type().try_into().unwrap());
                VecWrapper(
                    as_list_array(self)
                        .iter()
                        .map(|x| match x {
                            Some(v) => Value::List(VecList::new(dt.clone(), (&v).as_vec())),
                            None => Value::Null,
                        })
                        .collect(),
                )
            }
            LargeList(field_type) => {
                let dt: Arc<Type> = Arc::new(field_type.data_type().try_into().unwrap());
                VecWrapper(
                    as_large_list_array(self)
                        .iter()
                        .map(|x| match x {
                            Some(v) => Value::List(VecList::new(dt.clone(), (&v).as_vec())),
                            None => Value::Null,
                        })
                        .collect(),
                )
            }
            FixedSizeList(field_type, _) => {
                let dt: Arc<Type> = Arc::new(field_type.data_type().try_into().unwrap());
                let fsl = T::as_any(self)
                    .downcast_ref::<FixedSizeListArray>()
                    .expect("Unable to downcast to fixed size list array");
                VecWrapper(
                    (0..(fsl.len()))
                        .map(|i| Value::List(VecList::new(dt.clone(), (&fsl.value(i)).as_vec())))
                        .collect(),
                )
            }
            Struct(fields) => {
                let schema: Arc<Vec<super::types::Field>> = Arc::new(
                    try_arrow_fields_to_fields(fields).expect("Failed to convert struct fields"),
                );
                let struct_array = T::as_any(self)
                    .downcast_ref::<StructArray>()
                    .expect("Unable to downcast to struct array");

                // NOTE: This is the same fundamental operation as RecordBatch::records
                let mut columns: Vec<_> = struct_array
                    .columns()
                    .iter()
                    .map(|col| col.as_vec().into_iter())
                    .collect();

                VecWrapper(
                    (0..(struct_array.len()))
                        .map(|_| {
                            Value::Record(VecRow::new(
                                schema.clone(),
                                columns.iter_mut().map(|col| col.next().unwrap()).collect(),
                            ))
                        })
                        .collect(),
                )
            }
            Union(..) | Dictionary(..) | Map(..) | Time32(..) | Duration(..) => {
                panic!("unsupported array type {:?}", T::data_type(self))
            }
        };
        vec_list.0
    }
}

struct VecWrapper(Vec<Value>);

impl<T: ArrowPrimitiveType> From<&PrimitiveArray<T>> for VecWrapper
where
    Value: From<T::Native>,
{
    fn from(array: &PrimitiveArray<T>) -> Self {
        VecWrapper(
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

trait ArrayConvert {
    fn to_vec(&self) -> VecWrapper;
}

macro_rules! array_conversion {
    ($array_ty:ty, $arm:tt) => {
        impl ArrayConvert for $array_ty {
            fn to_vec(&self) -> VecWrapper {
                VecWrapper(
                    self.iter()
                        .map(|x| match x {
                            Some(v) => Value::$arm(v.into()),
                            None => Value::Null,
                        })
                        .collect(),
                )
            }
        }
    };
}

array_conversion!(StringArray, Utf8);
array_conversion!(LargeStringArray, Utf8);
array_conversion!(GenericBinaryArray<i32>, Binary);
array_conversion!(GenericBinaryArray<i64>, Binary);
array_conversion!(FixedSizeBinaryArray, Binary);
array_conversion!(Date32Array, Date32);
array_conversion!(Date64Array, Date64);
array_conversion!(Time32SecondArray, Time32Second);
array_conversion!(Time32MillisecondArray, Time32Millisecond);
array_conversion!(Time64MicrosecondArray, Time64Microsecond);
array_conversion!(Time64NanosecondArray, Time64Nanosecond);
array_conversion!(IntervalYearMonthArray, IntervalYearMonth);
array_conversion!(IntervalDayTimeArray, IntervalDayTime);
array_conversion!(IntervalMonthDayNanoArray, IntervalMonthDayNano);

macro_rules! timestamp_array_conversion {
    ($array_ty:ty, $arm:tt) => {
        impl ArrayConvert for $array_ty {
            fn to_vec(&self) -> VecWrapper {
                VecWrapper(
                    self.iter()
                        .map(|x| match x {
                            Some(v) => Value::$arm(v.into(), None),
                            None => Value::Null,
                        })
                        .collect(),
                )
            }
        }
    };
}

timestamp_array_conversion!(TimestampSecondArray, TimestampSecond);
timestamp_array_conversion!(TimestampMillisecondArray, TimestampMillisecond);
timestamp_array_conversion!(TimestampMicrosecondArray, TimestampMicrosecond);
timestamp_array_conversion!(TimestampNanosecondArray, TimestampNanosecond);
