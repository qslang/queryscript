use arrow::datatypes::{Date32Type as ArrowDate32Type, Date64Type as ArrowDate64Type};
use chrono::{TimeZone, Utc};
use serde::ser::{Serialize, SerializeMap, SerializeSeq, Serializer};

use super::value::{Record, Value};

impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match &self {
            Value::Null => serializer.serialize_none(),
            Self::Boolean(x) => x.serialize(serializer),
            Self::Int8(x) => x.serialize(serializer),
            Self::Int16(x) => x.serialize(serializer),
            Self::Int32(x) => x.serialize(serializer),
            Self::Int64(x) => x.serialize(serializer),
            Self::UInt8(x) => x.serialize(serializer),
            Self::UInt16(x) => x.serialize(serializer),
            Self::UInt32(x) => x.serialize(serializer),
            Self::UInt64(x) => x.serialize(serializer),
            Self::Float16(x) => x.serialize(serializer),
            Self::Float32(x) => x.serialize(serializer),
            Self::Float64(x) => x.serialize(serializer),
            Self::Decimal128(x) => x.serialize(serializer),

            // Most systems will parse this automatically as number if it fits
            Self::Decimal256(x) => x.to_string().serialize(serializer),

            Self::Utf8(x) => x.serialize(serializer),
            Self::LargeUtf8(x) => x.serialize(serializer),
            Self::Binary(x) => x.serialize(serializer),
            Self::FixedSizeBinary(_len, buf) => buf.serialize(serializer),
            Self::LargeBinary(x) => x.serialize(serializer),

            Self::List(l) => {
                let v = l.as_vec();

                let mut seq = serializer.serialize_seq(Some(v.len()))?;
                for e in v.iter() {
                    seq.serialize_element(e)?
                }
                seq.end()
            }

            Self::Relation(r) => {
                let v = (0..r.num_batches())
                    .flat_map(|i| r.batch(i).records())
                    .collect::<Vec<_>>();

                let mut seq = serializer.serialize_seq(Some(v.len()))?;
                for e in v.iter() {
                    seq.serialize_element(&e.as_ref())?
                }
                seq.end()
            }

            Self::Record(r) => r.as_ref().serialize(serializer),

            // NOTE: When we add timezone support, we'll probably want to use FixedOffset here.
            // NOTE: We should probably throw an error somewhere here (or earlier in the pipeline) if the timestamp
            // is invalid
            Self::TimestampSecond(x, _tz)
            | Self::TimestampMillisecond(x, _tz)
            | Self::TimestampMicrosecond(x, _tz)
            | Self::TimestampNanosecond(x, _tz) => {
                let (seconds, nanos) = match &self {
                    Self::TimestampSecond(..) => (*x, 0),
                    Self::TimestampMillisecond(..) => (*x / 1000, (*x % 1000) * 1_000_000),
                    Self::TimestampMicrosecond(..) => (*x / 1_000_000, (*x % 1_000_000) * 1_000),
                    Self::TimestampNanosecond(..) => (*x / 1_000_000_000, *x % 1_000_000_000),
                    _ => panic!("unreachable"),
                };
                Utc.timestamp_opt(seconds, nanos as u32)
                    .unwrap()
                    .to_rfc3339()
                    .serialize(serializer)
            }
            Self::Date32(x) => ArrowDate32Type::to_naive_date(*x)
                .format("%Y-%m-%d")
                .to_string()
                .serialize(serializer),
            Self::Date64(x) => ArrowDate64Type::to_naive_date(*x)
                .format("%Y-%m-%d")
                .to_string()
                .serialize(serializer),

            Self::Time32Second(..)
            | Self::Time32Millisecond(..)
            | Self::Time64Microsecond(..)
            | Self::Time64Nanosecond(..)
            | Self::IntervalYearMonth(..)
            | Self::IntervalDayTime(..)
            | Self::IntervalMonthDayNano(..)
            | Self::Fn(..) => {
                panic!("Unsupported: serializing value: {:?}", self)
            }
        }
    }
}

impl Serialize for &dyn Record {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // NOTE: In some ways, this is more like a struct than a map (in serde lingo),
        // but structs' fields have to be known at (Rust) compile time.
        let schema = self.schema();
        let mut map = serializer.serialize_map(Some(schema.len()))?;
        for (i, field) in schema.iter().enumerate() {
            map.serialize_entry(&field.name, self.column(i))?;
        }
        map.end()
    }
}
