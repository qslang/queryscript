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

            // The remaining types do not have a special (deserializable) representation, so we
            // represent them with their string format. Note that we're not ever actually parsing
            // values ourselves -- we rely on arrow/datafusion for that.
            _ => format!("{}", self).serialize(serializer),
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

impl Serialize for crate::ast::Ident {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.as_str().serialize(serializer)
    }
}
