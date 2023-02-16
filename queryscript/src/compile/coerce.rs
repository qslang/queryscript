use arrow::datatypes::DataType as ArrowDataType;
use sqlparser::ast::BinaryOperator;

use crate::types::{AtomicType, TimeUnit, Type, DECIMAL128_MAX_PRECISION, DECIMAL128_MAX_SCALE};

macro_rules! must {
    ($val: expr) => {
        match $val {
            Some(a) => a,
            None => return None,
        }
    };
}

macro_rules! must_atomic {
    ($type_: expr) => {
        match $type_ {
            Type::Atom(a) => a,
            _ => return None,
        }
    };
}

#[derive(Clone, Debug)]
pub enum CoerceOp {
    Binary(BinaryOperator),
    #[allow(unused)]
    Like,
    #[allow(unused)]
    IsDistinctFrom,
}

impl From<BinaryOperator> for CoerceOp {
    fn from(op: BinaryOperator) -> CoerceOp {
        CoerceOp::Binary(op)
    }
}

// NOTES:
// - The following coercion logic is originally copied from the datafusion SQL planner:
//      datafusion/expr/src/type_cercion/binary.rs:coerce_types.
// - We strive to have "standard SQL" semantics, which we borrow from DataFusion, but do not intend to have
//   exactly the same semantics as DataFusion.
// - The code has been changed to use our operators (from sqlparser).
// - While we normally operate over MTypes in the compiler, coercion has to be done over "resolved" types,
//   not references to potentially unknown types. So these functions operate over runtime types.
// -------------

/// Coercion rules for all binary operators. Returns the output type
/// of applying `op` to an argument of `lhs_type` and `rhs_type`.
///
/// Returns None if no suitable type can be found.
///
/// TODO this function is trying to serve two purposes at once; it
/// determines the result type of the binary operation and also
/// determines how the inputs can be coerced but this results in
/// inconsistencies in some cases (particular around date + interval)
/// when the input argument types do not match the output argument
/// types
///
/// Tracking issue is https://github.com/apache/arrow-datafusion/issues/3419
pub fn coerce_types(lhs_type: &Type, op: &CoerceOp, rhs_type: &Type) -> Option<Type> {
    // This result MUST be compatible with `binary_coerce`
    match op {
        CoerceOp::Binary(op) => match op {
            BinaryOperator::BitwiseAnd
            | BinaryOperator::Xor
            | BinaryOperator::BitwiseOr
            | BinaryOperator::PGBitwiseXor
            | BinaryOperator::BitwiseXor
            | BinaryOperator::PGBitwiseShiftRight
            | BinaryOperator::PGBitwiseShiftLeft => bitwise_coercion(lhs_type, rhs_type),
            BinaryOperator::And | BinaryOperator::Or => match (lhs_type, rhs_type) {
                // logical binary boolean operators can only be evaluated in bools
                (Type::Atom(AtomicType::Boolean), Type::Atom(AtomicType::Boolean)) => {
                    Some(Type::Atom(AtomicType::Boolean))
                }
                _ => None,
            },
            // logical comparison operators have their own rules, and always return a boolean
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Lt
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
            | BinaryOperator::LtEq => comparison_coercion(lhs_type, rhs_type),
            // date +/- interval returns date
            BinaryOperator::Plus | BinaryOperator::Minus
                if (*lhs_type == Type::Atom(AtomicType::Date32)
                    || *lhs_type == Type::Atom(AtomicType::Date64)
                    || matches!(lhs_type, Type::Atom(AtomicType::Timestamp(_, _)))) =>
            {
                match rhs_type {
                    Type::Atom(AtomicType::Interval(_)) => Some(lhs_type.clone()),
                    _ => None,
                }
            }
            // for math expressions, the final value of the coercion is also the return type
            // because coercion favours higher information types
            BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Modulo
            | BinaryOperator::Divide
            | BinaryOperator::Multiply => mathematics_numerical_coercion(op, lhs_type, rhs_type),
            BinaryOperator::PGRegexMatch
            | BinaryOperator::PGRegexIMatch
            | BinaryOperator::PGRegexNotMatch
            | BinaryOperator::PGRegexNotIMatch => string_coercion(lhs_type, rhs_type),
            // "||" operator has its own rules, and always return a string type
            BinaryOperator::StringConcat => string_concat_coercion(lhs_type, rhs_type),

            // These are unsupported binary operators
            BinaryOperator::Spaceship | BinaryOperator::PGCustomBinaryOperator(_) => None,
        },
        CoerceOp::Like => {
            // "like" operators operate on strings and always return a boolean
            like_coercion(lhs_type, rhs_type)
        }
        CoerceOp::IsDistinctFrom => eq_coercion(lhs_type, rhs_type),
    }
}

/// Returns the output type of applying bitwise operations such as
/// `&`, `|`, or `xor`to arguments of `lhs_type` and `rhs_type`.
fn bitwise_coercion(left_type: &Type, right_type: &Type) -> Option<Type> {
    let (left_type, right_type) = must!(both_numeric_or_null_and_numeric(left_type, right_type));

    if left_type == right_type {
        return Some(Type::Atom(left_type.clone()));
    }

    // TODO support other data type
    use AtomicType::*;
    Some(Type::Atom(match (left_type, right_type) {
        (Int64, _) | (_, Int64) => Int64,
        (Int32, _) | (_, Int32) => Int32,
        (Int16, _) | (_, Int16) => Int16,
        (Int8, _) | (_, Int8) => Int8,
        (UInt64, _) | (_, UInt64) => UInt64,
        (UInt32, _) | (_, UInt32) => UInt32,
        (UInt16, _) | (_, UInt16) => UInt16,
        (UInt8, _) | (_, UInt8) => UInt8,
        _ => return None,
    }))
}

/// Determine if a Type is numeric or not
pub fn is_numeric(dt: &AtomicType) -> bool {
    use AtomicType::*;
    is_signed_numeric(dt) || matches!(dt, UInt8 | UInt16 | UInt32 | UInt64)
}

/// Determine if a Type is signed numeric or not
pub fn is_signed_numeric(dt: &AtomicType) -> bool {
    use AtomicType::*;
    matches!(
        dt,
        Int8 | Int16
            | Int32
            | Int64
            | Float16
            | Float32
            | Float64
            | Decimal128(_, _)
            | Decimal256(_, _)
    )
}

/// Returns the output type of applying comparison operations such as
/// `eq`, `not eq`, `lt`, `lteq`, `gt`, and `gteq` to arguments
/// of `lhs_type` and `rhs_type`.
pub fn comparison_coercion(lhs_type: &Type, rhs_type: &Type) -> Option<Type> {
    if lhs_type == rhs_type {
        // same type => equality is possible
        return Some(lhs_type.clone());
    }
    comparison_binary_numeric_coercion(lhs_type, rhs_type)
        // .or_else(|| dictionary_coercion(lhs_type, rhs_type, true)) // TODO: QueryScript does not support dictionaries
        .or_else(|| temporal_coercion(lhs_type, rhs_type))
        .or_else(|| string_coercion(lhs_type, rhs_type))
        .or_else(|| null_coercion(lhs_type, rhs_type))
        .or_else(|| string_numeric_coercion(lhs_type, rhs_type))
}

/// Returns the output type of applying numeric operations such as `=`
/// to arguments `lhs_type` and `rhs_type` if one is numeric and one
/// is `Utf8`/`LargeUtf8`.
fn string_numeric_coercion(lhs_type: &Type, rhs_type: &Type) -> Option<Type> {
    use AtomicType::*;
    Some(Type::Atom(
        match (must_atomic!(lhs_type), must_atomic!(rhs_type)) {
            (Utf8, rhs_type) if is_numeric(rhs_type) => Utf8,
            (LargeUtf8, rhs_type) if is_numeric(rhs_type) => LargeUtf8,
            (lhs_type, Utf8) if is_numeric(&lhs_type) => Utf8,
            (lhs_type, LargeUtf8) if is_numeric(&lhs_type) => LargeUtf8,
            _ => return None,
        },
    ))
}

/// Returns the output type of applying numeric operations such as `=`
/// to arguments `lhs_type` and `rhs_type` if both are numeric
fn comparison_binary_numeric_coercion(lhs_type: &Type, rhs_type: &Type) -> Option<Type> {
    let (lhs_type, rhs_type) = (must_atomic!(lhs_type), must_atomic!(rhs_type));
    if !is_numeric(lhs_type) || !is_numeric(rhs_type) {
        return None;
    };

    // same type => all good
    if lhs_type == rhs_type {
        return Some(Type::Atom(lhs_type.clone()));
    }

    use AtomicType::*;
    // these are ordered from most informative to least informative so
    // that the coercion does not lose information via truncation
    Some(Type::Atom(match (lhs_type, rhs_type) {
        // support decimal data type for comparison operation
        (d1 @ Decimal128(_, _), d2 @ Decimal128(_, _)) => must!(get_wider_decimal_type(d1, d2)),
        (Decimal128(_, _), _) => must!(get_comparison_common_decimal_type(lhs_type, rhs_type)),
        (_, Decimal128(_, _)) => must!(get_comparison_common_decimal_type(rhs_type, lhs_type)),
        (Float64, _) | (_, Float64) => Float64,
        (_, Float32) | (Float32, _) => Float32,
        (Int64, _) | (_, Int64) => Int64,
        (Int32, _) | (_, Int32) => Int32,
        (Int16, _) | (_, Int16) => Int16,
        (Int8, _) | (_, Int8) => Int8,
        (UInt64, _) | (_, UInt64) => UInt64,
        (UInt32, _) | (_, UInt32) => UInt32,
        (UInt16, _) | (_, UInt16) => UInt16,
        (UInt8, _) | (_, UInt8) => UInt8,
        _ => return None,
    }))
}

/// Returns the output type of applying numeric operations such as `=`
/// to a decimal type `decimal_type` and `other_type`
fn get_comparison_common_decimal_type(
    decimal_type: &AtomicType,
    other_type: &AtomicType,
) -> Option<AtomicType> {
    use AtomicType::*;
    let other_decimal_type = &match other_type {
        // This conversion rule is from spark
        // https://github.com/apache/spark/blob/1c81ad20296d34f137238dadd67cc6ae405944eb/sql/catalyst/src/main/scala/org/apache/spark/sql/types/DecimalType.scala#L127
        Int8 => Decimal128(3, 0),
        Int16 => Decimal128(5, 0),
        Int32 => Decimal128(10, 0),
        Int64 => Decimal128(20, 0),
        Float32 => Decimal128(14, 7),
        Float64 => Decimal128(30, 15),
        _ => {
            return None;
        }
    };
    match (decimal_type, &other_decimal_type) {
        (d1 @ Decimal128(_, _), d2 @ Decimal128(_, _)) => get_wider_decimal_type(d1, d2),
        _ => None,
    }
}

/// Returns a `Type::Decimal128` that can store any value from either
/// `lhs_decimal_type` and `rhs_decimal_type`
///
/// The result decimal type is `(max(s1, s2) + max(p1-s1, p2-s2), max(s1, s2))`.
fn get_wider_decimal_type(
    lhs_decimal_type: &AtomicType,
    rhs_type: &AtomicType,
) -> Option<AtomicType> {
    match (lhs_decimal_type, rhs_type) {
        (AtomicType::Decimal128(p1, s1), AtomicType::Decimal128(p2, s2)) => {
            // max(s1, s2) + max(p1-s1, p2-s2), max(s1, s2)
            let s = *s1.max(s2);
            let range = (*p1 as i16 - *s1 as i16).max(*p2 as i16 - *s2 as i16);
            Some(create_decimal_type((range + s as i16) as u8, s))
        }
        (_, _) => None,
    }
}

fn create_decimal_type(precision: u8, scale: i8) -> AtomicType {
    AtomicType::Decimal128(
        DECIMAL128_MAX_PRECISION.min(precision),
        DECIMAL128_MAX_SCALE.min(scale),
    )
}

/// Convert the numeric data type to the decimal data type.
/// Now, we just support the signed integer type and floating-point type.
fn coerce_numeric_type_to_decimal(numeric_type: &AtomicType) -> Option<AtomicType> {
    use AtomicType::*;
    match numeric_type {
        Int8 => Some(Decimal128(3, 0)),
        Int16 => Some(Decimal128(5, 0)),
        Int32 => Some(Decimal128(10, 0)),
        Int64 => Some(Decimal128(20, 0)),
        // TODO if we convert the floating-point data to the decimal type, it maybe overflow.
        Float32 => Some(Decimal128(14, 7)),
        Float64 => Some(Decimal128(30, 15)),
        _ => None,
    }
}

/// Returns the output type of applying mathematics operations such as
/// `+` to arguments of `lhs_type` and `rhs_type`.
fn mathematics_numerical_coercion(
    mathematics_op: &BinaryOperator,
    lhs_type: &Type,
    rhs_type: &Type,
) -> Option<Type> {
    let (lhs_type, rhs_type) = must!(both_numeric_or_null_and_numeric(lhs_type, rhs_type));

    // same type => all good
    // TODO: remove this
    // bug: https://github.com/apache/arrow-datafusion/issues/3387
    if lhs_type == rhs_type {
        return Some(Type::Atom(lhs_type.clone()));
    }

    use AtomicType::*;
    // these are ordered from most informative to least informative so
    // that the coercion removes the least amount of information
    Some(Type::Atom(match (lhs_type, rhs_type) {
        (Decimal128(_, _), Decimal128(_, _)) => {
            must!(coercion_decimal_mathematics_type(
                mathematics_op,
                lhs_type,
                rhs_type
            ))
        }
        (Null, dec_type @ Decimal128(_, _)) | (dec_type @ Decimal128(_, _), Null) => {
            dec_type.clone()
        }
        (Decimal128(_, _), Float32 | Float64) => Float64,
        (Float32 | Float64, Decimal128(_, _)) => Float64,
        (Decimal128(_, _), _) => {
            let right_decimal_type = must!(coerce_numeric_type_to_decimal(rhs_type));
            must!(coercion_decimal_mathematics_type(
                mathematics_op,
                lhs_type,
                &right_decimal_type
            ))
        }
        (_, Decimal128(_, _)) => {
            let left_decimal_type = must!(coerce_numeric_type_to_decimal(lhs_type));
            must!(coercion_decimal_mathematics_type(
                mathematics_op,
                &left_decimal_type,
                rhs_type
            ))
        }
        (Float64, _) | (_, Float64) => Float64,
        (_, Float32) | (Float32, _) => Float32,
        (Int64, _) | (_, Int64) => Int64,
        (Int32, _) | (_, Int32) => Int32,
        (Int16, _) | (_, Int16) => Int16,
        (Int8, _) | (_, Int8) => Int8,
        (UInt64, _) | (_, UInt64) => UInt64,
        (UInt32, _) | (_, UInt32) => UInt32,
        (UInt16, _) | (_, UInt16) => UInt16,
        (UInt8, _) | (_, UInt8) => UInt8,
        _ => return None,
    }))
}

fn coercion_decimal_mathematics_type(
    mathematics_op: &BinaryOperator,
    left_decimal_type: &AtomicType,
    right_decimal_type: &AtomicType,
) -> Option<AtomicType> {
    use AtomicType::*;
    match (left_decimal_type, right_decimal_type) {
        // The coercion rule from spark
        // https://github.com/apache/spark/blob/c20af535803a7250fef047c2bf0fe30be242369d/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/DecimalPrecision.scala#L35
        (Decimal128(p1, s1), Decimal128(p2, s2)) => {
            match mathematics_op {
                BinaryOperator::Plus | BinaryOperator::Minus => {
                    // max(s1, s2)
                    let result_scale = *s1.max(s2);
                    // max(s1, s2) + max(p1-s1, p2-s2) + 1
                    let result_precision = result_scale as i16
                        + (*p1 as i16 - *s1 as i16).max(*p2 as i16 - *s2 as i16)
                        + 1;
                    Some(create_decimal_type(result_precision as u8, result_scale))
                }
                BinaryOperator::Multiply => {
                    // s1 + s2
                    let result_scale = *s1 + *s2;
                    // p1 + p2 + 1
                    let result_precision = *p1 + *p2 + 1;
                    Some(create_decimal_type(result_precision, result_scale))
                }
                BinaryOperator::Divide => {
                    // max(6, s1 + p2 + 1)
                    let result_scale = 6.max(*s1 as i16 + *p2 as i16 + 1);
                    // p1 - s1 + s2 + max(6, s1 + p2 + 1)
                    let result_precision =
                        result_scale as i16 + *p1 as i16 - *s1 as i16 + *s2 as i16;
                    Some(create_decimal_type(
                        result_precision as u8,
                        result_scale as i8,
                    ))
                }
                BinaryOperator::Modulo => {
                    // max(s1, s2)
                    let result_scale = *s1.max(s2);
                    // min(p1-s1, p2-s2) + max(s1, s2)
                    let result_precision = result_scale as i16
                        + (*p1 as i16 - *s1 as i16).min(*p2 as i16 - *s2 as i16);
                    Some(create_decimal_type(
                        result_precision as u8,
                        result_scale as i8,
                    ))
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// Determine if at least of one of lhs and rhs is numeric, and the other must be NULL or numeric
fn both_numeric_or_null_and_numeric<'a, 'b>(
    lhs_type: &'a Type,
    rhs_type: &'b Type,
) -> Option<(&'a AtomicType, &'b AtomicType)> {
    let (lhs_type, rhs_type) = match (lhs_type, rhs_type) {
        (Type::Atom(lhs_atom), Type::Atom(rhs_atom)) => (lhs_atom, rhs_atom),
        _ => return None,
    };

    let valid = match (lhs_type, rhs_type) {
        (_, AtomicType::Null) => is_numeric(lhs_type),
        (AtomicType::Null, _) => is_numeric(rhs_type),
        _ => is_numeric(lhs_type) && is_numeric(rhs_type),
    };

    if valid {
        Some((lhs_type, rhs_type))
    } else {
        None
    }
}

/// Coercion rules for string concat.
/// This is a union of string coercion rules and specified rules:
/// 1. At lease one side of lhs and rhs should be string type (Utf8 / LargeUtf8)
/// 2. Data type of the other side should be able to cast to string type
fn string_concat_coercion(lhs_type: &Type, rhs_type: &Type) -> Option<Type> {
    use AtomicType::*;
    string_coercion(lhs_type, rhs_type).or(Some(Type::Atom(
        match (must_atomic!(lhs_type), must_atomic!(rhs_type)) {
            (Utf8, from_type) | (from_type, Utf8) => {
                must!(string_concat_internal_coercion(from_type, &Utf8))
            }
            (LargeUtf8, from_type) | (from_type, LargeUtf8) => {
                must!(string_concat_internal_coercion(from_type, &LargeUtf8))
            }
            _ => return None,
        },
    )))
}

fn string_concat_internal_coercion(
    from_type: &AtomicType,
    to_type: &AtomicType,
) -> Option<AtomicType> {
    if can_cast_types(&Type::Atom(from_type.clone()), &Type::Atom(to_type.clone())) {
        Some(to_type.to_owned())
    } else {
        None
    }
}

pub fn can_cast_types(from_type: &Type, to_type: &Type) -> bool {
    let arrow_from: Option<ArrowDataType> = from_type.try_into().ok();
    let arrow_to: Option<ArrowDataType> = to_type.try_into().ok();
    match (arrow_from, arrow_to) {
        (Some(arrow_from), Some(arrow_to)) => {
            arrow::compute::can_cast_types(&arrow_from, &arrow_to)
        }
        _ => false,
    }
}

/// Coercion rules for Strings: the type that both lhs and rhs can be
/// casted to for the purpose of a string computation
fn string_coercion(lhs_type: &Type, rhs_type: &Type) -> Option<Type> {
    use AtomicType::*;
    match (must_atomic!(lhs_type), must_atomic!(rhs_type)) {
        (Utf8, Utf8) => Some(Utf8),
        (LargeUtf8, Utf8) => Some(LargeUtf8),
        (Utf8, LargeUtf8) => Some(LargeUtf8),
        (LargeUtf8, LargeUtf8) => Some(LargeUtf8),
        _ => None,
    }
    .map(Type::Atom)
}

/// coercion rules for like operations.
/// This is a union of string coercion rules and dictionary coercion rules
fn like_coercion(lhs_type: &Type, rhs_type: &Type) -> Option<Type> {
    string_coercion(lhs_type, rhs_type)
        // .or_else(|| dictionary_coercion(lhs_type, rhs_type, false)) // TODO: Support dictionary types
        .or_else(|| null_coercion(lhs_type, rhs_type))
}

/// Coercion rules for Temporal columns: the type that both lhs and rhs can be
/// casted to for the purpose of a date computation
fn temporal_coercion(lhs_type: &Type, rhs_type: &Type) -> Option<Type> {
    use AtomicType::*;
    match (must_atomic!(lhs_type), must_atomic!(rhs_type)) {
        (Date64, Date32) => Some(Date64),
        (Date32, Date64) => Some(Date64),
        (Utf8, Date32) => Some(Date32),
        (Date32, Utf8) => Some(Date32),
        (Utf8, Date64) => Some(Date64),
        (Date64, Utf8) => Some(Date64),
        (Timestamp(lhs_unit, lhs_tz), Timestamp(rhs_unit, rhs_tz)) => {
            let tz = match (lhs_tz, rhs_tz) {
                // can't cast across timezones
                (Some(lhs_tz), Some(rhs_tz)) => {
                    if lhs_tz != rhs_tz {
                        return None;
                    } else {
                        Some(lhs_tz.clone())
                    }
                }
                (Some(lhs_tz), None) => Some(lhs_tz.clone()),
                (None, Some(rhs_tz)) => Some(rhs_tz.clone()),
                (None, None) => None,
            };

            let unit = match (lhs_unit, rhs_unit) {
                (TimeUnit::Second, TimeUnit::Millisecond) => TimeUnit::Second,
                (TimeUnit::Second, TimeUnit::Microsecond) => TimeUnit::Second,
                (TimeUnit::Second, TimeUnit::Nanosecond) => TimeUnit::Second,
                (TimeUnit::Millisecond, TimeUnit::Second) => TimeUnit::Second,
                (TimeUnit::Millisecond, TimeUnit::Microsecond) => TimeUnit::Millisecond,
                (TimeUnit::Millisecond, TimeUnit::Nanosecond) => TimeUnit::Millisecond,
                (TimeUnit::Microsecond, TimeUnit::Second) => TimeUnit::Second,
                (TimeUnit::Microsecond, TimeUnit::Millisecond) => TimeUnit::Millisecond,
                (TimeUnit::Microsecond, TimeUnit::Nanosecond) => TimeUnit::Microsecond,
                (TimeUnit::Nanosecond, TimeUnit::Second) => TimeUnit::Second,
                (TimeUnit::Nanosecond, TimeUnit::Millisecond) => TimeUnit::Millisecond,
                (TimeUnit::Nanosecond, TimeUnit::Microsecond) => TimeUnit::Microsecond,
                (l, r) => {
                    assert_eq!(l, r);
                    l.clone()
                }
            };

            Some(Timestamp(unit, tz))
        }
        _ => None,
    }
    .map(Type::Atom)
}

/// Coercion rule for numerical types: The type that both lhs and rhs
/// can be casted to for numerical calculation, while maintaining
/// maximum precision
fn numerical_coercion(lhs_type: &Type, rhs_type: &Type) -> Option<Type> {
    let (lhs_type, rhs_type) = (must_atomic!(lhs_type), must_atomic!(rhs_type));

    // error on any non-numeric type
    if !is_numeric(lhs_type) || !is_numeric(rhs_type) {
        return None;
    };

    if lhs_type == rhs_type {
        // same type => all good
        return Some(Type::Atom(lhs_type.clone()));
    }

    use AtomicType::*;
    // these are ordered from most informative to least informative so
    // that the coercion removes the least amount of information
    match (lhs_type, rhs_type) {
        (Float64, _) | (_, Float64) => Some(Float64),
        (_, Float32) | (Float32, _) => Some(Float32),
        (Int64, _) | (_, Int64) => Some(Int64),
        (Int32, _) | (_, Int32) => Some(Int32),
        (Int16, _) | (_, Int16) => Some(Int16),
        (Int8, _) | (_, Int8) => Some(Int8),
        (UInt64, _) | (_, UInt64) => Some(UInt64),
        (UInt32, _) | (_, UInt32) => Some(UInt32),
        (UInt16, _) | (_, UInt16) => Some(UInt16),
        (UInt8, _) | (_, UInt8) => Some(UInt8),
        _ => None,
    }
    .map(Type::Atom)
}

/// coercion rules for equality operations. This is a superset of all numerical coercion rules.
fn eq_coercion(lhs_type: &Type, rhs_type: &Type) -> Option<Type> {
    if lhs_type == rhs_type {
        // same type => equality is possible
        return Some(lhs_type.clone());
    }
    numerical_coercion(lhs_type, rhs_type)
        // .or_else(|| dictionary_coercion(lhs_type, rhs_type, true)) // TODO: Support dictionaries
        .or_else(|| temporal_coercion(lhs_type, rhs_type))
        .or_else(|| null_coercion(lhs_type, rhs_type))
}

/// coercion rules from NULL type. Since NULL can be casted to most of types in arrow,
/// either lhs or rhs is NULL, if NULL can be casted to type of the other side, the coercion is valid.
fn null_coercion(lhs_type: &Type, rhs_type: &Type) -> Option<Type> {
    match (lhs_type, rhs_type) {
        (Type::Atom(AtomicType::Null), other_type) | (other_type, Type::Atom(AtomicType::Null)) => {
            if can_cast_types(&Type::Atom(AtomicType::Null), other_type) {
                Some(other_type.clone())
            } else {
                None
            }
        }
        _ => None,
    }
}

mod tests {
    use super::*;
    use crate::compile::error::{CompileError, Result};

    // For some reason the rust linter doesn't think we use these imports or the function
    // definitions below (even though the tests require them).
    #[allow(unused_imports)]
    use {crate::types::AtomicType as DataType, sqlparser::ast::BinaryOperator as Operator};
    #[allow(unused)]
    fn coerce_types_r(lhs_type: &Type, op: &CoerceOp, rhs_type: &Type) -> Result<Type> {
        match coerce_types(lhs_type, op, rhs_type) {
            Some(x) => Ok(x),
            None => Err(CompileError::internal(
                crate::ast::SourceLocation::Unknown,
                "failed to coerce",
            )),
        }
    }

    #[test]
    fn test_coercion_error() {
        let result_type = coerce_types(
            &Type::Atom(AtomicType::Float32),
            &CoerceOp::Binary(BinaryOperator::Plus),
            &Type::Atom(AtomicType::Utf8),
        );

        assert!(matches!(result_type, None));
    }

    #[test]
    fn test_decimal_binary_comparison_coercion() -> Result<()> {
        let input_decimal = DataType::Decimal128(20, 3);
        let input_types = [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal128(38, 10),
            DataType::Decimal128(20, 8),
            DataType::Null,
        ];
        let result_types = [
            DataType::Decimal128(20, 3),
            DataType::Decimal128(20, 3),
            DataType::Decimal128(20, 3),
            DataType::Decimal128(23, 3),
            DataType::Decimal128(24, 7),
            DataType::Decimal128(32, 15),
            DataType::Decimal128(38, 10),
            DataType::Decimal128(25, 8),
            DataType::Decimal128(20, 3),
        ];
        let comparison_op_types = [
            Operator::NotEq,
            Operator::Eq,
            Operator::Gt,
            Operator::GtEq,
            Operator::Lt,
            Operator::LtEq,
        ];
        for (i, input_type) in input_types.iter().enumerate() {
            let expect_type = &result_types[i];
            for op in &comparison_op_types {
                let result_type = coerce_types_r(
                    &Type::Atom(input_decimal.clone()),
                    &CoerceOp::Binary(op.clone()),
                    &Type::Atom(input_type.clone()),
                )?;
                assert_eq!(&Type::Atom(expect_type.clone()), &result_type);
            }
        }
        // negative test
        let result_type = coerce_types_r(
            &Type::Atom(input_decimal),
            &CoerceOp::Binary(Operator::Eq),
            &Type::Atom(DataType::Boolean),
        );
        assert!(result_type.is_err());
        Ok(())
    }

    #[test]
    fn test_decimal_mathematics_op_type() {
        assert_eq!(
            coerce_numeric_type_to_decimal(&DataType::Int8).unwrap(),
            DataType::Decimal128(3, 0)
        );
        assert_eq!(
            coerce_numeric_type_to_decimal(&DataType::Int16).unwrap(),
            DataType::Decimal128(5, 0)
        );
        assert_eq!(
            coerce_numeric_type_to_decimal(&DataType::Int32).unwrap(),
            DataType::Decimal128(10, 0)
        );
        assert_eq!(
            coerce_numeric_type_to_decimal(&DataType::Int64).unwrap(),
            DataType::Decimal128(20, 0)
        );
        assert_eq!(
            coerce_numeric_type_to_decimal(&DataType::Float32).unwrap(),
            DataType::Decimal128(14, 7)
        );
        assert_eq!(
            coerce_numeric_type_to_decimal(&DataType::Float64).unwrap(),
            DataType::Decimal128(30, 15)
        );

        let op = Operator::Plus;
        let left_decimal_type = DataType::Decimal128(10, 3);
        let right_decimal_type = DataType::Decimal128(20, 4);
        let result =
            coercion_decimal_mathematics_type(&op, &left_decimal_type, &right_decimal_type);
        assert_eq!(DataType::Decimal128(21, 4), result.unwrap());
        let op = Operator::Minus;
        let result =
            coercion_decimal_mathematics_type(&op, &left_decimal_type, &right_decimal_type);
        assert_eq!(DataType::Decimal128(21, 4), result.unwrap());
        let op = Operator::Multiply;
        let result =
            coercion_decimal_mathematics_type(&op, &left_decimal_type, &right_decimal_type);
        assert_eq!(DataType::Decimal128(31, 7), result.unwrap());
        let op = Operator::Divide;
        let result =
            coercion_decimal_mathematics_type(&op, &left_decimal_type, &right_decimal_type);
        assert_eq!(DataType::Decimal128(35, 24), result.unwrap());
        let op = Operator::Modulo;
        let result =
            coercion_decimal_mathematics_type(&op, &left_decimal_type, &right_decimal_type);
        assert_eq!(DataType::Decimal128(11, 4), result.unwrap());
    }

    /*
    #[test]
    fn test_dictionary_type_coercion() {
        use DataType::*;

        let lhs_type = Dictionary(Box::new(Int8), Box::new(Int32));
        let rhs_type = Dictionary(Box::new(Int8), Box::new(Int16));
        assert_eq!(dictionary_coercion(&lhs_type, &rhs_type, true), Some(Int32));
        assert_eq!(
            dictionary_coercion(&lhs_type, &rhs_type, false),
            Some(Int32)
        );

        // Since we can coerce values of Int16 to Utf8 can support this
        let lhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
        let rhs_type = Dictionary(Box::new(Int8), Box::new(Int16));
        assert_eq!(dictionary_coercion(&lhs_type, &rhs_type, true), Some(Utf8));

        // Can not coerce values of Binary to int,  cannot support this
        let lhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
        let rhs_type = Dictionary(Box::new(Int8), Box::new(Binary));
        assert_eq!(dictionary_coercion(&lhs_type, &rhs_type, true), None);

        let lhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
        let rhs_type = Utf8;
        assert_eq!(dictionary_coercion(&lhs_type, &rhs_type, false), Some(Utf8));
        assert_eq!(
            dictionary_coercion(&lhs_type, &rhs_type, true),
            Some(lhs_type.clone())
        );

        let lhs_type = Utf8;
        let rhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
        assert_eq!(dictionary_coercion(&lhs_type, &rhs_type, false), Some(Utf8));
        assert_eq!(
            dictionary_coercion(&lhs_type, &rhs_type, true),
            Some(rhs_type.clone())
        );
    }
    */

    #[allow(unused)]
    macro_rules! test_coercion_binary_rule {
        ($A_TYPE:expr, $B_TYPE:expr, $OP:expr, $C_TYPE:expr) => {{
            let result = coerce_types_r(&Type::Atom($A_TYPE), &$OP.into(), &Type::Atom($B_TYPE))?;
            assert_eq!(result, Type::Atom($C_TYPE));
        }};
    }

    #[test]
    fn test_type_coercion() -> Result<()> {
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Utf8,
            CoerceOp::Like,
            DataType::Utf8
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Date32,
            CoerceOp::Binary(Operator::Eq),
            DataType::Date32
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Date64,
            CoerceOp::Binary(Operator::Lt),
            DataType::Date64
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Utf8,
            CoerceOp::Binary(Operator::PGRegexMatch),
            DataType::Utf8
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Utf8,
            CoerceOp::Binary(Operator::PGRegexNotMatch),
            DataType::Utf8
        );
        test_coercion_binary_rule!(
            DataType::Utf8,
            DataType::Utf8,
            CoerceOp::Binary(Operator::PGRegexNotIMatch),
            DataType::Utf8
        );
        test_coercion_binary_rule!(
            DataType::Int16,
            DataType::Int64,
            CoerceOp::Binary(Operator::BitwiseAnd),
            DataType::Int64
        );
        Ok(())
    }

    #[test]
    fn test_type_coercion_arithmetic() -> Result<()> {
        // integer
        test_coercion_binary_rule!(
            DataType::Int32,
            DataType::UInt32,
            Operator::Plus,
            DataType::Int32
        );
        test_coercion_binary_rule!(
            DataType::Int32,
            DataType::UInt16,
            Operator::Minus,
            DataType::Int32
        );
        test_coercion_binary_rule!(
            DataType::Int8,
            DataType::Int64,
            Operator::Multiply,
            DataType::Int64
        );
        // float
        test_coercion_binary_rule!(
            DataType::Float32,
            DataType::Int32,
            Operator::Plus,
            DataType::Float32
        );
        test_coercion_binary_rule!(
            DataType::Float32,
            DataType::Float64,
            Operator::Multiply,
            DataType::Float64
        );
        // decimal
        // bug: https://github.com/apache/arrow-datafusion/issues/3387 will be fixed in the next pr
        // test_coercion_binary_rule!(
        //     DataType::Decimal128(10, 2),
        //     DataType::Decimal128(10, 2),
        //     Operator::Plus,
        //     DataType::Decimal128(11, 2)
        // );
        test_coercion_binary_rule!(
            DataType::Int32,
            DataType::Decimal128(10, 2),
            Operator::Plus,
            DataType::Decimal128(13, 2)
        );
        test_coercion_binary_rule!(
            DataType::Int32,
            DataType::Decimal128(10, 2),
            Operator::Minus,
            DataType::Decimal128(13, 2)
        );
        test_coercion_binary_rule!(
            DataType::Int32,
            DataType::Decimal128(10, 2),
            Operator::Multiply,
            DataType::Decimal128(21, 2)
        );
        test_coercion_binary_rule!(
            DataType::Int32,
            DataType::Decimal128(10, 2),
            Operator::Divide,
            DataType::Decimal128(23, 11)
        );
        test_coercion_binary_rule!(
            DataType::Int32,
            DataType::Decimal128(10, 2),
            Operator::Modulo,
            DataType::Decimal128(10, 2)
        );
        // TODO add other data type
        Ok(())
    }

    #[test]
    fn test_type_coercion_compare() -> Result<()> {
        // boolean
        test_coercion_binary_rule!(
            DataType::Boolean,
            DataType::Boolean,
            Operator::Eq,
            DataType::Boolean
        );
        // float
        test_coercion_binary_rule!(
            DataType::Float32,
            DataType::Int64,
            Operator::Eq,
            DataType::Float32
        );
        test_coercion_binary_rule!(
            DataType::Float32,
            DataType::Float64,
            Operator::GtEq,
            DataType::Float64
        );
        // signed integer
        test_coercion_binary_rule!(
            DataType::Int8,
            DataType::Int32,
            Operator::LtEq,
            DataType::Int32
        );
        test_coercion_binary_rule!(
            DataType::Int64,
            DataType::Int32,
            Operator::LtEq,
            DataType::Int64
        );
        // unsigned integer
        test_coercion_binary_rule!(
            DataType::UInt32,
            DataType::UInt8,
            Operator::Gt,
            DataType::UInt32
        );
        // numeric/decimal
        test_coercion_binary_rule!(
            DataType::Int64,
            DataType::Decimal128(10, 0),
            Operator::Eq,
            DataType::Decimal128(20, 0)
        );
        test_coercion_binary_rule!(
            DataType::Int64,
            DataType::Decimal128(10, 2),
            Operator::Lt,
            DataType::Decimal128(22, 2)
        );
        test_coercion_binary_rule!(
            DataType::Float64,
            DataType::Decimal128(10, 3),
            Operator::Gt,
            DataType::Decimal128(30, 15)
        );
        test_coercion_binary_rule!(
            DataType::Int64,
            DataType::Decimal128(10, 0),
            Operator::Eq,
            DataType::Decimal128(20, 0)
        );
        test_coercion_binary_rule!(
            DataType::Decimal128(14, 2),
            DataType::Decimal128(10, 3),
            Operator::GtEq,
            DataType::Decimal128(15, 3)
        );

        // TODO add other data type
        Ok(())
    }

    #[test]
    fn test_type_coercion_logical_op() -> Result<()> {
        test_coercion_binary_rule!(
            DataType::Boolean,
            DataType::Boolean,
            Operator::And,
            DataType::Boolean
        );

        test_coercion_binary_rule!(
            DataType::Boolean,
            DataType::Boolean,
            Operator::Or,
            DataType::Boolean
        );
        Ok(())
    }
}
