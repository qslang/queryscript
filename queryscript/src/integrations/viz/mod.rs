use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::ops::ControlFlow;
use std::sync::{Arc, RwLock};

use crate::compile::builtin_types::AGG_NAMES;
use crate::types::AtomicType;
use crate::{
    ast::Ident,
    compile::schema::{self, TypedExpr},
    runtime::error::Result,
    types::Type,
};

pub fn normalize(
    vega_spec_val: serde_json::Value,
    expr: &TypedExpr<Arc<RwLock<Type>>>,
) -> Option<serde_json::Value> {
    let mut vega_spec: VegaLiteSingleSpec = serde_json::from_value(vega_spec_val.clone()).unwrap();
    let mut fields = summarize_fields(expr).ok()?;

    // Set the default visualization to be a bar if there are at least two fields
    match (&vega_spec.mark, fields.len()) {
        (None, l) if l >= 2 => {
            vega_spec.mark = Some(VegaLiteMark::Bar);
        }
        (None, _) => return None,
        _ => {}
    };

    if let None = vega_spec.encoding {
        vega_spec.encoding = Some(BTreeMap::new());
    }

    let encoding = vega_spec.encoding.as_mut().unwrap();
    match vega_spec.mark.as_ref().unwrap() {
        VegaLiteMark::Arc => {
            if encoding.get("theta").is_none() {
                if let Some(field) = first_matching_measure(&mut fields) {
                    encoding.insert("theta".to_string(), field.to_field(true));
                }
            }

            if encoding.get("color").is_none() {
                if let Some(field) = first_matching_dimension(&mut fields) {
                    encoding.insert("color".to_string(), field.to_field(false));
                }
            }
        }
        VegaLiteMark::Bar | VegaLiteMark::Line => {
            if encoding.get("y").is_none() {
                if let Some(field) = first_matching_measure(&mut fields) {
                    encoding.insert("y".to_string(), field.to_field(true));
                }
            }

            if encoding.get("x").is_none() {
                if let Some(field) = first_matching_dimension(&mut fields) {
                    encoding.insert("x".to_string(), field.to_field(false));
                }
            }
        }
    }

    return Some(json!(vega_spec));
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum VegaLiteMark {
    Arc,
    Bar,
    Line,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum VegaLiteType {
    Quantitative,
    Ordinal,
    Temporal,
    Nominal,
}

// https://vega.github.io/vega-lite/docs/spec.html#single
#[derive(Serialize, Deserialize, Debug)]
pub struct VegaLiteSingleSpec {
    #[serde(rename = "$schema", skip_serializing_if = "Option::is_none")]
    schema: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    background: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    padding: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    autosize: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    config: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    usermeta: Option<Value>,

    // Properties for any specifications
    #[serde(skip_serializing_if = "Option::is_none")]
    title: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transform: Option<Value>,

    // Properties for any single view specifications
    #[serde(skip_serializing_if = "Option::is_none")]
    width: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    height: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    mark: Option<VegaLiteMark>,

    #[serde(skip_serializing_if = "Option::is_none")]
    encoding: Option<BTreeMap<String, Value>>,
}

#[derive(Debug, Clone)]
struct FieldSummary {
    name: Ident,
    type_: Type,
    is_agg: bool,
}

impl FieldSummary {
    pub fn vega_type(&self, measure: bool) -> VegaLiteType {
        use AtomicType::*;
        match (&self.type_, measure) {
            (Type::Atom(t), measure) => match t {
                Null | Boolean | Interval(..) | Binary | FixedSizeBinary(..) | LargeBinary
                | Utf8 | LargeUtf8 => VegaLiteType::Nominal,

                Int8
                | Int16
                | Int32
                | Int64
                | UInt8
                | UInt16
                | UInt32
                | UInt64
                | Decimal256(_, 0)
                | Decimal128(_, 0) => {
                    if measure {
                        VegaLiteType::Quantitative
                    } else {
                        VegaLiteType::Ordinal
                    }
                }
                Float16 | Float32 | Float64 | Decimal256(_, _) | Decimal128(_, _) => {
                    VegaLiteType::Quantitative
                }
                Timestamp(..) | Date32 | Date64 | Time32(..) | Time64(..) => VegaLiteType::Temporal,
            },
            _ => VegaLiteType::Nominal,
        }
    }

    pub fn to_field(&self, measure: bool) -> serde_json::Value {
        let mut field = serde_json::Map::new();
        field.insert("field".to_string(), json!(self.name));
        field.insert("type".to_string(), json!(self.vega_type(measure)));
        serde_json::Value::Object(field)
    }
}

fn first_matching_measure(fields: &mut Vec<FieldSummary>) -> Option<FieldSummary> {
    let mut first_number = None;
    for i in 0..fields.len() {
        if matches!(fields[i].vega_type(true), VegaLiteType::Nominal) {
            continue;
        }
        if let None = first_number {
            first_number = Some(i);
        }
        if fields[i].is_agg {
            return Some(fields.swap_remove(i));
        }
    }
    first_number.map(|i| fields.swap_remove(i))
}

fn first_matching_dimension(fields: &mut Vec<FieldSummary>) -> Option<FieldSummary> {
    for i in 0..fields.len() {
        if !fields[i].is_agg {
            return Some(fields.swap_remove(i));
        }
    }
    if fields.len() > 0 {
        Some(fields.swap_remove(0))
    } else {
        None
    }
}

fn summarize_fields(expr: &TypedExpr<Arc<RwLock<Type>>>) -> Result<Vec<FieldSummary>> {
    let sql_snippet = match expr.expr.as_ref() {
        schema::Expr::SQL(e, _) => e.clone(),
        schema::Expr::Materialize(schema::MaterializeExpr { expr, .. }) => {
            return summarize_fields(expr)
        }
        _ => return Ok(vec![]),
    };

    // If it's an expression (rather than a table or query), then we know it's not going to be
    // a tabular result
    if let schema::SQLBody::Expr(_) = &sql_snippet.body {
        return Ok(vec![]);
    }

    let type_ = expr.type_.read()?;
    let field_types = match &*type_ {
        Type::List(t) => match t.as_ref() {
            Type::Record(fields) => fields,
            _ => return Ok(vec![]),
        },
        _ => return Ok(vec![]),
    };

    let query = sql_snippet.body.as_query()?;

    let projected_fields = match query.body.as_ref() {
        sqlparser::ast::SetExpr::Select(s) => &s.projection,
        _ => return Ok(vec![]),
    };

    let mut ret = Vec::new();
    for (ft, pf) in field_types.iter().zip(projected_fields.iter()) {
        let mut agg_finder = FoundAggVisitor(false);
        sqlparser::ast::Visit::visit(pf, &mut agg_finder);

        ret.push(FieldSummary {
            name: ft.name.clone(),
            type_: ft.type_.clone(),
            is_agg: agg_finder.0,
        });
    }

    Ok(ret)
}

struct FoundAggVisitor(bool);

impl sqlparser::ast::Visitor for FoundAggVisitor {
    type Break = ();

    fn pre_visit_expr(&mut self, expr: &sqlparser::ast::Expr) -> ControlFlow<Self::Break> {
        match expr {
            sqlparser::ast::Expr::Function(sqlparser::ast::Function { name, .. })
                if name.0.len() == 1 && AGG_NAMES.contains(&Ident::from(name.0[0].get())) =>
            {
                self.0 = true;
                return ControlFlow::Break(());
            }
            _ => ControlFlow::Continue(()),
        }
    }
}
