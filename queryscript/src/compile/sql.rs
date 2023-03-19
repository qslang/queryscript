use lazy_static::lazy_static;
use snafu::prelude::*;
use sqlparser::ast::WildcardAdditionalOptions;
use sqlparser::{ast as sqlast, ast::DataType as ParserDataType};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use crate::compile::casync;
use crate::compile::coerce::CoerceOp;
use crate::compile::compile::{
    compile_fn_body, lookup_path, resolve_global_atom, typecheck_path, Compiler, FnContext,
    SymbolKind,
};
use crate::compile::error::*;
use crate::compile::generics;
use crate::compile::generics::{as_generic, ConnectionType, ExternalType, GenericConstructor};
use crate::compile::inference::*;
use crate::compile::inline::*;
use crate::compile::schema::*;
use crate::compile::scope::{AvailableReferences, SQLScope};
use crate::compile::traverse::VisitSQL;
use crate::runtime::RuntimeError;
use crate::types::Field;
use crate::types::{number::parse_numeric_type, AtomicType, IntervalUnit, Type};
use crate::{
    ast,
    ast::{SourceLocation, ToPath, ToSqlIdent},
};

use super::compile::ExternalTypeRank;
use super::fmtstring::StringFormatter;

const QS_NAMESPACE: &str = "__qs";

#[derive(Clone, Debug)]
pub struct TypedSQL {
    pub type_: CRef<MType>,
    pub sql: Ref<SQL<CRef<MType>>>,
}

// Here, "C" means constrained.  In general, any structs prefixed with C indicate that there are
// structures that may be unknown within them.
//
#[derive(Clone, Debug)]
pub struct CTypedNameAndSQL {
    pub name: Located<Ident>,
    pub type_: CRef<MType>,
    pub sql: CRef<SQL<CRef<MType>>>,
}

#[derive(Clone, Debug)]
pub struct NameAndSQL {
    pub name: Located<Ident>,
    pub type_: CRef<MType>,
    pub sql: SQL<CRef<MType>>,
}

#[derive(Clone, Debug)]
pub struct NamedSQLSnippet<T> {
    pub name: Located<Ident>,
    pub body: T,
}

// Here, "C" means constrained.  In general, any structs prefixed with C indicate that there are
// structures that may be unknown within them.
//
#[derive(Clone, Debug)]
pub struct CTypedSQL {
    pub type_: CRef<MType>,
    pub sql: CRef<SQL<CRef<MType>>>,
}

#[derive(Clone, Debug)]
pub struct CTypedSQLSnippet<SQLAst>
where
    SQLAst: Clone + fmt::Debug + Send + Sync,
{
    pub type_: CRef<MType>,
    pub sql: CRef<SQLSnippet<CRef<MType>, SQLAst>>,
}

impl<Ty: Clone + fmt::Debug + Send + Sync, SQLAst: Clone + fmt::Debug + Send + Sync> Constrainable
    for SQLSnippet<Ty, SQLAst>
{
}
impl Constrainable for TypedSQL {}
impl Constrainable for NameAndSQL {}
impl Constrainable for CTypedNameAndSQL {}
impl Constrainable for CTypedSQL {}
impl<T: Clone + fmt::Debug + Send + Sync> Constrainable for CTypedSQLSnippet<T> {}
impl Constrainable for CTypedExpr {}

pub fn get_rowtype(compiler: Compiler, relation: CRef<MType>) -> Result<CRef<MType>> {
    Ok(compiler.clone().async_cref(async move {
        let r = &relation;
        let reltype = r.await?;
        let locked = reltype.read()?;
        match &*locked {
            MType::List(inner) => Ok(inner.get().clone()),
            MType::Generic(generic) => Ok(match generic.get_rowtype(compiler)? {
                Some(rowtype) => rowtype,
                None => relation.clone(),
            }),
            _ => Ok(relation.clone()),
        }
    })?)
}

pub fn param_ident(value: String) -> sqlast::Located<sqlast::Ident> {
    sqlast::Ident::new(value)
}

pub fn select_from(
    projection: Vec<sqlast::SelectItem>,
    from: Vec<sqlast::TableWithJoins>,
) -> sqlast::Query {
    sqlast::Query {
        with: None,
        body: Box::new(sqlast::SetExpr::Select(Box::new(sqlast::Select {
            distinct: false,
            top: None,
            projection,
            into: None,
            from,
            lateral_views: Vec::new(),
            selection: None,
            group_by: Vec::new(),
            cluster_by: Vec::new(),
            distribute_by: Vec::new(),
            sort_by: Vec::new(),
            having: None,
            qualify: None,
        }))),
        order_by: Vec::new(),
        limit: None,
        offset: None,
        fetch: None,
        locks: Vec::new(),
    }
}

pub fn select_no_from(
    expr: sqlast::Expr,
    alias: Option<sqlast::Located<sqlast::Ident>>,
) -> sqlast::Query {
    select_from(
        vec![match alias {
            Some(alias) => sqlast::SelectItem::ExprWithAlias { expr, alias },
            None => sqlast::SelectItem::UnnamedExpr(expr),
        }],
        Vec::new(),
    )
}

pub trait IntoTableFactor {
    fn to_table_factor(self) -> sqlast::TableFactor;
}

impl IntoTableFactor for sqlast::TableFactor {
    fn to_table_factor(self) -> sqlast::TableFactor {
        self
    }
}

impl IntoTableFactor for sqlast::Located<sqlast::Ident> {
    fn to_table_factor(self) -> sqlast::TableFactor {
        sqlast::TableFactor::Table {
            name: sqlast::ObjectName(vec![self]),
            alias: None,
            args: None,
            with_hints: Vec::new(),
        }
    }
}

impl IntoTableFactor for &Ident {
    fn to_table_factor(self) -> sqlast::TableFactor {
        sqlast::Located::new(Into::<sqlast::Ident>::into(self), None).to_table_factor()
    }
}

pub fn select_star_from<T: IntoTableFactor>(relation: T) -> sqlast::Query {
    select_from(
        vec![sqlast::SelectItem::Wildcard(WildcardAdditionalOptions {
            opt_exclude: None,
            opt_except: None,
            opt_rename: None,
            opt_replace: None,
        })],
        vec![sqlast::TableWithJoins {
            relation: relation.to_table_factor(),
            joins: Vec::new(),
        }],
    )
}

pub fn select_limit_0(mut query: sqlast::Query) -> sqlast::Query {
    query.limit = Some(sqlast::Expr::Value(sqlast::Value::Number(
        "0".to_string(),
        false,
    )));
    query
}

pub fn create_view_as(name: sqlast::ObjectName, query: sqlast::Query) -> sqlast::Statement {
    sqlast::Statement::CreateView {
        name,
        query: Box::new(query),
        or_replace: true,

        columns: Vec::new(),
        materialized: false,
        with_options: Vec::new(),
        cluster_by: Vec::new(),
    }
}

pub fn create_table_as(
    name: sqlast::ObjectName,
    query: sqlast::Query,
    temporary: bool,
) -> sqlast::Statement {
    sqlast::Statement::CreateTable {
        name,
        query: Some(Box::new(query)),
        or_replace: true,

        temporary,
        external: false,
        global: None,
        if_not_exists: false,
        transient: false,
        columns: Vec::new(),
        constraints: Vec::new(),
        hive_distribution: sqlast::HiveDistributionStyle::NONE,
        hive_formats: None,
        table_properties: Vec::new(),
        with_options: Vec::new(),
        file_format: None,
        location: None,
        without_rowid: false,
        like: None,
        clone: None,
        engine: None,
        default_charset: None,
        collation: None,
        on_commit: None,
        on_cluster: None,
        order_by: None,
    }
}

pub fn create_table(
    name: sqlast::ObjectName,
    fields: &Vec<Field>,
    temporary: bool,
) -> Result<sqlast::Statement> {
    let columns = fields
        .iter()
        .map(|f| {
            Ok(sqlast::ColumnDef {
                name: sqlast::Located::new((&f.name).into(), None),
                data_type: (&f.type_).try_into().context(TypesystemSnafu {
                    loc: SourceLocation::Unknown,
                })?,
                collation: None,
                options: if f.nullable {
                    vec![sqlast::ColumnOptionDef {
                        name: None,
                        option: sqlast::ColumnOption::Null,
                    }]
                } else {
                    vec![]
                },
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(sqlast::Statement::CreateTable {
        name,
        query: None,
        or_replace: true,
        temporary,
        external: false,
        global: None,
        if_not_exists: false,
        transient: false,
        columns,
        constraints: Vec::new(),
        hive_distribution: sqlast::HiveDistributionStyle::NONE,
        hive_formats: None,
        table_properties: Vec::new(),
        with_options: Vec::new(),
        file_format: None,
        location: None,
        without_rowid: false,
        like: None,
        clone: None,
        engine: None,
        default_charset: None,
        collation: None,
        on_commit: None,
        on_cluster: None,
        order_by: None,
    })
}

pub fn with_table_alias(
    table: &sqlast::TableFactor,
    alias: Option<sqlast::TableAlias>,
) -> sqlast::TableFactor {
    let mut table = table.clone();
    let alias_ref: &mut Option<sqlast::TableAlias> = match &mut table {
        sqlast::TableFactor::Table { alias, .. } => alias,
        sqlast::TableFactor::Derived { alias, .. } => alias,
        sqlast::TableFactor::TableFunction { alias, .. } => alias,
        sqlast::TableFactor::UNNEST { alias, .. } => alias,
        sqlast::TableFactor::NestedJoin { alias, .. } => alias,
    };
    *alias_ref = alias;
    table
}

pub fn compile_sqlreference(
    compiler: Compiler,
    schema: Ref<Schema>,
    scope: Ref<SQLScope>,
    sqlpath: &Vec<sqlast::Located<sqlast::Ident>>,
) -> Result<CTypedExpr> {
    let file = schema.read()?.file.clone();
    let path = sqlpath
        .iter()
        .map(|i| {
            Ident::from_formatted_sqlident(
                &compiler,
                &schema,
                &SourceLocation::from_file_range(file.clone(), i.location().clone()),
                i.get(),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    // We need to roundtrip the sql path because we might have formatted it in the path expression above
    let sqlpath: Vec<sqlast::Located<sqlast::Ident>> =
        path.iter().map(|i| i.to_sqlident()).collect();
    let loc = path_location(&path);
    match sqlpath.len() {
        0 => {
            return Err(CompileError::internal(
                loc.clone(),
                "Reference must have at least one part",
            ));
        }
        1 => {
            let name = sqlpath[0].clone();
            let name_ident: Ident = path[0].get().clone();

            if let Some((relation_type, relation_loc)) = scope.read()?.get_relation(&name_ident)? {
                let type_ = get_rowtype(compiler.clone(), relation_type)?;
                let expr = mkcref(Expr::native_sql(Arc::new(SQL {
                    names: CSQLNames::from_unbound(&sqlpath),
                    body: SQLBody::Expr(sqlast::Expr::CompoundIdentifier(sqlpath.clone())),
                })));
                compiler.run_on_symbol::<ExprEntry>(
                    path[0].clone(),
                    SymbolKind::Field,
                    mkcref(type_.clone().into()),
                    relation_loc,
                    None,
                )?;
                return Ok(CTypedExpr { type_, expr });
            } else {
                let available =
                    scope
                        .read()?
                        .get_available_references(compiler.clone(), &loc, None)?;

                let tse = available.then({
                    move |available: Ref<AvailableReferences>| {
                        if let Some(fm) = available.read()?.get(&name_ident) {
                            if let Some(type_) = fm.type_.clone() {
                                compiler.run_on_symbol::<ExprEntry>(
                                    path[0].clone(),
                                    SymbolKind::Field,
                                    mkcref(type_.clone().into()),
                                    fm.field.location().clone(),
                                    None,
                                )?;
                                Ok(mkcref(TypedExpr {
                                    type_: type_,
                                    expr: Arc::new(Expr::native_sql(Arc::new(fm.sql.clone()))),
                                }))
                            } else {
                                Err(CompileError::duplicate_entry(vec![Ident::from_sqlident(
                                    loc.clone(),
                                    name.get().clone(),
                                )]))
                            }
                        } else {
                            // If it doesn't match any names of fields in SQL relations,
                            // compile it as a normal reference.
                            //
                            let te = compile_reference(compiler.clone(), schema.clone(), &path)?;
                            Ok(mkcref(TypedExpr {
                                type_: te.type_,
                                expr: te.expr,
                            }))
                        }
                    }
                })?;
                let type_ =
                    tse.then(|tse: Ref<TypedExpr<CRef<MType>>>| Ok(tse.read()?.type_.clone()))?;
                let expr = tse.then(|tse: Ref<TypedExpr<CRef<MType>>>| {
                    Ok(mkcref(tse.read()?.expr.as_ref().clone()))
                })?;

                return Ok(CTypedExpr { type_, expr });
            }
        }
        2 => {
            let relation_name = sqlpath[0].get().into();

            // If the relation can't be found in the scope, just fall through
            //
            if let Some((relation_type, relation_loc)) =
                scope.read()?.get_relation(&relation_name)?
            {
                let rowtype = get_rowtype(compiler.clone(), relation_type)?;
                let type_ = typecheck_path(rowtype, vec![path[1].clone()].as_slice())?;
                let expr = mkcref(Expr::native_sql(Arc::new(SQL {
                    names: CSQLNames::from_unbound(&sqlpath),
                    body: SQLBody::Expr(sqlast::Expr::CompoundIdentifier(sqlpath.clone())),
                })));
                compiler.run_on_symbol::<ExprEntry>(
                    path[1].clone(),
                    SymbolKind::Value,
                    mkcref(type_.clone().into()),
                    relation_loc,
                    None,
                )?;
                return Ok(CTypedExpr { type_, expr });
            }
        }
        // References longer than two parts must be pointing outside the query, so just fall
        // through
        //
        _ => {}
    }

    let te = compile_reference(compiler.clone(), schema.clone(), &path)?;
    Ok(CTypedExpr {
        type_: te.type_.clone(),
        expr: mkcref(te.expr.as_ref().clone()),
    })
}

pub fn compile_reference(
    compiler: Compiler,
    schema: Ref<Schema>,
    path: &ast::Path,
) -> Result<TypedExpr<CRef<MType>>> {
    let (importer, decl, remainder) = lookup_path::<ExprEntry>(
        compiler.clone(),
        Importer::Schema(schema.clone()),
        &path,
        true, /* import_global */
        true, /* resolve_last */
    )?;

    let decl = match decl {
        Some(decl) => decl,
        None => match importer {
            Importer::Schema(..) => return Err(CompileError::no_such_entry(path.clone())),
            Importer::Connection(schema) => {
                let (url, loc) = {
                    let s = schema.read()?;
                    (s.url.clone(), s.location.clone())
                };
                Decl {
                    public: true,
                    extern_: false,
                    is_arg: false,
                    name: Located::new(url.db_name(), loc),
                    value: STypedExpr {
                        type_: SType::new_mono(mkcref(MType::Generic(Located::new(
                            ConnectionType::new(&SourceLocation::Unknown, Vec::new())?,
                            SourceLocation::Unknown,
                        )))),
                        expr: mkcref(Expr::Connection(url.clone())),
                    },
                }
            }
        },
    };
    let remainder_cpy = remainder.clone();

    let expr = &decl.value;
    let type_ = expr
        .type_
        .then(|t: Ref<SType>| Ok(t.read()?.instantiate()?))?;
    typecheck_path(type_.clone(), remainder_cpy.as_slice())?;

    let top_level_ref = TypedExpr {
        type_: type_.clone(),
        expr: Arc::new(Expr::SchemaEntry(expr.clone())),
    };

    let (stype, r) = match remainder.len() {
        0 => (expr.type_.clone(), top_level_ref),
        _ => {
            // Turn the top level reference into a SQL placeholder, and return
            // a path accessing it
            let (placeholder_name, placeholder) =
                intern_nonsql_placeholder(compiler.clone(), "param_ref", &top_level_ref)?;
            let mut full_name = vec![placeholder_name.clone()];
            full_name.extend(remainder.clone().into_iter().map(|n| n.to_sqlident()));

            let expr = Arc::new(Expr::native_sql(Arc::new(SQL {
                names: placeholder.names.clone(),
                body: SQLBody::Expr(sqlast::Expr::CompoundIdentifier(full_name)),
            })));

            (mkcref(type_.clone().into()), TypedExpr { type_, expr })
        }
    };

    if let Some(ident) = path.last() {
        let kind = if decl.is_arg {
            SymbolKind::Argument
        } else {
            SymbolKind::Value
        };
        compiler.run_on_symbol(
            ident.clone(),
            kind,
            stype.clone(),
            decl.name.location().clone(),
            Some(decl.clone()),
        )?;
    }

    Ok(r)
}

pub fn intern_placeholder(
    compiler: Compiler,
    kind: &str,
    expr: &TypedExpr<CRef<MType>>,
) -> Result<Arc<SQL<CRef<MType>>>> {
    match &*expr.expr {
        Expr::SQL(sql, url) => {
            if url.is_some() {
                return Err(CompileError::internal(
                    SourceLocation::Unknown,
                    "Cannot intern a placeholder for a remote SQL expression",
                ));
            }
            Ok(sql.clone())
        }
        _ => {
            let (_, e) = intern_nonsql_placeholder(compiler.clone(), kind, expr)?;
            Ok(e)
        }
    }
}

pub fn intern_nonsql_placeholder(
    compiler: Compiler,
    kind: &str,
    expr: &TypedExpr<CRef<MType>>,
) -> Result<(sqlast::Located<sqlast::Ident>, Arc<SQL<CRef<MType>>>)> {
    match &*expr.expr {
        Expr::SQL(..) => Err(CompileError::internal(
            SourceLocation::Unknown,
            "Cannot call intern_nonsql_placeholder on a SQL expression",
        )),
        _ => {
            let placeholder_name = "@".to_string() + compiler.next_placeholder(kind)?.as_str();

            Ok((
                param_ident(placeholder_name.clone()),
                Arc::new(SQL {
                    names: SQLNames {
                        params: Params::from([(placeholder_name.clone().into(), expr.clone())]),
                        unbound: BTreeSet::new(),
                    },
                    body: SQLBody::Expr(sqlast::Expr::Identifier(param_ident(
                        placeholder_name.clone(),
                    ))),
                }),
            ))
        }
    }
}

pub fn intern_cref_placeholder(
    compiler: Compiler,
    kind: String,
    te: CTypedExpr,
) -> Result<CTypedSQL> {
    let type_ = te.type_.clone();
    let sql = te.expr.clone().then(move |expr: Ref<Expr<CRef<MType>>>| {
        let te = te.clone();
        let sqlexpr: SQL<CRef<MType>> = intern_placeholder(
            compiler.clone(),
            kind.as_str(),
            &TypedExpr {
                type_: te.type_.clone(),
                expr: Arc::new(expr.read()?.clone()),
            },
        )?
        .as_ref()
        .clone();
        Ok(mkcref(sqlexpr))
    })?;
    Ok(CTypedSQL { type_, sql })
}

pub fn compile_sqlarg(
    compiler: Compiler,
    schema: Ref<Schema>,
    scope: Ref<SQLScope>,
    loc: &SourceLocation,
    expr: &sqlast::Expr,
) -> Result<CTypedSQL> {
    let compiled = compile_sqlexpr(compiler.clone(), schema.clone(), scope.clone(), loc, expr)?;
    intern_cref_placeholder(compiler.clone(), "param_arg".to_string(), compiled)
}

pub type CSQLNames = SQLNames<CRef<MType>>;

pub fn combine_crefs<T: 'static + Constrainable>(all: Vec<CRef<T>>) -> Result<CRef<Vec<Ref<T>>>> {
    let mut ret = mkcref(Vec::new());

    for a in all {
        ret = ret.then(move |sofar: Ref<Vec<Ref<T>>>| {
            a.then(move |a: Ref<T>| Ok(mkcref(vec![sofar.read()?.clone(), vec![a]].concat())))
        })?;
    }

    Ok(ret)
}

pub fn combine_sqlnames(all: &Vec<Ref<SQL<CRef<MType>>>>) -> Result<CSQLNames> {
    let mut ret = CSQLNames::new();
    for e in all {
        ret.extend(e.read()?.names.clone());
    }
    Ok(ret)
}

// This trait is used to compile sql expressions with their parameters, without computing
// their types. This is useful for expressions that do not directly impact a query's type,
// e.g. the OVER clause of a window function, or an ORDER BY clause.
type CSQLSnippet<T> = SQLSnippet<CRef<MType>, T>;
type CRefSnippet<T> = CRef<CWrap<CSQLSnippet<T>>>;

trait CompileSQL<Wrapper = CRefSnippet<Self>>: Clone + fmt::Debug + Send + Sync + 'static {
    fn compile_sql(
        &self,
        compiler: &Compiler,
        schema: &Ref<Schema>,
        scope: &Ref<SQLScope>,
        loc: &SourceLocation,
    ) -> Result<Wrapper>;
}

impl<T> CompileSQL for Option<T>
where
    T: CompileSQL,
{
    fn compile_sql(
        &self,
        compiler: &Compiler,
        schema: &Ref<Schema>,
        scope: &Ref<SQLScope>,
        loc: &SourceLocation,
    ) -> Result<CRefSnippet<Self>> {
        match self {
            Some(t) => {
                let c_t = t.compile_sql(compiler, schema, scope, loc)?;
                compiler.async_cref(async move {
                    let t = c_t.await?;
                    let t = cunwrap(t)?;
                    Ok(CSQLSnippet::wrap(t.names, Some(t.body)))
                })
            }
            None => Ok(CSQLSnippet::wrap(CSQLNames::new(), None)),
        }
    }
}

impl<T> CompileSQL for Box<T>
where
    T: CompileSQL,
{
    fn compile_sql(
        &self,
        compiler: &Compiler,
        schema: &Ref<Schema>,
        scope: &Ref<SQLScope>,
        loc: &SourceLocation,
    ) -> Result<CRefSnippet<Self>> {
        let c_t = self.as_ref().compile_sql(compiler, schema, scope, loc)?;
        compiler.async_cref(async move {
            let t = c_t.await?;
            let t = cunwrap(t)?;
            Ok(CSQLSnippet::wrap(t.names, Box::new(t.body)))
        })
    }
}

impl<T> CompileSQL for Vec<T>
where
    T: CompileSQL,
{
    fn compile_sql(
        &self,
        compiler: &Compiler,
        schema: &Ref<Schema>,
        scope: &Ref<SQLScope>,
        loc: &SourceLocation,
    ) -> Result<CRefSnippet<Self>> {
        let c_vec = combine_crefs(
            self.iter()
                .map(|t| t.compile_sql(compiler, schema, scope, loc))
                .collect::<Result<Vec<_>>>()?,
        )?;
        compiler.async_cref(async move {
            let c_vec = c_vec.await?;
            let mut names = CSQLNames::new();
            let mut ret = Vec::new();
            for element in c_vec.read()?.iter() {
                let element = cunwrap(element.clone())?;
                names.extend(element.names);
                ret.push(element.body);
            }

            Ok(CSQLSnippet::wrap(names, ret))
        })
    }
}

// This is not the primary implementation of compiling an expression, but is a convenience function so that we can
// run compile_sqlarg through the compile_sql() trait (e.g. to help compile an Option<Expr>).
impl CompileSQL for sqlast::Expr {
    fn compile_sql(
        &self,
        compiler: &Compiler,
        schema: &Ref<Schema>,
        scope: &Ref<SQLScope>,
        loc: &SourceLocation,
    ) -> Result<CRefSnippet<Self>> {
        let compiled = compile_sqlarg(compiler.clone(), schema.clone(), scope.clone(), loc, self)?;
        compiler.async_cref(async move {
            let compiled = compiled.sql.await?;
            let compiled = compiled.read()?;
            Ok(CSQLSnippet::wrap(
                compiled.names.clone(),
                compiled.body.as_expr()?,
            ))
        })
    }
}

impl CompileSQL for sqlast::TableFactor {
    fn compile_sql(
        &self,
        compiler: &Compiler,
        schema: &Ref<Schema>,
        scope: &Ref<SQLScope>,
        loc: &SourceLocation,
    ) -> Result<CRefSnippet<Self>> {
        let file = schema.read()?.file.clone();
        Ok(match self {
            sqlast::TableFactor::Table {
                name,
                alias,
                args,
                with_hints,
            } => {
                let loc = path_location(&name.0.to_path(file.clone()));

                if args.is_some() {
                    return Err(CompileError::unimplemented(
                        loc.clone(),
                        "Table valued functions",
                    ));
                }

                if with_hints.len() > 0 {
                    return Err(CompileError::unimplemented(loc.clone(), "WITH hints"));
                }

                // TODO: This currently assumes that table references always come from outside
                // the query, which is not actually the case.
                //
                let relation = compile_reference(
                    compiler.clone(),
                    schema.clone(),
                    &name.to_path(file.clone()),
                )?;

                let list_type = mkcref(MType::List(Located::new(
                    MType::new_unknown(format!("FROM {}", name.to_string()).as_str()),
                    loc.clone(),
                )));
                list_type.unify(&relation.type_)?;

                let name = match alias {
                    Some(a) => a.name.clone(),
                    None => name
                        .0
                        .last()
                        .ok_or_else(|| {
                            CompileError::internal(
                                loc.clone(),
                                "Table name must have at least one part",
                            )
                        })?
                        .clone(),
                };

                let mut from_names = CSQLNames::new();

                scope
                    .write()?
                    .add_reference(&name.get().into(), &loc, relation.type_.clone())?;

                let placeholder_name =
                    QS_NAMESPACE.to_string() + compiler.next_placeholder("rel")?.as_str();
                from_names
                    .params
                    .insert(placeholder_name.clone().into(), relation);

                CSQLSnippet::wrap(
                    from_names,
                    sqlast::TableFactor::Table {
                        name: sqlast::ObjectName(vec![param_ident(placeholder_name)]),
                        alias: Some(sqlast::TableAlias {
                            name: name.clone(),
                            columns: Vec::new(),
                        }),
                        args: None,
                        with_hints: Vec::new(),
                    },
                )
            }
            sqlast::TableFactor::Derived {
                lateral,
                subquery,
                alias,
            } => {
                if *lateral {
                    // This is a lateral subquery, which I haven't tested yet (because it will require
                    // forwarding the outer scope into the subquery).
                    return Err(CompileError::unimplemented(
                        loc.clone(),
                        "Lateral Subqueries",
                    ));
                }

                // NOTE: Once we thread locations through the parse tree, we should use the location here.
                let (_scope, subquery_type, subquery) = compile_sqlquery(
                    compiler.clone(),
                    schema.clone(),
                    Some(scope.clone()),
                    loc,
                    subquery,
                )?;

                let (loc, name) = match alias {
                    Some(a) => (
                        a.name
                            .location()
                            .as_ref()
                            .map(|r| SourceLocation::from_file_range(file.clone(), Some(r.clone())))
                            .unwrap_or(loc.clone()),
                        a.name.clone(),
                    ),
                    None => (
                        loc.clone(),
                        param_ident(compiler.next_placeholder("anonymous_subquery")?),
                    ),
                };

                scope
                    .write()?
                    .add_reference(&name.get().into(), &loc, subquery_type.clone())?;

                let lateral = *lateral;
                compiler.async_cref(async move {
                    let subquery_expr = cunwrap(subquery.await?)?;

                    Ok(CSQLSnippet::wrap(
                        subquery_expr.names,
                        sqlast::TableFactor::Derived {
                            lateral,
                            subquery: Box::new(subquery_expr.body),
                            alias: Some(sqlast::TableAlias {
                                name: name.clone(),
                                columns: Vec::new(),
                            }),
                        },
                    ))
                })?
            }
            sqlast::TableFactor::TableFunction { .. } => {
                return Err(CompileError::unimplemented(loc.clone(), "TABLE"))
            }
            sqlast::TableFactor::UNNEST { .. } => {
                return Err(CompileError::unimplemented(loc.clone(), "UNNEST"))
            }
            sqlast::TableFactor::NestedJoin { .. } => {
                return Err(CompileError::unimplemented(loc.clone(), "Nested JOIN"))
            }
        })
    }
}

impl CompileSQL for sqlast::JoinConstraint {
    fn compile_sql(
        &self,
        compiler: &Compiler,
        schema: &Ref<Schema>,
        scope: &Ref<SQLScope>,
        loc: &SourceLocation,
    ) -> Result<CRefSnippet<Self>> {
        use sqlast::JoinConstraint::*;
        Ok(match self {
            On(e) => {
                let sql = compile_sqlarg(compiler.clone(), schema.clone(), scope.clone(), loc, &e)?;
                sql.type_
                    .unify(&resolve_global_atom(compiler.clone(), "bool")?)?;
                compiler.async_cref(async move {
                    let sql = sql.sql.await?;
                    let sql = sql.read()?;
                    Ok(CSQLSnippet::wrap(
                        sql.names.clone(),
                        On(sql.body.as_expr()?),
                    ))
                })?
            }

            // USING is quite difficult to implement, because it also affects the type of the query itself. If a field
            // is "used" in a USING, then it's only projected from the first relation. We should probably store an exclusion
            // list on the scope, and then not include the field(s) during get_available_references(). Additionally, we will
            // need to pass the target table down here (the left side of the USING field should be whichever references is
            // available prior to excluding the field from the USING clause).
            Using(_) => return Err(CompileError::unimplemented(loc.clone(), "JOIN ... USING")),

            Natural => CSQLSnippet::wrap(CSQLNames::new(), Natural),
            None => CSQLSnippet::wrap(CSQLNames::new(), None),
        })
    }
}

impl CompileSQL for sqlast::JoinOperator {
    fn compile_sql(
        &self,
        compiler: &Compiler,
        schema: &Ref<Schema>,
        scope: &Ref<SQLScope>,
        loc: &SourceLocation,
    ) -> Result<CRefSnippet<Self>> {
        use sqlast::JoinOperator::*;
        let join_constructor = match self {
            Inner(_) => Inner,
            LeftOuter(_) => LeftOuter,
            RightOuter(_) => RightOuter,
            FullOuter(_) => FullOuter,
            o => {
                return Err(CompileError::unimplemented(
                    loc.clone(),
                    format!("{:?}", o).as_str(),
                ))
            }
        };

        Ok(match self {
            Inner(c) | LeftOuter(c) | RightOuter(c) | FullOuter(c) => {
                let constraint = c.compile_sql(compiler, schema, scope, loc)?;
                compiler.async_cref(async move {
                    let join_constraint = cunwrap(constraint.await?)?;
                    Ok(CSQLSnippet::wrap(
                        join_constraint.names,
                        join_constructor(join_constraint.body),
                    ))
                })?
            }
            _ => unreachable!(),
        })
    }
}

impl CompileSQL for sqlast::Join {
    fn compile_sql(
        &self,
        compiler: &Compiler,
        schema: &Ref<Schema>,
        scope: &Ref<SQLScope>,
        loc: &SourceLocation,
    ) -> Result<CRefSnippet<Self>> {
        let relation = self.relation.compile_sql(compiler, schema, scope, loc)?;
        let join_operator = self
            .join_operator
            .compile_sql(compiler, schema, scope, loc)?;
        compiler.async_cref(async move {
            let relation = cunwrap(relation.await?)?;
            let join_operator = cunwrap(join_operator.await?)?;

            let mut names = CSQLNames::new();
            names.extend(relation.names);
            names.extend(join_operator.names);
            Ok(CSQLSnippet::wrap(
                names,
                sqlast::Join {
                    relation: relation.body,
                    join_operator: join_operator.body,
                },
            ))
        })
    }
}

impl CompileSQL for sqlast::TableWithJoins {
    fn compile_sql(
        &self,
        compiler: &Compiler,
        schema: &Ref<Schema>,
        scope: &Ref<SQLScope>,
        loc: &SourceLocation,
    ) -> Result<CRefSnippet<Self>> {
        let c_relation = self.relation.compile_sql(compiler, schema, scope, loc)?;
        let c_joins = self.joins.compile_sql(compiler, schema, scope, loc)?;

        compiler.async_cref(async move {
            let relation = cunwrap(c_relation.await?)?;
            let joins = cunwrap(c_joins.await?)?;

            let mut table_params = CSQLNames::new();
            table_params.extend(relation.names);
            table_params.extend(joins.names);

            Ok(CSQLSnippet::wrap(
                table_params,
                sqlast::TableWithJoins {
                    relation: relation.body,
                    joins: joins.body,
                },
            ))
        })
    }
}

pub fn compile_from(
    compiler: &Compiler,
    schema: &Ref<Schema>,
    parent_scope: Option<Ref<SQLScope>>,
    loc: &SourceLocation,
    from: &Vec<sqlast::TableWithJoins>,
) -> Result<(Ref<SQLScope>, CRefSnippet<Vec<sqlast::TableWithJoins>>)> {
    let scope = SQLScope::new(parent_scope);
    let from = from.compile_sql(compiler, schema, &scope, loc)?;

    Ok((scope, from))
}

pub fn compile_order_by(
    compiler: &Compiler,
    schema: &Ref<Schema>,
    scope: &Ref<SQLScope>,
    loc: &SourceLocation,
    order_by: &Vec<sqlast::OrderByExpr>,
) -> Result<CRef<CWrap<(CSQLNames, Vec<sqlast::OrderByExpr>)>>> {
    let mut compiled_order_by = Vec::new();
    let mut compiled_opts = Vec::new();
    for ob in order_by {
        compiled_order_by.push(compile_gb_ob_expr(
            compiler.clone(),
            schema.clone(),
            scope.clone(),
            loc,
            &ob.expr,
        )?);
        compiled_opts.push((ob.asc.clone(), ob.nulls_first.clone()));
    }

    compiler.async_cref({
        async move {
            let mut resolved_order_by = Vec::new();
            let mut names = CSQLNames::new();
            for (expr, (asc, nulls_first)) in
                compiled_order_by.into_iter().zip(compiled_opts.into_iter())
            {
                let resolved_expr = expr.await?;
                let resolved_expr = resolved_expr.read()?;
                names.extend(resolved_expr.names.clone());
                resolved_order_by.push(sqlast::OrderByExpr {
                    expr: resolved_expr.body.as_expr()?,
                    asc,
                    nulls_first,
                });
            }

            Ok(cwrap((names, resolved_order_by)))
        }
    })
}

pub fn compile_gb_ob_expr(
    compiler: Compiler,
    schema: Ref<Schema>,
    scope: Ref<SQLScope>,
    loc: &SourceLocation,
    expr: &sqlast::Expr,
) -> Result<CRef<SQL<CRef<MType>>>> {
    let maybe_numeric_ref = match expr {
        sqlast::Expr::Value(sqlast::Value::Number(n, _)) => n.parse::<u64>().ok().map(|_| SQL {
            names: CSQLNames::new(),
            body: SQLBody::Expr(expr.clone()),
        }),
        _ => None,
    };

    Ok(match maybe_numeric_ref {
        Some(numeric_ref) => mkcref(numeric_ref),
        _ => {
            let compiled =
                compile_sqlarg(compiler.clone(), schema.clone(), scope.clone(), &loc, expr)?;
            compiled.sql
        }
    })
}

pub fn compile_gb_ob_set(
    compiler: &Compiler,
    schema: &Ref<Schema>,
    scope: &Ref<SQLScope>,
    loc: &SourceLocation,
    expr: &sqlast::ForEachOr<Vec<sqlast::Expr>>,
) -> Result<CRef<Vec<Ref<SQL<CRef<MType>>>>>> {
    Ok(match expr {
        sqlast::ForEachOr::ForEach(foreach) => {
            let compiled = compile_foreach(
                compiler,
                schema,
                scope,
                loc,
                foreach,
                |_, _, _, _| Ok(()),
                |compiler, schema, scope, loc, expr, _| {
                    compile_gb_ob_set(compiler, schema, scope, loc, expr)
                },
            )?;
            compiled
        }
        sqlast::ForEachOr::Item(set) => {
            let mut compiled_set = Vec::new();
            for expr in set {
                let expr =
                    compile_gb_ob_expr(compiler.clone(), schema.clone(), scope.clone(), loc, expr)?;
                compiled_set.push(expr);
            }
            combine_crefs(compiled_set)?
        }
    })
}

pub fn compile_gb_ob_multi_expr(
    compiler: &Compiler,
    schema: &Ref<Schema>,
    scope: &Ref<SQLScope>,
    loc: &SourceLocation,
    expr: &sqlast::ForEachOr<sqlast::Expr>,
) -> Result<CRef<Vec<Ref<SQL<CRef<MType>>>>>> {
    Ok(match expr {
        sqlast::ForEachOr::ForEach(foreach) => {
            let compiled = compile_foreach(
                compiler,
                schema,
                scope,
                loc,
                foreach,
                |_, _, _, _| Ok(()),
                |compiler, schema, scope, loc, expr, _| {
                    compile_gb_ob_multi_expr(compiler, schema, scope, loc, expr)
                },
            )?;
            compiled
        }
        sqlast::ForEachOr::Item(expr) => {
            let expr =
                compile_gb_ob_expr(compiler.clone(), schema.clone(), scope.clone(), loc, expr)?;
            combine_crefs(vec![expr])?
        }
    })
}

fn compile_foreach<T, O, LF, LS, C>(
    compiler: &Compiler,
    schema: &Ref<Schema>,
    scope: &Ref<SQLScope>,
    loc: &SourceLocation,
    foreach: &sqlast::ForEach<T>,
    loop_state_fn: LF,
    compile_fn: C,
) -> Result<CRef<Vec<O>>>
where
    T: Clone + fmt::Debug + Send + Sync + VisitSQL<ParamInliner> + 'static,
    O: Clone + fmt::Debug + Send + Sync + 'static + Constrainable,
    LF: Fn(&Compiler, &Ref<Schema>, &Ref<SQLScope>, &SourceLocation) -> Result<LS> + Send + 'static,
    C: Fn(
            &Compiler,
            &Ref<Schema>,
            &Ref<SQLScope>,
            &SourceLocation,
            &T,
            &mut LS,
        ) -> Result<CRef<Vec<O>>>
        + Send
        + 'static,
{
    let sqlast::ForEach { ranges, body } = foreach;
    let ranges = ranges.compile_sql(&compiler, &schema, &scope, &loc)?;

    compiler.async_cref({
        let compiler = compiler.clone();
        let schema = schema.clone();
        let scope = scope.clone();
        let loc = loc.clone();
        let body = body.clone();
        casync!({
            let SQLSnippet {
                names,
                body: ranges,
            } = cunwrap(ranges.await?)?;

            // First, inline as much as possible. This is important because we need to at least be
            // able to process the range as an array (even if the array's elements are params).
            let mut inlined_ranges = Vec::new();
            let mut inlined_names = SQLNames::new();
            for range in ranges {
                let range_expr = Expr::native_sql(Arc::new(SQL {
                    names: names.clone(),
                    body: SQLBody::Expr(range.range.as_ref().clone()),
                }));
                let inlined = inline_params(&range_expr).await?;
                match inlined {
                    Expr::SQL(expr, None) => {
                        inlined_names.extend(expr.names.clone());

                        inlined_ranges.push(sqlast::LoopRange {
                            item: range.item,
                            range: Box::new(expr.body.as_expr()?),
                        })
                    }
                    _ => {
                        return Err(CompileError::internal(
                            loc.clone(),
                            "ForEach range transformed into non-sql expression",
                        ))
                    }
                }
            }
            let ranges = inlined_ranges;
            let names = names;

            let schema = Schema::derive(schema)?;
            for (name, expr) in names.params {
                let stype = SType::new_mono(expr.type_.clone());
                schema.write()?.expr_decls.insert(
                    name.clone(),
                    Located::new(
                        Decl {
                            public: false,
                            extern_: true,
                            is_arg: true,
                            name: Located::new(name.clone(), SourceLocation::Unknown),
                            value: STypedExpr {
                                type_: stype,
                                expr: mkcref(expr.expr.as_ref().clone()),
                            },
                        },
                        loc.clone(),
                    ),
                );
            }

            let mut substituted = Vec::new();
            for substitutions in
                expand_foreach(&compiler, &schema, &scope, &loc, &ranges, BTreeMap::new())?
                    .into_iter()
            {
                let inliner = ParamInliner::new(substitutions.clone());
                let schema = Schema::derive(schema.clone())?;

                let mut loop_state = loop_state_fn(&compiler, &schema, &scope, &loc)?;

                // Although we substitute the loop items into the expressions (using param inlining), the loop
                // variable may be used in formatting aliases, which are not visited by the param inliner.
                // We save each of the loop items as a string value.
                for range in &ranges {
                    let stype = SType::new_mono(mkcref(MType::Atom(Located::new(
                        AtomicType::Utf8,
                        SourceLocation::Unknown,
                    ))));
                    let item_name: Ident = range.item.get().into();
                    let s_value = format!(
                        "{}",
                        substitutions
                            .get(&item_name)
                            .expect("missing loop variable substitution")
                            .as_expr()?
                    );

                    // Format strings look better if string literals and idents are treated the same
                    // way (i.e. without quotes)
                    let s_value = s_value
                        .strip_prefix("'")
                        .map_or(s_value.clone(), |s| s.to_string())
                        .strip_suffix("'")
                        .map_or(s_value.clone(), |s| s.to_string());

                    schema.write()?.expr_decls.insert(
                        item_name.clone(),
                        Located::new(
                            Decl {
                                public: false,
                                extern_: true,
                                is_arg: true,
                                name: Located::new(item_name.clone(), SourceLocation::Unknown),
                                value: STypedExpr {
                                    type_: stype,
                                    expr: mkcref(Expr::native_sql(Arc::new(SQL {
                                        names: SQLNames::new(),
                                        body: SQLBody::Expr(sqlast::Expr::Value(
                                            sqlast::Value::SingleQuotedString(s_value),
                                        )),
                                    }))),
                                },
                            },
                            loc.clone(),
                        ),
                    );
                }

                for expr in body.iter() {
                    let expr = expr.visit_sql(&inliner);
                    let expr =
                        compile_fn(&compiler, &schema, &scope, &loc, &expr, &mut loop_state)?;
                    substituted.push(expr);
                }
            }

            let all_substituted = combine_crefs(substituted)?;
            let all_substituted = all_substituted.await?;
            let all_substituted = all_substituted.read()?;

            let mut ret = Vec::new();
            for group in all_substituted.iter() {
                for e in group.read()?.iter() {
                    ret.push(e.clone());
                }
            }

            Ok(mkcref(ret))
        })
    })
}

#[derive(Debug, Clone)]
enum Renaming {
    Exclude,
    Name(Located<Ident>),
    Expr(Located<Ident>, CTypedSQL),
}

fn extract_renamings(
    compiler: &Compiler,
    schema: &Ref<Schema>,
    scope: &Ref<SQLScope>,
    loc: &SourceLocation,
    o: &sqlast::WildcardAdditionalOptions,
) -> Result<BTreeMap<Ident, (Option<sqlast::Range>, Renaming)>> {
    let mut ret = BTreeMap::new();

    if let Some(exclude) = &o.opt_exclude {
        use sqlast::ExcludeSelectItem::*;
        match exclude {
            Single(i) => {
                ret.insert(i.get().into(), (i.location().clone(), Renaming::Exclude));
            }
            Multiple(l) => {
                ret.extend(
                    l.iter()
                        .map(|i| (i.get().into(), (i.location().clone(), Renaming::Exclude))),
                );
            }
        }
    }

    if let Some(except) = &o.opt_except {
        ret.insert(
            except.first_element.get().into(),
            (except.first_element.location().clone(), Renaming::Exclude),
        );
        ret.extend(
            except
                .additional_elements
                .iter()
                .map(|i| (i.get().into(), (i.location().clone(), Renaming::Exclude))),
        );
    }

    if let Some(rename) = &o.opt_rename {
        use sqlast::RenameSelectItem::*;
        match rename {
            Single(i) => {
                ret.insert(
                    i.ident.get().into(),
                    (
                        i.ident.location().clone(),
                        Renaming::Name(Ident::from_located_sqlident(
                            loc.file().clone(),
                            i.alias.clone(),
                        )),
                    ),
                );
            }
            Multiple(l) => {
                ret.extend(l.iter().map(|i| {
                    (
                        i.ident.get().into(),
                        (
                            i.ident.location().clone(),
                            Renaming::Name(Ident::from_located_sqlident(
                                loc.file().clone(),
                                i.alias.clone(),
                            )),
                        ),
                    )
                }));
            }
        }
    }

    if let Some(replacements) = &o.opt_replace {
        for item in replacements.items.iter() {
            ret.insert(
                item.column_name.get().into(),
                (
                    item.column_name.location().clone(),
                    Renaming::Expr(
                        Ident::from_located_sqlident(loc.file().clone(), item.column_name.clone()),
                        compile_sqlarg(
                            compiler.clone(),
                            schema.clone(),
                            scope.clone(),
                            loc,
                            &item.expr,
                        )?,
                    ),
                ),
            );
        }
    }

    Ok(ret)
}

impl Ident {
    fn from_formatted_sqlident(
        compiler: &Compiler,
        schema: &Ref<Schema>,
        loc: &SourceLocation,
        sql_ident: &sqlast::Ident,
    ) -> Result<Located<Ident>> {
        let ident: Ident = sql_ident.into();
        if !ident.is_format() {
            return Ok(Located::new(ident, loc.clone()));
        }

        let fmt = StringFormatter::build((&ident).into(), loc.clone());
        let s = fmt.resolve_format_string(compiler, schema, loc)?;

        // Idents must be known at compile time for now
        Ok(Located::new(s.into(), loc.clone()))
    }
}

// If the expression is an identifier, then simply forward it along. In the case of a
// compound identifier (e.g. table.foo), SQL semantics are to pick the last element (i.e.
// foo) as the new name.
fn expr_to_alias(
    compiler: &Compiler,
    schema: &Ref<Schema>,
    expr: &sqlast::Expr,
    loc: &SourceLocation,
) -> Result<Located<Ident>> {
    let file = schema.read()?.file.clone();
    let (ident, loc) = match expr {
        sqlast::Expr::Identifier(i) => (
            i.get().clone(),
            SourceLocation::from_file_range(file, i.location().clone()),
        ),
        sqlast::Expr::CompoundIdentifier(c) => {
            let item = c
                .last()
                .expect("Compound identifiers should have at least one element");
            (
                item.get().clone(),
                SourceLocation::from_file_range(file, item.location().clone()),
            )
        }
        _ => (format!("{}", expr).as_str().into(), loc.clone()),
    };

    Ident::from_formatted_sqlident(compiler, schema, &loc, &ident)
}

fn compile_select_item(
    compiler: &Compiler,
    schema: &Ref<Schema>,
    scope: &Ref<SQLScope>,
    loc: &SourceLocation,
    select_item: &sqlast::SelectItem,
    projection_terms: &mut BTreeMap<Ident, CTypedSQL>,
) -> Result<CRef<Vec<CTypedNameAndSQL>>> {
    // Construct a scope that includes the projection terms we've seen so far. We clone it so that
    // each term can be compiled in parallel with the exact set of projection terms that it's
    // allowed to see
    let scope = SQLScope::deep_copy(scope)?;
    scope.write()?.projection_terms = projection_terms.clone();

    Ok(match select_item {
        sqlast::SelectItem::UnnamedExpr(expr) => {
            let compiled =
                compile_sqlarg(compiler.clone(), schema.clone(), scope.clone(), loc, expr)?;

            let name = expr_to_alias(&compiler, &schema, expr, loc)?.into_inner();
            let loc = loc.clone();
            compiler.async_cref(async move {
                let sql = compiled.sql.await?;
                let sql = sql.read()?;

                let mut ret = Vec::new();
                ret.push(CTypedNameAndSQL {
                    name: Ident::with_location(loc.clone(), name),
                    type_: compiled.type_,
                    sql: mkcref(sql.clone()),
                });

                Ok(mkcref(ret))
            })?
        }
        sqlast::SelectItem::ExprWithAlias { expr, alias } => {
            let compiled =
                compile_sqlarg(compiler.clone(), schema.clone(), scope.clone(), loc, expr)?;
            let name = Ident::from_formatted_sqlident(compiler, schema, loc, alias.get())?;

            projection_terms.insert(name.get().clone(), compiled.clone());

            mkcref(vec![CTypedNameAndSQL {
                name,
                type_: compiled.type_,
                sql: compiled.sql,
            }])
        }
        sqlast::SelectItem::Wildcard(..) | sqlast::SelectItem::QualifiedWildcard { .. } => {
            let (qualifier, mut renamings) = match select_item {
                sqlast::SelectItem::Wildcard(options) => (
                    None,
                    extract_renamings(compiler, schema, &scope, loc, options)?,
                ),
                sqlast::SelectItem::QualifiedWildcard(qualifier, options) => {
                    if qualifier.0.len() != 1 {
                        return Err(CompileError::unimplemented(
                            loc.clone(),
                            "Wildcard of lenght != 1",
                        ));
                    }

                    (
                        Some(qualifier.0[0].get().into()),
                        extract_renamings(compiler, schema, &scope, loc, options)?,
                    )
                }
                _ => unreachable!(),
            };

            let available =
                scope
                    .read()?
                    .get_available_references(compiler.clone(), loc, qualifier)?;

            let loc = loc.clone();
            compiler.async_cref(casync!({
                let available = available.await?;

                let mut ret = Vec::new();
                for (_, m) in available.read()?.current_level().unwrap().iter() {
                    let type_ = match &m.type_ {
                        Some(t) => t.clone(),
                        None => {
                            return Err(CompileError::duplicate_entry(vec![m
                                .field
                                .replace_location(loc.clone())]))
                        }
                    };

                    let compiled = match renamings.remove(&m.field) {
                        Some((_, Renaming::Exclude)) => continue,
                        Some((_, Renaming::Name(n))) => CTypedNameAndSQL {
                            name: n.clone(),
                            type_,
                            sql: mkcref(m.sql.clone()),
                        },
                        Some((_, Renaming::Expr(n, e))) => CTypedNameAndSQL {
                            name: n.clone(),
                            type_: e.type_.clone(),
                            sql: e.sql.clone(),
                        },
                        None => CTypedNameAndSQL {
                            name: m.field.clone(),
                            type_,
                            sql: mkcref(m.sql.clone()),
                        },
                    };

                    ret.push(compiled);
                }

                if let Some((name, (ident_loc, _))) = renamings.into_iter().next() {
                    return Err(CompileError::no_such_entry(vec![
                        Ident::from_located_sqlident(
                            loc.file(),
                            sqlast::Located::new((&name).into(), ident_loc),
                        ),
                    ]));
                }

                Ok(mkcref(ret))
            }))?
        }
        sqlast::SelectItem::ForEach(foreach) => compile_foreach(
            compiler,
            schema,
            &scope,
            loc,
            foreach,
            |_compiler, _schema, _scope, _loc| Ok(BTreeMap::new()),
            |compiler, schema, scope, loc, expr, mut projection_terms| {
                compile_select_item(compiler, schema, scope, loc, expr, &mut projection_terms)
            },
        )?,
    })
}

pub fn compile_select(
    compiler: Compiler,
    schema: Ref<Schema>,
    parent_scope: Option<Ref<SQLScope>>,
    loc: &SourceLocation,
    select: &sqlast::Select,
) -> Result<(
    Ref<SQLScope>,
    CRef<MType>,
    CRef<CWrap<(CSQLNames, sqlast::Select)>>,
)> {
    if select.top.is_some() {
        return Err(CompileError::unimplemented(loc.clone(), "TOP"));
    }

    if select.into.is_some() {
        return Err(CompileError::unimplemented(loc.clone(), "INTO"));
    }

    if select.lateral_views.len() > 0 {
        return Err(CompileError::unimplemented(loc.clone(), "Lateral views"));
    }

    if select.cluster_by.len() > 0 {
        return Err(CompileError::unimplemented(loc.clone(), "CLUSTER BY"));
    }

    if select.distribute_by.len() > 0 {
        return Err(CompileError::unimplemented(loc.clone(), "DISTRIBUTE BY"));
    }

    if select.sort_by.len() > 0 {
        return Err(CompileError::unimplemented(loc.clone(), "SORT BY"));
    }

    if select.having.is_some() {
        return Err(CompileError::unimplemented(loc.clone(), "HAVING"));
    }

    if select.qualify.is_some() {
        return Err(CompileError::unimplemented(loc.clone(), "QUALIFY"));
    }

    let (scope, from) = compile_from(&compiler, &schema, parent_scope.clone(), loc, &select.from)?;

    let mut projection_terms = BTreeMap::new();
    let exprs = select
        .projection
        .iter()
        .map(|p| compile_select_item(&compiler, &schema, &scope, loc, p, &mut projection_terms))
        .collect::<Result<Vec<_>>>()?;

    let projections = combine_crefs(exprs)?;

    let type_: CRef<MType> = projections.then({
        let loc = loc.clone();
        move |exprs: Ref<Vec<Ref<Vec<CTypedNameAndSQL>>>>| {
            let mut fields = Vec::new();
            for a in &*exprs.read()? {
                for b in &*a.read()? {
                    fields.push(MField {
                        name: b.name.get().clone(),
                        type_: b.type_.clone(),
                        nullable: true,
                    });
                }
            }

            Ok(mkcref(MType::List(Located::new(
                mkcref(MType::Record(Located::new(fields, loc.clone()))),
                loc.clone(),
            ))))
        }
    })?;

    let select = select.clone();
    let expr: CRef<_> = compiler.clone().async_cref({
        let scope = scope.clone();
        let loc = loc.clone();
        async move {
            let from = cunwrap(from.await?)?;

            let exprs = projections.await?;
            let mut proj_exprs = Vec::new();
            let exprs = (*exprs.read()?).clone();
            for a in exprs {
                let a = (*a.read()?).clone();
                for b in a {
                    let n = b.name.clone();
                    proj_exprs.push(NameAndSQL {
                        name: n.clone(),
                        type_: b.type_.clone(),
                        sql: b.sql.await?.read()?.clone(),
                    });
                }
            }

            let select = select.clone();

            let mut names = CSQLNames::new();
            let mut projection = Vec::new();
            for sqlexpr in &*proj_exprs {
                names.extend(sqlexpr.sql.names.clone());
                projection.push(sqlast::SelectItem::ExprWithAlias {
                    alias: sqlexpr.name.to_sqlident(),
                    expr: sqlexpr.sql.body.as_expr()?,
                });

                // Now that each projection term has been compiled (with its own copy of scope), we can add the projection
                // terms to the query's scope.
                scope.write()?.projection_terms.insert(
                    sqlexpr.name.get().clone(),
                    CTypedSQL {
                        type_: sqlexpr.type_.clone(),
                        sql: mkcref(sqlexpr.sql.clone()),
                    },
                );
            }
            names.extend(from.names);

            let selection = match &select.selection {
                Some(selection) => {
                    let compiled = compile_sqlarg(
                        compiler.clone(),
                        schema.clone(),
                        scope.clone(),
                        &loc,
                        selection,
                    )?;
                    compiled
                        .type_
                        .unify(&resolve_global_atom(compiler.clone(), "bool")?)?;
                    let sql = compiled.sql.await?.read()?.clone();
                    names.extend(sql.names.clone());
                    Some(sql.body.as_expr()?)
                }
                None => None,
            };

            let mut group_by = Vec::new();
            for gb in &select.group_by {
                let exprs = compile_gb_ob_multi_expr(&compiler, &schema, &scope, &loc, gb)?.await?;
                let exprs = exprs.read()?;
                for sql in exprs.iter() {
                    let sql = sql.read()?;
                    names.extend(sql.names.clone());
                    group_by.push(sqlast::ForEachOr::Item(sql.body.as_expr()?));
                }
            }

            let mut ret = select.clone();
            ret.from = from.body;
            ret.projection = projection;
            ret.selection = selection;
            ret.group_by = group_by;

            let names = scope
                .read()?
                .remove_bound_references(compiler.clone(), names)?;
            let names = names.await?.read()?.clone();

            Ok(cwrap((names, ret)))
        }
    })?;

    Ok((scope, type_, expr))
}

#[async_backtrace::framed]
pub async fn finish_sqlexpr(
    loc: &SourceLocation,
    expr: CRef<Expr<CRef<MType>>>,
    names: &mut CSQLNames,
) -> Result<sqlast::Expr> {
    let expr = expr.clone_inner().await?;
    Ok(match expr {
        Expr::SQL(s, url) => {
            if url.is_some() {
                return Err(CompileError::internal(
                    SourceLocation::Unknown,
                    "Cannot intern a placeholder for a remote SQL expression",
                ));
            }
            names.extend(s.names.clone());
            s.body.as_expr()?
        }
        _ => {
            return Err(CompileError::unimplemented(
                loc.clone(),
                "Non-SQL expression",
            ))
        }
    })
}

pub fn compile_sqlquery(
    compiler: Compiler,
    mut schema: Ref<Schema>,
    parent_scope: Option<Ref<SQLScope>>,
    loc: &SourceLocation,
    query: &sqlast::Query,
) -> Result<(Ref<SQLScope>, CRef<MType>, CRefSnippet<sqlast::Query>)> {
    let mut with_clause: Option<CRef<CWrap<CSQLSnippet<sqlast::With>>>> = None;
    if let Some(with) = &query.with {
        if with.recursive {
            return Err(CompileError::unimplemented(loc.clone(), "recursive CTE"));
        }

        let file = schema.read()?.file.clone();
        schema = Schema::derive(schema)?;

        let mut with_exprs = Vec::new();
        let mut aliases = Vec::new();

        for cte in with.cte_tables.iter() {
            let scope = SQLScope::new(parent_scope.clone());
            if cte.from.is_some() {
                // I don't think the parser ever generates FROM expressions in CTEs...
                return Err(CompileError::unimplemented(
                    loc.clone(),
                    "CTE with FROM clause",
                ));
            }

            let (_, type_, expr) = compile_sqlquery(
                compiler.clone(),
                schema.clone(),
                Some(scope),
                loc,
                &cte.query,
            )?;

            let name = Ident::from_located_sqlident(Some(file.clone()), cte.alias.name.clone());
            match schema.write()?.expr_decls.entry(name.get().clone()) {
                std::collections::btree_map::Entry::Occupied(_) => {
                    return Err(CompileError::duplicate_entry(vec![name.clone()]))
                }
                std::collections::btree_map::Entry::Vacant(v) => v.insert(Located::new(
                    Decl {
                        public: false,
                        extern_: false,
                        is_arg: false,
                        name: name.clone(),
                        value: STypedExpr {
                            type_: SType::new_mono(type_.clone()),
                            expr: mkcref(Expr::native_sql(Arc::new(SQL {
                                names: SQLNames::new(),
                                body: SQLBody::Table(cte.alias.name.clone().to_table_factor()),
                            }))),
                        },
                    },
                    loc.clone(),
                )),
            };

            aliases.push(cte.alias.clone());
            with_exprs.push(expr);
        }

        let with_exprs = combine_crefs(with_exprs)?;
        with_clause = Some(compiler.async_cref(casync!({
            let with_exprs = with_exprs.await?;
            let with_exprs = with_exprs.read()?;

            let with_exprs = with_exprs
                .iter()
                .map(|e| cunwrap(e.clone()))
                .collect::<Result<Vec<_>>>()?;

            let mut names = CSQLNames::new();
            let body = sqlast::With {
                recursive: false,
                cte_tables: aliases
                    .into_iter()
                    .zip(with_exprs.into_iter())
                    .map(|(alias, expr)| {
                        let SQLSnippet {
                            names: expr_names,
                            body,
                        } = expr;

                        names.extend(expr_names);

                        sqlast::Cte {
                            alias,
                            query: Box::new(body),
                            from: None,
                        }
                    })
                    .collect(),
            };

            Ok(CSQLSnippet::wrap(names, body))
        }))?);
    }

    let limit = match &query.limit {
        Some(limit) => {
            let expr = coerce_into(
                &compiler,
                loc,
                compile_sqlarg(
                    compiler.clone(),
                    schema.clone(),
                    SQLScope::empty(),
                    loc,
                    &limit,
                )?,
                resolve_global_atom(compiler.clone(), "bigint")?,
            )?;

            Some(expr)
        }
        None => None,
    };

    let offset = query
        .offset
        .compile_sql(&compiler, &schema, &SQLScope::empty(), loc)?;

    if query.fetch.is_some() {
        return Err(CompileError::unimplemented(loc.clone(), "FETCH"));
    }

    if query.locks.len() > 0 {
        return Err(CompileError::unimplemented(
            loc.clone(),
            "FOR { UPDATE | SHARE }",
        ));
    }

    let (scope, type_, set_expr) =
        compile_setexpr(&compiler, &schema, parent_scope.clone(), loc, &query.body)?;

    Ok((
        scope.clone(),
        type_,
        compiler.async_cref({
            let compiled_order_by =
                compile_order_by(&compiler, &schema, &scope, loc, &query.order_by)?;

            let compiler = compiler.clone();
            casync!({
                let mut names = SQLNames::new();

                let with = match with_clause {
                    Some(with) => {
                        let SQLSnippet {
                            names: body_names,
                            body,
                        } = cunwrap(with.await?)?;
                        names.extend(body_names);
                        Some(body)
                    }
                    None => None,
                };

                let body = cunwrap(set_expr.await?)?;
                let SQLSnippet {
                    names: body_names,
                    body,
                } = body;
                names.extend(body_names);

                let limit = match limit {
                    Some(limit) => {
                        let limit = limit.sql.await?;
                        let limit = limit.read()?.clone();
                        names.extend(limit.names);
                        Some(limit.body.as_expr()?)
                    }
                    None => None,
                };

                let SQLSnippet {
                    body: offset,
                    names: offset_names,
                } = cunwrap(offset.await?)?;
                names.extend(offset_names);

                let (ob_names, order_by) = cunwrap(compiled_order_by.await?)?;
                names.extend(ob_names);

                let names = scope
                    .read()?
                    .remove_bound_references(compiler.clone(), names)?;
                let names = names.await?.read()?.clone();

                Ok(SQLSnippet::wrap(
                    names,
                    sqlast::Query {
                        with,
                        body: Box::new(body),
                        order_by,
                        limit,
                        offset,
                        fetch: None,
                        locks: Vec::new(),
                    },
                ))
            })
        })?,
    ))
}

pub fn compile_setexpr(
    compiler: &Compiler,
    schema: &Ref<Schema>,
    parent_scope: Option<Ref<SQLScope>>,
    loc: &SourceLocation,
    set_expr: &sqlast::SetExpr,
) -> Result<(Ref<SQLScope>, CRef<MType>, CRefSnippet<sqlast::SetExpr>)> {
    match set_expr {
        sqlast::SetExpr::Select(s) => {
            let (scope, type_, select) =
                compile_select(compiler.clone(), schema.clone(), parent_scope, loc, s)?;

            Ok((
                scope,
                type_,
                compiler.async_cref(casync!({
                    let (names, body) = cunwrap(select.await?)?;
                    Ok(SQLSnippet::wrap(
                        names,
                        sqlast::SetExpr::Select(Box::new(body)),
                    ))
                }))?,
            ))
        }
        sqlast::SetExpr::Query(q) => {
            let (scope, type_, query) =
                compile_sqlquery(compiler.clone(), schema.clone(), parent_scope, loc, &q)?;

            Ok((
                scope,
                type_,
                compiler.async_cref(casync!({
                    let query = cunwrap(query.await?)?;
                    Ok(SQLSnippet::wrap(
                        query.names,
                        sqlast::SetExpr::Query(Box::new(query.body)),
                    ))
                }))?,
            ))
        }
        sqlast::SetExpr::SetOperation {
            op,
            set_quantifier,
            left,
            right,
        } => {
            let op = op.clone();
            let set_quantifier = set_quantifier.clone();
            let (left_scope, left_type, left_set_expr) =
                compile_setexpr(compiler, schema, parent_scope.clone(), loc, &left)?;
            let (_right_scope, right_type, right_set_expr) =
                compile_setexpr(compiler, schema, parent_scope.clone(), loc, &right)?;

            left_type.unify(&right_type)?;

            Ok((
                left_scope,
                left_type,
                compiler.async_cref({
                    casync!({
                        let left = cunwrap(left_set_expr.await?)?;
                        let right = cunwrap(right_set_expr.await?)?;

                        let SQLSnippet {
                            names: mut left_names,
                            body: left_body,
                        } = left;
                        let SQLSnippet {
                            names: right_names,
                            body: right_body,
                        } = right;

                        left_names.extend(right_names);
                        Ok(SQLSnippet::wrap(
                            left_names,
                            sqlast::SetExpr::SetOperation {
                                op,
                                set_quantifier,
                                left: Box::new(left_body),
                                right: Box::new(right_body),
                            },
                        ))
                    })
                })?,
            ))
        }
        sqlast::SetExpr::Values(_) => Err(CompileError::unimplemented(loc.clone(), "VALUES")),
        sqlast::SetExpr::Insert(_) => Err(CompileError::unimplemented(loc.clone(), "INSERT")),
        sqlast::SetExpr::Table(_) => Err(CompileError::unimplemented(loc.clone(), "TABLE")),
    }
}

lazy_static! {
    static ref GLOBAL_COMPILER: Compiler = Compiler::new().unwrap();
    static ref NULL_SQLEXPR: Arc<SQL<CRef<MType>>> = Arc::new(SQL {
        names: CSQLNames::new(),
        body: SQLBody::Expr(sqlast::Expr::Value(sqlast::Value::Null)),
    });
    static ref NULL: CTypedExpr = CTypedExpr {
        type_: resolve_global_atom(GLOBAL_COMPILER.clone(), "null").unwrap(),
        expr: mkcref(Expr::SQL(NULL_SQLEXPR.clone(), None)),
    };
}

fn apply_sqlcast(
    compiler: Compiler,
    arg: CTypedSQL,
    target_type: Ref<MType>,
) -> Result<CRef<SQL<CRef<MType>>>> {
    let target_type = target_type.read()?;
    let loc = target_type.location();
    let dt: ParserDataType = (&target_type
        .to_runtime_type()
        .context(RuntimeSnafu { loc: loc.clone() })?)
        .try_into()
        .context(TypesystemSnafu { loc: loc.clone() })?;

    Ok(compiler.async_cref(casync!({
        let final_type = arg.type_.await?;
        let final_expr = arg.sql.clone_inner().await?;

        let inner_expr = if let Type::Atom(AtomicType::Interval(_)) =
            final_type.read()?.to_runtime_type().context(RuntimeSnafu {
                loc: SourceLocation::Unknown,
            })? {
            // Do not apply casts to INTERVAL types
            final_expr.body.as_expr()?
        } else {
            sqlast::Expr::Cast {
                expr: Box::new(final_expr.body.as_expr()?),
                data_type: dt,
            }
        };

        Ok(mkcref(SQL {
            names: final_expr.names,
            body: SQLBody::Expr(inner_expr),
        }))
    }))?)
}

fn should_coerce_cast(my_type: &MType, target_rt: &Type) -> Result<bool, RuntimeError> {
    if matches!(my_type, MType::Name(_)) || matches!(my_type, MType::Record(_)) {
        return Ok(false);
    }

    if target_rt == &(my_type.to_runtime_type()?) {
        return Ok(false);
    }

    Ok(true)
}

fn coerce_all(
    compiler: &Compiler,
    op: &sqlparser::ast::BinaryOperator,
    loc: &SourceLocation,
    args: Vec<CTypedSQL>,
    target_type: Option<CRef<MType>>,
) -> Result<(CRef<MType>, Vec<CTypedSQL>)> {
    let mut arg_types = args.iter().map(|ts| ts.type_.clone()).collect::<Vec<_>>();
    if let Some(tt) = target_type {
        arg_types.push(tt);
    }
    let generic = Arc::new(generics::CoerceGeneric::new(
        loc.clone(),
        CoerceOp::Binary(op.clone()),
        arg_types.clone(),
    ));
    let target = MType::Generic(Located::new(generic.clone(), loc.clone())).resolve_generics()?;

    let mut ret = Vec::new();
    for arg in args.into_iter() {
        ret.push(CTypedSQL {
            type_: target
                .clone()
                .then(|t: Ref<MType>| t.read()?.resolve_generics())?,
            sql: compiler.clone().async_cref({
                let compiler = compiler.clone();
                let target = target.clone();
                casync!({
                    let target = target.await?.read()?.clone();
                    let my_type = arg.type_.clone().await?;
                    let loc = my_type.read()?.location();
                    let target_rt = target
                        .to_runtime_type()
                        .context(RuntimeSnafu { loc: loc.clone() })?;
                    let my_type = my_type.read()?;

                    Ok(
                        if should_coerce_cast(&my_type, &target_rt)
                            .context(RuntimeSnafu { loc: loc.clone() })?
                        {
                            apply_sqlcast(compiler, arg.clone(), mkref(target))?
                        } else {
                            arg.sql
                        },
                    )
                })
            })?,
        })
    }

    Ok((target, ret))
}

fn coerce_into(
    compiler: &Compiler,
    loc: &SourceLocation,
    arg: CTypedSQL,
    target_type: CRef<MType>,
) -> Result<CTypedSQL> {
    let (_, mut args) = coerce_all(
        compiler,
        &sqlast::BinaryOperator::Eq,
        loc,
        vec![arg],
        Some(target_type),
    )?;

    Ok(args.swap_remove(0))
}

pub fn unify_all<T, C, I>(mut iter: I, unknown_debug_name: &str) -> Result<CRef<T>>
where
    T: Constrainable + 'static,
    C: HasCType<T>,
    I: Iterator<Item = C>,
{
    if let Some(first) = iter.next() {
        for next in iter {
            first.type_().unify(&next.type_())?;
        }
        Ok(first.type_().clone())
    } else {
        Ok(CRef::new_unknown(unknown_debug_name))
    }
}

pub fn combine_sql_exprs<'a, I>(iter: I, names: &mut CSQLNames) -> Result<Vec<sqlast::Expr>>
where
    I: Iterator<Item = &'a Ref<SQL<CRef<MType>>>>,
{
    iter.map(|c| {
        let c = c.read()?;
        names.extend(c.names.clone());
        Ok(c.body.as_expr()?)
    })
    .collect::<Result<Vec<_>>>()
}

pub fn has_unbound_names(expr: Arc<Expr<CRef<MType>>>) -> bool {
    match expr.as_ref() {
        Expr::SQL(e, _url) => {
            !e.names.unbound.is_empty()
                || e.names
                    .params
                    .iter()
                    .any(|(_, p)| has_unbound_names(p.expr.clone()))
        }
        _ => false,
    }
}

pub fn schema_infer_load_fn(
    schema: SchemaRef,
    args: Vec<TypedNameAndExpr<CRef<MType>>>,
    inner_type: CRef<MType>,
) -> impl std::future::Future<Output = Result<()>> + Send + 'static {
    casync!({
        let mut ctx = crate::runtime::Context::new(
            schema.read()?.folder.clone(),
            crate::runtime::SQLEngineType::DuckDB,
        )
        .disable_typechecks();
        let mut runtime_args = Vec::new();
        for e in args {
            let runtime_expr = e.to_typed_expr().to_runtime_type().context(RuntimeSnafu {
                loc: SourceLocation::Unknown,
            })?;
            let eval_expr = crate::runtime::eval(&mut ctx, &runtime_expr)
                .await
                .context({
                    RuntimeSnafu {
                        loc: SourceLocation::Unknown,
                    }
                })?;
            runtime_args.push(eval_expr);
        }
        let inferred_type = crate::runtime::functions::LoadFileFn::infer(&ctx, runtime_args)
            .await
            .context(RuntimeSnafu {
                loc: SourceLocation::Unknown,
            })?;

        let inferred_mtype = mkcref(MType::from_runtime_type(&inferred_type)?);

        inner_type.unify(&inferred_mtype)?;
        Ok(())
    })
}

// This implementation is used to compile OrderByExprs that cannot refer to projection
// terms (e.g. ORDER BY 1 _cannot_ be compiled by this function). It's used by window
// functions, array_agg, etc.
impl CompileSQL for sqlast::OrderByExpr {
    fn compile_sql(
        &self,
        compiler: &Compiler,
        schema: &Ref<Schema>,
        scope: &Ref<SQLScope>,
        loc: &SourceLocation,
    ) -> Result<CRefSnippet<Self>> {
        let cexpr = self.expr.compile_sql(compiler, schema, scope, loc)?;

        let asc = self.asc.clone();
        let nulls_first = self.nulls_first.clone();
        Ok(compiler.async_cref(casync!({
            let ob = cunwrap(cexpr.await?)?;
            Ok(CSQLSnippet::wrap(
                ob.names,
                sqlast::OrderByExpr {
                    expr: ob.body,
                    asc,
                    nulls_first,
                },
            ))
        }))?)
    }
}

impl CompileSQL for sqlast::WindowFrameBound {
    fn compile_sql(
        &self,
        compiler: &Compiler,
        schema: &Ref<Schema>,
        scope: &Ref<SQLScope>,
        loc: &SourceLocation,
    ) -> Result<CRefSnippet<Self>> {
        use sqlast::WindowFrameBound::*;
        Ok(match self {
            CurrentRow => CSQLSnippet::wrap(CSQLNames::new(), CurrentRow),
            Preceding(e) | Following(e) => {
                let preceding = match self {
                    Preceding(_) => true,
                    Following(_) => false,
                    _ => unreachable!(),
                };

                let c_e = e.compile_sql(compiler, schema, scope, loc)?;
                compiler.async_cref(casync!({
                    let c_e = cunwrap(c_e.await?)?;
                    Ok(CSQLSnippet::wrap(
                        c_e.names,
                        match preceding {
                            true => Preceding(c_e.body),
                            false => Following(c_e.body),
                        },
                    ))
                }))?
            }
        })
    }
}

impl CompileSQL for sqlast::WindowFrame {
    fn compile_sql(
        &self,
        compiler: &Compiler,
        schema: &Ref<Schema>,
        scope: &Ref<SQLScope>,
        loc: &SourceLocation,
    ) -> Result<CRefSnippet<Self>> {
        let sqlast::WindowFrame {
            units,
            start_bound,
            end_bound,
        } = self;
        let c_start = start_bound.compile_sql(compiler, schema, scope, loc)?;
        let c_end = end_bound.compile_sql(compiler, schema, scope, loc)?;
        let units = units.clone();
        compiler.async_cref(async move {
            let start_bound = cunwrap(c_start.await?)?;
            let end_bound = cunwrap(c_end.await?)?;

            let mut names = CSQLNames::new();
            names.extend(start_bound.names);
            names.extend(end_bound.names);

            Ok(CSQLSnippet::wrap(
                names,
                sqlast::WindowFrame {
                    units,
                    start_bound: start_bound.body,
                    end_bound: end_bound.body,
                },
            ))
        })
    }
}

impl CompileSQL for sqlast::WindowSpec {
    fn compile_sql(
        &self,
        compiler: &Compiler,
        schema: &Ref<Schema>,
        scope: &Ref<SQLScope>,
        loc: &SourceLocation,
    ) -> Result<CRefSnippet<Self>> {
        let sqlast::WindowSpec {
            partition_by,
            order_by,
            window_frame,
        } = self;

        let c_partition_by = partition_by.compile_sql(compiler, schema, scope, loc)?;
        let c_order_by = order_by.compile_sql(compiler, schema, scope, loc)?;

        let window_frame = window_frame.compile_sql(compiler, schema, scope, loc)?;

        compiler.async_cref(async move {
            let partition_by = cunwrap(c_partition_by.await?)?;
            let order_by = cunwrap(c_order_by.await?)?;
            let window_frame = cunwrap(window_frame.await?)?;

            let mut names = SQLNames::new();
            names.extend(partition_by.names);
            names.extend(order_by.names);
            names.extend(window_frame.names);

            Ok(CSQLSnippet::wrap(
                names,
                sqlast::WindowSpec {
                    partition_by: partition_by.body,
                    order_by: order_by.body,
                    window_frame: window_frame.body,
                },
            ))
        })
    }
}

impl CompileSQL for sqlast::LoopRange {
    fn compile_sql(
        &self,
        compiler: &Compiler,
        schema: &Ref<Schema>,
        scope: &Ref<SQLScope>,
        loc: &SourceLocation,
    ) -> Result<CRefSnippet<Self>> {
        let sqlast::LoopRange { item, range } = self;

        let item = item.clone();
        let c_range = range.compile_sql(compiler, schema, scope, loc)?;

        compiler.async_cref(async move {
            let SQLSnippet { names, body } = cunwrap(c_range.await?)?;

            Ok(CSQLSnippet::wrap(
                names,
                sqlast::LoopRange { item, range: body },
            ))
        })
    }
}

impl CompileSQL for sqlast::Offset {
    fn compile_sql(
        &self,
        compiler: &Compiler,
        schema: &Ref<Schema>,
        scope: &Ref<SQLScope>,
        loc: &SourceLocation,
    ) -> Result<CRefSnippet<Self>> {
        let expr = compile_sqlarg(
            compiler.clone(),
            schema.clone(),
            scope.clone(),
            loc,
            &self.value,
        )?;

        let expr = coerce_into(
            compiler,
            loc,
            expr,
            resolve_global_atom(compiler.clone(), "bigint")?,
        )?;
        let rows = self.rows.clone();
        compiler.async_cref(casync!({
            let expr = expr.sql.await?;
            let expr = expr.read()?.clone();
            let SQLSnippet { names, body } = expr;

            Ok(CSQLSnippet::wrap(
                names,
                sqlast::Offset {
                    value: body.as_expr()?,
                    rows: rows,
                },
            ))
        }))
    }
}

#[derive(Clone, Debug)]
struct CompiledCaseArm {
    condition: CTypedSQL,
    result: CTypedSQL,
}

impl Constrainable for CompiledCaseArm {}

fn compile_case_arm_expr(
    compiler: &Compiler,
    schema: &Ref<Schema>,
    scope: &Ref<SQLScope>,
    loc: &SourceLocation,
    arm: &sqlast::ForEachOr<sqlast::CaseArm>,
) -> Result<CRef<Vec<CompiledCaseArm>>> {
    Ok(match arm {
        sqlast::ForEachOr::ForEach(foreach) => {
            let fe = compile_foreach(
                &compiler,
                &schema,
                &scope,
                loc,
                foreach,
                |_, _, _, _| Ok(()),
                |compiler, schema, scope, loc, expr, _| {
                    compile_case_arm_expr(compiler, schema, scope, loc, expr)
                },
            )?;
            fe
        }
        sqlast::ForEachOr::Item(item) => {
            let condition = compile_sqlarg(
                compiler.clone(),
                schema.clone(),
                scope.clone(),
                loc,
                &item.condition,
            )?;
            let result = compile_sqlarg(
                compiler.clone(),
                schema.clone(),
                scope.clone(),
                loc,
                &item.result,
            )?;
            mkcref(vec![CompiledCaseArm {
                condition: CTypedSQL {
                    type_: condition.type_.clone(),
                    sql: condition.sql.clone(),
                },
                result: CTypedSQL {
                    type_: result.type_.clone(),
                    sql: result.sql.clone(),
                },
            }])
        }
    })
}

#[async_backtrace::framed]
async fn extract_scalar_subselect_field(
    loc: &SourceLocation,
    query_type: &MType,
) -> Result<MField> {
    let inner_record = match &query_type {
        MType::List(t) => {
            let inner_type = t.get().await?;
            let inner_type = inner_type.read()?;
            match &*inner_type {
                MType::Record(r) => Some(r.clone()),
                _ => None,
            }
        }
        _ => None,
    };

    let inner_record = match inner_record {
        Some(i) => i,
        None => {
            return Err(CompileError::internal(
                loc.clone(),
                format!(
                    "Subselect expected to return a list of records (not {:?})",
                    query_type
                )
                .as_str(),
            ))
        }
    };

    Ok(if inner_record.get().len() == 1 {
        inner_record.get()[0].clone()
    } else {
        return Err(CompileError::scalar_subselect(
            inner_record.location().clone(),
            format!(
                "should return a single field (not {})",
                inner_record.get().len()
            )
            .as_str(),
        ));
    })
}

fn expand_foreach(
    compiler: &Compiler,
    schema: &Ref<Schema>,
    scope: &Ref<SQLScope>,
    loc: &SourceLocation,
    ranges: &[sqlast::LoopRange],
    substitutions: BTreeMap<Ident, SQLBody>,
) -> Result<Vec<BTreeMap<Ident, SQLBody>>> {
    if ranges.len() == 0 {
        Ok(vec![substitutions])
    } else {
        let mut ret = vec![];
        let range = &ranges[0];
        match range.range.as_ref() {
            sqlast::Expr::Array(sqlast::Array { elem, .. }) => {
                for e in elem.iter() {
                    let mut substitutions = substitutions.clone();
                    substitutions.insert(range.item.get().into(), SQLBody::Expr(e.clone()));

                    ret.extend(expand_foreach(
                        compiler,
                        schema,
                        scope,
                        loc,
                        &ranges[1..],
                        substitutions,
                    )?);
                }
            }
            o => {
                return Err(CompileError::invalid_foreach(
                    loc.clone(),
                    &format!("foreach range expected to be a literal array (got {:?})", o),
                ));
            }
        };

        Ok(ret)
    }
}

pub fn compile_sqlexpr(
    compiler: Compiler,
    schema: Ref<Schema>,
    scope: Ref<SQLScope>,
    loc: &SourceLocation,
    expr: &sqlast::Expr,
) -> Result<CTypedExpr> {
    let file = schema.read()?.file.clone();
    let c_sqlarg =
        |e: &sqlast::Expr| compile_sqlarg(compiler.clone(), schema.clone(), scope.clone(), loc, e);

    let ret = match expr {
        sqlast::Expr::Value(v) => match v {
            sqlast::Value::Number(n, _) => {
                let numeric_type =
                    parse_numeric_type(n).context(TypesystemSnafu { loc: loc.clone() })?;
                let parser_data_type: ParserDataType = (&Type::Atom(numeric_type.clone()))
                    .try_into()
                    .context(TypesystemSnafu { loc: loc.clone() })?;

                CTypedExpr {
                    type_: mkcref(MType::Atom(Located::new(numeric_type, loc.clone()))),
                    expr: mkcref(Expr::native_sql(Arc::new(SQL {
                        names: CSQLNames::new(),
                        body: SQLBody::Expr(sqlast::Expr::Cast {
                            expr: Box::new(expr.clone()),
                            data_type: parser_data_type,
                        }),
                    }))),
                }
            }
            sqlast::Value::SingleQuotedString(_)
            | sqlast::Value::EscapedStringLiteral(_)
            | sqlast::Value::NationalStringLiteral(_)
            | sqlast::Value::HexStringLiteral(_)
            | sqlast::Value::UnQuotedString(_)
            | sqlast::Value::DollarQuotedString(_)
            | sqlast::Value::DoubleQuotedString(_)
            | sqlast::Value::RawStringLiteral(_) => CTypedExpr {
                type_: resolve_global_atom(compiler.clone(), "string")?,
                expr: mkcref(Expr::native_sql(Arc::new(SQL {
                    names: CSQLNames::new(),
                    body: SQLBody::Expr(expr.clone()),
                }))),
            },
            sqlast::Value::SingleQuotedByteStringLiteral(_)
            | sqlast::Value::DoubleQuotedByteStringLiteral(_) => CTypedExpr {
                type_: resolve_global_atom(compiler.clone(), "blob")?,
                expr: mkcref(Expr::native_sql(Arc::new(SQL {
                    names: CSQLNames::new(),
                    body: SQLBody::Expr(expr.clone()),
                }))),
            },
            sqlast::Value::Boolean(_) => CTypedExpr {
                type_: resolve_global_atom(compiler.clone(), "bool")?,
                expr: mkcref(Expr::native_sql(Arc::new(SQL {
                    names: CSQLNames::new(),
                    body: SQLBody::Expr(expr.clone()),
                }))),
            },
            sqlast::Value::Null => NULL.clone(),
            sqlast::Value::Placeholder(_) => {
                return Err(CompileError::unimplemented(
                    loc.clone(),
                    format!("SQL Parameter syntax: {}", expr).as_str(),
                ))
            }
            sqlast::Value::FormatString(_) => {
                return Err(CompileError::unimplemented(
                    loc.clone(),
                    format!("Format string values: {}", expr).as_str(),
                ))
            }
        },
        sqlast::Expr::Array(sqlast::Array { elem, named }) => {
            let c_elems = elem
                .iter()
                .map(|e| c_sqlarg(e))
                .collect::<Result<Vec<_>>>()?;
            let mut c_elem_iter = c_elems.iter();
            let data_type = if let Some(first) = c_elem_iter.next() {
                for next in c_elem_iter {
                    first.type_.unify(&next.type_)?;
                }
                first.type_.clone()
            } else {
                mkcref(MType::Atom(Located::new(AtomicType::Null, loc.clone())))
            };

            let named = *named;
            CTypedExpr {
                type_: mkcref(MType::List(Located::new(
                    data_type,
                    SourceLocation::Unknown,
                ))),
                expr: combine_crefs(c_elems.iter().map(|s| s.sql.clone()).collect())?.then(
                    move |args: Ref<Vec<Ref<SQL<CRef<MType>>>>>| {
                        let names = combine_sqlnames(&*args.read()?)?;
                        Ok(mkcref(Expr::native_sql(Arc::new(SQL {
                            names,
                            body: SQLBody::Expr(sqlast::Expr::Array(sqlast::Array {
                                elem: args
                                    .read()?
                                    .iter()
                                    .map(|s| Ok(s.read()?.body.as_expr()?))
                                    .collect::<Result<Vec<_>>>()?,
                                named,
                            })),
                        }))))
                    },
                )?,
            }
        }
        sqlast::Expr::Struct(sqlast::Struct(elems)) => {
            let mut field_names = Vec::new();
            let mut fields = Vec::new();
            let mut c_values = Vec::new();
            for sqlast::StructField { name, value } in elems {
                let CTypedSQL { type_, sql } = c_sqlarg(value)?;

                fields.push(MField {
                    name: name.get().into(),
                    type_,
                    nullable: true, // Just assume all literals are nulalble
                });

                field_names.push(name.clone());
                c_values.push(sql);
            }

            let c_values = combine_crefs(c_values)?;

            CTypedExpr {
                type_: mkcref(MType::Record(Located::new(fields, loc.clone()))),
                expr: compiler.async_cref(async move {
                    let values = c_values.await?;
                    let values = values.read()?;

                    let mut names = CSQLNames::new();
                    let values = field_names
                        .into_iter()
                        .zip(values.iter())
                        .map(|(name, value)| {
                            let value = value.read()?;
                            names.extend(value.names.clone());
                            Ok(sqlast::StructField {
                                name,
                                value: value.body.as_expr()?,
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;

                    Ok(mkcref(Expr::native_sql(Arc::new(SQL {
                        names,
                        body: SQLBody::Expr(sqlast::Expr::Struct(sqlast::Struct(values))),
                    }))))
                })?,
            }
        }
        sqlast::Expr::IsNull(e) | sqlast::Expr::IsNotNull(e) => {
            let compiled =
                compile_sqlarg(compiler.clone(), schema.clone(), scope.clone(), loc, &e)?;
            let constructor = match expr {
                sqlast::Expr::IsNull(_) => sqlast::Expr::IsNull,
                sqlast::Expr::IsNotNull(_) => sqlast::Expr::IsNotNull,
                _ => unreachable!(),
            };
            CTypedExpr {
                type_: resolve_global_atom(compiler.clone(), "bool")?,
                expr: compiled.sql.then({
                    move |sqlexpr: Ref<SQL<CRef<MType>>>| {
                        Ok(mkcref(Expr::native_sql(Arc::new(SQL {
                            names: sqlexpr.read()?.names.clone(),
                            body: SQLBody::Expr(constructor(Box::new(
                                sqlexpr.read()?.body.as_expr()?,
                            ))),
                        }))))
                    }
                })?,
            }
        }
        sqlast::Expr::Between {
            expr,
            negated,
            low,
            high,
        } => {
            let op = sqlast::BinaryOperator::Lt;
            let cexpr = c_sqlarg(expr)?;
            let clow = c_sqlarg(low)?;
            let chigh = c_sqlarg(high)?;

            let (_, casted) = coerce_all(&compiler, &op, loc, vec![cexpr, clow, chigh], None)?;
            let type_ = resolve_global_atom(compiler.clone(), "bool")?;
            let negated = *negated;

            CTypedExpr {
                type_,
                expr: combine_crefs(casted.into_iter().map(|c| c.sql).collect())?.then({
                    move |args: Ref<Vec<_>>| {
                        let names = combine_sqlnames(&*args.read()?)?;
                        Ok(mkcref(Expr::native_sql(Arc::new(SQL {
                            names,
                            body: SQLBody::Expr(sqlast::Expr::Between {
                                expr: Box::new(args.read()?[0].read()?.body.as_expr()?),
                                negated: negated,
                                low: Box::new(args.read()?[1].read()?.body.as_expr()?),
                                high: Box::new(args.read()?[2].read()?.body.as_expr()?),
                            }),
                        }))))
                    }
                })?,
            }
        }
        sqlast::Expr::BinaryOp { left, op, right } => {
            let op = op.clone();
            let mut cleft = compile_sqlarg(
                compiler.clone(),
                schema.clone(),
                scope.clone(),
                loc,
                left.as_ref(),
            )?;
            let mut cright = compile_sqlarg(
                compiler.clone(),
                schema.clone(),
                scope.clone(),
                loc,
                right.as_ref(),
            )?;
            use sqlast::BinaryOperator::*;
            let type_ = match op {
                Plus | Minus | Multiply | Divide => {
                    let (result_type, casted) =
                        coerce_all(&compiler, &op, loc, vec![cleft, cright], None)?;
                    (cleft, cright) = (casted[0].clone(), casted[1].clone());
                    result_type
                }
                Eq | NotEq | Lt | LtEq | Gt | GtEq => {
                    let (_, casted) = coerce_all(&compiler, &op, loc, vec![cleft, cright], None)?;
                    (cleft, cright) = (casted[0].clone(), casted[1].clone());
                    resolve_global_atom(compiler.clone(), "bool")?
                }

                And | Or | Xor | BitwiseOr | BitwiseAnd | BitwiseXor | PGBitwiseXor
                | PGBitwiseShiftLeft | PGBitwiseShiftRight => {
                    let bool_val = CTypedSQL {
                        type_: resolve_global_atom(compiler.clone(), "bool")?,
                        sql: mkcref(SQL {
                            names: CSQLNames::new(),
                            body: SQLBody::Expr(sqlast::Expr::Value(sqlast::Value::Null)),
                        }),
                    };
                    let (_, casted) =
                        coerce_all(&compiler, &op, loc, vec![cleft, cright, bool_val], None)?;
                    (cleft, cright) = (casted[0].clone(), casted[1].clone());
                    resolve_global_atom(compiler.clone(), "bool")?
                }
                _ => {
                    return Err(CompileError::unimplemented(
                        loc.clone(),
                        format!("Binary operator: {}", op).as_str(),
                    ));
                }
            };
            CTypedExpr {
                type_,
                expr: combine_crefs(vec![cleft.sql, cright.sql])?.then({
                    move |args: Ref<Vec<Ref<SQL<CRef<MType>>>>>| {
                        let names = combine_sqlnames(&*args.read()?)?;
                        Ok(mkcref(Expr::native_sql(Arc::new(SQL {
                            names,
                            body: SQLBody::Expr(sqlast::Expr::BinaryOp {
                                left: Box::new(args.read()?[0].read()?.body.as_expr()?),
                                op: op.clone(),
                                right: Box::new(args.read()?[1].read()?.body.as_expr()?),
                            }),
                        }))))
                    }
                })?,
            }
        }
        sqlast::Expr::UnaryOp { op, expr } => {
            let op = op.clone();
            let cexpr = compile_sqlarg(
                compiler.clone(),
                schema.clone(),
                scope.clone(),
                loc,
                expr.as_ref(),
            )?;
            use sqlast::UnaryOperator::*;
            let type_ = match op {
                Plus | Minus => {
                    // NOTE: There is some logic varies per SQL engine about which types are
                    // accepted in the + and - unary operators. Ideally, we throw a compiler error
                    // here, depending on the engine.
                    cexpr.type_.clone()
                }
                Not => resolve_global_atom(compiler.clone(), "bool")?,
                _ => {
                    return Err(CompileError::unimplemented(
                        loc.clone(),
                        format!("Unary operator: {}", op).as_str(),
                    ));
                }
            };
            CTypedExpr {
                type_,
                expr: compiler.async_cref(async move {
                    let expr = cexpr.sql.await?;
                    let expr = expr.read()?;

                    Ok(mkcref(Expr::native_sql(Arc::new(SQL {
                        names: expr.names.clone(),
                        body: SQLBody::Expr(sqlast::Expr::UnaryOp {
                            op,
                            expr: Box::new(expr.body.as_expr()?),
                        }),
                    }))))
                })?,
            }
        }
        sqlast::Expr::Case {
            operand,
            arms,
            else_result,
        } => {
            let c_operand = match operand {
                Some(o) => Some(c_sqlarg(&o)?),
                None => None,
            };

            let arms = combine_crefs(
                arms.iter()
                    .map(|arm| compile_case_arm_expr(&compiler, &schema, &scope, loc, arm))
                    .collect::<Result<Vec<_>>>()?,
            )?;

            let loc = loc.clone();
            let else_result = else_result.clone();

            let else_result = if let Some(e) = else_result {
                Some(c_sqlarg(&e)?)
            } else {
                None
            };

            let full_expr = compiler.clone().async_cref({
                let compiler = compiler.clone();
                casync!({
                    let arms = arms.await?;
                    let arms = arms.read()?.clone();

                    let mut all_arms = Vec::new();
                    for arm in arms.iter() {
                        let arm_set = arm.read()?;
                        all_arms.extend(arm_set.iter().cloned());
                    }

                    // If there's an operand, then unify the conditions against it, otherwise
                    // unify them to bool.
                    let condition_type = match &c_operand {
                        Some(o) => o.type_.clone(),
                        None => resolve_global_atom(compiler.clone(), "bool")?,
                    };

                    for arm in all_arms.iter() {
                        arm.condition.type_.unify(&condition_type)?;
                    }
                    let mut all_results = Vec::new();
                    for arm in all_arms.iter() {
                        all_results.push(arm.result.clone());
                    }

                    if let Some(e) = &else_result {
                        all_results.push(e.clone());
                    }

                    let (result_type, mut c_results) = coerce_all(
                        &compiler,
                        &sqlast::BinaryOperator::Eq,
                        &loc,
                        all_results,
                        None,
                    )?;

                    let c_else_result = match else_result {
                        Some(_) => Some(c_results.pop().unwrap()),
                        None => None,
                    };

                    let mut names = CSQLNames::new();
                    let operand = match c_operand {
                        Some(ref o) => {
                            let operand = (&o.sql).await?;
                            let operand = operand.read()?;
                            names.extend(operand.names.clone());
                            Some(Box::new(operand.body.as_expr()?))
                        }
                        None => None,
                    };

                    let mut arms = Vec::new();
                    for (arm, c_result) in all_arms.into_iter().zip(c_results) {
                        let condition = arm.condition.sql.await?;
                        let result = c_result.sql.await?;
                        let condition = condition.read()?;
                        let result = result.read()?;
                        names.extend(condition.names.clone());
                        names.extend(result.names.clone());
                        arms.push(sqlast::ForEachOr::Item(sqlast::CaseArm {
                            condition: condition.body.as_expr()?,
                            result: result.body.as_expr()?,
                        }));
                    }

                    let else_result = match c_else_result {
                        Some(ref o) => {
                            let operand = o.sql.clone().await?;
                            let operand = operand.read()?;
                            names.extend(operand.names.clone());
                            Some(Box::new(operand.body.as_expr()?))
                        }
                        None => None,
                    };

                    let body = sqlast::Expr::Case {
                        operand,
                        arms,
                        else_result,
                    };

                    Ok(mkcref(CTypedExpr {
                        type_: result_type,
                        expr: mkcref(Expr::native_sql(Arc::new(SQL {
                            names,
                            body: SQLBody::Expr(body),
                        }))),
                    }))
                })
            })?;

            CTypedExpr::split(full_expr)?
        }
        sqlast::Expr::Function(sqlast::Function {
            name,
            args,
            over,
            distinct,
            special,
        }) => {
            let distinct = *distinct;
            let special = *special;
            let over = over.compile_sql(&compiler, &schema, &scope, loc)?;

            let func_name = name.to_path(file.clone());

            let func = compile_reference(compiler.clone(), schema.clone(), &func_name)?;
            let fn_type = match func
                .type_
                .must()
                .context(RuntimeSnafu { loc: loc.clone() })?
                .read()?
                .clone()
            {
                MType::Fn(f) => f,
                _ => {
                    return Err(CompileError::wrong_type(
                        &MType::Fn(Located::new(
                            MFnType {
                                args: Vec::new(),
                                variadic_arg: None,
                                ret: MType::new_unknown("ret"),
                            },
                            loc.clone(),
                        )),
                        &*func
                            .type_
                            .must()
                            .context(RuntimeSnafu { loc: loc.clone() })?
                            .read()?,
                    ))
                }
            };
            let mut compiled_args: BTreeMap<Ident, CTypedNameAndExpr> = BTreeMap::new();
            let mut variadic_args = Vec::new();
            let mut pos: usize = 0;
            for arg in args {
                let (name, expr) = match arg {
                    sqlast::FunctionArg::Named { name, arg } => {
                        (Some(Ident::with_location(loc.clone(), name.get())), arg)
                    }
                    sqlast::FunctionArg::Unnamed(arg) => {
                        if pos >= fn_type.args.len() {
                            if fn_type.variadic_arg.is_some() {
                                (None, arg)
                            } else {
                                return Err(CompileError::no_such_entry(vec![
                                    Ident::with_location(loc.clone(), format!("argument {}", pos)),
                                ]));
                            }
                        } else {
                            pos += 1;
                            (
                                Some(Ident::with_location(
                                    loc.clone(),
                                    fn_type.args[pos - 1].name.clone(),
                                )),
                                arg,
                            )
                        }
                    }
                };

                let expr = match expr {
                    sqlast::FunctionArgExpr::Expr(e) => Cow::Borrowed(e),
                    sqlast::FunctionArgExpr::Wildcard
                    | sqlast::FunctionArgExpr::QualifiedWildcard(_) => {
                        // TODO: This is a really clunky way of checking for wildcards. Ideally we do not need to produce
                        // the count function name each time.
                        //
                        // Wildcards (qualified or not) are only supported for certain functions
                        // (count as far as we know, and potentially others).
                        if func_name.as_slice().first().map(|s| s.get())
                            == Some(Into::<Ident>::into("count")).as_ref()
                        {
                            Cow::Owned(sqlast::Expr::Value(sqlast::Value::Number(
                                "1".to_string(),
                                false,
                            )))
                        } else {
                            return Err(CompileError::unimplemented(
                                loc.clone(),
                                &format!("wildcard arguments for {:?} function", func_name),
                            ));
                        }
                    }
                };

                let compiled_arg =
                    compile_sqlexpr(compiler.clone(), schema.clone(), scope.clone(), loc, &expr)?;

                match name {
                    Some(name) => {
                        if compiled_args.get(&name).is_some() {
                            return Err(CompileError::duplicate_entry(vec![name]));
                        }

                        compiled_args.insert(
                            name.get().clone(),
                            CTypedNameAndExpr {
                                name: name.get().clone(),
                                type_: compiled_arg.type_,
                                expr: compiled_arg.expr,
                            },
                        );
                    }
                    None => {
                        variadic_args.push(compiled_arg);
                    }
                }
            }

            let mut arg_exprs = Vec::new();
            for arg in &fn_type.args {
                if let Some(compiled_arg) = compiled_args.get_mut(&arg.name) {
                    arg.type_.unify(&compiled_arg.type_)?;
                    arg_exprs.push(compiled_arg.clone());
                } else if arg.nullable {
                    // If the argument is missing and nullable, then set it to NULL
                    // as a default value. Eventually we may want to generalize this
                    // so that functions can declare other kinds of default values too.
                    arg_exprs.push(CTypedNameAndExpr {
                        name: arg.name.clone(),
                        type_: NULL.type_.clone(),
                        expr: NULL.expr.clone(),
                    });
                } else {
                    return Err(CompileError::missing_arg(vec![Ident::without_location(
                        arg.name.clone(),
                    )]));
                }
            }

            if let Some(variadic_arg) = &fn_type.variadic_arg {
                for compiled_arg in &variadic_args {
                    variadic_arg.type_.unify(&compiled_arg.type_)?;
                }
            }

            let type_ = fn_type.ret.clone();

            let expr = compiler.async_cref({
                let compiler = compiler.clone();
                let schema = schema.clone();
                let loc = loc.clone();
                let type_ = type_.clone();
                casync!({
                    let arg_exprs = arg_exprs
                        .into_iter()
                        .map(move |cte| {
                            cte.expr.then(move |expr: Ref<Expr<CRef<MType>>>| {
                                Ok(mkcref(TypedNameAndExpr {
                                    type_: cte.type_.clone(),
                                    name: cte.name.clone(),
                                    expr: Arc::new(expr.read()?.clone()),
                                }))
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let arg_exprs = combine_crefs(arg_exprs)?.await?;

                    let variadic_args = variadic_args
                        .into_iter()
                        .map(move |cte| {
                            cte.expr.then(move |expr: Ref<Expr<CRef<MType>>>| {
                                Ok(mkcref(TypedExpr {
                                    type_: cte.type_.clone(),
                                    expr: Arc::new(expr.read()?.clone()),
                                }))
                            })
                        })
                        .collect::<Result<Vec<_>>>()?;
                    let variadic_args = combine_crefs(variadic_args)?.await?;

                    let over = cunwrap(over.await?)?;

                    let args = arg_exprs
                        .read()?
                        .iter()
                        .map(|e| Ok(e.read()?.clone()))
                        .collect::<Result<Vec<_>>>()?;

                    let variadic_args = variadic_args
                        .read()?
                        .iter()
                        .map(|e| Ok(e.read()?.clone()))
                        .collect::<Result<Vec<_>>>()?;

                    if type_.is_known()? {
                        match &*type_
                            .must()
                            .context(RuntimeSnafu { loc: loc.clone() })?
                            .read()?
                        {
                            MType::Generic(generic) => {
                                if let Some(generic) =
                                    as_generic::<ExternalType>(generic.get().as_ref())
                                {
                                    let inner_type = generic.inner_type();

                                    // TODO We should place some metadata on the function, or have a whitelist
                                    // of functions that work this way, but for now, we simply special case the
                                    // load function
                                    if func_name.as_slice()[0].get() != &Into::<Ident>::into("load")
                                    {
                                        return Err(CompileError::unimplemented(
                                            loc.clone(),
                                            "external types for non-load functions",
                                        ));
                                    }

                                    if variadic_args.len() > 0 {
                                        return Err(CompileError::unimplemented(
                                            loc.clone(),
                                            "variadic arguments for external types",
                                        ));
                                    }

                                    let resolve = schema_infer_load_fn(
                                        schema.clone(),
                                        args.clone(),
                                        inner_type.clone(),
                                    );
                                    compiler.add_external_type(
                                        resolve,
                                        inner_type.clone(),
                                        ExternalTypeRank::Load,
                                    )?;
                                }
                            }
                            _ => {}
                        }
                    }

                    let func_expr = func.expr.unwrap_schema_entry().await?;
                    let compiled_func_expr = match func_expr {
                        Expr::UncompiledFn(def) => {
                            if variadic_args.len() > 0 {
                                return Err(CompileError::unimplemented(
                                    loc.clone(),
                                    "variadic arguments for user defined generic functions",
                                ));
                            }

                            let arg_map = args
                                .iter()
                                .map(|e| (e.name.clone(), e.clone()))
                                .collect::<BTreeMap<_, _>>();

                            let (compiled_body, _generics) = compile_fn_body(
                                compiler.clone(),
                                schema.clone(),
                                loc.clone(),
                                &def,
                                arg_map,
                                FnContext::Call,
                            )?;
                            // NOTE: See comment in compile_fn_body() about why we don't
                            // compile functions, even without generics.
                            /*
                            if generics.is_empty() {
                                return Err(CompileError::internal(
                                    loc.clone(),
                                    "Non-generic function should have been compiled ahead of time",
                                ));
                            } */

                            compiled_body.type_.unify(&func.type_)?;
                            compiled_body.expr.await?.read()?.clone()
                        }
                        _ => func_expr,
                    };

                    let (fn_kind, fn_body) = match compiled_func_expr {
                        Expr::NativeFn(_) => (FnKind::Native, None),
                        Expr::Fn(FnExpr { body, .. }) => match body {
                            FnBody::SQLBuiltin(name) => (FnKind::SQLBuiltin(name.clone()), None),
                            FnBody::Expr(expr) => (FnKind::Expr, Some(expr.clone())),
                        },
                        _ => {
                            return Err(CompileError::internal(
                                loc.clone(),
                                "Function value must be function expression",
                            ))
                        }
                    };

                    // Function calls against native functions that do not reference any unbound
                    // SQL names in their arguments can be lifted out of the SQL body, which is
                    // important because we don't yet support running native functions within SQL.
                    // It will always be necessary to some extent, because some native functions
                    // can't be pushed down either because of their types or because they must be
                    // run locally (e.g. `load`).
                    //
                    let can_lift = !args
                        .iter()
                        .map(|arg| &arg.expr)
                        .chain(variadic_args.iter().map(|arg| &arg.expr))
                        .any(|expr| has_unbound_names(expr.clone()))
                        || !over.names.unbound.is_empty();
                    let should_lift = if compiler.allow_inlining()? {
                        matches!(fn_kind, FnKind::Native)
                    } else {
                        !matches!(fn_kind, FnKind::SQLBuiltin(..))
                    };
                    let lift = can_lift && should_lift;

                    if lift {
                        let args = args
                            .iter()
                            .map(TypedNameAndExpr::to_typed_expr)
                            .collect::<Vec<_>>();
                        if variadic_args.len() > 0 {
                            return Err(CompileError::unimplemented(
                                loc.clone(),
                                "variadic arguments for native functions",
                            ));
                        }
                        Ok(mkcref(Expr::FnCall(FnCallExpr {
                            func: Arc::new(TypedExpr {
                                type_: mkcref(MType::Fn(fn_type.clone())),
                                expr: func.expr.clone(),
                            }),
                            args,
                            ctx_folder: schema.read()?.folder.clone(),
                        })))
                    } else {
                        match (&fn_kind, fn_body) {
                            // If the function body is an expression, inline it.
                            //
                            (FnKind::Expr, Some(fn_body)) if compiler.allow_inlining()? => {
                                // Within a function body, arguments are represented as
                                // Expr::ContextRef with the given name.  This first pass will
                                // replace any context references with the actual argument bodies,
                                // but leave those expressions in place.  This means the function
                                // aguments will be embedded within the params of the expression
                                // temporarily, even though they may contain free SQL variables.
                                //
                                let fn_body = inline_context(
                                    fn_body,
                                    args.iter()
                                        .map(|a| (a.name.clone(), a.expr.clone()))
                                        .collect(),
                                )
                                .await?;
                                // Next, eagerly inline any parameters with SQL definitions into
                                // the SQL of the function body.  This should result in a version
                                // of the body with all SQL arguments fully inlined.
                                //
                                let fn_body = inline_params(fn_body.as_ref()).await?;

                                if variadic_args.len() > 0 {
                                    return Err(CompileError::unimplemented(
                                        loc.clone(),
                                        "variadic arguments for inlined functions",
                                    ));
                                }
                                Ok(mkcref(fn_body))
                            }
                            // Otherwise, create a SQL function call.
                            //
                            _ => {
                                let mut names = CSQLNames::new();
                                let mut args = Vec::new();

                                // Our SQL builtin functions have arbitrarily named function arguments, so just use Unnamed arguments
                                for arg in &*arg_exprs.read()? {
                                    let sql = intern_placeholder(
                                        compiler.clone(),
                                        "arg",
                                        &arg.read()?.to_typed_expr(),
                                    )?;
                                    args.push(sqlast::FunctionArg::Unnamed(
                                        sqlast::FunctionArgExpr::Expr(sql.body.as_expr()?),
                                    ));
                                    names.extend(sql.names.clone());
                                }

                                for arg in &variadic_args {
                                    let sql = intern_placeholder(compiler.clone(), "arg", &arg)?;
                                    args.push(sqlast::FunctionArg::Unnamed(
                                        sqlast::FunctionArgExpr::Expr(sql.body.as_expr()?),
                                    ));
                                    names.extend(sql.names.clone());
                                }

                                let name = match fn_kind {
                                    FnKind::SQLBuiltin(sql_name) => (&sql_name).into(),
                                    _ => {
                                        // Note that this branch will only be matched in the case
                                        // of a native function that couldn't be lifted (i.e.
                                        // because an argument referenced a SQL name).  It will
                                        // attempt to provide the function value as a parameter to
                                        // the SQL, which will fail in the runtime code until we
                                        // implement UDFs.
                                        //
                                        let (func_name, func) = intern_nonsql_placeholder(
                                            compiler.clone(),
                                            "func",
                                            &func,
                                        )?;
                                        names.extend(func.names.clone());
                                        sqlast::ObjectName(vec![func_name])
                                    }
                                };
                                Ok(mkcref(Expr::native_sql(Arc::new(SQL {
                                    names,
                                    body: SQLBody::Expr(sqlast::Expr::Function(sqlast::Function {
                                        name,
                                        args,
                                        over: over.body,
                                        distinct,
                                        special,
                                    })),
                                }))))
                            }
                        }
                    }
                })
            })?;

            CTypedExpr { type_, expr }
        }
        sqlast::Expr::Tuple(fields) => {
            let c_fields = fields
                .iter()
                .map(|f| compile_sqlarg(compiler.clone(), schema.clone(), scope.clone(), loc, f))
                .collect::<Result<Vec<_>>>()?;

            let c_types = c_fields.iter().map(|f| f.type_.clone()).collect::<Vec<_>>();
            let c_exprs =
                combine_crefs(c_fields.iter().map(|f| f.sql.clone()).collect::<Vec<_>>())?;

            CTypedExpr {
                // NOTE: Postgres turns tuples into records whose fields are named f1, f2, ...,
                // whereas DuckDB creates records into fields named v1, v2 ,... We pick Postgres
                // semantics here, but may need to specify this based on the target dialect.
                type_: mkcref(MType::Record(Located::new(
                    c_types
                        .iter()
                        .enumerate()
                        .map(|(i, t)| {
                            Ok(MField {
                                name: format!("f{}", i + 1).into(),
                                type_: t.clone(),
                                nullable: true,
                            })
                        })
                        .collect::<Result<Vec<_>>>()?,
                    SourceLocation::Unknown,
                ))),
                expr: compiler.async_cref(async move {
                    let exprs = c_exprs.await?;
                    let mut names = CSQLNames::new();

                    let mut ret = Vec::new();
                    for expr in &*exprs.read()? {
                        let expr = expr.read()?;
                        names.extend(expr.names.clone());
                        ret.push(expr.body.as_expr()?);
                    }

                    Ok(mkcref(Expr::native_sql(Arc::new(SQL {
                        names,
                        body: SQLBody::Expr(sqlast::Expr::Tuple(ret)),
                    }))))
                })?,
            }
        }
        sqlast::Expr::ArrayAgg(sqlast::ArrayAgg {
            distinct,
            expr,
            order_by,
            limit,
            within_group,
        }) => {
            let compiled_expr = compile_sqlarg(
                compiler.clone(),
                schema.clone(),
                scope.clone(),
                loc,
                expr.as_ref(),
            )?;

            let ob = order_by.compile_sql(&compiler, &schema, &scope, loc)?;

            let limit = match limit {
                Some(l) => Some(compile_sqlarg(
                    compiler.clone(),
                    schema.clone(),
                    scope.clone(),
                    loc,
                    l,
                )?),
                None => None,
            };

            let distinct = *distinct;
            let within_group = *within_group;

            CTypedExpr {
                type_: mkcref(MType::List(Located::new(
                    compiled_expr.type_.clone(),
                    loc.clone(),
                ))),
                expr: compiler.async_cref(async move {
                    let mut names = CSQLNames::new();
                    let expr = compiled_expr.sql.await?;

                    let ob = cunwrap(ob.await?)?;

                    let limit = match limit {
                        Some(limit) => Some(limit.sql.await?),
                        None => None,
                    };

                    let expr = expr.read()?;
                    names.extend(expr.names.clone());

                    names.extend(ob.names);

                    let limit = match limit {
                        Some(limit) => {
                            let limit = limit.read()?;
                            names.extend(limit.names.clone());
                            Some(Box::new(limit.body.as_expr()?))
                        }
                        None => None,
                    };

                    Ok(mkcref(Expr::native_sql(Arc::new(SQL {
                        names,
                        body: SQLBody::Expr(sqlast::Expr::ArrayAgg(sqlast::ArrayAgg {
                            distinct,
                            expr: Box::new(expr.body.as_expr()?),
                            order_by: ob.body,
                            limit,
                            within_group,
                        })),
                    }))))
                })?,
            }
        }
        sqlast::Expr::Subquery(query) => {
            let (_scope, type_, subquery) = compile_sqlquery(
                compiler.clone(),
                schema.clone(),
                Some(scope.clone()),
                loc,
                query.as_ref(),
            )?;

            let loc = loc.clone();
            let type_ = compiler.async_cref(async move {
                let query_type = type_.await?;
                let query_type = query_type.read()?.clone();

                let first_field = extract_scalar_subselect_field(&loc, &query_type).await?;
                Ok(first_field.type_)
            })?;

            let expr = compiler.async_cref(async {
                let inner_query = cunwrap(subquery.await?)?;
                Ok(mkcref(Expr::native_sql(Arc::new(SQL {
                    names: inner_query.names.clone(),
                    body: SQLBody::Expr(sqlast::Expr::Subquery(Box::new(inner_query.body))),
                }))))
            })?;

            CTypedExpr { type_, expr }
        }
        sqlast::Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            let expr = compile_sqlarg(
                compiler.clone(),
                schema.clone(),
                scope.clone(),
                loc,
                expr.as_ref(),
            )?;
            let (_scope, subquery_type, subquery) = compile_sqlquery(
                compiler.clone(),
                schema.clone(),
                Some(scope.clone()),
                loc,
                subquery.as_ref(),
            )?;

            compiler.async_cref({
                let loc = loc.clone();
                async move {
                    let subquery_type = subquery_type.await?;
                    let subquery_type = subquery_type.read()?.clone();

                    let first_field = extract_scalar_subselect_field(&loc, &subquery_type).await?;
                    first_field.type_.unify(&expr.type_)?;

                    Ok(mkcref(()))
                }
            })?;

            let negated = *negated;

            CTypedExpr {
                type_: resolve_global_atom(compiler.clone(), "bool")?,
                expr: compiler.async_cref(async move {
                    let expr = expr.sql.clone().await?;
                    let subquery = cunwrap(subquery.await?)?;
                    let expr = expr.read()?;

                    let mut names = CSQLNames::new();
                    names.extend(expr.names.clone());
                    names.extend(subquery.names);

                    Ok(mkcref(Expr::native_sql(Arc::new(SQL {
                        names,
                        body: SQLBody::Expr(sqlast::Expr::InSubquery {
                            expr: Box::new(expr.body.as_expr()?),
                            subquery: Box::new(subquery.body),
                            negated,
                        }),
                    }))))
                })?,
            }
        }
        sqlast::Expr::CompoundIdentifier(sqlpath) => {
            compile_sqlreference(compiler.clone(), schema.clone(), scope.clone(), sqlpath)?
        }
        sqlast::Expr::Identifier(ident) => compile_sqlreference(
            compiler.clone(),
            schema.clone(),
            scope.clone(),
            &vec![ident.clone()],
        )?,
        sqlast::Expr::Nested(expr) => compile_sqlexpr(
            compiler.clone(),
            schema.clone(),
            scope.clone(),
            loc,
            expr.as_ref(),
        )?,
        sqlast::Expr::Substring {
            expr,
            substring_from,
            substring_for,
        } => {
            let expr = c_sqlarg(expr.as_ref())?;

            // This is a bit of a hack, since we won't typecheck these arguments.
            let substring_from = substring_from.compile_sql(&compiler, &schema, &scope, &loc)?;
            let substring_for = substring_for.compile_sql(&compiler, &schema, &scope, &loc)?;

            let type_ = resolve_global_atom(compiler.clone(), "string")?;
            expr.type_.unify(&type_)?;

            CTypedExpr {
                type_,
                expr: compiler.async_cref(async move {
                    let expr = expr.sql.await?;
                    let substring_from = cunwrap(substring_from.await?)?;
                    let substring_for = cunwrap(substring_for.await?)?;

                    let SQLSnippet { mut names, body } = (*expr.read()?).clone();

                    names.extend(substring_from.names);
                    names.extend(substring_for.names);

                    Ok(mkcref(Expr::native_sql(Arc::new(SQL {
                        names,
                        body: SQLBody::Expr(sqlast::Expr::Substring {
                            expr: Box::new(body.as_expr()?),
                            substring_from: substring_from.body,
                            substring_for: substring_for.body,
                        }),
                    }))))
                })?,
            }
        }
        sqlast::Expr::Cast { expr, data_type } => {
            let expr = expr.compile_sql(&compiler, &schema, &scope, &loc)?;
            let type_ = mkcref(MType::from_runtime_type(
                &data_type
                    .try_into()
                    .context(TypesystemSnafu { loc: loc.clone() })?,
            )?);

            // We could do some validation here to know whether the type can be cast, but currently we assume
            // that all casts are possible.

            let data_type = data_type.clone();
            CTypedExpr {
                type_,
                expr: compiler.async_cref(async move {
                    let SQLSnippet { names, body } = cunwrap(expr.await?)?;

                    Ok(mkcref(Expr::native_sql(Arc::new(SQL {
                        names,
                        body: SQLBody::Expr(sqlast::Expr::Cast {
                            expr: body,
                            data_type,
                        }),
                    }))))
                })?,
            }
        }
        sqlast::Expr::Interval {
            value,
            leading_field,
            leading_precision,
            last_field,
            fractional_seconds_precision,
        } => {
            let expr = c_sqlarg(value.as_ref())?;
            let leading_field = leading_field.clone();
            let leading_precision = leading_precision.clone();
            let last_field = last_field.clone();
            let fractional_seconds_precision = fractional_seconds_precision.clone();

            if last_field.is_some() {
                return Err(CompileError::unimplemented(
                    loc.clone(),
                    "Interval last field",
                ));
            }

            let leading_field = match leading_field {
                Some(f) => f,
                None => {
                    return Err(CompileError::unimplemented(
                        loc.clone(),
                        "Interval without leading field",
                    ))
                }
            };

            let type_ = mkcref(MType::Atom(Located::new(
                AtomicType::Interval(IntervalUnit::MonthDayNano),
                SourceLocation::Unknown,
            )));

            CTypedExpr {
                type_,
                expr: compiler.async_cref(async move {
                    let expr = expr.sql.await?;
                    let SQLSnippet { names, body } = expr.read()?.clone();

                    // SQL parsers don't like the cast we apply here.
                    let value = match body.as_expr()? {
                        sqlast::Expr::Cast { expr, .. } => expr,
                        other => Box::new(other),
                    };

                    Ok(mkcref(Expr::native_sql(Arc::new(SQL {
                        names,
                        body: SQLBody::Expr(sqlast::Expr::Interval {
                            value,
                            leading_field: Some(leading_field),
                            leading_precision,
                            last_field,
                            fractional_seconds_precision,
                        }),
                    }))))
                })?,
            }
        }
        sqlast::Expr::GroupingSets(sets) => {
            let mut compiled_sets = Vec::new();
            for set in sets {
                compiled_sets.push(compile_gb_ob_set(&compiler, &schema, &scope, loc, set)?);
            }

            let compiled_sets = combine_crefs(compiled_sets)?;

            CTypedExpr {
                type_: resolve_global_atom(compiler.clone(), "null")?,
                expr: compiler.async_cref(async move {
                    let compiled_sets = compiled_sets.await?;

                    let mut sets = Vec::new();
                    let mut names = SQLNames::new();
                    for compiled_set in compiled_sets.read()?.iter() {
                        let mut set = Vec::new();
                        for expr in compiled_set.read()?.iter() {
                            let expr = expr.read()?;
                            names.extend(expr.names.clone());
                            set.push(expr.body.as_expr()?);
                        }
                        sets.push(sqlast::ForEachOr::Item(set));
                    }

                    Ok(mkcref(Expr::native_sql(Arc::new(SQL {
                        names,
                        body: SQLBody::Expr(sqlast::Expr::GroupingSets(sets)),
                    }))))
                })?,
            }
        }
        _ => {
            return Err(CompileError::unimplemented(
                loc.clone(),
                format!("Expression: {:?}", expr).as_str(),
            ))
        }
    };

    Ok(ret)
}
