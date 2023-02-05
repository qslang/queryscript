use async_trait::async_trait;
use sqlparser::ast::*;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use crate::compile::error::Result;
use crate::compile::schema;

pub trait SQLVisitor {
    fn visit_sqlexpr(&self, _expr: &Expr) -> Option<Expr> {
        None
    }

    fn visit_sqlquery(&self, _query: &Query) -> Option<Query> {
        None
    }

    fn visit_sqlpath(&self, _path: &Vec<Located<Ident>>) -> Option<Vec<Located<Ident>>> {
        None
    }

    // NOTE: Most (all?) users should derive visit_sqlpath instead of visit_sqlident. There are
    // some ambiguities in the parser where it will sometimes return an Ident or a CompoundIdent,
    // but semantically these are just "paths" (e.g. foo vs. foo.bar). One example of _only_ defining
    // visit_sqlident would be changing quote behavior. That said, we may want to remove this method
    // from the trait, and just implement it in VisitSQL for Ident.
    fn visit_sqlident(&self, ident: &Ident) -> Option<Ident> {
        let path = vec![Located::new(ident.clone(), None)];
        if let Some(mut path) = self.visit_sqlpath(&path) {
            assert!(
                path.len() == 1,
                "visit_sqlpath should return a single element for a single ident",
            );
            return Some(path.swap_remove(0).into_inner());
        }
        None
    }

    fn visit_sqltable(&self, _table: &TableFactor) -> Option<TableFactor> {
        None
    }
}

#[async_trait]
pub trait Visitor<TypeRef>: SQLVisitor
where
    TypeRef: Clone + fmt::Debug + Send + Sync,
{
    async fn visit_expr(
        &self,
        _expr: &schema::Expr<TypeRef>,
    ) -> Result<Option<schema::Expr<TypeRef>>> {
        Ok(None)
    }
}

pub trait VisitSQL<V>
where
    V: SQLVisitor,
{
    fn visit_sql(&self, visitor: &V) -> Self;
}

#[async_trait]
pub trait Visit<V, TypeRef>: Sized
where
    TypeRef: Clone + fmt::Debug + Send + Sync,
{
    async fn visit(&self, visitor: &V) -> Result<Self>;
}

impl<V: SQLVisitor> VisitSQL<V> for Statement {
    fn visit_sql(&self, visitor: &V) -> Self {
        match self {
            Statement::Query(query) => Statement::Query(query.visit_sql(visitor)),
            Statement::CreateView {
                name,
                columns,
                query,
                materialized,
                or_replace,
                with_options,
                cluster_by,
            } => Statement::CreateView {
                name: name.visit_sql(visitor),
                columns: columns.visit_sql(visitor),
                query: query.visit_sql(visitor),
                materialized: *materialized,
                or_replace: *or_replace,
                with_options: with_options.visit_sql(visitor),
                cluster_by: cluster_by.visit_sql(visitor),
            },
            _ => self.clone(),
        }
    }
}

impl<V: SQLVisitor> VisitSQL<V> for Query {
    fn visit_sql(&self, visitor: &V) -> Self {
        if let Some(q) = visitor.visit_sqlquery(self) {
            return q;
        }

        Query {
            with: self.with.as_ref().map(|w| With {
                recursive: w.recursive,
                cte_tables: w.cte_tables.visit_sql(visitor),
            }),
            body: self.body.visit_sql(visitor),
            order_by: self.order_by.visit_sql(visitor),
            limit: self.limit.visit_sql(visitor),
            offset: self.offset.as_ref().map(|o| Offset {
                value: o.value.visit_sql(visitor),
                rows: o.rows.clone(),
            }),
            fetch: self.fetch.as_ref().map(|f| Fetch {
                with_ties: f.with_ties,
                percent: f.percent,
                quantity: f.quantity.visit_sql(visitor),
            }),
            locks: self.locks.clone(),
        }
    }
}

impl<V: SQLVisitor> VisitSQL<V> for Expr {
    fn visit_sql(&self, visitor: &V) -> Self {
        if let Some(e) = visitor.visit_sqlexpr(self) {
            return e;
        }

        use Expr::*;
        match self {
            Identifier(x) => Identifier(x.visit_sql(visitor)),
            CompoundIdentifier(v) => {
                if let Some(v) = visitor.visit_sqlpath(v) {
                    return CompoundIdentifier(v);
                }
                CompoundIdentifier(v.visit_sql(visitor))
            }
            JsonAccess {
                left,
                operator,
                right,
            } => JsonAccess {
                left: left.visit_sql(visitor),
                operator: operator.clone(),
                right: right.visit_sql(visitor),
            },
            CompositeAccess { expr, key } => CompositeAccess {
                expr: expr.visit_sql(visitor),
                key: key.visit_sql(visitor),
            },
            IsFalse(e) => IsFalse(e.visit_sql(visitor)),
            IsNotFalse(e) => IsNotFalse(e.visit_sql(visitor)),
            IsTrue(e) => IsTrue(e.visit_sql(visitor)),
            IsNotTrue(e) => IsNotTrue(e.visit_sql(visitor)),
            IsNull(e) => IsNull(e.visit_sql(visitor)),

            IsNotNull(e) => IsNotNull(e.visit_sql(visitor)),
            IsUnknown(e) => IsUnknown(e.visit_sql(visitor)),
            IsNotUnknown(e) => IsNotUnknown(e.visit_sql(visitor)),

            IsDistinctFrom(e1, e2) => IsDistinctFrom(e1.visit_sql(visitor), e2.visit_sql(visitor)),
            IsNotDistinctFrom(e1, e2) => {
                IsNotDistinctFrom(e1.visit_sql(visitor), e2.visit_sql(visitor))
            }
            InList {
                expr,
                list,
                negated,
            } => InList {
                expr: expr.visit_sql(visitor),
                list: list.visit_sql(visitor),
                negated: *negated,
            },
            InSubquery {
                expr,
                subquery,
                negated,
            } => InSubquery {
                expr: expr.visit_sql(visitor),
                subquery: subquery.visit_sql(visitor),
                negated: *negated,
            },
            InUnnest {
                expr,
                array_expr,
                negated,
            } => InUnnest {
                expr: expr.visit_sql(visitor),
                array_expr: array_expr.visit_sql(visitor),
                negated: *negated,
            },
            Between {
                expr,
                negated,
                low,
                high,
            } => Between {
                expr: expr.visit_sql(visitor),
                negated: *negated,
                low: low.visit_sql(visitor),
                high: high.visit_sql(visitor),
            },
            BinaryOp { left, op, right } => BinaryOp {
                left: left.visit_sql(visitor),
                op: op.clone(),
                right: right.visit_sql(visitor),
            },
            Like {
                negated,
                expr,
                pattern,
                escape_char,
            } => Like {
                negated: *negated,
                expr: expr.visit_sql(visitor),
                pattern: pattern.visit_sql(visitor),
                escape_char: escape_char.clone(),
            },
            ILike {
                negated,
                expr,
                pattern,
                escape_char,
            } => ILike {
                negated: *negated,
                expr: expr.visit_sql(visitor),
                pattern: pattern.visit_sql(visitor),
                escape_char: escape_char.clone(),
            },
            SimilarTo {
                negated,
                expr,
                pattern,
                escape_char,
            } => SimilarTo {
                negated: *negated,
                expr: expr.visit_sql(visitor),
                pattern: pattern.visit_sql(visitor),
                escape_char: escape_char.clone(),
            },
            AnyOp(e) => AnyOp(e.visit_sql(visitor)),
            AllOp(e) => AllOp(e.visit_sql(visitor)),
            UnaryOp { op, expr } => UnaryOp {
                op: op.clone(),
                expr: expr.visit_sql(visitor),
            },
            Cast { expr, data_type } => Cast {
                expr: expr.visit_sql(visitor),
                data_type: data_type.clone(),
            },
            TryCast { expr, data_type } => TryCast {
                expr: expr.visit_sql(visitor),
                data_type: data_type.clone(),
            },
            SafeCast { expr, data_type } => SafeCast {
                expr: expr.visit_sql(visitor),
                data_type: data_type.clone(),
            },
            AtTimeZone {
                timestamp,
                time_zone,
            } => AtTimeZone {
                timestamp: timestamp.visit_sql(visitor),
                time_zone: time_zone.clone(),
            },
            Extract { field, expr } => Extract {
                field: field.clone(),
                expr: expr.visit_sql(visitor),
            },
            Ceil { field, expr } => Ceil {
                field: field.clone(),
                expr: expr.visit_sql(visitor),
            },
            Floor { field, expr } => Floor {
                field: field.clone(),
                expr: expr.visit_sql(visitor),
            },
            Position { expr, r#in } => Position {
                expr: expr.visit_sql(visitor),
                r#in: r#in.visit_sql(visitor),
            },
            Substring {
                expr,
                substring_from,
                substring_for,
            } => Substring {
                expr: expr.visit_sql(visitor),
                substring_from: substring_from.visit_sql(visitor),
                substring_for: substring_for.visit_sql(visitor),
            },
            Trim {
                expr,
                // ([BOTH | LEADING | TRAILING]
                trim_where,
                trim_what,
            } => Trim {
                expr: expr.visit_sql(visitor),
                trim_where: trim_where.clone(),
                trim_what: trim_what.visit_sql(visitor),
            },
            Overlay {
                expr,
                overlay_what,
                overlay_from,
                overlay_for,
            } => Overlay {
                expr: expr.visit_sql(visitor),
                overlay_what: overlay_what.visit_sql(visitor),
                overlay_from: overlay_from.visit_sql(visitor),
                overlay_for: overlay_for.visit_sql(visitor),
            },
            Collate { expr, collation } => Collate {
                expr: expr.visit_sql(visitor),
                collation: collation.visit_sql(visitor),
            },
            Nested(e) => Nested(e.visit_sql(visitor)),
            Value(v) => Value(v.clone()),
            TypedString { data_type, value } => TypedString {
                data_type: data_type.clone(),
                value: value.clone(),
            },
            MapAccess { column, keys } => MapAccess {
                column: column.visit_sql(visitor),
                keys: keys.visit_sql(visitor),
            },
            Function(f) => Function(f.visit_sql(visitor)),
            AggregateExpressionWithFilter { expr, filter } => AggregateExpressionWithFilter {
                expr: expr.visit_sql(visitor),
                filter: filter.visit_sql(visitor),
            },
            Case {
                operand,
                conditions,
                results,
                else_result,
            } => Case {
                operand: operand.visit_sql(visitor),
                conditions: conditions.visit_sql(visitor),
                results: results.visit_sql(visitor),
                else_result: else_result.visit_sql(visitor),
            },
            Exists { subquery, negated } => Exists {
                subquery: subquery.visit_sql(visitor),
                negated: *negated,
            },
            Subquery(q) => Subquery(q.visit_sql(visitor)),
            ArraySubquery(q) => ArraySubquery(q.visit_sql(visitor)),
            ListAgg(la) => ListAgg(la.visit_sql(visitor)),
            ArrayAgg(aa) => ArrayAgg(aa.visit_sql(visitor)),
            MatchAgainst {
                columns,
                match_value,
                opt_search_modifier,
            } => MatchAgainst {
                columns: columns.visit_sql(visitor),
                match_value: match_value.clone(),
                opt_search_modifier: opt_search_modifier.clone(),
            },
            GroupingSets(gs) => GroupingSets(gs.visit_sql(visitor)),
            Cube(c) => Cube(c.visit_sql(visitor)),
            Rollup(r) => Rollup(r.visit_sql(visitor)),
            Tuple(t) => Tuple(t.visit_sql(visitor)),
            ArrayIndex { obj, indexes } => ArrayIndex {
                obj: obj.visit_sql(visitor),
                indexes: indexes.visit_sql(visitor),
            },
            Array(sqlparser::ast::Array { elem, named }) => Array(sqlparser::ast::Array {
                elem: elem.visit_sql(visitor),
                named: *named,
            }),
            Interval {
                value,
                leading_field,
                leading_precision,
                last_field,
                fractional_seconds_precision,
            } => Interval {
                value: value.visit_sql(visitor),
                leading_field: leading_field.clone(),
                leading_precision: leading_precision.clone(),
                last_field: last_field.clone(),
                fractional_seconds_precision: fractional_seconds_precision.clone(),
            },
        }
    }
}

impl<V: SQLVisitor> VisitSQL<V> for Function {
    fn visit_sql(&self, visitor: &V) -> Self {
        Function {
            name: self.name.visit_sql(visitor),
            args: self.args.visit_sql(visitor),
            over: self.over.visit_sql(visitor),
            distinct: self.distinct,
            special: self.special,
        }
    }
}

impl<V: SQLVisitor> VisitSQL<V> for WindowSpec {
    fn visit_sql(&self, visitor: &V) -> Self {
        WindowSpec {
            partition_by: self.partition_by.visit_sql(visitor),
            order_by: self.order_by.visit_sql(visitor),
            window_frame: self.window_frame.visit_sql(visitor),
        }
    }
}

impl<V: SQLVisitor> VisitSQL<V> for WindowFrame {
    fn visit_sql(&self, visitor: &V) -> Self {
        WindowFrame {
            units: self.units.clone(),
            start_bound: self.start_bound.visit_sql(visitor),
            end_bound: self.end_bound.visit_sql(visitor),
        }
    }
}

impl<V: SQLVisitor> VisitSQL<V> for WindowFrameBound {
    fn visit_sql(&self, visitor: &V) -> Self {
        use WindowFrameBound::*;
        match self {
            CurrentRow => CurrentRow,
            Preceding(e) => Preceding(e.visit_sql(visitor)),
            Following(e) => Following(e.visit_sql(visitor)),
        }
    }
}

impl<V: SQLVisitor> VisitSQL<V> for ListAgg {
    fn visit_sql(&self, visitor: &V) -> Self {
        ListAgg {
            distinct: self.distinct,
            expr: self.expr.visit_sql(visitor),
            separator: self.separator.visit_sql(visitor),
            on_overflow: self.on_overflow.visit_sql(visitor),
            within_group: self.within_group.visit_sql(visitor),
        }
    }
}

impl<V: SQLVisitor> VisitSQL<V> for ArrayAgg {
    fn visit_sql(&self, visitor: &V) -> Self {
        ArrayAgg {
            distinct: self.distinct,
            expr: self.expr.visit_sql(visitor),
            order_by: self.order_by.visit_sql(visitor),
            limit: self.limit.visit_sql(visitor),
            within_group: self.within_group,
        }
    }
}

impl<V: SQLVisitor> VisitSQL<V> for ListAggOnOverflow {
    fn visit_sql(&self, visitor: &V) -> Self {
        use ListAggOnOverflow::*;
        match self {
            Error => Error,
            Truncate { filler, with_count } => Truncate {
                filler: filler.visit_sql(visitor),
                with_count: *with_count,
            },
        }
    }
}

impl<V: SQLVisitor> VisitSQL<V> for SetExpr {
    fn visit_sql(&self, visitor: &V) -> Self {
        use SetExpr::*;
        match self {
            Select(s) => Select(s.visit_sql(visitor)),
            Query(q) => Query(q.visit_sql(visitor)),
            SetOperation {
                op,
                set_quantifier,
                left,
                right,
            } => SetOperation {
                op: op.clone(),
                set_quantifier: set_quantifier.clone(),
                left: left.visit_sql(visitor),
                right: right.visit_sql(visitor),
            },
            Values(sqlparser::ast::Values { explicit_row, rows }) => {
                Values(sqlparser::ast::Values {
                    explicit_row: *explicit_row,
                    rows: rows.visit_sql(visitor),
                })
            }
            Insert(_) => panic!("Unimplemented: INSERT statements"),
            Table(t) => Table(t.visit_sql(visitor)),
        }
    }
}

impl<V: SQLVisitor> VisitSQL<V> for Select {
    fn visit_sql(&self, visitor: &V) -> Self {
        Select {
            distinct: self.distinct,
            top: self.top.as_ref().map(|t| Top {
                with_ties: t.with_ties,
                percent: t.percent,
                quantity: t.quantity.visit_sql(visitor),
            }),
            projection: self.projection.visit_sql(visitor),
            into: self.into.as_ref().map(|f| SelectInto {
                temporary: f.temporary,
                unlogged: f.unlogged,
                table: f.table,
                name: f.name.visit_sql(visitor),
            }),
            from: self.from.visit_sql(visitor),
            lateral_views: self.lateral_views.visit_sql(visitor),
            selection: self.selection.visit_sql(visitor),
            group_by: self.group_by.visit_sql(visitor),
            cluster_by: self.cluster_by.visit_sql(visitor),
            distribute_by: self.distribute_by.visit_sql(visitor),
            sort_by: self.sort_by.visit_sql(visitor),
            having: self.having.visit_sql(visitor),
            qualify: self.qualify.visit_sql(visitor),
        }
    }
}

impl<V: SQLVisitor> VisitSQL<V> for Table {
    fn visit_sql(&self, _visitor: &V) -> Self {
        // TODO: Maybe these should be thought of as idents, or a path?
        Table {
            table_name: self.table_name.clone(),
            schema_name: self.schema_name.clone(),
        }
    }
}

impl<V: SQLVisitor> VisitSQL<V> for TableWithJoins {
    fn visit_sql(&self, visitor: &V) -> Self {
        TableWithJoins {
            relation: self.relation.visit_sql(visitor),
            joins: self.joins.visit_sql(visitor),
        }
    }
}
impl<V: SQLVisitor> VisitSQL<V> for Join {
    fn visit_sql(&self, visitor: &V) -> Self {
        Join {
            relation: self.relation.visit_sql(visitor),
            join_operator: self.join_operator.visit_sql(visitor),
        }
    }
}

impl<V: SQLVisitor> VisitSQL<V> for JoinOperator {
    fn visit_sql(&self, visitor: &V) -> Self {
        use JoinOperator::*;
        match self {
            Inner(c) => Inner(c.visit_sql(visitor)),
            LeftOuter(c) => LeftOuter(c.visit_sql(visitor)),
            RightOuter(c) => RightOuter(c.visit_sql(visitor)),
            FullOuter(c) => FullOuter(c.visit_sql(visitor)),
            CrossJoin => CrossJoin,
            LeftSemi(c) => LeftSemi(c.visit_sql(visitor)),
            RightSemi(c) => RightSemi(c.visit_sql(visitor)),
            LeftAnti(c) => LeftAnti(c.visit_sql(visitor)),
            RightAnti(c) => RightAnti(c.visit_sql(visitor)),
            CrossApply => CrossApply,
            OuterApply => OuterApply,
        }
    }
}

impl<V: SQLVisitor> VisitSQL<V> for JoinConstraint {
    fn visit_sql(&self, visitor: &V) -> Self {
        use JoinConstraint::*;
        match self {
            On(e) => On(e.visit_sql(visitor)),
            Using(v) => Using(v.visit_sql(visitor)),
            Natural => Natural,
            None => None,
        }
    }
}

impl<V: SQLVisitor> VisitSQL<V> for TableFactor {
    fn visit_sql(&self, visitor: &V) -> Self {
        if let Some(t) = visitor.visit_sqltable(self) {
            return t;
        }

        use TableFactor::*;
        match self {
            Table {
                name,
                alias,
                args,
                with_hints,
            } => Table {
                name: name.visit_sql(visitor),
                alias: alias.visit_sql(visitor),
                args: args.visit_sql(visitor),
                with_hints: with_hints.visit_sql(visitor),
            },
            Derived {
                lateral,
                subquery,
                alias,
            } => Derived {
                lateral: *lateral,
                subquery: subquery.visit_sql(visitor),
                alias: alias.visit_sql(visitor),
            },
            TableFunction { expr, alias } => TableFunction {
                expr: expr.visit_sql(visitor),
                alias: alias.visit_sql(visitor),
            },
            UNNEST {
                alias,
                array_expr,
                with_offset,
                with_offset_alias,
            } => UNNEST {
                alias: alias.visit_sql(visitor),
                array_expr: array_expr.visit_sql(visitor),
                with_offset: *with_offset,
                with_offset_alias: with_offset_alias.visit_sql(visitor),
            },
            NestedJoin {
                table_with_joins,
                alias,
            } => NestedJoin {
                table_with_joins: table_with_joins.visit_sql(visitor),
                alias: alias.visit_sql(visitor),
            },
        }
    }
}

impl<V: SQLVisitor> VisitSQL<V> for LateralView {
    fn visit_sql(&self, visitor: &V) -> Self {
        LateralView {
            lateral_view: self.lateral_view.visit_sql(visitor),
            lateral_view_name: self.lateral_view_name.visit_sql(visitor),
            lateral_col_alias: self.lateral_col_alias.visit_sql(visitor),
            outer: self.outer,
        }
    }
}

impl<V: SQLVisitor> VisitSQL<V> for FunctionArg {
    fn visit_sql(&self, visitor: &V) -> Self {
        use FunctionArg::*;
        match self {
            Named { name, arg } => Named {
                name: name.visit_sql(visitor),
                arg: arg.visit_sql(visitor),
            },
            Unnamed(arg) => Unnamed(arg.visit_sql(visitor)),
        }
    }
}

impl<V: SQLVisitor> VisitSQL<V> for FunctionArgExpr {
    fn visit_sql(&self, visitor: &V) -> Self {
        use FunctionArgExpr::*;
        match self {
            Expr(e) => Expr(e.visit_sql(visitor)),
            QualifiedWildcard(name) => QualifiedWildcard(name.visit_sql(visitor)),
            Wildcard => Wildcard,
        }
    }
}

impl<V: SQLVisitor> VisitSQL<V> for SelectItem {
    fn visit_sql(&self, visitor: &V) -> Self {
        use SelectItem::*;
        match self {
            UnnamedExpr(e) => UnnamedExpr(e.visit_sql(visitor)),
            ExprWithAlias { expr, alias } => ExprWithAlias {
                expr: expr.visit_sql(visitor),
                alias: alias.clone(), // Do not visit the alias -- changing it can bork the type
            },
            QualifiedWildcard(name, options) => {
                QualifiedWildcard(name.visit_sql(visitor), options.visit_sql(visitor))
            }
            Wildcard(options) => Wildcard(options.visit_sql(visitor)),
        }
    }
}

impl<V: SQLVisitor> VisitSQL<V> for WildcardAdditionalOptions {
    fn visit_sql(&self, visitor: &V) -> Self {
        WildcardAdditionalOptions {
            opt_exclude: self.opt_exclude.visit_sql(visitor),
            opt_except: self.opt_except.visit_sql(visitor),
            opt_rename: self.opt_rename.visit_sql(visitor),
        }
    }
}

impl<V: SQLVisitor> VisitSQL<V> for ExcludeSelectItem {
    fn visit_sql(&self, visitor: &V) -> Self {
        use ExcludeSelectItem::*;
        match self {
            Single(name) => Single(name.visit_sql(visitor)),
            Multiple(names) => Multiple(names.visit_sql(visitor)),
        }
    }
}

impl<V: SQLVisitor> VisitSQL<V> for ExceptSelectItem {
    fn visit_sql(&self, visitor: &V) -> Self {
        ExceptSelectItem {
            first_element: self.first_element.visit_sql(visitor),
            additional_elements: self.additional_elements.visit_sql(visitor),
        }
    }
}

impl<V: SQLVisitor> VisitSQL<V> for RenameSelectItem {
    fn visit_sql(&self, visitor: &V) -> Self {
        use RenameSelectItem::*;
        match self {
            Single(name) => Single(name.visit_sql(visitor)),
            Multiple(names) => Multiple(names.visit_sql(visitor)),
        }
    }
}

impl<V: SQLVisitor> VisitSQL<V> for IdentWithAlias {
    fn visit_sql(&self, visitor: &V) -> Self {
        IdentWithAlias {
            ident: self.ident.visit_sql(visitor),
            alias: self.alias.visit_sql(visitor), // NOTE: We may not want to visit the alias
        }
    }
}

impl<V: SQLVisitor> VisitSQL<V> for ObjectName {
    fn visit_sql(&self, visitor: &V) -> Self {
        if let Some(path) = visitor.visit_sqlpath(&self.0) {
            return ObjectName(path);
        }
        ObjectName(self.0.visit_sql(visitor))
    }
}

impl<V: SQLVisitor> VisitSQL<V> for Cte {
    fn visit_sql(&self, visitor: &V) -> Self {
        Cte {
            alias: self.alias.visit_sql(visitor),
            query: self.query.visit_sql(visitor),
            from: self.from.visit_sql(visitor),
        }
    }
}

impl<V: SQLVisitor> VisitSQL<V> for Ident {
    fn visit_sql(&self, visitor: &V) -> Self {
        if let Some(i) = visitor.visit_sqlident(self) {
            return i;
        }
        self.clone()
    }
}

impl<V: SQLVisitor> VisitSQL<V> for TableAlias {
    fn visit_sql(&self, visitor: &V) -> Self {
        TableAlias {
            name: self.name.visit_sql(visitor),
            columns: self.columns.visit_sql(visitor),
        }
    }
}

impl<V: SQLVisitor> VisitSQL<V> for OrderByExpr {
    fn visit_sql(&self, visitor: &V) -> Self {
        OrderByExpr {
            expr: self.expr.visit_sql(visitor),
            asc: self.asc.clone(),
            nulls_first: self.nulls_first.clone(),
        }
    }
}

impl<V: SQLVisitor> VisitSQL<V> for SqlOption {
    fn visit_sql(&self, visitor: &V) -> Self {
        SqlOption {
            name: self.name.visit_sql(visitor),
            value: self.value.clone(),
        }
    }
}

impl<V: SQLVisitor, T: VisitSQL<V>> VisitSQL<V> for Vec<T> {
    fn visit_sql(&self, visitor: &V) -> Self {
        self.iter().map(|o| o.visit_sql(visitor)).collect()
    }
}

impl<V: SQLVisitor, T: VisitSQL<V>> VisitSQL<V> for Option<T> {
    fn visit_sql(&self, visitor: &V) -> Self {
        self.as_ref().map(|x| x.visit_sql(visitor))
    }
}

impl<V: SQLVisitor, T: VisitSQL<V>> VisitSQL<V> for Box<T> {
    fn visit_sql(&self, visitor: &V) -> Self {
        Box::new(self.as_ref().visit_sql(visitor))
    }
}

impl<V: SQLVisitor, T: VisitSQL<V>> VisitSQL<V> for Located<T> {
    fn visit_sql(&self, visitor: &V) -> Self {
        let range = self.location().clone();
        Located::new(self.get().visit_sql(visitor), range)
    }
}

impl<V: SQLVisitor> VisitSQL<V> for schema::SQLBody {
    fn visit_sql(&self, visitor: &V) -> Self {
        match self {
            schema::SQLBody::Expr(e) => schema::SQLBody::Expr(e.visit_sql(visitor)),
            schema::SQLBody::Query(q) => schema::SQLBody::Query(q.visit_sql(visitor)),
            schema::SQLBody::Table(t) => schema::SQLBody::Table(t.visit_sql(visitor)),
        }
    }
}

#[async_trait]
impl<V: Visitor<schema::CRef<schema::MType>> + Sync> Visit<V, schema::CRef<schema::MType>>
    for schema::Expr<schema::CRef<schema::MType>>
{
    async fn visit(&self, visitor: &V) -> Result<Self> {
        use schema::*;

        if let Some(e) = visitor.visit_expr(&self).await? {
            return Ok(e);
        }

        Ok(match self {
            Expr::SQL(e, url) => {
                let SQL { names, body } = e.as_ref();
                let mut params = BTreeMap::new();
                for (name, param) in &names.params {
                    params.insert(name.clone(), param.visit(visitor).await?);
                }
                Expr::SQL(
                    Arc::new(SQL {
                        names: SQLNames {
                            params,
                            unbound: names.unbound.clone(),
                        },
                        body: match body {
                            SQLBody::Expr(expr) => SQLBody::Expr(expr.visit_sql(visitor)),
                            SQLBody::Query(query) => SQLBody::Query(query.visit_sql(visitor)),
                            SQLBody::Table(table) => SQLBody::Table(table.visit_sql(visitor)),
                        },
                    }),
                    url.clone(),
                )
            }
            Expr::Fn(FnExpr { inner_schema, body }) => Expr::Fn(FnExpr {
                inner_schema: inner_schema.clone(),
                body: match body {
                    FnBody::SQLBuiltin => FnBody::SQLBuiltin,
                    FnBody::Expr(expr) => FnBody::Expr(Arc::new(expr.visit(visitor).await?)),
                },
            }),
            Expr::FnCall(FnCallExpr {
                func,
                args,
                ctx_folder,
            }) => {
                let mut visited_args = Vec::new();
                for a in args {
                    visited_args.push(a.visit(visitor).await?);
                }
                Expr::FnCall(FnCallExpr {
                    func: Arc::new(func.visit(visitor).await?),
                    args: visited_args,
                    ctx_folder: ctx_folder.clone(),
                })
            }
            Expr::SchemaEntry(e) => {
                let expr = (&e.expr).await?.read()?.clone();
                expr.visit(visitor).await?
            }
            Expr::NativeFn(f) => Expr::NativeFn(f.clone()),
            Expr::ContextRef(r) => Expr::ContextRef(r.clone()),
            Expr::Connection(u) => Expr::Connection(u.clone()),
            Expr::Materialize(MaterializeExpr {
                key,
                expr,
                url,
                decl_name,
                inlined,
            }) => Expr::Materialize(MaterializeExpr {
                key: key.clone(),
                expr: expr.visit(visitor).await?,
                url: url.clone(),
                decl_name: decl_name.clone(),
                inlined: inlined.clone(),
            }),
            Expr::Unknown => Expr::Unknown,
        })
    }
}

#[async_trait]
impl<V: Visitor<schema::CRef<schema::MType>> + Sync> Visit<V, schema::CRef<schema::MType>>
    for schema::TypedExpr<schema::CRef<schema::MType>>
{
    async fn visit(&self, visitor: &V) -> Result<Self> {
        Ok(schema::TypedExpr {
            type_: self.type_.clone(),
            expr: Arc::new(self.expr.visit(visitor).await?),
        })
    }
}
