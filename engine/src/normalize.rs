use sqlparser::ast::*;
use std::collections::HashMap;

pub trait TreeNormalizer {
    fn quote_style(&self) -> Option<char>;
    fn params(&self) -> &HashMap<String, String>;
}

pub trait Normalize<N>
where
    N: TreeNormalizer,
{
    fn normalize(&self, normalizer: &N) -> Self;
}

impl<N: TreeNormalizer> Normalize<N> for Query {
    fn normalize(&self, normalizer: &N) -> Self {
        Query {
            with: self.with.as_ref().map(|w| With {
                recursive: w.recursive,
                cte_tables: w.cte_tables.normalize(normalizer),
            }),
            body: self.body.normalize(normalizer),
            order_by: self.order_by.normalize(normalizer),
            limit: self.limit.normalize(normalizer),
            offset: self.offset.as_ref().map(|o| Offset {
                value: o.value.normalize(normalizer),
                rows: o.rows.clone(),
            }),
            fetch: self.fetch.as_ref().map(|f| Fetch {
                with_ties: f.with_ties,
                percent: f.percent,
                quantity: f.quantity.normalize(normalizer),
            }),
            lock: self.lock.clone(),
        }
    }
}

impl<N: TreeNormalizer> Normalize<N> for Expr {
    fn normalize(&self, normalizer: &N) -> Self {
        use Expr::*;
        match self {
            Identifier(x) => Identifier(x.normalize(normalizer)),
            CompoundIdentifier(v) => CompoundIdentifier(v.clone()),
            JsonAccess {
                left,
                operator,
                right,
            } => JsonAccess {
                left: left.normalize(normalizer),
                operator: operator.clone(),
                right: right.normalize(normalizer),
            },
            CompositeAccess { expr, key } => CompositeAccess {
                expr: expr.normalize(normalizer),
                key: key.normalize(normalizer),
            },
            IsFalse(e) => IsFalse(e.normalize(normalizer)),
            IsNotFalse(e) => IsNotFalse(e.normalize(normalizer)),
            IsTrue(e) => IsTrue(e.normalize(normalizer)),
            IsNotTrue(e) => IsNotTrue(e.normalize(normalizer)),
            IsNull(e) => IsNull(e.normalize(normalizer)),

            IsNotNull(e) => IsNotNull(e.normalize(normalizer)),
            IsUnknown(e) => IsUnknown(e.normalize(normalizer)),
            IsNotUnknown(e) => IsNotUnknown(e.normalize(normalizer)),

            IsDistinctFrom(e1, e2) => {
                IsDistinctFrom(e1.normalize(normalizer), e2.normalize(normalizer))
            }
            IsNotDistinctFrom(e1, e2) => {
                IsNotDistinctFrom(e1.normalize(normalizer), e2.normalize(normalizer))
            }
            InList {
                expr,
                list,
                negated,
            } => InList {
                expr: expr.normalize(normalizer),
                list: list.normalize(normalizer),
                negated: *negated,
            },
            InSubquery {
                expr,
                subquery,
                negated,
            } => InSubquery {
                expr: expr.normalize(normalizer),
                subquery: subquery.normalize(normalizer),
                negated: *negated,
            },
            InUnnest {
                expr,
                array_expr,
                negated,
            } => InUnnest {
                expr: expr.normalize(normalizer),
                array_expr: array_expr.normalize(normalizer),
                negated: *negated,
            },
            Between {
                expr,
                negated,
                low,
                high,
            } => Between {
                expr: expr.normalize(normalizer),
                negated: *negated,
                low: low.normalize(normalizer),
                high: high.normalize(normalizer),
            },
            BinaryOp { left, op, right } => BinaryOp {
                left: left.normalize(normalizer),
                op: op.clone(),
                right: right.normalize(normalizer),
            },
            Like {
                negated,
                expr,
                pattern,
                escape_char,
            } => Like {
                negated: *negated,
                expr: expr.normalize(normalizer),
                pattern: pattern.normalize(normalizer),
                escape_char: escape_char.clone(),
            },
            ILike {
                negated,
                expr,
                pattern,
                escape_char,
            } => ILike {
                negated: *negated,
                expr: expr.normalize(normalizer),
                pattern: pattern.normalize(normalizer),
                escape_char: escape_char.clone(),
            },
            SimilarTo {
                negated,
                expr,
                pattern,
                escape_char,
            } => SimilarTo {
                negated: *negated,
                expr: expr.normalize(normalizer),
                pattern: pattern.normalize(normalizer),
                escape_char: escape_char.clone(),
            },
            AnyOp(e) => AnyOp(e.normalize(normalizer)),
            AllOp(e) => AllOp(e.normalize(normalizer)),
            UnaryOp { op, expr } => UnaryOp {
                op: op.clone(),
                expr: expr.normalize(normalizer),
            },
            Cast { expr, data_type } => Cast {
                expr: expr.normalize(normalizer),
                data_type: data_type.clone(),
            },
            TryCast { expr, data_type } => TryCast {
                expr: expr.normalize(normalizer),
                data_type: data_type.clone(),
            },
            SafeCast { expr, data_type } => SafeCast {
                expr: expr.normalize(normalizer),
                data_type: data_type.clone(),
            },
            AtTimeZone {
                timestamp,
                time_zone,
            } => AtTimeZone {
                timestamp: timestamp.normalize(normalizer),
                time_zone: time_zone.clone(),
            },
            Extract { field, expr } => Extract {
                field: field.clone(),
                expr: expr.normalize(normalizer),
            },
            Ceil { field, expr } => Ceil {
                field: field.clone(),
                expr: expr.normalize(normalizer),
            },
            Floor { field, expr } => Floor {
                field: field.clone(),
                expr: expr.normalize(normalizer),
            },
            Position { expr, r#in } => Position {
                expr: expr.normalize(normalizer),
                r#in: r#in.normalize(normalizer),
            },
            Substring {
                expr,
                substring_from,
                substring_for,
            } => Substring {
                expr: expr.normalize(normalizer),
                substring_from: substring_from.normalize(normalizer),
                substring_for: substring_for.normalize(normalizer),
            },
            Trim {
                expr,
                // ([BOTH | LEADING | TRAILING]
                trim_where,
                trim_what,
            } => Trim {
                expr: expr.normalize(normalizer),
                trim_where: trim_where.clone(),
                trim_what: trim_what.normalize(normalizer),
            },
            Overlay {
                expr,
                overlay_what,
                overlay_from,
                overlay_for,
            } => Overlay {
                expr: expr.normalize(normalizer),
                overlay_what: overlay_what.normalize(normalizer),
                overlay_from: overlay_from.normalize(normalizer),
                overlay_for: overlay_for.normalize(normalizer),
            },
            Collate { expr, collation } => Collate {
                expr: expr.normalize(normalizer),
                collation: collation.normalize(normalizer),
            },
            Nested(e) => Nested(e.normalize(normalizer)),
            Value(v) => Value(v.clone()),
            TypedString { data_type, value } => TypedString {
                data_type: data_type.clone(),
                value: value.clone(),
            },
            MapAccess { column, keys } => MapAccess {
                column: column.normalize(normalizer),
                keys: keys.normalize(normalizer),
            },
            Function(f) => Function(f.normalize(normalizer)),
            AggregateExpressionWithFilter { expr, filter } => AggregateExpressionWithFilter {
                expr: expr.normalize(normalizer),
                filter: filter.normalize(normalizer),
            },
            Case {
                operand,
                conditions,
                results,
                else_result,
            } => Case {
                operand: operand.normalize(normalizer),
                conditions: conditions.normalize(normalizer),
                results: results.normalize(normalizer),
                else_result: else_result.normalize(normalizer),
            },
            Exists { subquery, negated } => Exists {
                subquery: subquery.normalize(normalizer),
                negated: *negated,
            },
            Subquery(q) => Subquery(q.normalize(normalizer)),
            ArraySubquery(q) => ArraySubquery(q.normalize(normalizer)),
            ListAgg(la) => ListAgg(la.normalize(normalizer)),
            GroupingSets(gs) => GroupingSets(gs.normalize(normalizer)),
            Cube(c) => Cube(c.normalize(normalizer)),
            Rollup(r) => Rollup(r.normalize(normalizer)),
            Tuple(t) => Tuple(t.normalize(normalizer)),
            ArrayIndex { obj, indexes } => ArrayIndex {
                obj: obj.normalize(normalizer),
                indexes: indexes.normalize(normalizer),
            },
            Array(sqlparser::ast::Array { elem, named }) => Array(sqlparser::ast::Array {
                elem: elem.normalize(normalizer),
                named: *named,
            }),
            Interval {
                value,
                leading_field,
                leading_precision,
                last_field,
                fractional_seconds_precision,
            } => Interval {
                value: value.normalize(normalizer),
                leading_field: leading_field.clone(),
                leading_precision: leading_precision.clone(),
                last_field: last_field.clone(),
                fractional_seconds_precision: fractional_seconds_precision.clone(),
            },
        }
    }
}

impl<N: TreeNormalizer> Normalize<N> for Function {
    fn normalize(&self, normalizer: &N) -> Self {
        Function {
            name: self.name.normalize(normalizer),
            args: self.args.normalize(normalizer),
            over: self.over.normalize(normalizer),
            distinct: self.distinct,
            special: self.special,
        }
    }
}

impl<N: TreeNormalizer> Normalize<N> for WindowSpec {
    fn normalize(&self, normalizer: &N) -> Self {
        WindowSpec {
            partition_by: self.partition_by.normalize(normalizer),
            order_by: self.order_by.normalize(normalizer),
            window_frame: self.window_frame.normalize(normalizer),
        }
    }
}

impl<N: TreeNormalizer> Normalize<N> for WindowFrame {
    fn normalize(&self, normalizer: &N) -> Self {
        WindowFrame {
            units: self.units.clone(),
            start_bound: self.start_bound.normalize(normalizer),
            end_bound: self.end_bound.normalize(normalizer),
        }
    }
}

impl<N: TreeNormalizer> Normalize<N> for WindowFrameBound {
    fn normalize(&self, normalizer: &N) -> Self {
        use WindowFrameBound::*;
        match self {
            CurrentRow => CurrentRow,
            Preceding(e) => Preceding(e.normalize(normalizer)),
            Following(e) => Following(e.normalize(normalizer)),
        }
    }
}

impl<N: TreeNormalizer> Normalize<N> for ListAgg {
    fn normalize(&self, normalizer: &N) -> Self {
        ListAgg {
            distinct: self.distinct,
            expr: self.expr.normalize(normalizer),
            separator: self.separator.normalize(normalizer),
            on_overflow: self.on_overflow.normalize(normalizer),
            within_group: self.within_group.normalize(normalizer),
        }
    }
}

impl<N: TreeNormalizer> Normalize<N> for ListAggOnOverflow {
    fn normalize(&self, normalizer: &N) -> Self {
        use ListAggOnOverflow::*;
        match self {
            Error => Error,
            Truncate { filler, with_count } => Truncate {
                filler: filler.normalize(normalizer),
                with_count: *with_count,
            },
        }
    }
}

impl<N: TreeNormalizer> Normalize<N> for SetExpr {
    fn normalize(&self, normalizer: &N) -> Self {
        use SetExpr::*;
        match self {
            Select(s) => Select(s.normalize(normalizer)),
            Query(q) => Query(q.normalize(normalizer)),
            SetOperation {
                op,
                all,
                left,
                right,
            } => SetOperation {
                op: op.clone(),
                all: all.clone(),
                left: left.normalize(normalizer),
                right: right.normalize(normalizer),
            },
            Values(sqlparser::ast::Values(v)) => {
                Values(sqlparser::ast::Values(v.normalize(normalizer)))
            }
            Insert(_) => panic!("Unimplemented: INSERT statements"),
        }
    }
}

impl<N: TreeNormalizer> Normalize<N> for Select {
    fn normalize(&self, normalizer: &N) -> Self {
        Select {
            distinct: self.distinct,
            top: self.top.as_ref().map(|t| Top {
                with_ties: t.with_ties,
                percent: t.percent,
                quantity: t.quantity.normalize(normalizer),
            }),
            projection: self.projection.normalize(normalizer),
            into: self.into.as_ref().map(|f| SelectInto {
                temporary: f.temporary,
                unlogged: f.unlogged,
                table: f.table,
                name: f.name.normalize(normalizer),
            }),
            from: self.from.normalize(normalizer),
            lateral_views: self.lateral_views.normalize(normalizer),
            selection: self.selection.normalize(normalizer),
            group_by: self.group_by.normalize(normalizer),
            cluster_by: self.cluster_by.normalize(normalizer),
            distribute_by: self.distribute_by.normalize(normalizer),
            sort_by: self.sort_by.normalize(normalizer),
            having: self.having.normalize(normalizer),
            qualify: self.qualify.normalize(normalizer),
        }
    }
}

impl<N: TreeNormalizer> Normalize<N> for TableWithJoins {
    fn normalize(&self, normalizer: &N) -> Self {
        TableWithJoins {
            relation: self.relation.normalize(normalizer),
            joins: self.joins.normalize(normalizer),
        }
    }
}
impl<N: TreeNormalizer> Normalize<N> for Join {
    fn normalize(&self, normalizer: &N) -> Self {
        Join {
            relation: self.relation.normalize(normalizer),
            join_operator: self.join_operator.normalize(normalizer),
        }
    }
}

impl<N: TreeNormalizer> Normalize<N> for JoinOperator {
    fn normalize(&self, normalizer: &N) -> Self {
        use JoinOperator::*;
        match self {
            Inner(c) => Inner(c.normalize(normalizer)),
            LeftOuter(c) => LeftOuter(c.normalize(normalizer)),
            RightOuter(c) => RightOuter(c.normalize(normalizer)),
            FullOuter(c) => FullOuter(c.normalize(normalizer)),
            CrossJoin => CrossJoin,
            CrossApply => CrossApply,
            OuterApply => OuterApply,
        }
    }
}

impl<N: TreeNormalizer> Normalize<N> for JoinConstraint {
    fn normalize(&self, normalizer: &N) -> Self {
        use JoinConstraint::*;
        match self {
            On(e) => On(e.normalize(normalizer)),
            Using(v) => Using(v.normalize(normalizer)),
            Natural => Natural,
            None => None,
        }
    }
}

impl<N: TreeNormalizer> Normalize<N> for TableFactor {
    fn normalize(&self, normalizer: &N) -> Self {
        use TableFactor::*;
        match self {
            Table {
                name,
                alias,
                args,
                with_hints,
            } => Table {
                name: name.normalize(normalizer),
                alias: alias.normalize(normalizer),
                args: args.normalize(normalizer),
                with_hints: with_hints.normalize(normalizer),
            },
            Derived {
                lateral,
                subquery,
                alias,
            } => Derived {
                lateral: *lateral,
                subquery: subquery.normalize(normalizer),
                alias: alias.normalize(normalizer),
            },
            TableFunction { expr, alias } => TableFunction {
                expr: expr.normalize(normalizer),
                alias: alias.normalize(normalizer),
            },
            UNNEST {
                alias,
                array_expr,
                with_offset,
                with_offset_alias,
            } => UNNEST {
                alias: alias.normalize(normalizer),
                array_expr: array_expr.normalize(normalizer),
                with_offset: *with_offset,
                with_offset_alias: with_offset_alias.normalize(normalizer),
            },
            NestedJoin {
                table_with_joins,
                alias,
            } => NestedJoin {
                table_with_joins: table_with_joins.normalize(normalizer),
                alias: alias.normalize(normalizer),
            },
        }
    }
}

impl<N: TreeNormalizer> Normalize<N> for LateralView {
    fn normalize(&self, normalizer: &N) -> Self {
        LateralView {
            lateral_view: self.lateral_view.normalize(normalizer),
            lateral_view_name: self.lateral_view_name.normalize(normalizer),
            lateral_col_alias: self.lateral_col_alias.normalize(normalizer),
            outer: self.outer,
        }
    }
}

impl<N: TreeNormalizer> Normalize<N> for FunctionArg {
    fn normalize(&self, normalizer: &N) -> Self {
        use FunctionArg::*;
        match self {
            Named { name, arg } => Named {
                name: name.normalize(normalizer),
                arg: arg.normalize(normalizer),
            },
            Unnamed(arg) => Unnamed(arg.normalize(normalizer)),
        }
    }
}

impl<N: TreeNormalizer> Normalize<N> for FunctionArgExpr {
    fn normalize(&self, normalizer: &N) -> Self {
        use FunctionArgExpr::*;
        match self {
            Expr(e) => Expr(e.normalize(normalizer)),
            QualifiedWildcard(name) => QualifiedWildcard(name.normalize(normalizer)),
            Wildcard => Wildcard,
        }
    }
}

impl<N: TreeNormalizer> Normalize<N> for SelectItem {
    fn normalize(&self, normalizer: &N) -> Self {
        use SelectItem::*;
        match self {
            UnnamedExpr(e) => UnnamedExpr(e.normalize(normalizer)),
            ExprWithAlias { expr, alias } => ExprWithAlias {
                expr: expr.normalize(normalizer),
                alias: alias.normalize(normalizer),
            },
            QualifiedWildcard(name) => QualifiedWildcard(name.normalize(normalizer)),
            Wildcard => Wildcard,
        }
    }
}

impl<N: TreeNormalizer> Normalize<N> for ObjectName {
    fn normalize(&self, normalizer: &N) -> Self {
        ObjectName(self.0.normalize(normalizer))
    }
}

impl<N: TreeNormalizer> Normalize<N> for Cte {
    fn normalize(&self, normalizer: &N) -> Self {
        Cte {
            alias: self.alias.normalize(normalizer),
            query: self.query.normalize(normalizer),
            from: self.from.normalize(normalizer),
        }
    }
}

impl<N: TreeNormalizer> Normalize<N> for Ident {
    fn normalize(&self, normalizer: &N) -> Self {
        let params = normalizer.params();
        match params.get(&self.value) {
            Some(name) => Ident {
                value: name.clone(),
                quote_style: None,
            },
            None => Ident {
                value: self.value.clone(),
                quote_style: normalizer.quote_style(),
            },
        }
    }
}

impl<N: TreeNormalizer> Normalize<N> for TableAlias {
    fn normalize(&self, normalizer: &N) -> Self {
        TableAlias {
            name: self.name.normalize(normalizer),
            columns: self.columns.normalize(normalizer),
        }
    }
}

impl<N: TreeNormalizer> Normalize<N> for OrderByExpr {
    fn normalize(&self, normalizer: &N) -> Self {
        OrderByExpr {
            expr: self.expr.normalize(normalizer),
            asc: self.asc.clone(),
            nulls_first: self.nulls_first.clone(),
        }
    }
}

impl<N: TreeNormalizer, T: Normalize<N>> Normalize<N> for Vec<T> {
    fn normalize(&self, normalizer: &N) -> Self {
        self.iter().map(|o| o.normalize(normalizer)).collect()
    }
}

impl<N: TreeNormalizer, T: Normalize<N>> Normalize<N> for Option<T> {
    fn normalize(&self, normalizer: &N) -> Self {
        self.as_ref().map(|x| x.normalize(normalizer))
    }
}

impl<N: TreeNormalizer, T: Normalize<N>> Normalize<N> for Box<T> {
    fn normalize(&self, normalizer: &N) -> Self {
        Box::new(self.as_ref().normalize(normalizer))
    }
}
