use sqlparser::ast::*;

pub trait Visitor {
    fn visit_expr(&self, _expr: &Expr) -> Option<Expr> {
        None
    }

    fn visit_query(&self, _query: &Query) -> Option<Query> {
        None
    }

    fn visit_ident(&self, _ident: &Ident) -> Option<Ident> {
        None
    }
}

pub trait Visit<V>
where
    V: Visitor,
{
    fn visit(&self, visitor: &V) -> Self;
}

impl<V: Visitor> Visit<V> for Query {
    fn visit(&self, visitor: &V) -> Self {
        if let Some(q) = visitor.visit_query(self) {
            return q;
        }

        Query {
            with: self.with.as_ref().map(|w| With {
                recursive: w.recursive,
                cte_tables: w.cte_tables.visit(visitor),
            }),
            body: self.body.visit(visitor),
            order_by: self.order_by.visit(visitor),
            limit: self.limit.visit(visitor),
            offset: self.offset.as_ref().map(|o| Offset {
                value: o.value.visit(visitor),
                rows: o.rows.clone(),
            }),
            fetch: self.fetch.as_ref().map(|f| Fetch {
                with_ties: f.with_ties,
                percent: f.percent,
                quantity: f.quantity.visit(visitor),
            }),
            lock: self.lock.clone(),
        }
    }
}

impl<V: Visitor> Visit<V> for Expr {
    fn visit(&self, visitor: &V) -> Self {
        if let Some(e) = visitor.visit_expr(self) {
            return e;
        }

        use Expr::*;
        match self {
            Identifier(x) => Identifier(x.visit(visitor)),
            CompoundIdentifier(v) => CompoundIdentifier(v.clone()),
            JsonAccess {
                left,
                operator,
                right,
            } => JsonAccess {
                left: left.visit(visitor),
                operator: operator.clone(),
                right: right.visit(visitor),
            },
            CompositeAccess { expr, key } => CompositeAccess {
                expr: expr.visit(visitor),
                key: key.visit(visitor),
            },
            IsFalse(e) => IsFalse(e.visit(visitor)),
            IsNotFalse(e) => IsNotFalse(e.visit(visitor)),
            IsTrue(e) => IsTrue(e.visit(visitor)),
            IsNotTrue(e) => IsNotTrue(e.visit(visitor)),
            IsNull(e) => IsNull(e.visit(visitor)),

            IsNotNull(e) => IsNotNull(e.visit(visitor)),
            IsUnknown(e) => IsUnknown(e.visit(visitor)),
            IsNotUnknown(e) => IsNotUnknown(e.visit(visitor)),

            IsDistinctFrom(e1, e2) => IsDistinctFrom(e1.visit(visitor), e2.visit(visitor)),
            IsNotDistinctFrom(e1, e2) => IsNotDistinctFrom(e1.visit(visitor), e2.visit(visitor)),
            InList {
                expr,
                list,
                negated,
            } => InList {
                expr: expr.visit(visitor),
                list: list.visit(visitor),
                negated: *negated,
            },
            InSubquery {
                expr,
                subquery,
                negated,
            } => InSubquery {
                expr: expr.visit(visitor),
                subquery: subquery.visit(visitor),
                negated: *negated,
            },
            InUnnest {
                expr,
                array_expr,
                negated,
            } => InUnnest {
                expr: expr.visit(visitor),
                array_expr: array_expr.visit(visitor),
                negated: *negated,
            },
            Between {
                expr,
                negated,
                low,
                high,
            } => Between {
                expr: expr.visit(visitor),
                negated: *negated,
                low: low.visit(visitor),
                high: high.visit(visitor),
            },
            BinaryOp { left, op, right } => BinaryOp {
                left: left.visit(visitor),
                op: op.clone(),
                right: right.visit(visitor),
            },
            Like {
                negated,
                expr,
                pattern,
                escape_char,
            } => Like {
                negated: *negated,
                expr: expr.visit(visitor),
                pattern: pattern.visit(visitor),
                escape_char: escape_char.clone(),
            },
            ILike {
                negated,
                expr,
                pattern,
                escape_char,
            } => ILike {
                negated: *negated,
                expr: expr.visit(visitor),
                pattern: pattern.visit(visitor),
                escape_char: escape_char.clone(),
            },
            SimilarTo {
                negated,
                expr,
                pattern,
                escape_char,
            } => SimilarTo {
                negated: *negated,
                expr: expr.visit(visitor),
                pattern: pattern.visit(visitor),
                escape_char: escape_char.clone(),
            },
            AnyOp(e) => AnyOp(e.visit(visitor)),
            AllOp(e) => AllOp(e.visit(visitor)),
            UnaryOp { op, expr } => UnaryOp {
                op: op.clone(),
                expr: expr.visit(visitor),
            },
            Cast { expr, data_type } => Cast {
                expr: expr.visit(visitor),
                data_type: data_type.clone(),
            },
            TryCast { expr, data_type } => TryCast {
                expr: expr.visit(visitor),
                data_type: data_type.clone(),
            },
            SafeCast { expr, data_type } => SafeCast {
                expr: expr.visit(visitor),
                data_type: data_type.clone(),
            },
            AtTimeZone {
                timestamp,
                time_zone,
            } => AtTimeZone {
                timestamp: timestamp.visit(visitor),
                time_zone: time_zone.clone(),
            },
            Extract { field, expr } => Extract {
                field: field.clone(),
                expr: expr.visit(visitor),
            },
            Ceil { field, expr } => Ceil {
                field: field.clone(),
                expr: expr.visit(visitor),
            },
            Floor { field, expr } => Floor {
                field: field.clone(),
                expr: expr.visit(visitor),
            },
            Position { expr, r#in } => Position {
                expr: expr.visit(visitor),
                r#in: r#in.visit(visitor),
            },
            Substring {
                expr,
                substring_from,
                substring_for,
            } => Substring {
                expr: expr.visit(visitor),
                substring_from: substring_from.visit(visitor),
                substring_for: substring_for.visit(visitor),
            },
            Trim {
                expr,
                // ([BOTH | LEADING | TRAILING]
                trim_where,
                trim_what,
            } => Trim {
                expr: expr.visit(visitor),
                trim_where: trim_where.clone(),
                trim_what: trim_what.visit(visitor),
            },
            Overlay {
                expr,
                overlay_what,
                overlay_from,
                overlay_for,
            } => Overlay {
                expr: expr.visit(visitor),
                overlay_what: overlay_what.visit(visitor),
                overlay_from: overlay_from.visit(visitor),
                overlay_for: overlay_for.visit(visitor),
            },
            Collate { expr, collation } => Collate {
                expr: expr.visit(visitor),
                collation: collation.visit(visitor),
            },
            Nested(e) => Nested(e.visit(visitor)),
            Value(v) => Value(v.clone()),
            TypedString { data_type, value } => TypedString {
                data_type: data_type.clone(),
                value: value.clone(),
            },
            MapAccess { column, keys } => MapAccess {
                column: column.visit(visitor),
                keys: keys.visit(visitor),
            },
            Function(f) => Function(f.visit(visitor)),
            AggregateExpressionWithFilter { expr, filter } => AggregateExpressionWithFilter {
                expr: expr.visit(visitor),
                filter: filter.visit(visitor),
            },
            Case {
                operand,
                conditions,
                results,
                else_result,
            } => Case {
                operand: operand.visit(visitor),
                conditions: conditions.visit(visitor),
                results: results.visit(visitor),
                else_result: else_result.visit(visitor),
            },
            Exists { subquery, negated } => Exists {
                subquery: subquery.visit(visitor),
                negated: *negated,
            },
            Subquery(q) => Subquery(q.visit(visitor)),
            ArraySubquery(q) => ArraySubquery(q.visit(visitor)),
            ListAgg(la) => ListAgg(la.visit(visitor)),
            GroupingSets(gs) => GroupingSets(gs.visit(visitor)),
            Cube(c) => Cube(c.visit(visitor)),
            Rollup(r) => Rollup(r.visit(visitor)),
            Tuple(t) => Tuple(t.visit(visitor)),
            ArrayIndex { obj, indexes } => ArrayIndex {
                obj: obj.visit(visitor),
                indexes: indexes.visit(visitor),
            },
            Array(sqlparser::ast::Array { elem, named }) => Array(sqlparser::ast::Array {
                elem: elem.visit(visitor),
                named: *named,
            }),
            Interval {
                value,
                leading_field,
                leading_precision,
                last_field,
                fractional_seconds_precision,
            } => Interval {
                value: value.visit(visitor),
                leading_field: leading_field.clone(),
                leading_precision: leading_precision.clone(),
                last_field: last_field.clone(),
                fractional_seconds_precision: fractional_seconds_precision.clone(),
            },
        }
    }
}

impl<V: Visitor> Visit<V> for Function {
    fn visit(&self, visitor: &V) -> Self {
        Function {
            name: self.name.visit(visitor),
            args: self.args.visit(visitor),
            over: self.over.visit(visitor),
            distinct: self.distinct,
            special: self.special,
        }
    }
}

impl<V: Visitor> Visit<V> for WindowSpec {
    fn visit(&self, visitor: &V) -> Self {
        WindowSpec {
            partition_by: self.partition_by.visit(visitor),
            order_by: self.order_by.visit(visitor),
            window_frame: self.window_frame.visit(visitor),
        }
    }
}

impl<V: Visitor> Visit<V> for WindowFrame {
    fn visit(&self, visitor: &V) -> Self {
        WindowFrame {
            units: self.units.clone(),
            start_bound: self.start_bound.visit(visitor),
            end_bound: self.end_bound.visit(visitor),
        }
    }
}

impl<V: Visitor> Visit<V> for WindowFrameBound {
    fn visit(&self, visitor: &V) -> Self {
        use WindowFrameBound::*;
        match self {
            CurrentRow => CurrentRow,
            Preceding(e) => Preceding(e.visit(visitor)),
            Following(e) => Following(e.visit(visitor)),
        }
    }
}

impl<V: Visitor> Visit<V> for ListAgg {
    fn visit(&self, visitor: &V) -> Self {
        ListAgg {
            distinct: self.distinct,
            expr: self.expr.visit(visitor),
            separator: self.separator.visit(visitor),
            on_overflow: self.on_overflow.visit(visitor),
            within_group: self.within_group.visit(visitor),
        }
    }
}

impl<V: Visitor> Visit<V> for ListAggOnOverflow {
    fn visit(&self, visitor: &V) -> Self {
        use ListAggOnOverflow::*;
        match self {
            Error => Error,
            Truncate { filler, with_count } => Truncate {
                filler: filler.visit(visitor),
                with_count: *with_count,
            },
        }
    }
}

impl<V: Visitor> Visit<V> for SetExpr {
    fn visit(&self, visitor: &V) -> Self {
        use SetExpr::*;
        match self {
            Select(s) => Select(s.visit(visitor)),
            Query(q) => Query(q.visit(visitor)),
            SetOperation {
                op,
                all,
                left,
                right,
            } => SetOperation {
                op: op.clone(),
                all: all.clone(),
                left: left.visit(visitor),
                right: right.visit(visitor),
            },
            Values(sqlparser::ast::Values(v)) => Values(sqlparser::ast::Values(v.visit(visitor))),
            Insert(_) => panic!("Unimplemented: INSERT statements"),
        }
    }
}

impl<V: Visitor> Visit<V> for Select {
    fn visit(&self, visitor: &V) -> Self {
        Select {
            distinct: self.distinct,
            top: self.top.as_ref().map(|t| Top {
                with_ties: t.with_ties,
                percent: t.percent,
                quantity: t.quantity.visit(visitor),
            }),
            projection: self.projection.visit(visitor),
            into: self.into.as_ref().map(|f| SelectInto {
                temporary: f.temporary,
                unlogged: f.unlogged,
                table: f.table,
                name: f.name.visit(visitor),
            }),
            from: self.from.visit(visitor),
            lateral_views: self.lateral_views.visit(visitor),
            selection: self.selection.visit(visitor),
            group_by: self.group_by.visit(visitor),
            cluster_by: self.cluster_by.visit(visitor),
            distribute_by: self.distribute_by.visit(visitor),
            sort_by: self.sort_by.visit(visitor),
            having: self.having.visit(visitor),
            qualify: self.qualify.visit(visitor),
        }
    }
}

impl<V: Visitor> Visit<V> for TableWithJoins {
    fn visit(&self, visitor: &V) -> Self {
        TableWithJoins {
            relation: self.relation.visit(visitor),
            joins: self.joins.visit(visitor),
        }
    }
}
impl<V: Visitor> Visit<V> for Join {
    fn visit(&self, visitor: &V) -> Self {
        Join {
            relation: self.relation.visit(visitor),
            join_operator: self.join_operator.visit(visitor),
        }
    }
}

impl<V: Visitor> Visit<V> for JoinOperator {
    fn visit(&self, visitor: &V) -> Self {
        use JoinOperator::*;
        match self {
            Inner(c) => Inner(c.visit(visitor)),
            LeftOuter(c) => LeftOuter(c.visit(visitor)),
            RightOuter(c) => RightOuter(c.visit(visitor)),
            FullOuter(c) => FullOuter(c.visit(visitor)),
            CrossJoin => CrossJoin,
            CrossApply => CrossApply,
            OuterApply => OuterApply,
        }
    }
}

impl<V: Visitor> Visit<V> for JoinConstraint {
    fn visit(&self, visitor: &V) -> Self {
        use JoinConstraint::*;
        match self {
            On(e) => On(e.visit(visitor)),
            Using(v) => Using(v.visit(visitor)),
            Natural => Natural,
            None => None,
        }
    }
}

impl<V: Visitor> Visit<V> for TableFactor {
    fn visit(&self, visitor: &V) -> Self {
        use TableFactor::*;
        match self {
            Table {
                name,
                alias,
                args,
                with_hints,
            } => Table {
                name: name.visit(visitor),
                alias: alias.visit(visitor),
                args: args.visit(visitor),
                with_hints: with_hints.visit(visitor),
            },
            Derived {
                lateral,
                subquery,
                alias,
            } => Derived {
                lateral: *lateral,
                subquery: subquery.visit(visitor),
                alias: alias.visit(visitor),
            },
            TableFunction { expr, alias } => TableFunction {
                expr: expr.visit(visitor),
                alias: alias.visit(visitor),
            },
            UNNEST {
                alias,
                array_expr,
                with_offset,
                with_offset_alias,
            } => UNNEST {
                alias: alias.visit(visitor),
                array_expr: array_expr.visit(visitor),
                with_offset: *with_offset,
                with_offset_alias: with_offset_alias.visit(visitor),
            },
            NestedJoin {
                table_with_joins,
                alias,
            } => NestedJoin {
                table_with_joins: table_with_joins.visit(visitor),
                alias: alias.visit(visitor),
            },
        }
    }
}

impl<V: Visitor> Visit<V> for LateralView {
    fn visit(&self, visitor: &V) -> Self {
        LateralView {
            lateral_view: self.lateral_view.visit(visitor),
            lateral_view_name: self.lateral_view_name.visit(visitor),
            lateral_col_alias: self.lateral_col_alias.visit(visitor),
            outer: self.outer,
        }
    }
}

impl<V: Visitor> Visit<V> for FunctionArg {
    fn visit(&self, visitor: &V) -> Self {
        use FunctionArg::*;
        match self {
            Named { name, arg } => Named {
                name: name.visit(visitor),
                arg: arg.visit(visitor),
            },
            Unnamed(arg) => Unnamed(arg.visit(visitor)),
        }
    }
}

impl<V: Visitor> Visit<V> for FunctionArgExpr {
    fn visit(&self, visitor: &V) -> Self {
        use FunctionArgExpr::*;
        match self {
            Expr(e) => Expr(e.visit(visitor)),
            QualifiedWildcard(name) => QualifiedWildcard(name.visit(visitor)),
            Wildcard => Wildcard,
        }
    }
}

impl<V: Visitor> Visit<V> for SelectItem {
    fn visit(&self, visitor: &V) -> Self {
        use SelectItem::*;
        match self {
            UnnamedExpr(e) => UnnamedExpr(e.visit(visitor)),
            ExprWithAlias { expr, alias } => ExprWithAlias {
                expr: expr.visit(visitor),
                alias: alias.visit(visitor),
            },
            QualifiedWildcard(name) => QualifiedWildcard(name.visit(visitor)),
            Wildcard => Wildcard,
        }
    }
}

impl<V: Visitor> Visit<V> for ObjectName {
    fn visit(&self, visitor: &V) -> Self {
        ObjectName(self.0.visit(visitor))
    }
}

impl<V: Visitor> Visit<V> for Cte {
    fn visit(&self, visitor: &V) -> Self {
        Cte {
            alias: self.alias.visit(visitor),
            query: self.query.visit(visitor),
            from: self.from.visit(visitor),
        }
    }
}

impl<V: Visitor> Visit<V> for Ident {
    fn visit(&self, visitor: &V) -> Self {
        if let Some(i) = visitor.visit_ident(self) {
            return i;
        }
        self.clone()
    }
}

impl<V: Visitor> Visit<V> for TableAlias {
    fn visit(&self, visitor: &V) -> Self {
        TableAlias {
            name: self.name.visit(visitor),
            columns: self.columns.visit(visitor),
        }
    }
}

impl<V: Visitor> Visit<V> for OrderByExpr {
    fn visit(&self, visitor: &V) -> Self {
        OrderByExpr {
            expr: self.expr.visit(visitor),
            asc: self.asc.clone(),
            nulls_first: self.nulls_first.clone(),
        }
    }
}

impl<V: Visitor, T: Visit<V>> Visit<V> for Vec<T> {
    fn visit(&self, visitor: &V) -> Self {
        self.iter().map(|o| o.visit(visitor)).collect()
    }
}

impl<V: Visitor, T: Visit<V>> Visit<V> for Option<T> {
    fn visit(&self, visitor: &V) -> Self {
        self.as_ref().map(|x| x.visit(visitor))
    }
}

impl<V: Visitor, T: Visit<V>> Visit<V> for Box<T> {
    fn visit(&self, visitor: &V) -> Self {
        Box::new(self.as_ref().visit(visitor))
    }
}
