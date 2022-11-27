// This is copied from datafusion
// (arrow-datafusion/datafusion/sql/src/planner.rs::parse_sql_binary_op)

pub fn parser_binop_to_df_binop(
    op: &sqlparser::ast::BinaryOperator,
) -> std::result::Result<datafusion::logical_expr::Operator, datafusion::common::DataFusionError> {
    use datafusion::common::DataFusionError;
    use datafusion::logical_expr::Operator;
    use sqlparser::ast::BinaryOperator;
    match op {
        BinaryOperator::Gt => Ok(Operator::Gt),
        BinaryOperator::GtEq => Ok(Operator::GtEq),
        BinaryOperator::Lt => Ok(Operator::Lt),
        BinaryOperator::LtEq => Ok(Operator::LtEq),
        BinaryOperator::Eq => Ok(Operator::Eq),
        BinaryOperator::NotEq => Ok(Operator::NotEq),
        BinaryOperator::Plus => Ok(Operator::Plus),
        BinaryOperator::Minus => Ok(Operator::Minus),
        BinaryOperator::Multiply => Ok(Operator::Multiply),
        BinaryOperator::Divide => Ok(Operator::Divide),
        BinaryOperator::Modulo => Ok(Operator::Modulo),
        BinaryOperator::And => Ok(Operator::And),
        BinaryOperator::Or => Ok(Operator::Or),
        BinaryOperator::PGRegexMatch => Ok(Operator::RegexMatch),
        BinaryOperator::PGRegexIMatch => Ok(Operator::RegexIMatch),
        BinaryOperator::PGRegexNotMatch => Ok(Operator::RegexNotMatch),
        BinaryOperator::PGRegexNotIMatch => Ok(Operator::RegexNotIMatch),
        BinaryOperator::BitwiseAnd => Ok(Operator::BitwiseAnd),
        BinaryOperator::BitwiseOr => Ok(Operator::BitwiseOr),
        BinaryOperator::BitwiseXor => Ok(Operator::BitwiseXor),
        BinaryOperator::PGBitwiseShiftRight => Ok(Operator::BitwiseShiftRight),
        BinaryOperator::PGBitwiseShiftLeft => Ok(Operator::BitwiseShiftLeft),
        BinaryOperator::StringConcat => Ok(Operator::StringConcat),
        _ => Err(DataFusionError::NotImplemented(format!(
            "Unsupported SQL binary operator {:?}",
            op
        ))),
    }
}
