use crate::catalog::schema::Schema;
use crate::common::TableId;
use crate::parser::ast::{self, BinaryOperator, UnaryOperator};
use crate::tuple::value::Value;
use crate::tuple::Tuple;

#[derive(Debug, PartialEq)]
pub enum QueryType {
    Select,
}

#[derive(Debug, PartialEq)]
pub enum Expr {
    ColumnReference(u8),
    Integer(i32),
    Unary {
        op: ast::UnaryOperator,
        expr: Box<Expr>,
    },
    Binary {
        left: Box<Expr>,
        op: ast::BinaryOperator,
        right: Box<Expr>,
    },
}

impl Expr {
    pub fn evaluate(&self, tuple: &Tuple) -> Value {
        match self {
            Expr::ColumnReference(col) => tuple.values().get(*col as usize).unwrap().clone(),
            Expr::Integer(number) => Value::Integer(*number),
            Expr::Unary { op, expr } => match op {
                UnaryOperator::Plus => expr.evaluate(tuple),
                UnaryOperator::Minus => Value::Integer(-expr.evaluate(tuple).as_i32()),
            },
            Expr::Binary { left, op, right } => {
                let left = left.evaluate(tuple).as_i32();
                let right = right.evaluate(tuple).as_i32();
                match op {
                    BinaryOperator::Plus => Value::Integer(left + right),
                    BinaryOperator::Minus => Value::Integer(left - right),
                    BinaryOperator::Multiply => Value::Integer(left * right),
                    BinaryOperator::Divide => Value::Integer(left / right),
                }
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Table {
    TableReference { table_id: TableId, schema: Schema },
}

impl Table {
    pub fn schema(&self) -> &Schema {
        match self {
            Table::TableReference {
                table_id: _,
                schema,
            } => schema,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Query {
    pub query_type: QueryType,
    /// FROM clause
    pub from: Table,
    /// SELECT list
    pub projections: Vec<Expr>,
    /// schema of the query output
    pub output_schema: Schema,
}

#[cfg(test)]
mod tests {
    use super::Expr;
    use crate::parser::ast::{BinaryOperator, UnaryOperator};
    use crate::tuple::value::Value;
    use crate::tuple::Tuple;

    #[test]
    fn can_evaluate_arithmetic_expressions() {
        // expr = -2 + 2 * (3 + 5) == 14
        let expr = Expr::Binary {
            left: Box::new(Expr::Unary {
                op: UnaryOperator::Minus,
                expr: Box::new(Expr::Integer(2)),
            }),
            op: BinaryOperator::Plus,
            right: Box::new(Expr::Binary {
                left: Box::new(Expr::Integer(2)),
                op: BinaryOperator::Multiply,
                right: Box::new(Expr::Binary {
                    left: Box::new(Expr::Integer(3)),
                    op: BinaryOperator::Plus,
                    right: Box::new(Expr::Integer(5)),
                }),
            }),
        };

        let value = expr.evaluate(&(Tuple::new(vec![])));
        assert_eq!(value, Value::Integer(14));
    }
}
