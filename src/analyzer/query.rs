use lazy_static::lazy_static;

use crate::catalog::schema::Schema;
use crate::common::TableId;
use crate::parser::ast::{self, BinaryOperator, UnaryOperator};
use crate::tuple::value::Value;
use crate::tuple::Tuple;

lazy_static! {
    pub static ref EMPTY_SCHEMA: Schema = Schema::new(vec![]);
}

#[derive(Debug, PartialEq)]
pub enum QueryType {
    Select,
    Insert,
}

#[derive(Debug, PartialEq)]
pub enum Expr {
    ColumnReference(u8),
    Integer(i32),
    String(String),
    Boolean(bool),
    Null,
    Unary {
        op: ast::UnaryOperator,
        expr: Box<Expr>,
    },
    Binary {
        left: Box<Expr>,
        op: ast::BinaryOperator,
        right: Box<Expr>,
    },
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
}

impl Expr {
    pub fn evaluate(&self, tuple: &Tuple) -> Value {
        match self {
            Expr::ColumnReference(col) => tuple.values().get(*col as usize).unwrap().clone(),
            Expr::Integer(number) => Value::Integer(*number),
            Expr::String(s) => Value::String(s.clone()),
            Expr::Boolean(val) => Value::Boolean(*val),
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
            Expr::IsNull(expr) => {
                let val = expr.evaluate(tuple);
                Value::Boolean(val.is_null())
            }
            Expr::IsNotNull(expr) => {
                let val = expr.evaluate(tuple);
                Value::Boolean(!val.is_null())
            }
            Expr::Null => Value::Null,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Table {
    TableReference { table_id: TableId, schema: Schema },
    EmptyTable,
}

impl Table {
    pub fn schema(&self) -> &Schema {
        match self {
            Table::TableReference {
                table_id: _,
                schema,
            } => schema,
            Table::EmptyTable => &EMPTY_SCHEMA,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Query {
    pub query_type: QueryType,
    /// VALUES
    pub values: Option<Vec<Vec<Expr>>>,
    /// FROM clause
    pub from: Table,
    /// SELECT list
    pub projections: Vec<Expr>,
    /// schema of the query output
    pub output_schema: Schema,
    /// target table id of output, only for INSERT/DELETE/UPDATE
    pub target: Option<TableId>,
    /// target schema, only for INSERT/DELETE/UPDATE
    pub target_schema: Option<Schema>,
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

    #[test]
    fn can_evaluate_is_null() {
        let expr = Expr::IsNull(Box::new(Expr::Null));
        assert_eq!(expr.evaluate(&(Tuple::new(vec![]))), Value::Boolean(true));

        let expr = Expr::IsNull(Box::new(Expr::Integer(42)));
        assert_eq!(expr.evaluate(&(Tuple::new(vec![]))), Value::Boolean(false));
    }

    #[test]
    fn can_evaluate_is_not_null() {
        let expr = Expr::IsNotNull(Box::new(Expr::Null));
        assert_eq!(expr.evaluate(&(Tuple::new(vec![]))), Value::Boolean(false));

        let expr = Expr::IsNotNull(Box::new(Expr::Integer(42)));
        assert_eq!(expr.evaluate(&(Tuple::new(vec![]))), Value::Boolean(true));
    }
}
