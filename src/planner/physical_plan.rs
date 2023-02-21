use crate::catalog::schema::Schema;
use crate::common::TableId;
use crate::parser::ast::{BinaryOperator, UnaryOperator};
use crate::tuple::value::Value;
use crate::tuple::Tuple;

#[derive(Debug, PartialEq)]
pub enum Expr {
    ColumnReference {
        /// some plans have more than one child (join plans)
        /// tuple_idx indicates from which child the tuple comes
        tuple_idx: usize,
        col_idx: usize,
    },
    Value(Value),
    Unary {
        op: UnaryOperator,
        expr: Box<Expr>,
    },
    Binary {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
}

impl Expr {
    pub fn evaluate(&self, tuple: &[&Tuple]) -> Value {
        match self {
            Expr::ColumnReference { tuple_idx, col_idx } => {
                tuple[*tuple_idx].values().get(*col_idx).unwrap().clone()
            }
            Expr::Value(val) => val.clone(),
            Expr::Unary { op, expr } => match op {
                UnaryOperator::Plus => expr.evaluate(tuple),
                UnaryOperator::Minus => Value::Integer(-expr.evaluate(tuple).as_i32()),
            },
            Expr::Binary { left, op, right } => {
                let left = left.evaluate(tuple);
                let right = right.evaluate(tuple);
                left.evaluate_binary_expression(&right, *op)
            }
            Expr::IsNull(expr) => {
                let val = expr.evaluate(tuple);
                Value::Boolean(val.is_null())
            }
            Expr::IsNotNull(expr) => {
                let val = expr.evaluate(tuple);
                Value::Boolean(!val.is_null())
            }
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum PhysicalPlan {
    SequentialScan {
        table_id: TableId,
        output_schema: Schema,
    },
    Projection {
        projections: Vec<Expr>,
        child: Box<PhysicalPlan>,
        output_schema: Schema,
    },
    ValuesPlan {
        values: Vec<Vec<Expr>>,
        output_schema: Schema,
    },
    InsertPlan {
        target: TableId,
        target_schema: Schema,
        child: Box<PhysicalPlan>,
    },
    FilterPlan {
        filter: Expr,
        child: Box<PhysicalPlan>,
    },
    Join {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        on: Vec<Expr>,
        output_schema: Schema,
    },
}

impl PhysicalPlan {
    pub fn schema(&self) -> &Schema {
        match self {
            Self::SequentialScan {
                table_id: _,
                output_schema,
            } => output_schema,
            Self::Projection {
                projections: _,
                child: _,
                output_schema,
            } => output_schema,
            Self::ValuesPlan {
                values: _,
                output_schema,
            } => output_schema,
            Self::InsertPlan {
                target: _,
                target_schema: _,
                child: _,
            } => unreachable!(),
            Self::FilterPlan { filter: _, child } => child.schema(),
            Self::Join {
                left: _,
                right: _,
                on: _,
                output_schema,
            } => output_schema,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::parser::ast::{BinaryOperator, UnaryOperator};
    use crate::planner::physical_plan::Expr;
    use crate::tuple::value::Value;

    #[test]
    fn can_evaluate_arithmetic_expressions() {
        // expr = -2 + 2 * (3 + 5) == 14
        let expr = Expr::Binary {
            left: Box::new(Expr::Unary {
                op: UnaryOperator::Minus,
                expr: Box::new(Expr::Value(Value::Integer(2))),
            }),
            op: BinaryOperator::Plus,
            right: Box::new(Expr::Binary {
                left: Box::new(Expr::Value(Value::Integer(2))),
                op: BinaryOperator::Multiply,
                right: Box::new(Expr::Binary {
                    left: Box::new(Expr::Value(Value::Integer(3))),
                    op: BinaryOperator::Plus,
                    right: Box::new(Expr::Value(Value::Integer(5))),
                }),
            }),
        };

        let value = expr.evaluate(&[]);
        assert_eq!(value, Value::Integer(14));
    }

    #[test]
    fn can_evaluate_is_null() {
        let expr = Expr::IsNull(Box::new(Expr::Value(Value::Null)));
        assert_eq!(expr.evaluate(&[]), Value::Boolean(true));

        let expr = Expr::IsNull(Box::new(Expr::Value(Value::Integer(42))));
        assert_eq!(expr.evaluate(&[]), Value::Boolean(false));
    }

    #[test]
    fn can_evaluate_is_not_null() {
        let expr = Expr::IsNotNull(Box::new(Expr::Value(Value::Null)));
        assert_eq!(expr.evaluate(&[]), Value::Boolean(false));

        let expr = Expr::IsNotNull(Box::new(Expr::Value(Value::Integer(42))));
        assert_eq!(expr.evaluate(&[]), Value::Boolean(true));
    }
}
