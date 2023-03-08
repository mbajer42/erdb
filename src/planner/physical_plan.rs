use std::collections::HashMap;
use std::fmt::{self, Debug};

use crate::catalog::schema::Schema;
use crate::common::TableId;
use crate::parser::ast::{BinaryOperator, JoinType, UnaryOperator};
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

struct ExprWriter<'a> {
    expr: &'a Expr,
    plans: &'a [&'a PhysicalPlan],
}

impl fmt::Display for ExprWriter<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.expr {
            Expr::Value(val) => fmt::Display::fmt(val, f),
            Expr::ColumnReference { tuple_idx, col_idx } => {
                let schema = self.plans[*tuple_idx].schema();
                let column = &schema.columns()[*col_idx];
                f.write_str(&column.column_name)
            }
            Expr::Unary { op, expr } => {
                write!(f, "{}", op)?;
                let expr_writer = ExprWriter {
                    expr,
                    plans: self.plans,
                };
                write!(f, "{}", expr_writer)
            }
            Expr::Binary { left, op, right } => {
                let expr_writer = ExprWriter {
                    expr: left,
                    plans: self.plans,
                };
                write!(f, "{}", expr_writer)?;
                write!(f, "{}", op)?;
                let expr_writer = ExprWriter {
                    expr: right,
                    plans: self.plans,
                };
                write!(f, "{}", expr_writer)
            }
            Expr::IsNull(expr) => {
                let expr_writer = ExprWriter {
                    expr,
                    plans: self.plans,
                };
                write!(f, "{}", expr_writer)?;
                f.write_str(" IS NULL")
            }
            Expr::IsNotNull(expr) => {
                let expr_writer = ExprWriter {
                    expr,
                    plans: self.plans,
                };
                write!(f, "{}", expr_writer)?;
                f.write_str(" IS NOT NULL")
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
    Values {
        values: Vec<Vec<Expr>>,
        output_schema: Schema,
    },
    Insert {
        target: TableId,
        target_schema: Schema,
        child: Box<PhysicalPlan>,
    },
    Update {
        table: TableId,
        set: HashMap<usize, Expr>,
        child: Box<PhysicalPlan>,
    },
    Delete {
        from: TableId,
        child: Box<PhysicalPlan>,
    },
    Filter {
        filter: Vec<Expr>,
        child: Box<PhysicalPlan>,
    },
    NestedLoopJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        join_type: JoinType,
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
            Self::Values {
                values: _,
                output_schema,
            } => output_schema,
            Self::Insert {
                target: _,
                target_schema: _,
                child: _,
            } => unreachable!(),
            Self::Delete { from: _, child: _ } => unreachable!(),
            Self::Update {
                table: _,
                set: _,
                child: _,
            } => unreachable!(),
            Self::Filter { filter: _, child } => child.schema(),
            Self::NestedLoopJoin {
                left: _,
                right: _,
                join_type: _,
                on: _,
                output_schema,
            } => output_schema,
        }
    }
}

struct PaddedWriter<'a> {
    buffer: &'a mut (dyn fmt::Write + 'a),
    use_padding: bool,
}

impl fmt::Write for PaddedWriter<'_> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        for s in s.split_inclusive('\n') {
            if self.use_padding {
                self.buffer.write_str("  ")?;
            }
            self.use_padding = s.ends_with('\n');
            self.buffer.write_str(s)?;
        }

        Ok(())
    }
}

impl fmt::Display for PhysicalPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use std::fmt::Write;

        match self {
            Self::SequentialScan {
                table_id,
                output_schema: _,
            } => write!(f, "Sequential Scan on table with id {}", table_id),
            Self::Projection {
                projections: _,
                child,
                output_schema: _,
            } => write!(f, "{}", child),
            Self::Values {
                values: _,
                output_schema: _,
            } => write!(f, "Values Scan"),
            Self::Insert {
                target,
                target_schema: _,
                child,
            } => {
                writeln!(f, "Insert into table with id {}", target)?;
                let mut writer = PaddedWriter {
                    buffer: f,
                    use_padding: true,
                };
                write!(&mut writer, "{}", child)
            }
            Self::Update {
                table,
                set: _,
                child,
            } => {
                writeln!(f, "Update table with id {}", table)?;
                let mut writer = PaddedWriter {
                    buffer: f,
                    use_padding: true,
                };
                write!(&mut writer, "{}", child)
            }
            Self::Delete { from, child } => {
                writeln!(f, "Delete from table with id {}", from)?;
                let mut writer = PaddedWriter {
                    buffer: f,
                    use_padding: true,
                };
                write!(&mut writer, "{}", child)
            }
            Self::Filter { filter, child } => {
                let filter_expr = filter
                    .iter()
                    .map(|expr| {
                        let expr_writer = ExprWriter {
                            expr,
                            plans: &[child],
                        };
                        format!("{}", expr_writer)
                    })
                    .collect::<Vec<_>>()
                    .join("AND");

                writeln!(f, "Filter ({})", filter_expr)?;
                let mut writer = PaddedWriter {
                    buffer: f,
                    use_padding: true,
                };
                write!(&mut writer, "{}", child)
            }
            Self::NestedLoopJoin {
                left,
                right,
                join_type: _,
                on,
                output_schema: _,
            } => {
                let on_expr = on
                    .iter()
                    .map(|expr| {
                        let expr_writer = ExprWriter {
                            expr,
                            plans: &[left, right],
                        };
                        format!("{}", expr_writer)
                    })
                    .collect::<Vec<_>>()
                    .join("AND");

                writeln!(f, "Nested Loop Join ({})", on_expr)?;

                let mut writer = PaddedWriter {
                    buffer: f,
                    use_padding: true,
                };
                writeln!(&mut writer, "{}", left)?;
                write!(&mut writer, "{}", right)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::PhysicalPlan;
    use crate::catalog::schema::{ColumnDefinition, Schema, TypeId};
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

    #[test]
    fn can_format_physical_plan() {
        let seq_scan = PhysicalPlan::SequentialScan {
            table_id: 21,
            output_schema: Schema::new(vec![
                ColumnDefinition::new(TypeId::Integer, "id".to_owned(), 0, true),
                ColumnDefinition::new(TypeId::Text, "name".to_owned(), 1, true),
                ColumnDefinition::new(TypeId::Integer, "count".to_owned(), 2, true),
            ]),
        };

        let filter = PhysicalPlan::Filter {
            filter: vec![Expr::Binary {
                left: Box::new(Expr::ColumnReference {
                    tuple_idx: 0,
                    col_idx: 0,
                }),
                op: BinaryOperator::Eq,
                right: Box::new(Expr::Value(Value::Integer(1))),
            }],
            child: Box::new(seq_scan),
        };

        let formatted = format!("{}", filter);
        let expected = "Filter (id=1)\n  Sequential Scan on table with id 21";
        assert_eq!(expected, formatted);
    }
}
