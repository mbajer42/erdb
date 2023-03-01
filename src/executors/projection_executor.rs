use anyhow::Result;

use super::Executor;
use crate::catalog::schema::Schema;
use crate::planner::physical_plan::Expr;
use crate::tuple::Tuple;

pub struct ProjectionExecutor<'a> {
    child: Box<dyn Executor + 'a>,
    projections: Vec<Expr>,
    output_schema: Schema,
}

impl<'a> ProjectionExecutor<'a> {
    pub fn new(
        child: Box<dyn Executor + 'a>,
        projections: Vec<Expr>,
        output_schema: Schema,
    ) -> Self {
        Self {
            child,
            projections,
            output_schema,
        }
    }
}

impl<'a> Executor for ProjectionExecutor<'a> {
    fn next(&mut self) -> Option<Result<Tuple>> {
        self.child.next().map(|tuple| {
            tuple.map(|tuple| {
                let values = self
                    .projections
                    .iter()
                    .map(|expr| expr.evaluate(&[&tuple]))
                    .collect();
                Tuple::new(values)
            })
        })
    }

    fn rewind(&mut self) -> Result<()> {
        self.child.rewind()
    }

    fn schema(&self) -> &Schema {
        &self.output_schema
    }
}

#[cfg(test)]
mod tests {

    use crate::catalog::schema::{ColumnDefinition, TypeId};
    use crate::concurrency::IsolationLevel;
    use crate::executors::tests::{EmptyTestContext, ExecutionTestContext};
    use crate::parser::ast::BinaryOperator;
    use crate::tuple::value::Value;

    fn execute_query_expect_single_tuple(
        sql: &str,
        execution_test_context: &ExecutionTestContext,
        expected: Value,
    ) {
        let tuples = execution_test_context.execute_query(sql).unwrap();
        assert_eq!(tuples.len(), 1);
        let values = tuples.get(0).unwrap().values();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], expected, "when evaluating {}", sql);
    }

    #[test]
    fn can_execute_comparison_expressions() {
        let empty_test_context = EmptyTestContext::new();
        let execution_test_context = ExecutionTestContext::new(&empty_test_context);

        let arg_op_expected_result = [
            (Value::Integer(42), BinaryOperator::Eq, Value::Boolean(true)),
            (
                Value::Integer(42),
                BinaryOperator::NotEq,
                Value::Boolean(false),
            ),
            (
                Value::Integer(42),
                BinaryOperator::Less,
                Value::Boolean(false),
            ),
            (
                Value::Integer(42),
                BinaryOperator::LessEq,
                Value::Boolean(true),
            ),
            (
                Value::Integer(42),
                BinaryOperator::Greater,
                Value::Boolean(false),
            ),
            (
                Value::Integer(42),
                BinaryOperator::GreaterEq,
                Value::Boolean(true),
            ),
            (Value::Null, BinaryOperator::Eq, Value::Null),
            (
                Value::Integer(21),
                BinaryOperator::Less,
                Value::Boolean(true),
            ),
        ];

        for (arg, op, expected) in arg_op_expected_result {
            let sql = format!("select {} {} 42", arg, op);
            execute_query_expect_single_tuple(&sql, &execution_test_context, expected);
        }
    }

    #[test]
    fn can_execute_arithmetic_expressions() {
        let empty_test_context = EmptyTestContext::new();
        let execution_test_context = ExecutionTestContext::new(&empty_test_context);

        let left_op_right_result = vec![
            (
                Value::Integer(1),
                BinaryOperator::Plus,
                Value::Integer(2),
                Value::Integer(3),
            ),
            (
                Value::Integer(21),
                BinaryOperator::Multiply,
                Value::Integer(2),
                Value::Integer(42),
            ),
            (
                Value::Integer(42),
                BinaryOperator::Divide,
                Value::Integer(2),
                Value::Integer(21),
            ),
            (
                Value::Integer(17),
                BinaryOperator::Minus,
                Value::Integer(21),
                Value::Integer(-4),
            ),
            (
                Value::Integer(3),
                BinaryOperator::Modulo,
                Value::Integer(2),
                Value::Integer(1),
            ),
            (
                Value::Integer(4),
                BinaryOperator::Modulo,
                Value::Integer(2),
                Value::Integer(0),
            ),
        ];

        for (left, op, right, expected) in left_op_right_result {
            let sql = format!("select {} {} {}", left, op, right);
            execute_query_expect_single_tuple(&sql, &execution_test_context, expected);
        }
    }

    #[test]
    fn can_execute_or_and_expressions() {
        let empty_test_context = EmptyTestContext::new();
        let execution_test_context = ExecutionTestContext::new(&empty_test_context);
        let left_op_right_result = vec![
            (
                Value::Boolean(true),
                BinaryOperator::And,
                Value::Boolean(true),
                Value::Boolean(true),
            ),
            (
                Value::Boolean(false),
                BinaryOperator::And,
                Value::Boolean(true),
                Value::Boolean(false),
            ),
            (
                Value::Boolean(false),
                BinaryOperator::Or,
                Value::Boolean(true),
                Value::Boolean(true),
            ),
            (
                Value::Boolean(false),
                BinaryOperator::Or,
                Value::Boolean(false),
                Value::Boolean(false),
            ),
            (
                Value::Boolean(false),
                BinaryOperator::Or,
                Value::Null,
                Value::Null,
            ),
        ];

        for (left, op, right, expected) in left_op_right_result {
            let sql = format!("select {} {} {}", left, op, right);
            execute_query_expect_single_tuple(&sql, &execution_test_context, expected);
        }
    }

    #[test]
    fn repeatable_read_selects_see_only_rows_committed_before_transaction() {
        let empty_test_context = EmptyTestContext::new();
        let execution_test_context = ExecutionTestContext::new(&empty_test_context);
        execution_test_context
            .create_table(
                "numbers",
                vec![ColumnDefinition::new(
                    TypeId::Integer,
                    "number".to_owned(),
                    0,
                    true,
                )],
            )
            .unwrap();

        let insert_statement = "insert into numbers values (1), (2), (3)";
        execution_test_context
            .execute_query(insert_statement)
            .unwrap();

        let mut select_transaction = execution_test_context
            .transaction_manager
            .start_transaction(Some(IsolationLevel::RepeatableRead))
            .unwrap();

        let insert_statement = "insert into numbers values (4), (5)";
        execution_test_context
            .execute_query(insert_statement)
            .unwrap();

        execution_test_context
            .transaction_manager
            .refresh_transaction(&mut select_transaction)
            .unwrap();
        let select_statement = "select * from numbers";
        let mut result = execution_test_context
            .execute_query_with_transaction(select_statement, &select_transaction)
            .unwrap()
            .into_iter()
            .map(|tuple| tuple.values[0].as_i32())
            .collect::<Vec<_>>();
        result.sort();

        assert_eq!(result, vec![1, 2, 3]);
    }

    #[test]
    fn read_committed_sees_all_committed_rows() {
        let empty_test_context = EmptyTestContext::new();
        let execution_test_context = ExecutionTestContext::new(&empty_test_context);
        execution_test_context
            .create_table(
                "numbers",
                vec![ColumnDefinition::new(
                    TypeId::Integer,
                    "number".to_owned(),
                    0,
                    true,
                )],
            )
            .unwrap();

        let insert_statement = "insert into numbers values (1), (2), (3)";
        execution_test_context
            .execute_query(insert_statement)
            .unwrap();

        let mut select_transaction = execution_test_context
            .transaction_manager
            .start_transaction(Some(IsolationLevel::ReadCommitted))
            .unwrap();

        let insert_statement = "insert into numbers values (4), (5)";
        execution_test_context
            .execute_query(insert_statement)
            .unwrap();

        execution_test_context
            .transaction_manager
            .refresh_transaction(&mut select_transaction)
            .unwrap();
        let select_statement = "select * from numbers";
        let mut result = execution_test_context
            .execute_query_with_transaction(select_statement, &select_transaction)
            .unwrap()
            .into_iter()
            .map(|tuple| tuple.values[0].as_i32())
            .collect::<Vec<_>>();
        result.sort();

        assert_eq!(result, vec![1, 2, 3, 4, 5]);
    }
}
