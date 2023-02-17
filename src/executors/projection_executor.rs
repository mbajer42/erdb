use anyhow::Result;

use super::Executor;
use crate::analyzer::query::Expr;
use crate::catalog::schema::Schema;
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
                    .map(|expr| expr.evaluate(&tuple))
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
    use tempfile::tempdir;

    use crate::analyzer::Analyzer;
    use crate::buffer::buffer_manager::BufferManager;
    use crate::catalog::Catalog;
    use crate::executors::ExecutorFactory;
    use crate::parser::ast::BinaryOperator;
    use crate::parser::parse_sql;
    use crate::planner::Planner;
    use crate::storage::file_manager::FileManager;
    use crate::tuple::value::Value;

    fn execute_query_expect_single_tuple(
        buffer_manager: &BufferManager,
        sql: &str,
        analyzer: &Analyzer,
        expected: Value,
    ) {
        let query = parse_sql(sql).unwrap();
        let query = analyzer.analyze(query).unwrap();
        let planner = Planner::new();
        let plan = planner.plan_query(query);
        let mut executor_factory = ExecutorFactory::new(buffer_manager);
        let mut executor = executor_factory.create_executor(plan).unwrap();
        let result = executor.next().transpose().unwrap();
        let tuple = result.unwrap();
        let values = tuple.values();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], expected, "when evaluating {}", sql);
        assert!(executor.next().is_none());
    }

    #[test]
    fn can_execute_comparison_expressions() {
        let data_dir = tempdir().unwrap();
        let file_manager = FileManager::new(data_dir.path()).unwrap();
        let buffer_manager = BufferManager::new(file_manager, 1);
        let catalog = Catalog::new(&buffer_manager, true).unwrap();
        let analyzer = Analyzer::new(&catalog);

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
            execute_query_expect_single_tuple(&buffer_manager, &sql, &analyzer, expected);
        }
    }

    #[test]
    fn can_execute_arithmetic_expressions() {
        let data_dir = tempdir().unwrap();
        let file_manager = FileManager::new(data_dir.path()).unwrap();
        let buffer_manager = BufferManager::new(file_manager, 1);
        let catalog = Catalog::new(&buffer_manager, true).unwrap();
        let analyzer = Analyzer::new(&catalog);

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
            execute_query_expect_single_tuple(&buffer_manager, &sql, &analyzer, expected);
        }
    }
}
