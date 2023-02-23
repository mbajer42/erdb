use anyhow::Result;

use super::Executor;
use crate::catalog::schema::Schema;

use crate::planner::physical_plan::Expr;
use crate::tuple::value::Value;
use crate::tuple::Tuple;

pub struct FilterExecutor<'a> {
    child: Box<dyn Executor + 'a>,
    filter: Expr,
}

impl<'a> FilterExecutor<'a> {
    pub fn new(child: Box<dyn Executor + 'a>, filter: Expr) -> Self {
        Self { child, filter }
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        loop {
            if let Some(tuple) = self.child.next().transpose()? {
                match self.filter.evaluate(&[&tuple]) {
                    Value::Boolean(b) => {
                        if b {
                            return Ok(Some(tuple));
                        } else {
                            continue;
                        }
                    }
                    _ => continue,
                }
            } else {
                return Ok(None);
            }
        }
    }
}

impl<'a> Executor for FilterExecutor<'a> {
    fn next(&mut self) -> Option<Result<Tuple>> {
        self.next().transpose()
    }

    fn rewind(&mut self) -> Result<()> {
        self.child.rewind()
    }

    fn schema(&self) -> &Schema {
        self.child.schema()
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::schema::{ColumnDefinition, TypeId};
    use crate::executors::tests::{EmptyTestContext, ExecutionTestContext};

    #[test]
    fn can_execute_queries_with_filter_conditions() {
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

        let insert_statement =
            "insert into numbers values (1), (2), (3), (4), (5), (6), (7), (8), (9)";
        execution_test_context
            .execute_query(insert_statement)
            .unwrap();

        let select = "select number from numbers where number % 2 = 0";
        let mut result = execution_test_context
            .execute_query(select)
            .unwrap()
            .iter()
            .map(|tuple| tuple.values()[0].as_i32())
            .collect::<Vec<i32>>();
        result.sort();

        let expected_numbers = vec![2, 4, 6, 8];

        assert_eq!(result, expected_numbers);
    }
}
