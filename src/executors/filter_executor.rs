use anyhow::Result;

use super::Executor;
use crate::analyzer::query::Expr;
use crate::catalog::schema::Schema;
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
                match self.filter.evaluate(&tuple) {
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
    use tempfile::tempdir;

    use crate::analyzer::Analyzer;
    use crate::buffer::buffer_manager::BufferManager;
    use crate::catalog::schema::{ColumnDefinition, TypeId};
    use crate::catalog::Catalog;
    use crate::executors::ExecutorFactory;
    use crate::parser::parse_sql;
    use crate::planner::Planner;
    use crate::storage::file_manager::FileManager;
    use crate::tuple::Tuple;

    fn execute_query(sql: &str, buffer_manager: &BufferManager, analyzer: &Analyzer) -> Vec<Tuple> {
        let query = parse_sql(sql).unwrap();
        let query = analyzer.analyze(query).unwrap();
        let planner = Planner::new();
        let plan = planner.plan_query(query);
        let mut executor_factory = ExecutorFactory::new(buffer_manager);
        let mut executor = executor_factory.create_executor(plan).unwrap();

        let mut tuples = vec![];
        while let Some(tuple) = executor.next() {
            tuples.push(tuple.unwrap());
        }

        tuples
    }

    #[test]
    fn can_execute_queries_with_filter_conditions() {
        let data_dir = tempdir().unwrap();
        let file_manager = FileManager::new(data_dir.path()).unwrap();
        let buffer_manager = BufferManager::new(file_manager, 1);
        let catalog = Catalog::new(&buffer_manager, true).unwrap();
        catalog
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
        let analyzer = Analyzer::new(&catalog);

        let insert_statement =
            "insert into numbers values (1), (2), (3), (4), (5), (6), (7), (8), (9)";
        execute_query(insert_statement, &buffer_manager, &analyzer);

        let select = "select number from numbers where number % 2 = 0";
        let mut result = execute_query(select, &buffer_manager, &analyzer)
            .iter()
            .map(|tuple| tuple.values()[0].as_i32())
            .collect::<Vec<i32>>();
        result.sort();

        let expected_numbers = vec![2, 4, 6, 8];

        assert_eq!(result, expected_numbers);
    }
}
