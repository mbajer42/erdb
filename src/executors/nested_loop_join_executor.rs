use anyhow::Result;

use super::Executor;
use crate::catalog::schema::Schema;
use crate::tuple::Tuple;

pub struct NestedLoopJoinExecutor<'a> {
    left_child: Box<dyn Executor + 'a>,
    right_child: Box<dyn Executor + 'a>,
    left_tuple: Option<Tuple>,
    schema: Schema,
}

impl<'a> NestedLoopJoinExecutor<'a> {
    pub fn new(
        left_child: Box<dyn Executor + 'a>,
        right_child: Box<dyn Executor + 'a>,
        schema: Schema,
    ) -> Self {
        Self {
            left_child,
            right_child,
            left_tuple: None,
            schema,
        }
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        if self.left_tuple.is_none() {
            self.left_tuple = self.left_child.next().transpose()?;
            self.right_child.rewind()?;
        }
        while let Some(ref left_tuple) = self.left_tuple {
            let right = self.right_child.next().transpose()?;
            if let Some(mut right_tuple) = right {
                let mut left_values = left_tuple.values().to_vec();
                left_values.append(&mut right_tuple.values);
                return Ok(Some(Tuple::new(left_values)));
            } else {
                self.left_tuple = self.left_child.next().transpose()?;
                self.right_child.rewind()?;
            }
        }
        Ok(None)
    }
}

impl<'a> Executor for NestedLoopJoinExecutor<'a> {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn next(&mut self) -> Option<Result<Tuple>> {
        self.next().transpose()
    }

    fn rewind(&mut self) -> Result<()> {
        self.left_child.rewind()?;
        self.right_child.rewind()?;
        Ok(())
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
    use crate::tuple::value::Value;
    use crate::tuple::Tuple;

    fn execute_query(sql: &str, buffer_manager: &BufferManager, analyzer: &Analyzer) -> Vec<Tuple> {
        let query = parse_sql(sql).unwrap();
        let query = analyzer.analyze(query).unwrap();
        let planner = Planner::new();
        let plan = planner.prepare_logical_plan(query).unwrap();
        let mut executor_factory = ExecutorFactory::new(buffer_manager);
        let mut executor = executor_factory.create_executor(plan).unwrap();

        let mut tuples = vec![];
        while let Some(tuple) = executor.next() {
            tuples.push(tuple.unwrap());
        }

        tuples
    }

    #[test]
    fn can_execute_cross_joins() {
        let data_dir = tempdir().unwrap();
        let file_manager = FileManager::new(data_dir.path()).unwrap();
        let buffer_manager = BufferManager::new(file_manager, 1);
        let catalog = Catalog::new(&buffer_manager, true).unwrap();
        let analyzer = Analyzer::new(&catalog);

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
        catalog
            .create_table(
                "strings",
                vec![ColumnDefinition::new(
                    TypeId::Text,
                    "string".to_owned(),
                    0,
                    true,
                )],
            )
            .unwrap();

        let insert_numbers = "insert into numbers values (1), (2), (3)";
        execute_query(insert_numbers, &buffer_manager, &analyzer);

        let insert_strings = "insert into strings values ('foo'), ('bar'), ('baz')";
        execute_query(insert_strings, &buffer_manager, &analyzer);

        let cross_join = "select number, string from numbers, strings";
        let mut result = execute_query(cross_join, &buffer_manager, &analyzer);
        result.sort_by_key(|tuple| {
            (
                tuple.values()[0].as_i32(),
                tuple.values()[1].as_str().to_owned(),
            )
        });

        let expected_result = vec![
            Tuple::new(vec![Value::Integer(1), Value::String("bar".to_owned())]),
            Tuple::new(vec![Value::Integer(1), Value::String("baz".to_owned())]),
            Tuple::new(vec![Value::Integer(1), Value::String("foo".to_owned())]),
            Tuple::new(vec![Value::Integer(2), Value::String("bar".to_owned())]),
            Tuple::new(vec![Value::Integer(2), Value::String("baz".to_owned())]),
            Tuple::new(vec![Value::Integer(2), Value::String("foo".to_owned())]),
            Tuple::new(vec![Value::Integer(3), Value::String("bar".to_owned())]),
            Tuple::new(vec![Value::Integer(3), Value::String("baz".to_owned())]),
            Tuple::new(vec![Value::Integer(3), Value::String("foo".to_owned())]),
        ];

        assert_eq!(expected_result, result);
    }

    #[test]
    fn conditions_on_cross_joins() {
        let data_dir = tempdir().unwrap();
        let file_manager = FileManager::new(data_dir.path()).unwrap();
        let buffer_manager = BufferManager::new(file_manager, 1);
        let catalog = Catalog::new(&buffer_manager, true).unwrap();
        let analyzer = Analyzer::new(&catalog);

        catalog
            .create_table(
                "numbers",
                vec![
                    ColumnDefinition::new(TypeId::Integer, "id".to_owned(), 0, true),
                    ColumnDefinition::new(TypeId::Integer, "number".to_owned(), 1, true),
                ],
            )
            .unwrap();
        catalog
            .create_table(
                "strings",
                vec![
                    ColumnDefinition::new(TypeId::Integer, "id".to_owned(), 0, true),
                    ColumnDefinition::new(TypeId::Text, "string".to_owned(), 1, true),
                ],
            )
            .unwrap();

        let insert_numbers = "insert into numbers values (1, 1), (2, 2), (3, 3)";
        execute_query(insert_numbers, &buffer_manager, &analyzer);

        let insert_strings = "insert into strings values (1, 'foo'), (2, 'bar'), (3, 'baz')";
        execute_query(insert_strings, &buffer_manager, &analyzer);

        let cross_join =
            "select number, string from numbers, strings where numbers.id = strings.id";
        let mut result = execute_query(cross_join, &buffer_manager, &analyzer);
        result.sort_by_key(|tuple| (tuple.values()[0].as_i32()));

        let expected_result = vec![
            Tuple::new(vec![Value::Integer(1), Value::String("foo".to_owned())]),
            Tuple::new(vec![Value::Integer(2), Value::String("bar".to_owned())]),
            Tuple::new(vec![Value::Integer(3), Value::String("baz".to_owned())]),
        ];

        assert_eq!(expected_result, result);
    }
}
