use anyhow::Result;

use super::Executor;
use crate::catalog::schema::Schema;
use crate::planner::physical_plan::Aggregation;
use crate::tuple::value::Value;
use crate::tuple::Tuple;

pub struct AggregateExecutor<'a> {
    child: Box<dyn Executor + 'a>,
    aggregations: Vec<Aggregation>,
    agg_results: Vec<Value>,
    done: bool,
}

impl<'a> AggregateExecutor<'a> {
    pub fn new(child: Box<dyn Executor + 'a>, aggregations: Vec<Aggregation>) -> Self {
        let initial_agg_results = aggregations
            .iter()
            .map(|agg| agg.initial_accumulator_value())
            .collect();
        Self {
            child,
            aggregations,
            agg_results: initial_agg_results,
            done: false,
        }
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        if self.done {
            Ok(None)
        } else {
            while let Some(tuple) = self.child.next().transpose()? {
                for (agg_result, aggregation) in
                    self.agg_results.iter_mut().zip(self.aggregations.iter())
                {
                    aggregation.aggregate(agg_result, &tuple);
                }
            }
            self.done = true;
            Ok(Some(Tuple::new(self.agg_results.clone())))
        }
    }
}

impl<'a> Executor for AggregateExecutor<'a> {
    fn next(&mut self) -> Option<Result<Tuple>> {
        self.next().transpose()
    }

    fn schema(&self) -> &Schema {
        unimplemented!()
    }

    fn rewind(&mut self) -> Result<()> {
        self.done = false;
        self.agg_results = self
            .aggregations
            .iter()
            .map(|agg| agg.initial_accumulator_value())
            .collect();
        self.child.rewind()
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::schema::{ColumnDefinition, TypeId};
    use crate::executors::tests::TestDb;
    use crate::tuple::value::Value;

    #[test]
    fn can_execute_count_aggregations() {
        let test_db = TestDb::new();
        test_db
            .create_table(
                "accounts",
                vec![
                    ColumnDefinition::new(TypeId::Integer, "id".to_owned(), 0, true),
                    ColumnDefinition::new(TypeId::Text, "name".to_owned(), 1, false),
                ],
            )
            .unwrap();

        let insert_statement = "
            insert into accounts values (1, 'foo'), (2, NULL), (3, 'baz')
        ";
        test_db.execute_query(insert_statement).unwrap();

        let select = "
            select 
                count(id) as id_count, 
                count(name) non_null_name_count, 
                2 * count(id) + 1,
                count(name is null) null_name_count 
            from accounts
        ";

        let result = test_db.execute_query(select).unwrap();
        assert_eq!(result.len(), 1);

        let expected_values = vec![
            Value::Integer(3),
            Value::Integer(2),
            Value::Integer(7),
            Value::Integer(3),
        ];
        assert_eq!(result.get(0).unwrap().values, expected_values);
    }

    #[test]
    fn can_execute_max_aggregations() {
        let test_db = TestDb::new();
        test_db
            .create_table(
                "accounts",
                vec![
                    ColumnDefinition::new(TypeId::Integer, "id".to_owned(), 0, true),
                    ColumnDefinition::new(TypeId::Text, "name".to_owned(), 1, false),
                ],
            )
            .unwrap();

        let insert_statement = "
            insert into accounts values (1, 'foo'), (2, NULL), (3, 'baz')
        ";
        test_db.execute_query(insert_statement).unwrap();

        let select = "
            select
                max(id),
                max(name),
                max(id) * 2 as double_max
            from accounts
        ";

        let result = test_db.execute_query(select).unwrap();
        assert_eq!(result.len(), 1);

        let expected_values = vec![
            Value::Integer(3),
            Value::String("foo".to_owned()),
            Value::Integer(6),
        ];
        assert_eq!(result.get(0).unwrap().values, expected_values);
    }

    #[test]
    fn can_execute_combination_of_aggregations() {
        let test_db = TestDb::new();
        test_db
            .create_table(
                "accounts",
                vec![
                    ColumnDefinition::new(TypeId::Integer, "id".to_owned(), 0, true),
                    ColumnDefinition::new(TypeId::Text, "name".to_owned(), 1, false),
                ],
            )
            .unwrap();

        let insert_statement = "
            insert into accounts values (1, 'foo'), (2, NULL), (3, 'baz')
        ";
        test_db.execute_query(insert_statement).unwrap();

        let select = "
            select
                max(id) + count(id),
                2 * (max(id) + count(name))
            from accounts
        ";

        let result = test_db.execute_query(select).unwrap();
        assert_eq!(result.len(), 1);

        let expected_values = vec![Value::Integer(6), Value::Integer(10)];
        assert_eq!(result.get(0).unwrap().values, expected_values);
    }
}
