use anyhow::Result;

use super::Executor;
use crate::catalog::schema::Schema;
use crate::parser::ast::JoinType;
use crate::planner::physical_plan::Expr;
use crate::tuple::value::Value;
use crate::tuple::Tuple;

pub struct NestedLoopJoinExecutor<'a> {
    left_child: Box<dyn Executor + 'a>,
    right_child: Box<dyn Executor + 'a>,
    join_type: JoinType,
    on: Vec<Expr>,
    left_tuple: Option<Tuple>,
    left_had_match: bool,
    schema: Schema,
}

impl<'a> NestedLoopJoinExecutor<'a> {
    pub fn new(
        left_child: Box<dyn Executor + 'a>,
        right_child: Box<dyn Executor + 'a>,
        join_type: JoinType,
        on: Vec<Expr>,
        schema: Schema,
    ) -> Self {
        // a right router join is just a left outer join but the left and right tables are swapped
        // we do this here, but have to remember to correct it later
        let (left, right) = match join_type {
            JoinType::Left | JoinType::Inner => (left_child, right_child),
            JoinType::Right => (right_child, left_child),
        };
        Self {
            left_child: left,
            right_child: right,
            join_type,
            on,
            left_tuple: None,
            left_had_match: false,
            schema,
        }
    }

    fn join_condition_evaluates_to_true(&self, left: &Tuple, right: &Tuple) -> bool {
        let tuples = match self.join_type {
            JoinType::Left | JoinType::Inner => [left, right],
            JoinType::Right => [right, left],
        };
        self.on.iter().all(|expr| match expr.evaluate(&tuples) {
            Value::Boolean(val) => val,
            _ => false,
        })
    }

    fn construct_result(
        &self,
        mut left: Vec<Value>,
        mut right: Vec<Value>,
    ) -> Result<Option<Tuple>> {
        let values = match self.join_type {
            JoinType::Inner | JoinType::Left => {
                left.append(&mut right);
                left
            }
            JoinType::Right => {
                right.append(&mut left);
                right
            }
        };
        Ok(Some(Tuple::new(values)))
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        if self.left_tuple.is_none() {
            self.left_had_match = false;
            self.left_tuple = self.left_child.next().transpose()?;
            self.right_child.rewind()?;
        }
        while let Some(ref left_tuple) = self.left_tuple {
            while let Some(right_tuple) = self.right_child.next().transpose()? {
                if self.join_condition_evaluates_to_true(left_tuple, &right_tuple) {
                    self.left_had_match = true;
                    return self.construct_result(left_tuple.values.clone(), right_tuple.values);
                }
            }

            if !self.left_had_match && self.join_type.is_outer() {
                let left_values = left_tuple.values.clone();
                let right_null_values = (0..self.right_child.schema().columns().len())
                    .map(|_| Value::Null)
                    .collect();
                self.left_tuple = None;
                return self.construct_result(left_values, right_null_values);
            } else {
                self.left_had_match = false;
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

    use crate::catalog::schema::{ColumnDefinition, TypeId};
    use crate::executors::tests::TestDb;
    use crate::tuple::value::Value;
    use crate::tuple::Tuple;

    fn prepare_tables(test_db: &TestDb) {
        test_db
            .create_table(
                "numbers",
                vec![
                    ColumnDefinition::new(TypeId::Integer, "id".to_owned(), 0, true),
                    ColumnDefinition::new(TypeId::Integer, "number".to_owned(), 1, true),
                ],
            )
            .unwrap();

        test_db
            .create_table(
                "strings",
                vec![
                    ColumnDefinition::new(TypeId::Integer, "id".to_owned(), 0, true),
                    ColumnDefinition::new(TypeId::Text, "string".to_owned(), 1, true),
                ],
            )
            .unwrap();

        let insert_numbers = "insert into numbers values (1, 1), (2, 2), (3, 3), (4, 4)";
        test_db.execute_query(insert_numbers).unwrap();

        let insert_strings = "insert into strings values (1, 'foo'), (2, 'bar'), (3, 'baz')";
        test_db.execute_query(insert_strings).unwrap();
    }

    #[test]
    fn can_execute_cross_joins() {
        let test_db = TestDb::new();
        prepare_tables(&test_db);

        let cross_join = "select number, string from numbers, strings";
        let mut result = test_db.execute_query(cross_join).unwrap();
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
            Tuple::new(vec![Value::Integer(4), Value::String("bar".to_owned())]),
            Tuple::new(vec![Value::Integer(4), Value::String("baz".to_owned())]),
            Tuple::new(vec![Value::Integer(4), Value::String("foo".to_owned())]),
        ];

        assert_eq!(expected_result, result);
    }

    #[test]
    fn conditions_on_cross_joins() {
        let test_db = TestDb::new();
        prepare_tables(&test_db);

        let cross_join =
            "select number, string from numbers, strings where numbers.id = strings.id";
        let mut result = test_db.execute_query(cross_join).unwrap();
        result.sort_by_key(|tuple| (tuple.values()[0].as_i32()));

        let expected_result = vec![
            Tuple::new(vec![Value::Integer(1), Value::String("foo".to_owned())]),
            Tuple::new(vec![Value::Integer(2), Value::String("bar".to_owned())]),
            Tuple::new(vec![Value::Integer(3), Value::String("baz".to_owned())]),
        ];

        assert_eq!(expected_result, result);
    }

    #[test]
    fn can_execute_inner_joins() {
        let test_db = TestDb::new();
        prepare_tables(&test_db);

        let inner_join = "select number, string from numbers n join strings s on n.id = s.id";
        let mut result = test_db.execute_query(inner_join).unwrap();
        result.sort_by_key(|tuple| (tuple.values()[0].as_i32()));

        let expected_result = vec![
            Tuple::new(vec![Value::Integer(1), Value::String("foo".to_owned())]),
            Tuple::new(vec![Value::Integer(2), Value::String("bar".to_owned())]),
            Tuple::new(vec![Value::Integer(3), Value::String("baz".to_owned())]),
        ];

        assert_eq!(expected_result, result);
    }

    #[test]
    fn can_execute_left_joins() {
        let test_db = TestDb::new();
        prepare_tables(&test_db);

        let left_join = "select number, string from numbers n left join strings s on n.id = s.id";
        let mut result = test_db.execute_query(left_join).unwrap();
        result.sort_by_key(|tuple| (tuple.values()[0].as_i32()));

        let expected_result = vec![
            Tuple::new(vec![Value::Integer(1), Value::String("foo".to_owned())]),
            Tuple::new(vec![Value::Integer(2), Value::String("bar".to_owned())]),
            Tuple::new(vec![Value::Integer(3), Value::String("baz".to_owned())]),
            Tuple::new(vec![Value::Integer(4), Value::Null]),
        ];

        assert_eq!(expected_result, result);
    }

    #[test]
    fn can_execute_right_joins() {
        let test_db = TestDb::new();
        prepare_tables(&test_db);

        let right_join = "select string, number from strings s right join numbers n on n.id = s.id";
        let mut result = test_db.execute_query(right_join).unwrap();
        result.sort_by_key(|tuple| (tuple.values()[1].as_i32()));

        let expected_result = vec![
            Tuple::new(vec![Value::String("foo".to_owned()), Value::Integer(1)]),
            Tuple::new(vec![Value::String("bar".to_owned()), Value::Integer(2)]),
            Tuple::new(vec![Value::String("baz".to_owned()), Value::Integer(3)]),
            Tuple::new(vec![Value::Null, Value::Integer(4)]),
        ];

        assert_eq!(expected_result, result);
    }
}
