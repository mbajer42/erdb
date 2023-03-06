use anyhow::Result;
use lazy_static::lazy_static;

use super::Executor;
use crate::catalog::schema::{ColumnDefinition, Schema, TypeId};
use crate::concurrency::Transaction;
use crate::storage::heap::table::Table;
use crate::tuple::value::Value;
use crate::tuple::Tuple;

lazy_static! {
    static ref INSERT_EXECUTOR_SCHEMA: Schema = Schema::new(vec![ColumnDefinition::new(
        TypeId::Integer,
        "inserted".to_owned(),
        0,
        true
    )]);
}
pub struct InsertExecutor<'a> {
    table: &'a Table,
    child: Box<dyn Executor + 'a>,
    transaction: &'a Transaction<'a>,
    tuples_inserted: i32,
    done: bool,
}

impl<'a> InsertExecutor<'a> {
    pub fn new(
        table: &'a Table,
        child: Box<dyn Executor + 'a>,
        transaction: &'a Transaction,
    ) -> Self {
        Self {
            table,
            child,
            tuples_inserted: 0,
            done: false,
            transaction,
        }
    }

    fn try_insert(&mut self) -> Result<()> {
        while let Some(tuple) = self.child.next().transpose()? {
            self.table.insert_tuple(&tuple, self.transaction)?;
            self.tuples_inserted += 1;
        }
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        if self.done {
            Ok(None)
        } else {
            self.done = true;
            match self.try_insert() {
                Err(e) => {
                    if self.transaction.auto_commit() {
                        self.transaction.abort()?;
                    } else {
                        self.transaction.expect_rollback();
                    }
                    return Err(e);
                }
                Ok(()) => {
                    if self.transaction.auto_commit() {
                        self.transaction.commit()?;
                    }
                }
            };
            Ok(Some(Tuple::new(vec![Value::Integer(self.tuples_inserted)])))
        }
    }
}

impl<'a> Executor for InsertExecutor<'a> {
    fn schema(&self) -> &Schema {
        &INSERT_EXECUTOR_SCHEMA
    }

    fn next(&mut self) -> Option<Result<Tuple>> {
        self.next().transpose()
    }

    fn rewind(&mut self) -> Result<()> {
        unreachable!()
    }
}

#[cfg(test)]
mod tests {
    use crate::catalog::schema::{ColumnDefinition, TypeId};
    use crate::concurrency::IsolationLevel;
    use crate::executors::tests::TestDb;

    #[test]
    fn can_insert_from_own_table() {
        let test_db = TestDb::new();
        test_db
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

        let insert_statement = "insert into numbers values (1), (3), (5), (7), (9)";
        test_db.execute_query(insert_statement).unwrap();

        let insert_statement = "insert into numbers select number+1 from numbers";
        test_db.execute_query(insert_statement).unwrap();

        let select = "select number from numbers";
        let mut result = test_db
            .execute_query(select)
            .unwrap()
            .iter()
            .map(|tuple| tuple.values()[0].as_i32())
            .collect::<Vec<i32>>();
        result.sort();

        let expected_numbers = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

        assert_eq!(result, expected_numbers);
    }

    #[test]
    fn repeatable_read_sees_own_inserted_values() {
        let test_db = TestDb::new();
        test_db
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

        let mut insert_transaction = test_db
            .transaction_manager
            .start_transaction(Some(IsolationLevel::RepeatableRead))
            .unwrap();
        test_db
            .execute_query_with_transaction(
                "insert into numbers values (1), (2), (3)",
                &insert_transaction,
            )
            .unwrap();
        test_db
            .transaction_manager
            .refresh_transaction(&mut insert_transaction)
            .unwrap();

        let mut rows = test_db
            .execute_query_with_transaction("select * from numbers", &insert_transaction)
            .unwrap()
            .iter()
            .map(|tuple| tuple.values[0].as_i32())
            .collect::<Vec<_>>();
        rows.sort();

        assert_eq!(rows, vec![1, 2, 3]);
    }
}
