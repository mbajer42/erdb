use anyhow::Result;
use lazy_static::lazy_static;

use super::Executor;
use crate::catalog::schema::{ColumnDefinition, Schema, TypeId};
use crate::concurrency::Transaction;
use crate::storage::heap::table::{HeapTupleUpdateResult, Table};
use crate::tuple::value::Value;
use crate::tuple::Tuple;

lazy_static! {
    static ref DELETE_EXECUTOR_SCHEMA: Schema = Schema::new(vec![ColumnDefinition::new(
        TypeId::Integer,
        "deleted".to_owned(),
        0,
        true
    )]);
}

pub struct DeleteExecutor<'a> {
    table: &'a Table<'a>,
    child: Box<dyn Executor + 'a>,
    transaction: &'a Transaction<'a>,
    tuples_deleted: i32,
    done: bool,
}

impl<'a> DeleteExecutor<'a> {
    pub fn new(
        table: &'a Table<'a>,
        child: Box<dyn Executor + 'a>,
        transaction: &'a Transaction,
    ) -> Self {
        Self {
            table,
            child,
            transaction,
            tuples_deleted: 0,
            done: false,
        }
    }

    fn try_delete(&mut self) -> Result<()> {
        while let Some(tuple) = self.child.next().transpose()? {
            let delete_result = self.table.delete_tuple(tuple.tuple_id, self.transaction)?;

            match delete_result {
                HeapTupleUpdateResult::Ok => self.tuples_deleted += 1,
                HeapTupleUpdateResult::Deleted | HeapTupleUpdateResult::SelfUpdated => (),
                _ => unreachable!(),
            };
        }

        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        if self.done {
            Ok(None)
        } else {
            self.done = true;
            match self.try_delete() {
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
            Ok(Some(Tuple::new(vec![Value::Integer(self.tuples_deleted)])))
        }
    }
}

impl<'a> Executor for DeleteExecutor<'a> {
    fn schema(&self) -> &Schema {
        &DELETE_EXECUTOR_SCHEMA
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
    use crate::executors::tests::{EmptyTestContext, ExecutionTestContext};

    #[test]
    fn can_execute_delete_statements() {
        let empty_test_context = EmptyTestContext::new();
        let execution_test_context = ExecutionTestContext::new(&empty_test_context);
        execution_test_context
            .create_table(
                "items",
                vec![
                    ColumnDefinition::new(TypeId::Text, "name".to_owned(), 0, true),
                    ColumnDefinition::new(TypeId::Integer, "count".to_owned(), 1, true),
                ],
            )
            .unwrap();

        let insert_statement = "insert into items values ('foo', 0), ('bar', 2), ('baz', 0)";
        execution_test_context
            .execute_query(insert_statement)
            .unwrap();

        let delete = "delete from items where count = 0";
        let result = execution_test_context
            .execute_query(delete)
            .unwrap()
            .iter()
            .map(|tuple| tuple.values()[0].as_i32())
            .collect::<Vec<i32>>();
        assert_eq!(vec![2], result);
    }
}
