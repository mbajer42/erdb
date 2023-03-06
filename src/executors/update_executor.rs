use std::collections::HashMap;

use anyhow::{Error, Result};
use lazy_static::lazy_static;

use super::Executor;
use crate::catalog::schema::{ColumnDefinition, Schema, TypeId};
use crate::concurrency::{IsolationLevel, Transaction};
use crate::planner::physical_plan::Expr;
use crate::storage::heap::table::{HeapTupleUpdateResult, Table};
use crate::storage::TupleId;
use crate::tuple::value::Value;
use crate::tuple::Tuple;

lazy_static! {
    static ref UPDATE_EXECUTOR_SCHEMA: Schema = Schema::new(vec![ColumnDefinition::new(
        TypeId::Integer,
        "updated".to_owned(),
        0,
        true
    )]);
}

pub struct UpdateExecutor<'a> {
    table: &'a Table,
    child: Box<dyn Executor + 'a>,
    transaction: &'a Transaction<'a>,
    set_expressions: HashMap<usize, Expr>,
    tuples_updated: i32,
    done: bool,
}

impl<'a> UpdateExecutor<'a> {
    pub fn new(
        table: &'a Table,
        child: Box<dyn Executor + 'a>,
        set_expressions: HashMap<usize, Expr>,
        transaction: &'a Transaction,
    ) -> Self {
        Self {
            table,
            child,
            transaction,
            set_expressions,
            tuples_updated: 0,
            done: false,
        }
    }

    fn try_update_single_tuple(&mut self, mut tuple_id: TupleId, mut tuple: Tuple) -> Result<()> {
        loop {
            let values = tuple
                .values
                .iter()
                .enumerate()
                .map(|(col_idx, col_value)| {
                    self.set_expressions
                        .get(&col_idx)
                        .map(|expr| expr.evaluate(&[&tuple]))
                        .unwrap_or_else(|| col_value.clone())
                })
                .collect();
            let updated_tuple = Tuple::new(values);

            let update_result =
                self.table
                    .update_tuple(tuple_id, &updated_tuple, self.transaction)?;

            match update_result {
                HeapTupleUpdateResult::Ok => {
                    self.tuples_updated += 1;
                    return Ok(());
                }
                HeapTupleUpdateResult::Deleted => match self.transaction.isolation_level() {
                    IsolationLevel::ReadCommitted => return Ok(()),
                    IsolationLevel::RepeatableRead => {
                        return Err(Error::msg("Could not serialize due to concurrent update"))
                    }
                },
                HeapTupleUpdateResult::SelfUpdated => return Ok(()),
                HeapTupleUpdateResult::Updated(updated_tuple_id) => {
                    match self.transaction.isolation_level() {
                        IsolationLevel::ReadCommitted => {
                            tuple_id = updated_tuple_id;
                            tuple = self.table.fetch_tuple(tuple_id)?;
                            if self.child.re_evaluate_tuple(&tuple) {
                                continue;
                            } else {
                                return Ok(());
                            }
                        }
                        IsolationLevel::RepeatableRead => {
                            return Err(Error::msg("Could not serialize due to concurrent update"))
                        }
                    }
                }
                _ => unreachable!(),
            }
        }
    }

    fn try_update(&mut self) -> Result<()> {
        while let Some(tuple) = self.child.next().transpose()? {
            self.try_update_single_tuple(tuple.tuple_id, tuple)?;
        }

        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        if self.done {
            Ok(None)
        } else {
            self.done = true;
            match self.try_update() {
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
            Ok(Some(Tuple::new(vec![Value::Integer(self.tuples_updated)])))
        }
    }
}

impl<'a> Executor for UpdateExecutor<'a> {
    fn schema(&self) -> &Schema {
        &UPDATE_EXECUTOR_SCHEMA
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
    use crate::executors::tests::{EmptyTestContext, ExecutionTestContext};
    use crate::tuple::value::Value;
    use crate::tuple::Tuple;

    #[test]
    fn can_execute_update_statements() {
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

        let insert_statement = "insert into items values ('foo', 0), ('bar', 2), ('baz', 3)";
        execution_test_context
            .execute_query(insert_statement)
            .unwrap();

        let update = "update items set count = 1 where name = 'foo'";
        let result = execution_test_context
            .execute_query(update)
            .unwrap()
            .iter()
            .map(|tuple| tuple.values()[0].as_i32())
            .collect::<Vec<i32>>();
        assert_eq!(vec![1], result);

        let select = "select * from items";
        let mut result = execution_test_context.execute_query(select).unwrap();
        result.sort_by_key(|tuple| tuple.values()[1].as_i32());

        let expected_result = vec![
            Tuple::new(vec![Value::String("foo".to_owned()), Value::Integer(1)]),
            Tuple::new(vec![Value::String("bar".to_owned()), Value::Integer(2)]),
            Tuple::new(vec![Value::String("baz".to_owned()), Value::Integer(3)]),
        ];

        assert_eq!(expected_result, result);
    }

    #[test]
    fn repeatable_read_updates_fail_on_concurrent_updates() {
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

        let mut update_transaction = execution_test_context
            .context
            .transaction_manager
            .start_transaction(Some(IsolationLevel::RepeatableRead))
            .unwrap();

        let update_statement = "update numbers set number = 4 where number = 1";
        execution_test_context
            .execute_query(update_statement)
            .unwrap();

        execution_test_context
            .context
            .transaction_manager
            .refresh_transaction(&mut update_transaction)
            .unwrap();

        let result = execution_test_context
            .execute_query_with_transaction(update_statement, &update_transaction);

        assert!(result.is_err());
        assert_eq!(
            result.err().unwrap().root_cause().to_string(),
            "Could not serialize due to concurrent update".to_owned()
        );
    }
}
