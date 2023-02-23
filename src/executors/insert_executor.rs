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
    table: &'a Table<'a>,
    child: Box<dyn Executor + 'a>,
    transaction: &'a Transaction<'a>,
    tuples_inserted: i32,
    done: bool,
}

impl<'a> InsertExecutor<'a> {
    pub fn new(
        table: &'a Table<'a>,
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

    fn next(&mut self) -> Result<Option<Tuple>> {
        if self.done {
            Ok(None)
        } else {
            self.done = true;
            while let Some(tuple) = self.child.next().transpose()? {
                self.table.insert_tuple(&tuple, self.transaction.tid())?;
                self.tuples_inserted += 1;
            }
            self.transaction.commit()?;
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
