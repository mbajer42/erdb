use anyhow::Result;
use lazy_static::lazy_static;

use super::Executor;
use crate::catalog::schema::{ColumnDefinition, Schema, TypeId};
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
    tuples_inserted: i32,
    done: bool,
}

impl<'a> InsertExecutor<'a> {
    pub fn new(table: &'a Table<'a>, child: Box<dyn Executor + 'a>) -> Self {
        Self {
            table,
            child,
            tuples_inserted: 0,
            done: false,
        }
    }

    fn next(&mut self) -> Result<Option<Tuple>> {
        if self.done {
            Ok(None)
        } else {
            self.done = true;
            while let Some(tuple) = self.child.next().transpose()? {
                self.table.insert_tuple(&tuple)?;
                self.tuples_inserted += 1;
            }
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
