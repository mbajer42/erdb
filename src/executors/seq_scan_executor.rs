use anyhow::Result;

use super::Executor;
use crate::catalog::schema::Schema;
use crate::storage::heap::table::{HeapTupleIterator, Table};
use crate::tuple::Tuple;

pub struct SeqScanExecutor<'a> {
    table: &'a Table<'a>,
    table_iter: HeapTupleIterator<'a>,
}

impl<'a> SeqScanExecutor<'a> {
    pub fn new(table: &'a Table<'a>) -> Result<Self> {
        Ok(Self {
            table,
            table_iter: table.iter()?,
        })
    }
}

impl<'a> Executor for SeqScanExecutor<'a> {
    fn next(&mut self) -> Option<Result<Tuple>> {
        self.table_iter.next()
    }

    fn rewind(&mut self) -> Result<()> {
        let iter = self.table.iter()?;
        self.table_iter = iter;
        Ok(())
    }

    fn schema(&self) -> &Schema {
        self.table.schema()
    }
}
