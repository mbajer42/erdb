use std::ops::DerefMut;

use anyhow::{Error, Result};
use lazy_static::lazy_static;

use super::tuple::{
    parse_heap_tuple, parse_heap_tuple_header, required_free_space, serialize_heap_tuple,
    MAX_TUPLE_SIZE,
};
use crate::buffer::buffer_manager::{BufferGuard, BufferManager};
use crate::catalog::schema::Schema;
use crate::common::{PageNo, TableId, INVALID_PAGE_NO, PAGE_SIZE};
use crate::concurrency::Transaction;
use crate::storage::common::{PageHeader, TUPLE_SLOT_SIZE};
use crate::tuple::Tuple;

lazy_static! {
    static ref EMPTY_HEAP_PAGE: [u8; PAGE_SIZE as usize] = {
        let mut data = [0u8; PAGE_SIZE as usize];
        let empty_header = PageHeader::empty();
        empty_header.serialize(&mut data);
        data
    };
}

pub struct HeapTupleIterator<'a> {
    curr_page_no: PageNo,
    max_page_no: PageNo,
    curr_slot: u8,
    table: &'a Table<'a>,
    transaction: &'a Transaction<'a>,
}

impl<'a> HeapTupleIterator<'a> {
    fn new(max_page_no: PageNo, table: &'a Table<'a>, transaction: &'a Transaction<'a>) -> Self {
        Self {
            curr_page_no: 1,
            max_page_no,
            curr_slot: 0,
            table,
            transaction,
        }
    }

    fn fetch_next_tuple(&mut self) -> Result<Option<Tuple>> {
        loop {
            if self.curr_page_no > self.max_page_no {
                return Ok(None);
            }
            let page = self.table.fetch_page(self.curr_page_no)?;
            let data = page.read();
            let page_header = PageHeader::parse(&data);
            let slots = page_header.slots();
            if self.curr_slot == slots {
                self.curr_page_no += 1;
                self.curr_slot = 0;
            } else {
                let (offset, _size) = PageHeader::tuple_slot(&data, self.curr_slot);
                self.curr_slot += 1;

                let tuple_data = &(&data)[offset as usize..];
                let header = parse_heap_tuple_header(tuple_data, &self.table.schema);
                if self.transaction.is_tuple_visible(
                    header.insert_tid(),
                    header.command_id(),
                    header.delete_tid(),
                )? {
                    let tuple =
                        parse_heap_tuple(&(&data)[offset as usize..], &header, &self.table.schema);
                    return Ok(Some(tuple));
                }
            }
        }
    }
}

impl<'a> std::iter::Iterator for HeapTupleIterator<'a> {
    type Item = Result<Tuple>;

    fn next(&mut self) -> Option<Self::Item> {
        self.fetch_next_tuple().transpose()
    }
}

fn insert_tuple(
    buffer: &mut [u8],
    tuple_size: u16,
    tuple: &Tuple,
    transaction: &Transaction,
) -> bool {
    let mut header = PageHeader::parse(buffer);
    if header.free_space() < tuple_size + TUPLE_SLOT_SIZE {
        return false;
    }
    let tuple_start = header.add_tuple_slot(buffer, tuple_size);
    serialize_heap_tuple(
        &mut buffer[tuple_start as usize..],
        tuple,
        transaction.tid(),
        transaction.command_id(),
    );
    header.serialize(buffer);

    true
}
pub struct Table<'a> {
    table_id: TableId,
    buffer_manager: &'a BufferManager,
    schema: Schema,
}

impl<'a> Table<'a> {
    pub fn new(table_id: TableId, buffer_manager: &'a BufferManager, schema: Schema) -> Self {
        Self {
            table_id,
            buffer_manager,
            schema,
        }
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    fn fetch_page(&self, page_no: PageNo) -> Result<BufferGuard> {
        let buffer = self.buffer_manager.fetch((self.table_id, page_no))?;
        match buffer {
            None => Err(Error::msg(format!(
                "Could not fetch page {} for table {}. All buffers in buffer manager are pinned.",
                page_no, self.table_id
            ))),
            Some(buffer) => Ok(buffer),
        }
    }

    fn allocate_new_page(&self) -> Result<BufferGuard> {
        let buffer = self
            .buffer_manager
            .allocate_new_page(self.table_id, EMPTY_HEAP_PAGE.as_slice())?;
        match buffer {
            None => Err(Error::msg(format!(
                "Could not allocate new page for table {}. All buffers in buffer manager are pinned.",
                self.table_id
            ))),
            Some(buffer) => Ok(buffer),
        }
    }

    pub fn insert_tuple(&self, tuple: &Tuple, transaction: &Transaction) -> Result<()> {
        let required_size = required_free_space(tuple);
        if required_size >= MAX_TUPLE_SIZE {
            return Err(Error::msg(format!(
                "Attempted to insert a tuple which would occupy {required_size} bytes."
            )));
        }

        let page_no = self.buffer_manager.highest_page_no(self.table_id)?;
        let mut buffer = if page_no == INVALID_PAGE_NO {
            self.allocate_new_page()?
        } else {
            self.fetch_page(page_no)?
        };

        loop {
            let mut data = buffer.write();
            if insert_tuple(data.deref_mut(), required_size, tuple, transaction) {
                buffer.mark_dirty();
                return Ok(());
            } else {
                drop(data);
                buffer = self.allocate_new_page()?;
            }
        }
    }

    pub fn iter(&'a self, transaction: &'a Transaction) -> Result<HeapTupleIterator<'a>> {
        let highest_page_no = self.buffer_manager.highest_page_no(self.table_id)?;
        Ok(HeapTupleIterator::new(highest_page_no, self, transaction))
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use rand::distributions::{Alphanumeric, DistString};
    use rand::Rng;
    use tempfile::tempdir;

    use super::Table;
    use crate::buffer::buffer_manager::BufferManager;
    use crate::catalog::schema::{ColumnDefinition, Schema, TypeId};
    use crate::concurrency::TransactionManager;
    use crate::storage::file_manager::FileManager;
    use crate::tuple::value::Value;
    use crate::tuple::Tuple;

    fn random_string() -> String {
        let mut rng = rand::thread_rng();
        let length = rng.gen_range(5..20);
        Alphanumeric.sample_string(&mut rng, length)
    }

    #[test]
    fn basic_test() -> Result<()> {
        let data_dir = tempdir()?;
        let file_manager = FileManager::new(data_dir.path())?;
        file_manager.create_table(1)?;
        let buffer_manager = BufferManager::new(file_manager, 2);
        let transaction_manager = TransactionManager::new(&buffer_manager, true).unwrap();

        let schema = Schema::new(vec![
            ColumnDefinition::new(TypeId::Integer, "non_null_integer".to_owned(), 0, true),
            ColumnDefinition::new(TypeId::Text, "non_null_text".to_owned(), 1, true),
            ColumnDefinition::new(TypeId::Boolean, "non_null_boolean".to_owned(), 2, true),
            ColumnDefinition::new(TypeId::Integer, "nullable_integer".to_owned(), 3, true),
        ]);

        let table = Table::new(1, &buffer_manager, schema.clone());

        let tuples = (0..10)
            .map(|i| {
                let values = vec![
                    Value::Integer(i),
                    Value::String(random_string()),
                    Value::Boolean(rand::random()),
                    if rand::random() {
                        Value::Null
                    } else {
                        Value::Integer(rand::random())
                    },
                ];
                Tuple::new(values)
            })
            .collect::<Vec<_>>();

        let transaction = transaction_manager.start_transaction().unwrap();
        for tuple in &tuples {
            table.insert_tuple(tuple, &transaction)?;
        }
        transaction.commit()?;

        let transaction = transaction_manager.start_transaction().unwrap();
        let collected_tuples = table.iter(&transaction)?.collect::<Vec<_>>();
        assert_eq!(tuples.len(), collected_tuples.len());
        for tuple in collected_tuples {
            let tuple = tuple?;
            assert_eq!(tuple.values().len(), schema.columns().len());
        }

        Ok(())
    }
}
