use std::ops::DerefMut;

use anyhow::{Error, Result};
use lazy_static::lazy_static;

use super::header::HeapTupleHeader;
use super::tuple::{
    parse_heap_tuple, parse_heap_tuple_header, required_free_space, serialize_heap_tuple,
    MAX_TUPLE_SIZE,
};
use crate::buffer::buffer_manager::{BufferGuard, BufferManager};
use crate::catalog::schema::Schema;
use crate::common::{PageNo, TableId, INVALID_PAGE_NO, PAGE_SIZE};
use crate::concurrency::lock_manager::LockMode;
use crate::concurrency::{Transaction, TransactionStatus, INVALID_TRANSACTION_ID};
use crate::storage::utils::{PageHeader, TUPLE_SLOT_SIZE};
use crate::storage::TupleId;
use crate::tuple::Tuple;

lazy_static! {
    static ref EMPTY_HEAP_PAGE: [u8; PAGE_SIZE as usize] = {
        let mut data = [0u8; PAGE_SIZE as usize];
        let empty_header = PageHeader::empty();
        empty_header.serialize(&mut data);
        data
    };
}

/// Result codes for attempts to update/delete a tuple
#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
pub enum HeapTupleUpdateResult {
    /// Tuple can be updated by the current transaction
    Ok,
    /// Tuple has been already updated by the current transaction
    SelfUpdated,
    /// Tuple was deleted by a committed transaction
    Deleted,
    /// Tuple is being modified by an in-progress transaction
    BeingModified,
}

fn heap_tuple_satisfies_update(
    header: &HeapTupleHeader,
    transaction: &Transaction,
) -> Result<HeapTupleUpdateResult> {
    if transaction
        .manager
        .get_transaction_status(header.insert_tid)?
        != TransactionStatus::Committed
    {
        // if it wasn't inserted by the current transaction at an earlier point, something is clearly wrong
        debug_assert!(
            header.insert_tid == transaction.tid() && header.command_id < transaction.command_id()
        );
        Ok(HeapTupleUpdateResult::Ok)
    } else if header.delete_tid == INVALID_TRANSACTION_ID {
        Ok(HeapTupleUpdateResult::Ok)
    } else if header.delete_tid == transaction.tid() {
        // we already deleted it
        Ok(HeapTupleUpdateResult::SelfUpdated)
    } else {
        match transaction
            .manager
            .get_transaction_status(header.delete_tid)?
        {
            TransactionStatus::Committed => Ok(HeapTupleUpdateResult::Deleted),
            TransactionStatus::Aborted => Ok(HeapTupleUpdateResult::Ok),
            TransactionStatus::InProgress => Ok(HeapTupleUpdateResult::BeingModified),
            _ => unreachable!(),
        }
    }
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
                    header.insert_tid,
                    header.command_id,
                    header.delete_tid,
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
    page_no: PageNo,
    tuple: &Tuple,
    transaction: &Transaction,
) -> bool {
    let mut header = PageHeader::parse(buffer);
    if header.free_space() < tuple_size + TUPLE_SLOT_SIZE {
        return false;
    }
    let (slot, tuple_start) = header.add_tuple_slot(buffer, tuple_size);
    serialize_heap_tuple(
        &mut buffer[tuple_start as usize..],
        tuple,
        transaction.tid(),
        transaction.command_id(),
        (page_no, slot),
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
            if insert_tuple(data.deref_mut(), required_size, page_no, tuple, transaction) {
                buffer.mark_dirty();
                return Ok(());
            } else {
                drop(data);
                buffer = self.allocate_new_page()?;
            }
        }
    }

    pub fn delete_tuple(
        &self,
        tuple_id: TupleId,
        transaction: &Transaction,
    ) -> Result<HeapTupleUpdateResult> {
        let (page_no, slot) = tuple_id;
        let mut tuple_lock = None;
        let buffer = self.fetch_page(page_no)?;

        loop {
            let mut data = buffer.write();

            let (start, size) = PageHeader::tuple_slot(&data, slot);
            let tuple_data = &mut (&mut data)[start as usize..(start + size) as usize];
            let mut header = parse_heap_tuple_header(tuple_data, &self.schema);

            match heap_tuple_satisfies_update(&header, transaction)? {
                result @ (HeapTupleUpdateResult::SelfUpdated | HeapTupleUpdateResult::Deleted) => {
                    return Ok(result)
                }
                HeapTupleUpdateResult::Ok => {
                    // we can delete it
                    header.delete_tid = transaction.tid();
                    header.serialize(tuple_data);
                    buffer.mark_dirty();
                    return Ok(HeapTupleUpdateResult::Ok);
                }
                HeapTupleUpdateResult::BeingModified => {
                    // tuple is currently being modified by another transaction.
                    // lock this tuple, so that we have priority over it once the other transaction ends
                    let table_id_tuple_id = (self.table_id, tuple_id);
                    if tuple_lock.is_none() {
                        tuple_lock = Some(
                            transaction
                                .manager
                                .lock_manager
                                .lock_tuple(table_id_tuple_id, LockMode::Exclusive),
                        );
                    }
                    transaction.wait_for_transaction_to_end(header.delete_tid);

                    // transaction ended, retry
                }
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
    use std::sync::{Arc, Condvar, Mutex};
    use std::thread;
    use std::time::Duration;

    use anyhow::Result;
    use rand::distributions::{Alphanumeric, DistString};
    use rand::Rng;
    use tempfile::tempdir;

    use super::Table;
    use crate::buffer::buffer_manager::BufferManager;
    use crate::catalog::schema::{ColumnDefinition, Schema, TypeId};
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::TransactionManager;
    use crate::storage::file_manager::FileManager;
    use crate::storage::heap::table::HeapTupleUpdateResult;
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
        let lock_manager = LockManager::new();
        let transaction_manager =
            TransactionManager::new(&buffer_manager, &lock_manager, true).unwrap();

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

    #[test]
    fn can_delete_tuple() -> Result<()> {
        let data_dir = tempdir()?;
        let file_manager = FileManager::new(data_dir.path())?;
        file_manager.create_table(1)?;
        let buffer_manager = BufferManager::new(file_manager, 2);
        let lock_manager = LockManager::new();
        let transaction_manager =
            TransactionManager::new(&buffer_manager, &lock_manager, true).unwrap();

        let schema = Schema::new(vec![ColumnDefinition::new(
            TypeId::Integer,
            "number".to_owned(),
            0,
            true,
        )]);

        let table = Table::new(1, &buffer_manager, schema);
        let tuple = Tuple::new(vec![Value::Integer(42)]);

        let insert_transaction = transaction_manager.start_transaction()?;
        table.insert_tuple(&tuple, &insert_transaction)?;
        insert_transaction.commit()?;

        let delete_transaction = transaction_manager.start_transaction()?;
        let result = table.delete_tuple((1, 0), &delete_transaction)?;
        assert_eq!(result, HeapTupleUpdateResult::Ok);
        delete_transaction.commit()?;

        let select_transaction = transaction_manager.start_transaction()?;
        assert_eq!(table.iter(&select_transaction)?.count(), 0);

        Ok(())
    }

    #[test]
    fn can_delete_tuple_if_previous_transaction_aborted_delete() -> Result<()> {
        let data_dir = tempdir()?;
        let file_manager = FileManager::new(data_dir.path())?;
        file_manager.create_table(1)?;
        let buffer_manager = BufferManager::new(file_manager, 2);
        let lock_manager = LockManager::new();
        let transaction_manager =
            TransactionManager::new(&buffer_manager, &lock_manager, true).unwrap();

        let schema = Schema::new(vec![ColumnDefinition::new(
            TypeId::Integer,
            "number".to_owned(),
            0,
            true,
        )]);

        let table = Arc::new(Table::new(1, &buffer_manager, schema));
        let tuple = Tuple::new(vec![Value::Integer(42)]);

        let insert_transaction = transaction_manager.start_transaction()?;
        table.insert_tuple(&tuple, &insert_transaction)?;
        insert_transaction.commit()?;

        let delete_started = (Mutex::new(false), Condvar::new());
        thread::scope(|scope| {
            let transaction_manager = &transaction_manager;
            let delete_started = &delete_started;
            scope.spawn(|| {
                let delete_transaction = transaction_manager.start_transaction().unwrap();
                let result = table.delete_tuple((1, 0), &delete_transaction).unwrap();
                assert_eq!(result, HeapTupleUpdateResult::Ok);
                let (lock, condvar) = delete_started;
                let mut lock = lock.lock().unwrap();
                *lock = true;
                condvar.notify_all();

                // sleep so that other transaction can try delete
                thread::sleep(Duration::from_millis(200));
                delete_transaction.abort().unwrap();
            });

            let (lock, condvar) = delete_started;
            let _guard = condvar
                .wait_while(lock.lock().unwrap(), |delete_started| !*delete_started)
                .unwrap();

            let delete_transaction = transaction_manager.start_transaction().unwrap();
            let result = table.delete_tuple((1, 0), &delete_transaction).unwrap();
            assert_eq!(result, HeapTupleUpdateResult::Ok);
        });

        Ok(())
    }

    #[test]
    fn already_deleted_tuple_does_not_need_any_action() -> Result<()> {
        let data_dir = tempdir()?;
        let file_manager = FileManager::new(data_dir.path())?;
        file_manager.create_table(1)?;
        let buffer_manager = BufferManager::new(file_manager, 2);
        let lock_manager = LockManager::new();
        let transaction_manager =
            TransactionManager::new(&buffer_manager, &lock_manager, true).unwrap();

        let schema = Schema::new(vec![ColumnDefinition::new(
            TypeId::Integer,
            "number".to_owned(),
            0,
            true,
        )]);

        let table = Arc::new(Table::new(1, &buffer_manager, schema));
        let tuple = Tuple::new(vec![Value::Integer(42)]);

        let insert_transaction = transaction_manager.start_transaction()?;
        table.insert_tuple(&tuple, &insert_transaction)?;
        insert_transaction.commit()?;

        let delete_started = (Mutex::new(false), Condvar::new());
        thread::scope(|scope| {
            let transaction_manager = &transaction_manager;
            let delete_started = &delete_started;
            scope.spawn(|| {
                let delete_transaction = transaction_manager.start_transaction().unwrap();
                let result = table.delete_tuple((1, 0), &delete_transaction).unwrap();
                assert_eq!(result, HeapTupleUpdateResult::Ok);
                let (lock, condvar) = delete_started;
                let mut lock = lock.lock().unwrap();
                *lock = true;
                condvar.notify_all();

                // sleep so that other transaction can try delete
                thread::sleep(Duration::from_millis(200));
                delete_transaction.commit().unwrap();
            });

            let (lock, condvar) = delete_started;
            let _guard = condvar
                .wait_while(lock.lock().unwrap(), |delete_started| !*delete_started)
                .unwrap();

            let delete_transaction = transaction_manager.start_transaction().unwrap();
            let result = table.delete_tuple((1, 0), &delete_transaction).unwrap();
            assert_eq!(result, HeapTupleUpdateResult::Deleted);
        });

        Ok(())
    }
}
