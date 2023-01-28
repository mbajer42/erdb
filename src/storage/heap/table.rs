use std::ops::{Deref, DerefMut};

use crate::{
    buffer::buffer_manager::{BufferGuard, BufferManager},
    common::{PageNo, TableId},
    storage::common::{PageHeader, Serialize, TUPLE_SLOT_SIZE},
    tuple::{schema::ColumnDefinition, Tuple},
};

use anyhow::{Error, Result};

use super::tuple::{parse_heap_tuple, required_free_space, serialize_heap_tuple, MAX_TUPLE_SIZE};

pub struct HeapTupleIterator<'a, 'b> {
    curr_page_no: PageNo,
    max_page_no: PageNo,
    curr_slot: u8,
    table: &'a Table<'b>,
}

impl<'a, 'b> HeapTupleIterator<'a, 'b> {
    fn new(max_page_no: PageNo, table: &'a Table<'b>) -> Self {
        Self {
            curr_page_no: 0,
            max_page_no,
            curr_slot: 0,
            table,
        }
    }

    fn fetch_next_tuple(&mut self) -> Result<Option<Tuple<'b>>> {
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
                let tuple = parse_heap_tuple(&(&data)[offset as usize..], self.table.columns);
                self.curr_slot += 1;

                return Ok(Some(tuple));
            }
        }
    }
}

impl<'a, 'b> std::iter::Iterator for HeapTupleIterator<'a, 'b> {
    type Item = Result<Tuple<'b>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.fetch_next_tuple().transpose()
    }
}

fn insert_tuple(buffer: &mut [u8], tuple_size: u16, tuple: &Tuple) -> bool {
    let mut header = PageHeader::parse(buffer);
    if header.free_space() < tuple_size + TUPLE_SLOT_SIZE {
        return false;
    }
    let tuple_start = header.add_tuple_slot(buffer, tuple_size);
    serialize_heap_tuple(&mut buffer[tuple_start as usize..], tuple);
    header.serialize(buffer);

    true
}
pub struct Table<'a> {
    table_id: TableId,
    buffer_manager: &'a BufferManager,
    columns: &'a [ColumnDefinition],
}

impl<'a> Table<'a> {
    pub fn new(
        table_id: TableId,
        buffer_manager: &'a BufferManager,
        columns: &'a [ColumnDefinition],
    ) -> Self {
        Self {
            table_id,
            buffer_manager,
            columns,
        }
    }

    fn fetch_page(&self, page_no: PageNo) -> Result<BufferGuard> {
        let page = self.buffer_manager.fetch((self.table_id, page_no))?;
        match page {
            None => Err(Error::msg(format!(
                "Could not fetch page {} for table {}. All buffers in buffer manager are pinned.",
                page_no, self.table_id
            ))),
            Some(buffer) => Ok(buffer),
        }
    }

    pub fn insert_tuple(&self, tuple: &Tuple) -> Result<()> {
        let required_size = required_free_space(tuple);
        if required_size >= MAX_TUPLE_SIZE {
            return Err(Error::msg(format!(
                "Attempted to insert a tuple which would occupy {required_size} bytes."
            )));
        }
        let mut page_no = self.buffer_manager.highest_page_no(self.table_id)?;
        loop {
            let page = self.fetch_page(page_no)?;
            let mut data = page.write();
            if insert_tuple(data.deref_mut(), required_size, tuple) {
                page.mark_dirty();
                return Ok(());
            } else {
                page_no += 1;
            }
        }
    }

    pub fn iter(&self) -> Result<HeapTupleIterator> {
        let highest_page_no = self.buffer_manager.highest_page_no(self.table_id)?;
        Ok(HeapTupleIterator::new(highest_page_no, self))
    }
}

#[cfg(test)]
mod tests {
    use rand::{
        distributions::{Alphanumeric, DistString},
        Rng,
    };
    use tempfile::tempdir;

    use crate::{
        buffer::buffer_manager::BufferManager,
        storage::file_manager::FileManager,
        tuple::{
            schema::{ColumnDefinition, TypeId},
            value::Value,
            Tuple,
        },
    };

    use anyhow::Result;

    use super::Table;

    fn random_string() -> String {
        let mut rng = rand::thread_rng();
        let length = rng.gen_range(5..20);
        Alphanumeric.sample_string(&mut rng, length)
    }

    #[test]
    fn basic_test() -> Result<()> {
        let data_dir = tempdir()?;
        let mut file_manager = FileManager::new(data_dir.path())?;
        file_manager.create_table(1)?;
        let buffer_manager = BufferManager::new(file_manager, 1);

        let column_definitions = vec![
            ColumnDefinition::new(TypeId::Integer, "non_null_integer".to_owned(), 0, true),
            ColumnDefinition::new(TypeId::Text, "non_null_text".to_owned(), 1, true),
            ColumnDefinition::new(TypeId::Boolean, "non_null_boolean".to_owned(), 2, true),
            ColumnDefinition::new(TypeId::Integer, "nullable_integer".to_owned(), 3, true),
        ];

        let table = Table::new(1, &buffer_manager, &column_definitions);

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
                Tuple::new(values, &column_definitions)
            })
            .collect::<Vec<_>>();

        for tuple in &tuples {
            table.insert_tuple(tuple)?;
        }

        let collected_tuples = table.iter()?.collect::<Vec<_>>();
        assert_eq!(tuples.len(), collected_tuples.len());

        Ok(())
    }
}
