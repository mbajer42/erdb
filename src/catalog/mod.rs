use std::collections::{hash_map, HashMap};
use std::sync::atomic::{AtomicU16, Ordering};

use anyhow::{Error, Result};
use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use lazy_static::lazy_static;

use crate::buffer::buffer_manager::BufferManager;
use crate::catalog::schema::{ColumnDefinition, Schema, TypeId};
use crate::common::{
    TableId, CATALOG_COLUMNS_TABLE_ID, CATALOG_TABLES_TABLE_ID, USER_DATA_TABLE_ID_START,
};
use crate::concurrency::Transaction;
use crate::storage::heap::table::Table;
use crate::tuple::value::Value;
use crate::tuple::Tuple;

pub mod schema;

const CATALOG_TABLES_NAME: &str = "system_catalog_tables";
const CATALOG_COLUMNS_NAME: &str = "system_catalog_columns";

lazy_static! {
    static ref CATALOG_TABLES_SCHEMA: Schema = Schema::new(vec![
        ColumnDefinition::new(TypeId::Integer, "table_id".to_owned(), 0, true),
        ColumnDefinition::new(TypeId::Text, "table_name".to_owned(), 1, true),
    ]);
    static ref CATALOG_COLUMNS_SCHEMA: Schema = Schema::new(vec![
        ColumnDefinition::new(TypeId::Integer, "table_id".to_owned(), 0, true),
        ColumnDefinition::new(TypeId::Text, "column_name".to_owned(), 1, true),
        ColumnDefinition::new(TypeId::Integer, "column_offset".to_owned(), 2, true),
        ColumnDefinition::new(TypeId::Text, "column_type".to_owned(), 3, true),
        ColumnDefinition::new(TypeId::Boolean, "not_null".to_owned(), 4, true)
    ]);
}

// TODO: The catalog defies currently MVCC. Fix it.
pub struct Catalog<'a> {
    buffer_manager: &'a BufferManager,
    next_table_id: AtomicU16,
    tables_table: Table<'a>,
    columns_table: Table<'a>,
    table_name_to_id: DashMap<String, TableId>,
    table_id_to_schema: DashMap<TableId, Schema>,
}

impl<'a> Catalog<'a> {
    pub fn new(
        buffer_manager: &'a BufferManager,
        bootstrap: bool,
        bootstrap_transaction: &Transaction,
    ) -> Result<Self> {
        let tables_table = Table::new(
            CATALOG_TABLES_TABLE_ID,
            buffer_manager,
            CATALOG_TABLES_SCHEMA.clone(),
        );

        let columns_table = Table::new(
            CATALOG_COLUMNS_TABLE_ID,
            buffer_manager,
            CATALOG_COLUMNS_SCHEMA.clone(),
        );

        let this = Self {
            buffer_manager,
            next_table_id: AtomicU16::new(USER_DATA_TABLE_ID_START),
            tables_table,
            columns_table,
            table_name_to_id: DashMap::new(),
            table_id_to_schema: DashMap::new(),
        };

        if bootstrap {
            this.create_catalog_tables(bootstrap_transaction)?;
        } else {
            this.load_tables(bootstrap_transaction)?;
        }

        Ok(this)
    }

    pub fn get_table_id(&self, table_name: &str) -> Option<TableId> {
        self.table_name_to_id.get(table_name).map(|kv| *kv.value())
    }

    pub fn get_schema(&self, table_name: &str) -> Option<Schema> {
        self.table_name_to_id.get(table_name).and_then(|id| {
            self.table_id_to_schema
                .get(id.value())
                .map(|schema| schema.value().clone())
        })
    }

    fn create_catalog_tables(&self, bootstrap_transaction: &Transaction) -> Result<()> {
        self.buffer_manager.create_table(CATALOG_TABLES_TABLE_ID)?;
        self.buffer_manager.create_table(CATALOG_COLUMNS_TABLE_ID)?;

        self.persist_table(
            CATALOG_TABLES_TABLE_ID,
            CATALOG_TABLES_NAME,
            bootstrap_transaction,
        )?;
        self.persist_table(
            CATALOG_COLUMNS_TABLE_ID,
            CATALOG_COLUMNS_NAME,
            bootstrap_transaction,
        )?;
        self.persist_columns(
            CATALOG_TABLES_TABLE_ID,
            CATALOG_TABLES_SCHEMA.columns(),
            bootstrap_transaction,
        )?;
        self.persist_columns(
            CATALOG_COLUMNS_TABLE_ID,
            CATALOG_COLUMNS_SCHEMA.columns(),
            bootstrap_transaction,
        )?;

        Ok(())
    }
    pub fn list_tables(&self) -> Vec<String> {
        self.table_name_to_id
            .iter()
            .map(|s| s.key().to_owned())
            .collect()
    }

    fn load_tables(&self, bootstrap_transaction: &Transaction) -> Result<()> {
        let mut next_table_id = self.next_table_id.load(Ordering::Relaxed);
        for table in self.tables_table.iter(bootstrap_transaction)? {
            let table = table?;
            let name = table.as_str(1).to_owned();
            let id = table.as_i32(0) as u16;
            next_table_id = next_table_id.max(id + 1);
            self.table_name_to_id.insert(name, id);
        }

        let mut table_id_to_columns: HashMap<TableId, Vec<ColumnDefinition>> = HashMap::new();
        for column in self.columns_table.iter(bootstrap_transaction)? {
            let column = column?;
            let table_id = column.as_i32(0) as u16;
            let column_definition = column.into();
            match table_id_to_columns.entry(table_id) {
                hash_map::Entry::Occupied(cols) => cols.into_mut().push(column_definition),
                hash_map::Entry::Vacant(v) => {
                    v.insert(vec![column_definition]);
                }
            };
        }

        for (table_id, mut columns) in table_id_to_columns.into_iter() {
            columns.sort_by_key(|a| a.column_offset());
            self.table_id_to_schema
                .insert(table_id, Schema::new(columns));
        }

        Ok(())
    }

    pub fn create_table(
        &self,
        table_name: &str,
        columns: Vec<ColumnDefinition>,
        transaction: &Transaction,
    ) -> Result<()> {
        match self.table_name_to_id.entry(table_name.to_owned()) {
            Entry::Occupied(_) => {
                return Err(Error::msg(format!(
                    "Table with name {} already exists",
                    table_name
                )))
            }
            Entry::Vacant(vacant) => {
                let table_id = self.generate_table_id()?;
                self.buffer_manager.create_table(table_id)?;

                self.persist_table(table_id, table_name, transaction)?;
                self.persist_columns(table_id, &columns, transaction)?;

                self.table_id_to_schema
                    .insert(table_id, Schema::new(columns));

                vacant.insert(table_id);
            }
        };
        Ok(())
    }

    fn persist_columns(
        &self,
        table_id: TableId,
        columns: &[ColumnDefinition],
        transaction: &Transaction,
    ) -> Result<()> {
        for column in columns {
            let values = vec![
                Value::Integer(table_id as i32),
                Value::String(column.column_name().to_owned()),
                Value::Integer(column.column_offset() as i32),
                Value::String(column.type_id().to_string()),
                Value::Boolean(column.not_null()),
            ];
            let tuple = Tuple::new(values);
            self.columns_table.insert_tuple(&tuple, transaction)?;
        }
        Ok(())
    }

    fn generate_table_id(&self) -> Result<u16> {
        self.next_table_id
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |prev| {
                if prev == u16::MAX {
                    None
                } else {
                    Some(prev + 1)
                }
            })
            .map_err(|_| Error::msg("Cannot create new table. TableId space is already exhausted"))
    }

    fn persist_table(
        &self,
        table_id: TableId,
        table_name: &str,
        transaction: &Transaction,
    ) -> Result<()> {
        let table_tuple = Tuple::new(vec![
            Value::Integer(table_id as i32),
            Value::String(table_name.to_owned()),
        ]);
        self.tables_table.insert_tuple(&table_tuple, transaction)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use anyhow::Result;
    use tempfile::tempdir;

    use super::{Catalog, CATALOG_TABLES_SCHEMA};
    use crate::buffer::buffer_manager::BufferManager;
    use crate::catalog::schema::{ColumnDefinition, Schema, TypeId};
    use crate::catalog::{CATALOG_COLUMNS_NAME, CATALOG_COLUMNS_SCHEMA, CATALOG_TABLES_NAME};
    use crate::concurrency::TransactionManager;
    use crate::storage::file_manager::FileManager;

    #[test]
    fn can_create_system_tables() -> Result<()> {
        let data_dir = tempdir()?;
        let file_manager = FileManager::new(data_dir.path())?;
        let buffer_manager = Arc::new(BufferManager::new(file_manager, 2));
        let transaction_manager =
            TransactionManager::new(Arc::clone(&buffer_manager), true).unwrap();
        let bootstrap_transaction = transaction_manager.bootstrap();

        let _ = Catalog::new(&buffer_manager, true, &bootstrap_transaction)?;
        bootstrap_transaction.commit()?;
        let catalog = Catalog::new(&buffer_manager, false, &bootstrap_transaction)?;

        let expected_tables_schema: Option<Schema> = Some(CATALOG_TABLES_SCHEMA.clone());
        assert_eq!(
            catalog.get_schema(CATALOG_TABLES_NAME),
            expected_tables_schema
        );

        let expected_columns_schema: Option<Schema> = Some(CATALOG_COLUMNS_SCHEMA.clone());
        assert_eq!(
            catalog.get_schema(CATALOG_COLUMNS_NAME),
            expected_columns_schema
        );

        Ok(())
    }

    #[test]
    fn can_create_user_table() -> Result<()> {
        let data_dir = tempdir()?;
        let file_manager = FileManager::new(data_dir.path())?;
        let buffer_manager = Arc::new(BufferManager::new(file_manager, 2));
        let transaction_manager =
            TransactionManager::new(Arc::clone(&buffer_manager), true).unwrap();
        let bootstrap_transaction = transaction_manager.bootstrap();
        let catalog = Catalog::new(&buffer_manager, true, &bootstrap_transaction)?;
        bootstrap_transaction.commit().unwrap();

        let expected_columns = vec![
            ColumnDefinition::new(TypeId::Integer, "id".to_owned(), 0, true),
            ColumnDefinition::new(TypeId::Text, "name".to_owned(), 1, true),
            ColumnDefinition::new(TypeId::Boolean, "blocked".to_owned(), 2, true),
            ColumnDefinition::new(TypeId::Text, "email".to_owned(), 3, false),
        ];

        let transaction = transaction_manager.start_transaction(None)?;
        catalog.create_table("accounts", expected_columns.clone(), &transaction)?;
        transaction.commit()?;
        let fetched_columns = catalog.get_schema("accounts");
        assert!(fetched_columns.is_some());
        let fetched_columns = fetched_columns.unwrap();
        assert_eq!(fetched_columns.columns(), &expected_columns);

        Ok(())
    }
}
