use lazy_static::lazy_static;

use crate::tuple::schema::{ColumnDefinition, Schema, TypeId};

lazy_static! {
    static ref CATALOG_TABLES_SCHEMA: Schema = Schema::new(vec![
        ColumnDefinition::new(TypeId::Integer, "table_id".to_owned(), 0, true),
        ColumnDefinition::new(TypeId::Text, "table_name".to_owned(), 1, true),
    ]);
    static ref CATALOG_COLUMNS_SCHEMA: Schema = Schema::new(vec![
        ColumnDefinition::new(TypeId::Integer, "table_id".to_owned(), 0, true),
        ColumnDefinition::new(TypeId::Text, "column_name".to_owned(), 1, true),
        ColumnDefinition::new(TypeId::Integer, "column_offset".to_owned(), 2, true),
        ColumnDefinition::new(TypeId::Integer, "column_type".to_owned(), 3, true),
        ColumnDefinition::new(TypeId::Boolean, "not_null".to_owned(), 4, true)
    ]);
}
