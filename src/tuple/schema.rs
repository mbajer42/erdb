#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum TypeId {
    Boolean,
    Integer,
    Text,
}

#[derive(Debug)]
pub struct ColumnDefinition {
    type_id: TypeId,
    column_name: String,
    column_offset: u8,
    not_null: bool,
}

impl ColumnDefinition {
    pub fn new(type_id: TypeId, column_name: String, column_offset: u8, not_null: bool) -> Self {
        Self {
            type_id,
            column_name,
            column_offset,
            not_null,
        }
    }

    pub fn type_id(&self) -> TypeId {
        self.type_id
    }

    pub fn column_offset(&self) -> u8 {
        self.column_offset
    }
}

#[derive(Debug)]
pub struct Schema {
    columns: Vec<ColumnDefinition>,
}

impl Schema {
    pub fn new(columns: Vec<ColumnDefinition>) -> Self {
        Self { columns }
    }
}
