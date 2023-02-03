#[derive(Debug, PartialEq)]
pub enum DataType {
    Integer,
    Text,
    Boolean,
}

#[derive(Debug, PartialEq)]
pub struct ColumnDefinition {
    pub name: String,
    pub offset: u8,
    pub data_type: DataType,
    pub not_null: bool,
}

#[derive(Debug, PartialEq)]
pub enum Statement {
    CreateTable {
        name: String,
        columns: Vec<ColumnDefinition>,
    },
}
