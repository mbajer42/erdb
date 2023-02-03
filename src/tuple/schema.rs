use std::fmt::Display;
use std::str::FromStr;

use anyhow::Error;

use super::Tuple;
use crate::parser::ast;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum TypeId {
    Boolean,
    Integer,
    Text,
}

impl FromStr for TypeId {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Boolean" => Ok(TypeId::Boolean),
            "Integer" => Ok(TypeId::Integer),
            "Text" => Ok(TypeId::Text),
            s => Err(Error::msg(format!("Invalid TypeId {}", s))),
        }
    }
}

impl From<ast::DataType> for TypeId {
    fn from(value: ast::DataType) -> Self {
        match value {
            ast::DataType::Integer => Self::Integer,
            ast::DataType::Text => Self::Text,
            ast::DataType::Boolean => Self::Boolean,
        }
    }
}

impl Display for TypeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Clone, Debug, PartialEq)]
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

    pub fn column_name(&self) -> &str {
        &self.column_name
    }

    pub fn column_offset(&self) -> u8 {
        self.column_offset
    }

    pub fn not_null(&self) -> bool {
        self.not_null
    }
}

impl From<Tuple> for ColumnDefinition {
    fn from(tuple: Tuple) -> Self {
        Self {
            type_id: tuple.as_str(3).parse().unwrap(),
            column_name: tuple.as_str(1).to_owned(),
            column_offset: tuple.as_i32(2) as u8,
            not_null: tuple.as_bool(4),
        }
    }
}

impl From<ast::ColumnDefinition> for ColumnDefinition {
    fn from(value: ast::ColumnDefinition) -> Self {
        Self {
            type_id: value.data_type.into(),
            column_name: value.name,
            column_offset: value.offset,
            not_null: value.not_null,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Schema {
    columns: Vec<ColumnDefinition>,
}

impl Schema {
    pub fn new(columns: Vec<ColumnDefinition>) -> Self {
        Self { columns }
    }

    pub fn columns(&self) -> &[ColumnDefinition] {
        &self.columns
    }
}
