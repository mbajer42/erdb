use crate::catalog::schema::{Schema, TypeId};
use crate::common::TableId;

#[derive(Debug, PartialEq)]
pub enum QueryType {
    Select,
}

#[derive(Debug, PartialEq)]
pub enum Expr {
    ColumnReference(u8),
}

#[derive(Debug, PartialEq)]
pub enum Table {
    TableReference { table_id: TableId, schema: Schema },
}

impl Table {
    pub fn schema(&self) -> &Schema {
        match self {
            Table::TableReference {
                table_id: _,
                schema,
            } => schema,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Query {
    pub query_type: QueryType,
    /// FROM clause
    pub from: Table,
    /// SELECT list
    pub projections: Vec<Expr>,
    /// name and type of a projection
    pub projection_specification: Vec<(String, TypeId)>,
}
