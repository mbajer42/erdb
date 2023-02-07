use crate::catalog::schema::{Schema};
use crate::common::TableId;
use crate::tuple::value::Value;
use crate::tuple::Tuple;

#[derive(Debug, PartialEq)]
pub enum QueryType {
    Select,
}

#[derive(Debug, PartialEq)]
pub enum Expr {
    ColumnReference(u8),
}

impl Expr {
    pub fn evaluate(&self, tuple: &Tuple) -> Value {
        match self {
            Expr::ColumnReference(col) => tuple.values().get(*col as usize).unwrap().clone(),
        }
    }
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
    /// schema of the query output
    pub output_schema: Schema,
}
