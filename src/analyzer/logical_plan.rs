use lazy_static::lazy_static;

use crate::catalog::schema::Schema;
use crate::common::TableId;
use crate::parser::ast::{self};

lazy_static! {
    pub static ref EMPTY_SCHEMA: Schema = Schema::new(vec![]);
}

#[derive(Debug, PartialEq)]
pub enum LogicalExpr {
    /// A fully specified column
    Column(Vec<String>),
    Integer(i32),
    String(String),
    Boolean(bool),
    Null,
    Unary {
        op: ast::UnaryOperator,
        expr: Box<LogicalExpr>,
    },
    Binary {
        left: Box<LogicalExpr>,
        op: ast::BinaryOperator,
        right: Box<LogicalExpr>,
    },
    IsNull(Box<LogicalExpr>),
    IsNotNull(Box<LogicalExpr>),
}

#[derive(Debug, PartialEq)]
pub enum TableReference {
    BaseTable {
        table_id: TableId,
        name: String,
        schema: Schema,
    },
    Join {
        left: Box<TableReference>,
        right: Box<TableReference>,
        on: Vec<LogicalExpr>,
    },
    EmptyTable,
}

#[derive(Debug, PartialEq)]
pub enum LogicalPlan {
    Insert {
        query: Query,
        target: TableId,
        target_schema: Schema,
    },
    Select(Query),
}

#[derive(Debug, PartialEq)]
pub struct Query {
    /// VALUES
    pub values: Vec<Vec<LogicalExpr>>,
    /// FROM clause
    pub from: TableReference,
    /// SELECT list
    pub projections: Vec<LogicalExpr>,
    /// WHERE clause
    pub filter: Option<LogicalExpr>,
    /// the output schema of the query
    pub output_schema: Schema,
}
