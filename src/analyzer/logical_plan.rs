use std::collections::HashMap;
use std::str::FromStr;

use anyhow::{Error, Result};
use lazy_static::lazy_static;

use crate::catalog::schema::{Schema, TypeId};
use crate::common::TableId;
use crate::parser::ast::{self, JoinType};

lazy_static! {
    pub static ref EMPTY_SCHEMA: Schema = Schema::new(vec![]);
}

#[derive(Debug, PartialEq)]
pub enum AggregationFunc {
    Count,
    Max,
}

impl AggregationFunc {
    /// Validates whether this aggregation can be applied to the child type
    pub fn validate_aggregation_type(&self, child_type: TypeId) -> Result<()> {
        match self {
            Self::Count => Ok(()),
            Self::Max => {
                if !&[TypeId::Text, TypeId::Integer].contains(&child_type) {
                    Err(Error::msg(format!(
                        "`max` accepts text and integer, found {}",
                        child_type
                    )))
                } else {
                    Ok(())
                }
            }
        }
    }

    /// Returns the type of the aggregation result
    pub fn aggregation_result_type(&self, child_type: TypeId) -> TypeId {
        match self {
            Self::Count => TypeId::Integer,
            Self::Max => child_type,
        }
    }
}

impl FromStr for AggregationFunc {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let res = match s {
            "count" => Self::Count,
            "max" => Self::Max,
            _ => return Err(()),
        };
        Ok(res)
    }
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
    Aggregation(AggregationFunc, Box<LogicalExpr>),
}

impl LogicalExpr {
    /// Returns whether an expression contains any aggregation calls
    pub fn has_aggregation(&self) -> bool {
        match self {
            Self::Column(_) => false,
            Self::Integer(_) => false,
            Self::String(_) => false,
            Self::Boolean(_) => false,
            Self::Null => false,
            Self::Unary { op: _, expr } => expr.has_aggregation(),
            Self::Binary { left, op: _, right } => {
                left.has_aggregation() || right.has_aggregation()
            }
            Self::IsNull(expr) => expr.has_aggregation(),
            Self::IsNotNull(expr) => expr.has_aggregation(),
            Self::Aggregation(_, _) => true,
        }
    }

    /// If this expression references any column, returns the first it finds, else None.
    /// Ignores any column references in aggregations
    pub fn find_any_referenced_column(&self) -> Option<String> {
        match self {
            Self::Column(col) => Some(col.join(".")),
            Self::Integer(_) => None,
            Self::String(_) => None,
            Self::Boolean(_) => None,
            Self::Null => None,
            Self::Unary { op: _, expr } => expr.find_any_referenced_column(),
            Self::Binary { left, op: _, right } => {
                if let Some(col_ref) = left.find_any_referenced_column() {
                    Some(col_ref)
                } else {
                    right.find_any_referenced_column()
                }
            }
            Self::IsNull(expr) => expr.find_any_referenced_column(),
            Self::IsNotNull(expr) => expr.find_any_referenced_column(),
            Self::Aggregation(_, _) => None,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum TableReference {
    BaseTable {
        table_id: TableId,
        name: String,
        schema: Schema,
        filter: Vec<LogicalExpr>,
    },
    Join {
        left: Box<TableReference>,
        right: Box<TableReference>,
        join_type: JoinType,
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
    Delete {
        from: TableReference,
        /// WHERE clause
        filter: Vec<LogicalExpr>,
    },
    Update {
        table: TableReference,
        set: HashMap<Vec<String>, LogicalExpr>,
        filter: Vec<LogicalExpr>,
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
    pub filter: Vec<LogicalExpr>,
    /// the output schema of the query
    pub output_schema: Schema,
}
