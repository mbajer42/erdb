use anyhow::{Error, Result};

use self::physical_plan::{Expr, PhysicalPlan};
use crate::analyzer::logical_plan::{
    LogicalExpr, LogicalPlan, Query, TableReference, EMPTY_SCHEMA,
};
use crate::catalog::schema::Schema;
use crate::tuple::value::Value;

pub mod physical_plan;

pub struct Planner {}

impl Planner {
    pub fn new() -> Self {
        Self {}
    }

    pub fn prepare_logical_plan(&self, logical_plan: LogicalPlan) -> Result<PhysicalPlan> {
        match logical_plan {
            LogicalPlan::Select(query) => self.plan_query(query),
            LogicalPlan::Insert {
                query,
                target,
                target_schema,
            } => {
                let plan = self.plan_query(query)?;
                Ok(PhysicalPlan::InsertPlan {
                    target,
                    target_schema,
                    child: Box::new(plan),
                })
            }
        }
    }

    fn plan_query(&self, query: Query) -> Result<PhysicalPlan> {
        let Query {
            values,
            from,
            projections,
            filter,
            output_schema,
        } = query;

        if !values.is_empty() {
            Ok(PhysicalPlan::ValuesPlan {
                values: values
                    .into_iter()
                    .map(|values| self.plan_expressions(values, &[]))
                    .collect::<Result<Vec<_>>>()?,
                output_schema,
            })
        } else {
            let mut plan = Self::plan_table_reference(from);
            plan = self.plan_filter(filter, plan)?;
            plan = self.plan_projections(projections, output_schema, plan)?;
            Ok(plan)
        }
    }

    fn plan_filter(
        &self,
        filter: Option<LogicalExpr>,
        child: PhysicalPlan,
    ) -> Result<PhysicalPlan> {
        if let Some(filter) = filter {
            Ok(PhysicalPlan::FilterPlan {
                filter: self.plan_expression(filter, &[&child])?,
                child: Box::new(child),
            })
        } else {
            Ok(child)
        }
    }

    fn plan_table_reference(table: TableReference) -> PhysicalPlan {
        match table {
            TableReference::BaseTable {
                table_id,
                name,
                mut schema,
            } => {
                schema.prepend_column_name(&name);
                PhysicalPlan::SequentialScan {
                    table_id,
                    output_schema: schema,
                }
            }
            TableReference::CrossJoin { left, right } => {
                let left_child = Self::plan_table_reference(*left);
                let right_child = Self::plan_table_reference(*right);

                let mut left_columns = left_child.schema().columns().to_vec();
                let mut right_columns = right_child.schema().columns().to_vec();
                left_columns.append(&mut right_columns);
                let output_schema = Schema::new(left_columns);

                PhysicalPlan::Join {
                    left: Box::new(left_child),
                    right: Box::new(right_child),
                    output_schema,
                }
            }
            TableReference::EmptyTable => PhysicalPlan::ValuesPlan {
                values: vec![vec![]],
                output_schema: EMPTY_SCHEMA.clone(),
            },
        }
    }

    fn plan_projections(
        &self,
        projections: Vec<LogicalExpr>,
        schema: Schema,
        child: PhysicalPlan,
    ) -> Result<PhysicalPlan> {
        if projections.is_empty() {
            Ok(child)
        } else {
            Ok(PhysicalPlan::Projection {
                projections: self.plan_expressions(projections, &[&child])?,
                child: Box::new(child),
                output_schema: schema,
            })
        }
    }

    fn plan_expressions(
        &self,
        logical_expressions: Vec<LogicalExpr>,
        children: &[&PhysicalPlan],
    ) -> Result<Vec<Expr>> {
        logical_expressions
            .into_iter()
            .map(|expr| self.plan_expression(expr, children))
            .collect()
    }

    fn plan_expression(
        &self,
        logical_expr: LogicalExpr,
        children: &[&PhysicalPlan],
    ) -> Result<Expr> {
        let res = match logical_expr {
            LogicalExpr::Column(path) => self.resolve_column(path, children)?,
            LogicalExpr::Integer(num) => Expr::Value(Value::Integer(num)),
            LogicalExpr::String(s) => Expr::Value(Value::String(s)),
            LogicalExpr::Boolean(val) => Expr::Value(Value::Boolean(val)),
            LogicalExpr::Null => Expr::Value(Value::Null),
            LogicalExpr::Unary { op, expr } => Expr::Unary {
                op,
                expr: Box::new(self.plan_expression(*expr, children)?),
            },
            LogicalExpr::Binary { left, op, right } => Expr::Binary {
                left: Box::new(self.plan_expression(*left, children)?),
                op,
                right: Box::new(self.plan_expression(*right, children)?),
            },
            LogicalExpr::IsNull(expr) => {
                Expr::IsNull(Box::new(self.plan_expression(*expr, children)?))
            }
            LogicalExpr::IsNotNull(expr) => {
                Expr::IsNotNull(Box::new(self.plan_expression(*expr, children)?))
            }
        };
        Ok(res)
    }

    fn resolve_column(&self, path: Vec<String>, children: &[&PhysicalPlan]) -> Result<Expr> {
        let column_name = path.join(".");
        if children.is_empty() {
            Err(Error::msg(format!(
                "Could not resolve column {}.",
                column_name
            )))
        } else if children.len() == 1 {
            let schema = children[0].schema();
            if let Some(col) = self.find_column_offset(schema, &column_name) {
                Ok(Expr::ColumnReference {
                    tuple_idx: 0,
                    col_idx: col,
                })
            } else {
                Err(Error::msg(format!(
                    "Could not resolve column {}.",
                    column_name
                )))
            }
        } else if children.len() == 2 {
            let left = children[0].schema();
            let right = children[1].schema();

            let left = self.find_column_offset(left, &column_name);
            let right = self.find_column_offset(right, &column_name);

            if let Some(col) = left {
                if right.is_some() {
                    Err(Error::msg(format!("Column '{}' is ambiguous", column_name)))
                } else {
                    Ok(Expr::ColumnReference {
                        tuple_idx: 0,
                        col_idx: col,
                    })
                }
            } else if let Some(col) = right {
                Ok(Expr::ColumnReference {
                    tuple_idx: 1,
                    col_idx: col,
                })
            } else {
                Err(Error::msg(format!(
                    "Could not resolve column {}.",
                    column_name
                )))
            }
        } else {
            unreachable!("No physical plan has currently more than 2 children")
        }
    }

    fn find_column_offset(&self, schema: &Schema, column_name: &str) -> Option<usize> {
        schema
            .columns()
            .iter()
            .enumerate()
            .find(|(_, col_def)| col_def.column_name() == column_name)
            .map(|(pos, _)| pos)
    }
}
