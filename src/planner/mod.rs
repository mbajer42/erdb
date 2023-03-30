use std::collections::HashMap;

use anyhow::{Error, Result};

use self::physical_plan::{Aggregation, Expr, PhysicalPlan};
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
            LogicalPlan::Delete { from, filter } => self.plan_delete(from, filter),
            LogicalPlan::Update { table, set, filter } => self.plan_update(table, set, filter),
            LogicalPlan::Insert {
                query,
                target,
                target_schema,
            } => {
                let plan = self.plan_query(query)?;
                Ok(PhysicalPlan::Insert {
                    target,
                    target_schema,
                    child: Box::new(plan),
                })
            }
        }
    }

    fn plan_update(
        &self,
        table: TableReference,
        set_expressions: HashMap<Vec<String>, LogicalExpr>,
        filter: Vec<LogicalExpr>,
    ) -> Result<PhysicalPlan> {
        let table_id = match &table {
            TableReference::BaseTable {
                table_id,
                name: _,
                schema: _,
                filter: _,
            } => *table_id,
            _ => unreachable!(),
        };

        let child = self.plan_table_reference(table)?;
        let child = self.plan_filter(filter, child)?;

        let set_expressions = set_expressions
            .into_iter()
            .map(|(column, expr)| {
                let column = match self.resolve_column(column, &[&child])? {
                    Expr::ColumnReference {
                        tuple_idx: _,
                        col_idx,
                    } => col_idx,
                    _ => unreachable!(),
                };

                let expr = self.plan_expression(expr, &[&child])?;

                Ok((column, expr))
            })
            .collect::<Result<HashMap<_, _>>>()?;

        Ok(PhysicalPlan::Update {
            table: table_id,
            set: set_expressions,
            child: Box::new(child),
        })
    }

    fn plan_delete(&self, from: TableReference, filter: Vec<LogicalExpr>) -> Result<PhysicalPlan> {
        let table_id = match &from {
            TableReference::BaseTable {
                table_id,
                name: _,
                schema: _,
                filter: _,
            } => *table_id,
            _ => unreachable!(),
        };
        let child = self.plan_table_reference(from)?;
        let child = self.plan_filter(filter, child)?;

        Ok(PhysicalPlan::Delete {
            from: table_id,
            child: Box::new(child),
        })
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
            Ok(PhysicalPlan::Values {
                values: values
                    .into_iter()
                    .map(|values| self.plan_expressions(values, &[]))
                    .collect::<Result<Vec<_>>>()?,
                output_schema,
            })
        } else {
            let mut plan = self.plan_table_reference(from)?;
            plan = self.plan_filter(filter, plan)?;

            if projections.iter().any(|expr| expr.has_aggregation()) {
                let (aggregations, projections) = self.plan_aggregations(projections, &plan)?;
                plan = PhysicalPlan::Aggregate {
                    aggregations,
                    child: Box::new(plan),
                };
                plan = PhysicalPlan::Projection {
                    projections,
                    child: Box::new(plan),
                    output_schema,
                };
            } else {
                plan = self.plan_projections(projections, output_schema, plan)?;
            }
            Ok(plan)
        }
    }

    fn plan_filter(&self, filter: Vec<LogicalExpr>, child: PhysicalPlan) -> Result<PhysicalPlan> {
        if !filter.is_empty() {
            Ok(PhysicalPlan::Filter {
                filter: self.plan_expressions(filter, &[&child])?,
                child: Box::new(child),
            })
        } else {
            Ok(child)
        }
    }

    fn plan_table_reference(&self, table: TableReference) -> Result<PhysicalPlan> {
        let plan = match table {
            TableReference::BaseTable {
                table_id,
                name,
                mut schema,
                filter,
            } => {
                schema.prepend_column_name(&name);
                let seq_scan = PhysicalPlan::SequentialScan {
                    table_id,
                    output_schema: schema,
                };
                self.plan_filter(filter, seq_scan)?
            }
            TableReference::Join {
                left,
                right,
                join_type,
                on,
            } => {
                let left_child = self.plan_table_reference(*left)?;
                let right_child = self.plan_table_reference(*right)?;

                let on = self.plan_expressions(on, &[&left_child, &right_child])?;

                let mut left_columns = left_child.schema().columns().to_vec();
                let mut right_columns = right_child.schema().columns().to_vec();
                left_columns.append(&mut right_columns);
                let output_schema = Schema::new(left_columns);

                PhysicalPlan::NestedLoopJoin {
                    left: Box::new(left_child),
                    right: Box::new(right_child),
                    join_type,
                    on,
                    output_schema,
                }
            }
            TableReference::EmptyTable => PhysicalPlan::Values {
                values: vec![vec![]],
                output_schema: EMPTY_SCHEMA.clone(),
            },
        };

        Ok(plan)
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

    /// Transforms the expressions (of which at least one contains an aggregation) and returns
    /// the aggregations and their projections
    fn plan_aggregations(
        &self,
        exprs: Vec<LogicalExpr>,
        child: &PhysicalPlan,
    ) -> Result<(Vec<Aggregation>, Vec<Expr>)> {
        let mut aggregations = vec![];

        let planned_expressions = exprs
            .into_iter()
            .map(|expr| self.plan_aggregation(expr, child, &mut aggregations))
            .collect::<Result<Vec<_>>>()?;

        Ok((aggregations, planned_expressions))
    }

    /// Transforms a logical to a physical expression. Any encountered aggregations is pushed to the aggregations vec
    /// and replaced by a column reference, so that the aggregation result can be referenced by parent physical plans
    fn plan_aggregation(
        &self,
        expr: LogicalExpr,
        child: &PhysicalPlan,
        aggregations: &mut Vec<Aggregation>,
    ) -> Result<Expr> {
        let res = match expr {
            LogicalExpr::Column(_) => {
                unreachable!("Mixing column references and aggregations is not possible")
            }
            LogicalExpr::Integer(num) => Expr::Value(Value::Integer(num)),
            LogicalExpr::String(s) => Expr::Value(Value::String(s)),
            LogicalExpr::Boolean(val) => Expr::Value(Value::Boolean(val)),
            LogicalExpr::Null => Expr::Value(Value::Null),
            LogicalExpr::Unary { op, expr } => Expr::Unary {
                op,
                expr: Box::new(self.plan_aggregation(*expr, child, aggregations)?),
            },
            LogicalExpr::Binary { left, op, right } => Expr::Binary {
                left: Box::new(self.plan_aggregation(*left, child, aggregations)?),
                op,
                right: Box::new(self.plan_aggregation(*right, child, aggregations)?),
            },
            LogicalExpr::IsNull(expr) => Expr::IsNull(Box::new(self.plan_aggregation(
                *expr,
                child,
                aggregations,
            )?)),
            LogicalExpr::IsNotNull(expr) => Expr::IsNotNull(Box::new(self.plan_aggregation(
                *expr,
                child,
                aggregations,
            )?)),
            LogicalExpr::Aggregation(agg_func, expr) => {
                aggregations.push(Aggregation::new(
                    agg_func,
                    self.plan_expression(*expr, &[child])?,
                ));
                Expr::ColumnReference {
                    tuple_idx: 0,
                    col_idx: aggregations.len() - 1,
                }
            }
        };
        Ok(res)
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
            LogicalExpr::Aggregation(_, _) => unreachable!(),
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
