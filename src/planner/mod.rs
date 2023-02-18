use self::plans::Plan;
use crate::analyzer::query::{Expr, Query, QueryType, Table, EMPTY_SCHEMA};
use crate::catalog::schema::Schema;

pub mod plans;

pub struct Planner {}

impl Planner {
    pub fn new() -> Self {
        Self {}
    }

    pub fn plan_query(&self, query: Query) -> Plan {
        let Query {
            query_type,
            from,
            projections,
            filter,
            output_schema,
            target,
            target_schema,
        } = query;

        let mut plan = self.plan_table_reference(from);
        plan = self.plan_filter(filter, plan);
        plan = self.plan_projections(projections, output_schema, plan);

        if query_type == QueryType::Insert {
            Plan::InsertPlan {
                target: target.unwrap(),
                child: Box::new(plan),
                target_schema: target_schema.unwrap(),
            }
        } else {
            plan
        }
    }

    fn plan_filter(&self, filter: Option<Expr>, child: Plan) -> Plan {
        if let Some(filter) = filter {
            Plan::FilterPlan {
                filter,
                child: Box::new(child),
            }
        } else {
            child
        }
    }

    fn plan_table_reference(&self, table: Table) -> Plan {
        match table {
            Table::Reference { table_id, schema } => Plan::SequentialScan {
                table_id,
                output_schema: schema,
            },
            Table::EmptyTable => Plan::ValuesPlan {
                values: vec![vec![]],
                output_schema: EMPTY_SCHEMA.clone(),
            },
            Table::Values { values, schema } => Plan::ValuesPlan {
                values,
                output_schema: schema,
            },
        }
    }

    fn plan_projections(&self, projections: Vec<Expr>, schema: Schema, child: Plan) -> Plan {
        if projections.is_empty() {
            child
        } else {
            Plan::Projection {
                projections,
                child: Box::new(child),
                output_schema: schema,
            }
        }
    }
}
