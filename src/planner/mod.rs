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
            values,
            from,
            projections,
            output_schema,
            target,
            target_schema,
        } = query;

        let plan = {
            if let Some(values) = values {
                Plan::ValuesPlan {
                    values,
                    output_schema,
                }
            } else {
                let plan = self.plan_table_reference(from);

                self.plan_projections(projections, output_schema, plan)
            }
        };

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

    fn plan_table_reference(&self, table: Table) -> Plan {
        match table {
            Table::TableReference { table_id, schema } => Plan::SequentialScan {
                table_id,
                output_schema: schema,
            },
            Table::EmptyTable => Plan::ValuesPlan {
                values: vec![vec![]],
                output_schema: EMPTY_SCHEMA.clone(),
            },
        }
    }

    fn plan_projections(&self, projections: Vec<Expr>, schema: Schema, child: Plan) -> Plan {
        Plan::Projection {
            projections,
            child: Box::new(child),
            output_schema: schema,
        }
    }
}
