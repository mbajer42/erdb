use self::plans::Plan;
use crate::analyzer::query::{Expr, Query, Table};
use crate::catalog::schema::Schema;

pub mod plans;

pub struct Planner {}

impl Planner {
    pub fn new() -> Self {
        Self {}
    }

    pub fn plan_query(&self, query: Query) -> Plan {
        let Query {
            query_type: _,
            from,
            projections,
            output_schema,
        } = query;

        let plan = self.plan_table_reference(from);

        self.plan_projections(projections, output_schema, plan)
    }

    fn plan_table_reference(&self, table: Table) -> Plan {
        match table {
            Table::TableReference { table_id, schema } => Plan::SequentialScan {
                table_id,
                output_schema: schema,
            },
            Table::EmptyTable => Plan::ValuesPlan {
                values: vec![vec![]],
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
