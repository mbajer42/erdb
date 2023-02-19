use self::plans::PhysicalPlan;
use crate::analyzer::query::{DataSource, Expr, Query, QueryType, EMPTY_SCHEMA};
use crate::catalog::schema::Schema;

pub mod plans;

pub struct Planner {}

impl Planner {
    pub fn new() -> Self {
        Self {}
    }

    pub fn plan_query(&self, query: Query) -> PhysicalPlan {
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
            PhysicalPlan::InsertPlan {
                target: target.unwrap(),
                child: Box::new(plan),
                target_schema: target_schema.unwrap(),
            }
        } else {
            plan
        }
    }

    fn plan_filter(&self, filter: Option<Expr>, child: PhysicalPlan) -> PhysicalPlan {
        if let Some(filter) = filter {
            PhysicalPlan::FilterPlan {
                filter,
                child: Box::new(child),
            }
        } else {
            child
        }
    }

    fn plan_table_reference(&self, table: DataSource) -> PhysicalPlan {
        match table {
            DataSource::Table { table_id, schema } => PhysicalPlan::SequentialScan {
                table_id,
                output_schema: schema,
            },
            DataSource::EmptyTable => PhysicalPlan::ValuesPlan {
                values: vec![vec![]],
                output_schema: EMPTY_SCHEMA.clone(),
            },
            DataSource::Values { values, schema } => PhysicalPlan::ValuesPlan {
                values,
                output_schema: schema,
            },
        }
    }

    fn plan_projections(
        &self,
        projections: Vec<Expr>,
        schema: Schema,
        child: PhysicalPlan,
    ) -> PhysicalPlan {
        if projections.is_empty() {
            child
        } else {
            PhysicalPlan::Projection {
                projections,
                child: Box::new(child),
                output_schema: schema,
            }
        }
    }
}
