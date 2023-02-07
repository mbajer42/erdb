use anyhow::Result;

use super::Executor;
use crate::analyzer::query::Expr;
use crate::catalog::schema::Schema;
use crate::tuple::Tuple;

pub struct ProjectionExecutor<'a> {
    child: Box<dyn Executor + 'a>,
    projections: Vec<Expr>,
    output_schema: Schema,
}

impl<'a> ProjectionExecutor<'a> {
    pub fn new(
        child: Box<dyn Executor + 'a>,
        projections: Vec<Expr>,
        output_schema: Schema,
    ) -> Self {
        Self {
            child,
            projections,
            output_schema,
        }
    }
}

impl<'a> Executor for ProjectionExecutor<'a> {
    fn next(&mut self) -> Option<Result<Tuple>> {
        self.child.next().map(|tuple| {
            tuple.map(|tuple| {
                let values = self
                    .projections
                    .iter()
                    .map(|expr| expr.evaluate(&tuple))
                    .collect();
                Tuple::new(values)
            })
        })
    }

    fn rewind(&mut self) -> Result<()> {
        self.child.rewind()
    }

    fn schema(&self) -> &Schema {
        &self.output_schema
    }
}
