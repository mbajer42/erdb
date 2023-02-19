use anyhow::Result;

use super::Executor;
use crate::catalog::schema::Schema;
use crate::planner::physical_plan::Expr;
use crate::tuple::Tuple;

pub struct ValuesExecutor {
    cursor: usize,
    values: Vec<Vec<Expr>>,
    schema: Schema,
}

impl ValuesExecutor {
    pub fn new(values: Vec<Vec<Expr>>, schema: Schema) -> Self {
        Self {
            cursor: 0,
            values,
            schema,
        }
    }
}

impl Executor for ValuesExecutor {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn next(&mut self) -> Option<Result<Tuple>> {
        if let Some(exprs) = self.values.get(self.cursor) {
            let values = exprs.iter().map(|expr| expr.evaluate(&[])).collect();
            self.cursor += 1;
            Some(Ok(Tuple::new(values)))
        } else {
            None
        }
    }

    fn rewind(&mut self) -> Result<()> {
        self.cursor = 0;
        Ok(())
    }
}
