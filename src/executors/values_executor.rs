use anyhow::Result;

use super::Executor;
use crate::analyzer::query::{Expr, EMPTY_SCHEMA};
use crate::catalog::schema::Schema;
use crate::tuple::Tuple;

pub struct ValuesExecutor {
    cursor: usize,
    values: Vec<Vec<Expr>>,
}

impl ValuesExecutor {
    pub fn new(values: Vec<Vec<Expr>>) -> Self {
        Self { cursor: 0, values }
    }
}

impl Executor for ValuesExecutor {
    fn schema(&self) -> &Schema {
        &EMPTY_SCHEMA
    }

    fn next(&mut self) -> Option<Result<Tuple>> {
        if let Some(exprs) = self.values.get(self.cursor) {
            let values = exprs
                .iter()
                .map(|expr| expr.evaluate(&Tuple::new(vec![])))
                .collect();
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
