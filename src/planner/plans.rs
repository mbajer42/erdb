use crate::analyzer::query::Expr;
use crate::catalog::schema::Schema;
use crate::common::TableId;

#[derive(Debug, PartialEq)]
pub enum Plan {
    SequentialScan {
        table_id: TableId,
        output_schema: Schema,
    },
    Projection {
        projections: Vec<Expr>,
        child: Box<Plan>,
        output_schema: Schema,
    },
    ValuesPlan {
        values: Vec<Vec<Expr>>,
        output_schema: Schema,
    },
    InsertPlan {
        target: TableId,
        target_schema: Schema,
        child: Box<Plan>,
    },
}
