use std::collections::HashMap;

use anyhow::Result;

use self::projection_executor::ProjectionExecutor;
use self::seq_scan_executor::SeqScanExecutor;
use crate::buffer::buffer_manager::BufferManager;
use crate::catalog::schema::Schema;
use crate::common::TableId;
use crate::planner::plans::Plan;
use crate::storage::heap::table::Table;
use crate::tuple::Tuple;

mod projection_executor;
mod seq_scan_executor;

pub trait Executor {
    fn schema(&self) -> &Schema;
    fn next(&mut self) -> Option<Result<Tuple>>;
    fn rewind(&mut self) -> Result<()>;
}

pub struct ExecutorFactory<'a> {
    buffer_manager: &'a BufferManager,
    table_id_to_table: HashMap<TableId, Table<'a>>,
}

impl<'a> ExecutorFactory<'a> {
    pub fn new(buffer_manager: &'a BufferManager) -> Self {
        Self {
            buffer_manager,
            table_id_to_table: HashMap::new(),
        }
    }

    pub fn create_executor(&'a mut self, plan: Plan) -> Result<Box<dyn Executor + 'a>> {
        match plan {
            Plan::SequentialScan {
                table_id,
                output_schema,
            } => Ok(Box::new(
                self.create_seq_scan_executor(table_id, output_schema)?,
            )),
            Plan::Projection {
                projections,
                child,
                output_schema,
            } => {
                let child = self.create_executor(*child)?;
                Ok(Box::new(ProjectionExecutor::new(
                    child,
                    projections,
                    output_schema,
                )))
            }
        }
    }

    fn create_seq_scan_executor(
        &'a mut self,
        table_id: TableId,
        output_schema: Schema,
    ) -> Result<SeqScanExecutor<'a>> {
        let table = self.get_table(table_id, output_schema);
        SeqScanExecutor::new(table)
    }

    fn get_table(&mut self, table_id: TableId, schema: Schema) -> &Table {
        self.table_id_to_table
            .entry(table_id)
            .or_insert_with(|| Table::new(table_id, self.buffer_manager, schema))
    }
}
