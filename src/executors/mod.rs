use std::collections::HashMap;

use anyhow::Result;

use self::insert_executor::InsertExecutor;
use self::projection_executor::ProjectionExecutor;
use self::seq_scan_executor::SeqScanExecutor;
use self::values_executor::ValuesExecutor;
use crate::buffer::buffer_manager::BufferManager;
use crate::catalog::schema::Schema;
use crate::common::TableId;
use crate::planner::plans::Plan;
use crate::storage::heap::table::Table;
use crate::tuple::Tuple;

mod insert_executor;
mod projection_executor;
mod seq_scan_executor;
mod values_executor;

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
        self.insert_tables(&plan);
        self.create_executor_internal(plan)
    }

    fn insert_tables(&mut self, plan: &Plan) {
        let (table_id, schema) = match plan {
            Plan::SequentialScan {
                table_id,
                output_schema,
            } => (*table_id, output_schema.clone()),
            Plan::InsertPlan {
                target,
                target_schema,
                child,
            } => {
                self.insert_tables(child);
                (*target, target_schema.clone())
            }
            Plan::Projection {
                projections: _,
                child,
                output_schema: _,
            } => return self.insert_tables(child),
            _ => return,
        };

        self.insert_table(table_id, schema);
    }

    fn create_executor_internal(&'a self, plan: Plan) -> Result<Box<dyn Executor + 'a>> {
        match plan {
            Plan::SequentialScan {
                table_id,
                output_schema: _,
            } => Ok(Box::new(self.create_seq_scan_executor(table_id)?)),
            Plan::Projection {
                projections,
                child,
                output_schema,
            } => {
                let child = self.create_executor_internal(*child)?;
                Ok(Box::new(ProjectionExecutor::new(
                    child,
                    projections,
                    output_schema,
                )))
            }
            Plan::ValuesPlan {
                values,
                output_schema,
            } => Ok(Box::new(ValuesExecutor::new(values, output_schema))),
            Plan::InsertPlan {
                target,
                child,
                target_schema: _,
            } => {
                let table = self.get_table(target);
                let child = self.create_executor_internal(*child)?;
                Ok(Box::new(InsertExecutor::new(table, child)))
            }
        }
    }

    fn insert_table(&mut self, table_id: TableId, schema: Schema) {
        self.table_id_to_table
            .entry(table_id)
            .or_insert_with(|| Table::new(table_id, self.buffer_manager, schema));
    }

    fn create_seq_scan_executor(&'a self, table_id: TableId) -> Result<SeqScanExecutor<'a>> {
        let table = self.get_table(table_id);
        SeqScanExecutor::new(table)
    }

    fn get_table(&'a self, table_id: TableId) -> &Table {
        self.table_id_to_table.get(&table_id).unwrap()
    }
}
