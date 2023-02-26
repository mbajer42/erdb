use std::collections::HashMap;

use anyhow::Result;

use self::filter_executor::FilterExecutor;
use self::insert_executor::InsertExecutor;
use self::nested_loop_join_executor::NestedLoopJoinExecutor;
use self::projection_executor::ProjectionExecutor;
use self::seq_scan_executor::SeqScanExecutor;
use self::values_executor::ValuesExecutor;
use crate::buffer::buffer_manager::BufferManager;
use crate::catalog::schema::Schema;
use crate::common::TableId;
use crate::concurrency::Transaction;
use crate::planner::physical_plan::PhysicalPlan;
use crate::storage::heap::table::Table;
use crate::tuple::Tuple;

mod filter_executor;
mod insert_executor;
mod nested_loop_join_executor;
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
    transaction: &'a Transaction<'a>,
}

impl<'a> ExecutorFactory<'a> {
    pub fn new(buffer_manager: &'a BufferManager, transaction: &'a Transaction) -> Self {
        Self {
            buffer_manager,
            table_id_to_table: HashMap::new(),
            transaction,
        }
    }

    pub fn create_executor(&'a mut self, plan: PhysicalPlan) -> Result<Box<dyn Executor + 'a>> {
        self.insert_tables(&plan);
        self.create_executor_internal(plan)
    }

    fn insert_tables(&mut self, plan: &PhysicalPlan) {
        let (table_id, schema) = match plan {
            PhysicalPlan::SequentialScan {
                table_id,
                output_schema,
            } => (*table_id, output_schema.clone()),
            PhysicalPlan::NestedLoopJoin {
                left,
                right,
                join_type: _,
                on: _,
                output_schema: _,
            } => {
                self.insert_tables(left);
                self.insert_tables(right);
                return;
            }
            PhysicalPlan::Insert {
                target,
                target_schema,
                child,
            } => {
                self.insert_tables(child);
                (*target, target_schema.clone())
            }
            PhysicalPlan::Projection {
                projections: _,
                child,
                output_schema: _,
            } => return self.insert_tables(child),
            PhysicalPlan::Filter { filter: _, child } => return self.insert_tables(child),
            _ => return,
        };

        self.insert_table(table_id, schema);
    }

    fn create_executor_internal(&'a self, plan: PhysicalPlan) -> Result<Box<dyn Executor + 'a>> {
        match plan {
            PhysicalPlan::SequentialScan {
                table_id,
                output_schema: _,
            } => Ok(Box::new(self.create_seq_scan_executor(table_id)?)),
            PhysicalPlan::NestedLoopJoin {
                left,
                right,
                join_type,
                on,
                output_schema,
            } => {
                let left_child = self.create_executor_internal(*left)?;
                let right_child = self.create_executor_internal(*right)?;

                Ok(Box::new(NestedLoopJoinExecutor::new(
                    left_child,
                    right_child,
                    join_type,
                    on,
                    output_schema,
                )))
            }
            PhysicalPlan::Projection {
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
            PhysicalPlan::Values {
                values,
                output_schema,
            } => Ok(Box::new(ValuesExecutor::new(values, output_schema))),
            PhysicalPlan::Insert {
                target,
                child,
                target_schema: _,
            } => {
                let table = self.get_table(target);
                let child = self.create_executor_internal(*child)?;
                Ok(Box::new(InsertExecutor::new(
                    table,
                    child,
                    self.transaction,
                )))
            }
            PhysicalPlan::Filter { filter, child } => {
                let child = self.create_executor_internal(*child)?;
                Ok(Box::new(FilterExecutor::new(child, filter)))
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
        SeqScanExecutor::new(table, self.transaction)
    }

    fn get_table(&'a self, table_id: TableId) -> &Table {
        self.table_id_to_table.get(&table_id).unwrap()
    }
}

#[cfg(test)]
mod tests {

    use anyhow::Result;
    use tempfile::{tempdir, TempDir};

    use super::ExecutorFactory;
    use crate::analyzer::Analyzer;
    use crate::buffer::buffer_manager::BufferManager;
    use crate::catalog::schema::{ColumnDefinition, TypeId};
    use crate::catalog::Catalog;
    use crate::concurrency::lock_manager::LockManager;
    use crate::concurrency::{Transaction, TransactionManager};
    use crate::parser::parse_sql;
    use crate::planner::Planner;
    use crate::storage::file_manager::FileManager;
    use crate::tuple::Tuple;

    #[allow(dead_code)]
    pub struct EmptyTestContext {
        data_dir: TempDir,
        buffer_manager: BufferManager,
        lock_manager: LockManager,
    }

    impl EmptyTestContext {
        pub fn new() -> Self {
            let data_dir = tempdir().unwrap();
            let file_manager = FileManager::new(data_dir.path()).unwrap();
            let buffer_manager = BufferManager::new(file_manager, 2);
            Self {
                data_dir,
                buffer_manager,
                lock_manager: LockManager::new(),
            }
        }
    }

    pub struct ExecutionTestContext<'a> {
        buffer_manager: &'a BufferManager,
        catalog: Catalog<'a>,
        transaction_manager: TransactionManager<'a>,
    }

    impl<'a> ExecutionTestContext<'a> {
        pub fn new(context: &'a EmptyTestContext) -> Self {
            let buffer_manager = &context.buffer_manager;
            let lock_manager = &context.lock_manager;
            let transaction_manager =
                TransactionManager::new(buffer_manager, lock_manager, true).unwrap();
            let bootstrap_transaction = transaction_manager.bootstrap();
            let catalog = Catalog::new(buffer_manager, true, &bootstrap_transaction).unwrap();
            bootstrap_transaction.commit().unwrap();
            drop(bootstrap_transaction);

            Self {
                buffer_manager,
                catalog,
                transaction_manager,
            }
        }

        pub fn create_table(&self, table_name: &str, columns: Vec<ColumnDefinition>) -> Result<()> {
            let transaction = self.transaction_manager.start_transaction()?;
            self.catalog
                .create_table(table_name, columns, &transaction)?;
            transaction.commit()
        }

        pub fn start_transaction(&'a self) -> Result<Transaction<'a>> {
            self.transaction_manager.start_transaction()
        }

        pub fn execute_query(&self, sql: &str) -> Result<Vec<Tuple>> {
            let transaction = self.transaction_manager.start_implicit_transaction()?;
            self.execute_query_with_transaction(sql, &transaction)
        }

        pub fn execute_query_with_transaction(
            &self,
            sql: &str,
            transaction: &Transaction,
        ) -> Result<Vec<Tuple>> {
            let query = parse_sql(sql)?;
            let analyzer = Analyzer::new(&self.catalog);
            let query = analyzer.analyze(query)?;
            let planner = Planner::new();
            let plan = planner.prepare_logical_plan(query)?;
            let mut executor_factory = ExecutorFactory::new(self.buffer_manager, transaction);
            let mut executor = executor_factory.create_executor(plan)?;
            let mut tuples = vec![];
            while let Some(tuple) = executor.next().transpose()? {
                tuples.push(tuple);
            }
            Ok(tuples)
        }
    }

    #[test]
    fn can_only_see_committed_tuples() {
        let empty_test_context = EmptyTestContext::new();
        let execution_test_context = ExecutionTestContext::new(&empty_test_context);
        execution_test_context
            .create_table(
                "numbers",
                vec![ColumnDefinition::new(
                    TypeId::Integer,
                    "number".to_owned(),
                    0,
                    true,
                )],
            )
            .unwrap();

        let insert_transaction = execution_test_context.start_transaction().unwrap();
        let insert_statement = "insert into numbers values (1), (2), (3)";
        execution_test_context
            .execute_query_with_transaction(insert_statement, &insert_transaction)
            .unwrap();

        let select_numbers = "select * from numbers";
        let tuples = execution_test_context
            .execute_query(select_numbers)
            .unwrap();
        assert!(tuples.is_empty());

        insert_transaction.commit().unwrap();
        let tuples = execution_test_context
            .execute_query(select_numbers)
            .unwrap();
        assert_eq!(tuples.len(), 3);
    }
}
