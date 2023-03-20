use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;

use self::delete_executor::DeleteExecutor;
use self::filter_executor::FilterExecutor;
use self::insert_executor::InsertExecutor;
use self::nested_loop_join_executor::NestedLoopJoinExecutor;
use self::projection_executor::ProjectionExecutor;
use self::seq_scan_executor::SeqScanExecutor;
use self::update_executor::UpdateExecutor;
use self::values_executor::ValuesExecutor;
use crate::buffer::buffer_manager::BufferManager;
use crate::catalog::schema::Schema;
use crate::common::TableId;
use crate::concurrency::Transaction;
use crate::planner::physical_plan::PhysicalPlan;
use crate::storage::heap::table::Table;
use crate::tuple::Tuple;

mod delete_executor;
mod filter_executor;
mod insert_executor;
mod nested_loop_join_executor;
mod projection_executor;
mod seq_scan_executor;
mod update_executor;
mod values_executor;

pub trait Executor {
    fn schema(&self) -> &Schema;
    fn next(&mut self) -> Option<Result<Tuple>>;
    /// Rewinds the executor to its initial state
    fn rewind(&mut self) -> Result<()>;
    /// Re-evaluates a tuple, whether this executor would return it again.
    /// Needed for READ COMMITTED transactions, when another transaction modified
    /// it in the meantime, re-evaluate if it still meets the criteria
    fn re_evaluate_tuple(&self, _tuple: &Tuple) -> bool {
        unreachable!()
    }
}

pub struct ExecutorFactory<'a> {
    buffer_manager: Arc<BufferManager>,
    table_id_to_table: HashMap<TableId, Table>,
    transaction: &'a Transaction<'a>,
}

impl<'a> ExecutorFactory<'a> {
    pub fn new(buffer_manager: Arc<BufferManager>, transaction: &'a Transaction) -> Self {
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
            PhysicalPlan::Delete { from: _, child } => return self.insert_tables(child),
            PhysicalPlan::Update {
                table: _,
                set: _,
                child,
            } => return self.insert_tables(child),
            PhysicalPlan::Values {
                values: _,
                output_schema: _,
            } => return,
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
            PhysicalPlan::Delete { from, child } => {
                let child = self.create_executor_internal(*child)?;
                let table = self.get_table(from);
                Ok(Box::new(DeleteExecutor::new(
                    table,
                    child,
                    self.transaction,
                )))
            }
            PhysicalPlan::Update { table, set, child } => {
                let child = self.create_executor_internal(*child)?;
                let table = self.get_table(table);
                Ok(Box::new(UpdateExecutor::new(
                    table,
                    child,
                    set,
                    self.transaction,
                )))
            }
        }
    }

    fn insert_table(&mut self, table_id: TableId, schema: Schema) {
        self.table_id_to_table
            .entry(table_id)
            .or_insert_with(|| Table::new(table_id, Arc::clone(&self.buffer_manager), schema));
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

    use std::sync::Arc;

    use anyhow::Result;
    use tempfile::{tempdir, TempDir};

    use super::ExecutorFactory;
    use crate::analyzer::Analyzer;
    use crate::buffer::buffer_manager::BufferManager;
    use crate::catalog::schema::{ColumnDefinition, TypeId};
    use crate::catalog::Catalog;
    use crate::concurrency::{Transaction, TransactionManager};
    use crate::optimizer::optimize;
    use crate::parser::parse_sql;
    use crate::planner::Planner;
    use crate::storage::file_manager::FileManager;
    use crate::tuple::Tuple;

    #[allow(dead_code)]
    pub struct TestDb {
        data_dir: TempDir,
        buffer_manager: Arc<BufferManager>,
        catalog: Catalog,
        pub transaction_manager: TransactionManager,
    }

    impl TestDb {
        pub fn new() -> Self {
            let data_dir = tempdir().unwrap();
            let file_manager = FileManager::new(data_dir.path()).unwrap();
            let buffer_manager = Arc::new(BufferManager::new(file_manager, 2));
            let transaction_manager =
                TransactionManager::new(Arc::clone(&buffer_manager), true).unwrap();

            let bootstrap_transaction = transaction_manager.bootstrap();
            let catalog =
                Catalog::new(Arc::clone(&buffer_manager), true, &bootstrap_transaction).unwrap();
            bootstrap_transaction.commit().unwrap();
            drop(bootstrap_transaction);
            Self {
                data_dir,
                buffer_manager,
                catalog,
                transaction_manager,
            }
        }

        pub fn create_table(&self, table_name: &str, columns: Vec<ColumnDefinition>) -> Result<()> {
            let transaction = self.transaction_manager.start_transaction(None)?;
            self.catalog
                .create_table(table_name, columns, &transaction)?;
            transaction.commit()
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
            let (_, query) = parse_sql(sql)?;
            let analyzer = Analyzer::new(&self.catalog);
            let logical_plan = analyzer.analyze(query)?;
            let logical_plan = optimize(logical_plan);
            let planner = Planner::new();
            let plan = planner.prepare_logical_plan(logical_plan)?;
            let mut executor_factory =
                ExecutorFactory::new(Arc::clone(&self.buffer_manager), transaction);
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
        let test_db = TestDb::new();
        test_db
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

        let insert_transaction = test_db.transaction_manager.start_transaction(None).unwrap();
        let insert_statement = "insert into numbers values (1), (2), (3)";
        test_db
            .execute_query_with_transaction(insert_statement, &insert_transaction)
            .unwrap();

        let select_numbers = "select * from numbers";
        let tuples = test_db.execute_query(select_numbers).unwrap();
        assert!(tuples.is_empty());

        insert_transaction.commit().unwrap();
        let tuples = test_db.execute_query(select_numbers).unwrap();
        assert_eq!(tuples.len(), 3);
    }
}
