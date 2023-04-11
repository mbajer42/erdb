use std::collections::{HashMap, VecDeque};

use anyhow::{Error, Result};

use crate::analyzer::logical_plan::Query;
use crate::catalog::schema::{ColumnDefinition, Schema, TypeId};
use crate::catalog::Catalog;
use crate::parser::ast::{
    self, BinaryOperator, ExprNode, JoinType, Projection, SelectStatement, Statement, TableNode,
};

pub mod logical_plan;

use logical_plan::LogicalPlan;

use self::logical_plan::{AggregationFunc, LogicalExpr, TableReference};

/// Splits an expression into a conjunctive normal form
/// i.e. a AND b AND c will be split into vec![a, b, c]
fn split_expression(expr: LogicalExpr) -> Vec<LogicalExpr> {
    let mut expressions = vec![];
    let mut to_visit = vec![expr];
    while let Some(expr) = to_visit.pop() {
        match expr {
            LogicalExpr::Binary { left, op, right } if op == BinaryOperator::And => {
                to_visit.push(*left);
                to_visit.push(*right);
            }
            expr => expressions.push(expr),
        }
    }

    expressions
}

pub struct Analyzer<'a> {
    catalog: &'a Catalog,
}

impl<'a> Analyzer<'a> {
    pub fn new(catalog: &'a Catalog) -> Self {
        Self { catalog }
    }

    pub fn analyze(&self, query: ast::Statement) -> Result<LogicalPlan> {
        match query {
            Statement::Select(select) => Ok(LogicalPlan::Select(self.analyze_select(select)?)),
            Statement::Delete { from, filter } => self.analyze_delete(from, filter),
            Statement::Insert { into, select } => self.analyze_insert(into, select),
            Statement::Update { table, set, filter } => self.analyze_update(table, set, filter),
            _ => unreachable!(),
        }
    }

    fn analyze_update(
        &self,
        table: ast::TableNode,
        set_expressions: HashMap<String, ExprNode>,
        filter: Option<ExprNode>,
    ) -> Result<LogicalPlan> {
        let table = self.analyze_table(table)?;

        let set = set_expressions
            .into_iter()
            .map(|(column, expression)| {
                let (col_expr, col_def) =
                    Self::analyze_expression(ExprNode::Identifier(column), &table)?;
                let column: Vec<String> = match col_expr {
                    LogicalExpr::Column(col) => col,
                    _ => unreachable!(),
                };
                let (value_expr, value_def) = Self::analyze_expression(expression, &table)?;

                if value_def.type_id != TypeId::Unknown && value_def.type_id != col_def.type_id {
                    return Err(Error::msg(format!(
                        "Cannot set value for column '{}'. Left type '{}', right type '{}'",
                        column.join("."),
                        col_def.type_id,
                        value_def.type_id
                    )));
                }
                if value_def.type_id == TypeId::Unknown && value_def.not_null {
                    return Err(Error::msg(format!(
                        "Cannot set NULL for column '{}'. Column has 'NOT NULL' constraint",
                        column.join(".")
                    )));
                }

                Ok((column, value_expr))
            })
            .collect::<Result<HashMap<_, _>>>()?;

        let filter = if let Some(filter_expr) = filter {
            let (expr, col_def) = Self::analyze_expression(filter_expr, &table)?;
            if col_def.type_id != TypeId::Unknown && col_def.type_id != TypeId::Boolean {
                return Err(Error::msg(format!(
                    "WHERE condition must evaluate to boolean, but evaluates to {}",
                    col_def.type_id
                )));
            }
            split_expression(expr)
        } else {
            vec![]
        };

        Ok(LogicalPlan::Update { table, set, filter })
    }

    fn analyze_delete(
        &self,
        from: ast::TableNode,
        filter: Option<ExprNode>,
    ) -> Result<LogicalPlan> {
        let table = self.analyze_table(from)?;

        let filter = if let Some(filter_expr) = filter {
            let (expr, col_def) = Self::analyze_expression(filter_expr, &table)?;
            if col_def.type_id != TypeId::Unknown && col_def.type_id != TypeId::Boolean {
                return Err(Error::msg(format!(
                    "WHERE condition must evaluate to boolean, but evaluates to {}",
                    col_def.type_id
                )));
            }
            split_expression(expr)
        } else {
            vec![]
        };

        Ok(LogicalPlan::Delete {
            from: table,
            filter,
        })
    }

    fn analyze_insert(&self, into: ast::TableNode, select: SelectStatement) -> Result<LogicalPlan> {
        let (table_id, schema) = match self.analyze_table(into)? {
            TableReference::BaseTable {
                table_id,
                name: _,
                schema,
                filter: _,
            } => (table_id, schema),
            _ => unreachable!(),
        };

        let query = self.analyze_select(select)?;

        if schema.columns().len() != query.output_schema.columns().len() {
            return Err(Error::msg(format!(
                "Insert target has {} columns but only {} were provided",
                schema.columns().len(),
                query.output_schema.columns().len()
            )));
        }
        for (col_offset, (target_col, value_col)) in schema
            .columns()
            .iter()
            .zip(query.output_schema.columns())
            .enumerate()
        {
            if target_col.type_id() != value_col.type_id() && value_col.type_id() != TypeId::Unknown
            {
                return Err(Error::msg(format!(
                    "Column {} is of type {}, but value is of type {}",
                    col_offset,
                    target_col.type_id(),
                    value_col.type_id()
                )));
            }
            if target_col.not_null() && !value_col.not_null() {
                return Err(Error::msg(format!(
                    "Cannot insert NULL into column {}",
                    col_offset
                )));
            }
        }

        Ok(LogicalPlan::Insert {
            query,
            target: table_id,
            target_schema: schema,
        })
    }

    fn analyze_select(&self, select: SelectStatement) -> Result<Query> {
        let SelectStatement {
            values,
            projections,
            from,
            filter,
        } = select;
        if let Some(values) = values {
            return Self::analyze_values(values);
        }

        let table = self.analyze_tables(from)?;
        let projections_with_specification = self.analyze_projections(projections, &table)?;

        let mut projections = vec![];
        let mut output_columns = vec![];

        let mut has_aggregations = false;
        let mut referenced_column = None;
        for (col, (expr, mut col_def)) in projections_with_specification.into_iter().enumerate() {
            has_aggregations |= expr.has_aggregation();
            if referenced_column.is_none() {
                referenced_column = expr.find_any_referenced_column();
            }

            match &referenced_column {
                Some(column) if has_aggregations => {
                    // don't allow column references if projections contains any aggregations
                    // disallow e.g. SELECT count(col_a), col_a from table
                    return Err(Error::msg(format!(
                        "column '{}' must be used in an aggregation",
                        column
                    )));
                }
                _ => (),
            }

            projections.push(expr);
            col_def.column_offset = col as u8;
            output_columns.push(col_def);
        }

        let filter = if let Some(filter_expr) = filter {
            let (expr, col_def) = Self::analyze_expression(filter_expr, &table)?;
            if col_def.type_id != TypeId::Unknown && col_def.type_id != TypeId::Boolean {
                return Err(Error::msg(format!(
                    "WHERE condition must evaluate to boolean, but evaluates to {}",
                    col_def.type_id
                )));
            }
            split_expression(expr)
        } else {
            vec![]
        };

        Ok(Query {
            values: vec![],
            from: table,
            projections,
            filter,
            output_schema: Schema::new(output_columns),
        })
    }

    fn analyze_values(values: Vec<Vec<ast::ExprNode>>) -> Result<Query> {
        let mut expressions = vec![];
        let mut output_columns = vec![];

        let mut first_row_added = false;
        for (row, current_values) in values.into_iter().enumerate() {
            let mut current_expressions = vec![];
            for (col, value) in current_values.into_iter().enumerate() {
                let (expr, mut col_def) =
                    Self::analyze_expression(value, &TableReference::EmptyTable)?;

                if !first_row_added {
                    col_def.column_name = format!("col_{}", col);
                    col_def.not_null = expr != LogicalExpr::Null;
                    col_def.column_offset = col as u8;
                    output_columns.push(col_def);
                } else if let Some(result_def) = output_columns.get_mut(col) {
                    if result_def.type_id == TypeId::Unknown {
                        // first value was null
                        result_def.type_id = col_def.type_id;
                    } else if col_def.type_id == TypeId::Unknown {
                        // current value is null, so column is nullable
                        result_def.not_null = false;
                    } else if result_def.type_id != col_def.type_id {
                        return Err(Error::msg(format!(
                            "Type mismatch in row {}. Expected '{}' but found '{}'",
                            row,
                            result_def.type_id(),
                            col_def.type_id
                        )));
                    }
                }

                current_expressions.push(expr);
            }

            if first_row_added && output_columns.len() != current_expressions.len() {
                return Err(Error::msg(format!(
                    "Expected {} values, but {} row has {}.",
                    output_columns.len(),
                    row,
                    current_expressions.len()
                )));
            }

            first_row_added = true;
            expressions.push(current_expressions);
        }

        Ok(Query {
            from: TableReference::EmptyTable,
            projections: vec![],
            filter: vec![],
            values: expressions,
            output_schema: Schema::new(output_columns),
        })
    }

    fn analyze_tables(&self, mut tables: VecDeque<ast::TableNode>) -> Result<TableReference> {
        if tables.is_empty() {
            Ok(TableReference::EmptyTable)
        } else if tables.len() > 1 {
            let left = self.analyze_table(tables.pop_front().unwrap())?;
            let right = self.analyze_table(tables.pop_front().unwrap())?;

            let mut result = TableReference::Join {
                left: Box::new(left),
                right: Box::new(right),
                join_type: JoinType::Inner,
                on: vec![],
            };
            while let Some(table) = tables.pop_front() {
                let table = self.analyze_table(table)?;

                result = TableReference::Join {
                    left: Box::new(result),
                    right: Box::new(table),
                    join_type: JoinType::Inner,
                    on: vec![],
                }
            }

            Ok(result)
        } else {
            self.analyze_table(tables.pop_front().unwrap())
        }
    }

    fn analyze_table(&self, table: TableNode) -> Result<TableReference> {
        match table {
            TableNode::TableReference { name, alias } => {
                let table_id = self
                    .catalog
                    .get_table_id(&name)
                    .ok_or_else(|| Error::msg(format!("Could not find table {}", name)))?;
                let schema = self.catalog.get_schema(&name).unwrap();
                Ok(TableReference::BaseTable {
                    table_id,
                    name: alias.unwrap_or(name),
                    schema,
                    filter: vec![],
                })
            }
            TableNode::Join {
                left,
                right,
                join_type,
                on,
            } => {
                let left = self.analyze_table(*left)?;
                let right = self.analyze_table(*right)?;
                let mut result_table = TableReference::Join {
                    left: Box::new(left),
                    right: Box::new(right),
                    join_type,
                    on: vec![],
                };
                let (on_expr, on_def) = Self::analyze_expression(on, &result_table)?;

                if on_def.type_id != TypeId::Boolean && on_def.type_id != TypeId::Unknown {
                    return Err(Error::msg(format!(
                        "JOIN conditions must evaluate to boolean but evaluates to: {}",
                        on_def.type_id
                    )));
                }

                match &mut result_table {
                    TableReference::Join {
                        left: _,
                        right: _,
                        join_type: _,
                        on,
                    } => on.push(on_expr),
                    _ => unreachable!(),
                };

                Ok(result_table)
            }
            TableNode::CrossJoin { left, right } => {
                let left = self.analyze_table(*left)?;
                let right = self.analyze_table(*right)?;

                Ok(TableReference::Join {
                    left: Box::new(left),
                    right: Box::new(right),
                    join_type: JoinType::Inner,
                    on: vec![],
                })
            }
        }
    }

    fn analyze_projections(
        &self,
        projections: Vec<ast::Projection>,
        scope: &TableReference,
    ) -> Result<Vec<(LogicalExpr, ColumnDefinition)>> {
        let mut result = vec![];

        for projection in projections.into_iter() {
            match projection {
                Projection::Wildcard => {
                    let mut wildcard = Self::get_all_columns(scope, None);
                    result.append(&mut wildcard);
                }
                Projection::QualifiedWildcard { table } => {
                    let mut wildcard = Self::get_all_columns(scope, Some(table));
                    result.append(&mut wildcard);
                }
                _ => result.push(self.analyze_projection(projection, scope)?),
            }
        }

        Ok(result)
    }

    fn analyze_projection(
        &self,
        projection: ast::Projection,
        scope: &TableReference,
    ) -> Result<(LogicalExpr, ColumnDefinition)> {
        match projection {
            Projection::UnnamedExpr(expr) => {
                let alias = expr.to_string();
                let (expr, mut col_def) = Self::analyze_expression(expr, scope)?;
                col_def.column_name = alias;
                Ok((expr, col_def))
            }
            Projection::NamedExpr { expr, alias } => {
                let (expr, mut col_def) = Self::analyze_expression(expr, scope)?;
                col_def.column_name = alias;
                Ok((expr, col_def))
            }
            Projection::Wildcard | Projection::QualifiedWildcard { table: _ } => {
                unreachable!("Should be already handled")
            }
        }
    }

    fn analyze_expression(
        expr: ast::ExprNode,
        scope: &TableReference,
    ) -> Result<(LogicalExpr, ColumnDefinition)> {
        match expr {
            ExprNode::Identifier(column_name) => {
                let column = Self::identify_column(scope, None, &column_name)?;
                if let Some(res) = column {
                    Ok(res)
                } else {
                    Err(Error::msg(format!("Could not find column {}", column_name)))
                }
            }
            ExprNode::QualifiedIdentifier(table, column_name) => {
                let column = Self::identify_column(scope, Some(&table), &column_name)?;
                column.ok_or_else(|| {
                    Error::msg(format!("Coudl not find column {}.{}", table, column_name))
                })
            }

            ExprNode::Number(number) => {
                let num = number.parse::<i32>()?;
                Ok((
                    LogicalExpr::Integer(num),
                    ColumnDefinition::with_type_id(TypeId::Integer),
                ))
            }
            ExprNode::String(s) => Ok((
                LogicalExpr::String(s),
                ColumnDefinition::with_type_id(TypeId::Text),
            )),
            ExprNode::Boolean(val) => Ok((
                LogicalExpr::Boolean(val),
                ColumnDefinition::with_type_id(TypeId::Boolean),
            )),
            ExprNode::Grouping(expr) => Self::analyze_expression(*expr, scope),
            ExprNode::Binary { left, op, right } => {
                let (left, left_def) = Self::analyze_expression(*left, scope)?;
                let (right, right_def) = Self::analyze_expression(*right, scope)?;
                let result_type = match op {
                    BinaryOperator::Plus
                    | BinaryOperator::Minus
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide
                    | BinaryOperator::Modulo => {
                        if left_def.type_id != TypeId::Integer
                            || right_def.type_id != TypeId::Integer
                        {
                            return Err(Error::msg(format!(
                                "Arguments for '{}' must be of type integer. Left: {}, Right: {}",
                                op, left_def.type_id, right_def.type_id
                            )));
                        }
                        ColumnDefinition::with_type_id(TypeId::Integer)
                    }
                    BinaryOperator::Eq
                    | BinaryOperator::NotEq
                    | BinaryOperator::Less
                    | BinaryOperator::LessEq
                    | BinaryOperator::Greater
                    | BinaryOperator::GreaterEq => {
                        if left_def.type_id != right_def.type_id
                            && left_def.type_id != TypeId::Unknown
                            && right_def.type_id != TypeId::Unknown
                        {
                            return Err(Error::msg(format!(
                                "Arguments for '{}' must be of same type. Left: {}, Right: {}",
                                op, left_def.type_id, right_def.type_id
                            )));
                        }
                        ColumnDefinition::with_type_id(TypeId::Boolean)
                    }
                    BinaryOperator::And | BinaryOperator::Or => {
                        let valid_types = [TypeId::Boolean, TypeId::Unknown];
                        if !valid_types.contains(&left_def.type_id)
                            || !valid_types.contains(&right_def.type_id)
                        {
                            return Err(Error::msg(format!(
                                "Arguments for '{}' must be of type boolean. Left: {}, Right: {}",
                                op, left_def.type_id, right_def.type_id
                            )));
                        }
                        ColumnDefinition::with_type_id(TypeId::Boolean)
                    }
                };
                Ok((
                    LogicalExpr::Binary {
                        left: Box::new(left),
                        op,
                        right: Box::new(right),
                    },
                    result_type,
                ))
            }
            ExprNode::Unary { op, expr } => {
                let (expr, col_def) = Self::analyze_expression(*expr, scope)?;
                if col_def.type_id != TypeId::Integer {
                    Err(Error::msg(format!(
                        "Cannot apply '{}' to type {}",
                        op, col_def.type_id
                    )))
                } else {
                    Ok((
                        LogicalExpr::Unary {
                            op,
                            expr: Box::new(expr),
                        },
                        ColumnDefinition::with_type_id(TypeId::Integer),
                    ))
                }
            }
            ExprNode::IsNull(expr) => {
                let (expr, _) = Self::analyze_expression(*expr, scope)?;
                Ok((
                    LogicalExpr::IsNull(Box::new(expr)),
                    ColumnDefinition::with_type_id(TypeId::Boolean),
                ))
            }
            ExprNode::IsNotNull(expr) => {
                let (expr, _) = Self::analyze_expression(*expr, scope)?;
                Ok((
                    LogicalExpr::IsNotNull(Box::new(expr)),
                    ColumnDefinition::with_type_id(TypeId::Boolean),
                ))
            }
            ExprNode::FunctionCall { name, expr } => {
                if let Some(agg) = AggregationFunc::is_aggregation_func(&name) {
                    let (expr, col_def) = Self::analyze_expression(*expr, scope)?;
                    if expr.has_aggregation() {
                        return Err(Error::msg("Aggregations cannot be nested"));
                    }

                    agg.validate_aggregation_type(col_def.type_id)?;
                    let result_type = agg.aggregation_result_type(col_def.type_id);

                    let agg_expr = LogicalExpr::Aggregation(agg, Box::new(expr));
                    Ok((agg_expr, ColumnDefinition::with_type_id(result_type)))
                } else {
                    Err(Error::msg(format!("Cannot find function {}.", name)))
                }
            }
            ExprNode::Null => Ok((
                LogicalExpr::Null,
                ColumnDefinition::with_type_id(TypeId::Unknown),
            )),
        }
    }

    fn identify_column(
        scope: &TableReference,
        table: Option<&str>,
        column: &str,
    ) -> Result<Option<(LogicalExpr, ColumnDefinition)>> {
        match scope {
            TableReference::BaseTable {
                table_id: _,
                name,
                schema,
                filter: _,
            } => {
                if let Some(table) = table {
                    if name != table {
                        return Ok(None);
                    }
                }
                let column = schema.find_column(column).map(|col_def| {
                    (
                        LogicalExpr::Column(vec![name.clone(), col_def.column_name().to_owned()]),
                        ColumnDefinition::new(
                            col_def.type_id(),
                            String::new(),
                            col_def.column_offset(),
                            col_def.not_null(),
                        ),
                    )
                });
                Ok(column)
            }
            TableReference::Join {
                left,
                right,
                join_type: _,
                on: _,
            } => {
                let left = Self::identify_column(left, table, column)?;
                let right = Self::identify_column(right, table, column)?;

                if let Some(left) = left {
                    if right.is_some() {
                        Err(Error::msg(format!("Column '{}' is ambiguous", column)))
                    } else {
                        Ok(Some(left))
                    }
                } else {
                    Ok(right)
                }
            }
            TableReference::EmptyTable => Ok(None),
        }
    }

    fn get_all_columns(
        scope: &TableReference,
        table: Option<String>,
    ) -> Vec<(LogicalExpr, ColumnDefinition)> {
        match scope {
            TableReference::BaseTable {
                table_id: _,
                name: table_name,
                schema,
                filter: _,
            } => {
                if let Some(table) = table {
                    if table_name != &table {
                        return vec![];
                    }
                }
                schema
                    .columns()
                    .iter()
                    .map(|col_def| {
                        let expr = LogicalExpr::Column(vec![
                            table_name.clone(),
                            col_def.column_name().to_owned(),
                        ]);
                        let name = format!("{}.{}", table_name, col_def.column_name());
                        let new_col_def = ColumnDefinition::new(
                            col_def.type_id(),
                            name,
                            col_def.column_offset(),
                            col_def.not_null(),
                        );
                        (expr, new_col_def)
                    })
                    .collect()
            }
            TableReference::Join {
                left,
                right,
                join_type: _,
                on: _,
            } => {
                let mut left_columns = Self::get_all_columns(left, table.clone());
                let mut right_columns = Self::get_all_columns(right, table.clone());
                left_columns.append(&mut right_columns);
                left_columns
            }
            TableReference::EmptyTable => vec![],
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::Result;
    use tempfile::{tempdir, TempDir};

    use super::logical_plan::{LogicalExpr, LogicalPlan, TableReference};
    use super::Analyzer;
    use crate::analyzer::logical_plan::{AggregationFunc, Query};
    use crate::buffer::buffer_manager::BufferManager;
    use crate::catalog::schema::{ColumnDefinition, Schema, TypeId};
    use crate::catalog::Catalog;
    use crate::concurrency::TransactionManager;
    use crate::parser::ast::{BinaryOperator, UnaryOperator};
    use crate::parser::parse_sql;
    use crate::storage::file_manager::FileManager;

    struct AnalyzerTestSuite {
        #[allow(dead_code)]
        data_dir: TempDir,
        transaction_manager: TransactionManager,
        catalog: Catalog,
    }

    impl AnalyzerTestSuite {
        fn new() -> Result<Self> {
            let data_dir = tempdir()?;
            let file_manager = FileManager::new(data_dir.path())?;
            let buffer_manager = Arc::new(BufferManager::new(file_manager, 1));
            let transaction_manager = TransactionManager::new(Arc::clone(&buffer_manager), true)?;

            let bootstrap_transaction = transaction_manager.bootstrap();
            let catalog =
                Catalog::new(Arc::clone(&buffer_manager), true, &bootstrap_transaction).unwrap();

            bootstrap_transaction.commit()?;

            Ok(Self {
                data_dir,
                transaction_manager,
                catalog,
            })
        }

        fn create_table(&self, table_name: &str, columns: Vec<ColumnDefinition>) -> Result<()> {
            let transaction = self.transaction_manager.start_transaction(None)?;
            self.catalog
                .create_table(table_name, columns, &transaction)?;
            transaction.commit()?;

            Ok(())
        }

        fn analyze_query(&self, sql: &str) -> Result<LogicalPlan> {
            let (_, statement) = parse_sql(sql)?;
            let analyzer = Analyzer::new(&self.catalog);
            analyzer.analyze(statement)
        }
    }

    #[test]
    fn can_bind_wildcard_select() {
        let columns = vec![
            ColumnDefinition::new(TypeId::Integer, "id".to_owned(), 0, true),
            ColumnDefinition::new(TypeId::Text, "name".to_owned(), 1, true),
        ];
        let analyzer_test_suite = AnalyzerTestSuite::new().unwrap();
        analyzer_test_suite
            .create_table("accounts", columns)
            .unwrap();
        let table_id = analyzer_test_suite
            .catalog
            .get_table_id("accounts")
            .unwrap();
        let schema = analyzer_test_suite.catalog.get_schema("accounts").unwrap();
        let sql = "
            select * from accounts
        ";
        let query = analyzer_test_suite.analyze_query(sql).unwrap();

        let expected_query = LogicalPlan::Select(Query {
            from: TableReference::BaseTable {
                table_id,
                name: "accounts".to_owned(),
                schema,
                filter: vec![],
            },
            projections: vec![
                LogicalExpr::Column(vec!["accounts".to_owned(), "id".to_owned()]),
                LogicalExpr::Column(vec!["accounts".to_owned(), "name".to_owned()]),
            ],
            filter: vec![],
            output_schema: Schema::new(vec![
                ColumnDefinition::new(TypeId::Integer, "accounts.id".to_owned(), 0, true),
                ColumnDefinition::new(TypeId::Text, "accounts.name".to_owned(), 1, true),
            ]),
            values: vec![],
        });

        assert_eq!(query, expected_query);
    }

    #[test]
    fn can_bind_qualified_wildcard_select() {
        let columns = vec![
            ColumnDefinition::new(TypeId::Integer, "id".to_owned(), 0, true),
            ColumnDefinition::new(TypeId::Text, "name".to_owned(), 1, true),
        ];
        let analyzer_test_suite = AnalyzerTestSuite::new().unwrap();
        analyzer_test_suite
            .create_table("accounts", columns)
            .unwrap();
        let table_id = analyzer_test_suite
            .catalog
            .get_table_id("accounts")
            .unwrap();
        let schema = analyzer_test_suite.catalog.get_schema("accounts").unwrap();

        let sql = "
            select acc.* from accounts acc
        ";
        let query = analyzer_test_suite.analyze_query(sql).unwrap();

        let expected_query = LogicalPlan::Select(Query {
            from: TableReference::BaseTable {
                table_id,
                name: "acc".to_owned(),
                schema,
                filter: vec![],
            },
            projections: vec![
                LogicalExpr::Column(vec!["acc".to_owned(), "id".to_owned()]),
                LogicalExpr::Column(vec!["acc".to_owned(), "name".to_owned()]),
            ],
            filter: vec![],
            output_schema: Schema::new(vec![
                ColumnDefinition::new(TypeId::Integer, "acc.id".to_owned(), 0, true),
                ColumnDefinition::new(TypeId::Text, "acc.name".to_owned(), 1, true),
            ]),
            values: vec![],
        });

        assert_eq!(query, expected_query);
    }

    #[test]
    fn can_analyze_arithmetic_expressions() {
        let columns = vec![
            ColumnDefinition::new(TypeId::Integer, "id".to_owned(), 0, true),
            ColumnDefinition::new(TypeId::Text, "name".to_owned(), 1, true),
        ];
        let analyzer_test_suite = AnalyzerTestSuite::new().unwrap();
        analyzer_test_suite
            .create_table("accounts", columns)
            .unwrap();
        let table_id = analyzer_test_suite
            .catalog
            .get_table_id("accounts")
            .unwrap();
        let schema = analyzer_test_suite.catalog.get_schema("accounts").unwrap();

        let sql = "
            select -id as negative_id, id+1, 2 * (3+5) from accounts
        ";
        let query = analyzer_test_suite.analyze_query(sql).unwrap();

        let expected_query = LogicalPlan::Select(Query {
            from: TableReference::BaseTable {
                table_id,
                name: "accounts".to_owned(),
                schema,
                filter: vec![],
            },
            projections: vec![
                LogicalExpr::Unary {
                    op: UnaryOperator::Minus,
                    expr: Box::new(LogicalExpr::Column(vec![
                        "accounts".to_owned(),
                        "id".to_owned(),
                    ])),
                },
                LogicalExpr::Binary {
                    left: Box::new(LogicalExpr::Column(vec![
                        "accounts".to_owned(),
                        "id".to_owned(),
                    ])),
                    op: BinaryOperator::Plus,
                    right: Box::new(LogicalExpr::Integer(1)),
                },
                LogicalExpr::Binary {
                    left: Box::new(LogicalExpr::Integer(2)),
                    op: BinaryOperator::Multiply,
                    right: Box::new(LogicalExpr::Binary {
                        left: Box::new(LogicalExpr::Integer(3)),
                        op: BinaryOperator::Plus,
                        right: Box::new(LogicalExpr::Integer(5)),
                    }),
                },
            ],
            filter: vec![],
            output_schema: Schema::new(vec![
                ColumnDefinition::new(TypeId::Integer, "negative_id".to_owned(), 0, true),
                ColumnDefinition::new(TypeId::Integer, "id + 1".to_owned(), 1, true),
                ColumnDefinition::new(TypeId::Integer, "2 * (3 + 5)".to_owned(), 2, true),
            ]),
            values: vec![],
        });

        assert_eq!(query, expected_query);
    }

    #[test]
    fn can_analyze_values() {
        let analyzer_test_suite = AnalyzerTestSuite::new().unwrap();

        let sql = "
            values (1, NULL, 'foo', true), (2, 'bar', NULL, false);
        ";
        let query = analyzer_test_suite.analyze_query(sql).unwrap();
        let expected_output_schema = Schema::new(vec![
            ColumnDefinition::new(TypeId::Integer, "col_0".to_owned(), 0, true),
            ColumnDefinition::new(TypeId::Text, "col_1".to_owned(), 1, false),
            ColumnDefinition::new(TypeId::Text, "col_2".to_owned(), 2, false),
            ColumnDefinition::new(TypeId::Boolean, "col_3".to_owned(), 3, true),
        ]);

        let expected_query = LogicalPlan::Select(Query {
            from: TableReference::EmptyTable,
            values: vec![
                vec![
                    LogicalExpr::Integer(1),
                    LogicalExpr::Null,
                    LogicalExpr::String("foo".to_owned()),
                    LogicalExpr::Boolean(true),
                ],
                vec![
                    LogicalExpr::Integer(2),
                    LogicalExpr::String("bar".to_owned()),
                    LogicalExpr::Null,
                    LogicalExpr::Boolean(false),
                ],
            ],
            filter: vec![],
            projections: vec![],
            output_schema: expected_output_schema,
        });

        assert_eq!(query, expected_query);
    }

    #[test]
    fn can_analyze_count_aggregations() {
        let columns = vec![
            ColumnDefinition::new(TypeId::Integer, "id".to_owned(), 0, true),
            ColumnDefinition::new(TypeId::Text, "name".to_owned(), 1, true),
        ];
        let analyzer_test_suite = AnalyzerTestSuite::new().unwrap();
        analyzer_test_suite
            .create_table("accounts", columns)
            .unwrap();
        let table_id = analyzer_test_suite
            .catalog
            .get_table_id("accounts")
            .unwrap();
        let schema = analyzer_test_suite.catalog.get_schema("accounts").unwrap();
        let sql = "
            select count(name), 2 * count(name) as double_count from accounts
        ";
        let query = analyzer_test_suite.analyze_query(sql).unwrap();

        let expected_query = LogicalPlan::Select(Query {
            from: TableReference::BaseTable {
                table_id,
                name: "accounts".to_owned(),
                schema,
                filter: vec![],
            },
            projections: vec![
                LogicalExpr::Aggregation(
                    AggregationFunc::Count,
                    Box::new(LogicalExpr::Column(vec![
                        "accounts".to_owned(),
                        "name".to_owned(),
                    ])),
                ),
                LogicalExpr::Binary {
                    left: Box::new(LogicalExpr::Integer(2)),
                    op: BinaryOperator::Multiply,
                    right: Box::new(LogicalExpr::Aggregation(
                        AggregationFunc::Count,
                        Box::new(LogicalExpr::Column(vec![
                            "accounts".to_owned(),
                            "name".to_owned(),
                        ])),
                    )),
                },
            ],
            filter: vec![],
            output_schema: Schema::new(vec![
                ColumnDefinition::new(TypeId::Integer, "count(name)".to_owned(), 0, true),
                ColumnDefinition::new(TypeId::Integer, "double_count".to_owned(), 1, true),
            ]),
            values: vec![],
        });

        assert_eq!(query, expected_query);
    }

    #[test]
    fn mixing_aggregations_and_column_references_is_disallowed() {
        let columns = vec![
            ColumnDefinition::new(TypeId::Integer, "id".to_owned(), 0, true),
            ColumnDefinition::new(TypeId::Text, "name".to_owned(), 1, true),
        ];
        let analyzer_test_suite = AnalyzerTestSuite::new().unwrap();
        analyzer_test_suite
            .create_table("accounts", columns)
            .unwrap();
        let sql = "
            select id, count(name) as double_count from accounts
        ";

        let result = analyzer_test_suite.analyze_query(sql);
        assert!(result.is_err());
        assert_eq!(
            &result.err().unwrap().root_cause().to_string(),
            "column 'accounts.id' must be used in an aggregation"
        );
    }
}
