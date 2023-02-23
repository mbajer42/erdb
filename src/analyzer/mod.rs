use std::collections::VecDeque;

use anyhow::{Error, Result};

use crate::analyzer::logical_plan::Query;
use crate::catalog::schema::{ColumnDefinition, Schema, TypeId};
use crate::catalog::Catalog;
use crate::parser::ast::{
    self, BinaryOperator, ExprNode, JoinType, Projection, SelectStatement, Statement, TableNode,
};

pub mod logical_plan;

use logical_plan::LogicalPlan;

use self::logical_plan::{LogicalExpr, TableReference};

pub struct Analyzer<'a> {
    catalog: &'a Catalog<'a>,
}

impl<'a> Analyzer<'a> {
    pub fn new(catalog: &'a Catalog<'a>) -> Self {
        Self { catalog }
    }

    pub fn analyze(&self, query: ast::Statement) -> Result<LogicalPlan> {
        match query {
            Statement::Select(select) => Ok(LogicalPlan::Select(self.analyze_select(select)?)),
            Statement::Insert { into, select } => self.analyze_insert(into, select),
            _ => unreachable!(),
        }
    }

    fn analyze_insert(&self, into: ast::TableNode, select: SelectStatement) -> Result<LogicalPlan> {
        let (table_id, schema) = match self.analyze_table(into)? {
            TableReference::BaseTable {
                table_id,
                name: _,
                schema,
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

        for (col, (expr, name, type_id)) in projections_with_specification.into_iter().enumerate() {
            projections.push(expr);
            output_columns.push(ColumnDefinition::new(type_id, name, col as u8, false));
        }

        let filter = if let Some(filter_expr) = filter {
            let (expr, type_id) = Self::analyze_expression(filter_expr, &table)?;
            if type_id != TypeId::Unknown && type_id != TypeId::Boolean {
                return Err(Error::msg(format!(
                    "WHERE condition must evaluate to boolean, but evaluates to {}",
                    type_id
                )));
            }
            Some(expr)
        } else {
            None
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
                let (expr, type_id) = Self::analyze_expression(value, &TableReference::EmptyTable)?;

                if !first_row_added {
                    let column_name = format!("col_{}", col);
                    let not_null = expr != LogicalExpr::Null;
                    output_columns.push(ColumnDefinition::new(
                        type_id,
                        column_name,
                        col as u8,
                        not_null,
                    ));
                } else if let Some(col_def) = output_columns.get_mut(col) {
                    if col_def.type_id() == TypeId::Unknown {
                        // first value was null
                        col_def.set_type_id(type_id);
                    } else if type_id == TypeId::Unknown {
                        // current value is null, so column is nullable
                        col_def.set_not_null(false);
                    } else if col_def.type_id() != type_id {
                        return Err(Error::msg(format!(
                            "Type mismatch in row {}. Expected '{}' but found '{}'",
                            row,
                            col_def.type_id(),
                            type_id
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
            filter: None,
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
                let (on_expr, on_type) = Self::analyze_expression(on, &result_table)?;

                if on_type != TypeId::Boolean && on_type != TypeId::Unknown {
                    return Err(Error::msg(format!(
                        "JOIN conditions must evaluate to boolean but evaluates to: {}",
                        on_type
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
    ) -> Result<Vec<(LogicalExpr, String, TypeId)>> {
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
    ) -> Result<(LogicalExpr, String, TypeId)> {
        match projection {
            Projection::UnnamedExpr(expr) => {
                let alias = expr.to_string();
                let (expr, type_id) = Self::analyze_expression(expr, scope)?;
                Ok((expr, alias, type_id))
            }
            Projection::NamedExpr { expr, alias } => {
                let (expr, type_id) = Self::analyze_expression(expr, scope)?;
                Ok((expr, alias, type_id))
            }
            Projection::Wildcard | Projection::QualifiedWildcard { table: _ } => {
                unreachable!("Should be already handled")
            }
        }
    }

    fn analyze_expression(
        expr: ast::ExprNode,
        scope: &TableReference,
    ) -> Result<(LogicalExpr, TypeId)> {
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
                if let Some(res) = column {
                    Ok(res)
                } else {
                    Err(Error::msg(format!(
                        "Could not find column {}.{}",
                        table, column_name
                    )))
                }
            }

            ExprNode::Number(number) => {
                let num = number.parse::<i32>()?;
                Ok((LogicalExpr::Integer(num), TypeId::Integer))
            }
            ExprNode::String(s) => Ok((LogicalExpr::String(s), TypeId::Text)),
            ExprNode::Boolean(val) => Ok((LogicalExpr::Boolean(val), TypeId::Boolean)),
            ExprNode::Grouping(expr) => Self::analyze_expression(*expr, scope),
            ExprNode::Binary { left, op, right } => {
                let (left, left_type) = Self::analyze_expression(*left, scope)?;
                let (right, right_type) = Self::analyze_expression(*right, scope)?;
                let result_type = match op {
                    BinaryOperator::Plus
                    | BinaryOperator::Minus
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide
                    | BinaryOperator::Modulo => {
                        if left_type != TypeId::Integer || right_type != TypeId::Integer {
                            return Err(Error::msg(format!(
                                "Arguments for '{}' must be of type integer. Left: {}, Right: {}",
                                op, left_type, right_type
                            )));
                        }
                        TypeId::Integer
                    }
                    BinaryOperator::Eq
                    | BinaryOperator::NotEq
                    | BinaryOperator::Less
                    | BinaryOperator::LessEq
                    | BinaryOperator::Greater
                    | BinaryOperator::GreaterEq => {
                        if left_type != right_type
                            && left_type != TypeId::Unknown
                            && right_type != TypeId::Unknown
                        {
                            return Err(Error::msg(format!(
                                "Arguments for '{}' must be of same type. Left: {}, Right: {}",
                                op, left_type, right_type
                            )));
                        }
                        TypeId::Boolean
                    }
                    BinaryOperator::And | BinaryOperator::Or => {
                        let valid_types = [TypeId::Boolean, TypeId::Unknown];
                        if !valid_types.contains(&left_type) || !valid_types.contains(&right_type) {
                            return Err(Error::msg(format!(
                                "Arguments for '{}' must be of type boolean. Left: {}, Right: {}",
                                op, left_type, right_type
                            )));
                        }
                        TypeId::Boolean
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
                let (expr, type_id) = Self::analyze_expression(*expr, scope)?;
                if type_id != TypeId::Integer {
                    Err(Error::msg(format!(
                        "Cannot apply '{}' to type {}",
                        op, type_id
                    )))
                } else {
                    Ok((
                        LogicalExpr::Unary {
                            op,
                            expr: Box::new(expr),
                        },
                        type_id,
                    ))
                }
            }
            ExprNode::IsNull(expr) => {
                let (expr, _) = Self::analyze_expression(*expr, scope)?;
                Ok((LogicalExpr::IsNull(Box::new(expr)), TypeId::Boolean))
            }
            ExprNode::IsNotNull(expr) => {
                let (expr, _) = Self::analyze_expression(*expr, scope)?;
                Ok((LogicalExpr::IsNotNull(Box::new(expr)), TypeId::Boolean))
            }
            ExprNode::Null => Ok((LogicalExpr::Null, TypeId::Unknown)),
        }
    }

    fn identify_column(
        scope: &TableReference,
        table: Option<&str>,
        column: &str,
    ) -> Result<Option<(LogicalExpr, TypeId)>> {
        match scope {
            TableReference::BaseTable {
                table_id: _,
                name,
                schema,
            } => {
                if let Some(table) = table {
                    if name != table {
                        return Ok(None);
                    }
                }
                let column = schema.find_column(column).map(|col_def| {
                    (
                        LogicalExpr::Column(vec![name.clone(), col_def.column_name().to_owned()]),
                        col_def.type_id(),
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
    ) -> Vec<(LogicalExpr, String, TypeId)> {
        match scope {
            TableReference::BaseTable {
                table_id: _,
                name: table_name,
                schema,
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
                        (
                            LogicalExpr::Column(vec![
                                table_name.clone(),
                                col_def.column_name().to_owned(),
                            ]),
                            format!("{}.{}", table_name, col_def.column_name()),
                            col_def.type_id(),
                        )
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
    use tempfile::tempdir;

    use super::logical_plan::{LogicalExpr, LogicalPlan, TableReference};
    use super::Analyzer;
    use crate::analyzer::logical_plan::Query;
    use crate::buffer::buffer_manager::BufferManager;
    use crate::catalog::schema::{ColumnDefinition, Schema, TypeId};
    use crate::catalog::Catalog;
    use crate::concurrency::TransactionManager;
    use crate::parser::ast::{BinaryOperator, UnaryOperator};
    use crate::parser::parse_sql;
    use crate::storage::file_manager::FileManager;

    #[test]
    fn can_bind_wildcard_select() {
        let data_dir = tempdir().unwrap();
        let file_manager = FileManager::new(data_dir.path()).unwrap();
        let buffer_manager = BufferManager::new(file_manager, 1);
        let transaction_manager = TransactionManager::new(&buffer_manager, true).unwrap();
        let bootstrap_transaction = transaction_manager.bootstrap();

        let catalog = Catalog::new(&buffer_manager, true, &bootstrap_transaction).unwrap();
        let columns = vec![
            ColumnDefinition::new(TypeId::Integer, "id".to_owned(), 0, true),
            ColumnDefinition::new(TypeId::Text, "name".to_owned(), 1, true),
        ];
        catalog.create_table("accounts", columns).unwrap();
        let table_id = catalog.get_table_id("accounts").unwrap();
        let schema = catalog.get_schema("accounts").unwrap();

        let sql = "
            select * from accounts
        ";
        let statement = parse_sql(sql).unwrap();
        let analyzer = Analyzer::new(&catalog);
        let query = analyzer.analyze(statement).unwrap();

        let expected_query = LogicalPlan::Select(Query {
            from: TableReference::BaseTable {
                table_id,
                name: "accounts".to_owned(),
                schema,
            },
            projections: vec![
                LogicalExpr::Column(vec!["accounts".to_owned(), "id".to_owned()]),
                LogicalExpr::Column(vec!["accounts".to_owned(), "name".to_owned()]),
            ],
            filter: None,
            output_schema: Schema::new(vec![
                ColumnDefinition::new(TypeId::Integer, "accounts.id".to_owned(), 0, false),
                ColumnDefinition::new(TypeId::Text, "accounts.name".to_owned(), 1, false),
            ]),
            values: vec![],
        });

        assert_eq!(query, expected_query);
    }

    #[test]
    fn can_bind_qualified_wildcard_select() {
        let data_dir = tempdir().unwrap();
        let file_manager = FileManager::new(data_dir.path()).unwrap();
        let buffer_manager = BufferManager::new(file_manager, 1);
        let transaction_manager = TransactionManager::new(&buffer_manager, true).unwrap();
        let bootstrap_transaction = transaction_manager.bootstrap();

        let catalog = Catalog::new(&buffer_manager, true, &bootstrap_transaction).unwrap();
        let columns = vec![
            ColumnDefinition::new(TypeId::Integer, "id".to_owned(), 0, true),
            ColumnDefinition::new(TypeId::Text, "name".to_owned(), 1, true),
        ];
        catalog.create_table("accounts", columns).unwrap();
        let table_id = catalog.get_table_id("accounts").unwrap();
        let schema = catalog.get_schema("accounts").unwrap();

        let sql = "
            select acc.* from accounts acc
        ";
        let statement = parse_sql(sql).unwrap();
        let analyzer = Analyzer::new(&catalog);
        let query = analyzer.analyze(statement).unwrap();

        let expected_query = LogicalPlan::Select(Query {
            from: TableReference::BaseTable {
                table_id,
                name: "acc".to_owned(),
                schema,
            },
            projections: vec![
                LogicalExpr::Column(vec!["acc".to_owned(), "id".to_owned()]),
                LogicalExpr::Column(vec!["acc".to_owned(), "name".to_owned()]),
            ],
            filter: None,
            output_schema: Schema::new(vec![
                ColumnDefinition::new(TypeId::Integer, "acc.id".to_owned(), 0, false),
                ColumnDefinition::new(TypeId::Text, "acc.name".to_owned(), 1, false),
            ]),
            values: vec![],
        });

        assert_eq!(query, expected_query);
    }

    #[test]
    fn can_analyze_arithmetic_expressions() {
        let data_dir = tempdir().unwrap();
        let file_manager = FileManager::new(data_dir.path()).unwrap();
        let buffer_manager = BufferManager::new(file_manager, 1);
        let transaction_manager = TransactionManager::new(&buffer_manager, true).unwrap();
        let bootstrap_transaction = transaction_manager.bootstrap();

        let catalog = Catalog::new(&buffer_manager, true, &bootstrap_transaction).unwrap();
        let columns = vec![ColumnDefinition::new(
            TypeId::Integer,
            "id".to_owned(),
            0,
            true,
        )];
        catalog.create_table("accounts", columns).unwrap();
        let table_id = catalog.get_table_id("accounts").unwrap();
        let schema = catalog.get_schema("accounts").unwrap();

        let sql = "
            select -id as negative_id, id+1, 2 * (3+5) from accounts
        ";
        let statement = parse_sql(sql).unwrap();
        let analyzer = Analyzer::new(&catalog);
        let query = analyzer.analyze(statement).unwrap();

        let expected_query = LogicalPlan::Select(Query {
            from: TableReference::BaseTable {
                table_id,
                name: "accounts".to_owned(),
                schema,
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
            filter: None,
            output_schema: Schema::new(vec![
                ColumnDefinition::new(TypeId::Integer, "negative_id".to_owned(), 0, false),
                ColumnDefinition::new(TypeId::Integer, "id + 1".to_owned(), 1, false),
                ColumnDefinition::new(TypeId::Integer, "2 * (3 + 5)".to_owned(), 2, false),
            ]),
            values: vec![],
        });

        assert_eq!(query, expected_query);
    }

    #[test]
    fn can_analyze_values() {
        let data_dir = tempdir().unwrap();
        let file_manager = FileManager::new(data_dir.path()).unwrap();
        let buffer_manager = BufferManager::new(file_manager, 1);
        let transaction_manager = TransactionManager::new(&buffer_manager, true).unwrap();
        let bootstrap_transaction = transaction_manager.bootstrap();

        let sql = "
            values (1, NULL, 'foo', true), (2, 'bar', NULL, false);
        ";
        let statement = parse_sql(sql).unwrap();

        let catalog = Catalog::new(&buffer_manager, true, &bootstrap_transaction).unwrap();
        let analyzer = Analyzer::new(&catalog);
        let query = analyzer.analyze(statement).unwrap();
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
            filter: None,
            projections: vec![],
            output_schema: expected_output_schema,
        });

        assert_eq!(query, expected_query);
    }
}
