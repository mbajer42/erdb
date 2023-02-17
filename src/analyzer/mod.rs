use anyhow::{Error, Result};

use crate::catalog::schema::{ColumnDefinition, Schema, TypeId};
use crate::catalog::Catalog;
use crate::parser::ast::{self, BinaryOperator, Projection, Statement};

pub mod query;

use query::Query;

use self::query::{Expr, QueryType, Table};

pub struct Analyzer<'a> {
    catalog: &'a Catalog<'a>,
}

impl<'a> Analyzer<'a> {
    pub fn new(catalog: &'a Catalog<'a>) -> Self {
        Self { catalog }
    }

    pub fn analyze(&self, query: ast::Statement) -> Result<Query> {
        match query {
            Statement::Select {
                values,
                projections,
                from,
            } => self.analyze_select(values, projections, from),
            Statement::Insert { into, select } => self.analyze_insert(into, *select),
            _ => unreachable!(),
        }
    }

    fn analyze_insert(&self, into: ast::Table, select: Statement) -> Result<Query> {
        let (table_id, schema) = match self.analyze_table(into)? {
            Table::TableReference { table_id, schema } => (table_id, schema),
            _ => unreachable!(),
        };

        let mut query = self.analyze(select)?;

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

        query.query_type = QueryType::Insert;
        query.target = Some(table_id);
        query.target_schema = Some(schema);

        Ok(query)
    }

    fn analyze_select(
        &self,
        values: Option<Vec<Vec<ast::Expr>>>,
        projections: Vec<ast::Projection>,
        from: ast::Table,
    ) -> Result<Query> {
        if let Some(values) = values {
            return Self::analyze_values(values);
        }

        let table = self.analyze_table(from)?;
        let projections_with_specification = self.analyze_projections(projections, &table)?;

        let mut projections = vec![];
        let mut output_columns = vec![];

        for (col, (expr, name, type_id)) in projections_with_specification.into_iter().enumerate() {
            projections.push(expr);
            output_columns.push(ColumnDefinition::new(type_id, name, col as u8, false));
        }

        Ok(Query {
            query_type: QueryType::Select,
            values: None,
            from: table,
            projections,
            output_schema: Schema::new(output_columns),
            target_schema: None,
            target: None,
        })
    }

    fn analyze_values(values: Vec<Vec<ast::Expr>>) -> Result<Query> {
        let mut expressions = vec![];
        let mut output_columns = vec![];

        let mut first_row_added = false;
        for (row, current_values) in values.into_iter().enumerate() {
            let mut current_expressions = vec![];
            for (col, value) in current_values.into_iter().enumerate() {
                let (expr, type_id) = Self::analyze_expression(value, &Table::EmptyTable)?;

                if !first_row_added {
                    let column_name = format!("col_{}", col);
                    let not_null = expr != Expr::Null;
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
            query_type: QueryType::Select,
            values: Some(expressions),
            from: Table::EmptyTable,
            projections: vec![],
            output_schema: Schema::new(output_columns),
            target_schema: None,
            target: None,
        })
    }

    fn analyze_table(&self, table: ast::Table) -> Result<Table> {
        match table {
            ast::Table::TableReference { name, alias: _ } => {
                let table_id = self
                    .catalog
                    .get_table_id(&name)
                    .ok_or_else(|| Error::msg(format!("Could not find table {}", name)))?;
                let schema = self.catalog.get_schema(&name).unwrap().clone();
                Ok(Table::TableReference { table_id, schema })
            }
            ast::Table::EmptyTable => Ok(Table::EmptyTable),
        }
    }

    fn analyze_projections(
        &self,
        projections: Vec<ast::Projection>,
        scope: &Table,
    ) -> Result<Vec<(Expr, String, TypeId)>> {
        let mut result = vec![];
        let mut has_wildcard = false;

        for projection in projections.into_iter() {
            if projection == Projection::Wildcard {
                if !result.is_empty() {
                    return Err(Error::msg("`SELECT *` cannot have other expressions."));
                }
                has_wildcard = true;
                for col in scope.schema().columns() {
                    result.push((
                        Expr::ColumnReference(col.column_offset()),
                        col.column_name().to_owned(),
                        col.type_id(),
                    ))
                }
            } else {
                if has_wildcard {
                    return Err(Error::msg("`SELECT *` cannot have other expressions."));
                }
                result.push(self.analyze_projection(projection, scope)?)
            }
        }

        Ok(result)
    }

    fn analyze_projection(
        &self,
        projection: ast::Projection,
        scope: &Table,
    ) -> Result<(Expr, String, TypeId)> {
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
            Projection::Wildcard => unreachable!("Should be already handled"),
        }
    }

    fn analyze_expression(expr: ast::Expr, scope: &Table) -> Result<(Expr, TypeId)> {
        match expr {
            ast::Expr::Identifier(name) => {
                let column = scope
                    .schema()
                    .find_column(&name)
                    .ok_or_else(|| Error::msg(format!("Could not find column {}", name)))?;
                let column_offset = column.column_offset();
                let type_id = column.type_id();
                Ok((Expr::ColumnReference(column_offset), type_id))
            }
            ast::Expr::Number(number) => {
                let num = number.parse::<i32>()?;
                Ok((Expr::Integer(num), TypeId::Integer))
            }
            ast::Expr::String(s) => Ok((Expr::String(s), TypeId::Text)),
            ast::Expr::Boolean(val) => Ok((Expr::Boolean(val), TypeId::Boolean)),
            ast::Expr::Grouping(expr) => Self::analyze_expression(*expr, scope),
            ast::Expr::Binary { left, op, right } => {
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
                    Expr::Binary {
                        left: Box::new(left),
                        op,
                        right: Box::new(right),
                    },
                    result_type,
                ))
            }
            ast::Expr::Unary { op, expr } => {
                let (expr, type_id) = Self::analyze_expression(*expr, scope)?;
                if type_id != TypeId::Integer {
                    Err(Error::msg(format!(
                        "Cannot apply '{}' to type {}",
                        op, type_id
                    )))
                } else {
                    Ok((
                        Expr::Unary {
                            op,
                            expr: Box::new(expr),
                        },
                        type_id,
                    ))
                }
            }
            ast::Expr::IsNull(expr) => {
                let (expr, _) = Self::analyze_expression(*expr, scope)?;
                Ok((Expr::IsNull(Box::new(expr)), TypeId::Boolean))
            }
            ast::Expr::IsNotNull(expr) => {
                let (expr, _) = Self::analyze_expression(*expr, scope)?;
                Ok((Expr::IsNotNull(Box::new(expr)), TypeId::Boolean))
            }
            ast::Expr::Null => Ok((Expr::Null, TypeId::Unknown)),
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::query::{Expr, Query, QueryType, Table};
    use super::Analyzer;
    use crate::buffer::buffer_manager::BufferManager;
    use crate::catalog::schema::{ColumnDefinition, Schema, TypeId};
    use crate::catalog::Catalog;
    use crate::parser::ast::{BinaryOperator, UnaryOperator};
    use crate::parser::parse_sql;
    use crate::storage::file_manager::FileManager;

    #[test]
    fn can_bind_wildcard_select() {
        let data_dir = tempdir().unwrap();
        let file_manager = FileManager::new(data_dir.path()).unwrap();
        let buffer_manager = BufferManager::new(file_manager, 1);

        let mut catalog = Catalog::new(&buffer_manager, true).unwrap();
        let columns = vec![
            ColumnDefinition::new(TypeId::Integer, "id".to_owned(), 0, true),
            ColumnDefinition::new(TypeId::Text, "name".to_owned(), 1, true),
        ];
        catalog.create_table("accounts", columns).unwrap();
        let table_id = catalog.get_table_id("accounts").unwrap();
        let schema = catalog.get_schema("accounts").unwrap().clone();

        let sql = "
            select * from accounts
        ";
        let statement = parse_sql(sql).unwrap();
        let analyzer = Analyzer::new(&catalog);
        let query = analyzer.analyze(statement).unwrap();

        let expected_query = Query {
            query_type: QueryType::Select,
            values: None,
            from: Table::TableReference { table_id, schema },
            projections: vec![Expr::ColumnReference(0), Expr::ColumnReference(1)],
            output_schema: Schema::new(vec![
                ColumnDefinition::new(TypeId::Integer, "id".to_owned(), 0, false),
                ColumnDefinition::new(TypeId::Text, "name".to_owned(), 1, false),
            ]),
            target_schema: None,
            target: None,
        };

        assert_eq!(query, expected_query);
    }

    #[test]
    fn can_analyze_arithmetic_expressions() {
        let data_dir = tempdir().unwrap();
        let file_manager = FileManager::new(data_dir.path()).unwrap();
        let buffer_manager = BufferManager::new(file_manager, 1);

        let mut catalog = Catalog::new(&buffer_manager, true).unwrap();
        let columns = vec![ColumnDefinition::new(
            TypeId::Integer,
            "id".to_owned(),
            0,
            true,
        )];
        catalog.create_table("accounts", columns).unwrap();
        let table_id = catalog.get_table_id("accounts").unwrap();
        let schema = catalog.get_schema("accounts").unwrap().clone();

        let sql = "
            select -id as negative_id, id+1, 2 * (3+5) from accounts
        ";
        let statement = parse_sql(sql).unwrap();
        let analyzer = Analyzer::new(&catalog);
        let query = analyzer.analyze(statement).unwrap();

        let expected_query = Query {
            query_type: QueryType::Select,
            values: None,
            from: Table::TableReference { table_id, schema },
            projections: vec![
                Expr::Unary {
                    op: UnaryOperator::Minus,
                    expr: Box::new(Expr::ColumnReference(0)),
                },
                Expr::Binary {
                    left: Box::new(Expr::ColumnReference(0)),
                    op: BinaryOperator::Plus,
                    right: Box::new(Expr::Integer(1)),
                },
                Expr::Binary {
                    left: Box::new(Expr::Integer(2)),
                    op: BinaryOperator::Multiply,
                    right: Box::new(Expr::Binary {
                        left: Box::new(Expr::Integer(3)),
                        op: BinaryOperator::Plus,
                        right: Box::new(Expr::Integer(5)),
                    }),
                },
            ],
            output_schema: Schema::new(vec![
                ColumnDefinition::new(TypeId::Integer, "negative_id".to_owned(), 0, false),
                ColumnDefinition::new(TypeId::Integer, "id + 1".to_owned(), 1, false),
                ColumnDefinition::new(TypeId::Integer, "2 * (3 + 5)".to_owned(), 2, false),
            ]),
            target_schema: None,
            target: None,
        };

        assert_eq!(query, expected_query);
    }

    #[test]
    fn can_analyze_values() {
        let data_dir = tempdir().unwrap();
        let file_manager = FileManager::new(data_dir.path()).unwrap();
        let buffer_manager = BufferManager::new(file_manager, 1);

        let sql = "
            values (1, NULL, 'foo', true), (2, 'bar', NULL, false);
        ";
        let statement = parse_sql(sql).unwrap();

        let catalog = Catalog::new(&buffer_manager, true).unwrap();
        let analyzer = Analyzer::new(&catalog);
        let query = analyzer.analyze(statement).unwrap();

        let expected_query = Query {
            query_type: QueryType::Select,
            values: Some(vec![
                vec![
                    Expr::Integer(1),
                    Expr::Null,
                    Expr::String("foo".to_owned()),
                    Expr::Boolean(true),
                ],
                vec![
                    Expr::Integer(2),
                    Expr::String("bar".to_owned()),
                    Expr::Null,
                    Expr::Boolean(false),
                ],
            ]),
            from: Table::EmptyTable,
            projections: vec![],
            output_schema: Schema::new(vec![
                ColumnDefinition::new(TypeId::Integer, "col_0".to_owned(), 0, true),
                ColumnDefinition::new(TypeId::Text, "col_1".to_owned(), 1, false),
                ColumnDefinition::new(TypeId::Text, "col_2".to_owned(), 2, false),
                ColumnDefinition::new(TypeId::Boolean, "col_3".to_owned(), 3, true),
            ]),
            target_schema: None,
            target: None,
        };

        assert_eq!(query, expected_query);
    }
}
