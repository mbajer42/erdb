use anyhow::{Error, Result};

use crate::catalog::schema::{ColumnDefinition, Schema, TypeId};
use crate::catalog::Catalog;
use crate::parser::ast::{self, Projection, Statement};

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
            Statement::Select { projections, from } => self.analyze_select(projections, from),
            _ => unreachable!(),
        }
    }

    fn analyze_select(&self, projections: Vec<ast::Projection>, from: ast::Table) -> Result<Query> {
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
            from: table,
            projections,
            output_schema: Schema::new(output_columns),
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
            ast::Expr::Grouping(expr) => Self::analyze_expression(*expr, scope),
            ast::Expr::Binary { left, op, right } => {
                let (left, left_type) = Self::analyze_expression(*left, scope)?;
                let (right, right_type) = Self::analyze_expression(*right, scope)?;
                if left_type != TypeId::Integer {
                    Err(Error::msg(format!(
                        "Cannot apply '{}' to type {}",
                        op, left_type
                    )))
                } else if right_type != TypeId::Integer {
                    Err(Error::msg(format!(
                        "Cannot apply '{}' to type {}",
                        op, right_type
                    )))
                } else {
                    Ok((
                        Expr::Binary {
                            left: Box::new(left),
                            op,
                            right: Box::new(right),
                        },
                        left_type,
                    ))
                }
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
            // todo: boolean is of course not the actual type
            ast::Expr::Null => Ok((Expr::Null, TypeId::Boolean)),
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
            from: Table::TableReference { table_id, schema },
            projections: vec![Expr::ColumnReference(0), Expr::ColumnReference(1)],
            output_schema: Schema::new(vec![
                ColumnDefinition::new(TypeId::Integer, "id".to_owned(), 0, false),
                ColumnDefinition::new(TypeId::Text, "name".to_owned(), 1, false),
            ]),
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
        };

        assert_eq!(query, expected_query);
    }
}
