use std::iter::Peekable;
use std::vec::IntoIter;

use anyhow::{Error, Result};

use self::ast::{ColumnDefinition, DataType, Expr, Projection, Statement, Table};
use self::token::{tokenize, Keyword, Token};

pub mod ast;
mod token;

pub struct Parser {
    tokens: Peekable<IntoIter<Token>>,
}

impl Parser {
    fn new(sql: &str) -> Result<Self> {
        let tokens = tokenize(sql)?;
        Ok(Self {
            tokens: tokens.into_iter().peekable(),
        })
    }

    fn next_token(&mut self) -> Token {
        match self.tokens.next() {
            Some(token) => token,
            None => Token::End,
        }
    }

    fn peek_token(&mut self) -> &Token {
        match self.tokens.peek() {
            Some(token) => token,
            None => &Token::End,
        }
    }

    fn parse_statement(&mut self) -> Result<Statement> {
        match self.next_token() {
            Token::Keyword(keyword) => match keyword {
                Keyword::Create => self.parse_create_statement(),
                Keyword::Select => self.parse_select_statement(),
                found => self.wrong_keyword("a statement", found)?,
            },
            found => self.wrong_token("a statement", found)?,
        }
    }

    fn parse_select_statement(&mut self) -> Result<Statement> {
        let projections = self.parse_projections()?;

        self.expect(Token::Keyword(Keyword::From))?;
        let from = self.parse_table()?;

        Ok(Statement::Select { projections, from })
    }

    fn parse_table(&mut self) -> Result<Table> {
        let table_name = self.parse_identifier()?;

        let alias = if self.peek_token() == &Token::Keyword(Keyword::As) {
            self.next_token();
            Some(self.parse_identifier()?)
        } else if let Token::Identifier(_s) = self.peek_token() {
            Some(self.parse_identifier()?)
        } else {
            None
        };

        Ok(Table::TableReference {
            name: table_name,
            alias,
        })
    }

    fn parse_projections(&mut self) -> Result<Vec<Projection>> {
        let mut projections = vec![];

        loop {
            projections.push(self.parse_projection()?);
            if self.peek_token() == &Token::Comma {
                self.next_token();
            } else {
                break;
            }
        }

        Ok(projections)
    }

    fn parse_projection(&mut self) -> Result<Projection> {
        match self.peek_token() {
            Token::Star => {
                self.next_token();
                Ok(Projection::Wildcard)
            }
            _ => {
                let expr = self.parse_expression()?;
                match self.peek_token() {
                    Token::Keyword(Keyword::As) => {
                        // consume 'AS'
                        self.next_token();
                        let alias = self.parse_identifier()?;
                        Ok(Projection::NamedExpr {
                            expression: expr,
                            alias,
                        })
                    }
                    Token::Identifier(_s) => {
                        let alias = self.parse_identifier()?;
                        Ok(Projection::NamedExpr {
                            expression: expr,
                            alias,
                        })
                    }
                    _ => Ok(Projection::UnnamedExpr(expr)),
                }
            }
        }
    }

    fn parse_expression(&mut self) -> Result<Expr> {
        match self.next_token() {
            Token::Identifier(s) => Ok(Expr::Identifier(s)),
            found => self.wrong_token("an expression", found)?,
        }
    }

    fn parse_create_statement(&mut self) -> Result<Statement> {
        match self.next_token() {
            Token::Keyword(keyword) => match keyword {
                Keyword::Table => self.parse_create_table_statement(),
                found => self.wrong_keyword("a create statement", found)?,
            },
            found => self.wrong_token("a create stament", found)?,
        }
    }

    fn parse_create_table_statement(&mut self) -> Result<Statement> {
        let name = self.parse_identifier()?;

        Ok(Statement::CreateTable {
            name,
            columns: self.parse_column_definitions()?,
        })
    }

    fn parse_column_definitions(&mut self) -> Result<Vec<ColumnDefinition>> {
        self.expect(Token::LeftParen)?;

        let mut columns = vec![];
        let mut offset = 0;

        loop {
            columns.push(self.parse_column_definition(offset)?);
            let comma = if self.peek_token() == &Token::Comma {
                self.next_token();
                true
            } else {
                false
            };

            if self.peek_token() == &Token::RightParen {
                self.next_token();
                break;
            } else if !comma {
                let token = self.next_token();
                self.wrong_token("')' or ',' after a column definition", token)?;
            }

            offset = offset.wrapping_add(1);
            if offset == 0 {
                return Err(Error::msg("Only 256 columns are currently supported"));
            }
        }

        Ok(columns)
    }

    fn parse_column_definition(&mut self, offset: u8) -> Result<ColumnDefinition> {
        let column_name = self.parse_identifier()?;
        let data_type = self.parse_data_type()?;

        let not_null = if self.peek_token() == &Token::Keyword(Keyword::Not) {
            self.next_token();
            self.expect(Token::Keyword(Keyword::Null))?;
            true
        } else if self.peek_token() == &Token::Keyword(Keyword::Null) {
            self.next_token();
            false
        } else {
            false
        };

        Ok(ColumnDefinition {
            name: column_name,
            data_type,
            offset,
            not_null,
        })
    }

    fn parse_data_type(&mut self) -> Result<DataType> {
        let token = self.next_token();
        let data_type = match token {
            Token::Keyword(keyword) => match keyword {
                Keyword::Boolean => DataType::Boolean,
                Keyword::Integer => DataType::Integer,
                Keyword::Text => DataType::Text,
                found => self.wrong_keyword("a data type", found)?,
            },
            found => self.wrong_token("a data type", found)?,
        };

        Ok(data_type)
    }

    fn parse_identifier(&mut self) -> Result<String> {
        match self.next_token() {
            Token::Identifier(s) => Ok(s),
            found => self.wrong_token("an identifier", found)?,
        }
    }

    fn expect(&mut self, expected_token: Token) -> Result<()> {
        let token = self.next_token();
        if token != expected_token {
            self.wrong_token(&format!("{:?}", expected_token), token)?
        } else {
            Ok(())
        }
    }

    fn wrong_token<T>(&self, expected: &str, found: Token) -> Result<T> {
        Err(Error::msg(format!(
            "Expected {}, but found: {:?}",
            expected, found
        )))
    }

    fn wrong_keyword<T>(&self, expected: &str, found: Keyword) -> Result<T> {
        Err(Error::msg(format!(
            "Expected {}, but found: {:?}",
            expected, found
        )))
    }
}

pub fn parse_sql(sql: &str) -> Result<Statement> {
    let mut parser = Parser::new(sql)?;
    parser.parse_statement()
}

#[cfg(test)]
mod tests {
    use super::ast::{ColumnDefinition, DataType, Expr, Projection, Statement, Table};
    use super::parse_sql;

    #[test]
    fn can_parse_create_table_statements() {
        let sql = "
            create table accounts (
                id integer not null,
                name text not null,
                active boolean null,
                email text
            );
        ";

        let statement = parse_sql(sql).unwrap();
        let expected_statement = Statement::CreateTable {
            name: "accounts".to_owned(),
            columns: vec![
                ColumnDefinition {
                    name: "id".to_owned(),
                    data_type: DataType::Integer,
                    offset: 0,
                    not_null: true,
                },
                ColumnDefinition {
                    name: "name".to_owned(),
                    data_type: DataType::Text,
                    offset: 1,
                    not_null: true,
                },
                ColumnDefinition {
                    name: "active".to_owned(),
                    data_type: DataType::Boolean,
                    offset: 2,
                    not_null: false,
                },
                ColumnDefinition {
                    name: "email".to_owned(),
                    data_type: DataType::Text,
                    offset: 3,
                    not_null: false,
                },
            ],
        };

        assert_eq!(statement, expected_statement);
    }

    #[test]
    fn can_parse_wildcard_select_statement() {
        let sql = "
            select * from accounts
        ";

        let statement = parse_sql(sql).unwrap();
        let expected_statement = Statement::Select {
            projections: vec![Projection::Wildcard],
            from: Table::TableReference {
                name: "accounts".to_owned(),
                alias: None,
            },
        };

        assert_eq!(statement, expected_statement);
    }

    #[test]
    fn can_parse_select_statements_with_aliases() {
        let sql = "
            select id, name as full_name, active is_active
            from table1 table_alias
        ";

        let _statement = parse_sql(sql).unwrap();
        let _expected_statement = Statement::Select {
            projections: vec![
                Projection::UnnamedExpr(Expr::Identifier("id".to_owned())),
                Projection::NamedExpr {
                    expression: Expr::Identifier("name".to_owned()),
                    alias: "full_name".to_owned(),
                },
                Projection::NamedExpr {
                    expression: Expr::Identifier("active".to_owned()),
                    alias: "is_active".to_owned(),
                },
            ],
            from: Table::TableReference {
                name: "table1".to_owned(),
                alias: Some("table_alias".to_owned()),
            },
        };
    }
}
