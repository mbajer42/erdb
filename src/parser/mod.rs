use std::collections::VecDeque;

use anyhow::{Error, Result};

use self::ast::{
    BinaryOperator, ColumnDefinition, DataType, Expr, Projection, Statement, Table, UnaryOperator,
};
use self::token::{tokenize, Keyword, Token};

pub mod ast;
mod token;

pub(in self::super) mod precedence {
    pub const NOT_NULL: u8 = 11;
    pub const IS_NULL: u8 = 12;
    pub const PLUS_MINUS: u8 = 14;
    pub const PRODUCT_DIVISION: u8 = 15;
}
pub struct Parser {
    tokens: VecDeque<Token>,
}

impl Parser {
    fn new(sql: &str) -> Result<Self> {
        let tokens = tokenize(sql)?;
        Ok(Self {
            tokens: tokens.into(),
        })
    }

    fn next_token(&mut self) -> Token {
        match self.tokens.pop_front() {
            Some(token) => token,
            None => Token::End,
        }
    }

    fn peek_token(&self) -> &Token {
        match self.tokens.front() {
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

        let from = self.parse_table()?;

        Ok(Statement::Select { projections, from })
    }

    fn parse_table(&mut self) -> Result<Table> {
        if self.peek_token() == &Token::Semicolon || self.peek_token() == &Token::End {
            return Ok(Table::EmptyTable);
        }

        self.expect(Token::Keyword(Keyword::From))?;
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
                        Ok(Projection::NamedExpr { expr, alias })
                    }
                    Token::Identifier(_s) => {
                        let alias = self.parse_identifier()?;
                        Ok(Projection::NamedExpr { expr, alias })
                    }
                    _ => Ok(Projection::UnnamedExpr(expr)),
                }
            }
        }
    }

    fn parse_expression(&mut self) -> Result<Expr> {
        self.parse_expression_with_precedence(0)
    }

    fn parse_expression_with_precedence(&mut self, precedence: u8) -> Result<Expr> {
        let mut expr = self.parse_prefix_expression()?;

        loop {
            let next_precedence = self.next_precedence();
            if precedence >= next_precedence {
                break;
            }

            expr = self.parse_infix_expression(expr, next_precedence)?;
        }

        Ok(expr)
    }

    fn parse_prefix_expression(&mut self) -> Result<Expr> {
        match self.next_token() {
            Token::Identifier(id) => Ok(Expr::Identifier(id)),
            Token::Number(num) => Ok(Expr::Number(num)),
            Token::Minus => {
                let expr = self.parse_expression_with_precedence(precedence::PLUS_MINUS)?;
                Ok(Expr::Unary {
                    op: UnaryOperator::Minus,
                    expr: Box::new(expr),
                })
            }
            Token::Keyword(Keyword::Null) => Ok(Expr::Null),
            Token::Plus => {
                let expr = self.parse_expression_with_precedence(precedence::PLUS_MINUS)?;
                Ok(Expr::Unary {
                    op: UnaryOperator::Plus,
                    expr: Box::new(expr),
                })
            }
            Token::LeftParen => {
                let expr = self.parse_expression()?;
                self.expect(Token::RightParen)?;
                Ok(Expr::Grouping(Box::new(expr)))
            }
            found => self.wrong_token("an expression", found)?,
        }
    }

    fn parse_infix_expression(&mut self, left: Expr, precedence: u8) -> Result<Expr> {
        match self.next_token() {
            token @ (Token::Plus | Token::Minus | Token::Star | Token::Division) => {
                let right = self.parse_expression_with_precedence(precedence)?;
                let binary_op = match token {
                    Token::Plus => BinaryOperator::Plus,
                    Token::Minus => BinaryOperator::Minus,
                    Token::Star => BinaryOperator::Multiply,
                    Token::Division => BinaryOperator::Divide,
                    _ => unreachable!(),
                };
                Ok(Expr::Binary {
                    left: Box::new(left),
                    op: binary_op,
                    right: Box::new(right),
                })
            }
            Token::Keyword(Keyword::Is) => {
                if self.peek_keywords_match(&[Keyword::Null]) {
                    self.advance(1);
                    Ok(Expr::IsNull(Box::new(left)))
                } else if self.peek_keywords_match(&[Keyword::Not, Keyword::Null]) {
                    self.advance(2);
                    Ok(Expr::IsNotNull(Box::new(left)))
                } else {
                    Err(Error::msg(format!(
                        "Expected 'NULL' or 'NOT NULL' but found {:?}",
                        self.next_token()
                    )))
                }
            }
            found => Err(Error::msg(format!(
                "Could not parse infix expression for {:?}",
                found
            ))),
        }
    }

    fn next_precedence(&self) -> u8 {
        match self.peek_token() {
            Token::Plus | Token::Minus => precedence::PLUS_MINUS,
            Token::Star | Token::Division => precedence::PRODUCT_DIVISION,
            Token::Keyword(Keyword::Is)
                if self.peek_keywords_match(&[Keyword::Is, Keyword::Null]) =>
            {
                precedence::IS_NULL
            }
            Token::Keyword(Keyword::Is)
                if self.peek_keywords_match(&[Keyword::Is, Keyword::Not, Keyword::Null]) =>
            {
                precedence::NOT_NULL
            }
            _ => 0,
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

    fn peek_keywords_match(&self, expected: &[Keyword]) -> bool {
        for (i, keyword) in expected.iter().enumerate() {
            match self.tokens.get(i) {
                Some(Token::Keyword(kw)) if kw == keyword => continue,
                _ => return false,
            }
        }
        true
    }

    fn advance(&mut self, mut cnt: usize) {
        while cnt > 0 {
            self.tokens.pop_front();
            cnt -= 1;
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
    use super::ast::{
        BinaryOperator, ColumnDefinition, DataType, Expr, Projection, Statement, Table,
        UnaryOperator,
    };
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

        let statement = parse_sql(sql).unwrap();
        let expected_statement = Statement::Select {
            projections: vec![
                Projection::UnnamedExpr(Expr::Identifier("id".to_owned())),
                Projection::NamedExpr {
                    expr: Expr::Identifier("name".to_owned()),
                    alias: "full_name".to_owned(),
                },
                Projection::NamedExpr {
                    expr: Expr::Identifier("active".to_owned()),
                    alias: "is_active".to_owned(),
                },
            ],
            from: Table::TableReference {
                name: "table1".to_owned(),
                alias: Some("table_alias".to_owned()),
            },
        };

        assert_eq!(statement, expected_statement);
    }

    #[test]
    fn can_parse_arithmetic_expression() {
        let sql = "
            select -id + 2 * (3 + 5) from table_1
        ";

        let statement = parse_sql(sql).unwrap();
        let expected_statement = Statement::Select {
            projections: vec![Projection::UnnamedExpr(Expr::Binary {
                left: Box::new(Expr::Unary {
                    op: UnaryOperator::Minus,
                    expr: Box::new(Expr::Identifier("id".to_owned())),
                }),
                op: BinaryOperator::Plus,
                right: Box::new(Expr::Binary {
                    left: Box::new(Expr::Number("2".to_owned())),
                    op: BinaryOperator::Multiply,
                    right: Box::new(Expr::Grouping(Box::new(Expr::Binary {
                        left: Box::new(Expr::Number("3".to_owned())),
                        op: BinaryOperator::Plus,
                        right: Box::new(Expr::Number("5".to_owned())),
                    }))),
                }),
            })],
            from: Table::TableReference {
                name: "table_1".to_owned(),
                alias: None,
            },
        };

        assert_eq!(statement, expected_statement);
    }

    #[test]
    fn can_parse_empty_tables() {
        let sql = "
            select -3 + 2 * (3 + 5);
        ";

        let statement = parse_sql(sql).unwrap();
        let expected_statement = Statement::Select {
            projections: vec![Projection::UnnamedExpr(Expr::Binary {
                left: Box::new(Expr::Unary {
                    op: UnaryOperator::Minus,
                    expr: Box::new(Expr::Number("3".to_owned())),
                }),
                op: BinaryOperator::Plus,
                right: Box::new(Expr::Binary {
                    left: Box::new(Expr::Number("2".to_owned())),
                    op: BinaryOperator::Multiply,
                    right: Box::new(Expr::Grouping(Box::new(Expr::Binary {
                        left: Box::new(Expr::Number("3".to_owned())),
                        op: BinaryOperator::Plus,
                        right: Box::new(Expr::Number("5".to_owned())),
                    }))),
                }),
            })],
            from: Table::EmptyTable,
        };

        assert_eq!(statement, expected_statement);
    }
}
