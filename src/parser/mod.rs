use std::collections::VecDeque;

use anyhow::{Error, Result};

use self::ast::{
    BinaryOperator, ColumnDefinition, DataType, ExprNode, Projection, Statement, Table,
    UnaryOperator,
};
use self::token::{tokenize, Keyword, Token};

pub mod ast;
mod token;

/// taken from https://www.postgresql.org/docs/current/sql-syntax-lexical.html
pub(in self::super) mod precedence {
    pub const OR: u8 = 1;
    pub const AND: u8 = 2;
    pub const IS: u8 = 4;
    pub const COMPARISON: u8 = 5;
    pub const PLUS_MINUS: u8 = 8;
    pub const PRODUCT_DIVISION_MODULO: u8 = 9;
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

    fn peek_token_ahead(&self, ahead: usize) -> &Token {
        match self.tokens.get(ahead) {
            Some(token) => token,
            None => &Token::End,
        }
    }

    fn parse_statement(&mut self) -> Result<Statement> {
        match self.next_token() {
            Token::Keyword(keyword) => match keyword {
                Keyword::Create => self.parse_create_statement(),
                Keyword::Select => self.parse_select_statement(),
                Keyword::Values => self.parse_values(),
                Keyword::Insert => self.parse_insert(),
                found => self.wrong_keyword("a statement", found)?,
            },
            found => self.wrong_token("a statement", found)?,
        }
    }

    fn parse_insert(&mut self) -> Result<Statement> {
        self.expect(Token::Keyword(Keyword::Into))?;

        let table_name = self.parse_identifier()?;
        let table = Table::TableReference {
            name: table_name,
            alias: None,
        };

        let select = match self.next_token() {
            Token::Keyword(Keyword::Values) => self.parse_values()?,
            Token::Keyword(Keyword::Select) => self.parse_select_statement()?,
            found => {
                return Err(Error::msg(format!(
                    "Expected a query after `INSERT INTO <table_name>`, but found {:?}",
                    found
                )))
            }
        };

        Ok(Statement::Insert {
            into: table,
            select: Box::new(select),
        })
    }

    fn parse_values(&mut self) -> Result<Statement> {
        let mut values = vec![];
        loop {
            self.expect(Token::LeftParen)?;

            let mut current_values = vec![];
            loop {
                current_values.push(self.parse_expression()?);

                match self.next_token() {
                    Token::Comma => continue,
                    Token::RightParen => break,
                    found => {
                        self.wrong_token("',' followed by another expression or ')'", found)?
                    }
                }
            }
            values.push(current_values);

            match self.next_token() {
                Token::Comma => continue,
                Token::Semicolon | Token::End => break,
                found => self.wrong_token(
                    "Expected ',' followed by more expressions or end of statement",
                    found,
                )?,
            }
        }

        Ok(Statement::Select {
            values: Some(values),
            projections: vec![],
            from: Table::EmptyTable,
            filter: None,
        })
    }

    fn parse_select_statement(&mut self) -> Result<Statement> {
        let projections = self.parse_projections()?;

        let from = self.parse_table()?;
        let filter = self.parse_filter()?;

        Ok(Statement::Select {
            values: None,
            projections,
            from,
            filter,
        })
    }

    fn parse_filter(&mut self) -> Result<Option<ExprNode>> {
        match self.next_token() {
            Token::Keyword(Keyword::Where) => Ok(Some(self.parse_expression()?)),
            Token::End | Token::Semicolon => Ok(None),
            found => self.wrong_token("end of statement or WHERE", found),
        }
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
        let peek1 = self.peek_token_ahead(0);
        let peek2 = self.peek_token_ahead(1);
        let peek3 = self.peek_token_ahead(2);
        match peek1 {
            Token::Star => {
                self.next_token();
                Ok(Projection::Wildcard)
            }
            // SELECT a.* FROM a
            Token::Identifier(_) if peek2 == &Token::Dot && peek3 == &Token::Star => {
                let table_name = self.parse_identifier()?;
                let _dot = self.next_token();
                let _star = self.next_token();
                Ok(Projection::QualifiedWildcard { table: table_name })
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

    fn parse_expression(&mut self) -> Result<ExprNode> {
        self.parse_expression_with_precedence(0)
    }

    fn parse_expression_with_precedence(&mut self, precedence: u8) -> Result<ExprNode> {
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

    fn parse_prefix_expression(&mut self) -> Result<ExprNode> {
        match self.next_token() {
            Token::Identifier(id) => {
                if self.peek_token() == &Token::Dot {
                    let _dot = self.next_token();
                    let col = self.parse_identifier()?;
                    Ok(ExprNode::QualifiedIdentifier(id, col))
                } else {
                    Ok(ExprNode::Identifier(id))
                }
            }
            Token::Number(num) => Ok(ExprNode::Number(num)),
            Token::QuotedString(s) => Ok(ExprNode::String(s)),
            Token::Keyword(Keyword::True) => Ok(ExprNode::Boolean(true)),
            Token::Keyword(Keyword::False) => Ok(ExprNode::Boolean(false)),
            Token::Minus => {
                let expr = self.parse_expression_with_precedence(precedence::PLUS_MINUS)?;
                Ok(ExprNode::Unary {
                    op: UnaryOperator::Minus,
                    expr: Box::new(expr),
                })
            }
            Token::Keyword(Keyword::Null) => Ok(ExprNode::Null),
            Token::Plus => {
                let expr = self.parse_expression_with_precedence(precedence::PLUS_MINUS)?;
                Ok(ExprNode::Unary {
                    op: UnaryOperator::Plus,
                    expr: Box::new(expr),
                })
            }
            Token::LeftParen => {
                let expr = self.parse_expression()?;
                self.expect(Token::RightParen)?;
                Ok(ExprNode::Grouping(Box::new(expr)))
            }
            found => self.wrong_token("an expression", found)?,
        }
    }

    fn parse_infix_expression(&mut self, left: ExprNode, precedence: u8) -> Result<ExprNode> {
        match self.next_token() {
            token @ (Token::Plus
            | Token::Minus
            | Token::Star
            | Token::Division
            | Token::Modulo
            | Token::Eq
            | Token::NotEq
            | Token::Less
            | Token::LessEq
            | Token::Greater
            | Token::GreaterEq
            | Token::Keyword(Keyword::And)
            | Token::Keyword(Keyword::Or)) => {
                let right = self.parse_expression_with_precedence(precedence)?;
                let binary_op = match token {
                    Token::Plus => BinaryOperator::Plus,
                    Token::Minus => BinaryOperator::Minus,
                    Token::Star => BinaryOperator::Multiply,
                    Token::Division => BinaryOperator::Divide,
                    Token::Modulo => BinaryOperator::Modulo,
                    Token::Eq => BinaryOperator::Eq,
                    Token::NotEq => BinaryOperator::NotEq,
                    Token::Less => BinaryOperator::Less,
                    Token::LessEq => BinaryOperator::LessEq,
                    Token::Greater => BinaryOperator::Greater,
                    Token::GreaterEq => BinaryOperator::GreaterEq,
                    Token::Keyword(Keyword::And) => BinaryOperator::And,
                    Token::Keyword(Keyword::Or) => BinaryOperator::Or,
                    _ => unreachable!(),
                };
                Ok(ExprNode::Binary {
                    left: Box::new(left),
                    op: binary_op,
                    right: Box::new(right),
                })
            }
            Token::Keyword(Keyword::Is) => {
                if self.peek_keywords_match(&[Keyword::Null]) {
                    self.advance(1);
                    Ok(ExprNode::IsNull(Box::new(left)))
                } else if self.peek_keywords_match(&[Keyword::Not, Keyword::Null]) {
                    self.advance(2);
                    Ok(ExprNode::IsNotNull(Box::new(left)))
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
            Token::Star | Token::Division | Token::Modulo => precedence::PRODUCT_DIVISION_MODULO,
            Token::Eq
            | Token::NotEq
            | Token::Less
            | Token::LessEq
            | Token::Greater
            | Token::GreaterEq => precedence::COMPARISON,
            Token::Keyword(Keyword::Is) => precedence::IS,
            Token::Keyword(Keyword::And) => precedence::AND,
            Token::Keyword(Keyword::Or) => precedence::OR,
            _ => 0,
        }
    }

    fn parse_create_statement(&mut self) -> Result<Statement> {
        match self.next_token() {
            Token::Keyword(keyword) => match keyword {
                Keyword::Table => self.parse_create_table_statement(),
                found => self.wrong_keyword("a create statement", found)?,
            },
            found => self.wrong_token("a create statement", found)?,
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
        BinaryOperator, ColumnDefinition, DataType, ExprNode, Projection, Statement, Table,
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
            values: None,
            projections: vec![Projection::Wildcard],
            from: Table::TableReference {
                name: "accounts".to_owned(),
                alias: None,
            },
            filter: None,
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
            values: None,
            projections: vec![
                Projection::UnnamedExpr(ExprNode::Identifier("id".to_owned())),
                Projection::NamedExpr {
                    expr: ExprNode::Identifier("name".to_owned()),
                    alias: "full_name".to_owned(),
                },
                Projection::NamedExpr {
                    expr: ExprNode::Identifier("active".to_owned()),
                    alias: "is_active".to_owned(),
                },
            ],
            from: Table::TableReference {
                name: "table1".to_owned(),
                alias: Some("table_alias".to_owned()),
            },
            filter: None,
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
            values: None,
            projections: vec![Projection::UnnamedExpr(ExprNode::Binary {
                left: Box::new(ExprNode::Unary {
                    op: UnaryOperator::Minus,
                    expr: Box::new(ExprNode::Identifier("id".to_owned())),
                }),
                op: BinaryOperator::Plus,
                right: Box::new(ExprNode::Binary {
                    left: Box::new(ExprNode::Number("2".to_owned())),
                    op: BinaryOperator::Multiply,
                    right: Box::new(ExprNode::Grouping(Box::new(ExprNode::Binary {
                        left: Box::new(ExprNode::Number("3".to_owned())),
                        op: BinaryOperator::Plus,
                        right: Box::new(ExprNode::Number("5".to_owned())),
                    }))),
                }),
            })],
            from: Table::TableReference {
                name: "table_1".to_owned(),
                alias: None,
            },
            filter: None,
        };

        assert_eq!(statement, expected_statement);
    }

    #[test]
    fn can_parse_comparisons_eq() {
        let comparison_operators = [
            BinaryOperator::Eq,
            BinaryOperator::NotEq,
            BinaryOperator::Less,
            BinaryOperator::LessEq,
            BinaryOperator::Greater,
            BinaryOperator::GreaterEq,
        ];

        for op in comparison_operators {
            let sql = format!("select id {} 42 from table_name;", op);
            let statement = parse_sql(&sql).unwrap();
            let expected_statement = Statement::Select {
                values: None,
                projections: vec![Projection::UnnamedExpr(ExprNode::Binary {
                    left: Box::new(ExprNode::Identifier("id".to_owned())),
                    op,
                    right: Box::new(ExprNode::Number("42".to_owned())),
                })],
                from: Table::TableReference {
                    name: "table_name".to_owned(),
                    alias: None,
                },
                filter: None,
            };

            assert_eq!(statement, expected_statement);
        }
    }

    #[test]
    fn can_parse_empty_tables() {
        let sql = "
            select -3 + 2 * (3 + 5);
        ";

        let statement = parse_sql(sql).unwrap();
        let expected_statement = Statement::Select {
            values: None,
            projections: vec![Projection::UnnamedExpr(ExprNode::Binary {
                left: Box::new(ExprNode::Unary {
                    op: UnaryOperator::Minus,
                    expr: Box::new(ExprNode::Number("3".to_owned())),
                }),
                op: BinaryOperator::Plus,
                right: Box::new(ExprNode::Binary {
                    left: Box::new(ExprNode::Number("2".to_owned())),
                    op: BinaryOperator::Multiply,
                    right: Box::new(ExprNode::Grouping(Box::new(ExprNode::Binary {
                        left: Box::new(ExprNode::Number("3".to_owned())),
                        op: BinaryOperator::Plus,
                        right: Box::new(ExprNode::Number("5".to_owned())),
                    }))),
                }),
            })],
            from: Table::EmptyTable,
            filter: None,
        };

        assert_eq!(statement, expected_statement);
    }

    #[test]
    fn can_parse_values() {
        let sql = "
            values (1, 'foo', true), (2, 'bar', false)
        ";

        let statement = parse_sql(sql).unwrap();
        let expected_statement = Statement::Select {
            values: Some(vec![
                vec![
                    ExprNode::Number("1".to_owned()),
                    ExprNode::String("foo".to_owned()),
                    ExprNode::Boolean(true),
                ],
                vec![
                    ExprNode::Number("2".to_owned()),
                    ExprNode::String("bar".to_owned()),
                    ExprNode::Boolean(false),
                ],
            ]),
            projections: vec![],
            from: Table::EmptyTable,
            filter: None,
        };

        assert_eq!(statement, expected_statement);
    }

    #[test]
    fn can_parse_insert_values_into_table() {
        let sql = "
            insert into table_name values (1, 'foo', true), (2, 'bar', false)
        ";

        let statement = parse_sql(sql).unwrap();
        let expected_statement = Statement::Insert {
            into: Table::TableReference {
                name: "table_name".to_owned(),
                alias: None,
            },
            select: Box::new(Statement::Select {
                values: Some(vec![
                    vec![
                        ExprNode::Number("1".to_owned()),
                        ExprNode::String("foo".to_owned()),
                        ExprNode::Boolean(true),
                    ],
                    vec![
                        ExprNode::Number("2".to_owned()),
                        ExprNode::String("bar".to_owned()),
                        ExprNode::Boolean(false),
                    ],
                ]),
                projections: vec![],
                from: Table::EmptyTable,
                filter: None,
            }),
        };

        assert_eq!(statement, expected_statement);
    }

    #[test]
    fn can_parse_insert_select_into_table() {
        let sql = "
            insert into new_table select * from old_table
        ";

        let statement = parse_sql(sql).unwrap();
        let expected_statement = Statement::Insert {
            into: Table::TableReference {
                name: "new_table".to_owned(),
                alias: None,
            },
            select: Box::new(Statement::Select {
                values: None,
                projections: vec![Projection::Wildcard],
                from: Table::TableReference {
                    name: "old_table".to_owned(),
                    alias: None,
                },
                filter: None,
            }),
        };

        assert_eq!(statement, expected_statement);
    }
}
