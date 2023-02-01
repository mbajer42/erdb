use std::iter::Peekable;
use std::vec::IntoIter;

use anyhow::{Error, Result};

use self::ast::{ColumnDef, DataType, Statement};
use self::token::{tokenize, Keyword, Token};

mod ast;
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
                found => self.wrong_keyword("a statement", found)?,
            },
            found => self.wrong_token("a statement", found)?,
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

    fn parse_column_definitions(&mut self) -> Result<Vec<ColumnDef>> {
        self.expect(Token::LeftParen)?;

        let mut columns = vec![];

        loop {
            columns.push(self.parse_column_definition()?);
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
        }

        Ok(columns)
    }

    fn parse_column_definition(&mut self) -> Result<ColumnDef> {
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

        Ok(ColumnDef {
            name: column_name,
            data_type,
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
    use super::ast::{ColumnDef, DataType, Statement};
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
                ColumnDef {
                    name: "id".to_owned(),
                    data_type: DataType::Integer,
                    not_null: true,
                },
                ColumnDef {
                    name: "name".to_owned(),
                    data_type: DataType::Text,
                    not_null: true,
                },
                ColumnDef {
                    name: "active".to_owned(),
                    data_type: DataType::Boolean,
                    not_null: false,
                },
                ColumnDef {
                    name: "email".to_owned(),
                    data_type: DataType::Text,
                    not_null: false,
                },
            ],
        };

        assert_eq!(statement, expected_statement);
    }
}
