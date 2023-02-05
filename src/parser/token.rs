use std::iter::{Enumerate, Peekable};
use std::str::{Chars, FromStr};

use anyhow::{Error, Result};

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Keyword {
    As,
    Boolean,
    Create,
    From,
    Integer,
    Not,
    Null,
    Select,
    Table,
    Text,
}

impl FromStr for Keyword {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let res = match s {
            "as" => Self::As,
            "boolean" => Self::Boolean,
            "create" => Self::Create,
            "from" => Self::From,
            "integer" => Self::Integer,
            "not" => Self::Not,
            "null" => Self::Null,
            "select" => Self::Select,
            "table" => Self::Table,
            "text" => Self::Text,
            _ => return Err(()),
        };
        Ok(res)
    }
}

#[derive(Debug, PartialEq)]
pub enum Token {
    /// an SQL identifier
    Identifier(String),
    /// A keyword (e.g. CREATE)
    Keyword(Keyword),
    // Comma ','
    Comma,
    // Left parenthesis '('
    LeftParen,
    // Right parenthesis ')'
    RightParen,
    // Semicolon ';'
    Semicolon,
    // star '*'
    Star,
    // not a token, just end of query
    End,
}

struct Tokenizer<'a> {
    sql: &'a str,
    chars: Peekable<Enumerate<Chars<'a>>>,
}

impl<'a> Tokenizer<'a> {
    fn new(sql: &'a str) -> Self {
        Self {
            sql,
            chars: sql.chars().enumerate().peekable(),
        }
    }

    fn word(&mut self, start: usize) -> String {
        let mut end = start + 1;
        while let Some((pos, ch)) = self.chars.peek() {
            if ('a'..='z').contains(ch)
                || ('A'..='Z').contains(ch)
                || ('0'..='9').contains(ch)
                || *ch == '_'
            {
                self.chars.next();
                continue;
            } else {
                end = *pos;
                break;
            }
        }
        self.sql[start..end].to_lowercase()
    }

    fn next_token(&mut self) -> Result<Option<Token>> {
        let token = match self.chars.next() {
            Some((pos, ch)) => match ch {
                ch if ch.is_whitespace() => return self.next_token(),
                '(' => Token::LeftParen,
                ')' => Token::RightParen,
                ';' => Token::Semicolon,
                ',' => Token::Comma,
                '*' => Token::Star,
                'a'..='z' | 'A'..='Z' | '_' => {
                    let word = self.word(pos);
                    if let Ok(keyword) = Keyword::from_str(&word) {
                        Token::Keyword(keyword)
                    } else {
                        Token::Identifier(word)
                    }
                }
                ch => return Err(Error::msg(format!("Unexpected character '{ch}'"))),
            },
            None => return Ok(None),
        };

        Ok(Some(token))
    }
}

pub fn tokenize(sql: &str) -> Result<Vec<Token>> {
    let mut tokens = vec![];
    let mut tokenizer = Tokenizer::new(sql);
    while let Some(token) = tokenizer.next_token()? {
        tokens.push(token);
    }
    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::{tokenize, Keyword, Token};

    #[test]
    fn can_tokenize_create_table_statement() {
        let sql = "
            create table tablename (
                id integer not null,
                name text not null,
                email text,
                active boolean,
            )
        ";

        let tokens = tokenize(sql).expect("Expected to tokenize without any errors");
        let expected = vec![
            Token::Keyword(Keyword::Create),
            Token::Keyword(Keyword::Table),
            Token::Identifier("tablename".to_owned()),
            Token::LeftParen,
            Token::Identifier("id".to_owned()),
            Token::Keyword(Keyword::Integer),
            Token::Keyword(Keyword::Not),
            Token::Keyword(Keyword::Null),
            Token::Comma,
            Token::Identifier("name".to_owned()),
            Token::Keyword(Keyword::Text),
            Token::Keyword(Keyword::Not),
            Token::Keyword(Keyword::Null),
            Token::Comma,
            Token::Identifier("email".to_owned()),
            Token::Keyword(Keyword::Text),
            Token::Comma,
            Token::Identifier("active".to_owned()),
            Token::Keyword(Keyword::Boolean),
            Token::Comma,
            Token::RightParen,
        ];

        assert_eq!(tokens, expected);
    }

    #[test]
    fn can_tokenize_wildcard_select() {
        let sql = "
            select * from tablename
        ";

        let tokens = tokenize(sql).expect("Expected to tokenize without any errors");
        let expected = vec![
            Token::Keyword(Keyword::Select),
            Token::Star,
            Token::Keyword(Keyword::From),
            Token::Identifier("tablename".to_owned()),
        ];

        assert_eq!(tokens, expected);
    }

    #[test]
    fn can_tokenize_select_of_columns() {
        let sql = "
            select id, mail as email from tablename
        ";

        let tokens = tokenize(sql).expect("Expected to tokenize without any errors");
        let expected = vec![
            Token::Keyword(Keyword::Select),
            Token::Identifier("id".to_owned()),
            Token::Comma,
            Token::Identifier("mail".to_owned()),
            Token::Keyword(Keyword::As),
            Token::Identifier("email".to_owned()),
            Token::Keyword(Keyword::From),
            Token::Identifier("tablename".to_owned()),
        ];

        assert_eq!(tokens, expected);
    }
}
