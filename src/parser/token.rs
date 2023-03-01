use std::iter::{Enumerate, Peekable};
use std::str::{Chars, FromStr};

use anyhow::{Error, Result};

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Keyword {
    And,
    As,
    Boolean,
    Commit,
    Committed,
    Create,
    Cross,
    Delete,
    False,
    From,
    Inner,
    Insert,
    Integer,
    Into,
    Is,
    Isolation,
    Join,
    Left,
    Level,
    Not,
    Null,
    On,
    Or,
    Outer,
    Read,
    Repeatable,
    Right,
    Rollback,
    Select,
    Set,
    Start,
    Table,
    Text,
    Transaction,
    True,
    Update,
    Values,
    Where,
}

impl FromStr for Keyword {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let res = match s {
            "and" => Self::And,
            "as" => Self::As,
            "boolean" => Self::Boolean,
            "commit" => Self::Commit,
            "committed" => Self::Committed,
            "create" => Self::Create,
            "cross" => Self::Cross,
            "delete" => Self::Delete,
            "false" => Self::False,
            "from" => Self::From,
            "inner" => Self::Inner,
            "insert" => Self::Insert,
            "integer" => Self::Integer,
            "into" => Self::Into,
            "is" => Self::Is,
            "isolation" => Self::Isolation,
            "join" => Self::Join,
            "left" => Self::Left,
            "level" => Self::Level,
            "not" => Self::Not,
            "null" => Self::Null,
            "on" => Self::On,
            "or" => Self::Or,
            "outer" => Self::Outer,
            "read" => Self::Read,
            "repeatable" => Self::Repeatable,
            "right" => Self::Right,
            "rollback" => Self::Rollback,
            "select" => Self::Select,
            "set" => Self::Set,
            "start" => Self::Start,
            "table" => Self::Table,
            "text" => Self::Text,
            "transaction" => Self::Transaction,
            "true" => Self::True,
            "update" => Self::Update,
            "values" => Self::Values,
            "where" => Self::Where,
            _ => return Err(()),
        };
        Ok(res)
    }
}

#[derive(Debug, PartialEq)]
pub enum Token {
    /// an SQL identifier
    Identifier(String),
    /// a keyword (e.g. CREATE)
    Keyword(Keyword),
    /// a number, like 123
    Number(String),
    /// a quoted string
    QuotedString(String),
    /// Dot '.'
    Dot,
    /// Comma ','
    Comma,
    /// Left parenthesis '('
    LeftParen,
    /// Right parenthesis ')'
    RightParen,
    /// Semicolon ';'
    Semicolon,
    /// star '*'
    Star,
    /// Minus '-'
    Minus,
    /// Plus '+'
    Plus,
    /// Division '/'
    Division,
    /// Modulo operator '%'
    Modulo,
    /// Equal '='
    Eq,
    /// Not equal (either '<>' or '!=')
    NotEq,
    /// Less than '<'
    Less,
    /// Greater than '>'
    Greater,
    /// Less than or equal '<='
    LessEq,
    /// Greater than or equal '>='
    GreaterEq,
    /// Exclamation mark '!'
    Exclamation,
    /// not a token, just end of query
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
                end = *pos + 1;
                self.chars.next();
                continue;
            } else {
                break;
            }
        }
        self.sql[start..end].to_lowercase()
    }

    fn number(&mut self, start: usize) -> String {
        let mut end = start + 1;
        while let Some((pos, ch)) = self.chars.peek() {
            if ('0'..='9').contains(ch) {
                end = *pos + 1;
                self.chars.next();
                continue;
            } else {
                break;
            }
        }

        self.sql[start..end].to_owned()
    }

    fn quoted_string(&mut self, start: usize) -> Result<String> {
        for (pos, ch) in self.chars.by_ref() {
            if ch == '\'' {
                return Ok(self.sql[start..pos].to_owned());
            }
        }

        Err(Error::msg(format!(
            "Unterminated string literal {}",
            &self.sql[start..]
        )))
    }

    fn next_token(&mut self) -> Result<Option<Token>> {
        let token = match self.chars.next() {
            Some((pos, ch)) => match ch {
                ch if ch.is_whitespace() => return self.next_token(),
                '(' => Token::LeftParen,
                ')' => Token::RightParen,
                ';' => Token::Semicolon,
                ',' => Token::Comma,
                '.' => Token::Dot,
                '*' => Token::Star,
                '+' => Token::Plus,
                '-' => Token::Minus,
                '/' => Token::Division,
                '%' => Token::Modulo,
                '!' => match self.chars.peek() {
                    Some((_pos, '=')) => {
                        self.chars.next();
                        Token::NotEq
                    }
                    _ => Token::Exclamation,
                },
                '=' => Token::Eq,
                '<' => match self.chars.peek() {
                    Some((_pos, '>')) => {
                        self.chars.next();
                        Token::NotEq
                    }
                    Some((_pos, '=')) => {
                        self.chars.next();
                        Token::LessEq
                    }
                    _ => Token::Less,
                },
                '>' => match self.chars.peek() {
                    Some((_pos, '=')) => {
                        self.chars.next();
                        Token::GreaterEq
                    }
                    _ => Token::Greater,
                },
                '\'' => Token::QuotedString(self.quoted_string(pos + 1)?),
                'a'..='z' | 'A'..='Z' | '_' => {
                    let word = self.word(pos);
                    if let Ok(keyword) = Keyword::from_str(&word) {
                        Token::Keyword(keyword)
                    } else {
                        Token::Identifier(word)
                    }
                }
                '0'..='9' => Token::Number(self.number(pos)),
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

    #[test]
    fn can_tokenize_values() {
        let sql = "
            values (1, 'foo', true), (2, 'bar', false)
        ";

        let tokens = tokenize(sql).expect("Expected to tokenize without any errors");
        let expected = vec![
            Token::Keyword(Keyword::Values),
            Token::LeftParen,
            Token::Number("1".to_owned()),
            Token::Comma,
            Token::QuotedString("foo".to_owned()),
            Token::Comma,
            Token::Keyword(Keyword::True),
            Token::RightParen,
            Token::Comma,
            Token::LeftParen,
            Token::Number("2".to_owned()),
            Token::Comma,
            Token::QuotedString("bar".to_owned()),
            Token::Comma,
            Token::Keyword(Keyword::False),
            Token::RightParen,
        ];

        assert_eq!(tokens, expected);
    }

    #[test]
    fn can_tokenize_insert_into_table() {
        let sql = "
            insert into table_name values (1, 'foo', true), (2, 'bar', NULL)
        ";

        let tokens = tokenize(sql).expect("Expected to tokenize without any errors");
        let expected = vec![
            Token::Keyword(Keyword::Insert),
            Token::Keyword(Keyword::Into),
            Token::Identifier("table_name".to_owned()),
            Token::Keyword(Keyword::Values),
            Token::LeftParen,
            Token::Number("1".to_owned()),
            Token::Comma,
            Token::QuotedString("foo".to_owned()),
            Token::Comma,
            Token::Keyword(Keyword::True),
            Token::RightParen,
            Token::Comma,
            Token::LeftParen,
            Token::Number("2".to_owned()),
            Token::Comma,
            Token::QuotedString("bar".to_owned()),
            Token::Comma,
            Token::Keyword(Keyword::Null),
            Token::RightParen,
        ];

        assert_eq!(tokens, expected);
    }
}
