use std::fmt::Display;

#[derive(Debug, PartialEq)]
pub enum DataType {
    Integer,
    Text,
    Boolean,
}

#[derive(Debug, PartialEq)]
pub struct ColumnDefinition {
    pub name: String,
    pub offset: u8,
    pub data_type: DataType,
    pub not_null: bool,
}

#[derive(Debug, PartialEq)]
pub enum UnaryOperator {
    Plus,
    Minus,
}

impl Display for UnaryOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plus => write!(f, "+"),
            Self::Minus => write!(f, "-"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum BinaryOperator {
    Plus,
    Minus,
    Multiply,
    Divide,
    Modulo,
    Less,
    LessEq,
    Eq,
    GreaterEq,
    Greater,
    NotEq,
    And,
    Or,
}

impl Display for BinaryOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plus => write!(f, "+"),
            Self::Minus => write!(f, "-"),
            Self::Multiply => write!(f, "*"),
            Self::Divide => write!(f, "/"),
            Self::Modulo => write!(f, "%"),
            Self::Less => write!(f, "<"),
            Self::LessEq => write!(f, "<="),
            Self::Eq => write!(f, "="),
            Self::GreaterEq => write!(f, ">="),
            Self::Greater => write!(f, ">"),
            Self::NotEq => write!(f, "<>"),
            Self::And => write!(f, "AND"),
            Self::Or => write!(f, "OR"),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Expr {
    Identifier(String),
    Number(String),
    String(String),
    Boolean(bool),
    // an expression in parenthesis, e.g. (1+1)
    Grouping(Box<Expr>),
    Binary {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    Unary {
        op: UnaryOperator,
        expr: Box<Expr>,
    },
    IsNull(Box<Expr>),
    IsNotNull(Box<Expr>),
    Null,
}

impl Display for Expr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Identifier(id) => write!(f, "{}", id),
            Self::Number(num) => write!(f, "{}", num),
            Self::String(s) => write!(f, "'{}'", s),
            Self::Boolean(b) => write!(f, "{}", b),
            Self::Grouping(expr) => write!(f, "({})", expr),
            Expr::Binary { left, op, right } => write!(f, "{} {} {}", left, op, right),
            Expr::Unary { op, expr } => write!(f, "{}{}", op, expr),
            Expr::IsNull(expr) => write!(f, "{} IS NULL", expr),
            Expr::IsNotNull(expr) => write!(f, "{} IS NOT NULL", expr),
            Expr::Null => write!(f, "NULL"),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Table {
    TableReference { name: String, alias: Option<String> },
    EmptyTable,
}

#[derive(Debug, PartialEq)]
pub enum Projection {
    UnnamedExpr(Expr),
    NamedExpr { expr: Expr, alias: String },
    Wildcard,
}

#[derive(Debug, PartialEq)]
pub enum Statement {
    CreateTable {
        name: String,
        columns: Vec<ColumnDefinition>,
    },
    Select {
        values: Option<Vec<Vec<Expr>>>,
        projections: Vec<Projection>,
        from: Table,
        filter: Option<Expr>,
    },
    Insert {
        into: Table,
        select: Box<Statement>,
    },
}
