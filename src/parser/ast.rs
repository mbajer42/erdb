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

#[derive(Debug, PartialEq)]
pub enum BinaryOperator {
    Plus,
    Minus,
    Multiply,
    Divide,
}

impl Display for BinaryOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Plus => write!(f, "+"),
            Self::Minus => write!(f, "-"),
            Self::Multiply => write!(f, "*"),
            Self::Divide => write!(f, "/"),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Expr {
    Identifier(String),
    Number(String),
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
        projections: Vec<Projection>,
        from: Table,
    },
}
