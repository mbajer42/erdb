use std::collections::VecDeque;
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
pub enum ExprNode {
    Identifier(String),
    /// an identifier which includes the table name, e.g. SELECT a.id FROM a
    QualifiedIdentifier(String, String),
    Number(String),
    String(String),
    Boolean(bool),
    // an expression in parenthesis, e.g. (1+1)
    Grouping(Box<ExprNode>),
    Binary {
        left: Box<ExprNode>,
        op: BinaryOperator,
        right: Box<ExprNode>,
    },
    Unary {
        op: UnaryOperator,
        expr: Box<ExprNode>,
    },
    IsNull(Box<ExprNode>),
    IsNotNull(Box<ExprNode>),
    Null,
}

impl Display for ExprNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Identifier(id) => write!(f, "{}", id),
            Self::QualifiedIdentifier(table, col) => write!(f, "{}.{}", table, col),
            Self::Number(num) => write!(f, "{}", num),
            Self::String(s) => write!(f, "'{}'", s),
            Self::Boolean(b) => write!(f, "{}", b),
            Self::Grouping(expr) => write!(f, "({})", expr),
            ExprNode::Binary { left, op, right } => write!(f, "{} {} {}", left, op, right),
            ExprNode::Unary { op, expr } => write!(f, "{}{}", op, expr),
            ExprNode::IsNull(expr) => write!(f, "{} IS NULL", expr),
            ExprNode::IsNotNull(expr) => write!(f, "{} IS NOT NULL", expr),
            ExprNode::Null => write!(f, "NULL"),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
}

impl JoinType {
    pub fn is_outer(&self) -> bool {
        match self {
            Self::Inner => false,
            Self::Left | Self::Right => true,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum TableNode {
    TableReference {
        name: String,
        alias: Option<String>,
    },
    CrossJoin {
        left: Box<TableNode>,
        right: Box<TableNode>,
    },
    Join {
        left: Box<TableNode>,
        right: Box<TableNode>,
        join_type: JoinType,
        on: ExprNode,
    },
}

#[derive(Debug, PartialEq)]
pub enum Projection {
    UnnamedExpr(ExprNode),
    NamedExpr { expr: ExprNode, alias: String },
    Wildcard,
    QualifiedWildcard { table: String },
}

#[derive(Debug, PartialEq)]
pub enum Statement {
    CreateTable {
        name: String,
        columns: Vec<ColumnDefinition>,
    },
    Select(SelectStatement),
    Insert {
        into: TableNode,
        select: SelectStatement,
    },
    Delete {
        from: TableNode,
        filter: Option<ExprNode>,
    },
    StartTransaction,
    Commit,
    Rollback,
}

#[derive(Debug, PartialEq)]
pub struct SelectStatement {
    pub values: Option<Vec<Vec<ExprNode>>>,
    pub projections: Vec<Projection>,
    pub from: VecDeque<TableNode>,
    pub filter: Option<ExprNode>,
}
