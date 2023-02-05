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
pub enum Expr {
    Identifier(String),
}

#[derive(Debug, PartialEq)]
pub enum Table {
    TableReference { name: String, alias: Option<String> },
}

#[derive(Debug, PartialEq)]
pub enum Projection {
    UnnamedExpr(Expr),
    NamedExpr { expression: Expr, alias: String },
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
