use self::{schema::ColumnDefinition, value::Value};

pub mod schema;
pub mod value;

pub struct Tuple<'a> {
    values: Vec<Value>,
    columns: &'a [ColumnDefinition],
    has_null: bool,
}

impl<'a> Tuple<'a> {
    pub fn new(values: Vec<Value>, columns: &'a [ColumnDefinition]) -> Self {
        debug_assert!(
            values.len() == columns.len(),
            "Expected values and columns to be of same length"
        );

        let has_null = values.iter().any(|val| val.is_null());
        Self {
            values,
            columns,
            has_null,
        }
    }

    pub fn has_null(&self) -> bool {
        self.has_null
    }

    pub fn values(&self) -> &[Value] {
        &self.values
    }

    pub fn columns(&self) -> &[ColumnDefinition] {
        self.columns
    }
}
