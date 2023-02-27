use self::value::Value;
use crate::common::INVALID_PAGE_NO;
use crate::storage::TupleId;

pub mod value;

const INVALID_TUPLE_ID: TupleId = (INVALID_PAGE_NO, 0);

#[derive(Debug, PartialEq)]
pub struct Tuple {
    /// the physical location of a tuple in a table (if exists)
    pub tuple_id: TupleId,
    pub values: Vec<Value>,
    has_null: bool,
}

impl Tuple {
    pub fn new(values: Vec<Value>) -> Self {
        let has_null = values.iter().any(|val| val.is_null());
        Self {
            tuple_id: INVALID_TUPLE_ID,
            values,
            has_null,
        }
    }

    pub fn has_null(&self) -> bool {
        self.has_null
    }

    pub fn values(&self) -> &[Value] {
        &self.values
    }

    pub fn as_str(&self, col_idx: usize) -> &str {
        self.values[col_idx].as_str()
    }

    pub fn as_i32(&self, col_idx: usize) -> i32 {
        self.values[col_idx].as_i32()
    }

    pub fn as_bool(&self, col_idx: usize) -> bool {
        self.values[col_idx].as_bool()
    }
}
