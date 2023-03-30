use std::fmt::Display;

use crate::catalog::schema::{ColumnDefinition, TypeId};
use crate::parser::ast::BinaryOperator;

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Boolean(bool),
    Integer(i32),
    String(String),
    Null,
}

fn compare<T: PartialEq + PartialOrd + ?Sized>(left: &T, right: &T, op: BinaryOperator) -> bool {
    match op {
        BinaryOperator::Eq => left == right,
        BinaryOperator::NotEq => left != right,
        BinaryOperator::Less => left < right,
        BinaryOperator::LessEq => left <= right,
        BinaryOperator::Greater => left > right,
        BinaryOperator::GreaterEq => left >= right,
        _ => unreachable!(),
    }
}

impl Value {
    /// parses a value from bytes
    pub fn parse_value(bytes: &[u8], column: &ColumnDefinition, is_null: bool) -> Self {
        if is_null {
            return Value::Null;
        }
        match column.type_id() {
            TypeId::Boolean => {
                let val = bytes[0] == 1;
                Value::Boolean(val)
            }
            TypeId::Integer => {
                let val = i32::from_be_bytes(bytes[..4].try_into().unwrap());
                Value::Integer(val)
            }
            TypeId::Text => {
                let len = bytes[0] as usize;
                let slice = &bytes[1..len + 1];
                let val = std::str::from_utf8(slice).unwrap().to_owned();
                Value::String(val)
            }
            TypeId::Unknown => unreachable!(),
        }
    }

    pub fn serialize_value(&self, buffer: &mut [u8]) {
        match self {
            Value::Boolean(b) => buffer[0] = *b as u8,
            Value::Integer(val) => {
                buffer[..std::mem::size_of::<i32>()].copy_from_slice(val.to_be_bytes().as_slice())
            }
            Value::String(val) => {
                let len = val.as_bytes().len() as u8;
                buffer[0] = len;
                buffer[1..len as usize + 1].copy_from_slice(val.as_bytes())
            }
            Value::Null => (),
        }
    }

    pub fn is_null(&self) -> bool {
        *self == Value::Null
    }

    /// Returns how many bytes a serialized value occupies
    pub fn size(&self) -> usize {
        match self {
            Value::Boolean(_) => std::mem::size_of::<bool>(),
            Value::Integer(_) => std::mem::size_of::<i32>(),
            Value::String(val) => std::mem::size_of::<u8>() + val.as_bytes().len(),
            Value::Null => 0,
        }
    }

    /// Compares itself with another value and assigns the greater of these to itself.
    /// Assumes that the other value is of same type.
    /// Currently only implemented for integer and text
    pub fn cmp_and_set_max(&mut self, other: Value) {
        if other.is_null() {
            return;
        }

        match (self, other) {
            (Value::Integer(val), Value::Integer(other)) => {
                if *val < other {
                    *val = other;
                }
            }
            (Value::String(val), Value::String(other)) => {
                if *val < other {
                    *val = other;
                }
            }
            (_, Value::Null) => (),
            (this @ Value::Null, other) => {
                *this = other;
            }
            _ => unreachable!(),
        }
    }

    pub fn as_str(&self) -> &str {
        match &self {
            Value::String(val) => val,
            _ => unreachable!(),
        }
    }

    pub fn as_i32(&self) -> i32 {
        match self {
            Value::Integer(val) => *val,
            _ => unreachable!(),
        }
    }

    pub fn as_bool(&self) -> bool {
        match self {
            Value::Boolean(val) => *val,
            _ => unreachable!(),
        }
    }

    /// Evaluates the binary expression.
    pub fn evaluate_binary_expression(&self, right: &Self, op: BinaryOperator) -> Value {
        if self == &Value::Null || right == &Value::Null {
            Value::Null
        } else {
            match op {
                BinaryOperator::Plus
                | BinaryOperator::Minus
                | BinaryOperator::Multiply
                | BinaryOperator::Divide
                | BinaryOperator::Modulo => self.evaluate_arithmetic_expression(right, op),
                BinaryOperator::Eq
                | BinaryOperator::NotEq
                | BinaryOperator::Less
                | BinaryOperator::LessEq
                | BinaryOperator::Greater
                | BinaryOperator::GreaterEq => self.evaluate_comparison(right, op),
                BinaryOperator::And => Value::Boolean(self.as_bool() && right.as_bool()),
                BinaryOperator::Or => Value::Boolean(self.as_bool() || right.as_bool()),
            }
        }
    }

    fn evaluate_arithmetic_expression(&self, right: &Self, op: BinaryOperator) -> Value {
        match op {
            BinaryOperator::Plus => Value::Integer(self.as_i32() + right.as_i32()),
            BinaryOperator::Minus => Value::Integer(self.as_i32() - right.as_i32()),
            BinaryOperator::Multiply => Value::Integer(self.as_i32() * right.as_i32()),
            BinaryOperator::Divide => Value::Integer(self.as_i32() / right.as_i32()),
            BinaryOperator::Modulo => Value::Integer(self.as_i32() % right.as_i32()),
            _ => unreachable!(),
        }
    }

    fn evaluate_comparison(&self, right: &Self, op: BinaryOperator) -> Value {
        let val = match self {
            Value::Integer(left) => compare(left, &right.as_i32(), op),
            Value::String(left) => compare(left.as_str(), right.as_str(), op),
            Value::Boolean(left) => compare(left, &right.as_bool(), op),
            Value::Null => unreachable!(),
        };

        Value::Boolean(val)
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Boolean(val) => Display::fmt(val, f),
            Value::Integer(val) => Display::fmt(val, f),
            Value::String(val) => Display::fmt(val, f),
            Value::Null => Display::fmt("NULL", f),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Value;
    use crate::catalog::schema::{ColumnDefinition, TypeId};

    fn serialize_parse_test_helper(buffer: &mut [u8], col: ColumnDefinition, value: Value) {
        value.serialize_value(buffer);
        let parsed_value = Value::parse_value(buffer, &col, false);
        assert_eq!(parsed_value, value);
    }

    #[test]
    fn serialize_parse_test() {
        let mut buffer = [0u8; 16];
        let integer_column = ColumnDefinition::new(TypeId::Integer, "".to_owned(), 0, true);
        serialize_parse_test_helper(&mut buffer, integer_column, Value::Integer(42));

        let mut buffer = [0u8; 16];
        let integer_column = ColumnDefinition::new(TypeId::Boolean, "".to_owned(), 0, true);
        serialize_parse_test_helper(&mut buffer, integer_column, Value::Boolean(true));

        let mut buffer = [0u8; 16];
        let integer_column = ColumnDefinition::new(TypeId::Text, "".to_owned(), 0, true);
        serialize_parse_test_helper(
            &mut buffer,
            integer_column,
            Value::String("erdb".to_owned()),
        );
    }
}
