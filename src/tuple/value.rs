use super::schema::{ColumnDefinition, TypeId};

#[derive(PartialEq, Debug)]
pub enum Value {
    Boolean(bool),
    Integer(i32),
    String(String),
    Null,
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
}

#[cfg(test)]
mod tests {
    use super::Value;
    use crate::tuple::schema::{ColumnDefinition, TypeId};

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
