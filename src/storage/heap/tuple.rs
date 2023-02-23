use super::header::HeapTupleHeader;
use crate::catalog::schema::Schema;
use crate::common::PAGE_SIZE;
use crate::concurrency::{TransactionId};
use crate::storage::common::{PageHeader, TUPLE_SLOT_SIZE};
use crate::tuple::value::Value;
use crate::tuple::Tuple;

// The maximum allowed size of a tuple and its header.
// There has to be enough space so that the PageHeader and a TupleSlot will fit.
pub const MAX_TUPLE_SIZE: u16 = PAGE_SIZE - PageHeader::SIZE - TUPLE_SLOT_SIZE;

pub fn parse_heap_tuple_header(bytes: &[u8], schema: &Schema) -> HeapTupleHeader {
    HeapTupleHeader::from_bytes(bytes, schema.columns().len() as u8)
}

pub fn parse_heap_tuple(bytes: &[u8], header: &HeapTupleHeader, schema: &Schema) -> Tuple {
    let tuple_has_null = header.has_null();

    let mut offset = header.user_data_start();
    let mut values = Vec::with_capacity(schema.columns().len());
    for column in schema.columns() {
        let is_null = tuple_has_null && header.is_null(column.column_offset());
        let value = Value::parse_value(&bytes[offset..], column, is_null);
        offset += value.size();
        values.push(value);
    }

    Tuple::new(values)
}

/// Calculates how many bytes a serialized tuple, including its header would occupy
pub fn required_free_space(tuple: &Tuple) -> u16 {
    let header_size = HeapTupleHeader::required_free_space(tuple);
    let data_size: usize = tuple.values().iter().map(|val| val.size()).sum();

    (header_size + data_size) as u16
}

pub fn serialize_heap_tuple(buffer: &mut [u8], tuple: &Tuple, insert_tid: TransactionId) {
    let header = HeapTupleHeader::new_tuple(tuple, insert_tid);
    let mut user_data_next_value = header.user_data_start();
    for (_column, value) in tuple.values().iter().enumerate() {
        if !value.is_null() {
            value.serialize_value(&mut buffer[user_data_next_value..]);
            user_data_next_value += value.size();
        }
    }
    header.serialize(buffer);
}

#[cfg(test)]
mod tests {

    use lazy_static::lazy_static;

    use super::{parse_heap_tuple, parse_heap_tuple_header, serialize_heap_tuple};
    use crate::catalog::schema::{ColumnDefinition, Schema, TypeId};
    use crate::tuple::value::Value;
    use crate::tuple::Tuple;

    lazy_static! {
        static ref TEST_SCHEMA: Schema = Schema::new(vec![
            ColumnDefinition::new(TypeId::Integer, "non_null_integer".to_owned(), 0, true),
            ColumnDefinition::new(TypeId::Text, "non_null_text".to_owned(), 1, true),
            ColumnDefinition::new(TypeId::Boolean, "non_null_boolean".to_owned(), 2, true),
            ColumnDefinition::new(TypeId::Integer, "nullable_integer".to_owned(), 3, true),
        ]);
    }

    #[test]
    fn can_serialize_tuple() {
        let mut buffer = [0u8; 128];

        let values = vec![
            Value::Integer(42),
            Value::String("the answer".to_owned()),
            Value::Boolean(true),
            Value::Null,
        ];
        let tuple = Tuple::new(values);
        serialize_heap_tuple(&mut buffer, &tuple, 0);

        let header = parse_heap_tuple_header(&buffer, &TEST_SCHEMA);
        let parsed_tuple = parse_heap_tuple(&buffer, &header, &TEST_SCHEMA);

        for (v1, v2) in tuple.values().iter().zip(parsed_tuple.values().iter()) {
            assert_eq!(v1, v2);
        }
    }
}
