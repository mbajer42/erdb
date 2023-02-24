use crate::common::MAX_COLUMNS;
use crate::concurrency::{CommandId, TransactionId};
use crate::storage::common::{Deserializer, Serializer};
use crate::tuple::value::Value;
use crate::tuple::Tuple;

const MAX_NULL_BITS_SIZE: usize = (MAX_COLUMNS / 8) as usize;

pub struct HeapTupleHeader {
    /// the id of the transaction which inserted this tuple
    insert_tid: TransactionId,
    /// the id of the transaction which deleted/updated this tuple.
    /// Set to 0 if it has not been deleted/updated
    delete_tid: TransactionId,
    /// how many commands were run before this tuple was created by a transaction
    /// a transaction can only see tuples from previous commands, not from current ones
    command_id: CommandId,
    flags: u8,
    user_data_start: u8,
    /// a bitmap, where a bit is set if the value is NULL
    /// is only present if the tuple has any NULL values
    null_bitmap: [u8; MAX_NULL_BITS_SIZE],
    // below fields are not serialized
    column_count: u8,
}

const HAS_NULL_FLAG: u8 = 0x01;

fn has_null(flags: u8) -> bool {
    (flags & HAS_NULL_FLAG) != 0
}

/// Returns the size in bytes of the null bitmap
fn null_bitmap_size(column_count: u8) -> u8 {
    (column_count - 1) / 8 + 1
}

impl HeapTupleHeader {
    // Required bytes when serialized, regardless of tuple
    // i.e. without the null_bits bitmap
    // Currently, it consists of:
    // 1. insert_tid (4 bytes)
    // 2. delete_tid (4 bytes)
    // 3. command_id (1 byte)
    // 4. flags (1 byte)
    // 5. user_data_start (1 byte)
    const CONSTANT_SIZE: usize = 11;

    pub fn from_bytes(bytes: &[u8], column_count: u8) -> Self {
        let mut deserializer = Deserializer::new(bytes);

        let insert_tid = deserializer.deserialize_u32();
        let delete_tid = deserializer.deserialize_u32();
        let command_id = deserializer.deserialize_u8();
        let flags = deserializer.deserialize_u8();
        let user_data_start = deserializer.deserialize_u8();

        let mut null_bitmap = [0u8; MAX_NULL_BITS_SIZE];
        if has_null(flags) {
            deserializer.copy_bytes(&mut null_bitmap, null_bitmap_size(column_count) as usize);
        }

        Self {
            insert_tid,
            delete_tid,
            command_id,
            flags,
            user_data_start,
            null_bitmap,
            column_count,
        }
    }

    pub fn new_tuple(tuple: &Tuple, insert_tid: TransactionId, command_id: u8) -> Self {
        let mut flags = 0;
        let mut null_bitmap = [0u8; MAX_NULL_BITS_SIZE];
        for (column, value) in tuple.values().iter().enumerate() {
            if value == &Value::Null {
                flags |= HAS_NULL_FLAG;
                null_bitmap[column / 8] |= 1 << (column % 8);
            }
        }
        let user_data_start = if has_null(flags) {
            Self::CONSTANT_SIZE as u8 + null_bitmap_size(tuple.values().len() as u8)
        } else {
            Self::CONSTANT_SIZE as u8
        };

        Self {
            insert_tid,
            delete_tid: 0,
            command_id,
            flags,
            user_data_start,
            null_bitmap,
            column_count: tuple.values().len() as u8,
        }
    }

    pub fn insert_tid(&self) -> TransactionId {
        self.insert_tid
    }

    pub fn delete_tid(&self) -> TransactionId {
        self.delete_tid
    }

    pub fn command_id(&self) -> CommandId {
        self.command_id
    }

    /// Serializes the header to a buffer
    pub fn serialize(&self, buffer: &mut [u8]) {
        let mut serializer = Serializer::new(buffer);
        serializer.serialize_u32(self.insert_tid);
        serializer.serialize_u32(self.delete_tid);
        serializer.serialize_u8(self.command_id);
        serializer.serialize_u8(self.flags);
        serializer.serialize_u8(self.user_data_start);

        if self.has_null() {
            serializer.copy_bytes(&self.null_bitmap[..null_bitmap_size(self.column_count) as usize])
        }
    }

    pub fn user_data_start(&self) -> usize {
        self.user_data_start as usize
    }

    /// Returns whether the tuple contains NULL values
    pub fn has_null(&self) -> bool {
        has_null(self.flags)
    }

    /// Returns whether the n_th column of the tuple is null
    pub fn is_null(&self, column: u8) -> bool {
        let byte = self.null_bitmap[(column / 8) as usize];
        let mask = 1 << (column % 8);
        (byte & mask) != 0
    }

    /// Calculates how many bytes a header of a tuple would occupy when serialized
    pub fn required_free_space(tuple: &Tuple) -> usize {
        if tuple.has_null() {
            Self::CONSTANT_SIZE + null_bitmap_size(tuple.values().len() as u8) as usize
        } else {
            Self::CONSTANT_SIZE
        }
    }
}
