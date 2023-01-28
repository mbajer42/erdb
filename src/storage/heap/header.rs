use std::marker::PhantomData;

use crate::tuple::Tuple;

pub(in crate::storage::heap) struct HeapTupleHeader<'a> {
    flags: u8,
    user_data_start: u8,
    null_bits: *const u8,
    // below fields are helper data and won't be serialized
    // size of the slice that null_bits points to
    null_bits_size: u8,
    // if null_bits points to a mutable slice
    mutable: bool,
    // null bits should only live as long as null_bits pointer is valid
    phantom: PhantomData<&'a [u8]>,
}

const HAS_NULL_FLAG: u8 = 0x01;

fn has_null(flags: u8) -> bool {
    (flags & HAS_NULL_FLAG) != 0
}

impl<'a> HeapTupleHeader<'a> {
    // Required bytes when serialized, regardless of tuple
    // i.e. without the null_bits bitmap
    // Currently, the size is 2, one byte for the flags and one byte for user_data_start
    const CONSTANT_SIZE: usize = 2;

    fn null_bits_size(column_count: u8) -> u8 {
        (column_count - 1) / 8 + 1
    }

    pub fn from_bytes(bytes: &[u8], column_count: u8) -> Self {
        let flags = bytes[0];
        let user_data_start = bytes[1];
        let null_bits = if has_null(flags) {
            bytes[Self::CONSTANT_SIZE..].as_ptr()
        } else {
            std::ptr::null()
        };

        Self {
            flags,
            user_data_start,
            null_bits,
            null_bits_size: Self::null_bits_size(column_count),
            mutable: false,
            phantom: PhantomData,
        }
    }

    pub fn from_tuple(tuple: &Tuple, buffer: &mut [u8]) -> Self {
        let flags = if tuple.has_null() { HAS_NULL_FLAG } else { 0 };
        let user_data_start = if tuple.has_null() {
            Self::CONSTANT_SIZE as u8 + Self::null_bits_size(tuple.values().len() as u8)
        } else {
            Self::CONSTANT_SIZE as u8
        };

        let null_bits_size = if tuple.has_null() {
            Self::null_bits_size(tuple.values().len() as u8)
        } else {
            0
        };

        let null_bits = if tuple.has_null() {
            buffer[Self::CONSTANT_SIZE..].as_ptr()
        } else {
            std::ptr::null()
        };

        Self {
            flags,
            user_data_start,
            null_bits,
            null_bits_size,
            mutable: true,
            phantom: PhantomData,
        }
    }

    /// Serializes the header to a buffer
    pub fn serialize(&self, buffer: &mut [u8]) {
        buffer[0] = self.flags;
        buffer[1] = self.user_data_start;
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
        let null_bits =
            unsafe { std::slice::from_raw_parts(self.null_bits, self.null_bits_size as usize) };
        let byte = null_bits[(column / 8) as usize];
        let mask = 1 << (column % 8);
        (byte & mask) != 0
    }

    /// Marks the n_th column of the tuple as null
    pub fn mark_null(&mut self, column: u8) {
        if !self.mutable {
            return;
        }
        self.flags |= HAS_NULL_FLAG;
        let null_bits = unsafe {
            // SAFETY CHECK:
            // The phantom field in our struct ensures that the self.null_bits pointer is valid
            // self.mutable ensures that we actually point to a mutable slice
            std::slice::from_raw_parts_mut(self.null_bits as *mut u8, self.null_bits_size as usize)
        };
        null_bits[(column / 8) as usize] |= 1 << (column % 8);
    }

    /// Calculates how many bytes a header of a tuple would occupy when serialized
    pub fn required_free_space(tuple: &Tuple) -> usize {
        if tuple.has_null() {
            Self::CONSTANT_SIZE + Self::null_bits_size(tuple.values().len() as u8) as usize
        } else {
            Self::CONSTANT_SIZE
        }
    }
}
