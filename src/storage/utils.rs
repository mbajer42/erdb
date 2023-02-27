use super::{Slot, TupleId};
use crate::common::PAGE_SIZE;

pub type TupleOffset = u16;
pub type TupleSize = u16;

pub type TupleSlot = (TupleOffset, TupleSize);

const U8_SIZE: usize = std::mem::size_of::<u8>();
const U16_SIZE: usize = std::mem::size_of::<u16>();
const U32_SIZE: usize = std::mem::size_of::<u32>();
pub(in crate::storage) const TUPLE_SLOT_SIZE: u16 = std::mem::size_of::<TupleSlot>() as u16;

pub(in crate::storage) struct Serializer<'a> {
    buffer: &'a mut [u8],
    pos: usize,
}

impl<'a> Serializer<'a> {
    pub fn new(buffer: &'a mut [u8]) -> Self {
        Self { buffer, pos: 0 }
    }

    pub fn serialize_u8(&mut self, val: u8) {
        self.buffer[self.pos..self.pos + U8_SIZE].copy_from_slice(val.to_be_bytes().as_slice());
        self.pos += U8_SIZE;
    }

    pub fn serialize_u16(&mut self, val: u16) {
        self.buffer[self.pos..self.pos + U16_SIZE].copy_from_slice(val.to_be_bytes().as_slice());
        self.pos += U16_SIZE;
    }

    pub fn serialize_u32(&mut self, val: u32) {
        self.buffer[self.pos..self.pos + U32_SIZE].copy_from_slice(val.to_be_bytes().as_slice());
        self.pos += U32_SIZE;
    }

    pub fn serialize_tuple_slot(&mut self, tuple_slot: TupleSlot) {
        let (offset, size) = tuple_slot;
        self.serialize_u16(offset);
        self.serialize_u16(size);
    }

    pub fn serialize_tuple_id(&mut self, tuple_id: TupleId) {
        let (page_no, slot) = tuple_id;
        self.serialize_u32(page_no);
        self.serialize_u8(slot);
    }

    pub fn copy_bytes(&mut self, bytes: &[u8]) {
        for byte in bytes {
            self.buffer[self.pos] = *byte;
            self.pos += 1;
        }
    }

    pub fn end(self) -> usize {
        self.pos
    }
}

pub(in crate::storage) struct Deserializer<'a> {
    buffer: &'a [u8],
    pos: usize,
}

impl<'a> Deserializer<'a> {
    pub fn new(buffer: &'a [u8]) -> Self {
        Self { buffer, pos: 0 }
    }

    pub fn deserialize_u8(&mut self) -> u8 {
        let val = u8::from_be_bytes(
            self.buffer[self.pos..self.pos + U8_SIZE]
                .try_into()
                .unwrap(),
        );
        self.pos += U8_SIZE;
        val
    }

    pub fn deserialize_u16(&mut self) -> u16 {
        let val = u16::from_be_bytes(
            self.buffer[self.pos..self.pos + U16_SIZE]
                .try_into()
                .unwrap(),
        );
        self.pos += U16_SIZE;
        val
    }

    pub fn deserialize_u32(&mut self) -> u32 {
        let val = u32::from_be_bytes(
            self.buffer[self.pos..self.pos + U32_SIZE]
                .try_into()
                .unwrap(),
        );
        self.pos += U32_SIZE;
        val
    }

    pub fn deserialize_tuple_id(&mut self) -> TupleId {
        let page_no = self.deserialize_u32();
        let slot = self.deserialize_u8();
        (page_no, slot)
    }

    pub fn copy_bytes(&mut self, dest: &mut [u8], count: usize) {
        for byte in dest.iter_mut().take(count) {
            *byte = self.buffer[self.pos];
            self.pos += 1;
        }
    }
}

#[derive(Debug)]
pub struct PageHeader {
    free_space_start: u16,
    free_space_end: u16,
}

impl PageHeader {
    pub const SIZE: u16 = 4;

    pub fn empty() -> Self {
        Self {
            free_space_start: Self::SIZE,
            free_space_end: PAGE_SIZE,
        }
    }

    pub fn parse(bytes: &[u8]) -> Self {
        let mut deserializer = Deserializer::new(bytes);
        let free_space_start = deserializer.deserialize_u16();
        let free_space_end = deserializer.deserialize_u16();

        Self {
            free_space_start,
            free_space_end,
        }
    }

    pub fn free_space(&self) -> u16 {
        self.free_space_end - self.free_space_start
    }

    pub fn slots(&self) -> u8 {
        ((self.free_space_start - Self::SIZE) / TUPLE_SLOT_SIZE) as u8
    }

    /// Returns the start offset and its size of a tuple stored at tuple_slot
    pub fn tuple_slot(bytes: &[u8], tuple_slot: Slot) -> TupleSlot {
        let slot_offset = (Self::SIZE + (tuple_slot as u16) * TUPLE_SLOT_SIZE) as usize;
        let mut deserializer = Deserializer::new(&bytes[slot_offset..]);
        let tuple_offset = deserializer.deserialize_u16();
        let tuple_size = deserializer.deserialize_u16();
        (tuple_offset, tuple_size)
    }

    /// Adds a new tuple slot to the page header.
    /// Returns the slot number and the start offset of the tuple on this page
    pub fn add_tuple_slot(&mut self, buffer: &mut [u8], tuple_size: u16) -> (Slot, u16) {
        self.free_space_end -= tuple_size;
        let slot: TupleSlot = (self.free_space_end, tuple_size);
        let mut serializer = Serializer::new(&mut buffer[self.free_space_start as usize..]);
        serializer.serialize_tuple_slot(slot);
        self.free_space_start += serializer.end() as u16;
        (self.slots() - 1, self.free_space_end)
    }

    /// serializes this PageHeader to its bytes so that it can be persisted to disk.
    /// Returns how many bytes were written to the buffer
    pub fn serialize(self, buffer: &mut [u8]) -> usize {
        let mut serializer = Serializer::new(buffer);
        serializer.serialize_u16(self.free_space_start);
        serializer.serialize_u16(self.free_space_end);
        serializer.end()
    }
}
