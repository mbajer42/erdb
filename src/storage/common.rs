use crate::common::PAGE_SIZE;

pub(in crate::storage) trait Serialize {
    const U16_SIZE: usize = std::mem::size_of::<u16>();

    fn serialize(&self, buffer: &mut [u8]) -> usize;
    fn serialize_u16(&self, buffer: &mut [u8], val: u16) -> usize {
        buffer[..Self::U16_SIZE].copy_from_slice(val.to_be_bytes().as_slice());
        Self::U16_SIZE
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
        let mut offset = 0;
        let free_space_start = u16::from_be_bytes(bytes[offset..offset + 2].try_into().unwrap());
        offset += 2;
        let free_space_end = u16::from_be_bytes(bytes[offset..offset + 2].try_into().unwrap());

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

    pub fn tuple_slot(bytes: &[u8], tuple_slot: u8) -> TupleSlot {
        let mut slot_offset = (Self::SIZE + (tuple_slot as u16) * TUPLE_SLOT_SIZE) as usize;
        let tuple_offset =
            u16::from_be_bytes(bytes[slot_offset..slot_offset + 2].try_into().unwrap());
        slot_offset += 2;
        let tuple_size =
            u16::from_be_bytes(bytes[slot_offset..slot_offset + 2].try_into().unwrap());
        (tuple_offset, tuple_size)
    }

    pub fn add_tuple_slot(&mut self, buffer: &mut [u8], tuple_size: u16) -> u16 {
        self.free_space_end -= tuple_size;
        let slot: TupleSlot = (self.free_space_end, tuple_size);
        self.free_space_start +=
            slot.serialize(&mut buffer[self.free_space_start as usize..]) as u16;
        self.free_space_end
    }
}

impl Serialize for PageHeader {
    fn serialize(&self, buffer: &mut [u8]) -> usize {
        let mut offset = self.serialize_u16(buffer, self.free_space_start);
        offset += self.serialize_u16(&mut buffer[offset..], self.free_space_end);
        offset
    }
}

pub type TupleOffset = u16;
pub type TupleSize = u16;

pub type TupleSlot = (TupleOffset, TupleSize);

impl Serialize for TupleSlot {
    fn serialize(&self, buffer: &mut [u8]) -> usize {
        let mut offset = self.serialize_u16(buffer, self.0);
        offset += self.serialize_u16(&mut buffer[offset..], self.1);
        offset
    }
}

pub(in crate::storage) const TUPLE_SLOT_SIZE: u16 = std::mem::size_of::<TupleSlot>() as u16;
