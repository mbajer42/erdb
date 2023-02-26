pub(in crate::storage) mod common;
pub mod file_manager;
pub mod heap;

pub type TupleOffset = u16;
pub type TupleSize = u16;

pub type TupleSlot = (TupleOffset, TupleSize);
