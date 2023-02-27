use crate::common::PageNo;

pub mod file_manager;
pub mod heap;
pub(in crate::storage) mod utils;

pub type Slot = u8;

/// A TupleId identifies a tuple within a table.
/// It consists of the page number, where the tuple is stored,
/// and a slot, where to find the tuple
pub type TupleId = (PageNo, Slot);
