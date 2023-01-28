pub mod file_manager;
pub mod heap;

pub struct PageHeader {
    free_space_start: u16,
    free_space_end: u16,
}

pub type ItemOffset = u16;
pub type ItemSize = u16;
pub type ItemPointer = (ItemOffset, ItemSize);
