pub type TableId = u16;
pub type PageNo = u32;
pub type PageId = (TableId, PageNo);
pub const INVALID_PAGE_ID: PageId = (0, 0);
pub const PAGE_SIZE: usize = 8192;
