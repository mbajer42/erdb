pub type TableId = u16;
pub type PageNo = u32;
pub type PageId = (TableId, PageNo);

pub const INVALID_TABLE_ID: TableId = 0;
pub const INVALID_PAGE_NO: PageNo = 0;
pub const INVALID_PAGE_ID: PageId = (INVALID_TABLE_ID, INVALID_PAGE_NO);
pub const PAGE_SIZE: u16 = 8192;
