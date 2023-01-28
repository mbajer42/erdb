pub type TableId = u16;
pub type PageNo = u32;
pub type PageId = (TableId, PageNo);

pub const INVALID_PAGE_ID: PageId = (0, 0);
pub const PAGE_SIZE: u16 = 8192;

pub const CATALOG_TABLES_TABLE_ID: TableId = 10000;
pub const CATALOG_COLUMNS_TABLE_ID: TableId = 10001;
