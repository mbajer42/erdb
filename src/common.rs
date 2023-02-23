pub type TableId = u16;
pub type PageNo = u32;
pub type PageId = (TableId, PageNo);

pub const INVALID_TABLE_ID: TableId = 0;
pub const INVALID_PAGE_NO: PageNo = 0;
pub const INVALID_PAGE_ID: PageId = (INVALID_TABLE_ID, INVALID_PAGE_NO);
pub const PAGE_SIZE: u16 = 8192;

pub const MAX_COLUMNS: u8 = u8::MAX;

pub const CATALOG_TABLES_TABLE_ID: TableId = 1;
pub const CATALOG_COLUMNS_TABLE_ID: TableId = 2;
pub const TRANSACTION_LOG_TABLE_ID: TableId = 3;
pub const USER_DATA_TABLE_ID_START: TableId = 10;
