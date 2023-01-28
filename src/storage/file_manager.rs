use std::cell::Cell;
use std::cmp::Ordering;
use std::fs::{DirEntry, File, OpenOptions};
use std::os::unix::prelude::FileExt;
use std::path::PathBuf;
use std::{collections::HashMap, ffi::OsStr};

use anyhow::{Context, Error, Result};

use crate::common::{PageNo, TableId, PAGE_SIZE};
use crate::storage::common::Serialize;

use super::common::PageHeader;

/// FileManager takes care of reading and writing pages of tables.
/// It assumes that all tables are stored inside a single directory, the data directory,
/// where each table is represented as a single file, with the table id used as the filename.
pub struct FileManager {
    data_directory: PathBuf,
    table_id_to_file: HashMap<TableId, FileHandle>,
}

impl FileManager {
    /// Creates a new FileManager.
    ///
    /// # Arguments
    /// * `data_directory` - The directory under which all tables are stored
    ///
    pub fn new(data_directory: impl Into<PathBuf>) -> Result<Self> {
        let data_directory = data_directory.into();
        if !data_directory.is_dir() {
            return Err(Error::msg(format!(
                "Could not create file manager. {} is not a directory",
                data_directory.display()
            )));
        }

        let mut table_id_to_file = HashMap::new();
        let content = data_directory.read_dir().with_context(|| {
            format!(
                "Could not read files in data directory {}",
                data_directory.display()
            )
        })?;

        for entry in content {
            let entry = entry.with_context(|| {
                format!(
                    "Could not read entry in data directory {}",
                    data_directory.display()
                )
            })?;
            if entry.file_type()?.is_file() {
                continue;
            }
            let table_id = to_table_id(&entry.file_name());
            if let Some(table_id) = table_id {
                let file = read_table(entry, table_id)?;
                table_id_to_file.insert(table_id, file);
            }
        }

        Ok(Self {
            data_directory,
            table_id_to_file,
        })
    }

    /// Returns the FileHandle object of a table.
    fn get_file(&self, table_id: TableId) -> Result<&FileHandle> {
        self.table_id_to_file
            .get(&table_id)
            .ok_or_else(|| Error::msg(format!("No data file for table with id {}", table_id)))
    }

    /// Returns the highest page number of a table.
    /// Returns an error if the table does not exist
    pub fn get_highest_page_no(&self, table_id: TableId) -> Result<PageNo> {
        let file = self.get_file(table_id)?;
        Ok(file.get_highest_page_no())
    }

    /// Creates a new table and initializes a first page.
    /// Returns an error if the table already exists or if the initila page could not be initialized.
    pub fn create_table(&mut self, table_id: TableId) -> Result<()> {
        if self.table_id_to_file.contains_key(&table_id) {
            return Err(Error::msg(format!(
                "Table with id {} already exists",
                table_id
            )));
        }
        let path = self.data_directory.join(table_id.to_string());
        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .create_new(true)
            .open(path)
            .with_context(|| format!("Failed to create data file for table {}", table_id))?;

        let file = FileHandle::new(table_id, file, 0);
        file.allocate_new_page()?;
        self.table_id_to_file.insert(table_id, file);

        Ok(())
    }

    /// Reads the specified page of a table into the buffer.
    pub fn read_page(&self, table_id: TableId, page_no: PageNo, buffer: &mut [u8]) -> Result<()> {
        let file = self.get_file(table_id)?;
        file.read_page(page_no, buffer)?;

        Ok(())
    }

    /// Writes data to an allocated page of a table. Returns an error if the page hasn't been allocated yet.
    pub fn write_page(&self, table_id: TableId, page_no: PageNo, buffer: &[u8]) -> Result<()> {
        let file = self.get_file(table_id)?;
        let offset = page_no as u64 * PAGE_SIZE as u64;
        if offset >= file.filesize() {
            Err(Error::msg(format!(
                "Attempted to write page {} for table {} before it has been allocated",
                page_no, table_id
            )))
        } else {
            file.write_page_at_offset(offset, buffer)
        }
    }
}

struct FileHandle {
    table_id: TableId,
    file: File,
    filesize: Cell<u64>,
}

impl FileHandle {
    fn new(table_id: TableId, file: File, filesize: u64) -> Self {
        Self {
            table_id,
            file,
            filesize: Cell::new(filesize),
        }
    }

    fn filesize(&self) -> u64 {
        self.filesize.get()
    }

    fn get_highest_page_no(&self) -> PageNo {
        let size = self.filesize.get();
        ((size / PAGE_SIZE as u64) - 1) as PageNo
    }

    /// Allocates a new page
    fn allocate_new_page(&self) -> Result<()> {
        let page_header = PageHeader::empty();
        let mut buffer = [0u8; PAGE_SIZE as usize];
        page_header.serialize(&mut buffer);
        self.write_page_at_offset(self.filesize.get(), &buffer)?;
        let size = self.filesize() + PAGE_SIZE as u64;
        self.filesize.set(size);
        Ok(())
    }

    /// Reads page into buffer.
    /// If the page does not exist yet but is at file boundary, as new page will be allocated,
    /// else, an error is returned.
    fn read_page(&self, page_no: PageNo, buffer: &mut [u8]) -> Result<()> {
        let offset = page_no as u64 * PAGE_SIZE as u64;
        let filesize = self.filesize.get();

        match offset.cmp(&filesize) {
            Ordering::Equal => self.allocate_new_page()?,
            Ordering::Greater => {
                return Err(Error::msg(format!(
                    "Attempted to read page {} at offset {} for table {}. But table is of size {}",
                    page_no, offset, self.table_id, filesize
                )));
            }
            Ordering::Less => (),
        }

        self.file.read_exact_at(buffer, offset).with_context(|| {
            format!(
                "Could not read page {} for table {}",
                page_no, self.table_id
            )
        })?;

        Ok(())
    }

    /// Writes data to the file at the given offset.
    fn write_page_at_offset(&self, offset: u64, buffer: &[u8]) -> Result<()> {
        self.file.write_all_at(buffer, offset).with_context(|| {
            format!(
                "Failed to write data at offset {} for table {}",
                offset, self.table_id
            )
        })?;
        self.file.sync_all().with_context(|| {
            format!(
                "Failed to sync data when writing at offset {} for table {}",
                offset, self.table_id
            )
        })?;

        Ok(())
    }
}

/// Returns the table id if the filename is a valid table id, else none.
fn to_table_id(filename: &OsStr) -> Option<TableId> {
    let filename = filename.to_str()?;

    let mut table_id = 0;
    for c in filename.chars() {
        if let Some(d) = c.to_digit(10) {
            table_id = 10 * table_id + d;
            if table_id > u16::MAX as u32 {
                return None;
            }
        } else {
            return None;
        };
    }

    Some(table_id as u16)
}

/// Opens and returns a File of a table, which can be written and read.
fn read_table(entry: DirEntry, table_id: TableId) -> Result<FileHandle> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(entry.path())
        .with_context(|| format!("Could not read data for table {}", table_id))?;

    let size = file
        .metadata()
        .with_context(|| format!("Could not read size of table {}", table_id))?
        .len();

    if size == 0 {
        return Err(Error::msg(format!("Table {} is empty.", table_id)));
    } else if size % PAGE_SIZE as u64 != 0 {
        return Err(Error::msg(format!(
            "Boundary check for table {} failed. {} is not divisable by page size {}",
            table_id, size, PAGE_SIZE
        )));
    }

    Ok(FileHandle::new(table_id, file, size))
}

#[cfg(test)]
mod tests {

    use super::FileManager;
    use super::PAGE_SIZE;

    use anyhow::Result;
    use tempfile::tempdir;

    #[test]
    fn basic_test() -> Result<()> {
        let data_dir = tempdir()?;
        let mut file_manager = FileManager::new(data_dir.path())?;
        let table_id = 1;
        file_manager.create_table(table_id)?;

        let page = file_manager.get_highest_page_no(table_id)?;
        assert_eq!(page, 0);

        let write_buffer = [1u8; PAGE_SIZE as usize];
        file_manager.write_page(table_id, page, &write_buffer)?;

        let mut read_buffer = [0u8; PAGE_SIZE as usize];
        file_manager.read_page(table_id, page, &mut read_buffer)?;

        assert_eq!(read_buffer, write_buffer);

        Ok(())
    }
}
