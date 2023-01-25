use std::fs::{DirEntry, File, OpenOptions};
use std::os::unix::prelude::FileExt;
use std::path::PathBuf;
use std::{collections::HashMap, ffi::OsStr};

use anyhow::{Context, Error, Result};

use crate::common::{PageNo, TableId, PAGE_SIZE};

/// FileManager takes care of reading and writing pages of tables.
/// It assumes that all tables are stored inside a single directory, the data directory,
/// where each table is represented as a single file, with the table id used as the filename.
pub struct FileManager {
    data_directory: PathBuf,
    table_id_to_file: HashMap<TableId, File>,
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
fn read_table(entry: DirEntry, table_id: TableId) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(entry.path())
        .with_context(|| format!("Could not read data for table {}", table_id))?;

    let size = file
        .metadata()
        .with_context(|| format!("Could not read size of table {}", table_id))?
        .len();
    if size % PAGE_SIZE as u64 != 0 {
        return Err(Error::msg(format!(
            "Boundary check for table {} failed. {} is not divisable by page size {}",
            table_id, size, PAGE_SIZE
        )));
    }

    Ok(file)
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

    /// Returns the File object of a table.
    fn get_file(&self, table_id: TableId) -> Result<&File> {
        self.table_id_to_file
            .get(&table_id)
            .ok_or_else(|| Error::msg(format!("No data file for table with id {}", table_id)))
    }

    /// Returns the highest page number of a table or None, if the table is empty.
    pub fn get_last_page_no(&self, table_id: TableId) -> Result<Option<PageNo>> {
        let file = self.get_file(table_id)?;
        let filesize = file
            .metadata()
            .with_context(|| format!("Could not read file size for table {}", table_id))?
            .len();

        if filesize == 0 {
            Ok(None)
        } else {
            Ok(Some(((filesize / PAGE_SIZE as u64) - 1) as PageNo))
        }
    }

    /// Creates a new table. Returns an error if the table already exists.
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
        self.table_id_to_file.insert(table_id, file);
        Ok(())
    }

    /// Reads the specified page of a table into the buffer.
    pub fn read_page(&self, table_id: TableId, page_id: PageNo, buffer: &mut [u8]) -> Result<()> {
        let file = self.get_file(table_id)?;
        let filesize = file
            .metadata()
            .with_context(|| format!("Could not read file size for table {}", table_id))?
            .len();
        let offset = page_id as u64 * PAGE_SIZE as u64;
        if offset >= filesize {
            return Err(Error::msg(format!(
                "Attempted to read page {} at offset {} for table {}. But table is of size {}",
                page_id, offset, table_id, filesize
            )));
        }
        file.read_exact_at(buffer, offset)
            .with_context(|| format!("Could not read page {} for table {}", page_id, table_id))?;

        Ok(())
    }

    /// Writes data to a page of a table.
    pub fn write_page(&self, table_id: TableId, page_id: PageNo, buffer: &[u8]) -> Result<()> {
        let file = self.get_file(table_id)?;
        let offset = page_id as u64 * PAGE_SIZE as u64;
        file.write_all_at(buffer, offset).with_context(|| {
            format!(
                "Failed to write data to page {} for table {}",
                page_id, table_id
            )
        })?;
        file.sync_all().with_context(|| {
            format!(
                "Failed to sync data when writing page {} for table {}",
                page_id, table_id
            )
        })?;

        Ok(())
    }
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
        file_manager.create_table(1)?;

        assert_eq!(file_manager.get_last_page_no(1)?, None);

        let write_buffer = [1u8; PAGE_SIZE];
        file_manager.write_page(1, 1, &write_buffer)?;
        assert_eq!(file_manager.get_last_page_no(1)?, Some(1));

        let mut read_buffer = [0u8; PAGE_SIZE];
        file_manager.read_page(1, 1, &mut read_buffer)?;

        assert_eq!(read_buffer, write_buffer);

        Ok(())
    }
}
