use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{atomic::AtomicBool, RwLock};
use std::sync::{Mutex, MutexGuard, RwLockReadGuard, RwLockWriteGuard};

use crate::common::{PageId, PageNo, TableId, INVALID_PAGE_ID, PAGE_SIZE};
use crate::storage::file_manager::FileManager;

use super::clock_replacer::ClockReplacer;
use super::PoolPos;

use anyhow::Result;

pub struct BufferGuard<'a> {
    buffer_manager: &'a BufferManager,
    buffer: &'a Buffer,
}

impl<'a> BufferGuard<'a> {
    fn new(buffer_manager: &'a BufferManager, buffer: &'a Buffer) -> Self {
        Self {
            buffer_manager,
            buffer,
        }
    }

    pub fn read(&self) -> RwLockReadGuard<[u8]> {
        self.buffer.data().read().unwrap()
    }

    pub fn write(&self) -> RwLockWriteGuard<[u8]> {
        self.buffer.data().write().unwrap()
    }

    pub fn mark_dirty(&self) {
        self.buffer.mark_dirty();
    }
}

impl Drop for BufferGuard<'_> {
    fn drop(&mut self) {
        self.buffer_manager.unpin(self.buffer)
    }
}

struct Buffer {
    pool_pos: PoolPos,
    page_id: RwLock<PageId>,
    dirty: AtomicBool,
    data: RwLock<[u8; PAGE_SIZE as usize]>,
}

impl Buffer {
    fn new(pool_pos: PoolPos) -> Self {
        Self {
            pool_pos,
            page_id: RwLock::new(INVALID_PAGE_ID),
            dirty: AtomicBool::new(false),
            data: RwLock::new([0; PAGE_SIZE as usize]),
        }
    }

    fn page_id(&self) -> PageId {
        *self.page_id.read().unwrap()
    }

    fn change_page(&self, new_page_id: PageId) {
        self.dirty.store(false, Ordering::Relaxed);
        let mut page_id = self.page_id.write().unwrap();
        *page_id = new_page_id;
    }

    fn data(&self) -> &RwLock<[u8]> {
        &self.data
    }

    fn dirty(&self) -> bool {
        self.dirty.load(Ordering::Relaxed)
    }

    fn mark_dirty(&self) {
        self.dirty.store(true, Ordering::Relaxed);
    }
}

pub struct BufferManager {
    pool: Box<[Buffer]>,
    clock_replacer: Mutex<ClockReplacer>,
    page_id_to_pool_pos: Mutex<HashMap<PageId, PoolPos>>,
    file_manager: RwLock<FileManager>,
}

impl BufferManager {
    pub fn new(file_manager: FileManager, pool_size: usize) -> Self {
        let clock_replacer = ClockReplacer::new(pool_size);
        let pool = (0..pool_size).map(Buffer::new).collect();

        Self {
            pool,
            clock_replacer: Mutex::new(clock_replacer),
            page_id_to_pool_pos: Mutex::new(HashMap::new()),
            file_manager: RwLock::new(file_manager),
        }
    }

    pub fn highest_page_no(&self, table_id: TableId) -> Result<PageNo> {
        let file_manager = self.file_manager.read().unwrap();
        file_manager.get_highest_page_no(table_id)
    }

    pub fn create_table(&self, table_id: TableId) -> Result<()> {
        let mut file_manager = self.file_manager.write().unwrap();
        file_manager.create_table(table_id)
    }

    pub fn allocate_new_page(
        &self,
        table_id: TableId,
        initial_data: &[u8],
    ) -> Result<Option<BufferGuard>> {
        let mut page_id_to_pool_pos = self.page_id_to_pool_pos.lock().unwrap();
        let mut clock_replacer = self.clock_replacer.lock().unwrap();

        if let Some(free_pool_pos) = clock_replacer.find_free_buffer() {
            let buffer = &self.pool[free_pool_pos];
            self.remove_page(&mut page_id_to_pool_pos, buffer)?;

            let file_manager = self.file_manager.read().unwrap();
            let page_no = file_manager.allocate_new_page(table_id, initial_data)?;

            let mut data = buffer.data().write().unwrap();
            data[..].copy_from_slice(initial_data);

            let page_id = (table_id, page_no);
            buffer.change_page(page_id);
            page_id_to_pool_pos.insert(page_id, free_pool_pos);
            clock_replacer.pin(free_pool_pos);

            let guard = BufferGuard::new(self, buffer);
            Ok(Some(guard))
        } else {
            Ok(None)
        }
    }

    pub fn fetch(&self, page_id: PageId) -> Result<Option<BufferGuard>> {
        let mut page_id_to_pool_pos = self.page_id_to_pool_pos.lock().unwrap();
        let mut clock_replacer = self.clock_replacer.lock().unwrap();

        if let Some(&pool_pos) = page_id_to_pool_pos.get(&page_id) {
            let buffer = self.pool.get(pool_pos).unwrap();
            clock_replacer.pin(pool_pos);
            let guard = BufferGuard::new(self, buffer);
            return Ok(Some(guard));
        }

        if let Some(free_pool_pos) = clock_replacer.find_free_buffer() {
            let buffer = &self.pool[free_pool_pos];
            self.remove_page(&mut page_id_to_pool_pos, buffer)?;

            let mut data = buffer.data().write().unwrap();
            let file_manager = self.file_manager.read().unwrap();
            file_manager.read_page(page_id.0, page_id.1, &mut data)?;

            buffer.change_page(page_id);
            page_id_to_pool_pos.insert(page_id, free_pool_pos);
            clock_replacer.pin(free_pool_pos);

            let guard = BufferGuard::new(self, buffer);
            Ok(Some(guard))
        } else {
            Ok(None)
        }
    }

    fn unpin(&self, buffer: &Buffer) {
        let pool_pos = buffer.pool_pos;
        let mut clock_replacer = self.clock_replacer.lock().unwrap();
        clock_replacer.unpin(pool_pos);
    }

    fn remove_page(
        &self,
        page_id_to_pool_pos: &mut MutexGuard<HashMap<PageId, PoolPos>>,
        buffer: &Buffer,
    ) -> Result<()> {
        let page_id = buffer.page_id();
        if page_id != INVALID_PAGE_ID {
            page_id_to_pool_pos.remove(&page_id);
            if buffer.dirty() {
                let data = buffer.data().read().unwrap();
                let file_manager = self.file_manager.read().unwrap();
                file_manager.write_page(page_id.0, page_id.1, &data)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use std::ops::Deref;

    use crate::common::PAGE_SIZE;

    use super::{BufferManager, FileManager};

    use anyhow::Result;
    use tempfile::tempdir;

    #[test]
    fn basic_binary_data_test() -> Result<()> {
        let table_id = 42;
        let data_dir = tempdir()?;
        let file_manager = FileManager::new(data_dir.path())?;
        let buffer_manager = BufferManager::new(file_manager, 1);
        buffer_manager.create_table(1)?;

        let page1 = [1u8; PAGE_SIZE as usize];
        let page2 = [2u8; PAGE_SIZE as usize];

        let buffer1 = buffer_manager.allocate_new_page(table_id, &page1)?;
        assert!(
            buffer1.is_some(),
            "A buffer manager with pool size 1 should be able to hold one buffer."
        );
        let buffer1 = buffer1.unwrap();
        assert_eq!(buffer1.read().deref(), &page1);

        let buffer2 = buffer_manager.allocate_new_page(table_id, &page2)?;
        assert!(
            buffer2.is_none(),
            "A buffer manager with pool size 1 should not be able to hold 2 buffers."
        );
        drop(buffer1);
        let buffer2 = buffer_manager.fetch((1, 2))?;
        assert!(
            buffer2.is_some(),
            "A buffer manager with pool size 1 should be able to fetch a new page into a buffer, once all other buffers have been unpinned"
        );
        let buffer2 = buffer2.unwrap();
        assert_eq!(buffer2.read().deref(), &page2);

        // Write something, mark it dirty and unpin it. When reading a new page, this page should be flushed to disk.
        buffer2.write()[0] = 42;
        buffer2.mark_dirty();
        drop(buffer2);

        let buffer1 = buffer_manager.fetch((1, 1))?;
        assert!(buffer1.is_some());
        drop(buffer1);

        let buffer2 = buffer_manager.fetch((1, 2))?;
        assert!(buffer2.is_some());
        let buffer2 = buffer2.unwrap();
        assert_eq!(buffer2.read()[0], 42);

        Ok(())
    }
}
