use std::collections::HashSet;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::RwLock;

use anyhow::{Context, Error, Result};

use crate::buffer::buffer_manager::{BufferGuard, BufferManager};
use crate::common::{INVALID_PAGE_NO, PAGE_SIZE, TRANSACTION_LOG_TABLE_ID};

pub type TransactionId = u32;
pub const INVALID_TRANSACTION_ID: TransactionId = 0;
pub const BOOTSTRAP_TRANSACTION_ID: TransactionId = 1;

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum TransactionStatus {
    /// This transaction does not exist yet
    Invalid = 0b00,
    InProgress = 0b01,
    Aborted = 0b10,
    Committed = 0b11,
}

impl From<u8> for TransactionStatus {
    fn from(value: u8) -> Self {
        match value {
            0b00 => Self::Invalid,
            0b01 => Self::InProgress,
            0b10 => Self::Aborted,
            0b11 => Self::Committed,
            _ => unreachable!(),
        }
    }
}

pub struct Transaction<'a> {
    /// the transaction id of the current transaction
    tid: TransactionId,
    /// first unassigned transaction id
    tid_max: TransactionId,
    alive_tids: HashSet<TransactionId>,
    manager: &'a TransactionManager<'a>,
}

impl<'a> Transaction<'a> {
    /// Returns its own transaction id
    pub fn tid(&self) -> TransactionId {
        self.tid
    }

    pub fn commit(&self) -> Result<()> {
        self.manager.commit(self.tid)
    }

    pub fn abort(&self) -> Result<()> {
        self.manager.abort(self.tid)
    }

    pub fn is_tuple_visible(
        &self,
        tuple_min_tid: TransactionId,
        tuple_max_tid: TransactionId,
    ) -> Result<bool> {
        if tuple_min_tid >= self.tid_max {
            return Ok(false);
        }

        match self.manager.get_transaction_status(tuple_min_tid)? {
            // invalid or aborted transaction ids are never visible
            TransactionStatus::Invalid | TransactionStatus::Aborted => Ok(false),
            // an in progress transaction id is only visible, if the tuple was inserted by the very same transaction
            TransactionStatus::InProgress => {
                if tuple_min_tid == self.tid {
                    Ok(tuple_max_tid == INVALID_TRANSACTION_ID)
                } else {
                    Ok(false)
                }
            }
            TransactionStatus::Committed => {
                if self.alive_tids.contains(&tuple_min_tid) {
                    // transaction committed, but when this transaction started it was still alive,
                    // hence, not visible
                    Ok(false)
                } else if tuple_max_tid == INVALID_TRANSACTION_ID || tuple_max_tid >= self.tid_max {
                    // 1. if there does not exist a newer version of a tuple (tuple_max_tid == INVALID_TRANSACTION_ID),
                    // then this tuple is visible
                    // 2. if there exists a newer version of a tuple, but regardless of its status,
                    // it won't be visible (as it's outside of the snapshot), so the current tuple is visible
                    Ok(true)
                } else {
                    match self.manager.get_transaction_status(tuple_max_tid)? {
                        // there is a newer version of this tuple, but its' transaction was aborted, so this tuple is visible
                        TransactionStatus::Invalid | TransactionStatus::Aborted => Ok(true),
                        // the newer version of this tuple is still in progress. If the current transaction inserted the newer version,
                        // then this tuple will not be visible (only the newer one will). If any other transaction inserted the
                        // newer version, then the newer version won't be visible
                        TransactionStatus::InProgress => Ok(tuple_max_tid != self.tid),
                        // newer version is committed, but only visible if the transaction is not marked as in progress for
                        // the current transaction
                        TransactionStatus::Committed => {
                            Ok(self.alive_tids.contains(&tuple_max_tid))
                        }
                    }
                }
            }
        }
    }
}

pub struct TransactionManager<'a> {
    buffer_manager: &'a BufferManager,
    next_tid: AtomicU32,
    alive_tids: RwLock<HashSet<TransactionId>>,
}

impl<'a> TransactionManager<'a> {
    pub fn new(buffer_manager: &'a BufferManager, bootstrap: bool) -> Result<Self> {
        let this = Self {
            buffer_manager,
            next_tid: AtomicU32::new(2),
            alive_tids: RwLock::new(HashSet::new()),
        };
        if bootstrap {
            buffer_manager
                .create_table(TRANSACTION_LOG_TABLE_ID)
                .with_context(|| {
                    "Could not create a transaction log during bootstrap".to_string()
                })?;
        } else {
            this.load_transaction_log()?;
        }

        Ok(this)
    }

    pub fn start_transaction(&'a self) -> Result<Transaction<'a>> {
        let tid = self
            .next_tid
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |val| {
                if val == u32::MAX {
                    None
                } else {
                    Some(val + 1)
                }
            })
            .map_err(|_| {
                Error::msg(
                    "Could not start a new transaction. Transaction ID space is already exhausted",
                )
            })?;
        // tid_max will become an invalid transaction id on overflow. This is fine for all visibility checks
        let tid_max = tid.wrapping_add(1);
        let mut alive_tids = self.alive_tids.write().unwrap();
        alive_tids.insert(tid);

        Ok(Transaction {
            tid,
            tid_max,
            alive_tids: alive_tids.clone(),
            manager: self,
        })
    }

    /// Starts a transaction which can be used by other parts to complete bootstrap process.
    /// Should be used only during bootstrap process
    pub fn bootstrap(&'a self) -> Transaction<'a> {
        Transaction {
            tid: BOOTSTRAP_TRANSACTION_ID,
            tid_max: TransactionId::MAX,
            alive_tids: HashSet::new(),
            manager: self,
        }
    }

    /// Should be used only once during server startup.
    fn load_transaction_log(&self) -> Result<()> {
        let highest_page_no = self
            .buffer_manager
            .highest_page_no(TRANSACTION_LOG_TABLE_ID)?;
        if highest_page_no == INVALID_PAGE_NO {
            return Err(Error::msg("Transaction log is empty. Either your data is corrupted or bootstrap process failed."));
        }
        let buffer = self
            .buffer_manager
            .fetch((TRANSACTION_LOG_TABLE_ID, highest_page_no))?;
        if let Some(buffer) = buffer {
            let data = buffer.read();
            let tid_offset = (highest_page_no - 1) * (PAGE_SIZE as u32) * 4;
            let mut highest_tid = tid_offset;
            for (offset, byte) in data.iter().enumerate() {
                let offset = offset as u32;
                // 4 transaction statuses fit into a single byte,
                // the status with the lower transaction status is stored in the lower bits of the byte
                highest_tid = match *byte {
                    b if b >= 64 => offset + tid_offset + 3,
                    b if b >= 16 => offset + tid_offset + 2,
                    b if b >= 4 => offset + tid_offset + 1,
                    b if b > 0 => offset + tid_offset,
                    _ => highest_tid,
                };
            }
            self.next_tid.store(highest_tid + 1, Ordering::Relaxed);
            Ok(())
        } else {
            Err(Error::msg("Could not read transaction log during loading process. Buffer Manager already full?"))
        }
    }

    /// Finds the page for which the status of this transaction id is stored
    fn get_page(&self, tid: TransactionId) -> Result<Option<BufferGuard>> {
        let array_pos = tid / 4;
        let page = array_pos / (PAGE_SIZE as u32) + 1;
        let highest_log_table_no = self
            .buffer_manager
            .highest_page_no(TRANSACTION_LOG_TABLE_ID)?;
        if highest_log_table_no < page {
            self.buffer_manager
                .allocate_new_page(TRANSACTION_LOG_TABLE_ID, &[0u8; PAGE_SIZE as usize])
        } else {
            self.buffer_manager.fetch((TRANSACTION_LOG_TABLE_ID, page))
        }
    }

    fn get_transaction_status(&self, tid: TransactionId) -> Result<TransactionStatus> {
        let alive_tids = self.alive_tids.read().unwrap();
        if alive_tids.contains(&tid) {
            return Ok(TransactionStatus::InProgress);
        }
        drop(alive_tids);

        if tid >= self.next_tid.load(Ordering::Relaxed) {
            return Ok(TransactionStatus::Invalid);
        }

        if let Some(buffer) = self.get_page(tid)? {
            let data = buffer.read();
            let page_pos = ((tid / 4) % PAGE_SIZE as u32) as usize;
            let byte_pos = (tid % 4) as usize * 2;
            let byte = data[page_pos];
            let status = (byte >> byte_pos) & 0b11;
            Ok(status.into())
        } else {
            Err(Error::msg(format!("Could not check transaction status for tid {}. All buffers in buffer manager are currently pinned", tid)))
        }
    }

    fn commit(&self, tid: TransactionId) -> Result<()> {
        self.change_transaction_status(tid, TransactionStatus::Committed)
            .with_context(|| format!("Failed to commit transaction with tid {}", tid))
    }

    fn abort(&self, tid: TransactionId) -> Result<()> {
        self.change_transaction_status(tid, TransactionStatus::Aborted)
            .with_context(|| format!("Failed to abort transaction with tid {}", tid))
    }

    fn change_transaction_status(
        &self,
        tid: TransactionId,
        new_status: TransactionStatus,
    ) -> Result<()> {
        let mut alive_tids = self.alive_tids.write().unwrap();
        alive_tids.remove(&tid);
        drop(alive_tids);

        if let Some(buffer) = self.get_page(tid)? {
            let mut data = buffer.write();
            let page_pos = ((tid / 4) % PAGE_SIZE as u32) as usize;
            let byte_pos = (tid % 4) as usize * 2;
            data[page_pos] |= (new_status as u8) << byte_pos;
            buffer.mark_dirty();

            drop(data);
            drop(buffer);
            // TODO: Once recovery is implemented, this needs to be removed
            self.buffer_manager.flush_all_buffers()?;
            Ok(())
        } else {
            Err(Error::msg(
                "All buffers in buffer manager are currently pinned",
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::TransactionManager;
    use crate::buffer::buffer_manager::BufferManager;
    use crate::common::PAGE_SIZE;
    use crate::concurrency::TransactionStatus;
    use crate::storage::file_manager::FileManager;

    #[test]
    fn can_bootstrap_and_load_logs() {
        let data_dir = tempdir().unwrap();
        let file_manager = FileManager::new(data_dir.path()).unwrap();
        let buffer_manager = BufferManager::new(file_manager, 1);
        let transaction_manager = TransactionManager::new(&buffer_manager, true).unwrap();

        let t1 = transaction_manager.start_transaction().unwrap();
        assert_eq!(t1.tid, 2);
        assert_eq!(
            transaction_manager.get_transaction_status(t1.tid).unwrap(),
            TransactionStatus::InProgress
        );
        t1.commit().unwrap();
        assert_eq!(
            transaction_manager.get_transaction_status(t1.tid).unwrap(),
            TransactionStatus::Committed
        );

        let t2 = transaction_manager.start_transaction().unwrap();
        assert_eq!(t2.tid, 3);
        assert_eq!(
            transaction_manager.get_transaction_status(t2.tid).unwrap(),
            TransactionStatus::InProgress
        );
        t2.abort().unwrap();
        assert_eq!(
            transaction_manager.get_transaction_status(t2.tid).unwrap(),
            TransactionStatus::Aborted
        );

        // fill at least a page of the transaction log
        for _ in 0..4 * PAGE_SIZE {
            let t = transaction_manager.start_transaction().unwrap();
            if t.tid % 5 == 0 {
                t.abort().unwrap();
            } else {
                t.commit().unwrap();
            }
        }

        let transaction_manager = TransactionManager::new(&buffer_manager, false).unwrap();
        transaction_manager.load_transaction_log().unwrap();

        for tid in 4..=(4 * PAGE_SIZE + 3) {
            if tid % 5 == 0 {
                assert_eq!(
                    transaction_manager
                        .get_transaction_status(tid as u32)
                        .unwrap(),
                    TransactionStatus::Aborted
                );
            } else {
                assert_eq!(
                    transaction_manager
                        .get_transaction_status(tid as u32)
                        .unwrap(),
                    TransactionStatus::Committed
                );
            }
        }

        let t = transaction_manager.start_transaction().unwrap();
        assert_eq!(t.tid, (4 * PAGE_SIZE + 4) as u32);
    }
}
