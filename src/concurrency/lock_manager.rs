use std::collections::VecDeque;
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, Mutex};

use dashmap::mapref::entry::Entry;
use dashmap::DashMap;

use super::TransactionId;
use crate::common::TableId;
use crate::storage::TupleId;

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum LockMode {
    Shared,
    Exclusive,
}

impl LockMode {
    fn compatible(&self, other: Self) -> bool {
        match self {
            Self::Shared => other == LockMode::Shared,
            Self::Exclusive => false,
        }
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
enum LockTag {
    Transaction(TransactionId),
    Tuple((TableId, TupleId)),
}

struct LockStatus {
    current_mode: LockMode,
    granted_count: u32,
    awaiting: VecDeque<LockRequest>,
}

impl LockStatus {
    fn new(mode: LockMode) -> Self {
        Self {
            current_mode: mode,
            granted_count: 1,
            awaiting: VecDeque::new(),
        }
    }

    fn can_grant(&self, requested_mode: LockMode) -> bool {
        self.awaiting.is_empty()
            && (self.granted_count == 0 || self.current_mode.compatible(requested_mode))
    }
}

struct LockRequest {
    mode: LockMode,
    grant_sender: Sender<()>,
}

impl LockRequest {
    fn new(mode: LockMode, grant_sender: Sender<()>) -> Self {
        Self { mode, grant_sender }
    }
}

struct Lock {
    status: Mutex<LockStatus>,
}

impl Lock {
    fn new(mode: LockMode) -> Self {
        let status = LockStatus::new(mode);
        Self {
            status: Mutex::new(status),
        }
    }
}

/// wakes up requests which are waiting for this look.
/// Returns true if any requests were waiting, else false
fn wake_up_waiting_requests(status: &mut LockStatus) {
    while let Some(request) = status.awaiting.pop_front() {
        if request.mode == LockMode::Exclusive {
            status.current_mode = LockMode::Exclusive;
            status.granted_count = 1;
            _ = request.grant_sender.send(());
            status.awaiting.pop_front();
            break;
        } else {
            status.current_mode = LockMode::Shared;
            status.granted_count += 1;
            _ = request.grant_sender.send(());
            if let Some(next_request) = status.awaiting.front() {
                if next_request.mode == LockMode::Exclusive {
                    break;
                }
            }
        }
    }
}

/// A LockGuard automatically unlocks the currently held lock
/// when it's dropped
pub struct LockGuard<'a> {
    lock_manager: &'a LockManager,
    lock_tag: LockTag,
    mode: LockMode,
}

impl<'a> Drop for LockGuard<'a> {
    fn drop(&mut self) {
        self.lock_manager.unlock(self.lock_tag, self.mode)
    }
}

pub struct LockManager {
    lock_table: DashMap<LockTag, Arc<Lock>>,
}

impl LockManager {
    pub fn new() -> Self {
        Self {
            lock_table: DashMap::new(),
        }
    }

    /// Locks a tuple. Waits until the lock can be granted.
    pub fn lock_tuple(&self, to_lock: (TableId, TupleId), mode: LockMode) -> LockGuard {
        self.lock(LockTag::Tuple(to_lock), mode)
    }

    /// Locks a transaction. Waits until the lock can be granted.
    pub fn lock_transaction(&self, to_lock: TransactionId, mode: LockMode) -> LockGuard {
        self.lock(LockTag::Transaction(to_lock), mode)
    }

    fn lock(&self, tag: LockTag, mode: LockMode) -> LockGuard {
        match self.lock_table.entry(tag) {
            Entry::Occupied(lock) => {
                let lock = &*lock.get().clone();

                let mut status = lock.status.lock().unwrap();
                if status.can_grant(mode) {
                    status.current_mode = mode;
                    status.granted_count += 1;
                } else {
                    let (sender, receiver) = channel();
                    let request = LockRequest::new(mode, sender);
                    status.awaiting.push_back(request);
                    _ = receiver.recv();
                }
            }
            Entry::Vacant(vacant) => {
                vacant.insert(Arc::new(Lock::new(mode)));
            }
        };

        LockGuard {
            lock_manager: self,
            lock_tag: tag,
            mode,
        }
    }

    /// Acquire an exclusive lock on the transaction that started.
    /// This allows other transactions to wait for completion of this transaction
    /// by acquiring a shared lock.
    /// Dirty hack: Use this to avoid explicit lifetimes
    pub(in self::super) fn start_transaction(&self, tid: TransactionId) {
        let tid = LockTag::Transaction(tid);
        self.lock_table
            .insert(tid, Arc::new(Lock::new(LockMode::Exclusive)));
    }

    /// Dirty hack: Use this to avoid explicit lifetimes
    pub(in self::super) fn end_transaction(&self, tid: TransactionId) {
        let tid = LockTag::Transaction(tid);
        self.unlock(tid, LockMode::Exclusive);
    }

    fn unlock(&self, tag: LockTag, mode: LockMode) {
        match self.lock_table.entry(tag) {
            Entry::Occupied(entry) => {
                let lock = &*entry.get().clone();
                let mut status = lock.status.lock().unwrap();
                assert!(status.current_mode == mode);
                status.granted_count -= 1;
                if status.granted_count == 0 {
                    wake_up_waiting_requests(&mut status);
                }
                if status.granted_count == 0 {
                    entry.remove();
                }
            }
            _ => unreachable!("Could not find lock tag {:?}", tag),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Barrier, Condvar, Mutex};
    use std::thread;
    use std::time::Duration;

    use super::{LockManager, LockMode};

    #[test]
    fn shared_lock_can_be_granted_multiple_times() {
        let lock_manager = Arc::new(LockManager::new());

        let shared_request_count = 5;
        let mut handles = Vec::with_capacity(shared_request_count);
        let barrier = Arc::new(Barrier::new(shared_request_count));

        for _ in 0..shared_request_count {
            let c = Arc::clone(&barrier);
            let lock_manager = Arc::clone(&lock_manager);
            handles.push(thread::spawn(move || {
                let guard = lock_manager.lock_transaction(42, LockMode::Shared);
                c.wait();
                drop(guard);
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn shared_and_exclusive_lock_cannot_be_granted_simultaneously() {
        let lock_manager = LockManager::new();

        let (exclusive_lock, condvar) = (Mutex::new(false), Condvar::new());
        let shared_locked = AtomicBool::new(false);

        thread::scope(|scope| {
            let exclusive_lock = &exclusive_lock;
            let condvar = &condvar;
            let lock_manager = &lock_manager;
            let shared_lock = &shared_locked;

            scope.spawn(move || {
                let mut exclusive = exclusive_lock.lock().unwrap();
                while !*exclusive {
                    exclusive = condvar.wait(exclusive).unwrap();
                }
                // transaction is exclusively locked now, try to get a shared lock
                let guard = lock_manager.lock_transaction(42, LockMode::Shared);
                shared_lock.store(true, Ordering::Relaxed);
                drop(guard);
            });

            scope.spawn(move || {
                let guard = lock_manager.lock_transaction(42, LockMode::Exclusive);

                let mut exclusive = exclusive_lock.lock().unwrap();
                *exclusive = true;
                condvar.notify_all();

                // sleep for a moment so that a shared lock can be requested
                thread::sleep(Duration::from_millis(200));
                let is_shared_locked = shared_lock.load(Ordering::Relaxed);
                assert!(!is_shared_locked);
                drop(guard);
            });
        });
    }
}
