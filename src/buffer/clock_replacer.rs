use super::PoolPos;

/// ClockReplacer implements the clock page replacement policy for a buffer manager
pub(in crate::buffer) struct ClockReplacer {
    pool_size: usize,
    clock_hand: PoolPos,
    free_buffers: usize,
    pin_counts: Vec<u32>,
    ref_bits: Vec<bool>,
}

impl ClockReplacer {
    /// Construct a new ClockReplacer for a BufferManager with a given pool_size
    pub fn new(pool_size: usize) -> Self {
        Self {
            pool_size,
            clock_hand: 0,
            free_buffers: pool_size,
            pin_counts: vec![0; pool_size],
            ref_bits: vec![false; pool_size],
        }
    }

    /// The buffer at the given position in the pool was pinned
    pub fn pin(&mut self, buffer: PoolPos) {
        self.pin_counts[buffer] += 1;
        self.ref_bits[buffer] = true;
        if self.pin_counts[buffer] == 1 {
            // previously free buffer is now used
            self.free_buffers -= 1;
        }
    }

    /// The buffer at the given position in the pool was unpinned
    pub fn unpin(&mut self, buffer: PoolPos) {
        self.pin_counts[buffer] -= 1;
        if self.pin_counts[buffer] == 0 {
            self.free_buffers += 1;
        }
    }

    /// Returns None if all buffers are currently pinned, else finds an unused buffer
    pub fn find_free_buffer(&mut self) -> Option<PoolPos> {
        if self.free_buffers == 0 {
            return None;
        }
        loop {
            let buffer = self.clock_hand;
            self.clock_hand = (self.clock_hand + 1) % self.pool_size;
            let pin_count = self.pin_counts[buffer];
            if pin_count != 0 {
                continue;
            }
            if !self.ref_bits[buffer] {
                return Some(buffer);
            } else {
                self.ref_bits[buffer] = false;
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use super::ClockReplacer;

    #[test]
    fn basic_test() {
        let mut clock_replacer = ClockReplacer::new(3);
        clock_replacer.pin(0);
        clock_replacer.pin(1);
        clock_replacer.pin(2);

        let free_buffer = clock_replacer.find_free_buffer();
        assert!(
            free_buffer.is_none(),
            "There shouldn't be any free buffers if have been pinned"
        );

        // After unpinning all buffers, all buffers should be free
        clock_replacer.unpin(0);
        clock_replacer.unpin(1);
        clock_replacer.unpin(2);
        let free_buffer = clock_replacer.find_free_buffer();
        assert_eq!(free_buffer, Some(0));
        let free_buffer = clock_replacer.find_free_buffer();
        assert_eq!(free_buffer, Some(1));
        let free_buffer = clock_replacer.find_free_buffer();
        assert_eq!(free_buffer, Some(2));

        // Pin all buffers twice
        clock_replacer.pin(0);
        clock_replacer.pin(0);
        clock_replacer.pin(1);
        clock_replacer.pin(1);
        clock_replacer.pin(2);
        clock_replacer.pin(2);

        // After unpinning every buffer once, there still shouldn't be a free buffer
        // since every buffer was pinned twice
        clock_replacer.unpin(0);
        clock_replacer.unpin(1);
        clock_replacer.unpin(2);
        assert!(clock_replacer.find_free_buffer().is_none());

        clock_replacer.unpin(2);
        assert_eq!(clock_replacer.find_free_buffer(), Some(2));
    }

    #[test]
    fn correct_free_buffer_order() {
        let mut clock_replacer = ClockReplacer::new(3);
        clock_replacer.pin(0);
        clock_replacer.pin(1);
        clock_replacer.pin(2);
        clock_replacer.unpin(1);

        assert_eq!(clock_replacer.find_free_buffer(), Some(1));
        clock_replacer.pin(1);
        clock_replacer.unpin(1);
        clock_replacer.unpin(2);
        clock_replacer.unpin(0);

        // Buffer 1 was recently used, so it should appear later as a free buffer
        assert_eq!(clock_replacer.find_free_buffer(), Some(2));
        assert_eq!(clock_replacer.find_free_buffer(), Some(0));
        assert_eq!(clock_replacer.find_free_buffer(), Some(1));
    }
}
