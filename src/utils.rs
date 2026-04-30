use std::collections::VecDeque;

/// A utility polling macro (you can specify by how much to poll, how many times and which sockets)
#[macro_export]
macro_rules! poll_socks {
    ($times:expr, $dt:expr, [$($sock:expr),*]) => {
        for _ in 0..$times {
            $(
                ($sock).poll($dt);
            )*
        }
    };
    ($dt:expr, [$($sock:expr),*]) => {
        poll_socks!(1, $dt, [$($sock),*]);
    }
}

/// A general purpose circular buffer that keeps only a specific amount of items at the same time.
/// Highly useful for circular windows or history keeping
pub(crate) struct Circular<T> {
    buffer: VecDeque<T>,
    len: usize,
}

impl<T> Circular<T> {
    pub fn new(len: usize) -> Self {
        Self {
            buffer: VecDeque::new(),
            len,
        }
    }

    /// Push a new value onto this buffer, possibly removing former values if they're outside the window
    pub fn push(&mut self, value: T) {
        if self.buffer.len() == self.len {
            self.buffer.pop_front();
        }

        self.buffer.push_back(value);
    }

    /// Get reference to the inner buffer
    pub fn inner(&self) -> &VecDeque<T> {
        &self.buffer
    }

    /// Return the current **buffer's** length
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }
}
