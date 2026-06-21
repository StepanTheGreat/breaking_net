use std::{collections::VecDeque, ops::{Deref, DerefMut}};

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

/// A `Cow`-like structure for arrays. 
/// 
/// When borrowed, borrows a slice, but when owned - allocates an array in a box
pub enum ArrCow<'a, T>
where T: Copy {
    Borrowed(&'a [T]),
    Boxed(Box<[T]>)
}

impl<'a, T> Deref for ArrCow<'a, T>
where T: Copy {
    type Target = [T];

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Borrowed(value) => value,
            Self::Boxed(value) => value
        }    
    }
}

impl<'a, T> DerefMut for ArrCow<'a, T>
where T: Copy {
    fn deref_mut(&mut self) -> &mut Self::Target {

        // If borrowed - copy data to a box
        if let Self::Borrowed(value) = self {
            *self = Self::Boxed(Box::from_iter(value.iter().copied()));
        }
        
        match self {
            Self::Boxed(value) => value,
            Self::Borrowed(_) => unreachable!()
        }    
    }
}

/// Assert equals with an epsilon. Useful for float comparisons
#[macro_export]
macro_rules! assert_eq_eps {
    ($a:expr, $b:expr, $c:expr) => {
        assert!(($b - $a).abs() <= $c);
    };
    ($a:expr, $b:expr, $c:expr, $m:expr) => {
        assert!(($b - $a).abs() <= $c, $m);
    };
}
