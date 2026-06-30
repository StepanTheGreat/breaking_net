use std::{array, collections::VecDeque, ops::Add};

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

    // pub fn is_empty(&self) -> bool {
    //     self.buffer.is_empty()
    // }
}

impl<T> Circular<T>
where
    T: Averageable,
{
    /// Compute an average of the buffer values. If no samples are present, this will return the default.
    pub fn average(&self) -> T {
        let len = self.len();

        if len == 0 {
            return T::default();
        }

        let mut sum = T::default();

        for val in self.inner() {
            sum = sum + *val;
        }

        sum.avg_divide(len)
    }
}

/// A type that can be averaged
pub trait Averageable: Default + Add<Self, Output = Self> + Copy {
    fn avg_divide(&self, by: usize) -> Self;
}

// We're using this macro to auto implement average computation for simple types
macro_rules! impl_avg_for_basic_types {
    ($($ty:ty),*) => {
        $(
            impl Averageable for $ty {
                fn avg_divide(&self, by: usize) -> Self {
                    ((*self as f64) / (by as f64)) as Self
                }
            }
        )*

    };
}

impl_avg_for_basic_types!(usize, f64, u32);

/// A minimal vector that starts on the stack and then moves to the heap
pub enum StackVec<T, const S: usize>
where
    T: Default + Copy,
{
    Stack { items: [T; S], length: usize },
    Heap(Vec<T>),
}

impl<T, const S: usize> StackVec<T, S>
where
    T: Default + Copy,
{
    pub fn new() -> Self {
        Self::Stack {
            items: array::from_fn(|_| T::default()),
            length: 0,
        }
    }

    pub fn is_stack(&self) -> bool {
        matches!(
            self,
            StackVec::Stack {
                items: _,
                length: _
            }
        )
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Heap(v) => v.len(),
            Self::Stack { length, .. } => *length,
        }
    }

    fn should_reallocate(&self) -> bool {
        self.is_stack() && self.len() == S
    }

    pub fn as_slice(&self) -> &[T] {
        match self {
            Self::Heap(v) => v,
            Self::Stack { items, length } => &items[..*length],
        }
    }

    // pub fn as_slice_mut(&mut self) -> &mut [T] {
    //     match self {
    //         Self::Heap(v) => v,
    //         Self::Stack { items, length } => &mut items[..*length]
    //     }
    // }

    /// Reallocates all items into a heap vector
    fn reallocate(&mut self) {
        assert!(self.should_reallocate(), "Invalid state for reallocation");

        let mut v = Vec::with_capacity(S + 1);

        for item in self.as_slice().iter().copied() {
            v.push(item);
        }

        *self = StackVec::Heap(v);
    }

    pub fn push(&mut self, item: T) {
        if self.should_reallocate() {
            self.reallocate();
        }

        match self {
            Self::Heap(v) => v.push(item),
            Self::Stack { items, length } => {
                items[*length] = item;
                *length += 1;
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    // pub fn pop(&mut self) -> Option<T> {
    //     if self.is_empty() {
    //         return None;
    //     }

    //     match self {
    //         Self::Heap(v) => v.pop(),
    //         Self::Stack { items, length } => {
    //             *length -= 1;
    //             Some(items[*length])
    //         }
    //     }
    // }
}

impl<T, const S: usize> From<Vec<T>> for StackVec<T, S>
where
    T: Default + Copy,
{
    fn from(value: Vec<T>) -> Self {
        Self::Heap(value)
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
