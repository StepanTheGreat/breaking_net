use std::time::Duration;

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

/// A utility timer
#[derive(Clone, Copy)]
pub(crate) struct Timer {
    left: Duration,
}

impl Timer {
    /// Create a new timer with the provided amount of time left
    pub fn new(left: Duration) -> Self {
        Self { left }
    }

    /// Update this clock's time
    pub fn tick(&mut self, dt: Duration) {
        self.left = self.left.saturating_sub(dt);
    }

    /// A clock will time out if there's no more time left
    pub fn timed_out(&self) -> bool {
        self.left.is_zero()
    }

    /// Set time on the clock
    pub fn set_time(&mut self, to: Duration) {
        self.left = to;
    }
}
