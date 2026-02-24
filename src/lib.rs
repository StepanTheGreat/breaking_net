#![doc = include_str!("../README.md")]

use std::sync::LazyLock;

mod broadcast;
mod crc32;
mod packet;
mod socket;
mod window;

mod utils;

pub use broadcast::{BroadcastListener, BroadcastWriter};
pub use packet::Reliability;
pub use socket::{Socket, SocketEvent};

pub(crate) use utils::Timer;

#[cfg(feature = "stress_testing")]
pub use socket::{
    reset_stress_environment, set_message_corruption_chance, set_message_dublication_chance,
    set_message_loss_chance, set_message_reorder_chance,
};

/// The private crate-level MTU size is just a little bit bigger, to be able to fit protocol-level stuff.
///
/// This way, the user can fully utilize the entire MTU limit, while the crate itself can fit its own metadata safely
pub(crate) const MTU_SIZE_PRIVATE: usize = 1200;

/// The maximum transport unit for our messages. I'm using a much lower number here to avoid
/// fragmentation on most networks, though usually you're supposed to query it directly from
/// a network interface.
pub const MTU_SIZE: usize = MTU_SIZE_PRIVATE - 50;

/// The protocol signature used when verifying received packets from other sockets
///
/// A signature mismatch will cause packets to simply not get received (because there's an obvious signature mismatch)
pub(crate) static PROTOCOL_SIGNATURE: LazyLock<&'static str> = LazyLock::new(|| {
    // Format our signature as the combination of our protocol's name and version
    let signature = format!("bnet{}", env!("CARGO_PKG_VERSION"));

    // Leak it for the entire duration of the program
    signature.leak()
});

/// Super tiny macro for constructing [SocketAddr] values
///
/// # Example
/// ```
/// use breaking_net::socket_addr;
///
/// // Make a localhost V4 address at port 2555
/// let addr = socket_addr!(127,0,0,1;2555);
///
/// // Or also via
/// let addr2 = socket_addr!(localhost;2555);
///
/// // For broadcast (255.255.255.255)
/// let addr3 = socket_addr!(broadcast;2555);
///
/// // And unspecified (0.0.0.0)
/// let addr4 = socket_addr!(unspecified;2555);
/// ```
#[macro_export]
macro_rules! socket_addr {
    ($a:expr, $b:expr, $c:expr, $d:expr; $port:expr) => {
        std::net::SocketAddr::V4(std::net::SocketAddrV4::new(
            std::net::Ipv4Addr::new($a, $b, $c, $d), $port)
        )
    };
    (localhost; $port:expr) => { socket_addr!(127,0,0,1;$port) };
    (broadcast; $port:expr) => { socket_addr!(255,255,255,255;$port) };
    (unspecified; $port:expr) => { socket_addr!(0,0,0,0;$port) };
}
