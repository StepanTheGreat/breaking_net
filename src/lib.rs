use std::io;
use std::net;
use std::sync::LazyLock;

use crate::socket::SimpleSock;
use crate::socket::SockSettings;

mod crc32;
mod packet;
mod socket;
mod window;

mod utils;

pub use packet::Reliability;
pub use socket::Socket;

pub(crate) use utils::*;

#[cfg(feature = "stress_testing")]
pub use socket::{
    reset_stress_environment, set_packed_corruption_chance, set_packed_dublication_chance,
    set_packet_loss_chance, set_packet_reorder_chance,
};

/// The private crate-level MTU size is just a little bit bigger, to be able to fit protocol-level stuff.
///
/// This way, the user can fully utilize the entire MTU limit, while the crate itself can fit its own metadata safely
pub(crate) const MTU_SIZE_PRIVATE: usize = 1200;

/// The maximum transport unit for our packets. I'm using a much lower number here to avoid
/// fragmentation on most networks, though usually you're supposed to query it directly from
/// a network interface.
pub const MTU_SIZE: usize = MTU_SIZE_PRIVATE-50;

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

pub struct BroadcastListener {
    socket: SimpleSock,
}

impl BroadcastListener {
    pub fn new(socket_addr: net::SocketAddr) -> io::Result<Self> {
        let socket = SimpleSock::new_ex(
            socket_addr,
            MTU_SIZE_PRIVATE,
            SockSettings {
                reuses_address: true,
                ..Default::default()
            },
        )?;

        Ok(Self { socket })
    }

    /// Check if this listener has any packets without consuming them from the queue
    pub fn has_packets(&self) -> bool {
        self.socket.has_packets()
    }

    /// Receive a single packet from the network.
    ///
    /// [None] means there are no packets
    pub fn recv(&mut self) -> Option<(Vec<u8>, net::SocketAddr)> {
        self.socket
            .recv_from()
            .map(|(data, addr)| (data.to_vec(), addr))
    }
}

/// A socket whose purpose is to specifically write broadcast messages.
///
/// This is a temporary API choice, and in the future will get replaced by allowing sockets to
/// send broadcasts directly
pub struct BroadcastWriter {
    socket: SimpleSock,
    broadcast_addr: net::SocketAddr,
}

impl BroadcastWriter {
    pub fn new(socket_addr: net::SocketAddr, port: u16) -> io::Result<Self> {
        // The address to which we're going to send packets
        let broadcast_addr = socket_addr!(broadcast;port);

        // The capacity is at zero, since we're not going to receive anything
        let socket = SimpleSock::new_ex(
            socket_addr,
            MTU_SIZE_PRIVATE,
            SockSettings {
                broadcaster: true,
                ..Default::default()
            },
        )?;

        Ok(Self {
            socket,
            broadcast_addr,
        })
    }

    /// Update the port of a broadcast writer
    pub fn set_port(&mut self, new_port: u16) {
        self.broadcast_addr = socket_addr!(255,255,255,255;new_port);
    }

    /// Send a broadcast message
    ///
    /// Note that this method will panic, if data's length is more than [MTU_SIZE]
    pub fn send(&mut self, data: &[u8]) -> Result<(), io::Error> {
        assert!(data.len() < MTU_SIZE, "Reached an MTU limit of {MTU_SIZE}");

        self.socket.send_to(data, self.broadcast_addr)
    }
}
