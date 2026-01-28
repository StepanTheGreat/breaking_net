use socket2 as sock;
use std::io;
use std::net;

use crate::socket::SimpleSock;

mod crc32;
mod packet;
mod socket;

/// The maximum transport unit for our packets. I'm using a much lower number here to avoid
/// fragmentation on most networks, though usually you're supposed to query it directly from
/// a network interface.
pub const MTU_SIZE: usize = 1200;

/// The size of an IP header
pub(crate) const HEADER_IP_SIZE: usize = 24;

/// The total header size of a single packet
pub(crate) const HEADER_PACKET_SIZE: usize = HEADER_IP_SIZE + (4 + 4 + 1 + 2); // seq_id + hash + kind + data_len

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
        // Create a new UDP socket
        let socket = sock::Socket::new(
            sock::Domain::IPV4,
            sock::Type::DGRAM,
            Some(sock::Protocol::UDP),
        )?;

        // Make it non-blocking
        socket.set_nonblocking(true).unwrap();
        // Allow address reuse (for multiple broadcast listeners on the same port)
        socket.set_reuse_address(true).unwrap();

        // Bind it to our address
        socket.bind(&socket_addr.into())?;

        let socket = SimpleSock::new(socket, MTU_SIZE);

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
        let broadcast_addr = socket_addr!(broadcast;port).into();

        // Create a new UDP socket
        let socket = sock::Socket::new(
            sock::Domain::IPV4,
            sock::Type::DGRAM,
            Some(sock::Protocol::UDP),
        )?;

        // Allow it to send broadcasts
        let _ = socket.set_broadcast(true);

        // Make it non-blocking
        let _ = socket.set_nonblocking(true);

        // Bind it to our socket address
        socket.bind(&socket_addr.into())?;

        // The capacity is at zero, since we're not going to receive anything
        let socket = SimpleSock::new(socket, 0);

        Ok(Self {
            socket,
            broadcast_addr,
        })
    }

    /// Update the port of a broadcast writer
    pub fn set_port(&mut self, new_port: u16) {
        self.broadcast_addr = socket_addr!(255,255,255,255;new_port).into();
    }

    /// Send a broadcast message
    ///
    /// Note that this method will panic, if data's length is more than [MTU_SIZE]
    pub fn send(&mut self, data: &[u8]) -> Result<(), io::Error> {
        assert!(data.len() < MTU_SIZE, "Reached an MTU limit of {MTU_SIZE}");

        self.socket.send_to(data, self.broadcast_addr)
    }
}
