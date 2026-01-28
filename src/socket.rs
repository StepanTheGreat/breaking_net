use socket2 as sock;
use std::{collections::HashMap, io, mem::MaybeUninit, net};

use crate::{
    MTU_SIZE,
    packet::{PacketCrateBuilder, PacketSeqId, UserPacket},
};

const MAX_HEARTBEAT: f32 = 5.0;

/// A simplified socket structure which directly handles buffers, reading and so on
pub struct SimpleSock {
    /// The socket itself
    socket: sock::Socket,

    /// The receive buffer
    buffer: Box<[u8]>,
}

impl SimpleSock {
    pub fn new(socket: sock::Socket, capacity: usize) -> Self {
        Self {
            socket,
            buffer: vec![0u8; capacity].into_boxed_slice(),
        }
    }

    /// Get a read reference to the underlying socket
    pub fn socket(&self) -> &sock::Socket {
        &self.socket
    }

    /// Send some data to the provided address
    pub fn send_to(&mut self, data: &[u8], to: net::SocketAddr) -> io::Result<()> {
        match self.socket.send_to(data, &to.into()) {
            Ok(written) if written == data.len() => Ok(()),
            _ => Err(io::Error::other("Unable to send the packet")),
        }
    }

    /// Receive a packet from anyone
    pub fn recv_from(&mut self) -> Option<(&[u8], net::SocketAddr)> {
        // Casting between MaybeUninit primitive types here is safe
        let buff = unsafe {
            std::mem::transmute::<&mut [u8], &mut [MaybeUninit<u8>]>(self.buffer.as_mut())
        };

        match self.socket.recv_from(buff) {
            Ok((read, addr)) => {
                // Nothing to do
                if read == 0 {
                    return None;
                }

                Some((&self.buffer[0..read], addr.as_socket()?))
            }
            Err(_) => None,
        }
    }

    /// Does this socket have any packets?
    ///
    /// Calling this method, compared to [SimpleSock::recv_from], doesn't consume the packets
    pub fn has_packets(&self) -> bool {
        self.socket.peek_sender().is_ok()
    }
}

struct SocketConnection {
    /// The connection is directed to
    to: net::SocketAddr,

    /// How much time has passed since the last heartbeat? This must be reset whenever we receive either an explicit
    last_heartbeat: f32,

    /// The builder with which we'll be building all packets
    crate_builder: PacketCrateBuilder,

    /// Packets to send with their respected decrementing timers
    ///
    /// TODO: The keys don't make any sense here
    packets: HashMap<Option<PacketSeqId>, (f32, UserPacket)>,
}

impl SocketConnection {
    fn new(to: net::SocketAddr) -> Self {
        Self {
            to,
            last_heartbeat: MAX_HEARTBEAT,

            crate_builder: PacketCrateBuilder::new(MTU_SIZE),

            packets: HashMap::with_capacity(20),
        }
    }

    /// Queue a new packet to send through this connection ASAP
    fn queue_packet(&mut self, packet: UserPacket) {
        self.packets.insert(packet.sequence_id(), (0.0, packet));
    }

    /// Acknowledgments have been received on this connection
    fn acknowledgments_received(&mut self, acks: &[PacketSeqId]) {
        if acks.is_empty() {
            return;
        }

        // For each acknowledged ID
        for ack in acks.iter().copied() {
            // We're going to remove it from the packet list
            let _ = self.packets.remove(&Some(ack));
        }
    }

    fn poll(&mut self, socket: &mut SimpleSock, dt: f32) {
        // For each packet we're going to simply decrement their timers
        for (_, (timer, packet)) in self.packets.iter_mut() {
            // Decrement its timer
            *timer -= dt;
        }

        // TODO: Build packets and send them
    }
}

pub enum SocketEvent {
    Connection(net::SocketAddr),
    Disconnection(net::SocketAddr),
    Packet {
        from: net::SocketAddr,
        payload: Box<[u8]>,
    },
}

pub struct Socket {
    socket: sock::Socket,
    addr: net::SocketAddr,
    connections: HashMap<net::SocketAddr, SocketConnection>,
}

impl Socket {
    pub fn new(addr: net::SocketAddr) -> io::Result<Self> {
        let domain = if addr.is_ipv4() {
            sock::Domain::IPV4
        } else {
            sock::Domain::IPV6
        };

        let socket = sock::Socket::new(domain, sock::Type::DGRAM, Some(sock::Protocol::UDP))?;

        Ok(Self {
            socket,
            addr,
            connections: HashMap::with_capacity(2),
        })
    }

    /// Is this socket connected to the provided socket? Note that this information might be invalid without polling.
    pub fn is_connected(&self, to: &net::SocketAddr) -> bool {
        self.connections.contains_key(to)
    }

    pub fn send_to(&mut self, to: &net::SocketAddr, data: &[u8]) {
        // self.connections.get(to).unwrap()
    }
}
