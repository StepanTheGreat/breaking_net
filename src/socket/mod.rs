use socket2 as sock;
use std::{
    collections::{HashMap, HashSet, VecDeque}, io, mem::MaybeUninit, net, rc::Rc
};

mod channels;
mod connection;

use crate::{
    MTU_SIZE, MTU_SIZE_PRIVATE,
    crc32::{CRC32_SIG_LEN, crc32},
    packet::{PacketCrate, PacketCrateBuilder, Reliability, UserPacket},
};

use connection::SocketConnection;

/// We'll keep up to 512 packets
const PACKET_WINDOW_BITS: usize = 512;

/// This small module implements utilities for testing different network environments. The main goal is to be able to "reproduce"
/// network instability, to workaround those in tests (because tests are in most cases run locally)
#[cfg(feature = "stress_testing")]
mod stress_testing {
    use std::cell::{Cell, LazyCell, RefCell};

    use rand::{Rng, SeedableRng, rngs::SmallRng};

    thread_local! {
        static PACKET_LOSS_CHANCE: Cell<f32> = Cell::default();

        static PACKET_DUBLICATION_CHANCE: Cell<f32> = Cell::default();

        static PACKET_CORRUPTION_CHANCE: Cell<f32> = Cell::default();

        static PACKET_REORDER_CHANCE: Cell<f32> = Cell::default();

        pub(crate) static RNG_STATE: LazyCell<RefCell<SmallRng>> = LazyCell::new(||
            RefCell::new(rand::rngs::SmallRng::from_os_rng())
        );
    }

    fn assert_chance_valid(chance: f32) {
        assert!(
            (0.0..=1.0).contains(&chance),
            "The chance percentage must be between 0 and 1"
        );
    }

    /// Set the thread-local packet loss chance
    pub fn set_packet_loss_chance(new_chance: f32) {
        assert_chance_valid(new_chance);
        PACKET_LOSS_CHANCE.set(new_chance);
    }

    /// Set the thread-local packet dublication chance
    pub fn set_packed_dublication_chance(new_chance: f32) {
        assert_chance_valid(new_chance);
        PACKET_DUBLICATION_CHANCE.set(new_chance);
    }

    /// Set the thread-local packet dublication chance
    pub fn set_packed_corruption_chance(new_chance: f32) {
        assert_chance_valid(new_chance);
        PACKET_CORRUPTION_CHANCE.set(new_chance);
    }

    /// Set the thread-local packet loss chance
    pub fn set_packet_reorder_chance(new_chance: f32) {
        assert_chance_valid(new_chance);
        PACKET_REORDER_CHANCE.set(new_chance);
    }

    /// Reset the stress-testing environment
    pub fn reset_stress_environment() {
        set_packed_corruption_chance(0.0);
        set_packed_dublication_chance(0.0);
        set_packet_loss_chance(0.0);
        set_packet_reorder_chance(0.0);
    }

    /// Generate a random number between 0 and 1, and check if it's less than the provided chance (thus returning `true`)
    fn satisfies_random_chance(chance: f32) -> bool {
        RNG_STATE.with(|rng| rng.borrow_mut().random_range(0.0..=1.0) <= chance)
    }

    /// Should this next packet get corrupted?
    pub(crate) fn should_corrupt_packet() -> bool {
        satisfies_random_chance(PACKET_CORRUPTION_CHANCE.get())
    }

    /// Should this next packet get lost?  
    pub(crate) fn should_lose_packet() -> bool {
        satisfies_random_chance(PACKET_LOSS_CHANCE.get())
    }

    /// Should this next packet get dublicated?
    pub(crate) fn should_dublicate_packet() -> bool {
        satisfies_random_chance(PACKET_DUBLICATION_CHANCE.get())
    }

    /// Should the next packets get reordered?
    pub(crate) fn should_reorder_packets() -> bool {
        satisfies_random_chance(PACKET_REORDER_CHANCE.get())
    }
}

#[cfg(feature = "stress_testing")]
pub use stress_testing::*;

#[derive(Default, Clone, Copy)]
pub struct SockSettings {
    pub broadcaster: bool,
    pub reuses_address: bool,
}

/// A simplified socket structure which directly handles buffers, reading and so on
pub struct SimpleSock {
    /// The socket itself
    socket: sock::Socket,

    addr: net::SocketAddr,

    /// The receive buffer
    recv_buffer: Box<[u8]>,

    send_buffer: Box<[u8]>,
}

impl SimpleSock {
    pub fn new_ex(
        addr: net::SocketAddr,
        capacity: usize,
        settings: SockSettings,
    ) -> io::Result<Self> {
        let domain = if addr.is_ipv4() {
            sock::Domain::IPV4
        } else {
            sock::Domain::IPV6
        };

        // Create a new socket
        let socket = sock::Socket::new(domain, sock::Type::DGRAM, Some(sock::Protocol::UDP))?;

        socket.set_nonblocking(true)?;

        // Apply our options
        socket.set_broadcast(settings.broadcaster)?;
        socket.set_reuse_address(settings.reuses_address)?;

        // Bind it to the provided address
        socket.bind(&addr.into())?;

        let addr = socket
            .local_addr()
            .expect("The socket is bound")
            .as_socket()
            .unwrap();

        Ok(Self {
            socket,
            addr,
            recv_buffer: vec![0u8; capacity].into_boxed_slice(),
            send_buffer: vec![0u8; capacity].into_boxed_slice(),
        })
    }

    pub fn new(addr: net::SocketAddr, capacity: usize) -> io::Result<Self> {
        Self::new_ex(addr, capacity, SockSettings::default())
    }

    /// Send some data to the provided address
    pub fn send_to(&mut self, data: &[u8], to: net::SocketAddr) -> io::Result<()> {
        // If we're stress testing - we'll just not do anything (like if the packet got naturally lost)
        #[cfg(feature = "stress_testing")]
        if should_lose_packet() {
            return Ok(());
        }

        if data.len() > self.send_buffer.len()+4 {
            return Err(io::Error::other("Reached socket's send capacity limits"));
        }

        // Copy the message to our buffer
        let mut data_len = data.len();
        self.send_buffer[..data_len].copy_from_slice(data);

        // Compute the CRC signature
        let crc_bytes = crc32(&self.send_buffer[..data_len]).to_be_bytes();

        // Add it to the end of the message
        self.send_buffer[data_len..data_len+crc_bytes.len()].copy_from_slice(&crc_bytes);
        data_len += crc_bytes.len();

        let data = &self.send_buffer[..data_len];

        match self.socket.send_to(data, &to.into()) {
            Ok(written) if written == data.len() => Ok(()),
            _ => Err(io::Error::other("Unable to send the packet")),
        }?;

        // If this packet must be dublicated - we'll just send it twice.
        #[cfg(feature = "stress_testing")]
        if should_dublicate_packet() {
            // For the sake of simplicity we're going to dublicate code here.
            match self.socket.send_to(data, &to.into()) {
                Ok(written) if written == data.len() => Ok(()),
                _ => Err(io::Error::other("Unable to send the packet")),
            }?;
        }

        Ok(())
    }

    /// Receive a packet from anyone
    pub fn recv_from(&mut self) -> Option<(&[u8], net::SocketAddr)> {
        // Casting between MaybeUninit primitive types here is safe
        let buff = unsafe {
            std::mem::transmute::<&mut [u8], &mut [MaybeUninit<u8>]>(self.recv_buffer.as_mut())
        };

        match self.socket.recv_from(buff) {
            Ok((read, addr)) => {
                // We received less bytes than our CRC signature
                if read < CRC32_SIG_LEN {
                    return None;
                }

                // If we're stress testing and the packet is supposed to be corrupted - we'll just reverse the received message
                #[cfg(feature = "stress_testing")]
                if should_corrupt_packet() {
                    buff[0..read].reverse();
                }

                // Let's compute the CRC signature from our
                let crc = crc32(&self.recv_buffer[..read - CRC32_SIG_LEN]);
                let crc_bytes: [u8; CRC32_SIG_LEN] = crc.to_be_bytes();

                if self.recv_buffer[read - CRC32_SIG_LEN..read] != crc_bytes {
                    return None;
                }

                Some((&self.recv_buffer[..read - CRC32_SIG_LEN], addr.as_socket()?))
            }
            Err(_) => None,
        }
    }

    pub fn addr(&self) -> net::SocketAddr {
        self.addr
    }

    /// Does this socket have any packets?
    ///
    /// Calling this method, compared to [SimpleSock::recv_from], doesn't consume the packets
    pub fn has_packets(&self) -> bool {
        self.socket.peek_sender().is_ok()
    }
}

pub struct ReceivedPacket {
    pub data: Vec<u8>,
    pub reliability: Reliability,
    pub sender: net::SocketAddr,
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
    socket: SimpleSock,

    packet_builder: PacketCrateBuilder,
    connections: HashMap<net::SocketAddr, SocketConnection>,

    requested_connections: HashSet<net::SocketAddr>,

    recv_buffer: VecDeque<ReceivedPacket>,
}

impl Socket {
    pub fn new(addr: net::SocketAddr) -> io::Result<Self> {
        let socket = SimpleSock::new(addr, MTU_SIZE_PRIVATE)?;

        Ok(Self {
            socket,

            packet_builder: PacketCrateBuilder::new(MTU_SIZE_PRIVATE),
            connections: HashMap::with_capacity(2),
            requested_connections: HashSet::new(),

            recv_buffer: VecDeque::new(),
        })
    }

    /// Is this socket connected to the provided socket? Note that this information might be invalid without polling.
    pub fn is_connected(&self, to: &net::SocketAddr) -> bool {
        self.connections.contains_key(to)
    }

    /// Get this socket's address
    pub fn addr(&self) -> net::SocketAddr {
        self.socket.addr()
    }

    /// Send a packet to the provided address
    pub fn send_to(&mut self, to: &net::SocketAddr, data: &[u8], how: Reliability) {
        assert!(data.len() <= MTU_SIZE, "Reached an MTU limit of {MTU_SIZE}");

        match self.connections.get_mut(to) {
            Some(connection) => {

                // If there's a connection - send through it
                connection.queue_packet(how, data.to_owned());
            },
            None => {
                // In any other case we'll construct a new unreliable packet with nothing and send it
                self.packet_builder.put_user_packet(UserPacket::new_unreliable(Rc::new(data.to_owned())));
                self.socket.send_to(self.packet_builder.build(), *to).unwrap();

                // Add it to our requested connections
                self.requested_connections.insert(*to);
            }
        }
    }

    /// Receive and distribute (to connections) packets
    fn receive_packets(&mut self) {
        while let Some((data, sender)) = self.socket.recv_from() {

            // If it's decodable
            let (ack_base, ack_map, packets) = match bitcode::decode::<PacketCrate>(data) {
                Ok(packet) => packet,
                Err(_) => continue
            };

            // If we received a packet from a requested address - we'll automatically create a connection to it
            if self.requested_connections.remove(&sender) {
                self.connect(sender);
            }

            // Get or make a new socket connection for this packet
            let mut connection = self.connections.get_mut(&sender);

            // If this is a packet from a known connection - let it mark all the acknowledgments it needs
            if let Some(conn) = connection.as_mut() {
                conn.own_acknowledgments_received(ack_base, ack_map);
            }

            // Now we're going to iterate every single packet
            for packet in packets {
                // If we have a connection and our packet contains a sequence ID - we're going to notify our connection about this sequence ID
                if let Some(conn) = connection.as_mut() {
                    // If this packet has a sequence ID - acknowledge it
                    if let Some(seqid) = packet.sequence_id() {
                        conn.other_acknowledgment_received(seqid);
                    }

                    // And finally - process it (filter, reorder it and so on)
                    conn.process_packet(packet);
                } else {
                    // In any other case we're just going to buffer it without a connection
                    self.recv_buffer.push_back(ReceivedPacket {
                        sender: sender,

                        // Even though it's factually incorrect - for us there's no connection,
                        // thus this packet is essentially unreliable no matter what
                        reliability: Reliability::Unreliable,
                        data: packet.consume_payload().unwrap(),
                    });
                }
            }
        }
    }

    /// Poll all our connections and 
    fn poll_connections(&mut self, dt: f32) {
        for (_, connection) in self.connections.iter_mut() {
            connection.poll(&mut self.socket, &mut self. packet_builder, dt);

            while let Some(packet) = connection.recv_packet() {
                let reliability = packet.reliability();
                let sender = connection.to_addr();

                // Finally, add it to our queue
                self.recv_buffer.push_back(ReceivedPacket {
                    data: packet.consume_payload().unwrap(),
                    reliability,
                    sender,
                });
            }
        }
    }

    /// Poll this socket thus updating its inner receive buffer and sending data.
    pub fn poll(&mut self, dt: f32) {
        assert!(dt >= 0.0);

        // We're going to collect all packets received by this socket
        self.receive_packets();

        // Now, we're going to poll each connection individually as well
        self.poll_connections(dt);
    }

    /// Check if we got a packet
    pub fn recv_from(&mut self) -> Option<ReceivedPacket> {
        self.recv_buffer.pop_front()
    }

    pub fn has_packets(&self) -> bool {
        !self.recv_buffer.is_empty()
    }

    /// "Establish" a new connection to the provided address.
    ///
    /// This doesn't actually establish anything, it just creates a logical connection between this address.
    /// Note that if the other address doesn't send any packets whatsoever - this connection will close very quickly.
    pub fn connect(&mut self, addr: net::SocketAddr) {
        if !self.connections.contains_key(&addr) {
            self.connections.insert(addr, SocketConnection::new(addr));
        }
    }

    /// How many packets are available
    pub fn packets(&self) -> usize {
        self.recv_buffer.len()
    }

    /// Get round trip time statistics (measured in seconds) for the provided address (if a connection exists)
    pub fn round_trip_time(&self, addr: net::SocketAddr) -> Option<f32> {
        self.connections.get(&addr)
            .map(|connection| connection.round_trip_time())
    }

    /// Get the average packet loss (betweeen 0 and 1) for the provided address (if a connection exists)
    pub fn packet_loss(&self, addr: net::SocketAddr) -> Option<f32> {
        self.connections.get(&addr)
            .map(|connection| connection.packet_loss())
    }
}
