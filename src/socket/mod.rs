use socket2 as sock;
use std::{
    collections::{HashMap, HashSet, VecDeque}, io, mem::MaybeUninit, net
};

mod channels;
mod connection;

use crate::{
    MTU_SIZE, MTU_SIZE_PRIVATE, PROTOCOL_SIGNATURE, crc32::{CRC32_SIG_LEN, crc32_sign, crc32_verify}, 
    packet::{PacketCrate, PacketCrateBuilder, Reliability}
};

use connection::SocketConnection;

/// This small module implements utilities for testing different network environments. The main goal is to be able to "reproduce"
/// network instability, to workaround those in tests (because tests are in most cases run locally)
#[cfg(feature = "stress_testing")]
mod stress_testing {
    use std::cell::{Cell, LazyCell, RefCell};

    use rand::{Rng, SeedableRng, rngs::SmallRng};

    thread_local! {
        static MESSAGE_LOSS_CHANCE: Cell<f32> = Cell::default();

        static MESSAGE_DUBLICATION_CHANCE: Cell<f32> = Cell::default();

        static MESSAGE_CORRUPTION_CHANCE: Cell<f32> = Cell::default();

        static MESSAGE_REORDER_CHANCE: Cell<f32> = Cell::default();

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

    /// Set the thread-local message loss chance
    pub fn set_message_loss_chance(new_chance: f32) {
        assert_chance_valid(new_chance);
        MESSAGE_LOSS_CHANCE.set(new_chance);
    }

    /// Set the thread-local message dublication chance
    pub fn set_message_dublication_chance(new_chance: f32) {
        assert_chance_valid(new_chance);
        MESSAGE_DUBLICATION_CHANCE.set(new_chance);
    }

    /// Set the thread-local message dublication chance
    pub fn set_message_corruption_chance(new_chance: f32) {
        assert_chance_valid(new_chance);
        MESSAGE_CORRUPTION_CHANCE.set(new_chance);
    }

    /// Set the thread-local message loss chance
    pub fn set_message_reorder_chance(new_chance: f32) {
        assert_chance_valid(new_chance);
        MESSAGE_REORDER_CHANCE.set(new_chance);
    }

    /// Reset the stress-testing environment
    pub fn reset_stress_environment() {
        set_message_corruption_chance(0.0);
        set_message_dublication_chance(0.0);
        set_message_loss_chance(0.0);
        set_message_reorder_chance(0.0);
    }

    /// Generate a random number between 0 and 1, and check if it's less than the provided chance (thus returning `true`)
    fn satisfies_random_chance(chance: f32) -> bool {
        RNG_STATE.with(|rng| rng.borrow_mut().random_range(0.0..=1.0) <= chance)
    }

    /// Should this next message get corrupted?
    pub(crate) fn should_corrupt_message() -> bool {
        satisfies_random_chance(MESSAGE_CORRUPTION_CHANCE.get())
    }

    /// Should this next message get lost?  
    pub(crate) fn should_lose_message() -> bool {
        satisfies_random_chance(MESSAGE_LOSS_CHANCE.get())
    }

    /// Should this next message get dublicated?
    pub(crate) fn should_dublicate_message() -> bool {
        satisfies_random_chance(MESSAGE_DUBLICATION_CHANCE.get())
    }

    /// Should the next messages get reordered?
    pub(crate) fn should_reorder_messages() -> bool {
        satisfies_random_chance(MESSAGE_REORDER_CHANCE.get())
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

    /// Protocol's signature
    signature: &'static str,

    /// The receive buffer
    recv_buffer: Box<[u8]>,

    send_buffer: Box<[u8]>,

    mtu: usize
}

impl SimpleSock {
    pub fn new_ex(
        addr: net::SocketAddr,
        mtu: usize,
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

        // Get our protocol signature
        let signature = *PROTOCOL_SIGNATURE;

        // Our buffers will be *slightly* larger to accomodate for the signature. The signature however isn't send, 
        // it's only used for CRC checks
        let buffer_capacity = mtu + signature.bytes().len();

        Ok(Self {
            socket,
            addr,
            signature,
            mtu,
            recv_buffer: vec![0u8; buffer_capacity].into_boxed_slice(),
            send_buffer: vec![0u8; buffer_capacity].into_boxed_slice(),
        })
    }

    pub fn new(addr: net::SocketAddr, capacity: usize) -> io::Result<Self> {
        Self::new_ex(addr, capacity, SockSettings::default())
    }

    /// Send some data to the provided address
    pub fn send_to(&mut self, data: &[u8], to: net::SocketAddr) -> io::Result<()> {
        // If we're stress testing - we'll just not do anything (like if the message got naturally lost)
        #[cfg(feature = "stress_testing")]
        if should_lose_message() {
            return Ok(());
        }

        if data.len() > self.mtu-CRC32_SIG_LEN {
            return Err(io::Error::other("Reached socket's MTU limits"));
        }

        // TODO: The socket shouldn't be responsible for verifying data integrity. It should be the responsibility of the layer
        // TODO: above. A socket is just a dumb primitive for sending/receiving data (and simulating network environment)
        let data_len = data.len();        
        let data_crc_len = data_len+CRC32_SIG_LEN;

        // Copy the message to our buffer
        self.send_buffer[..data_len].copy_from_slice(data);

        // Sign it
        crc32_sign(&mut self.send_buffer[..data_crc_len], Some(self.signature));

        // Augment our data slice to account for our new signature
        let data = &self.send_buffer[..data_crc_len];

        match self.socket.send_to(data, &to.into()) {
            Ok(written) if written == data.len() => Ok(()),
            _ => Err(io::Error::other("Unable to send the message")),
        }?;

        // If this message must be dublicated - we'll just send it twice.
        #[cfg(feature = "stress_testing")]
        if should_dublicate_message() {
            // For the sake of simplicity we're going to dublicate code here.
            match self.socket.send_to(data, &to.into()) {
                Ok(written) if written == data.len() => Ok(()),
                _ => Err(io::Error::other("Unable to send the message")),
            }?;
        }

        Ok(())
    }

    /// Receive a message from anyone
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

                // If we're stress testing and the message is supposed to be corrupted - we'll just reverse the received message
                #[cfg(feature = "stress_testing")]
                if should_corrupt_message() {
                    buff[0..read].reverse();
                }

                let crc_valid = crc32_verify(&self.recv_buffer[..read], Some(self.signature));

                // Only return when signatures match
                if crc_valid {
                    let data_len = read-CRC32_SIG_LEN;
                    
                    Some((&self.recv_buffer[..data_len], addr.as_socket()?))
                } else {
                    None
                }
            }
            Err(_) => None,
        }
    }

    pub fn addr(&self) -> net::SocketAddr {
        self.addr
    }

    /// Does this socket have any messages?
    ///
    /// Calling this method, compared to [SimpleSock::recv_from], doesn't consume the messages
    pub fn has_messages(&self) -> bool {
        self.socket.peek_sender().is_ok()
    }
}

pub struct ReceivedMessage {
    pub data: Vec<u8>,
    pub reliability: Reliability,
    pub sender: net::SocketAddr,
}

pub struct Socket {
    socket: SimpleSock,

    packet_builder: PacketCrateBuilder,
    connections: HashMap<net::SocketAddr, SocketConnection>,

    requested_connections: HashSet<net::SocketAddr>,

    recv_buffer: VecDeque<ReceivedMessage>,
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

    /// Send a message to the provided address (under the hood this will allocate resources for a new connection, even though the connection
    /// isn't for now mutual)
    pub fn send_to(&mut self, to: &net::SocketAddr, data: &[u8], how: Reliability) {
        assert!(data.len() <= MTU_SIZE, "Reached an MTU limit of {MTU_SIZE}");

        let connection = self.connections.entry(*to)
            .or_insert(SocketConnection::new(*to));

        connection.queue_message(how, data.to_owned());
    }

    /// Receive and distribute (to connections) messages
    fn receive_messages(&mut self) {
        while let Some((data, sender)) = self.socket.recv_from() {

            // If it's decodable
            let pcrate = match bitcode::decode::<PacketCrate>(data) {
                Ok(pcrate) => pcrate,
                Err(_) => continue
            };

            // If we received a message from a requested address - we'll automatically create a connection to it
            if self.requested_connections.remove(&sender) {
                self.connect(sender);
            }

            // Get or make a new socket connection for this message
            let mut connection = self.connections.get_mut(&sender);

            // If this is a message from a known connection - let it mark all the acknowledgments it needs
            if let Some(conn) = connection.as_mut() {
                conn.own_acknowledgments_received(pcrate.ack_base, pcrate.ack_map);
            }

            // Now we're going to iterate every single message
            for message in pcrate.messages {

                // If we have a connection and our message contains a message ID - we're going to notify our connection about it
                if let Some(conn) = connection.as_mut() {
                    // If this message has a sequence ID - acknowledge it
                    if let Some(seqid) = message.message_id() {
                        conn.other_acknowledgment_received(seqid);
                    }

                    // And finally - process it (filter, reorder it and so on)
                    conn.process_message(message);
                } else {
                    // In any other case we're just going to buffer it without a connection
                    self.recv_buffer.push_back(ReceivedMessage {
                        sender: sender,

                        // Even though it's factually incorrect - for us there's no connection,
                        // thus this message is essentially unreliable no matter what
                        reliability: Reliability::Unreliable,
                        data: message.consume_payload().unwrap(),
                    });
                }
            }
        }
    }

    /// Poll all our connections and 
    fn poll_connections(&mut self, dt: f32) {
        for (_, connection) in self.connections.iter_mut() {
            connection.poll(&mut self.socket, &mut self.packet_builder, dt);

            while let Some(message) = connection.recv_message() {
                let reliability = message.reliability();
                let sender = connection.to_addr();

                // Finally, add it to our queue
                self.recv_buffer.push_back(ReceivedMessage {
                    data: message.consume_payload().unwrap(),
                    reliability,
                    sender,
                });
            }
        }
    }

    /// Poll this socket thus updating its inner receive buffer and sending data.
    pub fn poll(&mut self, dt: f32) {
        assert!(dt >= 0.0);

        // We're going to collect all messages received by this socket
        self.receive_messages();

        // Now, we're going to poll each connection individually as well
        self.poll_connections(dt);
    }

    /// Check if we got a message
    pub fn recv_from(&mut self) -> Option<ReceivedMessage> {
        self.recv_buffer.pop_front()
    }

    pub fn has_messages(&self) -> bool {
        !self.recv_buffer.is_empty()
    }

    /// "Establish" a new connection to the provided address.
    ///
    /// This doesn't actually establish anything, it just creates a logical connection between this address.
    /// Note that if the other address doesn't send any messages whatsoever - this connection will close very quickly.
    pub fn connect(&mut self, addr: net::SocketAddr) {
        if !self.connections.contains_key(&addr) {
            self.connections.insert(addr, SocketConnection::new(addr));
        }
    }

    /// How many messages are available
    pub fn messages(&self) -> usize {
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
