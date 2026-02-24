use std::{
    collections::{HashMap, HashSet, VecDeque},
    io, net,
};

mod channels;
mod connection;
mod ssock;

mod receiver;
mod sender;
mod stats;

pub use ssock::{SimpleSock, SockSettings};
#[cfg(feature = "stress_testing")]
pub use ssock::{
    reset_stress_environment, set_message_corruption_chance, set_message_dublication_chance,
    set_message_loss_chance, set_message_reorder_chance,
};

use crate::{
    MTU_SIZE, MTU_SIZE_PRIVATE,
    packet::{PacketCrate, PacketCrateBuilder, Reliability},
};

use connection::SocketConnection;

/// A socket event describes some really rare socket events like connections and disconnections.
pub enum SocketEvent {
    /// A connection was established with the provided socket
    Connection(net::SocketAddr),

    /// A connection was terminated with the provided socket
    Disconnection(net::SocketAddr),
}

pub struct ReceivedMessage {
    pub data: Vec<u8>,
    pub reliability: Reliability,
    pub sender: net::SocketAddr,
}

/// The reliable socket used for reliable communications.
///
/// Automatically handles connection management (except for heartbeats), reliable delivery and so on.
pub struct Socket {
    socket: SimpleSock,

    packet_builder: PacketCrateBuilder,
    connections: HashMap<net::SocketAddr, SocketConnection>,

    requested_connections: HashSet<net::SocketAddr>,

    event_queue: VecDeque<SocketEvent>,
    message_queue: VecDeque<ReceivedMessage>,
}

impl Socket {
    /// Create a new socket on the provided address
    pub fn new(addr: net::SocketAddr) -> io::Result<Self> {
        let socket = SimpleSock::new(addr, MTU_SIZE_PRIVATE)?;

        Ok(Self {
            socket,

            packet_builder: PacketCrateBuilder::new(MTU_SIZE_PRIVATE),
            connections: HashMap::with_capacity(2),
            requested_connections: HashSet::new(),

            event_queue: VecDeque::new(),
            message_queue: VecDeque::new(),
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
    ///
    /// # Panics
    /// Will panic if the amount of bytes sent exceeds the [MTU_SIZE] limit
    pub fn send_to(&mut self, to: &net::SocketAddr, data: &[u8], how: Reliability) {
        assert!(data.len() <= MTU_SIZE, "Reached an MTU limit of {MTU_SIZE}");

        let connection = self
            .connections
            .entry(*to)
            .or_insert(SocketConnection::new(*to));

        connection.queue_message(data.to_owned(), how);
    }

    /// Receive and distribute (to connections) messages
    fn receive_messages(&mut self) {
        while let Some((data, sender)) = self.socket.recv_from() {
            // If it's decodable
            let pcrate = match bitcode::decode::<PacketCrate>(data) {
                Ok(pcrate) => pcrate,
                Err(_) => continue,
            };

            // If we received a message from a requested address - we'll automatically create a connection to it
            if self.requested_connections.remove(&sender) {
                self.connect(sender);

                // Push a connection event
                self.event_queue.push_front(SocketEvent::Connection(sender));
            }

            // Get or make a new socket connection for this message
            let mut connection = self.connections.get_mut(&sender);

            // If this is a message from a known connection - let it mark all the acknowledgments it needs
            if let Some(conn) = connection.as_mut() {
                conn.sent_message_acknowledgments_received(pcrate.msg_base, pcrate.msg_map);
            }

            // Now we're going to iterate every single message
            for message in pcrate.messages {
                // If we have a connection and our message contains a message ID - we're going to notify our connection about it
                if let Some(conn) = connection.as_mut() {
                    // Process it (filter, reorder it and so on)
                    conn.process_message(message);
                } else {
                    // In any other case we're just going to buffer it without a connection
                    self.message_queue.push_back(ReceivedMessage {
                        sender: sender,
                        reliability: message.reliability(),
                        data: message.consume_payload().unwrap(),
                    });
                }
            }
        }
    }

    /// Poll all our connections and receive their messages
    fn poll_connections(&mut self, dt: f32) {
        for (_, connection) in self.connections.iter_mut() {
            connection.poll(&mut self.socket, &mut self.packet_builder, dt);

            while let Some(message) = connection.recv_message() {
                let reliability = message.reliability();
                let sender = connection.to_addr();

                // Finally, add it to our queue
                self.message_queue.push_back(ReceivedMessage {
                    data: message.consume_payload().unwrap(),
                    reliability,
                    sender,
                });
            }
        }
    }

    /// Poll this socket thus updating its inner receive buffer and sending data.
    ///
    /// # Panics
    /// This will panic if delta is negative. It doesn't make any sense.
    pub fn poll(&mut self, dt: f32) {
        assert!(dt >= 0.0);

        // We're going to collect all messages received by this socket
        self.receive_messages();

        // Now, we're going to poll each connection individually as well
        self.poll_connections(dt);
    }

    /// Try receive a message (if there is any)
    pub fn recv_from(&mut self) -> Option<ReceivedMessage> {
        self.message_queue.pop_front()
    }

    /// Take a socket event if one is present
    pub fn get_event(&mut self) -> Option<SocketEvent> {
        self.event_queue.pop_front()
    }

    /// Check if the socket has any messages
    pub fn has_messages(&self) -> bool {
        !self.message_queue.is_empty()
    }

    /// Check if the socket has any events
    pub fn has_events(&self) -> bool {
        !self.event_queue.is_empty()
    }

    /// "Establish" a new connection to the provided address.
    ///
    /// This doesn't actually establish anything, it just creates a logical connection between this address.
    /// Note that if the other address doesn't send any messages whatsoever - this connection will close very quickly.
    pub fn connect(&mut self, addr: net::SocketAddr) {
        self.connections
            .entry(addr)
            .or_insert(SocketConnection::new(addr));
    }

    /// How many messages are available
    pub fn messages(&self) -> usize {
        self.message_queue.len()
    }
}
