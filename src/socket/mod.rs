use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
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
    socket_addr,
};

use connection::SocketConnection;

/// A socket event describes some really rare socket events like connections and disconnections.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SocketEvent {
    /// A connection was established with the provided socket.
    ///
    /// You can establish one simply by sending a message or responding to one
    Connection(net::SocketAddr),

    /// A connection was terminated with the provided socket.
    ///
    /// This happens when there are no more packets received from the other socket
    Disconnection(net::SocketAddr),
}

/// A message received from another socket
#[derive(Debug)]
pub struct ReceivedMessage {
    /// The payload of the message
    pub data: Vec<u8>,

    /// The sender behind this message
    pub sender: net::SocketAddr,

    /// The reliability of the message
    pub reliability: Reliability,
}

/// Socket customization settings which you can provide to extend default sockets
#[derive(Debug, Clone, Copy)]
pub struct SocketOptions {
    /// Whether this socket can broadcast messages. Without this setting, sockets can't send any broadcast packets.
    pub broadcaster: bool,

    /// Whether the address of this socket can be reused by other sockets. This is primarily useful for broadcast listeners, so usually keep it on default
    pub reuses_address: bool,
}

#[allow(clippy::derivable_impls, reason = "Keeping for explicitness")]
impl Default for SocketOptions {
    fn default() -> Self {
        Self {
            broadcaster: false,
            reuses_address: false,
        }
    }
}

/// The reliable socket used for reliable communications.
///
/// Automatically handles connection management (except for heartbeats), reliable delivery and so on.
pub struct Socket {
    socket: SimpleSock,
    options: SocketOptions,

    packet_builder: PacketCrateBuilder,
    connections: HashMap<net::SocketAddr, SocketConnection>,

    event_queue: VecDeque<SocketEvent>,
    message_queue: VecDeque<ReceivedMessage>,
}

impl Socket {
    /// Create a new socket on the provided address with extended configurations
    pub fn new_ex(addr: net::SocketAddr, options: SocketOptions) -> io::Result<Self> {
        let socket = SimpleSock::new_ex(
            addr,
            MTU_SIZE_PRIVATE,
            SockSettings {
                broadcaster: options.broadcaster,
                reuses_address: options.reuses_address,
            },
        )?;

        Ok(Self {
            socket,
            options,

            packet_builder: PacketCrateBuilder::new(MTU_SIZE_PRIVATE),
            connections: HashMap::new(),

            event_queue: VecDeque::new(),
            message_queue: VecDeque::new(),
        })
    }

    /// Create a simple socket on the provided address with default configurations.
    ///
    /// For extended configuration see [Socket::new_ex]
    pub fn new(addr: net::SocketAddr) -> io::Result<Self> {
        Self::new_ex(addr, SocketOptions::default())
    }

    /// Get a reference to the socket options used when creating the socket.
    pub fn options(&self) -> &SocketOptions {
        &self.options
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
    /// Panics if the amount of bytes sent exceeds the [MTU_SIZE] limit
    pub fn send_to(&mut self, to: &net::SocketAddr, data: &[u8], how: Reliability) {
        assert!(
            data.len() <= MTU_SIZE,
            "Reached the MTU limit of {MTU_SIZE}"
        );

        self.connect(*to);

        self.connections
            .get_mut(to)
            .unwrap()
            .queue_message(data.to_owned(), how);
    }

    /// Broadcast a message over the provided port. This operation doesn't establish any connection, is immediate, doesn't batch messages and
    /// is **always** unreliable. Essentially raw UDP.
    ///
    /// # Panics
    /// - If the socket isn't a broadcast socket (you create one with [Socket::new_ex])
    /// - If the amount of bytes sent exceeds the [MTU_SIZE] limit
    pub fn broadcast(&mut self, port: u16, data: &[u8]) {
        assert!(
            data.len() <= MTU_SIZE,
            "Reached the MTU limit of {MTU_SIZE}"
        );
        assert!(self.options.broadcaster, "Socket can't broadcast packets");

        let _ = self.socket.send_to(data, socket_addr!(broadcast;port));
    }

    /// Receive and distribute (to connections) messages
    fn receive_messages(&mut self) {
        while let Some((data, sender)) = self.socket.recv_from() {
            // If it's decodable
            let pcrate = match bitcode::decode::<PacketCrate>(data) {
                Ok(pcrate) => pcrate,
                Err(_) => continue,
            };

            // Get a connection for this sender
            let mut connection = self.connections.get_mut(&sender);

            // If this is a message from a known connection
            if let Some(conn) = connection.as_mut() {
                // let it mark all the acknowledgments it needs
                conn.sent_message_acknowledgments_received(pcrate.msg_base, pcrate.msg_map);

                // and reset its hearbeat timer as well
                conn.reset_heartbeat_timer();
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
                        sender,
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

            // If this connection has timed out - we're going to notify the user
            if connection.timed_out() {
                self.event_queue
                    .push_back(SocketEvent::Disconnection(connection.to_addr()));
            }
        }

        // Clear out all connections that were timed out
        self.connections
            .retain(|_, connection| !connection.timed_out());
    }

    /// Poll this socket thus updating its inner receive buffer and sending data.
    ///
    /// # Panics
    /// This will panic if delta is negative. It doesn't make any sense.
    pub fn poll(&mut self, dt: f32) {
        assert!(dt >= 0.0, "Delta time must be positive or zero");

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
        // If there's not yet connection
        if !self.is_connected(&addr) {
            self.event_queue.push_back(SocketEvent::Connection(addr));
            self.connections.insert(addr, SocketConnection::new(addr));
        }
    }

    /// Disconnect from the provided address.
    ///
    /// As with [Socket::connect], all this is doing is just removing a *logical* connection to the address.
    /// Doesn't do anything it no connection was established.
    pub fn disconnect(&mut self, addr: net::SocketAddr) {
        if self.is_connected(&addr) {
            self.event_queue.push_back(SocketEvent::Disconnection(addr));
            let _ = self.connections.remove(&addr);
        }
    }

    /// Clear the event queue. Super useful if you wish to ignore events
    pub fn clear_events(&mut self) {
        self.event_queue.clear();
    }

    /// Identical to [Socket::clear_events].
    pub fn clear_messages(&mut self) {
        self.message_queue.clear();
    }

    /// How many messages are available
    pub fn messages(&self) -> usize {
        self.message_queue.len()
    }
}

impl Debug for Socket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<Socket: {}>", self.addr())
    }
}
